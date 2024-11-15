/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl;

import static org.apache.pulsar.client.impl.UnAckedMessageTracker.addChunkedMessageIdsAndRemoveFromSequenceMap;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import java.io.Closeable;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NegativeAcksTracker implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(NegativeAcksTracker.class);

    // timestamp -> ledgerId -> entryId, no need to batch index, if different messages have
    // different timestamp, there will be multiple entries in the map
    // AVL Tree -> LongOpenHashMap -> Roaring64Bitmap
    // there are many timestamp, a few ledgerId, many entryId
    private Long2ObjectSortedMap<Long2ObjectMap<Roaring64Bitmap>> nackedMessages = null;

    private final ConsumerBase<?> consumer;
    private final Timer timer;
    private final long nackDelayMs;
    private final RedeliveryBackoff negativeAckRedeliveryBackoff;
    private final int negativeAckPrecisionBitCnt;

    private Timeout timeout;

    // Set a min delay to allow for grouping nacks within a single batch
    private static final long MIN_NACK_DELAY_MS = 100;
    private static final int DUMMY_PARTITION_INDEX = -2;

    public NegativeAcksTracker(ConsumerBase<?> consumer, ConsumerConfigurationData<?> conf) {
        this.consumer = consumer;
        this.timer = consumer.getClient().timer();
        this.nackDelayMs = Math.max(TimeUnit.MICROSECONDS.toMillis(conf.getNegativeAckRedeliveryDelayMicros()),
                MIN_NACK_DELAY_MS);
        this.negativeAckRedeliveryBackoff = conf.getNegativeAckRedeliveryBackoff();
        this.negativeAckPrecisionBitCnt = conf.getNegativeAckPrecisionBitCnt();
    }

    private synchronized void triggerRedelivery(Timeout t) {
        if (nackedMessages.isEmpty()) {
            this.timeout = null;
            return;
        }

        // Group all the nacked messages into one single re-delivery request
        Set<MessageId> messagesToRedeliver = new HashSet<>();
        long currentTimestamp = System.currentTimeMillis();
        for (long timestamp : nackedMessages.keySet()) {
            if (timestamp > currentTimestamp) {
                // We are done with all the messages that need to be redelivered
                break;
            }

            Long2ObjectMap<Roaring64Bitmap> ledgerMap = nackedMessages.get(timestamp);
            for (Long2ObjectMap.Entry<Roaring64Bitmap> ledgerEntry : ledgerMap.long2ObjectEntrySet()) {
                long ledgerId = ledgerEntry.getLongKey();
                Roaring64Bitmap entrySet = ledgerEntry.getValue();
                entrySet.forEach(entryId -> {
                    MessageId msgId = new MessageIdImpl(ledgerId, entryId, DUMMY_PARTITION_INDEX);
                    addChunkedMessageIdsAndRemoveFromSequenceMap(msgId, messagesToRedeliver, this.consumer);
                    messagesToRedeliver.add(msgId);
                });
            }
        }

        if (!messagesToRedeliver.isEmpty()) {
            // remove entries from the nackedMessages map
            for(long timestamp : nackedMessages.keySet()) {
                if (timestamp <= currentTimestamp) {
                    nackedMessages.remove(timestamp);
                } else {
                    break;
                }
            }
            consumer.onNegativeAcksSend(messagesToRedeliver);
            log.info("[{}] {} messages will be re-delivered", consumer, messagesToRedeliver.size());
            consumer.redeliverUnacknowledgedMessages(messagesToRedeliver);
        }

        if (!nackedMessages.isEmpty()) {
            long nextTriggerTimestamp = nackedMessages.firstLongKey();
            long delayMs = Math.max(nextTriggerTimestamp - currentTimestamp, 0);
            if (delayMs > 0) {
                this.timeout = timer.newTimeout(this::triggerRedelivery, delayMs, TimeUnit.MILLISECONDS);
            } else {
                this.timeout = timer.newTimeout(this::triggerRedelivery, 0, TimeUnit.MILLISECONDS);
            }
        } else {
            this.timeout = null;
        }
    }

    public synchronized void add(MessageId messageId) {
        add(messageId, 0);
    }

    public synchronized void add(Message<?> message) {
        add(message.getMessageId(), message.getRedeliveryCount());
    }

    static long trimLowerBit(long timestamp, int bits) {
        return timestamp & (-1L << bits);
    }

    private synchronized void add(MessageId messageId, int redeliveryCount) {
        if (nackedMessages == null) {
            nackedMessages = new Long2ObjectAVLTreeMap<>();
        }

        long backoffMs;
        if (negativeAckRedeliveryBackoff != null) {
            backoffMs = TimeUnit.MILLISECONDS.toMillis(negativeAckRedeliveryBackoff.next(redeliveryCount));
        } else {
            backoffMs = nackDelayMs;
        }
        MessageIdAdv messageIdAdv = (MessageIdAdv) messageId;
        long timestamp = trimLowerBit(System.currentTimeMillis() + backoffMs, negativeAckPrecisionBitCnt);
        if (nackedMessages.containsKey(timestamp)) {
            Long2ObjectMap<Roaring64Bitmap> ledgerMap = nackedMessages.get(timestamp);
            if (ledgerMap.containsKey(messageIdAdv.getLedgerId())) {
                Roaring64Bitmap entrySet = ledgerMap.get(messageIdAdv.getLedgerId());
                entrySet.add(messageIdAdv.getEntryId());
            } else {
                Roaring64Bitmap entrySet = new Roaring64Bitmap();
                entrySet.add(messageIdAdv.getEntryId());
                ledgerMap.put(messageIdAdv.getLedgerId(), entrySet);
            }
        } else {
            Roaring64Bitmap entrySet = new Roaring64Bitmap();
            entrySet.add(messageIdAdv.getEntryId());
            Long2ObjectMap<Roaring64Bitmap> ledgerMap = new Long2ObjectAVLTreeMap<>();
            ledgerMap.put(messageIdAdv.getLedgerId(), entrySet);
            nackedMessages.put(timestamp, ledgerMap);
        }

        if (this.timeout == null) {
            // Schedule a task and group all the redeliveries for same period. Leave a small buffer to allow for
            // nack immediately following the current one will be batched into the same redeliver request.
            this.timeout = timer.newTimeout(this::triggerRedelivery, backoffMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Discard the partition index from the message id.
     * @param messageId
     * @return
     */
    static public MessageId discardPartitionIndex(MessageId messageId) {
        if (messageId instanceof BatchMessageIdImpl) {
            BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) messageId;
            return new BatchMessageIdImpl(batchMessageId.getLedgerId(), batchMessageId.getEntryId(),
                    DUMMY_PARTITION_INDEX, batchMessageId.getBatchIndex(), batchMessageId.getBatchSize(),
                    batchMessageId.getAckSet());
        } else if (messageId instanceof MessageIdImpl) {
            MessageIdImpl messageID = (MessageIdImpl) messageId;
            return new MessageIdImpl(messageID.getLedgerId(), messageID.getEntryId(), DUMMY_PARTITION_INDEX);
        } else {
            return messageId;
        }
    }

    @VisibleForTesting
    long getNackedMessagesCount() {
        if (nackedMessages == null) {
            return 0;
        }
        return nackedMessages.values().stream().mapToLong(
                ledgerMap -> ledgerMap.values().stream().mapToLong(
                        Roaring64Bitmap::getLongCardinality).sum()).sum();
    }

    @Override
    public synchronized void close() {
        if (timeout != null && !timeout.isCancelled()) {
            timeout.cancel();
            timeout = null;
        }

        if (nackedMessages != null) {
            nackedMessages.clear();
            nackedMessages = null;
        }
    }
}
