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
package org.apache.pulsar.common.util.collections;

import java.util.HashMap;

public class ConcurrentTripleLong2LongHashMap {
    public class TripleLong{
        public long first;
        public long second;
        public long third;
        @Override
        public int hashCode() {
            return Long.hashCode(first) ^ Long.hashCode(second) ^ Long.hashCode(third);
        }
        @Override
        public boolean equals(Object obj) {
            if(obj instanceof TripleLong){
                TripleLong other = (TripleLong) obj;
                return first == other.first && second == other.second && third == other.third;
            }
            return false;
        }
    }

    private HashMap<TripleLong, Long> map;
    public ConcurrentTripleLong2LongHashMap(){
        // TODO: use hashmap for now
        map = new HashMap<>();
    }
    public void put(long first, long second, long third, long value){
        TripleLong key = new TripleLong();
        key.first = first;
        key.second = second;
        key.third = third;
        map.put(key, value);
    }
    public long get(long first, long second, long third){
        TripleLong key = new TripleLong();
        key.first = first;
        key.second = second;
        key.third = third;
        return map.get(key);
    }
    public long remove(long first, long second, long third){
        TripleLong key = new TripleLong();
        key.first = first;
        key.second = second;
        key.third = third;
        return map.remove(key);
    }
    public boolean containsKey(long first, long second, long third){
        TripleLong key = new TripleLong();
        key.first = first;
        key.second = second;
        key.third = third;
        return map.containsKey(key);
    }
    public void clear(){
        map.clear();
    }
    public long size(){
        return map.size();
    }
    public boolean isEmpty() {
        return map.isEmpty();
    }

    public interface TripleLongConsumer {
        void call(long first, long second, long third, long value);
    }
    public void forEach(TripleLongConsumer consumer){
        for(TripleLong key : map.keySet()){
            consumer.call(key.first, key.second, key.third, map.get(key));
        }
    }

}
