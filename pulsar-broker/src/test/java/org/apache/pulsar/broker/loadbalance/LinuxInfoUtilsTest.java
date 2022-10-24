/**
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
package org.apache.pulsar.broker.loadbalance;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.assertEquals;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class LinuxInfoUtilsTest {

    /**
     * simulate reading contents in /sys/fs/cgroup/cpuset/cpuset.cpus to get the number of Cpus
     * and return the limit of cpu.
     */
    @Test
    public void testGetTotalCpuCountAndLimit() throws IOException {
        try (MockedStatic<LinuxInfoUtils> linuxInfoUtils = mockStatic(LinuxInfoUtils.class)) {
            linuxInfoUtils.when(() -> LinuxInfoUtils.readTrimStringFromFile(any())).thenReturn("0-2,16,20-30");
            linuxInfoUtils.when(() -> LinuxInfoUtils.getTotalCpuCount()).thenCallRealMethod();
            assertEquals(LinuxInfoUtils.getTotalCpuCount(), 15);

            //set quota to -1
            linuxInfoUtils.when(() -> LinuxInfoUtils.readLongFromFile(any())).thenReturn(-1L);
            linuxInfoUtils.when(() -> LinuxInfoUtils.getTotalCpuLimit(true)).thenCallRealMethod();
            assertEquals(LinuxInfoUtils.getTotalCpuLimit(true), 1500);
        }
    }
}
