/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.store.node;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.config.ConfigException;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.rocksdb.access.RocksDBOptions;
import org.junit.Test;

/**
 * The raft disruptor and write buffer sizes AppConfig derives from the RocksDB budget.
 */
public class AppConfigTuningTest {

    private static final long MIB = 1024L * 1024L;

    @Test
    public void testDisruptorBufferSizeIsNeverZero() {
        assertEquals(32, AppConfig.disruptorBufferSize(512 * MIB));
        assertEquals(32, AppConfig.disruptorBufferSize(1_000_000_000L));
        assertEquals(1024, AppConfig.disruptorBufferSize(32_000_000_000L));
    }

    @Test
    public void testDerivedWriteBufferSizePassesRocksdbValidation() {
        // The 4 GiB container from apache/hugegraph#3254 auto-sizes to 512 MiB, whose
        // thousandth is under the 1 MiB rocksdb.write_buffer_size minimum
        long budget = 512 * MIB;
        assertEquals(MIB, AppConfig.writeBufferSize(budget));
        assertEquals(MIB, AppConfig.writeBufferSize(MIB));
        assertEquals(32_000_000L, AppConfig.writeBufferSize(32_000_000_000L));

        // The same validation BusinessHandlerImpl.initRocksdb() runs at Store startup
        OptionSpace.register("rocksdb", RocksDBOptions.class.getName());
        RocksDBOptions.instance();
        HugeConfig config = new HugeConfig(rocksdbConfig(budget,
                                                         AppConfig.writeBufferSize(budget)));
        assertEquals(MIB, (long) config.get(RocksDBOptions.MEMTABLE_SIZE));
        try {
            new HugeConfig(rocksdbConfig(budget, budget / 1000));
            fail("accepted a write buffer under 1 MiB");
        } catch (ConfigException e) {
            assertTrue(e.getMessage().contains("write_buffer_size"));
        }
    }

    private static Map<String, Object> rocksdbConfig(long totalMemory, long writeBufferSize) {
        Map<String, Object> config = new HashMap<>();
        config.put("rocksdb.total_memory_size", String.valueOf(totalMemory));
        config.put("rocksdb.write_buffer_size", String.valueOf(writeBufferSize));
        return config;
    }
}
