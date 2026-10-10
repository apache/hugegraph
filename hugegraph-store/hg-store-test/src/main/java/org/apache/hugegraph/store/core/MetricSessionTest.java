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

package org.apache.hugegraph.store.core;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.hugegraph.rocksdb.access.RocksDBFactory;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.metric.SystemMetricService;
import org.apache.hugegraph.store.rocksdb.BaseRocksDbTest;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.rocksdb.MemoryUsageType;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;

public class MetricSessionTest extends BaseRocksDbTest {

    @Test
    public void testRepeatedMetricsReturnBorrowedSessions() throws Exception {
        assertMetricsReturnSessions(false);
    }

    @Test
    public void testFailedMetricsReturnBorrowedSessions() throws Exception {
        assertMetricsReturnSessions(true);
    }

    private static void assertMetricsReturnSessions(boolean failStatistics) throws Exception {
        RocksDBFactory factory = RocksDBFactory.getInstance();
        String name = "metric-session-" + UUID.randomUUID();
        RocksDBSession borrowed = factory.createGraphDB(Files.createTempDirectory(name).toString(), name);
        Field sessionsField = RocksDBFactory.class.getDeclaredField("dbSessionMap");
        sessionsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, RocksDBSession> sessions = (Map<String, RocksDBSession>) sessionsField.get(factory);
        RocksDBSession owner = sessions.get(name);
        Field statisticsField = RocksDBSession.class.getDeclaredField("rocksDbStats");
        statisticsField.setAccessible(true);
        RocksDB nativeOwner = owner.getDB();
        Statistics original = owner.getRocksDbStats();
        Statistics failing = Mockito.mock(Statistics.class);
        Mockito.when(failing.getTickerCount(Mockito.any(TickerType.class)))
               .thenThrow(new IllegalStateException("statistics fixture"));
        try {
            if (failStatistics) {
                statisticsField.set(owner, failing);
            }
            HgStoreEngine engine = Mockito.mock(HgStoreEngine.class);
            BusinessHandler business = Mockito.mock(BusinessHandler.class);
            Mockito.when(engine.getBusinessHandler()).thenReturn(business);
            Map<MemoryUsageType, Long> memory = new HashMap<>();
            for (MemoryUsageType type : MemoryUsageType.values()) {
                memory.put(type, 0L);
            }
            Mockito.when(business.getApproximateMemoryUsageByType(null)).thenReturn(memory);
            SystemMetricService service = new SystemMetricService();
            service.setStoreEngine(engine);
            Method collect = SystemMetricService.class.getDeclaredMethod("loadRocksDbInfo", Map.class);
            collect.setAccessible(true);
            int baseline = owner.getRefCount();
            for (int i = 0; i < 3; i++) {
                Map<String, Long> metrics = new HashMap<>();
                collect.invoke(service, metrics);
                if (!failStatistics) {
                    Assert.assertTrue(metrics.containsKey("rocksdb.graph." + name + ".number_keys_written"));
                }
                Assert.assertEquals("metric collection must return its borrowed session", baseline,
                                    owner.getRefCount());
            }
            if (failStatistics) {
                Mockito.verify(failing, Mockito.times(3)).getTickerCount(TickerType.NUMBER_KEYS_WRITTEN);
            }
            statisticsField.set(owner, original);
            borrowed.close();
            factory.releaseGraphDB(name);
            Assert.assertFalse("the final owner release must close native RocksDB", nativeOwner.isOwningHandle());
        } finally {
            statisticsField.set(owner, original);
            borrowed.close();
            factory.releaseGraphDB(name);
            // Also release the native fixture if the old implementation leaked its borrowed references.
            Method shutdown = RocksDBSession.class.getDeclaredMethod("shutdown");
            shutdown.setAccessible(true);
            shutdown.invoke(owner);
        }
    }
}
