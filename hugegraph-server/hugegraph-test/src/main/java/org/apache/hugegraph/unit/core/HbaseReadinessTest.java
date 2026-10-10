/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.unit.core;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hugegraph.backend.store.hbase.HbaseStore;
import org.apache.hugegraph.testutil.Assert;
import org.junit.AfterClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/** The HBase probe outcome for available tables, one disabled table, a failing and a hung check. */
public class HbaseReadinessTest {

    private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();

    @AfterClass
    public static void shutdown() {
        EXECUTOR.shutdownNow();
    }

    private static Map<String, Object> readiness(Callable<Integer> check, long timeout) {
        return HbaseStore.readinessOf(check, timeout, EXECUTOR);
    }

    @Test
    public void testOutcomes() throws Exception {
        Map<String, Object> ok = readiness(() -> 0, 500L);
        Assert.assertEquals(true, ok.get("ready"));
        Assert.assertEquals("ok", ok.get("reason"));
        Assert.assertTrue(((Number) ok.get("hbase_millis")).longValue() >= 0L);

        // an existing but disabled table is not available, whichever table it is
        List<String> tables = ImmutableList.of("g_v", "g_oe", "g_ie", "g_si");
        Map<String, Object> disabled = readiness(() -> HbaseStore.firstUnavailable(tables, t -> !t.equals("g_ie")),
                                                 500L);
        Assert.assertEquals(false, disabled.get("ready"));
        Assert.assertEquals("table 3 of the graph is not available", disabled.get("reason"));
        Assert.assertEquals(0, HbaseStore.firstUnavailable(tables, t -> true));
        // the schema store's tables and its counters come first: a disabled counters table alone
        // is not ready, with its own number
        List<String> all = ImmutableList.of("s_pk", "s_vl", "s_el", "s_il", "c", "g_v", "g_oe", "g_ie", "g_si", "m");
        Assert.assertEquals(5, HbaseStore.firstUnavailable(all, t -> !t.equals("c")));
        Assert.assertEquals(1, HbaseStore.firstUnavailable(all, t -> !t.equals("s_pk")));
        Assert.assertEquals(0, HbaseStore.firstUnavailable(all, t -> true));
        Assert.assertEquals(1, HbaseStore.firstUnavailable(ImmutableList.of(), t -> true));
        // a throwing check surfaces as a failure, not as ready
        Assert.assertThrows(java.io.IOException.class, () -> {
            HbaseStore.firstUnavailable(tables, t -> {
                throw new java.io.IOException("x");
            });
        });

        Map<String, Object> failed = readiness(() -> {
            throw new java.io.IOException("Unable to resolve host hbase-master.svc");
        }, 500L);
        Assert.assertEquals(false, failed.get("ready"));
        Assert.assertEquals("hbase failed: IOException", failed.get("reason"));
        Assert.assertFalse(failed.toString().contains("svc"));

        long start = System.currentTimeMillis();
        Map<String, Object> hung = readiness(() -> {
            Thread.sleep(5_000L);
            return 0;
        }, 300L);
        Assert.assertEquals(false, hung.get("ready"));
        Assert.assertEquals("hbase did not answer within 300 ms", hung.get("reason"));
        Assert.assertTrue(System.currentTimeMillis() - start < 2_000L);
    }
}
