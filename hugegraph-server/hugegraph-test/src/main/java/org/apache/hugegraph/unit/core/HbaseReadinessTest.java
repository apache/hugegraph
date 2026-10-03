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

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hugegraph.backend.store.hbase.HbaseStore;
import org.apache.hugegraph.testutil.Assert;
import org.junit.AfterClass;
import org.junit.Test;

/** The HBase probe outcome for an available, a disabled, a failing and a hung table check. */
public class HbaseReadinessTest {

    private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();

    @AfterClass
    public static void shutdown() {
        EXECUTOR.shutdownNow();
    }

    private static Map<String, Object> readiness(Callable<Boolean> check, long timeout) {
        return HbaseStore.readinessOf(check, timeout, EXECUTOR);
    }

    @Test
    public void testOutcomes() {
        Map<String, Object> ok = readiness(() -> true, 500L);
        Assert.assertEquals(true, ok.get("ready"));
        Assert.assertEquals("ok", ok.get("reason"));
        Assert.assertTrue(((Number) ok.get("hbase_millis")).longValue() >= 0L);

        // an existing but disabled table is not available
        Map<String, Object> disabled = readiness(() -> false, 500L);
        Assert.assertEquals(false, disabled.get("ready"));
        Assert.assertEquals("the graph's first table is not available", disabled.get("reason"));

        Map<String, Object> failed = readiness(() -> {
            throw new java.io.IOException("Unable to resolve host hbase-master.svc");
        }, 500L);
        Assert.assertEquals(false, failed.get("ready"));
        Assert.assertEquals("hbase failed: IOException", failed.get("reason"));
        Assert.assertFalse(failed.toString().contains("svc"));

        long start = System.currentTimeMillis();
        Map<String, Object> hung = readiness(() -> {
            Thread.sleep(5_000L);
            return true;
        }, 300L);
        Assert.assertEquals(false, hung.get("ready"));
        Assert.assertEquals("hbase did not answer within 300 ms", hung.get("reason"));
        Assert.assertTrue(System.currentTimeMillis() - start < 2_000L);
    }
}
