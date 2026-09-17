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

package org.apache.hugegraph.store.client;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.script.ScriptException;
import javax.script.SimpleBindings;

import org.apache.hugegraph.security.HugeSecurityManager;
import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.client.util.ExecutorPool;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class OrderedScanSecurityTest {

    @Test
    public void testOrderedScanFromGremlin() throws Exception {
        assertScanUnderSandbox(null);
    }

    @Test
    public void testOrderedScanAfterWorkerExpiry() throws Exception {
        ThreadPoolExecutor executor = ExecutorPool.createExecutor(
                "ordered-scan-expiry-test", 1L, 0, 2,
                new ThreadPoolExecutor.AbortPolicy());
        try {
            executor.submit(() -> { }).get(3L, TimeUnit.SECONDS);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
            while (executor.getPoolSize() != 0 && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            Assert.assertEquals(0, executor.getPoolSize());
            assertScanUnderSandbox(executor);
        } finally {
            executor.shutdownNow();
        }
    }

    @SuppressWarnings("unchecked")
    private static void assertScanUnderSandbox(ExecutorService executor) throws Exception {
        HgKvIterator<HgKvEntry> first = Mockito.mock(HgKvIterator.class);
        HgKvIterator<HgKvEntry> second = Mockito.mock(HgKvIterator.class);
        GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        try (OrderedKvIterator scan = executor == null ?
                                      new OrderedKvIterator(Arrays.asList(first, second), 0L) :
                                      new OrderedKvIterator(Arrays.asList(first, second), 0L, executor)) {
            engine.eval("1 + 1");
            SimpleBindings bindings = new SimpleBindings();
            // Prime class loading without creating an initializer worker.
            HgKvIterator<HgKvEntry> warmup = Mockito.mock(HgKvIterator.class);
            try (OrderedKvIterator single = new OrderedKvIterator(
                    Collections.singletonList(warmup), 0L)) {
                bindings.put("scan", single);
                Assert.assertEquals(false, engine.eval("scan.hasNext()", bindings));
            }
            bindings.put("scan", scan);
            String factoryScript = "org.apache.hugegraph.store.client.util.ExecutorPool." +
                                   "newThreadFactory('untrusted').newThread({} as Runnable)";
            engine.eval(factoryScript);
            SecurityManager previous = System.getSecurityManager();
            String name = Thread.currentThread().getName();
            Thread.currentThread().setName("gremlin-server-exec-ordered-scan-test");
            System.setSecurityManager(new HugeSecurityManager());
            try {
                ScriptException denied = Assert.assertThrows(ScriptException.class,
                        () -> engine.eval("new Thread()"));
                Assert.assertTrue(denied.getMessage().contains(
                        "Not allowed to access thread group via Gremlin"));
                Assert.assertEquals(false, engine.eval("scan.hasNext()", bindings));
                denied = Assert.assertThrows(ScriptException.class,
                        () -> engine.eval(factoryScript));
                Assert.assertTrue(denied.getMessage().contains(
                        "Not allowed to access thread group via Gremlin"));
            } finally {
                System.setSecurityManager(previous);
                Thread.currentThread().setName(name);
            }
        }
        Mockito.verify(first).hasNext();
        Mockito.verify(second).hasNext();
        Mockito.verify(first).close();
        Mockito.verify(second).close();
    }
}
