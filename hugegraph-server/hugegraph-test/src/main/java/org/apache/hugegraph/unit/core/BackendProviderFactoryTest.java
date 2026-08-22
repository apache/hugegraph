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

package org.apache.hugegraph.unit.core;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.store.BackendProviderFactory;
import org.apache.hugegraph.backend.store.memory.InMemoryDBStoreProvider;
import org.apache.hugegraph.backend.store.raft.RaftBackendStoreProvider;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.Test;

public class BackendProviderFactoryTest extends BaseUnitTest {

    @Test
    public void testRegister() {
        String name = "fake-provider";
        String provider = InMemoryDBStoreProvider.class.getName();

        BackendProviderFactory.register(name, provider);
        BackendProviderFactory.register(name, provider);

        Assert.assertThrows(BackendException.class, () -> {
            BackendProviderFactory.register(
                    name, RaftBackendStoreProvider.class.getName());
        }, e -> {
            Assert.assertContains("Exists BackendStoreProvider:",
                                  e.getMessage());
        });
    }

    @Test
    public void testRegisterConcurrently() throws Exception {
        String name = "concurrent-provider-" + System.nanoTime();
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            List<Future<Class<?>>> registrations = List.of(
                    executor.submit(() -> {
                        ready.countDown();
                        start.await();
                        BackendProviderFactory.register(
                                name,
                                InMemoryDBStoreProvider.class.getName());
                        return InMemoryDBStoreProvider.class;
                    }),
                    executor.submit(() -> {
                        ready.countDown();
                        start.await();
                        BackendProviderFactory.register(
                                name,
                                RaftBackendStoreProvider.class.getName());
                        return RaftBackendStoreProvider.class;
                    }));

            Assert.assertTrue(ready.await(5L, TimeUnit.SECONDS));
            start.countDown();

            int successes = 0;
            int conflicts = 0;
            Class<?> registered = null;
            for (Future<Class<?>> registration : registrations) {
                try {
                    registered = registration.get(5L, TimeUnit.SECONDS);
                    successes++;
                } catch (ExecutionException e) {
                    Assert.assertInstanceOf(BackendException.class,
                                            e.getCause());
                    Assert.assertContains("Exists BackendStoreProvider:",
                                          e.getCause().getMessage());
                    conflicts++;
                }
            }

            Assert.assertEquals(1, successes);
            Assert.assertEquals(1, conflicts);
            Assert.assertNotNull(registered);
            BackendProviderFactory.register(name, registered.getName());
        } finally {
            start.countDown();
            executor.shutdownNow();
        }
    }
}
