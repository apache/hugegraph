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

package org.apache.hugegraph.unit.store;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.backend.store.ram.IntObjectMap;
import org.junit.Assert;
import org.junit.Test;

public class RamIntObjectMapTest {

    @Test
    public void testConcurrency() throws Exception {
        int size = 32;
        IntObjectMap<Integer> map = new IntObjectMap<>(size);
        int numThreads = 10;
        CountDownLatch startSignal = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<?>> workers = new ArrayList<>();
        try {
            for (int i = 0; i < numThreads; i++) {
                workers.add(executor.submit(() -> {
                    startSignal.await();
                    for (int j = 0; j < size; j++) {
                        map.set(j, j);
                    }
                    return null;
                }));
            }
            startSignal.countDown();
            for (Future<?> worker : workers) {
                worker.get(10, TimeUnit.SECONDS);
            }
            workers.clear();
            for (int i = 0; i < numThreads; i++) {
                workers.add(executor.submit(() -> {
                    for (int j = 0; j < size; j++) {
                        Assert.assertEquals(Integer.valueOf(j), map.get(j));
                    }
                }));
            }
            for (Future<?> worker : workers) {
                worker.get(10, TimeUnit.SECONDS);
            }
            Assert.assertEquals(size, map.size());
            for (int j = 0; j < size; j++) {
                Assert.assertEquals(Integer.valueOf(j), map.get(j));
            }
        } finally {
            startSignal.countDown();
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
