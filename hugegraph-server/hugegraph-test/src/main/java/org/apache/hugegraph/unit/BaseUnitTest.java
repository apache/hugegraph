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

package org.apache.hugegraph.unit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.TimeUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;

public class BaseUnitTest {

    protected static final Logger LOG = Log.logger(BaseUnitTest.class);

    @BeforeClass
    public static void init() {
        // pass
    }

    @AfterClass
    public static void clear() throws Exception {
        // pass
    }

    public static <V> void assertCollectionEquals(Collection<V> list1,
                                                  Collection<V> list2) {
        Assert.assertEquals(list1.size(), list2.size());
        Iterator<V> iter1 = list1.iterator();
        Iterator<V> iter2 = list2.iterator();
        while (iter1.hasNext()) {
            Assert.assertTrue(iter2.hasNext());
            Assert.assertEquals(iter1.next(), iter2.next());
        }
    }

    protected static final void runWithThreads(int threads, Runnable task) {
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(executor.submit(task));
        }
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                Throwable root = HugeException.rootCause(e);
                LOG.error(root.getMessage(), root);
                throw new RuntimeException(root.getMessage(), e);
            }
        }
    }

    protected static final void waitTillNext(long seconds) {
        TimeUtil.tillNextMillis(TimeUtil.timeGen() + seconds * 1000);
    }
}
