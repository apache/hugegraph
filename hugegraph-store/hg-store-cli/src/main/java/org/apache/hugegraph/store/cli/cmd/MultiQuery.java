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

package org.apache.hugegraph.store.cli.cmd;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.pd.cli.cmd.Command;
import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgScanQuery;
import org.apache.hugegraph.store.HgStoreClient;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.cli.util.HgCliUtil;
import org.apache.hugegraph.store.client.grpc.KvCloseableIterator;
import org.apache.hugegraph.store.client.util.MetricX;

import lombok.extern.slf4j.Slf4j;

/**
 * Multi-thread query
 * point: Start from an initial query point, then iteratively use the value obtained from each
 * query as the condition for the next query
 * scanCount: The number of threads allowed to be launched.
 *
 * @date 2023/10/20
 **/
@Slf4j
public class MultiQuery extends Command {

    private static final AtomicLong total = new AtomicLong();
    private static int batchLimit = 100;
    private final HgStoreClient storeClient;
    public String graphName = "hugegraphtest";
    volatile long startTime = System.currentTimeMillis();

    public MultiQuery(String pd) {
        super(pd);
        storeClient = HgStoreClient.create(config);
    }

    @Override
    public void action(String[] params) throws Exception {
        String point = params[0];
        String scanCount = params[1];
        log.info("--- start  startMultiprocessQuery---");
        startTime = System.currentTimeMillis();
        MetricX metrics = MetricX.ofStart();
        batchLimit = Integer.parseInt(scanCount);
        CountDownLatch latch = new CountDownLatch(batchLimit);
        HgStoreSession session = storeClient.openSession(graphName);
        final AtomicLong[] counter = {new AtomicLong()};
        final long[] start = {System.currentTimeMillis()};
        LinkedBlockingQueue[] queue = new LinkedBlockingQueue[batchLimit];
        for (int i = 0; i < batchLimit; i++) {
            queue[i] = new LinkedBlockingQueue();
        }
        List<String> strKey =
                Arrays.asList("20727483", "50329304", "26199460", "1177521",
                              "27960125",
                              "30440025", "15833920", "15015183", "33153097",
                              "21250581");
        strKey.forEach(key -> {
            log.info("newkey:{}", key);
            HgOwnerKey hgKey = HgCliUtil.toOwnerKey(key, key);
            queue[0].add(hgKey);
        });

        for (int i = 0; i < batchLimit; i++) {
            int finalI = i;
            KvCloseableIterator<HgKvIterator<HgKvEntry>> iterators =
                    session.scanBatch2(
                            HgScanQuery.prefixIteratorOf(HgCliUtil.TABLE_NAME, new Iterator<>() {
                                HgOwnerKey current = null;

                                @Override
                                public boolean hasNext() {
                                    while (current == null) {
                                        try {
                                            current = (HgOwnerKey) queue[finalI].poll(1,
                                                                                      TimeUnit.SECONDS);
                                        } catch (InterruptedException e) {
                                            Thread.currentThread().interrupt();
                                        }
                                    }
                                    if (current == null) {
                                        log.info("===== current is null ==========");
                                    }
                                    return current != null;
                                }

                                @Override
                                public HgOwnerKey next() {
                                    return current;
                                }
                            })
                    );

            new Thread(() -> {
                try {
                    while (iterators.hasNext()) {
                        HgKvIterator<HgKvEntry> iterator = iterators.next();
                        long c = 0;
                        while (iterator.hasNext()) {
                            String newPoint = HgCliUtil.toStr(iterator.next().value());
                            HgOwnerKey newHgKey = HgCliUtil.toOwnerKey(newPoint, newPoint);
                            if (queue[(int) (c % batchLimit)].size() < 1000000) {
                                queue[(int) (c % batchLimit)].add(newHgKey);
                            }
                            c++;
                        }
                        if (counter[0].addAndGet(c) > 1000000) {
                            synchronized (counter) {
                                if (counter[0].get() > 10000000) {
                                    log.info("count {}, qps {}", counter[0].get(),
                                             counter[0].get() * 1000 /
                                             (System.currentTimeMillis() - start[0]));
                                    start[0] = System.currentTimeMillis();
                                    counter[0].set(0);
                                }
                            }
                        }
                    }
                } finally {
                    latch.countDown();
                }
            }, "client query thread:" + i).start();
            log.info("===== read thread exit ==========");
        }
        latch.await();

        metrics.end();
        log.info("*************************************************");
        log.info("Main Thread process time :" + metrics.past() / 1000 + "seconds; query ：" +
                 total.get()
                 + "times，qps:" + total.get() * 1000 / metrics.past());
        log.info("*************************************************");
        System.out.println("-----Main thread end---------");
    }
}
