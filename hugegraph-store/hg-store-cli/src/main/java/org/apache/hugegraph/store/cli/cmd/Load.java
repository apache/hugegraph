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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.pd.cli.cmd.Command;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgStoreClient;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.cli.util.HgCliUtil;

import lombok.extern.slf4j.Slf4j;

/**
 * @date 2023/10/18
 **/
@Slf4j
public class Load extends Command {

    private static final int batchSize = 100000;
    private static int readerSize = 5;
    private static final long printSize = 10000000;
    private static final long printCount = printSize * 1000;
    private final int pc = Runtime.getRuntime().availableProcessors();
    private final int size = pc * 2;
    private final Semaphore semaphore = new Semaphore(size);
    private final ThreadPoolExecutor executor =
            new ThreadPoolExecutor(size, size, 0L, TimeUnit.MILLISECONDS,
                                   new LinkedBlockingQueue<Runnable>());
    private final LinkedBlockingQueue<List<String>> queue = new LinkedBlockingQueue<>(size * 2);
    private final HgStoreClient storeClient;
    private final AtomicLong insertCount = new AtomicLong();
    private final AtomicLong startTime = new AtomicLong();
    private String table;
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private String graph;
    protected Runnable r = () -> {
        long start = System.currentTimeMillis();
        try {
            while (!completed.get() || queue.peek() != null) {
                semaphore.acquire();
                List<String> data = queue.take();
                Runnable task = () -> {
                    try {
                        put(table, data);
                    } catch (Exception e) {
                        log.error("put data with error:", e);
                    } finally {
                        semaphore.release();
                    }
                };
                executor.submit(task);
            }
            semaphore.acquire(size);
            semaphore.release(size);
            log.info("*************************************************");
            long all = insertCount.get();
            long end = System.currentTimeMillis();
            log.info("Load data: {}s，total: {} entries，average：{} entries/s", (end - start) / 1000,
                     all, all * 1000 / (end - start));
            log.info("*************************************************");
        } catch (Exception e) {
            log.error("submit task with error:", e);
        } finally {
            try {
                executor.shutdownNow();
            } catch (Exception e) {

            }
        }
    };

    public Load(String pd) {
        super(pd);
        storeClient = HgStoreClient.create(config);
    }

    @Override
    public void action(String[] params) throws InterruptedException {
        if (params == null || params.length < 3) {
            log.error("usage: load <graph> <file1[,file2...]> <table>");
            return;
        }
        graph = params[0];
        this.table = params[2];
        Thread loadThread = new Thread(r, "load");
        loadThread.start();
        String path = params[1];
        String[] split = path.split(",");
        readerSize = split.length;
        CountDownLatch latch = new CountDownLatch(readerSize);
        log.info("--- start data loading---");
        for (int i = 0; i < readerSize; i++) {
            int fi = i;
            new Thread(() -> {
                try(InputStreamReader isr = new InputStreamReader(new FileInputStream(split[fi]),
                                                                       StandardCharsets.UTF_8);
                    BufferedReader reader = new BufferedReader(isr)) {
                    long count = 0;
                    String line;
                    try {
                        List<String> keys = new ArrayList<>(batchSize);
                        while ((line = reader.readLine()) != null) {
                            keys.add(line);
                            count++;
                            if (count % batchSize == 0) {
                                List<String> data = keys;
                                if (!data.isEmpty()) {
                                    queue.put(keys);
                                    keys = new ArrayList<>(batchSize);
                                }
                                continue;
                            }
                        }
                        if (count % batchSize != 0) {
                            queue.put(keys);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } catch (Exception e) {
                    log.error("send data with error:", e);
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        latch.await();
        completed.set(true);
        loadThread.join();
    }

    public boolean put(String table, List<String> keys) {
        HgStoreSession session = storeClient.openSession(graph);
        try {
            session.beginTx();
            for (String key : keys) {
                int j = key.indexOf("\t");
                if (j <= 0 || j == key.length() - 1) {
                    log.warn("skip bad line: {}", key);
                    continue;
                }
                String owner = key.substring(0, j);
                HgOwnerKey hgKey = HgCliUtil.toOwnerKey(owner, owner);
                byte[] value = HgCliUtil.toBytes(key.substring(j + 1));
                session.put(table, hgKey, value);
            }
            session.commit();
        } catch (Exception e) {
            log.error("batch put failed, rolling back. size={}", keys.size(), e);
            try {
                session.rollback();
            } catch (Exception e1) {
                log.error("rolling back failed", e1);
            }
            return false;
        }
        long sum;
        if ((sum = insertCount.addAndGet(keys.size())) % printSize == 0) {
            long c = System.currentTimeMillis();
            long start = startTime.getAndSet(c);
            if (c > start) {
                log.info("count: {}, tps: {}, worker: {},task queue: {}", sum,
                         printCount / (c - start), executor.getActiveCount(), queue.size());
            }
        }
        return true;
    }

}
