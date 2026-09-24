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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.pd.cli.cmd.Command;

import lombok.extern.slf4j.Slf4j;

/**
 * @date 2023/10/18
 **/
@Slf4j
public class ScanShard extends Command implements Scan {

    private final AtomicInteger sum = new AtomicInteger();

    public ScanShard(String pd) {
        super(pd);
    }

    @Override
    public void action(String[] params) {
        ExecutorService service = new ThreadPoolExecutor(500, Integer.MAX_VALUE,
                                                         0L,
                                                         TimeUnit.MILLISECONDS,
                                                         new LinkedBlockingQueue<>());
        long start = System.currentTimeMillis();
        if (params == null || params.length < 2) {
            log.info("Wrong number of parameters");
            return;
        }
        String[] addresses = params[1].split(",");
        int pSize = 72;
        int size = pSize * addresses.length;
        CountDownLatch latch = new CountDownLatch(size);
        for (int j = 0; j < pSize; j++) {
            for (int i = 0; i < addresses.length; i++) {
                String address = addresses[i];
                int finalJ = j;
                service.execute(() -> getData(finalJ, latch, address));
            }
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        long cost = end - start;
        log.info("all rows are: {}, cost: {},avg: {}", sum.get(),
                 cost, sum.get() / cost * 1000);
        service.shutdown();
    }
}
