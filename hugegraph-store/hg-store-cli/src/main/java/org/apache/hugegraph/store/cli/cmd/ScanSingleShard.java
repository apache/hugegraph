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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.pd.cli.cmd.Command;

import lombok.extern.slf4j.Slf4j;

/**
 * @date 2023/10/18
 **/
@Slf4j
public class ScanSingleShard extends Command implements Scan {

    //private final boolean closed = false;
    private final AtomicInteger sum = new AtomicInteger();
    //private final ConcurrentHashMap<Long, StreamObserver<ScanPartitionRequest>>
    //        observers = new ConcurrentHashMap<>();

    public ScanSingleShard(String pd) {
        super(pd);
    }

    @Override
    public void action(String[] params) {
        CountDownLatch latch = new CountDownLatch(1);
        if (params == null || params.length < 2) {
            log.error("Missing required parameters: partitionId and address");
            return;
        }
        int partitionId = Integer.parseInt(params[0]);
        String address = params[1];
        new Thread(() -> getData(partitionId, latch, address)).start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        log.info("all rows are: {}", sum.get());
    }
}
