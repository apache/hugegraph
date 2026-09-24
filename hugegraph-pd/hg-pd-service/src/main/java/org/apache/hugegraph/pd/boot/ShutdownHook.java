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


package org.apache.hugegraph.pd.boot;

import org.apache.hugegraph.pd.service.MetadataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadPoolExecutor;

public class ShutdownHook extends Thread {

    private static Logger log = LoggerFactory.getLogger(ShutdownHook.class);
    private static String msg = "there are still uninterruptible jobs that have not been completed and" +
                                " will wait for them to complete";
    private Thread main;

    public ShutdownHook(Thread main) {
        super();
        this.main = main;
    }

    @Override
    public void run() {
        log.info("shutdown signal received");
        main.interrupt();
        waitForShutdown();
        try {
            main.join();
        } catch (InterruptedException e) {
        }
        log.info("shutdown completed");
    }

    private void waitForShutdown() {
        checkUninterruptibleJobs();
    }

    private void checkUninterruptibleJobs() {
        ThreadPoolExecutor jobs = MetadataService.getUninterruptibleJobs();
        try {
            if (jobs != null) {
                long lastPrint = System.currentTimeMillis() - 5000;
                log.info("check for ongoing background jobs that cannot be interrupted, active:{}, queue:{}.",
                         jobs.getActiveCount(), jobs.getQueue().size());
                while (jobs.getActiveCount() != 0 || jobs.getQueue().size() != 0) {
                    synchronized (ShutdownHook.class) {
                        if (System.currentTimeMillis() - lastPrint > 5000) {
                            log.warn(msg);
                            lastPrint = System.currentTimeMillis();
                        }
                        try {
                            ShutdownHook.class.wait(200);
                        } catch (InterruptedException e) {
                            log.error("close jobs with error:", e);
                        }
                    }
                }
                log.info("all ongoing background jobs have been completed and the shutdown will continue");
            }

        } catch (Exception e) {
            log.error("close jobs with error:", e);
        }
        try {
            if (jobs != null) {
                jobs.shutdownNow();
            }
        } catch (Exception e) {
            log.error("close jobs with error:", e);
        }
    }
}
