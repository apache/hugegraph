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

package org.apache.hugegraph.store.node.listener;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.node.grpc.HgStoreStreamImpl;
import org.apache.hugegraph.store.node.task.TTLCleaner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.stereotype.Service;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ContextClosedListener implements ApplicationListener<ContextClosedEvent> {

    @Autowired
    HgStoreStreamImpl storeStream;
    @Autowired
    TTLCleaner cleaner;

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        try {
            try {
                transferLeaders();

                synchronized (ContextClosedListener.class) {
                    ContextClosedListener.class.wait(60 * 1000);
                }

                transferLeaders();

                synchronized (ContextClosedListener.class) {
                    ContextClosedListener.class.wait(30 * 1000);
                }
            } catch (Exception e) {
                log.info("shutdown hook: ", e);
            }

            log.info("closing scan threads....");
            if (storeStream != null) {
                ThreadPoolExecutor executor = storeStream.getRealExecutor();
                if (executor != null) {
                    try {
                        executor.shutdownNow();
                    } catch (Exception e) {
                    }
                }
            }

            if (cleaner != null) {
                ThreadPoolExecutor cleanerExecutor = cleaner.getExecutor();
                if (cleanerExecutor != null) {
                    try {
                        cleanerExecutor.shutdownNow();
                    } catch (Exception e) {

                    }
                }
                ScheduledExecutorService scheduler = cleaner.getScheduler();
                if (scheduler != null) {
                    try {
                        scheduler.shutdownNow();
                    } catch (Exception e) {

                    }
                }
            }
        } catch (Exception e) {
            log.error("ContextClosedListener: ", e);
        } finally {
            log.info("closed scan threads");
        }
    }

    private void transferLeaders() {
        try {
            HgStoreEngine.getInstance().getLeaderPartition()
                         .forEach(leader -> {
                             try {
                                 Status status =
                                         leader.getRaftNode().transferLeadershipTo(PeerId.ANY_PEER);
                                 log.info("partition {} transfer leader status: {}",
                                          leader.getGroupId(), status);
                             } catch (Exception e) {
                                 log.info("partition {} transfer leader error: ",
                                          leader.getGroupId(), e);
                             }
                         });
            HgStoreEngine.getInstance().getPartitionEngines().forEach(
                    ((integer, partitionEngine) -> partitionEngine.getRaftNode()
                                                                  .shutdown())
            );
        } catch (Exception e) {
            log.error("transfer leader failed: " + e.getMessage());
        }
    }
}
