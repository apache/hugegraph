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

package org.apache.hugegraph.pd.client;

import org.apache.hugegraph.pd.common.Useless;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatRequest;
import org.apache.hugegraph.pd.pulse.PulseServerNotice;
import org.junit.BeforeClass;
import org.junit.Test;
// import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Useless("used for development")
public class PDPulseTest {

    private static PDClient pdClient;

    private static PDConfig pdConfig;

    private long storeId = 0;
    private String storeAddress = "localhost";
    private String graphName = "graph1";

    private static final String SERVICE_NAME = "store";
    private static final String AUTHORITY = "";

    @BeforeClass
    public static void beforeClass() throws Exception {
        pdConfig = PDConfig.of("localhost:8686").setAuthority(SERVICE_NAME, AUTHORITY);
//        pdConfig.setEnableCache(true);
//        pdClient = PDClient.create(pdConfig);
//        pdClient.getLeader();

        pdClient = PDClient.create(pdConfig);
    }

    @Test
    public void listen() {

        PDPulse pulse = pdClient.getPulse();
        CountDownLatch latch = new CountDownLatch(100);

        PDPulse.Notifier<PartitionHeartbeatRequest.Builder> notifier1 =
                pulse.connectPartition(new PulseListener(latch, "test-listener"));
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
            pdClient.forceReconnect();
        }, 1, 2, TimeUnit.SECONDS);

        try {
            latch.await(12000, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        PartitionHeartbeatRequest.Builder builder = PartitionHeartbeatRequest.newBuilder();

        notifier1.notifyServer(builder);
        notifier1.close();

    }

    //@Test
    public void notifyServer() {
        CountDownLatch latch = new CountDownLatch(100);
        PDPulse pulse = pdClient.getPulse();
        PDPulse.Notifier<PartitionHeartbeatRequest.Builder> notifier =
                pulse.connectPartition(new PulseListener<>(latch, "test-listener"));
        for (int i = 0; i < 100; i++) {
            HgPDTestUtil.println("Notifying server [" + i + "] times.");
            notifier.notifyServer(PartitionHeartbeatRequest.newBuilder().setStates(
                    Metapb.PartitionStats.newBuilder().setId(i)
            ));
        }

    }

    private class PulseListener<T> implements PDPulse.Listener<T> {

        CountDownLatch latch = new CountDownLatch(10);
        private String listenerName;

        private PulseListener(CountDownLatch latch, String listenerName) {
            this.latch = latch;
            this.listenerName = listenerName;
        }

        @Override
        public void onNext(T response) {
            // println(this.listenerName+" res: "+response);
            // this.latch.countDown();
        }

        @Override
        public void onNotice(PulseServerNotice<T> notice) {
            //println("=> " + this.listenerName + " noticeId: " + notice.getNoticeId());
            notice.ack();
            //println("  => " + this.listenerName + " ack: " + notice.getNoticeId());
            this.latch.countDown();
        }

        @Override
        public void onError(Throwable throwable) {
            HgPDTestUtil.println(this.listenerName + " error: " + throwable.toString());
        }

        @Override
        public void onCompleted() {
            HgPDTestUtil.println(this.listenerName + " is completed");
        }
    }
}
