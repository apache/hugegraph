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

package org.apache.hugegraph.pd.rest;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.pd.RegistryService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.apache.hugegraph.pd.grpc.pulse.ChangeShard;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatResponse;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.apache.hugegraph.pd.meta.QueueStore;
import org.apache.hugegraph.pd.pulse.PDPulseSubject;
import org.apache.hugegraph.pd.watch.ChangeType;
import org.apache.hugegraph.pd.watch.PDWatchSubject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
@RequestMapping("/test")
public class TestAPI {

    @Autowired
    private PDConfig pdConfig;

    @GetMapping(value = "/discovery/{appName}", produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String discovery(@PathVariable(value = "appName", required = true) String appName) {
        RegistryService register = new RegistryService(pdConfig);
        // Query query=Query.newBuilder().setAppName("hugegraph").build();
        AtomicLong label = new AtomicLong();
        HashMap<String, String> labels = new HashMap<>();
        String labelValue = String.valueOf(label.incrementAndGet());
        //labels.put("address",labelValue);
        Query query = Query.newBuilder().build();
        // Query query = Query.newBuilder().setAppName("hugegraph").set.build();

        return register.getNodes(query).toString();
    }

    @GetMapping(value = "/pulse", produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String notifyClient() {
        PDPulseSubject.notifyClient(
                PartitionHeartbeatResponse.newBuilder()
                                          .setPartition(Metapb.Partition.newBuilder()
                                                                        .setId(8)
                                                                        .setGraphName("graphName8"))
                                          .setChangeShard(
                                                  ChangeShard.newBuilder()
                                                             .setChangeTypeValue(8)
                                                             .addShard(Metapb.Shard.newBuilder()
                                                                                   .setRoleValue(8)
                                                                                   .setStoreId(8)
                                                             )
                                          )

        );
        return "partition";
    }

    @GetMapping(value = "/partition", produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String noticePartition() {
        PDWatchSubject.notifyPartitionChange(ChangeType.ALTER, "graph-test", 99);
        return "partition";
    }

    @PutMapping(value = "/queue", produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    public String testPutQueue() {
        this.putQueue();
        return "queue";
    }

    public void putQueue() {
        PartitionHeartbeatResponse response = PartitionHeartbeatResponse.newBuilder()
                                                                        .setPartition(
                                                                                Metapb.Partition.newBuilder()
                                                                                                .setId(9)
                                                                                                .setGraphName(
                                                                                                        "graphName"))
                                                                        .setChangeShard(
                                                                                ChangeShard.newBuilder()
                                                                                           .setChangeTypeValue(
                                                                                                   9)
                                                                                           .addShard(
                                                                                                   Metapb.Shard.newBuilder()
                                                                                                               .setRoleValue(
                                                                                                                       9)
                                                                                                               .setStoreId(
                                                                                                                       9)
                                                                                           )
                                                                        ).build();

        Metapb.QueueItem.Builder builder = Metapb.QueueItem.newBuilder()
                                                           .setItemId("item-id")
                                                           .setItemClass("item-class")
                                                           .setItemContent(response.toByteString());

        QueueStore store = MetadataFactory.newQueueStore(pdConfig);

        try {
            store.addItem(builder.setItemId("item-id-1").build());
            store.addItem(builder.setItemId("item-id-2").build());
            store.addItem(builder.setItemId("item-id-3").build());
        } catch (PDException e) {
            e.printStackTrace();
        }
        List<Metapb.QueueItem> queue = null;
        try {
            queue = store.getQueue();
        } catch (PDException e) {
            e.printStackTrace();
        }
        Parser<PartitionHeartbeatResponse> parser = PartitionHeartbeatResponse.parser();

        queue.stream().forEach(e -> {
            PartitionHeartbeatResponse buf = null;
            try {
                buf = parser.parseFrom(e.getItemContent());
            } catch (InvalidProtocolBufferException ex) {
                ex.printStackTrace();
            }
            PDPulseSubject.notifyClient(PartitionHeartbeatResponse.newBuilder(buf));
        });

    }
}
