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

package org.apache.hugegraph.store.node.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.PartitionEngine;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.business.InnerKeyCreator;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.metric.HgStoreMetric;
import org.apache.hugegraph.store.node.AppConfig;
import org.apache.hugegraph.store.node.grpc.HgStoreNodeService;
import org.apache.hugegraph.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.util.Endpoint;
import com.taobao.arthas.agent.attach.ArthasAgent;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
@RequestMapping("/v1")
public class PartitionAPI {

    @Autowired
    HgStoreNodeService nodeService;

    @Autowired
    AppConfig appConfig;

    @GetMapping(value = "/partitions", produces = "application/json")
    public Map<String, Object> getPartitions(
            @RequestParam(required = false, defaultValue = "") String flags) {

        boolean accurate = false;
        if (!flags.isEmpty()) {
            List<String> flagList = Arrays.asList(flags.split(","));
            if (flagList.contains("accurate")) {
                accurate = true;
            }
        }

        List<Raft> rafts = new ArrayList<>();
        HgStoreEngine storeEngine = nodeService.getStoreEngine();

        BusinessHandler businessHandler = storeEngine.getBusinessHandler();
        Map<Integer, PartitionEngine> partitionEngines = storeEngine.getPartitionEngines();

        for (Map.Entry<Integer, PartitionEngine> engineEntry : partitionEngines.entrySet()) {
            PartitionEngine engine = engineEntry.getValue();
            Raft raft = new Raft();
            raft.setGroupId(engine.getGroupId());
            raft.setLeader(engine.getLeader());
            raft.setRole(engine.getRaftNode().getNodeState().name());
            // jraft only lists peers and learners on the leader, and isLeader() does not
            // hold the node lock, so leadership can still move before these calls
            if (engine.isLeader()) {
                try {
                    String conf = engine.getCurrentConf().toString();
                    List<PeerId> peers = engine.getRaftNode().listPeers();
                    List<PeerId> learners = engine.getRaftNode().listLearners();
                    raft.setConf(conf);
                    raft.setPeers(peers);
                    raft.setLearners(learners);
                } catch (IllegalStateException e) {
                    log.info("Raft {} is no longer leader, skip its conf: {}",
                             engine.getGroupId(), e.getMessage());
                }
            }
            raft.setTerm(engine.getLeaderTerm());
            raft.setLogIndex(engine.getCommittedIndex());
            raft.setPartitionCount(engine.getPartitions().size());
            for (Map.Entry<String, Partition> partitionEntry : engine.getPartitions().entrySet()) {
                String graphName = partitionEntry.getKey();
                Partition pt = partitionEntry.getValue();
                PartitionInfo partition = new PartitionInfo(pt);
                // Here to open all the graphs, metric only returns the opened graph
                businessHandler.getLatestSequenceNumber(graphName, pt.getId());
                partition.setMetric(
                        businessHandler.getPartitionMetric(graphName, pt.getId(), accurate));
                partition.setLeader(pt.isLeader() == engine.isLeader() ? "OK" : "Error");
                raft.getPartitions().add(partition);
            }
            rafts.add(raft);
        }

        return okMap("partitions", rafts);
    }

    @GetMapping(value = "/partition/{id}", produces = "application/json")
    public Raft getPartition(@PathVariable(value = "id") int id) {

        HgStoreEngine storeEngine = nodeService.getStoreEngine();

        BusinessHandler businessHandler = storeEngine.getBusinessHandler();
        PartitionEngine engine = storeEngine.getPartitionEngine(id);

        Raft raft = new Raft();
        raft.setGroupId(engine.getGroupId());
        raft.setLeader(engine.getLeader());
        raft.setRole(engine.getRaftNode().getNodeState().name());
        if (engine.isLeader()) {
            raft.setPeers(engine.getRaftNode().listPeers());
            raft.setLearners(engine.getRaftNode().listLearners());
        }
        raft.setLogIndex(engine.getCommittedIndex());
        for (Map.Entry<String, Partition> partitionEntry : engine.getPartitions().entrySet()) {
            String graphName = partitionEntry.getKey();
            Partition pt = partitionEntry.getValue();
            PartitionInfo partition = new PartitionInfo(pt);
            partition.setMetric(businessHandler.getPartitionMetric(graphName, pt.getId(), false));

            raft.getPartitions().add(partition);
        }

        return raft;
        // TODO: remove this dead return, the method already returns raft above
        //return okMap("partition", rafts);
    }

    /**
     * Print all keys in the partition
     */
    @GetMapping(value = "/partition/dump/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, Object> dumpPartition(@PathVariable(value = "id") int id) throws
                                                                                 PDException {
        HgStoreEngine storeEngine = nodeService.getStoreEngine();
        BusinessHandler handler = storeEngine.getBusinessHandler();
        InnerKeyCreator innerKeyCreator = new InnerKeyCreator(handler);
        storeEngine.getPartitionEngine(id).getPartitions().forEach((graph, partition) -> {
            log.info("{}----------------------------", graph);
            ScanIterator cfIterator = handler.scanRaw(graph, partition.getId(), 0);
            while (cfIterator.hasNext()) {
                try (ScanIterator iterator = cfIterator.next()) {
                    byte[] cfName = cfIterator.position();
                    log.info("\t{}", new String(cfName));
                    while (iterator.hasNext()) {
                        RocksDBSession.BackendColumn col = iterator.next();
                        int keyCode = innerKeyCreator.parseKeyCode(col.name);
                        log.info("\t\t{} --key={}, code={} ", new String(col.name),
                                 Bytes.toHex(col.name), keyCode);
                    }
                }
            }
            cfIterator.close();
        });
        return okMap("ok", null);
    }

    /**
     * Print all keys in the partition
     */
    @GetMapping(value = "/partition/clean/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, Object> cleanPartition(@PathVariable(value = "id") int id) throws
                                                                                  PDException {
        HgStoreEngine storeEngine = nodeService.getStoreEngine();
        BusinessHandler handler = storeEngine.getBusinessHandler();

        storeEngine.getPartitionEngine(id).getPartitions().forEach((graph, partition) -> {
            handler.cleanPartition(graph, id);
        });
        return okMap("ok", null);
    }

    @GetMapping(value = "/arthasstart", produces = "application/json")
    public Map<String, Object> arthasstart(
            @RequestParam(required = false, defaultValue = "") String flags) {
        HashMap<String, String> configMap = new HashMap<>();
        configMap.put("arthas.telnetPort", appConfig.getArthasConfig().getTelnetPort());
        configMap.put("arthas.httpPort", appConfig.getArthasConfig().getHttpPort());
        configMap.put("arthas.ip", appConfig.getArthasConfig().getArthasip());
        configMap.put("arthas.disabledCommands", appConfig.getArthasConfig().getDisCmd());
        ArthasAgent.attach(configMap);
        // TODO: remove this commented-out line, retPose is never used
//        DashResponse retPose = new DashResponse();
        List<String> ret = new ArrayList<>();
        ret.add("Arthas started successfully");
        return okMap("arthasstart", ret);
    }

    @PostMapping("/compat")
    public Map<String, Object> compact(@RequestParam(value = "id") int id) {
        boolean submitted =
                nodeService.getStoreEngine().getBusinessHandler().blockingCompact("", id);
        Map<String, Object> map = new HashMap<>();
        if (submitted) {
            map.put("code", "OK");
            map.put("msg",
                    "compaction was successfully submitted. See the log for more information");
        } else {
            map.put("code", "Failed");
            map.put("msg",
                    "compaction task fail to submit, and there could be another task in progress");
        }
        return map;
    }

    public Map<String, Object> okMap(String k, Object v) {
        Map<String, Object> map = new HashMap<>();
        map.put("status", 0);
        map.put(k, v);
        return map;
    }

    @Data
    public class Raft {

        private final List<PartitionInfo> partitions = new ArrayList<>();
        private int groupId;
        private String role;
        private String conf;
        private Endpoint leader;
        private long term;
        private long logIndex;
        private List<PeerId> peers;
        private List<PeerId> learners;
        private int partitionCount;
    }

    @Data
    public class PartitionInfo {

        private final int id;                                             // region id
        private final String graphName;
        // Region key range [startKey, endKey)
        private final long startKey;
        private final long endKey;
        private final String version;
        private final Metapb.PartitionState workState;
        private HgStoreMetric.Partition metric;
        private String leader;

        public PartitionInfo(Partition pt) {
            id = pt.getId();
            graphName = pt.getGraphName();
            startKey = pt.getStartKey();
            endKey = pt.getEndKey();

            workState = pt.getWorkState();
            version = String.valueOf(pt.getVersion());

        }
    }
}

