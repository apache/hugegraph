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
import java.util.List;

import org.apache.hugegraph.store.PartitionEngine;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.meta.Store;
import org.apache.hugegraph.store.node.grpc.HgStoreNodeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.alipay.sofa.jraft.entity.PeerId;

import lombok.extern.slf4j.Slf4j;

/**
 * For testing only
 */
@RestController
@Slf4j
@RequestMapping("/test")
public class HgTestController {

    @Autowired
    HgStoreNodeService nodeService;

    @GetMapping(value = "/leaderStore", produces = MediaType.APPLICATION_JSON_VALUE)
    public Store testGetStoreInfo() {

        Store store = null;
        PartitionEngine engine = nodeService.getStoreEngine().getPartitionEngine(0);

        for (Partition partition : engine.getPartitions().values()) {
            store = nodeService.getStoreEngine().getHgCmdClient()
                               .getStoreInfo(engine.getLeader().toString());
        }
        return store;
    }

    @GetMapping(value = "/raftRestart/{groupId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String restartRaftNode(@PathVariable(value = "groupId") int groupId) {
        PartitionEngine engine = nodeService.getStoreEngine().getPartitionEngine(groupId);
        if (engine != null) {
            engine.restartRaftNode();
            return "OK";
        } else {
            return "partition engine not found";
        }
    }

    @GetMapping(value = "/raftDelete/{groupId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String deleteRaftNode(@PathVariable(value = "groupId") int groupId) {
        List<String> graphs = new ArrayList<>();
        PartitionEngine engine = nodeService.getStoreEngine().getPartitionEngine(groupId);
        if (engine != null) {
            engine.getPartitions().forEach((k, v) -> {
                graphs.add(v.getGraphName());
            });
            nodeService.getStoreEngine().destroyPartitionEngine(groupId, graphs);
            return "OK";
        } else {
            return "Partition not found";
        }

    }

    @GetMapping(value = "/gc", produces = MediaType.APPLICATION_JSON_VALUE)
    public String doGc() {
        System.gc();
        return "gc OK!";
    }

    @GetMapping(value = "/flush", produces = MediaType.APPLICATION_JSON_VALUE)
    public String doFlush() {
        nodeService.getStoreEngine().getBusinessHandler().flushAll();
        return "flush all!";
    }

    @GetMapping(value = "/close", produces = MediaType.APPLICATION_JSON_VALUE)
    public String doCloseAll() {
        nodeService.getStoreEngine().getBusinessHandler().closeAll();
        return "close all!";
    }

    @GetMapping(value = "/snapshot", produces = MediaType.APPLICATION_JSON_VALUE)
    public String doSnapshot() {
        nodeService.getStoreEngine().getPartitionEngines().forEach((k, v) -> {
            v.snapshot();
        });
        return "snapshot OK!";
    }

    @GetMapping(value = "/compact", produces = MediaType.APPLICATION_JSON_VALUE)
    public String dbCompaction() {
        nodeService.getStoreEngine().getPartitionEngines().forEach((k, v) -> {
            nodeService.getStoreEngine().getBusinessHandler().dbCompaction("", k);
        });
        return "snapshot OK!";
    }

    @GetMapping(value = "/pulse/reset", produces = MediaType.APPLICATION_JSON_VALUE)
    public String resetPulse() {
        try {
            nodeService.getStoreEngine().getHeartbeatService().connectNewPulse();
            return "OK";
        } catch (Exception e) {
            log.error("pulse reset error: ", e);
            return e.getMessage();
        }
    }

    @GetMapping(value = "/transferLeaders", produces = MediaType.APPLICATION_JSON_VALUE)
    public String transferLeaders() {
        try {
            nodeService.getStoreEngine().getLeaderPartition().forEach(engine -> {
                try {
                    engine.getRaftNode().transferLeadershipTo(PeerId.ANY_PEER);
                } catch (Exception e) {
                    log.error("transfer leader error: ", e);
                }
            });
            return "OK";
        } catch (Exception e) {
            log.error("transfer leaders error: ", e);
            return e.getMessage();
        }
    }

    @GetMapping(value = "/restart_raft", produces = MediaType.APPLICATION_JSON_VALUE)
    public String restartRaft() {
        try {
            nodeService.getStoreEngine().getPartitionEngines().values()
                       .forEach(PartitionEngine::restartRaftNode);
            return "OK";
        } catch (Exception e) {
            log.error("restart raft error: ", e);
            return e.getMessage();
        }
    }

    @GetMapping(value = "/all_raft_start", produces = MediaType.APPLICATION_JSON_VALUE)
    public String isRaftAllStarted() {
        try {
            var engine = nodeService.getStoreEngine();
            var storeId = engine.getPartitionManager().getStore().getId();
            var flag = nodeService.getStoreEngine().getPdProvider().getPartitionsByStore(storeId)
                                  .stream()
                                  .mapToInt(Partition::getId)
                                  .allMatch(i -> engine.getPartitionEngine(i) != null);
            return flag ? "OK" : "NO";
        } catch (Exception e) {
            log.error("raft status check error: ", e);
            return e.getMessage();
        }
    }

}
