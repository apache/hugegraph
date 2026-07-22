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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.model.RestApiResponse;
import org.apache.hugegraph.pd.model.StoreRestRequest;
import org.apache.hugegraph.pd.model.TimeRangeRequest;
import org.apache.hugegraph.pd.service.PDRestService;
import org.apache.hugegraph.pd.util.DateUtil;
import org.apache.hugegraph.pd.util.StoreRestAddressUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.protobuf.util.JsonFormat;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
@RequestMapping("/v1")
public class StoreAPI extends API {

    @Autowired
    PDRestService pdRestService;

    @GetMapping(value = "/stores", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RestApiResponse getStores() {
        List<StoreStatistics> storeStatsList = new ArrayList<>();
        try {
            HashMap<String, Object> dataMap = new HashMap<>();
            Map<String, Integer> stateCountMap = new HashMap<>();
            for (Metapb.Store store : pdRestService.getStores("")) {
                String stateKey = store.getState().name();
                stateCountMap.put(stateKey, stateCountMap.getOrDefault(stateKey, 0) + 1);
                storeStatsList.add(new StoreStatistics(store));
            }
            storeStatsList.sort((o1, o2) -> o1.address.compareTo(o2.address));
            dataMap.put("stores", storeStatsList);
            dataMap.put("numOfService", storeStatsList.size());
            dataMap.put("numOfNormalService",
                        stateCountMap.getOrDefault(Metapb.StoreState.Up.name(), 0));
            dataMap.put("stateCountMap", stateCountMap);
            return new RestApiResponse(dataMap, Pdpb.ErrorType.OK, Pdpb.ErrorType.OK.name());
        } catch (PDException e) {
            log.error("PDException", e);
            return new RestApiResponse(null, e.getErrorCode(), e.getMessage());
        }
    }

    // Only storeState can be modified through this API
    @PostMapping(value = "/store/{storeId}", consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String setStore(@PathVariable long storeId, @RequestBody StoreRestRequest request) {
        try {
            Metapb.Store lastStore = pdRestService.getStore(storeId);
            if (lastStore != null) {
                Metapb.Store.Builder builder = Metapb.Store.newBuilder(lastStore);
                Metapb.StoreState storeState = Metapb.StoreState.valueOf(request.getStoreState());
                builder.setState(storeState);
                Metapb.Store newStore = pdRestService.updateStore(builder.build());
                return toJSON(newStore, "store");
            } else {
                return toJSON(new PDException(Pdpb.ErrorType.NOT_FOUND_VALUE, "error"));
            }
        } catch (PDException e) {
            return toJSON(e);
        }
    }

    @GetMapping(value = "/shardGroups", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String getShardGroups() {
        try {
            return toJSON(pdRestService.getShardGroups(), "shardGroups");
        } catch (PDException e) {
            return toJSON(e);
        }
    }

    /**
     * Returns the leader on each store
     *
     * @return
     */
    @GetMapping(value = "/shardLeaders")
    public Map<String, List<Integer>> shardLeaders() throws PDException {
        Map<String, List<Integer>> leaders = new HashMap<>();
        try {

            List<Metapb.ShardGroup> groups = pdRestService.getShardGroups();
            groups.forEach(group -> {
                group.getShardsList().forEach(shard -> {
                    if (shard.getRole() == Metapb.ShardRole.Leader) {
                        try {
                            String ip = pdRestService.getStore(shard.getStoreId()).getRaftAddress();
                            if (!leaders.containsKey(ip)) {
                                leaders.put(ip, new ArrayList<>());
                            }
                            leaders.get(ip).add(group.getId());
                        } catch (PDException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
            });
        } catch (PDException e) {
            throw e;
        }
        return leaders;
    }

    @GetMapping(value = "/balanceLeaders")
    public Map<Integer, Long> balanceLeaders() throws PDException {
        return pdRestService.balancePartitionLeader();
    }

    @DeleteMapping(value = "/store/{storeId}")
    public String removeStore(@PathVariable(value = "storeId") Long storeId) {
        try {
            pdRestService.removeStore(storeId);
        } catch (PDException e) {
            return e.getStackTrace().toString();
        }
        return "OK";
    }

    @PostMapping(value = "/store/log", consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String getStoreLog(@RequestBody TimeRangeRequest request) {
        try {
            Date dateStart = DateUtil.getDate(request.getStartTime());
            Date dateEnd = DateUtil.getDate(request.getEndTime());
            List<Metapb.LogRecord> changedStore =
                    pdRestService.getStoreStatusLog(dateStart.getTime(),
                                                    dateEnd.getTime());
            if (changedStore != null) {
                JsonFormat.TypeRegistry registry = JsonFormat.TypeRegistry
                        .newBuilder().add(Metapb.Store.getDescriptor()).build();
                return toJSON(changedStore, registry);
            } else {
                return toJSON(new PDException(Pdpb.ErrorType.NOT_FOUND_VALUE, "error"));
            }
        } catch (PDException e) {
            return toJSON(e);
        }
    }

    @GetMapping(value = "store/{storeId}", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RestApiResponse getStore(@PathVariable long storeId) {
        // Get the statistics of the store
        Metapb.Store store = null;
        try {
            store = pdRestService.getStore(storeId);
        } catch (PDException e) {
            return new RestApiResponse(null, e.getErrorCode(), e.getMessage());
        }
        if (store != null) {
            StoreStatistics resultStoreStats = resultStoreStats = new StoreStatistics(store);
            return new RestApiResponse(resultStoreStats, Pdpb.ErrorType.OK,
                                       Pdpb.ErrorType.OK.name());
        } else {
            return new RestApiResponse(null, Pdpb.ErrorType.STORE_ID_NOT_EXIST,
                                       Pdpb.ErrorType.STORE_ID_NOT_EXIST.name());
        }
    }

    @GetMapping(value = "storesAndStats", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String getStoresAndStats() {
        //for debug use
        try {
            List<Metapb.Store> stores = pdRestService.getStores("");
            return toJSON(stores, "stores");
        } catch (PDException e) {
            log.error("PD exception:" + e);
            return toJSON(e);
        }
    }

    @GetMapping(value = "store_monitor/json/{storeId}", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RestApiResponse getStoreMonitorData(@PathVariable long storeId) {
        try {
            List<Map<String, Long>> result = pdRestService.getMonitorData(storeId);
            return new RestApiResponse(result, Pdpb.ErrorType.OK, Pdpb.ErrorType.OK.name());
        } catch (PDException e) {
            return new RestApiResponse(null, e.getErrorCode(), e.getMessage());
        }
    }

    @GetMapping(value = "store_monitor/{storeId}")
    @ResponseBody
    public String getStoreMonitorDataText(@PathVariable long storeId) {
        try {
            return pdRestService.getMonitorDataText(storeId);
        } catch (PDException e) {
            return "error:" + e.getErrorCode() + e.getMessage();
        }
    }

    /**
     * Retrieve shard group cache information
     * This interface obtains shard group cache information via a GET request and returns a
     * JSON-formatted string
     *
     * @return JSON string containing shard group cache information
     */
    @GetMapping(value = "/shardGroupsCache", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String getShardGroupsCache() {
        return toJSON(new ArrayList<>(pdRestService.getShardGroupCache().values()), "shardGroups");
    }

    @Data
    class Partition {

        int partitionId;
        String graphName;
        String role; // shard role
        String workState;
        long dataSize; // The amount of storage space occupied

        Partition() {
        }

        Partition(Metapb.GraphStats graphStats) {
            partitionId = graphStats.getPartitionId();
            graphName = graphStats.getGraphName();
            final int postfixLength = 2;
            graphName = graphName.substring(0, graphName.length() - postfixLength);
            role = String.valueOf(graphStats.getRole());
            workState = String.valueOf(graphStats.getWorkState());
            dataSize = graphStats.getApproximateSize();
        }
    }

    @Data
    class StoreStatistics {

        // store statistics
        String storeId;
        String address;
        String restAddress;
        String raftAddress;
        String version;
        String state;
        String deployPath;
        String dataPath; // The path where the data is stored
        long startTimeStamp;
        // For the time being, the time of the first heartbeat is taken as the registration time
        long registedTimeStamp;
        long lastHeartBeat; // Last heartbeat time
        long capacity;
        long available;
        int partitionCount;
        int graphSize;
        long keyCount;
        long leaderCount; // shard role = 'Leader' The number of partitions
        String serviceName;
        String serviceVersion;
        long serviceCreatedTimeStamp; // The time when the service was created
        List<Partition> partitions;

        StoreStatistics(Metapb.Store store) {
            if (store != null) {
                storeId = String.valueOf(store.getId());
                address = store.getAddress();
                restAddress = StoreRestAddressUtil.getRestAddress(store);
                raftAddress = store.getRaftAddress();
                state = String.valueOf(store.getState());
                version = store.getVersion();
                deployPath = store.getDeployPath();
                final String prefix = "file:";
                if ((deployPath != null) && (deployPath.startsWith(prefix))) {
                    // Remove the prefix
                    deployPath = deployPath.substring(prefix.length());
                }
                if ((deployPath != null) && (deployPath.contains(".jar"))) {
                    // Remove the information after the jar package
                    deployPath = deployPath.substring(0, deployPath.indexOf(".jar") + 4);
                }
                dataPath = store.getDataPath();
                startTimeStamp = store.getStartTimestamp();
                try {
                    serviceCreatedTimeStamp = pdRestService.getStore(store.getId())
                                                           .getStats()
                                                           .getStartTime(); // Instance time
                    final int base = 1000;
                    serviceCreatedTimeStamp *= base; // Translates to milliseconds
                } catch (PDException e) {
                    e.printStackTrace();
                    serviceCreatedTimeStamp = store.getStartTimestamp();
                }
                registedTimeStamp = store.getStartTimestamp(); // Time of registration
                lastHeartBeat = store.getLastHeartbeat();
                capacity = store.getStats().getCapacity();
                available = store.getStats().getAvailable();
                partitionCount = store.getStats().getPartitionCount();
                serviceName = address + "-store";
                serviceVersion = store.getVersion();
                List<Metapb.GraphStats> graphStatsList = store.getStats().getGraphStatsList();
                // Save the partition information
                List<Partition> partitionStatsList = new ArrayList<>();
                // The number used for the chart
                HashSet<String> graphNameSet = new HashSet<>();
                // Statistics on the number of partitions of the leader
                HashSet<Integer> leaderPartitionIds = new HashSet<Integer>();
                // Construct partition information (graph information stored in the store)
                Map<Integer, Long> partition2KeyCount = new HashMap<>();
                for (Metapb.GraphStats graphStats : graphStatsList) {
                    String graphName = graphStats.getGraphName();
                    // Only the part in front of /g /m /s is retained in the title
                    final int postfixLength = 2;
                    graphNameSet.add(graphName.substring(0, graphName.length() - postfixLength));
                    if ((graphStats.getGraphName() != null) &&
                        (graphStats.getGraphName().endsWith("/g"))) {
                        Partition pt = new Partition(graphStats);
                        partitionStatsList.add(pt);
                    }
                    // Count the keyCount of each partition
                    partition2KeyCount.put(graphStats.getPartitionId(),
                                           graphStats.getApproximateKeys());
                    if (graphStats.getRole() == Metapb.ShardRole.Leader) {
                        leaderPartitionIds.add(graphStats.getPartitionId());
                    }
                }
                for (Map.Entry<Integer, Long> entry : partition2KeyCount.entrySet()) {
                    keyCount += entry.getValue();
                }
                partitions = partitionStatsList;
                graphSize = graphNameSet.size();
                leaderCount = leaderPartitionIds.size();
            }

        }
    }

    /**
     * Check Service Health Status
     * This interface is used to check the health status of the service by accessing the /health
     * path via a GET request.
     *
     * @return Returns a string indicating the service's health status. Typically, an empty
     * string indicates the service is healthy.
     */
    @GetMapping(value = "/health", produces = MediaType.TEXT_PLAIN_VALUE)
    public Serializable checkHealthy() {
        return "";
    }
}
