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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.model.RestApiResponse;
import org.apache.hugegraph.pd.model.TimeRangeRequest;
import org.apache.hugegraph.pd.service.PDRestService;
import org.apache.hugegraph.pd.util.DateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
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
public class PartitionAPI extends API {

    public static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    @Autowired
    PDRestService pdRestService;



    /**
     * Get advanced partition information
     * <p>
     * This interface is used to obtain advanced partition information in the system, including graph information, key-value count, data size, etc. for each partition.
     *
     * @return RestApiResponse object containing advanced partition information
     */
    @GetMapping(value = "/highLevelPartitions", produces = MediaType.APPLICATION_JSON_VALUE)
    public RestApiResponse getHighLevelPartitions() {
        // Information about multiple graphs under the partition
        Map<Integer, Map<String, GraphStats>> partitions2GraphsMap = new HashMap<>();
        Map<Integer, HighLevelPartition> resultPartitionsMap = new HashMap<>();
        // The keyCount of each partition is only taken from the leader
        Map<Integer, Long> partition2KeyCount = new HashMap<>();
        // The dataSize of each partition is only taken from the leader
        Map<Integer, Long> partition2DataSize = new HashMap<>();
        List<Metapb.Store> stores;
        Map<Long, Metapb.Store> storesMap = new HashMap<>();
        try {
            stores = pdRestService.getStores("");
        } catch (PDException e) {
            log.error("getStores error", e);
            return new RestApiResponse(null, e.getErrorCode(), e.getMessage());
        }
        for (Metapb.Store store : stores) {
            storesMap.put(store.getId(), store);
            List<Metapb.GraphStats> graphStatsList = store.getStats().getGraphStatsList();
            for (Metapb.GraphStats graphStats : graphStatsList) {
                // Obtaining Graph Information Saved by a Partition (Only from the Leader)
                if (Metapb.ShardRole.Leader != graphStats.getRole()) {
                    continue;
                }
                // Calculating the key count of partitions (indiscriminate graphs)
                partition2KeyCount.put(graphStats.getPartitionId(),
                                       partition2KeyCount.getOrDefault(graphStats.getPartitionId(),
                                                                       graphStats.getApproximateKeys()));
                // The dataSize of the partition is calculated by adding the size of the graph
                partition2DataSize.put(graphStats.getPartitionId(),
                                       partition2DataSize.getOrDefault(graphStats.getPartitionId(),
                                                                       0L)
                                       + graphStats.getApproximateSize());
                // Graph information under the structure partition
                if (partitions2GraphsMap.get(graphStats.getPartitionId()) == null) {
                    partitions2GraphsMap.put(graphStats.getPartitionId(),
                                             new HashMap<String, GraphStats>());
                }
                Map<String, GraphStats> partitionGraphsMap =
                        partitions2GraphsMap.get(graphStats.getPartitionId());
                partitionGraphsMap.put(graphStats.getGraphName(), new GraphStats(graphStats));
            }
        }
        // Construct all the information that needs to be returned for the partition
        List<Metapb.Partition> partitionList = pdRestService.getPartitions("");
        for (Metapb.Partition partition : partitionList) {
            // Supplement the startKey and endKey of the partition image
            if (partitions2GraphsMap.get(partition.getId()) != null) {
                GraphStats graphStats =
                        partitions2GraphsMap.get(partition.getId()).get(partition.getGraphName());
                if (graphStats != null) {
                    graphStats.startKey = partition.getStartKey();
                    graphStats.endKey = partition.getEndKey();
                }
            }
            // Construct the overall information of the partition (regardless of the diagram)
            if ((resultPartitionsMap.get(partition.getId()) == null)
                && (!partition.getGraphName().endsWith("/s"))
            ) {
                Metapb.PartitionStats partitionStats;
                try {
                    partitionStats = pdRestService.getPartitionStats(partition.getGraphName(),
                                                                     partition.getId());
                } catch (PDException e) {
                    log.error("getPartitionStats error", e);
                    partitionStats = null;
                }
                // Initialize the partition information
                HighLevelPartition resultPartition =
                        new HighLevelPartition(partition, partitionStats);
                resultPartition.keyCount =
                        partition2KeyCount.getOrDefault(resultPartition.partitionId, 0L);
                resultPartition.dataSize =
                        partition2DataSize.getOrDefault(resultPartition.partitionId, 0L);
                for (ShardStats shard : resultPartition.shards) {
                    // Assign values to the address and partition information of the replica
                    Metapb.Store s = storesMap.get(shard.storeId);
                    shard.address = (s != null) ? s.getAddress() : "";
                    if (s == null) {
                        log.error("store not found for shard storeId={}, partitionId={}",
                                 shard.storeId, partition.getId());
                    }
                }
                resultPartitionsMap.put(partition.getId(), resultPartition);
            }
        }
        // Construct a list of graphs under the partitions to be returned, return only /g, and
        // sort by name
        for (Map.Entry<Integer, HighLevelPartition> entry : resultPartitionsMap.entrySet()) {
            Integer partitionId = entry.getKey();
            HighLevelPartition currentPartition = resultPartitionsMap.get(partitionId);
            Map<String, GraphStats> graphsMap = partitions2GraphsMap
                    .getOrDefault(partitionId,
                                  new HashMap<>()); // Avoid null pointer exceptions at the back
            ArrayList<GraphStats> graphsList = new ArrayList<>();
            for (Map.Entry<String, GraphStats> entry1 : graphsMap.entrySet()) {
                if (!entry1.getKey().endsWith("/g")) {
                    continue; // Only the graph of /g is kept
                }
                String graphName = entry1.getKey();
                GraphStats tmpGraph = graphsMap.get(graphName);
                final int postfixLength = 2;
                tmpGraph.graphName = tmpGraph.graphName.substring(0, tmpGraph.graphName.length() -
                                                                     postfixLength);
                graphsList.add(tmpGraph);
            }
            graphsList.sort(Comparator.comparing(o -> o.graphName));
            currentPartition.graphs = graphsList;
        }
        List<HighLevelPartition> resultPartitionList = new ArrayList<>();
        if (!resultPartitionsMap.isEmpty()) {
            ArrayList<Integer> partitionids = new ArrayList(resultPartitionsMap.keySet());
            partitionids.sort((o1, o2) -> o1.intValue() - o2.intValue());
            for (Integer partitionId : partitionids) {
                resultPartitionList.add(resultPartitionsMap.get(partitionId));
            }
        }
        HashMap<String, Object> dataMap = new HashMap<>();
        dataMap.put("partitions", resultPartitionList);
        return new RestApiResponse(dataMap, Pdpb.ErrorType.OK, Pdpb.ErrorType.OK.name());
    }

    /**
     * Get partition information
     * <p>
     * Retrieve all partition information, as well as the Raft node status and shard index information for each partition, by calling the pdRestService service.
     * Then iterate through each partition to construct a partition object, including the partition name, ID, shard list, etc.
     * For each shard, retrieve its status, progress, role, and other information via the pdRestService service, and populate the shard object with this data.
     * Finally, add the constructed partition objects to the list and sort them by partition name and ID.
     *
     * @return A RestApiResponse object containing partition information
     * @throws PDException If an exception occurs while retrieving partition information, a PDException exception is thrown
     */
    @GetMapping(value = "/partitions", produces = MediaType.APPLICATION_JSON_VALUE)
    public RestApiResponse getPartitions() {
        try {
            List<Partition> partitions = new ArrayList<>();
            List<Metapb.Partition> partitionList = pdRestService.getPartitions("");
            List<Metapb.Store> stores = pdRestService.getStoreStats(false);
            // The status of the raft node of the partition
            HashMap<Long, HashMap<Integer, Metapb.RaftStats>> raftMap = new HashMap<>();

            HashMap<Long, HashMap<String, Metapb.GraphStats>> shardIndexMap = new HashMap<>();
            String delimiter = "@";
            for (int i = 0; i < stores.size(); i++) {
                Metapb.Store store = stores.get(i);
                Metapb.StoreStats storeStats = store.getStats();
                HashMap<Integer, Metapb.RaftStats> storeRaftStats = new HashMap<>();
                List<Metapb.RaftStats> raftStatsList = storeStats.getRaftStatsList();
                for (int j = 0; j < raftStatsList.size(); j++) {
                    Metapb.RaftStats raftStats = raftStatsList.get(j);
                    storeRaftStats.put(raftStats.getPartitionId(), raftStats);
                }

                HashMap<String, Metapb.GraphStats> partitionShardStats = new HashMap<>();
                List<Metapb.GraphStats> graphStatsList = storeStats.getGraphStatsList();
                StringBuilder builder = new StringBuilder();
                for (int j = 0; j < graphStatsList.size(); j++) {
                    Metapb.GraphStats graphStats = graphStatsList.get(j);
                    String graphName = graphStats.getGraphName();
                    String partitionId = Integer.toString(graphStats.getPartitionId());
                    builder.append(graphName).append(delimiter).append(partitionId);
                    partitionShardStats.put(builder.toString(), graphStats);
                    builder.setLength(0);
                }
                raftMap.put(store.getId(), storeRaftStats);
                shardIndexMap.put(store.getId(), partitionShardStats);
            }

            for (Metapb.Partition pt : partitionList) {
                Partition partition = new Partition(pt);
                String graphName = partition.getGraphName();
                partition.getShards().sort(Comparator.comparing(Shard::getStoreId));
                Metapb.PartitionStats partitionStats =
                        pdRestService.getPartitionStats(graphName, pt.getId());
                Map<Long, Metapb.ShardStats> shardStats = new HashMap<>();
                if (partitionStats != null) {
                    String dateTime = DateFormatUtils.format(
                            partitionStats.getTimestamp(), DEFAULT_DATETIME_FORMAT);
                    partition.setTimestamp(dateTime);
                    shardStats = getShardStats(partitionStats);
                }

                for (Metapb.Shard shard : pdRestService.getShardList(pt.getId())) {
                    Map<Long, Metapb.ShardStats> finalShardStats = shardStats;
                    partition.getShards().add(new Shard() {{
                        storeId = Long.toString(shard.getStoreId());
                        role = shard.getRole();
                        address = pdRestService.getStore(
                                shard.getStoreId()).getAddress();
                        if (finalShardStats.containsKey(shard.getStoreId())) {
                            state = finalShardStats.get(shard.getStoreId()).getState().toString();
                            progress = finalShardStats.get(shard.getStoreId()).getProgress();
                            role = finalShardStats.get(shard.getStoreId()).getRole();
                        }

                        HashMap<Integer, Metapb.RaftStats> storeRaftStats =
                                raftMap.get(shard.getStoreId());
                        if (storeRaftStats != null) {
                            Metapb.RaftStats raftStats = storeRaftStats.get(partition.getId());
                            if (raftStats != null) {
                                committedIndex = Long.toString(raftStats.getCommittedIndex());
                            }
                        }
                    }});
                }

                partition.setPartitionStats(partitionStats);

                partitions.add(partition);
            }
            partitions.sort(
                    Comparator.comparing(Partition::getGraphName).thenComparing(Partition::getId));
            HashMap<String, Object> dataMap = new HashMap<>();
            dataMap.put("partitions", partitions);
            return new RestApiResponse(dataMap, Pdpb.ErrorType.OK, Pdpb.ErrorType.OK.name());
        } catch (PDException e) {
            log.error("query metric data error", e);
            return new RestApiResponse(null, e.getErrorCode(), e.getMessage());
        }
    }

    /**
     * Get partitions and their statistics
     * <p>
     * This interface is used to get all partitions corresponding to the graph and their statistics, and returns them in JSON format.
     *
     * @return JSON string containing partitions and their statistics
     * @throws PDException If an exception occurs while getting partitions or statistics, a PDException exception is thrown.
     */
    @GetMapping(value = "/partitionsAndStats", produces = MediaType.APPLICATION_JSON_VALUE)
    public String getPartitionsAndStats() {
        //for debug use, return partition && partitionStats
        try {
            Map<String, List<Metapb.Partition>> graph2Partitions = new HashMap<>();
            Map<String, List<Metapb.PartitionStats>> graph2PartitionStats = new HashMap<>();
            for (Metapb.Graph graph : pdRestService.getGraphs()) {
                List<Metapb.Partition> partitionList = new ArrayList<>();
                List<Metapb.PartitionStats> partitionStatsList = new ArrayList<>();
                for (Metapb.Partition partition : pdRestService.getPartitions(
                        graph.getGraphName())) {
                    Metapb.PartitionStats partitionStats = pdRestService
                            .getPartitionStats(graph.getGraphName(), partition.getId());
                    partitionList.add(partition);
                    partitionStatsList.add(partitionStats);
                }
                graph2Partitions.put(graph.getGraphName(), partitionList);
                graph2PartitionStats.put(graph.getGraphName(), partitionStatsList);
            }
            StringBuilder builder = new StringBuilder();
            builder.append("{\"partitions\":").append(toJSON(graph2Partitions));
            builder.append(",\"partitionStats\":").append(toJSON(graph2PartitionStats)).append("}");
            return builder.toString();
        } catch (PDException e) {
            log.error("PD exception:" + e);
            return toJSON(e);
        }
    }

    private Map<Long, Metapb.ShardStats> getShardStats(Metapb.PartitionStats partitionStats) {
        Map<Long, Metapb.ShardStats> stats = new HashMap<>();
        if (partitionStats.getShardStatsList() != null) {
            partitionStats.getShardStatsList().forEach(shardStats -> {
                stats.put(shardStats.getStoreId(), shardStats);
            });
        }
        return stats;
    }

    /**
     * Get partition log
     * Request log records for a specified time range and return a JSON-formatted response.
     *
     * @param request Request body containing the requested time range, including start and end times
     * @return Returns a JSON string containing partition log records. If no records are found, returns a JSON string containing error information
     * @throws PDException If an exception occurs while retrieving partition logs, captures and returns a JSON string containing exception information
     */
    @PostMapping(value = "/partitions/log", consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String getPartitionLog(@RequestBody TimeRangeRequest request) {
        try {
            Date dateStart = DateUtil.getDate(request.getStartTime());
            Date dateEnd = DateUtil.getDate(request.getEndTime());
            List<Metapb.LogRecord> changedRecords =
                    pdRestService.getPartitionLog(dateStart.getTime(),
                                                  dateEnd.getTime());
            if (changedRecords != null) {
                JsonFormat.TypeRegistry registry = JsonFormat.TypeRegistry
                        .newBuilder().add(Pdpb.SplitDataRequest.getDescriptor()).build();
                return toJSON(changedRecords, registry);
            } else {
                return toJSON(new PDException(Pdpb.ErrorType.NOT_FOUND_VALUE, "error"));
            }
        } catch (PDException e) {
            return toJSON(e);
        }
    }

    /**
     * Reset all partition states
     * Access the “/resetPartitionState” path via a GET request to reset all partition states
     *
     * @return If the operation is successful, returns the string “OK”; if an exception occurs, returns a JSON string containing the exception information
     * @throws PDException If an exception occurs while resetting the partition state, it is caught and a JSON string containing the exception information is returned
     */
    @GetMapping(value = "/resetPartitionState", produces = MediaType.APPLICATION_JSON_VALUE)
    public String resetPartitionState() {
        try {
            for (Metapb.Partition partition : pdRestService.getPartitions("")) {
                pdRestService.resetPartitionState(partition);
            }
        } catch (PDException e) {
            return toJSON(e);
        }
        return "OK";
    }

    /**
     * Retrieve system statistics
     * This interface obtains system statistics via a GET request and returns a Statistics object containing the statistical data
     * The URL path is ‘/’, with the response data type being application/json
     *
     * @return A Statistics object containing system statistics
     * @throws PDException          Throws a PDException if an exception occurs while retrieving statistics
     * @throws ExecutionException   Throws an ExecutionException if a task execution exception occurs
     * @throws InterruptedException Throws an InterruptedException if the thread is interrupted while waiting
     */
    @GetMapping(value = "/", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public Statistics getStatistics() throws PDException, ExecutionException, InterruptedException {

        Statistics statistics = new Statistics();
        int partitionId = -1;
        return statistics;
    }

    @Data
    class Shard {

        String address;
        String storeId;
        Metapb.ShardRole role;
        String state;
        int progress;
        String committedIndex;
        long partitionId;

    }

    @Data
    class Partition {

        int id;
        long version;
        String graphName;
        long startKey;
        long endKey;

        Metapb.PartitionState workState;
        List<Shard> shards;
        String timestamp;

        Partition(Metapb.Partition pt) {
            id = pt.getId();
            version = pt.getVersion();
            graphName = pt.getGraphName();
            startKey = pt.getStartKey();
            endKey = pt.getEndKey();
            workState = pt.getState();
            shards = new ArrayList<>();
        }

        public void setPartitionStats(Metapb.PartitionStats stats) {

        }
    }

    @Data
    class Statistics {

    }

    @Data
    class HighLevelPartition {

        int partitionId;
        String state;
        String leaderAddress;
        long keyCount;
        long dataSize;
        String shardState;
        int progress;
        long raftTerm;
        List<GraphStats> graphs;
        List<ShardStats> shards;
        String failureCause = "";

        HighLevelPartition(Metapb.Partition partition, Metapb.PartitionStats partitionStats) {
            partitionId = partition.getId();
            state = String.valueOf(partition.getState());
            if (partitionStats != null) {
                raftTerm = partitionStats.getLeaderTerm();
            }
            Metapb.ShardState tmpShardState = Metapb.ShardState.SState_Normal;
            if (partitionStats != null) {
                shards = new ArrayList<>();
                for (Metapb.ShardStats shardStats : partitionStats.getShardStatsList()) {
                    if ((shardStats.getState() != Metapb.ShardState.UNRECOGNIZED)
                        && (shardStats.getState().getNumber() > tmpShardState.getNumber())) {
                        tmpShardState = shardStats.getState();
                        progress = shardStats.getProgress();
                    }
                    shards.add(new ShardStats(shardStats));
                }
            } else {
                shards = new ArrayList<>();
                try {
                    for (Metapb.Shard shard : pdRestService.getShardList(partition.getId())) {
                        shards.add(new ShardStats(shard));
                    }
                } catch (PDException e) {
                    log.error("get shard list failed, {}", e.getMessage());
                }
            }
            // Synthesize the state of all replicas and assign a value to shardState
            shardState = tmpShardState.name();
        }
    }

    @Data
    class GraphStats {

        String graphName;
        long keyCount;
        long startKey;
        long endKey;
        long dataSize;
        String workState;
        long partitionId;

        GraphStats(Metapb.GraphStats graphStats) {
            graphName = graphStats.getGraphName();
            keyCount = graphStats.getApproximateKeys();
            workState = graphStats.getWorkState().toString();
            dataSize = graphStats.getApproximateSize();
            partitionId = graphStats.getPartitionId();
        }
    }

    @Data
    class ShardStats {

        long storeId;
        String role;
        String state;
        int progress;
        // Extra attributes
        long partitionId;
        String address;

        ShardStats(Metapb.ShardStats shardStats) {
            storeId = shardStats.getStoreId();
            role = String.valueOf(shardStats.getRole());
            state = shardStats.getState().toString();
            progress = shardStats.getProgress();
        }

        ShardStats(Metapb.Shard shard) {
            // When there is no initialization method for shardStats
            storeId = shard.getStoreId();
            role = String.valueOf(shard.getRole());
            state = Metapb.ShardState.SState_Normal.name();
            progress = 0;
        }
    }
}
