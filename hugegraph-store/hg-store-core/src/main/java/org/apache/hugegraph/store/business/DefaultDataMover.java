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

package org.apache.hugegraph.store.business;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.pulse.CleanType;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.cmd.HgCmdClient;
import org.apache.hugegraph.store.cmd.request.BatchPutRequest;
import org.apache.hugegraph.store.cmd.request.CleanDataRequest;
import org.apache.hugegraph.store.cmd.request.UpdatePartitionRequest;
import org.apache.hugegraph.store.cmd.response.BatchPutResponse;
import org.apache.hugegraph.store.cmd.response.UpdatePartitionResponse;
import org.apache.hugegraph.store.term.Bits;

import com.alipay.sofa.jraft.Status;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Deprecated
public class DefaultDataMover implements DataMover {

    public static int Batch_Put_Size = 2000;
    private BusinessHandler businessHandler;
    private HgCmdClient client;

    private static Metapb.Partition findPartition(List<Metapb.Partition> partitions, int code) {
        for (Metapb.Partition partition : partitions) {
            if (code >= partition.getStartKey() && code < partition.getEndKey()) {
                return partition;
            }
        }
        return null;
    }

    @Override
    public void setBusinessHandler(BusinessHandler handler) {
        this.businessHandler = handler;
    }

    @Override
    public void setCmdClient(HgCmdClient client) {
        this.client = client;
    }

    @Override
    public Status moveData(Metapb.Partition source, List<Metapb.Partition> targets) throws
                                                                                    Exception {
        Status status = Status.OK();
        // Before starting to move data, take the partition offline first.
        UpdatePartitionResponse response =
                updatePartitionState(source, Metapb.PartitionState.PState_Offline);
        if (response.getStatus().isOK()) {
            status = moveData(source, targets, DefaultDataMover::findPartition);

            // Data migration successful, set new partition range and launch new partition.
            for (var target : targets) {
                if (status.isOk()) {
                    if (!(updatePartitionRange(target, (int) target.getStartKey(),
                                               (int) target.getEndKey())
                                  .getStatus().isOK()
                          && updatePartitionState(target,
                                                  Metapb.PartitionState.PState_Normal).getStatus()
                                                                                      .isOK())) {
                        status.setError(-3, "new partition online fail");
                    }
                }
            }
        } else {
            status.setError(-1, "source partition offline fail");
        }

        updatePartitionState(source, Metapb.PartitionState.PState_Normal);

        return status;
    }

    @Override
    public Status moveData(Metapb.Partition source, Metapb.Partition target) throws Exception {
        // only write to target
        return moveData(source, Collections.singletonList(target), (partitions, integer) -> target);
    }

    /**
     * move data from partition to targets
     *
     * @param source            source partition
     * @param targets           target partitions
     * @param partitionSelector the key of source partition belongs which target
     * @return execution result
     * @throws Exception exception when put data
     */

    private Status moveData(Metapb.Partition source, List<Metapb.Partition> targets,
                            BiFunction<List<Metapb.Partition>, Integer, Metapb.Partition> partitionSelector)
            throws Exception {

        Status status = Status.OK();
        String graphName = source.getGraphName();
        List<String> tables = businessHandler.getTableNames(graphName, source.getId());

        log.info("moveData, graph:{}, partition id:{} tables:{}, {}-{}", source.getGraphName(),
                 source.getId(), tables,
                 source.getStartKey(), source.getEndKey());
        WriteBatch batch = new WriteBatch(graphName);
        // target partition : count
        Map<Integer, Long> moveCount = new HashMap<>();

        for (String table : tables) {
            int total = 0;
            moveCount.clear();

            try (ScanIterator iterator =
                         businessHandler.scan(graphName, table, (int) source.getStartKey(),
                                              (int) source.getEndKey())) {
                int count = 0;
                while (iterator.hasNext() && status.isOk()) {
                    total += 1;
                    RocksDBSession.BackendColumn entry = iterator.next();
                    byte[] innerKey = entry.name;
                    byte[] key = Arrays.copyOfRange(innerKey, 0, innerKey.length - Short.BYTES);
                    int code = Bits.getShort(innerKey, innerKey.length - Short.BYTES);
                    Metapb.Partition partition = partitionSelector.apply(targets, code);
                    if (partition != null) {
                        moveCount.put(partition.getId(),
                                      moveCount.getOrDefault(partition.getId(), 0L) + 1);
                        batch.add(partition.getId(),
                                  BatchPutRequest.KV.of(table, code, key, entry.value));
                        if (++count >= Batch_Put_Size) {
                            if (!batch.sync()) {
                                status.setError(-2, "move data fail");
                            }
                            count = 0;
                        }
                    }
                }
                if (count > 0) {
                    if (!batch.sync()) {
                        status.setError(-2, "move data fail");
                    }
                }

                for (var pair : moveCount.entrySet()) {
                    log.info("{}-{}, table: {}, move to partition id {}, count:{}, total:{}",
                             source.getGraphName(), source.getId(), table, pair.getKey(),
                             pair.getValue(), total);
                }
            }
        }

        return status;
    }

    @Override
    public UpdatePartitionResponse updatePartitionState(Metapb.Partition partition,
                                                        Metapb.PartitionState state) {
        // When the partition splits, it actively needs to find the leader to synchronize
        // information.
        UpdatePartitionRequest request = new UpdatePartitionRequest();
        request.setWorkState(state);
        request.setPartitionId(partition.getId());
        request.setGraphName(partition.getGraphName());
        return client.raftUpdatePartition(request);
    }

    @Override
    public UpdatePartitionResponse updatePartitionRange(Metapb.Partition partition, int startKey,
                                                        int endKey) {
        // When the partition splits, it actively needs to find the leader for information
        // synchronization.
        UpdatePartitionRequest request = new UpdatePartitionRequest();
        request.setStartKey(startKey);
        request.setEndKey(endKey);
        request.setPartitionId(partition.getId());
        request.setGraphName(partition.getGraphName());
        return client.raftUpdatePartition(request);
    }

    @Override
    public void cleanData(Metapb.Partition partition) {
        String graphName = partition.getGraphName();
        CleanDataRequest request = new CleanDataRequest();
        request.setGraphName(graphName);
        request.setPartitionId(partition.getId());
        request.setCleanType(CleanType.CLEAN_TYPE_KEEP_RANGE);
        request.setKeyStart(partition.getStartKey());
        request.setKeyEnd(partition.getEndKey());
        request.setDeletePartition(false);

        try {
            client.cleanData(request);
        } catch (Exception e) {
            log.error("exception ", e);
        }
    }

    @Override
    public void doWriteData(BatchPutRequest request) {
        BusinessHandler.TxBuilder builder =
                businessHandler.txBuilder(request.getGraphName(), request.getPartitionId());
        BusinessHandler.Tx transaction = builder.build();
        try {
            for (BatchPutRequest.KV kv : request.getEntries()) {
                builder.put(kv.getCode(), kv.getTable(), kv.getKey(), kv.getValue());
            }
            transaction.commit();
        } catch (Throwable e) {
            try {
                transaction.rollback();
            } catch (Throwable rollbackError) {
                e.addSuppressed(rollbackError);
            }
            throw e;
        }
    }

    @Override
    public void doCleanData(CleanDataRequest request) {
        // raft performs real data cleanup
        businessHandler.cleanPartition(request.getGraphName(), request.getPartitionId(),
                                       request.getKeyStart(), request.getKeyEnd(),
                                       request.getCleanType());
    }

    class WriteBatch {

        private final Map<Integer, List<BatchPutRequest.KV>> data = new HashMap<>();
        private final String graphName;

        public WriteBatch(String graphName) {
            this.graphName = graphName;
        }

        public WriteBatch add(int partition, BatchPutRequest.KV kv) {
            if (!data.containsKey(partition)) {
                data.put(partition, new LinkedList<>());
            }
            data.get(partition).add(kv);
            return this;
        }

        public Boolean sync() throws Exception {
            Boolean ret = true;
            for (Map.Entry<Integer, List<BatchPutRequest.KV>> entry : data.entrySet()) {
                ret = ret && sendData(entry.getKey(), entry.getValue());
            }
            for (List<BatchPutRequest.KV> list : data.values()) {
                list.clear();
            }

            return ret;
        }

        public Boolean sendData(Integer partId, List<BatchPutRequest.KV> kvs) throws Exception {
            BatchPutRequest request = new BatchPutRequest();
            request.setGraphName(graphName);
            request.setPartitionId(partId);
            request.setEntries(kvs);
            BatchPutResponse response = client.batchPut(request);
            if (response == null || !response.getStatus().isOK()) {
                log.error("sendData moveData error, pId:{} status:{}", partId,
                          response != null ? response.getStatus() : "EMPTY_RESPONSE");
                return false;
            }
            return true;
        }
    }
}
