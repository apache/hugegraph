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

import static org.apache.hugegraph.store.constant.HugeServerTables.INDEX_TABLE;
import static org.apache.hugegraph.store.constant.HugeServerTables.OUT_EDGE_TABLE;
import static org.apache.hugegraph.store.constant.HugeServerTables.VERTEX_TABLE;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;

import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.id.IdUtil;
import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Metapb.PartitionState;
import org.apache.hugegraph.pd.grpc.pulse.CleanType;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.serializer.BinaryElementSerializer;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.cmd.HgCmdClient;
import org.apache.hugegraph.store.cmd.request.BatchPutRequest;
import org.apache.hugegraph.store.cmd.request.CleanDataRequest;
import org.apache.hugegraph.store.cmd.response.BatchPutResponse;
import org.apache.hugegraph.store.cmd.response.UpdatePartitionResponse;
import org.apache.hugegraph.store.meta.PartitionManager;
import org.apache.hugegraph.store.query.util.KeyUtil;
import org.apache.hugegraph.store.raft.RaftClosure;
import org.apache.hugegraph.store.raft.RaftOperation;
import org.apache.hugegraph.store.term.Bits;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.structure.BaseEdge;
import org.apache.hugegraph.structure.BaseElement;
import org.apache.hugegraph.structure.BaseVertex;
import org.apache.hugegraph.structure.Index;
import org.apache.hugegraph.structure.builder.IndexBuilder;

import com.alipay.sofa.jraft.Status;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataManagerImpl implements DataManager {

    public static final int BATCH_PUT_SIZE = 2000;
    private BusinessHandler businessHandler;
    private PartitionManager metaManager;
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
    public void setMetaManager(PartitionManager metaManager) {
        this.metaManager = metaManager;
    }

    @Override
    public void setCmdClient(HgCmdClient client) {
        this.client = client;
    }

    @Override
    public Status move(Metapb.Partition source, List<Metapb.Partition> targets) throws Exception {
        Status status = Status.OK();
        // Take the partition offline before starting data movement
        UpdatePartitionResponse response =
                metaManager.updateState(source, PartitionState.PState_Offline);
        if (response.getStatus().isOK()) {
            status = move(source, targets, DataManagerImpl::findPartition);

            // After successful data migration, set the new partition range and bring the new
            // partition online
            for (var target : targets) {
                if (status.isOk()) {
                    if (!(metaManager.updateRange(target, (int) target.getStartKey(),
                                                  (int) target.getEndKey())
                                     .getStatus().isOK()
                          &&
                          metaManager.updateState(target, PartitionState.PState_Normal).getStatus()
                                     .isOK())) {
                        status.setError(-3, "new partition online fail");
                    }
                }
            }
        } else {
            status.setError(-1, "source partition offline fail");
        }

        metaManager.updateState(source, PartitionState.PState_Normal);

        return status;
    }

    @Override
    public Status move(Metapb.Partition source, Metapb.Partition target) throws Exception {
        // Only write to target
        return move(source, Collections.singletonList(target), (partitions, integer) -> target);
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

    private Status move(Metapb.Partition source, List<Metapb.Partition> targets,
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
                        if (++count >= BATCH_PUT_SIZE) {
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
                             pair.getValue(),
                             total);
                }
            }
        }

        return status;
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
    public void write(BatchPutRequest request) {
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
    public void clean(CleanDataRequest request) {
        // Raft performs actual data cleanup
        businessHandler.cleanPartition(request.getGraphName(), request.getPartitionId(),
                                       request.getKeyStart(), request.getKeyEnd(),
                                       request.getCleanType());
    }

    @Override
    public Status doBuildIndex(Metapb.BuildIndexParam param, Metapb.Partition source) throws
                                                                                      Exception {

        var partitionId = source.getId();
        var graphName = param.getGraph();
        log.info("doBuildIndex begin, partition id :{},  with param: {}", partitionId, param);

        Status status = Status.OK();
        var graphSupplier = BusinessHandlerImpl.getGraphSupplier(graphName);

        var labelId = IdUtil.fromBytes(param.getLabelId().toByteArray());
        IndexLabel indexLabel = null;
        if (param.hasIndexLabel()) {
            indexLabel =
                    graphSupplier.indexLabel(IdUtil.fromBytes(param.getIndexLabel().toByteArray()));
        }

        WriteBatch batch = new WriteBatch(param.getGraph());
        IndexBuilder builder = new IndexBuilder(graphSupplier);
        BinaryElementSerializer serializer = new BinaryElementSerializer();

        long countTotal = 0;
        long start = System.currentTimeMillis();
        long countRecord = 0;

        // todo : table scan or prefix scan
        try (var itr = businessHandler.scan(graphName,
                                            param.getIsVertexLabel() ? VERTEX_TABLE :
                                            OUT_EDGE_TABLE,
                                            (int) source.getStartKey(), (int) source.getEndKey())) {

            int count = 0;
            while (itr.hasNext()) {
                RocksDBSession.BackendColumn entry = itr.next();

                byte[] innerKey = entry.name;
                byte[] key = Arrays.copyOfRange(innerKey, 0, innerKey.length - Short.BYTES);
                var column = BackendColumn.of(key, entry.value);

                BaseElement element = null;

                try {
                    if (param.getIsVertexLabel()) {
                        element = serializer.parseVertex(graphSupplier, column, null);
                    } else {
                        element = serializer.parseEdge(graphSupplier, column, null, true);
                    }
                } catch (Exception e) {
                    log.error("parse element failed, graph:{}, key:{}", graphName, e);
                    continue;
                }

                // filter by label id
                if (!element.schemaLabel().id().equals(labelId)) {
                    continue;
                }

                countRecord += 1;

                List<Index> array;
                if (indexLabel != null) {
                    // label id
                    array = builder.buildIndex(element, indexLabel);
                } else if (param.hasLabelIndex() && param.getLabelIndex()) {
                    // element type index
                    array = builder.buildLabelIndex(element);
                } else {
                    // rebuild all index
                    if (param.getIsVertexLabel()) {
                        assert element instanceof BaseVertex;
                        array = builder.buildVertexIndex((BaseVertex) element);
                    } else {
                        assert element instanceof BaseEdge;
                        array = builder.buildEdgeIndex((BaseEdge) element);
                    }
                }

                for (var index : array) {
                    var col = serializer.writeIndex(index);
                    int code = PartitionUtils.calcHashcode(KeyUtil.getOwnerId(index.elementId()));
                    // same partition id with element
                    batch.add(partitionId, BatchPutRequest.KV.of(INDEX_TABLE, code, col.name,
                                                                 col.value == null ? new byte[0] :
                                                                 col.value));

                    if (++count >= BATCH_PUT_SIZE) {
                        if (!batch.sync()) {
                            status.setError(-2, "sync index failed");
                            break;
                        }
                        count = 0;
                    }
                    countTotal++;
                }

                if (!status.isOk()) {
                    break;
                }
            }

            if (status.isOk()) {
                if (count > 0) {
                    if (!batch.sync()) {
                        status.setError(-2, "sync index failed");
                    }
                }
            }

            log.info("doBuildIndex end, partition id: {}, records: {},  total index: {}, cost: {}s",
                     source.getId(),
                     countRecord, countTotal, (System.currentTimeMillis() - start) / 1000);
        }

        return status;
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

        public Boolean sync() {
            boolean ret = true;
            for (Map.Entry<Integer, List<BatchPutRequest.KV>> entry : data.entrySet()) {
                ret = ret && sendData(entry.getKey(), entry.getValue());
            }
            for (List<BatchPutRequest.KV> list : data.values()) {
                list.clear();
            }

            return ret;
        }

        public Boolean sendData(Integer partId, List<BatchPutRequest.KV> kvs) {
            BatchPutRequest request = new BatchPutRequest();
            request.setGraphName(graphName);
            request.setPartitionId(partId);
            request.setEntries(kvs);

            var engine = HgStoreEngine.getInstance().getPartitionEngine(partId);

            if (engine != null && engine.isLeader()) {
                try {
                    CountDownLatch latch = new CountDownLatch(1);

                    final Boolean[] ret = {Boolean.FALSE};
                    engine.addRaftTask(RaftOperation.create(RaftOperation.IN_WRITE_OP, request),
                                       new RaftClosure() {
                                           @Override
                                           public void run(Status status) {
                                               if (status.isOk()) {
                                                   ret[0] = Boolean.TRUE;
                                               }
                                               latch.countDown();
                                           }
                                       });
                    latch.await();

                    if (ret[0]) {
                        return true;
                    }
                } catch (Exception e) {
                    // using send data by client when exception occurs
                    log.warn("send data by raft: pid: {}, error: ", partId, e);
                }
            }

            BatchPutResponse response = client.batchPut(request);
            if (response == null || !response.getStatus().isOK()) {
                log.error("sendData error, pId:{} status:{}", partId,
                          response != null ? response.getStatus() : "EMPTY_RESPONSE");
                return false;
            }

            return true;
        }
    }
}
