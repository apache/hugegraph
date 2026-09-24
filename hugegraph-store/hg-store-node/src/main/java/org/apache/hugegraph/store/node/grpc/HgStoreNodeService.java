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

package org.apache.hugegraph.store.node.grpc;

import static org.apache.hugegraph.store.grpc.common.GraphMethod.GRAPH_METHOD_DELETE;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.business.DataManagerImpl;
import org.apache.hugegraph.store.grpc.session.BatchReq;
import org.apache.hugegraph.store.grpc.session.CleanReq;
import org.apache.hugegraph.store.grpc.session.GraphReq;
import org.apache.hugegraph.store.grpc.session.TableReq;
import org.apache.hugegraph.store.node.AppConfig;
import org.apache.hugegraph.store.options.HgStoreEngineOptions;
import org.apache.hugegraph.store.options.RaftRocksdbOptions;
import org.apache.hugegraph.store.raft.RaftClosure;
import org.apache.hugegraph.store.raft.RaftOperation;
import org.apache.hugegraph.store.raft.RaftTaskHandler;
import org.apache.hugegraph.store.util.HgRaftError;
import org.apache.hugegraph.store.util.HgStoreException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.NodeMetrics;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import lombok.extern.slf4j.Slf4j;

/**
 * raft task executor
 */
@Slf4j
@Service
public class HgStoreNodeService implements RaftTaskHandler {

    public static final byte BATCH_OP = 0x12;
    public static final byte TABLE_OP = 0x13;
    public static final byte GRAPH_OP = 0x14;
    public static final byte CLEAN_OP = 0x15;
    public static final byte TTL_CLEAN_OP = 0x16;
    public static final byte MAX_OP = 0x59;
    private final AppConfig appConfig;
    @Autowired
    HgStoreSessionImpl hgStoreSession;
    private HgStoreEngine storeEngine;

    public HgStoreNodeService(@Autowired AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    public HgStoreEngine getStoreEngine() {
        return this.storeEngine;
    }

    @PostConstruct
    public void init() {
        log.info("{}", appConfig.toString());
        HgStoreEngineOptions options = new HgStoreEngineOptions() {{
            setRaftAddress(appConfig.getRaft().getAddress());
            setDataPath(appConfig.getDataPath());
            setRaftPath(appConfig.getRaftPath());
            setPdAddress(appConfig.getPdServerAddress());
            setFakePD(appConfig.isFakePd());
            setRocksdbConfig(appConfig.getRocksdbConfig());
            setGrpcAddress(appConfig.getStoreServerAddress());
            setLabels(appConfig.getLabelConfig().getLabel());
            setRaftOptions(new RaftOptions() {{
                setMetrics(appConfig.getRaft().isMetrics());
                setRpcDefaultTimeout(appConfig.getRaft().getRpcTimeOut());
                setSnapshotLogIndexMargin(appConfig.getRaft().getSnapshotLogIndexMargin());
                setSnapshotIntervalSecs(appConfig.getRaft().getSnapshotInterval());
                setDisruptorBufferSize(appConfig.getRaft().getDisruptorBufferSize());
                setMaxLogSize(appConfig.getRaft().getMaxLogFileSize());
                setAveLogEntrySizeRatio(appConfig.getRaft().getAveLogEntrySizeRation());
                setUseRocksDBSegmentLogStorage(appConfig.getRaft()
                                                        .isUseRocksDBSegmentLogStorage());
                setMaxSegmentFileSize(appConfig.getRaft().getMaxSegmentFileSize());
                setMaxReplicatorInflightMsgs(appConfig.getRaft().getMaxReplicatorInflightMsgs());
                setMaxEntriesSize(appConfig.getRaft().getMaxEntriesSize());
                setMaxBodySize(appConfig.getRaft().getMaxBodySize());
            }});
            setFakePdOptions(new FakePdOptions() {{
                setStoreList(appConfig.getFakePdConfig().getStoreList());
                setPeersList(appConfig.getFakePdConfig().getPeersList());
                setPartitionCount(appConfig.getFakePdConfig().getPartitionCount());
                setShardCount(appConfig.getFakePdConfig().getShardCount());
            }});
            setQueryPushDownOption(new QueryPushDownOption(){{
                setThreadPoolSize(appConfig.getQueryPushDownConfig().getThreadPoolSize());
                setFetchBatchSize(appConfig.getQueryPushDownConfig().getFetchBatchSize());
                setFetchTimeout(appConfig.getQueryPushDownConfig().getFetchTimeOut());
                setMemoryLimitCount(appConfig.getQueryPushDownConfig().getMemoryLimitCount());
                setIndexSizeLimitCount(appConfig.getQueryPushDownConfig().getIndexSizeLimitCount());
            }});
            setJobConfig(appConfig.getJobOptions());
        }};

        RaftRocksdbOptions.initRocksdbGlobalConfig(options.getRocksdbConfig());

        options.getLabels().put("rest.port", Integer.toString(appConfig.getRestPort()));
        log.info("HgStoreEngine init {}", options);
        options.setTaskHandler(this);
        options.setDataTransfer(new DataManagerImpl());
        storeEngine = HgStoreEngine.getInstance();
        storeEngine.init(options);

    }

    public List<Integer> getGraphLeaderPartitionIds(String graphName) {
        return storeEngine.getPartitionManager().getLeaderPartitionIds(graphName);
    }

    /**
     * Add raft task, forward data to raft
     *
     * @return true means the data has been submitted, false means not submitted, used to reduce
     * batch splitting for single-replica storage
     */
    public <Req extends com.google.protobuf.GeneratedMessageV3>
    void addRaftTask(byte methodId, String graphName, Integer partitionId, Req req,
                     RaftClosure closure) {
        if (!storeEngine.isClusterReady()) {
            closure.run(new Status(HgRaftError.CLUSTER_NOT_READY.getNumber(),
                                   "The cluster is not ready, please check active stores number!"));
            log.error("The cluster is not ready, please check active stores number!");
            return;
        }
        //
        try {
            // Serialization
            final byte[] buffer = new byte[req.getSerializedSize() + 1];
            final CodedOutputStream output = CodedOutputStream.newInstance(buffer);
            output.write(methodId);
            req.writeTo(output);
            output.checkNoSpaceLeft();
            output.flush();
            // Add raft task
            storeEngine.addRaftTask(graphName, partitionId,
                                    RaftOperation.create(methodId, buffer, req), closure);

        } catch (Exception e) {
            closure.run(new Status(HgRaftError.UNKNOWN.getNumber(), e.getMessage()));
            log.error("addRaftTask {}", e);
        }

    }

    /**
     * Tasks from logs, generally tasks from followers or log rollbacks
     */
    @Override
    public boolean invoke(int partId, byte[] request, RaftClosure response) throws
                                                                            HgStoreException {
        try {
            CodedInputStream input = CodedInputStream.newInstance(request);
            byte methodId = input.readRawByte();
            switch (methodId) {
                case HgStoreNodeService.BATCH_OP:
                    invoke(partId, methodId, BatchReq.parseFrom(input), response);
                    break;
                case HgStoreNodeService.TABLE_OP:
                    invoke(partId, methodId, TableReq.parseFrom(input), response);
                    break;
                case HgStoreNodeService.GRAPH_OP:
                    invoke(partId, methodId, GraphReq.parseFrom(input), response);
                    break;
                case HgStoreNodeService.CLEAN_OP:
                    invoke(partId, methodId, CleanReq.parseFrom(input), response);
                    break;
                default:
                    return false; // Unhandled
            }
        } catch (IOException e) {
            throw new HgStoreException(e.getMessage(), e);
        }
        return true;
    }

    /**
     * Process the data sent by raft
     */
    @Override
    public boolean invoke(int partId, byte methodId, Object req, RaftClosure response) throws
                                                                                       HgStoreException {
        switch (methodId) {
            case HgStoreNodeService.BATCH_OP:
                hgStoreSession.doBatch(partId, (BatchReq) req, response);
                break;
            case HgStoreNodeService.TABLE_OP:
                hgStoreSession.doTable(partId, (TableReq) req, response);
                break;
            case HgStoreNodeService.GRAPH_OP:
                if (((GraphReq) req).getMethod() == GRAPH_METHOD_DELETE) {
                    storeEngine.deletePartition(partId, ((GraphReq) req).getGraphName());
                }
                hgStoreSession.doGraph(partId, (GraphReq) req, response);
                break;
            case HgStoreNodeService.CLEAN_OP:
                hgStoreSession.doClean(partId, (CleanReq) req, response);
                break;
            default:
                return false; // Unhandled
        }
        return true;
    }

    @PreDestroy
    public void destroy() {
        storeEngine.shutdown();
    }

    private String getSerializingExceptionMessage(String target) {
        return "Serializing "
               + getClass().getName()
               + " to a "
               + target
               + " threw an IOException (should never happen).";
    }

    public Map<String, NodeMetrics> getNodeMetrics() {
        return storeEngine.getNodeMetrics();
    }
}
