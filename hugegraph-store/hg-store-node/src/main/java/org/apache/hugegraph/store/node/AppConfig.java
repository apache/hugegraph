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

package org.apache.hugegraph.store.node;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.hugegraph.store.options.JobOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import lombok.Data;

@Data
@Component
public class AppConfig {

    private static int cpus = Runtime.getRuntime().availableProcessors();

    @Value("${pdserver.address}")
    private String pdServerAddress;

    @Value("${grpc.host}")
    private String host;

    @Value("${grpc.port}")
    private int grpcPort;

    @Value("${grpc.server.wait-time: 3600}")
    private int serverWaitTime;

    @Value("${server.port}")
    private int restPort;

    // Built-in pd mode, for standalone deployment
    @Value("${app.data-path: store}")
    private String dataPath;

    @Value("${app.placeholder-size: 10}")
    private Integer placeholderSize;

    @Value("${app.raft-path:}")
    private String raftPath;

    // Built-in pd mode, for standalone deployment
    @Value("${app.fake-pd: false}")
    private boolean fakePd;
    @Autowired
    private Raft raft;
    @Autowired
    private ArthasConfig arthasConfig;
    @Autowired
    private FakePdConfig fakePdConfig;
    @Autowired
    private LabelConfig labelConfig;
    @Autowired
    private RocksdbConfig rocksdbConfig;
    @Autowired
    private ThreadPoolGrpc threadPoolGrpc;
    @Autowired
    private ThreadPoolScan threadPoolScan;

    @Autowired
    private JobConfig jobConfig;

    @Autowired
    private QueryPushDownConfig queryPushDownConfig;

    public String getRaftPath() {
        if (raftPath == null || raftPath.length() == 0) {
            return dataPath;
        }
        return raftPath;
    }

    @PostConstruct
    public void init() {
        Runtime rt = Runtime.getRuntime();
        if (threadPoolScan.core == 0) {
            threadPoolScan.core = rt.availableProcessors() * 4;
        }

        Map<String, String> rocksdb = rocksdbConfig.rocksdb;
        if (!rocksdb.containsKey("total_memory_size")
            || "0".equals(rocksdb.get("total_memory_size"))) {
            rocksdb.put("total_memory_size", Long.toString(rt.maxMemory()));
        }
        long totalMemory = Long.parseLong(rocksdbConfig.rocksdb.get("total_memory_size"));
        if (raft.getDisruptorBufferSize() == 0) {
            int size = (int) (totalMemory / 1000 / 1000 / 1000);
            size = (int) Math.pow(2, Math.round(Math.log(size) / Math.log(2))) * 32;
            raft.setDisruptorBufferSize(size); // Increase one buffer every 32M
        }

        if (!rocksdb.containsKey("write_buffer_size") ||
            "0".equals(rocksdb.get("write_buffer_size"))) {
            rocksdb.put("write_buffer_size", Long.toString(totalMemory / 1000));
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("AppConfig \n")
               .append("rocksdb:\n");
        rocksdbConfig.rocksdb.forEach((k, v) -> builder.append("\t" + k + ":")
                                                       .append(v)
                                                       .append("\n"));
        builder.append("raft:\n");
        builder.append("\tdisruptorBufferSize: " + raft.disruptorBufferSize);
        return builder.toString();
    }

    public String getStoreServerAddress() {
        return String.format("%s:%d", host, grpcPort);
    }

    public Map<String, Object> getRocksdbConfig() {
        Map<String, Object> config = new HashMap<>();
        rocksdbConfig.rocksdb.forEach((k, v) -> {
            config.put("rocksdb." + k, v);
        });
        return config;
    }

    @Data
    @Configuration
    public class ThreadPoolGrpc {

        @Value("${thread.pool.grpc.core:600}")
        private int core;
        @Value("${thread.pool.grpc.max:1000}")
        private int max;
        @Value("${thread.pool.grpc.queue:" + Integer.MAX_VALUE + "}")
        private int queue;
    }

    @Data
    @Configuration
    public class ThreadPoolScan {

        @Value("${thread.pool.scan.core: 128}")
        private int core;
        @Value("${thread.pool.scan.max: 1000}")
        private int max;
        @Value("${thread.pool.scan.queue: 0}")
        private int queue;
    }

    @Data
    @Configuration
    public class Raft {

        @Value("${raft.address}")
        private String address;

        @Value("${raft.rpc-timeout:10000}")
        private int rpcTimeOut;
        @Value("${raft.metrics:true}")
        private boolean metrics;
        @Value("${raft.snapshotLogIndexMargin:0}")
        private int snapshotLogIndexMargin;
        @Value("${raft.snapshotInterval:300}")
        private int snapshotInterval;
        @Value("${raft.disruptorBufferSize:0}")
        private int disruptorBufferSize;
        @Value("${raft.max-log-file-size:50000000000}")
        private long maxLogFileSize;
        @Value("${ave-logEntry-size-ratio:0.95}")
        private double aveLogEntrySizeRation;
        @Value("${raft.useRocksDBSegmentLogStorage:true}")
        private boolean useRocksDBSegmentLogStorage;
        @Value("${raft.maxSegmentFileSize:67108864}")
        private int maxSegmentFileSize;
        @Value("${raft.maxReplicatorInflightMsgs:256}")
        private int maxReplicatorInflightMsgs;
        @Value("${raft.maxEntriesSize:256}")
        private int maxEntriesSize;
        @Value("${raft.maxBodySize:524288}")
        private int maxBodySize;

    }

    @Data
    @Configuration
    public class ArthasConfig {

        @Value("${arthas.telnetPort:8566}")
        private String telnetPort;

        @Value("${arthas.httpPort:8565}")
        private String httpPort;

        @Value("${arthas.ip:0.0.0.0}")
        private String arthasip;

        @Value("${arthas.disabledCommands:jad}")
        private String disCmd;
    }

    @Data
    @Configuration
    public class FakePdConfig {

        @Value("${fake-pd.store-list:''}")
        private String storeList;
        @Value("${fake-pd.peers-list:''}")
        private String peersList;   // fakePd mode, raft cluster initial configuration
        @Value("${fake-pd.partition-count:3}")
        private int partitionCount;
        @Value("${fake-pd.shard-count:3}")
        private int shardCount;
    }

    @Data
    @Configuration
    public class JobConfig {

        @Value("${job.interruptableThreadPool.core:128}")
        private int core;

        @Value("${job.interruptableThreadPool.max:256}")
        private int max;

        @Value("${job.interruptableThreadPool.queue:" + Integer.MAX_VALUE + "}")
        private int queueSize;

        @Value("${job.cleaner.batch.size:10000}")
        private int batchSize;

        @Value("${job.start-time:0}")
        private int startTime;

        @Value("${job.uninterruptibleThreadPool.core:0}")
        private int uninterruptibleCore;

        @Value("${job.uninterruptibleThreadPool.max:256}")
        private int uninterruptibleMax;

        @Value("${job.uninterruptibleThreadPool.queue:" + Integer.MAX_VALUE + "}")
        private int uninterruptibleQueueSize;
    }

    @Data
    @Configuration
    public class QueryPushDownConfig {

        /**
         * query v2 thread pool size
         */
        @Value("${query.push-down.threads:1500}")
        private int threadPoolSize;

        /**
         * the batch size that each request gets
         */
        @Value("${query.push-down.fetch_batch:20000}")
        private int fetchBatchSize;

        /**
         * the timeout of request fetch (ms)
         */
        @Value("${query.push-down.fetch_timeout:300000}")
        private long fetchTimeOut;

        /**
         * the limit of memory operations, like sort etc.
         */
        @Value("${query.push-down.memory_limit_count:50000}")
        private int memoryLimitCount;

        /**
         * limit size of index sst file size (kB)
         */
        @Value("${query.push-down.index_size_limit_count:50000}")
        private int indexSizeLimitCount;
    }

    @Data
    @Configuration
    @ConfigurationProperties(prefix = "app")
    public class LabelConfig {

        private final Map<String, String> label = new HashMap<>();
    }

    @Data
    @Configuration
    @ConfigurationProperties(prefix = "")
    public class RocksdbConfig {

        private Map<String, String> rocksdb = new HashMap<>();
    }

    public JobOptions getJobOptions() {
        JobOptions jobOptions = new JobOptions();
        jobOptions.setCore(jobConfig.getCore() == 0 ? cpus : jobConfig.getCore());
        jobOptions.setMax(jobConfig.getMax() == 0 ? cpus * 4 : jobConfig.getMax());
        jobOptions.setQueueSize(jobConfig.getQueueSize());
        jobOptions.setBatchSize(jobConfig.getBatchSize());
        int uninterruptibleCore = jobOptions.getUninterruptibleCore();
        jobOptions.setUninterruptibleCore(uninterruptibleCore == 0 ? cpus : uninterruptibleCore);
        int uninterruptibleMax = jobOptions.getUninterruptibleMax();
        jobOptions.setUninterruptibleMax(uninterruptibleMax == 0 ? cpus * 4 : uninterruptibleMax);
        jobOptions.setUninterruptibleQueueSize(jobConfig.getUninterruptibleQueueSize());
        return jobOptions;
    }
}
