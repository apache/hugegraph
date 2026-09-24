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

package org.apache.hugegraph.store.node.task;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hugegraph.pd.client.KvClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.grpc.kv.KResponse;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.rocksdb.access.SessionOperator;
import org.apache.hugegraph.serializer.DirectBinarySerializer;
import org.apache.hugegraph.serializer.DirectBinarySerializer.DirectHugeElement;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.business.BusinessHandlerImpl;
import org.apache.hugegraph.store.business.InnerKeyCreator;
import org.apache.hugegraph.store.business.InnerKeyFilter;
import org.apache.hugegraph.store.constant.HugeServerTables;
import org.apache.hugegraph.store.consts.PoolNames;
import org.apache.hugegraph.store.node.AppConfig;
import org.apache.hugegraph.store.node.grpc.HgStoreNodeService;
import org.apache.hugegraph.store.node.task.ttl.DefaulTaskSubmitter;
import org.apache.hugegraph.store.node.task.ttl.RaftTaskSubmitter;
import org.apache.hugegraph.store.node.task.ttl.TaskInfo;
import org.apache.hugegraph.store.node.task.ttl.TaskSubmitter;
import org.apache.hugegraph.store.pd.DefaultPdProvider;
import org.apache.hugegraph.store.pd.PdProvider;
import org.apache.hugegraph.store.util.DefaultThreadFactory;
import org.apache.hugegraph.store.util.ExecutorUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;

import lombok.extern.slf4j.Slf4j;

/**
 * @date 2023/6/12
 **/
@Service
@Slf4j
public class TTLCleaner implements Runnable {

    private static final String[] tables =
            new String[]{
                    HugeServerTables.VERTEX_TABLE,
                    HugeServerTables.IN_EDGE_TABLE,
                    HugeServerTables.OUT_EDGE_TABLE,
                    HugeServerTables.INDEX_TABLE
            };
    private final ScheduledExecutorService scheduler;
    private final HgStoreEngine storeEngine;
    private PdProvider pd;
    private KvClient client;
    private ThreadPoolExecutor executor;
    private final Set<Integer> failedPartitions = Sets.newConcurrentHashSet();
    private final ScheduledFuture<?> future;
    private final String key = "HUGEGRAPH/hg/EXPIRED";
    private final DirectBinarySerializer serializer = new DirectBinarySerializer();
    @Autowired
    private HgStoreNodeService service;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private final AppConfig appConfig;
    private final AppConfig.JobConfig jobConfig;

    public TTLCleaner(@Autowired AppConfig config) {
        this.appConfig = config;
        jobConfig = config.getJobConfig();
        LocalDateTime now = LocalDateTime.now();
        int startTime = jobConfig.getStartTime();
        if (startTime < 0 || startTime > 23) {
            startTime = 19;
        }
        LocalDateTime next = now.withHour(startTime).withMinute(0).withSecond(0).withNano(0);
        Duration between = Duration.between(now, next);
        long delay = between.getSeconds(); // It's better to start the calculation in the early morning
        if (delay < 0) {
            delay += 3600 * 24;
        }
        log.info("clean task will begin in {} seconds", delay);
        DefaultThreadFactory factory = new DefaultThreadFactory("ttl-cleaner");
        scheduler = new ScheduledThreadPoolExecutor(1, factory);
        future = scheduler.scheduleAtFixedRate(this, delay, 24 * 3600, TimeUnit.SECONDS);
        storeEngine = HgStoreEngine.getInstance();
    }

    public void submit() {
        scheduler.submit(this);
    }

    public BiFunction<byte[], byte[], Boolean> getJudge(String table) {

        try {
            switch (table) {
                case HugeServerTables.VERTEX_TABLE:
                    return (key, value) -> {
                        DirectHugeElement el = serializer.parseVertex(key, value);
                        return predicate(el);
                    };
                case HugeServerTables.OUT_EDGE_TABLE:
                case HugeServerTables.IN_EDGE_TABLE:
                    return (key, value) -> {
                        DirectHugeElement el = serializer.parseEdge(key, value);
                        return predicate(el);
                    };
                case HugeServerTables.INDEX_TABLE:
                    return (key, value) -> {
                        DirectHugeElement el = serializer.parseIndex(key, value);
                        return predicate(el);
                    };
                default:
                    throw new UnsupportedOperationException("unsupported table");
            }

        } catch (Exception e) {
            log.error("failed to parse entry: ", e);
            throw e;
        }
    }

    private Boolean predicate(DirectHugeElement el) {
        long expiredTime = el.expiredTime();
        if (expired(expiredTime)) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    private boolean expired(long expiredTime) {
        return expiredTime != 0 && expiredTime < System.currentTimeMillis();
    }

    @Override
    public void run() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            running.set(true);
            if (client == null) {
                PDConfig config = PDConfig.of(appConfig.getPdServerAddress());
                config.setAuthority(DefaultPdProvider.name, DefaultPdProvider.authority);
                client = new KvClient(config);
            }
            KResponse k = client.get(key);
            String g = k.getValue();

            log.info("cleaner config:{}", jobConfig);
            if (executor == null) {
                executor =
                        ExecutorUtil.createExecutor(PoolNames.I_JOB, jobConfig.getCore(),
                                                    jobConfig.getMax(),
                                                    jobConfig.getQueueSize());
            }
            BusinessHandlerImpl handler = (BusinessHandlerImpl) storeEngine.getBusinessHandler();
            if (!StringUtils.isEmpty(g)) {
                String[] graphs = StringUtils.split(g, ",");
                log.info("clean task got graphs:{}", Arrays.toString(graphs));
                if (ArrayUtils.isEmpty(graphs)) {
                    return;
                }
                runAll(graphs, handler);
            } else {
                log.info("there is no specific graph to clean up and will do compact directly");
                Set<Integer> leaderPartitions = handler.getLeaderPartitionIdSet();
                leaderPartitions.forEach(
                        p -> new RaftTaskSubmitter(service, handler).submitCompaction(p));
            }
        } catch (Exception e) {
            log.error("clean ttl with error.", e);
        } finally {
            running.set(false);
        }
    }

    private void runAll(String[] graphs, BusinessHandlerImpl handler) throws InterruptedException {
        long start = System.currentTimeMillis();
        Map<String, TaskInfo> tasks = new ConcurrentHashMap<>(graphs.length);
        LinkedList<Triple<Integer, String, String>> elements = new LinkedList<>();
        Map<Integer, AtomicLong> pc = new ConcurrentHashMap<>();
        for (String graph : graphs) {
            if (!StringUtils.isEmpty(graph)) {
                String[] fields = graph.split(":");
                String graphName;
                long startTime = 0;
                boolean isRaft = false;
                if (fields.length > 0) {
                    graphName = fields[0];
                    if (fields.length > 1) {
                        String time = StringUtils.isEmpty(fields[1]) ? "0" : fields[1];
                        startTime = Long.parseLong(time);
                    }
                    if (fields.length > 2) {
                        String raft = StringUtils.isEmpty(fields[2]) ? "0" : fields[2];
                        if ("1".equals(raft)) {
                            isRaft = true;
                        }
                    }
                    TaskInfo taskInfo = new TaskInfo(handler, graphName, isRaft, startTime, tables,
                                                     service);
                    tasks.put(graphName, taskInfo);
                    List<Integer> ids = taskInfo.getPartitionIds();
                    for (Integer pId : ids) {
                        for (String table : tables) {
                            Triple<Integer, String, String> triple =
                                    new ImmutableTriple<>(pId, graphName, table);
                            elements.add(triple);
                        }
                        pc.putIfAbsent(pId, new AtomicLong(0));
                    }
                }
            }
        }
        CountDownLatch latch = new CountDownLatch(elements.size());
        for (Triple<Integer, String, String> t : elements) {
            Runnable r = getTask(handler, latch, t, tasks, pc);
            executor.execute(r);
        }
        latch.await();
        for (Map.Entry<Integer, AtomicLong> entry : pc.entrySet()) {
            AtomicLong count = entry.getValue();
            if (count.get() > 0) {
                Integer id = entry.getKey();
                new DefaulTaskSubmitter(service, handler).submitCompaction(id);
            }
        }
        Gson gson = new Gson();
        String msg = gson.toJson(tasks);
        long end = System.currentTimeMillis();
        log.info("clean data cost:{}, size :{}", (end - start), msg);
    }

    private Runnable getTask(
            BusinessHandlerImpl handler,
            CountDownLatch latch,
            Triple<Integer, String, String> t,
            Map<String, TaskInfo> counter,
            Map<Integer, AtomicLong> pc) {
        int batchSize = appConfig.getJobConfig().getBatchSize();
        return () -> {
            Integer id = t.getLeft();
            String graph = t.getMiddle();
            String table = t.getRight();
            TaskInfo taskInfo = counter.get(graph);
            ScanIterator scan = null;
            try {
                Map<String, AtomicLong> graphCounter = taskInfo.getTableCounter();
                TaskSubmitter submitter = taskInfo.getTaskSubmitter();
                AtomicLong tableCounter = graphCounter.get(table);
                RocksDBSession session = handler.getSession(id);
                InnerKeyCreator keyCreator = handler.getKeyCreator();
                SessionOperator op = session.sessionOp();
                BiFunction<byte[], byte[], Boolean> judge = getJudge(table);
                scan = op.scan(table,
                               keyCreator.getStartKey(id, graph),
                               keyCreator.getEndKey(id, graph),
                               ScanIterator.Trait.SCAN_LT_END);
                InnerKeyFilter filter = new InnerKeyFilter(scan, true);
                LinkedList<ByteString> all = new LinkedList<>();
                AtomicBoolean state = new AtomicBoolean(true);
                AtomicLong partitionCounter = pc.get(id);
                while (filter.hasNext() && state.get()) {
                    RocksDBSession.BackendColumn current = filter.next();
                    byte[] realKey =
                            Arrays.copyOfRange(current.name, 0, current.name.length - Short.BYTES);
                    if (judge.apply(realKey, current.value)) {
                        ByteString e = ByteString.copyFrom(current.name);
                        all.add(e);
                    }
                    if (all.size() >= batchSize) {
                        submitter.submitClean(id, graph, table, all, state, tableCounter,
                                              partitionCounter);
                        all = new LinkedList<>();
                    }
                }
                if (all.size() > 0 && state.get()) {
                    submitter.submitClean(id, graph, table, all, state, tableCounter,
                                          partitionCounter);
                }
                log.info("id:{}, graph:{}, table:{}, count:{} clean ttl data done and will do " +
                         "compact", id, graph, table, tableCounter.get());
            } catch (Exception e) {
                String s = "clean ttl with error by: partition-%s,graph-%s,table-%s:";
                String msg = String.format(s, id, graph, table);
                log.error(msg, e);
            } finally {
                latch.countDown();
                if (scan != null) {
                    scan.close();
                }
            }
        };
    }

    public ScheduledFuture<?> getFuture() {
        return future;
    }

    public ThreadPoolExecutor getExecutor() {
        return executor;
    }

    public ScheduledExecutorService getScheduler() {
        return scheduler;
    }
}
