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

package org.apache.hugegraph.store.processor;

import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatResponse;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.cmd.request.RedirectRaftTaskRequest;
import org.apache.hugegraph.store.meta.MetadataKeyHelper;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.raft.RaftClosure;
import org.apache.hugegraph.store.raft.RaftOperation;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.google.protobuf.GeneratedMessageV3;

import lombok.extern.slf4j.Slf4j;

/**
 * @date 2023/10/10
 **/
@Slf4j
public abstract class CommandProcessor {

    /**
     * Queue commands by partition for the respective partition
     */
    private static final Map<Integer, BlockingDeque<Runnable>> TASKS = new ConcurrentHashMap<>();
    /**
     * Execution status of partition tasks (whether they are running)
     */
    private static final Map<Integer, AtomicBoolean> TASK_STATS = new ConcurrentHashMap<>();
    protected static ExecutorService threadPool = HgStoreEngine.getUninterruptibleJobs();
    protected HgStoreEngine storeEngine;

    public CommandProcessor(HgStoreEngine storeEngine) {
        this.storeEngine = storeEngine;
    }

    /**
     * Check if any instructions are currently executing
     *
     * @return true if there is a task running, otherwise false
     */
    public static boolean isRunning() {
        return TASK_STATS.entrySet().stream().anyMatch(p -> p.getValue().get());
    }

    /**
     * Check if there are tasks waiting
     *
     * @return true if there are tasks waiting to be executed, otherwise false
     */
    @OnlyForTest
    public static boolean isEmpty() {
        return TASKS.entrySet().stream().allMatch(p -> p.getValue().isEmpty());
    }

    /**
     * using for test
     *
     * @throws InterruptedException
     */
    @OnlyForTest
    public static void waitingToFinished() throws InterruptedException {
        while (!isEmpty() || isRunning()) {
            Thread.sleep(1000);
        }
    }

    public abstract void process(long taskId, Partition partition, GeneratedMessageV3 data,
                                 Consumer<Integer> raftCompleteCallback);

    /**
     * Check if there is task data to be processed by this process
     *
     * @param instruct pd instruction
     * @return task metadata if the processor should handle, otherwise null
     */
    protected abstract GeneratedMessageV3 getTaskMeta(PartitionHeartbeatResponse instruct);

    /**
     * Determine whether to execute via thread pool (with blocking within the partition)
     *
     * @return true if execute in thread pool, false otherwise
     */
    protected boolean executeInBlockingMode() {
        return true;
    }

    /**
     * Whether a task is raft-task
     *
     * @return true if the task need to distributed to other followers
     */
    protected boolean isRaftTask() {
        return true;
    }

    /**
     * is the task exists
     *
     * @param taskId task id
     * @param partId partition id
     * @return true if exists, false otherwise
     */
    protected boolean preCheckTaskId(long taskId, int partId) {
        if (storeEngine.getPartitionEngine(partId) == null) {
            return false;
        }
        byte[] key = MetadataKeyHelper.getInstructionIdKey(taskId);
        var wrapper = storeEngine.getPartitionManager().getWrapper();
        byte[] value = wrapper.get(partId, key);
        if (value != null) {
            return true;
        }
        wrapper.put(partId, key, new byte[0]);
        return false;
    }

    /**
     * If leader, directly add and send raft task; otherwise redirect to leader
     *
     * @param partId  partition id
     * @param raftOp  raft operation
     * @param data    data
     * @param closure raft closure
     */
    protected void sendRaftTask(String graph, Integer partId, byte raftOp, Object data,
                                RaftClosure closure) {

        var partitionEngine = storeEngine.getPartitionEngine(partId);

        if (partitionEngine != null) {
            if (partitionEngine.isLeader()) {
                partitionEngine.addRaftTask(RaftOperation.create(raftOp, data), closure);
            } else {
                var request = new RedirectRaftTaskRequest(graph, partId, raftOp, data);
                var response = storeEngine.getHgCmdClient().redirectRaftTask(request);
                closure.run(response.getStatus().isOK() ? Status.OK() :
                            new Status(response.getStatus().getCode(),
                                       response.getStatus().getMsg()));
            }
        }
    }

    /**
     * 1. check if the processor should execute the instruction
     * 2. check if the task should be submitted to thread pool
     * 3. run in thread pool
     * 3.1: check whether where is a task in same partition executing
     * 3.2: process the instruction according to whether the task is raft task
     *
     * @param instruct pd instruction
     */
    public void executeInstruct(PartitionHeartbeatResponse instruct) {
        var meta = getTaskMeta(instruct);
        if (meta == null) {
            return;
        }

        var partition = new Partition(instruct.getPartition());
        if (!executeInBlockingMode()) {
            process(instruct.getId(), partition, meta, null);
        } else {
            // need to submit thread pool
            // checking prev execution state
            var partitionId = partition.getId();

            TASKS.computeIfAbsent(partitionId, k -> new LinkedBlockingDeque<>());
            TASK_STATS.computeIfAbsent(partitionId, k -> new AtomicBoolean(false));

            TASKS.get(partitionId).add(() -> {
                while (!TASK_STATS.get(partitionId).compareAndSet(false, true)) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        log.warn("interrupted: {}", e.getMessage());
                    }
                }

                if (isRaftTask()) {
                    var consumerWrapper = new Consumer<Integer>() {
                        @Override
                        public void accept(Integer integer) {
                            TASK_STATS.get(partitionId).set(false);
                            runNextTask(partitionId);
                        }
                    };
                    process(instruct.getId(), partition, meta, consumerWrapper);
                } else {
                    process(instruct.getId(), partition, meta, null);
                    TASK_STATS.get(partitionId).set(false);
                    runNextTask(partitionId);
                }
            });
            runNextTask(partitionId);
        }
    }

    private void runNextTask(int partitionId) {
        if (!TASK_STATS.get(partitionId).get()) {
            var task = TASKS.get(partitionId).poll();
            if (task != null) {
                threadPool.submit(task);
            }
        }
    }
}
