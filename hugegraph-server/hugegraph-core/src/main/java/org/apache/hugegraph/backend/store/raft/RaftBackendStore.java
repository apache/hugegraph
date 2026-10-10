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

package org.apache.hugegraph.backend.store.raft;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendFeatures;
import org.apache.hugegraph.backend.store.BackendMutation;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.backend.store.SystemSchemaStore;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.StoreAction;
import org.apache.hugegraph.backend.store.raft.rpc.RaftRequests.StoreType;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.util.BytesUtil;

public class RaftBackendStore implements BackendStore {

    private static final Logger LOG = Log.logger(RaftBackendStore.class);

    private final BackendStore store;
    private final RaftContext context;
    private final ThreadLocal<MutationBatch> mutationBatch;
    private final boolean isSafeRead;

    public RaftBackendStore(BackendStore store, RaftContext context) {
        this.store = store;
        this.context = context;
        this.mutationBatch = new ThreadLocal<>();
        this.isSafeRead = this.context.safeRead();
    }

    public BackendStore originStore() {
        return this.store;
    }

    private RaftNode node() {
        RaftNode node = this.context.node();
        E.checkState(node != null, "The raft node should be initialized first");
        return node;
    }

    @Override
    public String store() {
        return this.store.store();
    }

    @Override
    public String storedVersion() {
        return this.store.storedVersion();
    }

    @Override
    public String database() {
        return this.store.database();
    }

    @Override
    public BackendStoreProvider provider() {
        return this.store.provider();
    }

    @Override
    public SystemSchemaStore systemSchemaStore() {
        return this.store.systemSchemaStore();
    }

    @Override
    public boolean isSchemaStore() {
        return this.store.isSchemaStore();
    }

    @Override
    public synchronized void open(HugeConfig config) {
        this.store.open(config);
    }

    @Override
    public void close() {
        this.store.close();
    }

    @Override
    public boolean opened() {
        return this.store.opened();
    }

    @Override
    public void init() {
        // this.submitAndWait(StoreCommand.INIT);
        this.store.init();
    }

    @Override
    public void clear(boolean clearSpace) {
        byte value = clearSpace ? (byte) 1 : (byte) 0;
        byte[] bytes = StoreCommand.wrap(value);
        this.submitAndWait(StoreAction.CLEAR, bytes);
    }

    @Override
    public boolean initialized() {
        return this.store.initialized();
    }

    @Override
    public void truncate() {
        this.submitAndWait(StoreAction.TRUNCATE, null);
    }

    @Override
    public void mutate(BackendMutation mutation) {
        if (mutation.isEmpty()) {
            return;
        }
        // Just add to local buffer
        this.getOrNewBatch().add(mutation);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<BackendEntry> query(Query query) {
        return (Iterator<BackendEntry>)
                this.queryByRaft(query, o -> this.store.query(query));
    }

    @Override
    public Iterator<Iterator<BackendEntry>> query(Iterator<Query> queries,
                                                  Function<Query, Query> queryWriter,
                                                  HugeGraph hugeGraph) {
        throw new UnsupportedOperationException("unimplemented for RaftBackendStore");
    }

    @Override
    public Number queryNumber(Query query) {
        return (Number)
                this.queryByRaft(query, o -> this.store.queryNumber(query));
    }

    @Override
    public void beginTx() {
        // Don't write raft log, commitTx(in StateMachine) will call beginTx
    }

    @Override
    public void commitTx() {
        MutationBatch batch = this.getOrNewBatch();
        try {
            byte[] bytes = StoreSerializer.writeMutations(batch.mutations);
            this.submitAndWait(StoreAction.COMMIT_TX, bytes);
        } finally {
            batch.clear();
        }
    }

    @Override
    public void rollbackTx() {
        this.submitAndWait(StoreAction.ROLLBACK_TX, null);
    }

    @Override
    public <R> R metadata(HugeType type, String meta, Object[] args) {
        return this.store.metadata(type, meta, args);
    }

    @Override
    public BackendFeatures features() {
        return this.store.features();
    }

    @Override
    public void increaseCounter(HugeType type, long increment) {
        IncrCounter incrCounter = new IncrCounter(type, increment);
        byte[] bytes = StoreSerializer.writeIncrCounter(incrCounter);
        this.submitAndWait(StoreAction.INCR_COUNTER, bytes);
    }

    @Override
    public long getCounter(HugeType type) {
        Object counter = this.queryByRaft(type, true, o -> this.store.getCounter(type));
        assert counter instanceof Long;
        return (Long) counter;
    }

    private Object submitAndWait(StoreAction action, byte[] data) {
        StoreType type = this.context.storeType(this.store());
        return this.submitAndWait(new StoreCommand(type, action, data));
    }

    private Object submitAndWait(StoreCommand command) {
        RaftStoreClosure closure = new RaftStoreClosure(command);
        return this.node().submitAndWait(command, closure);
    }

    private Object queryByRaft(Object query, Function<Object, Object> func) {
        return this.queryByRaft(query, this.isSafeRead, func);
    }

    private Object queryByRaft(Object query, boolean safeRead,
                               Function<Object, Object> func) {
        if (!safeRead) {
            return func.apply(query);
        }

        RaftClosure<Object> future = new RaftClosure<>();
        ReadIndexClosure readIndexClosure = new ReadIndexClosure() {
            @Override
            public void run(Status status, long index, byte[] reqCtx) {
                if (status.isOk()) {
                    future.complete(status, () -> func.apply(query));
                } else {
                    future.failure(status, new BackendException(
                            "Failed to do raft read-index: %s",
                            status));
                }
            }
        };
        this.node().readIndex(BytesUtil.EMPTY_BYTES, readIndexClosure);
        try {
            return future.waitFinished();
        } catch (Throwable e) {
            LOG.warn("Failed to execute query '{}': {}",
                     query, future.status(), e);
            throw new BackendException("Failed to execute query: %s", e, query);
        }
    }

    private MutationBatch getOrNewBatch() {
        MutationBatch batch = this.mutationBatch.get();
        if (batch == null) {
            batch = new MutationBatch();
            this.mutationBatch.set(batch);
        }
        return batch;
    }

    private static class MutationBatch {

        // This object will stay in memory for a long time
        private final List<BackendMutation> mutations;

        public MutationBatch() {
            this.mutations = new ArrayList<>((int) Query.COMMIT_BATCH);
        }

        public void add(BackendMutation mutation) {
            this.mutations.add(mutation);
        }

        public void clear() {
            this.mutations.clear();
        }
    }

    protected static final class IncrCounter {

        private HugeType type;
        private long increment;

        public IncrCounter(HugeType type, long increment) {
            this.type = type;
            this.increment = increment;
        }

        public HugeType type() {
            return this.type;
        }

        public long increment() {
            return this.increment;
        }
    }
}
