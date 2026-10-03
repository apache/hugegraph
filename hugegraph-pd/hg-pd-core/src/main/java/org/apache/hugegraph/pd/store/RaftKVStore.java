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

package org.apache.hugegraph.pd.store;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.raft.KVOperation;
import org.apache.hugegraph.pd.raft.KVStoreClosure;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.raft.RaftStateMachine;
import org.apache.hugegraph.pd.raft.RaftTaskHandler;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RaftKVStore implements HgKVStore, RaftTaskHandler {

    private final RaftEngine engine;
    private final HgKVStore store;

    public RaftKVStore(RaftEngine engine, HgKVStore store) {
        this.engine = engine;
        this.store = store;
    }

    @Override
    public void init(PDConfig config) {
        this.store.init(config);
        this.engine.addTaskHandler(this);
    }

    private BaseKVStoreClosure createClosure() {
        return new BaseKVStoreClosure() {
            @Override
            public void run(Status status) {
                if (!status.isOk()) {
                    log.error("An exception occurred while performing the RAFT,{}",
                              status.getErrorMsg());
                } else {
                    log.info("RAFT done!");
                }
            }
        };
    }

    @Override
    public void put(byte[] key, byte[] value) throws PDException {
        KVOperation operation = KVOperation.createPut(key, value);
        try {
            applyOperation(operation).get();
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.UNKNOWN_VALUE, e.getMessage());
        }
    }

    /**
     * Queries can be read without rafting
     */
    @Override
    public byte[] get(byte[] key) throws PDException {
        return store.get(key);

    }

    @Override
    public List<KV> scanPrefix(byte[] prefix) {
        return store.scanPrefix(prefix);
    }

    @Override
    public long remove(byte[] bytes) throws PDException {
        try {
            applyOperation(KVOperation.createRemove(bytes)).get();
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.UNKNOWN_VALUE, e.getMessage());
        }
        return 0;
    }

    @Override
    public long removeByPrefix(byte[] bytes) throws PDException {
        try {
            applyOperation(KVOperation.createRemoveByPrefix(bytes)).get();
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.UNKNOWN_VALUE, e.getMessage());
        }
        return 0;
    }

    @Override
    public void clear() throws PDException {
        try {
            applyOperation(KVOperation.createClear()).get();
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.UNKNOWN_VALUE, e.getMessage());
        }
    }

    @Override
    public void putWithTTL(byte[] key, byte[] value, long ttl) throws PDException {
        try {
            applyOperation(KVOperation.createPutWithTTL(key, value, ttl)).get();
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.UNKNOWN_VALUE, e.getMessage());
        }
    }

    @Override
    public void putWithTTL(byte[] key, byte[] value, long ttl, TimeUnit timeUnit) throws
                                                                                  PDException {
        try {
            applyOperation(KVOperation.createPutWithTTL(key, value, ttl, timeUnit)).get();
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.UNKNOWN_VALUE, e.getMessage());
        }
    }

    @Override
    public List<byte[]> getListWithTTL(byte[] key) throws PDException {
        return store.getListWithTTL(key);
    }

    @Override
    public byte[] getWithTTL(byte[] key) throws PDException {
        return store.getWithTTL(key);
    }

    @Override
    public void removeWithTTL(byte[] key) throws PDException {
        try {
            applyOperation(KVOperation.createRemoveWithTTL(key)).get();
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.UNKNOWN_VALUE, e.getMessage());
        }
    }

    @Override
    public void saveSnapshot(String snapshotPath) throws PDException {
        store.saveSnapshot(snapshotPath);
    }

    @Override
    public void loadSnapshot(String snapshotPath) throws PDException {
        store.loadSnapshot(snapshotPath);
    }

    @Override
    public List<KV> scanRange(byte[] start, byte[] end) {
        return store.scanRange(start, end);
    }

    @Override
    public void close() {
        store.close();
    }

    /**
     * Reads above are served from the local state, which on a new leader can trail the
     * entries its predecessor committed; this waits for them to be applied
     */
    @Override
    public void waitReadIndex() throws PDException {
        this.engine.waitReadIndex();
    }

    /**
     * Need to walk the real operation of Raft
     */
    private void doPut(byte[] key, byte[] value) throws PDException {

        store.put(key, value);
    }

    public long doRemove(byte[] bytes) throws PDException {
        return this.store.remove(bytes);
    }

    public long doRemoveByPrefix(byte[] bytes) throws PDException {
        return this.store.removeByPrefix(bytes);
    }

    public void doRemoveWithTTL(byte[] key) throws PDException {
        this.store.removeWithTTL(key);
    }

    public void doClear() throws PDException {
        this.store.clear();
    }

    public void doPutWithTTL(byte[] key, byte[] value, long ttl) throws PDException {
        this.store.putWithTTL(key, value, ttl);
    }

    public void doPutWithTTL(byte[] key, byte[] value, long ttl, TimeUnit timeUnit) throws
                                                                                    PDException {
        this.store.putWithTTL(key, value, ttl, timeUnit);
    }

    public void doSaveSnapshot(String snapshotPath) throws PDException {
        this.store.saveSnapshot(snapshotPath);
    }

    public void doLoadSnapshot(String snapshotPath) throws PDException {
        this.store.loadSnapshot(snapshotPath);
    }

    private <T> CompletableFuture<T> applyOperation(final KVOperation op) throws PDException {
        CompletableFuture<T> future = new CompletableFuture<>();
        try {
            final Task task = new Task();
            task.setData(ByteBuffer.wrap(op.toByteArray()));
            task.setDone(new RaftStateMachine.RaftClosureAdapter(op, new KVStoreClosure() {
                Object data;
                Pdpb.Error error;

                @Override
                public Pdpb.Error getError() {
                    return error;
                }

                @Override
                public void setError(Pdpb.Error error) {
                    this.error = error;
                }

                @Override
                public Object getData() {
                    return data;
                }

                @Override
                public void setData(Object data) {
                    this.data = data;
                }

                @Override
                public void run(Status status) {
                    if (status.isOk()) {
                        future.complete((T) data);
                    } else {
                        RaftError raftError = status.getRaftError();
                        Pdpb.ErrorType type;
                        if (RaftError.EPERM.equals(raftError)) {
                            type = Pdpb.ErrorType.NOT_LEADER;
                        } else {
                            type = Pdpb.ErrorType.UNKNOWN;
                        }
                        error = Pdpb.Error.newBuilder().setType(type)
                                          .setMessage(status.getErrorMsg())
                                          .build();
                        future.completeExceptionally(
                                new PDException(error.getTypeValue()));
                    }
                }
            }));
            this.engine.addTask(task);
            return future;
        } catch (Exception e) {
            future.completeExceptionally(e);
            return future;
        }
    }

    private boolean isLeader() {
        return this.engine.isLeader();
    }

    @Override
    public boolean invoke(KVOperation op, KVStoreClosure response) throws PDException {
        switch (op.getOp()) {
            case KVOperation.GET:
                break;
            case KVOperation.PUT:
                doPut(op.getKey(), op.getValue());
                break;
            case KVOperation.REMOVE:
                doRemove(op.getKey());
                break;
            case KVOperation.PUT_WITH_TTL:
                doPutWithTTL(op.getKey(), op.getValue(), (long) op.getArg());
                break;
            case KVOperation.PUT_WITH_TTL_UNIT:
                Object[] arg = (Object[]) op.getArg();
                doPutWithTTL(op.getKey(), op.getValue(), (long) arg[0], (TimeUnit) arg[1]);
                break;
            case KVOperation.REMOVE_BY_PREFIX:
                doRemoveByPrefix(op.getKey());
                break;
            case KVOperation.REMOVE_WITH_TTL:
                doRemoveWithTTL(op.getKey());
                break;
            case KVOperation.CLEAR:
                doClear();
                break;
            case KVOperation.SAVE_SNAPSHOT:
                doSaveSnapshot((String) op.getAttach());
                break;
            case KVOperation.LOAD_SNAPSHOT:
                doLoadSnapshot((String) op.getAttach());
                break;
            default:
                log.error("Err op {}", op.getOp());
        }
        return false;
    }
}
