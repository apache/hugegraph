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

package org.apache.hugegraph.pd.service;

import java.io.File;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.pd.KvService;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.kv.K;
import org.apache.hugegraph.pd.grpc.kv.KResponse;
import org.apache.hugegraph.pd.grpc.kv.ScanPrefixResponse;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.error.RaftError;

import io.grpc.stub.StreamObserver;

/**
 * get and scanPrefix read the local store of the leader. A leader elected a moment ago may not
 * have applied every committed write, so both wait for a ReadIndex first, and return an error
 * rather than a stored value when it fails. The raft node answers each ReadIndex the way
 * jraft does: it sets the result and runs the closure.
 */
public class KvServiceGrpcImplTest {

    private final List<String> calls = new CopyOnWriteArrayList<>();
    private volatile Status readIndex = Status.OK();
    private Node originalRaftNode;
    private PDConfig.Raft originalRaftConfig;
    private KvServiceGrpcImpl service;

    @Before
    public void setUp() throws Exception {
        // The store is opened once per JVM, in the directory of the first test that makes a
        // KvService, and stays open: the directory has to outlive the test
        File dataPath = Files.createTempDirectory("kv-service-grpc").toFile();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> FileUtils.deleteQuietly(dataPath)));
        PDConfig config = new PDConfig();
        config.setDataPath(dataPath.getAbsolutePath());
        PDConfig.Raft raft = config.new Raft();
        raft.setEnable(false);
        // Bounds the wait for the ReadIndex: one that times out is reported at this deadline
        raft.setRpcTimeout(300);
        config.setRaft(raft);

        RaftEngine engine = RaftEngine.getInstance();
        this.originalRaftNode = engine.getRaftNode();
        this.originalRaftConfig = Whitebox.getInternalState(engine, "config");
        Whitebox.setInternalState(engine, "raftNode", leaderNode());
        Whitebox.setInternalState(engine, "config", raft);

        this.service = new KvServiceGrpcImpl() {
            @Override
            public boolean isLeader() {
                return true;
            }
        };
        // The reads alone: what they return is what the store holds when they run
        this.service.kvService = new KvService(config) {
            @Override
            public String get(String key) {
                KvServiceGrpcImplTest.this.calls.add("get");
                return "stored";
            }

            @Override
            public Map<String, String> scanWithPrefix(String key) {
                KvServiceGrpcImplTest.this.calls.add("scan");
                return Map.of(key + "a", "stored");
            }
        };
    }

    @After
    public void tearDown() {
        RaftEngine engine = RaftEngine.getInstance();
        Whitebox.setInternalState(engine, "raftNode", this.originalRaftNode);
        Whitebox.setInternalState(engine, "config", this.originalRaftConfig);
    }

    @Test
    public void testGetAndScanReadAfterTheReadIndex() {
        KResponse got = get("k");
        Assert.assertEquals(Pdpb.ErrorType.OK, got.getHeader().getError().getType());
        Assert.assertEquals("stored", got.getValue());
        Assert.assertEquals(List.of("readIndex", "get"), this.calls);

        this.calls.clear();
        ScanPrefixResponse scanned = scan("k/");
        Assert.assertEquals(Pdpb.ErrorType.OK, scanned.getHeader().getError().getType());
        Assert.assertEquals(Map.of("k/a", "stored"), scanned.getKvsMap());
        Assert.assertEquals(List.of("readIndex", "scan"), this.calls);
    }

    @Test
    public void testFailedReadIndexReturnsNoStoredValue() {
        // As on a leader that cannot confirm its log with a quorum
        this.readIndex = new Status(RaftError.ETIMEDOUT, "no quorum");

        KResponse got = get("k");
        Assert.assertEquals(Pdpb.ErrorType.UNKNOWN, got.getHeader().getError().getType());
        Assert.assertEquals("", got.getValue());

        ScanPrefixResponse scanned = scan("k/");
        Assert.assertEquals(Pdpb.ErrorType.UNKNOWN, scanned.getHeader().getError().getType());
        Assert.assertTrue(scanned.getKvsMap().isEmpty());
        // The store was not read at all
        Assert.assertEquals(List.of("readIndex", "readIndex"), this.calls);
    }

    private KResponse get(String key) {
        Capture<KResponse> observer = new Capture<>();
        this.service.get(K.newBuilder().setKey(key).build(), observer);
        return observer.result();
    }

    private ScanPrefixResponse scan(String prefix) {
        Capture<ScanPrefixResponse> observer = new Capture<>();
        this.service.scanPrefix(K.newBuilder().setKey(prefix).build(), observer);
        return observer.result();
    }

    private Node leaderNode() {
        return (Node) Proxy.newProxyInstance(
                Node.class.getClassLoader(), new Class<?>[]{Node.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "isLeader":
                            return true;
                        case "readIndex":
                            this.calls.add("readIndex");
                            Status status = this.readIndex;
                            ReadIndexClosure closure = (ReadIndexClosure) args[args.length - 1];
                            closure.setResult(status.isOk() ? 42L :
                                              ReadIndexClosure.INVALID_LOG_INDEX, new byte[0]);
                            closure.run(status);
                            return null;
                        case "toString":
                            return "leader raft node";
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        case "equals":
                            return proxy == args[0];
                        default:
                            throw new AssertionError("unexpected call " + method.getName());
                    }
                });
    }

    private static final class Capture<T> implements StreamObserver<T> {

        private T value;
        private boolean completed;

        @Override
        public void onNext(T value) {
            this.value = value;
        }

        @Override
        public void onError(Throwable t) {
            throw new AssertionError("unexpected stream error", t);
        }

        @Override
        public void onCompleted() {
            this.completed = true;
        }

        private T result() {
            Assert.assertTrue(this.completed);
            return this.value;
        }
    }
}
