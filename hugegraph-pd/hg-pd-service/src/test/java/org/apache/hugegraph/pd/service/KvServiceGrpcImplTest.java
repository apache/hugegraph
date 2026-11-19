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
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.pd.KvService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.kv.TxnOp;
import org.apache.hugegraph.pd.grpc.kv.TxnRequest;
import org.apache.hugegraph.pd.grpc.kv.TxnResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;

/**
 * The txn RPC of the KV service: who applies a txn, who forwards it, and what a failed apply
 * answers
 */
public class KvServiceGrpcImplTest {

    private final List<String> calls = new CopyOnWriteArrayList<>();
    private KvServiceGrpcImpl service;
    // What isLeader() answers, and what a redirect forwarded
    private volatile boolean leader = true;
    private volatile Object redirected;
    // Set to fail the next txn apply, and with deposedByTxn to lose the leadership first
    private volatile PDException txnFailure;
    private volatile boolean deposedByTxn;

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
        config.setRaft(raft);

        this.service = new KvServiceGrpcImpl() {
            @Override
            public boolean isLeader() {
                return KvServiceGrpcImplTest.this.leader;
            }

            @Override
            public <ReqT, RespT> void redirectToLeader(ManagedChannel channel,
                                                       MethodDescriptor<ReqT, RespT> method,
                                                       ReqT request,
                                                       StreamObserver<RespT> observer) {
                KvServiceGrpcImplTest.this.calls.add("redirect " + method.getBareMethodName());
                KvServiceGrpcImplTest.this.redirected = request;
            }
        };
        this.service.kvService = new KvService(config) {
            @Override
            public TxnResponse txn(TxnRequest request) throws PDException {
                KvServiceGrpcImplTest.this.calls.add("txn");
                PDException failure = KvServiceGrpcImplTest.this.txnFailure;
                if (failure != null) {
                    if (KvServiceGrpcImplTest.this.deposedByTxn) {
                        KvServiceGrpcImplTest.this.leader = false;
                    }
                    throw failure;
                }
                return TxnResponse.newBuilder().setSucceeded(true).build();
            }
        };
    }

    /**
     * Only the leader applies a txn, and a follower forwards it. A failed apply is answered
     * with its error, also when the node lost the leadership meanwhile: the entry may be on a
     * majority already, and a forward would let the next leader apply it twice.
     */
    @Test
    public void testTxnIsRedirectedToTheLeader() {
        TxnRequest request = TxnRequest.newBuilder().addOps(
                TxnOp.newBuilder().setKey("k").setType(TxnOp.Type.PUT).setValue("v")).build();
        this.leader = false;
        this.service.txn(request, new Capture<>());
        Assert.assertEquals(List.of("redirect txn"), this.calls);
        Assert.assertSame(request, this.redirected);

        for (boolean deposed : new boolean[]{true, false}) {
            this.calls.clear();
            this.redirected = null;
            this.leader = true;
            this.txnFailure = new PDException(Pdpb.ErrorType.NOT_LEADER_VALUE, "not leader");
            this.deposedByTxn = deposed;
            Capture<TxnResponse> failed = new Capture<>();
            this.service.txn(request, failed);
            Assert.assertEquals(List.of("txn"), this.calls);
            Assert.assertNull(this.redirected);
            Assert.assertFalse(failed.result().getSucceeded());
            Assert.assertEquals(Pdpb.ErrorType.NOT_LEADER,
                                failed.result().getHeader().getError().getType());
        }
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
