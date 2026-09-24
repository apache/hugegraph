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

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;

import org.apache.hugegraph.pd.KvService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.kv.K;
import org.apache.hugegraph.pd.grpc.kv.KResponse;
import org.apache.hugegraph.pd.grpc.kv.Kv;
import org.apache.hugegraph.pd.grpc.kv.KvResponse;
import org.apache.hugegraph.pd.grpc.kv.KvServiceGrpc;
import org.apache.hugegraph.pd.grpc.kv.LockRequest;
import org.apache.hugegraph.pd.grpc.kv.LockResponse;
import org.apache.hugegraph.pd.grpc.kv.ScanPrefixResponse;
import org.apache.hugegraph.pd.grpc.kv.TTLRequest;
import org.apache.hugegraph.pd.grpc.kv.TTLResponse;
import org.apache.hugegraph.pd.grpc.kv.WatchKv;
import org.apache.hugegraph.pd.grpc.kv.WatchRequest;
import org.apache.hugegraph.pd.grpc.kv.WatchResponse;
import org.apache.hugegraph.pd.grpc.kv.WatchState;
import org.apache.hugegraph.pd.grpc.kv.WatchType;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.raft.RaftStateListener;
import org.apache.hugegraph.pd.watch.KvWatchSubject;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * The core implementation class of KV storage
 */
@Slf4j
@GRpcService
public class KvServiceGrpcImpl extends KvServiceGrpc.KvServiceImplBase implements RaftStateListener,
                                                                                  ServiceGrpc {

    private ManagedChannel channel = null;
    KvService kvService;
    AtomicLong count = new AtomicLong();
    String msg = "node is not leader,it is necessary to  redirect to the leader on the client";
    @Autowired
    private PDConfig pdConfig;
    private KvWatchSubject subjects;
    private ScheduledExecutorService executor;

    @PostConstruct
    public void init() {
        RaftEngine.getInstance().init(pdConfig.getRaft());
        RaftEngine.getInstance().addStateListener(this);
        kvService = new KvService(pdConfig);
        subjects = new KvWatchSubject(pdConfig);
        executor = Executors.newScheduledThreadPool(1);
        executor.scheduleWithFixedDelay(() -> {
            if (isLeader()) {
                subjects.keepClientAlive();
            }
        }, 0, KvWatchSubject.WATCH_TTL * 1 / 3, TimeUnit.MILLISECONDS);
    }

    /**
     * Ordinary put
     *
     * @param request
     * @param responseObserver
     */
    public void put(Kv request, StreamObserver<KvResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(channel, KvServiceGrpc.getPutMethod(), request, responseObserver);
            return;
        }
        KvResponse response;
        KvResponse.Builder builder = KvResponse.newBuilder();
        try {
            String key = request.getKey();
            String value = request.getValue();
            this.kvService.put(key, value);
            WatchKv watchKV = getWatchKv(key, value);
            subjects.notifyAllObserver(key, WatchType.Put, new WatchKv[]{watchKV});
            response = builder.setHeader(getResponseHeader()).build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getPutMethod(), request, responseObserver);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Ordinary get
     *
     * @param request
     * @param responseObserver
     */
    public void get(K request, StreamObserver<KResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(channel, KvServiceGrpc.getGetMethod(), request, responseObserver);
            return;
        }
        KResponse response;
        KResponse.Builder builder = KResponse.newBuilder();
        try {
            String value = this.kvService.get(request.getKey());
            builder.setHeader(getResponseHeader());
            if (value != null) {
                builder.setValue(value);
            }
            response = builder.build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getGetMethod(), request, responseObserver);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Ordinary delete
     *
     * @param request
     * @param responseObserver
     */
    public void delete(K request, StreamObserver<KvResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(channel, KvServiceGrpc.getDeleteMethod(), request, responseObserver);
            return;
        }
        KvResponse response;
        KvResponse.Builder builder = KvResponse.newBuilder();
        try {
            String key = request.getKey();
            Kv deleted = this.kvService.delete(key);
            if (deleted.getValue() != null) {
                WatchKv watchKV = getWatchKv(deleted.getKey(), deleted.getValue());
                subjects.notifyAllObserver(key, WatchType.Delete, new WatchKv[]{watchKV});
            }
            response = builder.setHeader(getResponseHeader()).build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getDeleteMethod(), request,
                                 responseObserver);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Delete by prefix
     *
     * @param request
     * @param responseObserver
     */
    public void deletePrefix(K request, StreamObserver<KvResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(channel, KvServiceGrpc.getDeletePrefixMethod(), request,
                             responseObserver);
            return;
        }
        KvResponse response;
        KvResponse.Builder builder = KvResponse.newBuilder();
        try {
            String key = request.getKey();
            List<Kv> kvs = this.kvService.deleteWithPrefix(key);
            WatchKv[] watchKvs = new WatchKv[kvs.size()];
            int i = 0;
            for (Kv kv : kvs) {
                WatchKv watchKV = getWatchKv(kv.getKey(), kv.getValue());
                watchKvs[i++] = watchKV;
            }
            subjects.notifyAllObserver(key, WatchType.Delete, watchKvs);
            response = builder.setHeader(getResponseHeader()).build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getDeletePrefixMethod(), request,
                                 responseObserver);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Search by prefix
     *
     * @param request
     * @param responseObserver
     */
    public void scanPrefix(K request, StreamObserver<ScanPrefixResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(channel, KvServiceGrpc.getScanPrefixMethod(), request,
                             responseObserver);
            return;
        }
        ScanPrefixResponse response;
        ScanPrefixResponse.Builder builder = ScanPrefixResponse.newBuilder();
        try {
            Map kvs = this.kvService.scanWithPrefix(request.getKey());
            response = builder.setHeader(getResponseHeader()).putAllKvs(kvs).build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getScanPrefixMethod(), request,
                                 responseObserver);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Obtain a random non-0 string as an Id
     *
     * @return
     */
    private long getRandomLong() {

        long result;
        Random random = new Random();
        while ((result = random.nextLong()) == 0) {
            continue;
        }
        return result;
    }

    /**
     * Ordinary watch
     *
     * @param request
     * @param responseObserver
     */
    public void watch(WatchRequest request, StreamObserver<WatchResponse> responseObserver) {
        if (!isLeader()) {
            responseObserver.onError(new PDException(-1, msg));
            return;
        }
        try {
            clientWatch(request, responseObserver, false);
        } catch (PDException e) {
            if (!isLeader()) {
                try {
                    responseObserver.onError(new PDException(-1, msg));
                    return;
                } catch (IllegalStateException ie) {

                } catch (Exception e1) {
                    log.error("redirect with error: ", e1);
                }
            }
        }
    }

    /**
     * Ordinary prefix watch
     *
     * @param request
     * @param responseObserver
     */
    public void watchPrefix(WatchRequest request, StreamObserver<WatchResponse> responseObserver) {
        if (!isLeader()) {
            responseObserver.onError(new PDException(-1, msg));
            return;
        }
        try {
            clientWatch(request, responseObserver, true);
        } catch (PDException e) {
            if (!isLeader()) {
                try {
                    responseObserver.onError(new PDException(-1, msg));
                    return;
                } catch (IllegalStateException ie) {

                } catch (Exception e1) {
                    log.error("redirect with error: ", e1);
                }
            }
        }
    }

    /**
     * A generic approach to the above two methods
     *
     * @param request
     * @param responseObserver
     * @param isPrefix
     * @throws PDException
     */
    private void clientWatch(WatchRequest request, StreamObserver<WatchResponse> responseObserver,
                             boolean isPrefix) throws PDException {
        try {
            String key = request.getKey();
            long clientId = request.getClientId();
            WatchResponse.Builder builder = WatchResponse.newBuilder();
            WatchResponse response;
            if (request.getState().equals(WatchState.Starting) && clientId == 0) {
                clientId = getRandomLong();
                response = builder.setClientId(clientId).setState(WatchState.Starting).build();
            } else {
                response = builder.setState(WatchState.Started).build();
            }
            String delimiter =
                    isPrefix ? KvWatchSubject.PREFIX_DELIMITER : KvWatchSubject.KEY_DELIMITER;
            subjects.addObserver(key, clientId, responseObserver, delimiter);
            synchronized (responseObserver) {
                responseObserver.onNext(response);
            }
        } catch (PDException e) {
            if (!isLeader()) {
                throw new PDException(-1, msg);
            }
            throw new PDException(e.getErrorCode(), e);
        }

    }

    /**
     * Locking
     *
     * @param request
     * @param responseObserver
     */
    public void lock(LockRequest request, StreamObserver<LockResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(channel, KvServiceGrpc.getLockMethod(), request, responseObserver);
            return;
        }
        LockResponse response;
        LockResponse.Builder builder = LockResponse.newBuilder();
        try {
            long clientId = request.getClientId();
            if (clientId == 0) {
                clientId = getRandomLong();
            }
            boolean locked = this.kvService.lock(request.getKey(), request.getTtl(), clientId);
            response =
                    builder.setHeader(getResponseHeader()).setSucceed(locked).setClientId(clientId)
                           .build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getLockMethod(), request, responseObserver);
                return;
            }
            log.error("lock with error :", e);
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public void lockWithoutReentrant(LockRequest request,
                                     StreamObserver<LockResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(channel, KvServiceGrpc.getLockWithoutReentrantMethod(), request,
                             responseObserver);
            return;
        }
        LockResponse response;
        LockResponse.Builder builder = LockResponse.newBuilder();
        try {
            long clientId = request.getClientId();
            if (clientId == 0) {
                clientId = getRandomLong();
            }
            boolean locked = this.kvService.lockWithoutReentrant(request.getKey(), request.getTtl(),
                                                                 clientId);
            response =
                    builder.setHeader(getResponseHeader()).setSucceed(locked).setClientId(clientId)
                           .build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getLockWithoutReentrantMethod(), request,
                                 responseObserver);
                return;
            }
            log.error("lock with error :", e);
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public void isLocked(LockRequest request, StreamObserver<LockResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(channel, KvServiceGrpc.getIsLockedMethod(), request, responseObserver);
            return;
        }
        LockResponse response;
        LockResponse.Builder builder = LockResponse.newBuilder();
        try {
            boolean locked = this.kvService.locked(request.getKey());
            response = builder.setHeader(getResponseHeader()).setSucceed(locked).build();
        } catch (PDException e) {
            log.error("lock with error :", e);
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getIsLockedMethod(), request,
                                 responseObserver);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Unlock
     *
     * @param request
     * @param responseObserver
     */
    public void unlock(LockRequest request, StreamObserver<LockResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(channel, KvServiceGrpc.getUnlockMethod(), request, responseObserver);
            return;
        }
        LockResponse response;
        LockResponse.Builder builder = LockResponse.newBuilder();
        try {
            long clientId = request.getClientId();
            if (clientId == 0) {
                throw new PDException(-1, "incorrect clientId: 0");
            }
            boolean unlocked = this.kvService.unlock(request.getKey(), clientId);
            response = builder.setHeader(getResponseHeader()).setSucceed(unlocked)
                              .setClientId(clientId).build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getUnlockMethod(), request,
                                 responseObserver);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Lock renewal
     *
     * @param request
     * @param responseObserver
     */
    public void keepAlive(LockRequest request, StreamObserver<LockResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(channel, KvServiceGrpc.getKeepAliveMethod(), request,
                             responseObserver);
            return;
        }
        LockResponse response;
        LockResponse.Builder builder = LockResponse.newBuilder();
        try {
            long clientId = request.getClientId();
            if (clientId == 0) {
                throw new PDException(-1, "incorrect clientId: 0");
            }
            boolean alive = this.kvService.keepAlive(request.getKey(), clientId);
            response =
                    builder.setHeader(getResponseHeader()).setSucceed(alive).setClientId(clientId)
                           .build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getKeepAliveMethod(), request,
                                 responseObserver);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * PUT with timeout
     *
     * @param request
     * @param responseObserver
     */
    public void putTTL(TTLRequest request, StreamObserver<TTLResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(channel, KvServiceGrpc.getPutTTLMethod(), request, responseObserver);
            return;
        }
        TTLResponse response;
        TTLResponse.Builder builder = TTLResponse.newBuilder();
        try {
            this.kvService.put(request.getKey(), request.getValue(), request.getTtl());
            response = builder.setHeader(getResponseHeader()).setSucceed(true).build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getPutTTLMethod(), request,
                                 responseObserver);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Reactivate the key with a timeout period
     *
     * @param request
     * @param responseObserver
     */
    public void keepTTLAlive(TTLRequest request, StreamObserver<TTLResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(channel, KvServiceGrpc.getKeepTTLAliveMethod(), request,
                             responseObserver);
            return;
        }
        TTLResponse response;
        TTLResponse.Builder builder = TTLResponse.newBuilder();
        try {
            this.kvService.keepAlive(request.getKey());
            response = builder.setHeader(getResponseHeader()).setSucceed(true).build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getKeepTTLAliveMethod(), request,
                                 responseObserver);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private WatchKv getWatchKv(String key, String value) {
        WatchKv kv = WatchKv.newBuilder().setKey(key).setValue(value).build();
        return kv;
    }

    @Override
    public void onRaftLeaderChanged() {
        subjects.notifyClientChangeLeader();
    }
}
