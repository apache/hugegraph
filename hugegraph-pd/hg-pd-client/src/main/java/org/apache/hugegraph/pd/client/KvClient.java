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

package org.apache.hugegraph.pd.client;

import java.io.Closeable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Pdpb;
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
import org.apache.hugegraph.pd.grpc.kv.WatchEvent;
import org.apache.hugegraph.pd.grpc.kv.WatchKv;
import org.apache.hugegraph.pd.grpc.kv.WatchRequest;
import org.apache.hugegraph.pd.grpc.kv.WatchResponse;
import org.apache.hugegraph.pd.grpc.kv.WatchType;

import io.grpc.Status;
import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KvClient<T extends WatchResponse> extends AbstractClient implements Closeable {

    private static final long RECONNECT_DELAY_MS = 1000L;
    private static final Set<Status.Code> NON_RETRYABLE_WATCH_ERRORS =
            Set.of(Status.Code.CANCELLED,
                   Status.Code.INVALID_ARGUMENT,
                   Status.Code.NOT_FOUND,
                   Status.Code.ALREADY_EXISTS,
                   Status.Code.PERMISSION_DENIED,
                   Status.Code.FAILED_PRECONDITION,
                   Status.Code.OUT_OF_RANGE,
                   Status.Code.UNIMPLEMENTED,
                   Status.Code.DATA_LOSS,
                   Status.Code.UNAUTHENTICATED);

    private final AtomicLong lockClientId = new AtomicLong(0);
    private final AtomicLong watchClientId = new AtomicLong(0);
    private final Semaphore lockSemaphore = new Semaphore(1);
    private final Semaphore watchSemaphore = new Semaphore(1);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Set<WatchSubscription> subscriptions = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService reconnectExecutor;

    public KvClient(PDConfig pdConfig) {
        this(pdConfig, Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "pd-kv-watch-reconnect");
            thread.setDaemon(true);
            return thread;
        }));
    }

    KvClient(PDConfig pdConfig, ScheduledExecutorService reconnectExecutor) {
        super(pdConfig);
        this.reconnectExecutor = reconnectExecutor;
    }

    @Override
    protected AbstractStub createStub() {
        return KvServiceGrpc.newStub(channel);
    }

    @Override
    protected AbstractBlockingStub createBlockingStub() {
        return KvServiceGrpc.newBlockingStub(channel);
    }

    public KvResponse put(String key, String value) throws PDException {
        Kv kv = Kv.newBuilder().setKey(key).setValue(value).build();
        KvResponse response = blockingUnaryCall(KvServiceGrpc.getPutMethod(), kv);
        handleErrors(response.getHeader());
        return response;
    }

    public KResponse get(String key) throws PDException {
        K k = K.newBuilder().setKey(key).build();
        KResponse response = blockingUnaryCall(KvServiceGrpc.getGetMethod(), k);
        handleErrors(response.getHeader());
        return response;
    }

    public KvResponse delete(String key) throws PDException {
        K k = K.newBuilder().setKey(key).build();
        KvResponse response = blockingUnaryCall(KvServiceGrpc.getDeleteMethod(), k);
        handleErrors(response.getHeader());
        return response;
    }

    public KvResponse deletePrefix(String prefix) throws PDException {
        K k = K.newBuilder().setKey(prefix).build();
        KvResponse response = blockingUnaryCall(KvServiceGrpc.getDeletePrefixMethod(), k);
        handleErrors(response.getHeader());
        return response;
    }

    public ScanPrefixResponse scanPrefix(String prefix) throws PDException {
        K k = K.newBuilder().setKey(prefix).build();
        ScanPrefixResponse response = blockingUnaryCall(KvServiceGrpc.getScanPrefixMethod(), k);
        handleErrors(response.getHeader());
        return response;
    }

    public TTLResponse keepTTLAlive(String key) throws PDException {
        TTLRequest request = TTLRequest.newBuilder().setKey(key).build();
        TTLResponse response = blockingUnaryCall(KvServiceGrpc.getKeepTTLAliveMethod(), request);
        handleErrors(response.getHeader());
        return response;
    }

    public TTLResponse putTTL(String key, String value, long ttl) throws PDException {
        TTLRequest request =
                TTLRequest.newBuilder().setKey(key).setValue(value).setTtl(ttl).build();
        TTLResponse response = blockingUnaryCall(KvServiceGrpc.getPutTTLMethod(), request);
        handleErrors(response.getHeader());
        return response;
    }

    private void onEvent(WatchResponse value, Consumer<T> consumer) {
        log.debug("receive message for {},event Count:{}", value, value.getEventsCount());
        watchClientId.compareAndSet(0L, value.getClientId());
        if (value.getEventsCount() != 0) {
            try {
                consumer.accept((T) value);
            } catch (Exception e) {
                log.info(
                        "an error occurred while executing the client callback method, which " +
                        "should not " +
                        "have happened.Please check the callback method of the client", e);
            }
        }
    }

    private StreamObserver<WatchResponse> getObserver(WatchSubscription subscription) {
        return new StreamObserver<WatchResponse>() {
            @Override
            public void onNext(WatchResponse value) {
                if (subscription.observer.get() != this) {
                    return;
                }
                switch (value.getState()) {
                    case Starting:
                        boolean b = watchClientId.compareAndSet(0, value.getClientId());
                        if (b) {
                            log.info("set watch client id to :{}", value.getClientId());
                        }
                        release(watchSemaphore);
                        break;
                    case Started:
                        onEvent(value, subscription.consumer);
                        break;
                    case Leader_Changed:
                        requestReconnect(subscription, this);
                        break;
                    case Alive:
                        // only for check client is alive, do nothing
                        break;
                    default:
                        break;
                }
            }

            @Override
            public void onError(Throwable t) {
                if (isRetryableWatchError(t)) {
                    requestReconnect(subscription, this);
                } else {
                    stopWatch(subscription, this, t);
                }
            }

            @Override
            public void onCompleted() {
                requestReconnect(subscription, this);
            }
        };
    }

    public void listen(String key, Consumer<T> consumer) throws PDException {
        listen(key, consumer, false);
    }

    public void listenPrefix(String prefix, Consumer<T> consumer) throws PDException {
        listen(prefix, consumer, true);
    }

    private void listen(String key, Consumer<T> consumer, boolean prefix) throws PDException {
        WatchSubscription subscription = new WatchSubscription(key, consumer, prefix);
        subscriptions.add(subscription);
        try {
            if (!startWatch(subscription)) {
                throw new PDException(Pdpb.ErrorType.PD_UNREACHABLE_VALUE,
                                      "KvClient is closed");
            }
        } catch (PDException e) {
            subscription.observer.set(null);
            subscriptions.remove(subscription);
            throw e;
        }
    }

    private boolean startWatch(WatchSubscription subscription) throws PDException {
        if (closed.get()) {
            return false;
        }

        StreamObserver<WatchResponse> observer = getObserver(subscription);
        subscription.observer.set(observer);
        if (closed.get()) {
            subscription.observer.compareAndSet(observer, null);
            return false;
        }

        acquire(watchClientId, watchSemaphore);
        if (closed.get()) {
            subscription.observer.compareAndSet(observer, null);
            release(watchSemaphore);
            return false;
        }

        WatchRequest request = WatchRequest.newBuilder()
                                           .setClientId(watchClientId.get())
                                           .setKey(subscription.key)
                                           .build();
        try {
            if (subscription.prefix) {
                streamingCall(KvServiceGrpc.getWatchPrefixMethod(), request, observer, 1);
            } else {
                streamingCall(KvServiceGrpc.getWatchMethod(), request, observer, 1);
            }
            return true;
        } catch (Exception e) {
            release(watchSemaphore);
            if (e instanceof PDException) {
                throw (PDException) e;
            }
            throw new PDException(Pdpb.ErrorType.PD_UNREACHABLE_VALUE, e);
        }
    }

    private void requestReconnect(WatchSubscription subscription,
                                  StreamObserver<WatchResponse> sourceObserver) {
        if (closed.get() ||
            !subscription.observer.compareAndSet(sourceObserver, null)) {
            return;
        }
        watchClientId.set(0L);
        release(watchSemaphore);
        scheduleReconnect(subscription);
    }

    private static boolean isRetryableWatchError(Throwable throwable) {
        Status.Code code = Status.fromThrowable(throwable).getCode();
        return !NON_RETRYABLE_WATCH_ERRORS.contains(code);
    }

    private void stopWatch(WatchSubscription subscription,
                           StreamObserver<WatchResponse> sourceObserver,
                           Throwable throwable) {
        if (!subscription.observer.compareAndSet(sourceObserver, null)) {
            return;
        }
        release(watchSemaphore);
        subscriptions.remove(subscription);
        log.error("Watch for key {} stopped after a non-retryable error: {}",
                  subscription.key, Status.fromThrowable(throwable), throwable);
    }

    private void scheduleReconnect(WatchSubscription subscription) {
        if (closed.get() || !subscription.reconnectScheduled.compareAndSet(false, true)) {
            return;
        }
        try {
            reconnectExecutor.schedule(() -> reconnect(subscription), RECONNECT_DELAY_MS,
                                       TimeUnit.MILLISECONDS);
        } catch (RuntimeException e) {
            subscription.reconnectScheduled.set(false);
            if (!closed.get()) {
                log.warn("Failed to schedule watch reconnect for key {}", subscription.key, e);
            }
        }
    }

    private void reconnect(WatchSubscription subscription) {
        subscription.reconnectScheduled.set(false);
        if (closed.get()) {
            return;
        }
        try {
            startWatch(subscription);
        } catch (PDException e) {
            log.warn("Failed to reconnect watch for key {}", subscription.key, e);
            StreamObserver<WatchResponse> observer = subscription.observer.get();
            if (observer != null) {
                requestReconnect(subscription, observer);
            } else {
                scheduleReconnect(subscription);
            }
        }
    }

    private void acquire(AtomicLong clientId, Semaphore semaphore) {
        if (clientId.get() == 0L) {
            try {
                semaphore.acquire();
                if (clientId.get() != 0L) {
                    semaphore.release();
                }
                log.info("wait for client starting....");
            } catch (InterruptedException e) {
                log.error("get semaphore with error:", e);
            }
        }
    }

    private void release(Semaphore semaphore) {
        try {
            if (semaphore.availablePermits() == 0) {
                semaphore.release();
            }
        } catch (Exception e) {
            log.warn("release failed:", e);
        }
    }

    public List<String> getWatchList(T response) {
        List<String> values = new LinkedList<>();
        List<WatchEvent> eventsList = response.getEventsList();
        for (WatchEvent event : eventsList) {
            if (event.getType() != WatchType.Put) {
                return null;
            }
            String value = event.getCurrent().getValue();
            values.add(value);
        }
        return values;
    }

    public Map<String, String> getWatchMap(T response) {
        Map<String, String> values = new HashMap<>();
        List<WatchEvent> eventsList = response.getEventsList();
        for (WatchEvent event : eventsList) {
            if (event.getType() != WatchType.Put) {
                return null;
            }
            WatchKv current = event.getCurrent();
            String key = current.getKey();
            String value = current.getValue();
            values.put(key, value);
        }
        return values;
    }

    public LockResponse lock(String key, long ttl) throws PDException {
        acquire(lockClientId, lockSemaphore);
        LockResponse response;
        try {
            LockRequest k =
                    LockRequest.newBuilder().setKey(key).setClientId(lockClientId.get()).setTtl(ttl)
                               .build();
            response = blockingUnaryCall(KvServiceGrpc.getLockMethod(), k);
            handleErrors(response.getHeader());
            lockClientId.compareAndSet(0, response.getClientId());
        } catch (Exception e) {
            throw e;
        } finally {
            release(lockSemaphore);
        }
        return response;
    }

    public LockResponse lockWithoutReentrant(String key, long ttl) throws PDException {
        acquire(lockClientId, lockSemaphore);
        LockResponse response;
        try {
            LockRequest k =
                    LockRequest.newBuilder().setKey(key).setClientId(lockClientId.get()).setTtl(ttl)
                               .build();
            response = blockingUnaryCall(KvServiceGrpc.getLockWithoutReentrantMethod(), k);
            handleErrors(response.getHeader());
            lockClientId.compareAndSet(0, response.getClientId());
        } catch (Exception e) {
            throw e;
        } finally {
            release(lockSemaphore);
        }
        return response;
    }

    public LockResponse isLocked(String key) throws PDException {
        LockRequest k =
                LockRequest.newBuilder().setKey(key).setClientId(lockClientId.get()).build();
        LockResponse response = blockingUnaryCall(KvServiceGrpc.getIsLockedMethod(), k);
        handleErrors(response.getHeader());
        return response;
    }

    public LockResponse unlock(String key) throws PDException {
        assert lockClientId.get() != 0;
        LockRequest k =
                LockRequest.newBuilder().setKey(key).setClientId(lockClientId.get()).build();
        LockResponse response = blockingUnaryCall(KvServiceGrpc.getUnlockMethod(), k);
        handleErrors(response.getHeader());
        lockClientId.compareAndSet(0L, response.getClientId());
        assert lockClientId.get() == response.getClientId();
        return response;
    }

    public LockResponse keepAlive(String key) throws PDException {
        assert lockClientId.get() != 0;
        LockRequest k =
                LockRequest.newBuilder().setKey(key).setClientId(lockClientId.get()).build();
        LockResponse response = blockingUnaryCall(KvServiceGrpc.getKeepAliveMethod(), k);
        handleErrors(response.getHeader());
        lockClientId.compareAndSet(0L, response.getClientId());
        assert lockClientId.get() == response.getClientId();
        return response;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        reconnectExecutor.shutdownNow();
        release(lockSemaphore);
        release(watchSemaphore);
        for (WatchSubscription subscription : subscriptions) {
            try {
                StreamObserver<WatchResponse> observer =
                        subscription.observer.getAndSet(null);
                if (observer != null) {
                    observer.onCompleted();
                }
            } catch (Exception e) {
                log.warn("Failed to close watch for key {}", subscription.key, e);
            }
        }
        subscriptions.clear();
        super.close();
    }

    private final class WatchSubscription {

        private final String key;
        private final Consumer<T> consumer;
        private final boolean prefix;
        private final AtomicReference<StreamObserver<WatchResponse>> observer;
        private final AtomicBoolean reconnectScheduled;

        private WatchSubscription(String key, Consumer<T> consumer, boolean prefix) {
            this.key = key;
            this.consumer = consumer;
            this.prefix = prefix;
            this.observer = new AtomicReference<>();
            this.reconnectScheduled = new AtomicBoolean(false);
        }
    }
}
