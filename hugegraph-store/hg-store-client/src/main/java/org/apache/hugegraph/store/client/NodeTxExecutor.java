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

package org.apache.hugegraph.store.client;

import static org.apache.hugegraph.store.client.util.HgStoreClientConst.EMPTY_LIST;
import static org.apache.hugegraph.store.client.util.HgStoreClientConst.NODE_MAX_RETRYING_TIMES;
import static org.apache.hugegraph.store.client.util.HgStoreClientConst.TX_SESSIONS_MAP_CAPACITY;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.client.type.HgStoreClientException;
import org.apache.hugegraph.store.client.util.HgAssert;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;
import org.apache.hugegraph.store.term.HgPair;
import org.apache.hugegraph.store.term.HgTriple;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;

/**
 * 2021/11/18
 */
@Slf4j
@NotThreadSafe
final class NodeTxExecutor {

    private static final String maxTryMsg =
            "the number of retries reached the upper limit : " + NODE_MAX_RETRYING_TIMES +
            ",caused by:";

    static {
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism",
                           String.valueOf(Runtime.getRuntime().availableProcessors() * 2));
    }

    private final String graphName;
    NodeTxSessionProxy proxy;
    Collector<NodeTkv, ?, Map<Long, List<HgOwnerKey>>> collector = Collectors.groupingBy(
            nkv -> nkv.getNodeId(), Collectors.mapping(NodeTkv::getKey, Collectors.toList()));
    private Map<Long, HgStoreSession> sessions = new HashMap<>(TX_SESSIONS_MAP_CAPACITY, 1);
    private boolean isTx;
    private List<HgPair<HgTriple<String, HgOwnerKey, Object>,
            Function<NodeTkv, Boolean>>> entries = new LinkedList<>();

    private NodeTxExecutor(String graphName, NodeTxSessionProxy proxy) {
        this.graphName = graphName;
        this.proxy = proxy;
    }

    static NodeTxExecutor graphOf(String graphName, NodeTxSessionProxy proxy) {
        return new NodeTxExecutor(graphName, proxy);
    }

    public boolean isTx() {
        return isTx;
    }

    void setTx(boolean tx) {
        isTx = tx;
    }

    void commitTx() {
        if (!this.isTx) {
            throw new IllegalStateException("It's not in tx state");
        }

        this.doCommit();
    }

    void rollbackTx() {
        if (!this.isTx) {
            return;
        }
        try {
            this.sessions.values().stream().filter(HgStoreSession::isTx)
                         .forEach(HgStoreSession::rollback);
        } catch (Throwable t) {
            throw t;
        } finally {
            this.isTx = false;
            this.sessions.clear();
        }
    }

    void doCommit() {
        try {
            this.retryingInvoke(() -> {
                if (this.entries.isEmpty()) {
                    return true;
                }
                for (HgPair<HgTriple<String, HgOwnerKey, Object>, Function<NodeTkv, Boolean>> e :
                        this.entries) {
                    doAction(e.getKey(), e.getValue());
                }
                this.commitSessions(this.sessions.values());
                return true;
            });

        } catch (Throwable t) {
            throw t;
        } finally {
            this.isTx = false;
            this.entries = new LinkedList<>();
            this.sessions = new HashMap<>(TX_SESSIONS_MAP_CAPACITY, 1);
        }
    }

    // private Function<HgTriple<String, HgOwnerKey, Object>,
    //        List<HgPair<HgStoreNode, NodeTkv>>> nodeStreamWrapper = nodeParams -> {
    //    if (nodeParams.getZ() == null) {
    //        return this.proxy.getNode(nodeParams.getX(),
    //                                  nodeParams.getY());
    //    } else {
    //        if (nodeParams.getZ() instanceof HgOwnerKey) {
    //            return this.proxy.getNode(nodeParams.getX(),
    //                                      nodeParams.getY(),
    //                                      (HgOwnerKey) nodeParams.getZ());
    //        } if ( nodeParams.getZ() instanceof Integer ){
    //            return this.proxy.doPartition(nodeParams.getX(), (Integer) nodeParams.getZ())
    //                             .stream()
    //                             .map(e -> new NodeTkv(e, nodeParams.getX(), nodeParams.getY(),
    //                             nodeParams.getY()
    //                             .getKeyCode()))
    //                             .map(
    //                                     e -> new HgPair<>(this.proxy.getStoreNode(e.getNodeId
    //                                     ()), e)
    //                                 );
    //        }else {
    //            HgAssert.isTrue(nodeParams.getZ() instanceof byte[],
    //                            "Illegal parameter to get node id");
    //            throw new NotImplementedException();
    //        }
    //    }
    // };

    // private Function<HgTriple<String, HgOwnerKey, Object>,
    //        List<HgPair<HgStoreNode, NodeTkv>>> nodeStreamWrapper = nodeParams -> {
    //    if (nodeParams.getZ() == null) {
    //        return this.proxy.getNode(nodeParams.getX(), nodeParams.getY());
    //    } else {
    //        if (nodeParams.getZ() instanceof HgOwnerKey) {
    //            return this.proxy.getNode(nodeParams.getX(), nodeParams.getY(),
    //                                      (HgOwnerKey) nodeParams.getZ());
    //        }
    //        if (nodeParams.getZ() instanceof Integer) {
    //            Collection<HgNodePartition> nodePartitions = this.proxy.doPartition(nodeParams
    //            .getX(),
    //                                                                                (Integer)
    //                                                                                nodeParams
    //                                                                                .getZ());
    //            ArrayList<HgPair<HgStoreNode, NodeTkv>> hgPairs = new ArrayList<>
    //            (nodePartitions.size());
    //            for (HgNodePartition nodePartition : nodePartitions) {
    //                NodeTkv nodeTkv = new NodeTkv(nodePartition, nodeParams.getX(), nodeParams
    //                .getY(),
    //                                              nodeParams.getY().getKeyCode());
    //                hgPairs.add(new HgPair<>(this.proxy.getStoreNode(nodeTkv.getNodeId()),
    //                nodeTkv));
    //
    //            }
    //            return hgPairs;
    //        } else {
    //            HgAssert.isTrue(nodeParams.getZ() instanceof byte[], "Illegal parameter to get
    //            node id");
    //            throw new RuntimeException("not implemented");
    //        }
    //    }
    // };

    /**
     * Commit every tx session in parallel. When at least one commit fails, roll back (in tx
     * mode) and throw one exception that carries ALL failures: the first one as the cause, the
     * others as suppressed. The retry loop looks at all of them, so whether a commit is retried
     * no longer depends on which partition happened to fail first.
     */
    void commitSessions(Collection<HgStoreSession> sessions) {
        Queue<Throwable> failures = new ConcurrentLinkedQueue<>();
        sessions.parallelStream().forEach(e -> {
            if (e.isTx()) {
                try {
                    e.commit();
                } catch (Throwable t) {
                    failures.add(t);
                }
            }
        });
        if (failures.isEmpty()) {
            return;
        }
        if (isTx) {
            try {
                sessions.stream().forEach(HgStoreSession::rollback);
            } catch (Exception e) {
                // keep the commit failure as the reported one
            }
        }
        throw aggregate(failures);
    }

    static HgStoreClientException aggregate(Collection<Throwable> failures) {
        Iterator<Throwable> it = failures.iterator();
        Throwable first = it.next();
        Throwable cause = first.getCause() != null ? first.getCause() : first;
        HgStoreClientException result = cause instanceof HgStoreClientException ?
                                        (HgStoreClientException) cause :
                                        HgStoreClientException.of(cause);
        while (it.hasNext()) {
            Throwable other = it.next();
            if (other != result && other != cause) {
                result.addSuppressed(other);
            }
        }
        return result;
    }

    private boolean doAction(HgTriple<String, HgOwnerKey, Object> nodeParams,
                             Function<NodeTkv, Boolean> action) {
        if (nodeParams.getZ() == null) {
            return this.proxy.doAction(nodeParams.getX(), nodeParams.getY(), nodeParams.getY(),
                                       action);
        } else {
            if (nodeParams.getZ() instanceof HgOwnerKey) {
                boolean result = this.proxy.doAction(nodeParams.getX(), nodeParams.getY(),
                                                     (HgOwnerKey) nodeParams.getZ(), action);
                return result;
            }
            if (nodeParams.getZ() instanceof Integer) {
                return this.proxy.doAction(nodeParams.getX(), nodeParams.getY(),
                                           (Integer) nodeParams.getZ(), action);
            } else {
                HgAssert.isTrue(nodeParams.getZ() instanceof byte[],
                                "Illegal parameter to get node id");
                throw new RuntimeException("not implemented");
            }
        }
    }

    boolean prepareTx(HgTriple<String, HgOwnerKey, Object> nodeParams,
                      Function<NodeTkv, Boolean> sessionMapper) {
        if (this.isTx) {
            return this.entries.add(new HgPair(nodeParams, sessionMapper));
        } else {
            return this.isAllTrue(nodeParams, sessionMapper);
        }
    }

    public synchronized HgStoreSession openNodeSession(HgStoreNode node) {
        HgStoreSession res = this.sessions.get(node.getNodeId());
        if (res instanceof HgStoreNodeSession &&
            ((HgStoreNodeSession) res).getStoreNode() != node) {
            // A retry can receive the same node ID backed by a new Store process.
            this.sessions.remove(node.getNodeId());
            res = null;
        }
        if (res == null) {
            this.sessions.put(node.getNodeId(), (res = node.openSession(this.graphName)));
        }
        if (this.isTx) {
            res.beginTx();
        }

        return res;
    }

    <R> R limitOne(
            Supplier<Stream<HgPair<HgStoreNode, NodeTkv>>> nodeStreamSupplier,
            Function<SessionData<NodeTkv>, R> sessionMapper, R emptyObj) {

        Optional<R> res = retryingInvoke(
                () -> nodeStreamSupplier.get()
                                        .parallel()
                                        .map(
                                                pair -> new SessionData<NodeTkv>(
                                                        openNodeSession(pair.getKey()),
                                                        pair.getValue())
                                        ).map(sessionMapper)
                                        .filter(
                                                r -> isValid(r)
                                        )
                                        .findAny()
                                        .orElseGet(() -> emptyObj)
        );
        return res.orElse(emptyObj);
    }

    <R> List<R> toList(Function<Long, HgStoreNode> nodeFunction
            , List<HgOwnerKey> keyList
            , Function<HgOwnerKey, Stream<NodeTkv>> flatMapper
            , Function<SessionData<List<HgOwnerKey>>, List<R>> sessionMapper) {
        Optional<List<R>> res = retryingInvoke(
                () -> keyList.stream()
                             .flatMap(flatMapper)
                             .collect(collector)
                             .entrySet()
                             .stream()
                             .map(
                                     e -> new SessionData<>
                                             (
                                                     openNodeSession(
                                                             nodeFunction.apply(e.getKey())),
                                                     e.getValue()
                                             )
                             )
                             .parallel()
                             .map(sessionMapper)
                             .flatMap(
                                     e -> e.stream()
                             )
                             //.distinct()
                             .collect(Collectors.toList())
        );

        return res.orElse(EMPTY_LIST);
    }

    private boolean isAllTrue(HgTriple<String, HgOwnerKey, Object> nodeParams,
                              Function<NodeTkv, Boolean> action) {
        Optional<Boolean> res = retryingInvoke(() -> doAction(nodeParams, action));
        return res.orElse(false);
    }

    boolean isAllTrue(Supplier<Stream<HgPair<HgStoreNode, NodeTkv>>> dataSource,
                      Function<SessionData<NodeTkv>, Boolean> action) {
        Optional<Boolean> res = retryingInvoke(
                () -> dataSource.get()
                                .parallel()
                                .map(
                                        pair -> new SessionData<NodeTkv>(
                                                openNodeSession(pair.getKey()),
                                                pair.getValue())
                                ).map(action)
                                .allMatch(Boolean::booleanValue)
        );

        return res.orElse(false);
    }

    boolean ifAnyTrue(Supplier<Stream<HgPair<HgStoreNode, NodeTkv>>> nodeStreamSupplier
            , Function<SessionData<NodeTkv>, Boolean> sessionMapper) {

        Optional<Boolean> res = retryingInvoke(
                () -> nodeStreamSupplier.get()
                                        .parallel()
                                        .map(
                                                pair -> new SessionData<NodeTkv>(
                                                        openNodeSession(pair.getKey()),
                                                        pair.getValue())
                                        )
                                        .map(sessionMapper)
                                        .anyMatch(Boolean::booleanValue)
        );

        return res.orElse(false);
    }

    <T> Optional<T> retryingInvoke(Supplier<T> supplier) {
        boolean[] deadlineRetried = {false};
        return IntStream.rangeClosed(0, NODE_MAX_RETRYING_TIMES).boxed()
                        .map(
                                i -> {
                                    if (Thread.currentThread().isInterrupted()) {
                                        // The caller (e.g. a REST worker hitting
                                        // restserver.request_timeout) gave up: stop
                                        // retrying instead of holding its thread.
                                        // InterruptedException as the root cause: the
                                        // server's task cancel path recognises it
                                        // (HugeException.isInterrupted()).
                                        throw HgStoreClientException.of(
                                                "Interrupted before retry " + i,
                                                new InterruptedException());
                                    }
                                    T buffer = null;
                                    try {
                                        buffer = supplier.get();
                                    } catch (Throwable t) {
                                        Failure failure = classify(t);
                                        if (failure != Failure.DEADLINE) {
                                            // a different failure in between means the
                                            // next deadline is not "in a row" again
                                            deadlineRetried[0] = false;
                                        }
                                        if (failure == Failure.FATAL) {
                                            // The caller's thread was interrupted or the
                                            // call was cancelled: fail fast.
                                            log.warn("Not retrying after: {}",
                                                     t.getMessage(), t);
                                            throw HgStoreClientException.of(
                                                    t.getMessage(), t);
                                        }
                                        if (failure == Failure.DEADLINE) {
                                            // One retry: the NOT_WORK notice sent for the
                                            // failed RPC reloads the partition leaders, so
                                            // the next attempt can reach a new leader. A
                                            // second deadline in a row would only wait the
                                            // full deadline again on the same stalled store.
                                            if (deadlineRetried[0]) {
                                                log.warn("Not retrying a second deadline: {}",
                                                         t.getMessage(), t);
                                                throw HgStoreClientException.of(
                                                        t.getMessage(), t);
                                            }
                                            deadlineRetried[0] = true;
                                            log.warn("Deadline exceeded, retrying once in " +
                                                     "case the partition leader moved: {}",
                                                     t.getMessage());
                                        }
                                        if (i + 1 > NODE_MAX_RETRYING_TIMES) {
                                            log.error(maxTryMsg, t);
                                            throw HgStoreClientException.of(
                                                    t.getMessage(), t);
                                        }
                                        // The first three times try once every second,
                                        // subsequent incremental
                                        int sleepTime = i < 3 ? 1 : i - 1;
                                        log.info("Waiting {} seconds for the next try.",
                                                 sleepTime);
                                        try {
                                            Thread.sleep(sleepTime * 1000L);
                                        } catch (InterruptedException e) {
                                            Thread.currentThread().interrupt();
                                            e.addSuppressed(t);
                                            throw HgStoreClientException.of(
                                                    "Interrupted while waiting to retry: " +
                                                    t.getMessage(), e);
                                        }
                                    }
                                    return buffer;
                                }
                        )
                        .filter(e -> e != null)
                        .findFirst();

    }

    /**
     * How a failed attempt is treated by {@link #retryingInvoke}: FATAL is never retried (the
     * caller's thread was interrupted, or the call was cancelled), DEADLINE is retried exactly
     * once (the partition leader may have moved after the failed RPC invalidated the partition
     * cache; a second deadline in a row would only wait the full deadline again on the same
     * stalled store), everything else (transport errors, NOT_LEADER, store replacement) is
     * retried up to NODE_MAX_RETRYING_TIMES as before. For a parallel commit the failures of
     * the other partitions arrive as suppressed exceptions and are classified too; the most
     * severe class wins.
     */
    enum Failure {
        RETRYABLE, DEADLINE, FATAL
    }

    static Failure classify(Throwable t) {
        return classify(t, Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    private static Failure classify(Throwable t, Set<Throwable> seen) {
        Failure worst = Failure.RETRYABLE;
        Throwable c = t;
        while (c != null && seen.add(c)) {
            if (c instanceof InterruptedException) {
                return Failure.FATAL;
            }
            if (c instanceof StatusRuntimeException) {
                Status.Code code = ((StatusRuntimeException) c).getStatus().getCode();
                if (code == Status.Code.CANCELLED) {
                    return Failure.FATAL;
                }
                if (code == Status.Code.DEADLINE_EXCEEDED) {
                    worst = Failure.DEADLINE;
                }
            }
            for (Throwable suppressed : c.getSuppressed()) {
                Failure f = classify(suppressed, seen);
                if (f == Failure.FATAL) {
                    return f;
                }
                if (f.compareTo(worst) > 0) {
                    worst = f;
                }
            }
            c = c.getCause();
        }
        return worst;
    }

    private boolean isValid(Object obj) {
        if (obj == null) {
            return false;
        }

        if (HgStoreClientConst.EMPTY_BYTES.equals(obj)) {
            return false;
        }

        return !EMPTY_LIST.equals(obj);
    }

    class SessionData<T> {

        HgStoreSession session;
        T data;

        SessionData(HgStoreSession session, T data) {
            this.session = session;
            this.data = data;
        }

    }
}
