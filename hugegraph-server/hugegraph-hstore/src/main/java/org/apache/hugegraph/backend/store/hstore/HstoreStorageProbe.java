/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.backend.store.hstore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.grpc.state.HgStoreStateGrpc;
import org.apache.hugegraph.store.grpc.state.SubStateReq;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

/**
 * Storage-aware readiness of this server, from this server's point of view:
 * at least one Store answers a direct, local, read-only gRPC call
 * (HgStoreState.getScanState, which reads the node's own scan-pool stats and
 * never touches raft). The Store list comes from PD, refreshed in the
 * background (single-flight) while the last known list is used right away, so
 * PD only matters until the first list is known: a PD that is slow, restarting
 * or down afterwards does not change the readiness of a server whose Stores
 * still answer. Every known Store is pinged in parallel and the first answer
 * wins; every wait is bounded by one shared time budget. The result carries
 * no addresses and no raw exception text, since it is served without
 * authentication; the full messages go to the log.
 */
public final class HstoreStorageProbe {

    public static final String META_STORAGE_READINESS = "storage_readiness";

    private static final Logger LOG = Log.logger(HstoreStorageProbe.class);

    private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool(
            new BasicThreadFactory.Builder().namingPattern("storage-readiness-%d")
                                            .daemon(true).build());

    private static final KnownStores KNOWN = new KnownStores();
    private static final Map<String, ManagedChannel> CHANNELS = new ConcurrentHashMap<>();

    private HstoreStorageProbe() {
    }

    /** The active stores as PD sees them. */
    public interface StoreLister {

        List<Metapb.Store> activeStores() throws Exception;
    }

    /** One cheap call to one store; returning (any value) means it answered. */
    public interface StorePinger {

        void ping(Metapb.Store store, long timeoutMs) throws Exception;
    }

    /** The last store list PD answered with, shared by consecutive probes. */
    public static final class KnownStores {

        private volatile List<Metapb.Store> stores = Collections.emptyList();
        private volatile long at;
        private volatile Boolean pdOk;
        private volatile long pdAt;
        private final AtomicReference<CompletableFuture<List<Metapb.Store>>> inFlight =
                new AtomicReference<>();

        public List<Metapb.Store> stores() {
            return this.stores;
        }

        public long ageMs() {
            return this.at == 0L ? -1L : System.currentTimeMillis() - this.at;
        }

        /** Outcome of the last finished PD refresh, null before the first one. */
        public Boolean pdOk() {
            return this.pdOk;
        }

        public long pdAgeMs() {
            return this.pdAt == 0L ? -1L : System.currentTimeMillis() - this.pdAt;
        }

        public void update(List<Metapb.Store> stores) {
            this.pdOk = true;
            this.pdAt = System.currentTimeMillis();
            if (stores != null && !stores.isEmpty()) {
                this.stores = Collections.unmodifiableList(new ArrayList<>(stores));
                this.at = this.pdAt;
            }
        }

        public void pdFailed() {
            this.pdOk = false;
            this.pdAt = System.currentTimeMillis();
        }

        /**
         * The refresh in flight, or a new one started on `executor`: only one
         * PD call runs at a time no matter how many probes miss the cache,
         * so a hung PD parks one thread, not one per probe.
         */
        CompletableFuture<List<Metapb.Store>> refresh(StoreLister lister,
                                                      ExecutorService executor) {
            CompletableFuture<List<Metapb.Store>> running = this.inFlight.get();
            if (running != null && !running.isDone()) {
                return running;
            }
            CompletableFuture<List<Metapb.Store>> mine = new CompletableFuture<>();
            if (!this.inFlight.compareAndSet(running, mine)) {
                return this.inFlight.get();
            }
            executor.execute(() -> {
                try {
                    List<Metapb.Store> stores = lister.activeStores();
                    this.update(stores);
                    mine.complete(stores == null ? Collections.emptyList() : stores);
                } catch (Throwable e) {
                    this.pdFailed();
                    mine.completeExceptionally(e);
                }
            });
            return mine;
        }
    }

    /** The unauthenticated body: no addresses, no raw exception text. */
    private static Map<String, Object> result(boolean ready, String reason, int activeStores,
                                              Long answeredStore, Boolean pdReachable,
                                              long pdAgeMs, long storesAgeMs, long storeMillis) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("ready", ready);
        map.put("reason", reason);
        map.put("active_stores", activeStores);
        map.put("answered_store", answeredStore);
        map.put("pd_reachable", pdReachable);
        map.put("pd_checked_age_ms", pdAgeMs);
        map.put("stores_age_ms", storesAgeMs);
        map.put("store_millis", storeMillis);
        return map;
    }

    /**
     * Probe through the process-wide PD client and this probe's own plaintext
     * channels to the stores (the store gRPC server takes no credentials).
     *
     * @param timeoutMs the whole budget for PD plus stores
     */
    public static Map<String, Object> probe(long timeoutMs) {
        PDClient pd = HstoreSessionsImpl.getDefaultPdClient();
        if (pd == null) {
            return result(false, "pd client not initialised", 0, null, false, -1L, -1L, 0L);
        }
        return probe(KNOWN, () -> {
            List<Metapb.Store> stores = pd.getActiveStores();
            pruneChannels(CHANNELS, stores);
            return stores;
        }, HstoreStorageProbe::pingScanState, timeoutMs, EXECUTOR);
    }

    /**
     * Shut down the channels of addresses PD no longer lists (replaced
     * Stores). An empty answer is ignored, the same rule KnownStores.update
     * applies: the pings keep using the last known Stores, so their channels
     * must stay open.
     */
    static void pruneChannels(Map<String, ManagedChannel> channels,
                              List<Metapb.Store> stores) {
        if (stores == null || stores.isEmpty()) {
            return;
        }
        Set<String> live = new HashSet<>();
        for (Metapb.Store store : stores) {
            live.add(store.getAddress());
        }
        channels.entrySet().removeIf(e -> {
            if (live.contains(e.getKey())) {
                return false;
            }
            e.getValue().shutdownNow();
            return true;
        });
    }

    private static void pingScanState(Metapb.Store store, long timeoutMs) {
        ManagedChannel channel = CHANNELS.computeIfAbsent(store.getAddress(), address -> {
            return ManagedChannelBuilder.forTarget(address).usePlaintext().build();
        });
        HgStoreStateGrpc.newBlockingStub(channel)
                        .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                        .getScanState(SubStateReq.getDefaultInstance());
    }

    public static Map<String, Object> probe(KnownStores known, StoreLister lister,
                                            StorePinger pinger, long timeoutMs,
                                            ExecutorService executor) {
        E.checkArgument(timeoutMs > 0, "The probe timeout must be > 0, but got %s", timeoutMs);
        long deadline = System.currentTimeMillis() + timeoutMs;

        // Refresh the store list from PD in the background (single-flight);
        // whatever PD answers lands in `known` for this or the next probe
        CompletableFuture<List<Metapb.Store>> refresh = known.refresh(lister, executor);

        List<Metapb.Store> stores = known.stores();
        Boolean pdReachable = null;
        if (stores.isEmpty()) {
            // Nothing known yet (first probe after start): PD is the only source
            try {
                stores = await(refresh, deadline);
                pdReachable = true;
            } catch (TimeoutException e) {
                return result(false, "no store list known and pd did not answer within " +
                                         timeoutMs + " ms", 0, null, false, -1L, -1L, 0L);
            } catch (Exception e) {
                LOG.warn("Storage readiness: no store list known and pd failed", e);
                return result(false, "no store list known and pd failed: " + category(e),
                                  0, null, false, known.pdAgeMs(), -1L, 0L);
            }
            if (stores.isEmpty()) {
                return result(false, "no active store registered in pd",
                                  0, null, true, known.pdAgeMs(), known.ageMs(), 0L);
            }
        }

        long storeStart = System.currentTimeMillis();
        // Ping every known store at once and take the first answer: a store
        // whose connection hangs (a pod that just went away) must not eat the
        // budget of the stores that are fine, or a rolling restart would
        // flap the readiness of every server
        CompletionService<Metapb.Store> pings = new ExecutorCompletionService<>(executor);
        List<Future<Metapb.Store>> futures = new ArrayList<>(stores.size());
        for (Metapb.Store store : stores) {
            futures.add(pings.submit(() -> {
                pinger.ping(store, Math.max(1L, deadline - System.currentTimeMillis()));
                return store;
            }));
        }
        List<String> failures = new ArrayList<>();
        Map<String, Object> result = null;
        try {
            for (int done = 0; done < stores.size() && result == null; done++) {
                long remaining = deadline - System.currentTimeMillis();
                Future<Metapb.Store> first;
                try {
                    first = remaining > 0 ?
                            pings.poll(remaining, TimeUnit.MILLISECONDS) :
                            pings.poll();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    failures.add("interrupted");
                    break;
                }
                if (first == null) {
                    failures.add((stores.size() - done) + " store(s) did not answer within " +
                                 timeoutMs + " ms");
                    break;
                }
                try {
                    Metapb.Store store = first.get();
                    result = result(true, "ok", stores.size(), store.getId(),
                                        pdState(refresh, known, pdReachable), known.pdAgeMs(),
                                        known.ageMs(), elapsed(storeStart));
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause() != null ? e.getCause() : e;
                    LOG.debug("Storage readiness: a store ping failed", cause);
                    failures.add("a store failed: " + category(cause));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    failures.add("interrupted");
                    break;
                }
            }
        } finally {
            for (Future<Metapb.Store> f : futures) {
                f.cancel(true);
            }
        }
        if (result != null) {
            return result;
        }
        return result(false, "none of " + stores.size() + " known store(s) answered: " +
                                 String.join("; ", failures),
                          stores.size(), null, pdState(refresh, known, pdReachable),
                          known.pdAgeMs(), known.ageMs(), elapsed(storeStart));
    }

    /**
     * The outcome of this probe's PD refresh when it already finished, else
     * the outcome of the last finished one (null before any finished).
     */
    private static Boolean pdState(CompletableFuture<?> refresh, KnownStores known,
                                   Boolean awaited) {
        if (awaited != null) {
            return awaited;
        }
        if (refresh.isDone()) {
            return !refresh.isCompletedExceptionally();
        }
        return known.pdOk();
    }

    private static <T> T await(Future<T> future, long deadline) throws Exception {
        long remaining = Math.max(1L, deadline - System.currentTimeMillis());
        try {
            return future.get(remaining, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw cause instanceof Exception ? (Exception) cause : new RuntimeException(cause);
        }
    }

    private static long elapsed(long since) {
        return System.currentTimeMillis() - since;
    }

    /**
     * A fixed category for the unauthenticated body: the gRPC status code, "pd
     * unreachable" or the exception class, never the message (it can carry PD
     * peers and Store host names).
     */
    static String category(Throwable e) {
        if (e instanceof StatusRuntimeException) {
            return ((StatusRuntimeException) e).getStatus().getCode().name();
        }
        if (e instanceof PDException) {
            return "pd unreachable";
        }
        return e.getClass().getSimpleName();
    }
}
