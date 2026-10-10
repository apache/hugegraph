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

package org.apache.hugegraph.api.profile;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableSet;

/**
 * Whether this server can serve graph traffic, for a readiness probe.
 * Graphs on an embedded backend are ready as soon as the REST layer answers.
 * Graphs on a remote backend are probed through the backend's
 * "storage_readiness" metadata: on hstore at least one known Store answers a
 * cheap direct call (PD is only needed until the first Store list is known),
 * on hbase the cluster answers an admin call about one of the graph's tables. The storage is shared by every
 * hstore graph of the process, so one graph is probed and the result is
 * reused for a short TTL to keep repeated probes cheap. The body never
 * carries raw exception text, since the endpoint is unauthenticated.
 */
public final class StorageReadiness {

    public static final String STORAGE_READINESS_META = "storage_readiness";
    public static final String BACKEND_HSTORE = "hstore";
    public static final String BACKEND_HBASE = "hbase";
    /** Backends on a remote cluster, whose availability the probe checks. */
    public static final Set<String> REMOTE_BACKENDS = ImmutableSet.of(BACKEND_HSTORE,
                                                                       BACKEND_HBASE);

    private static final Logger LOG = Log.logger(StorageReadiness.class);

    /** A probe result with the time it was taken: the TTL check and the body come from one probe. */
    private static final class Cached {

        private final Map<String, Object> result;
        private final long at;

        private Cached(Map<String, Object> result, long at) {
            this.result = result;
            this.at = at;
        }
    }

    private static final AtomicReference<Cached> LAST = new AtomicReference<>();
    private static final AtomicReference<CompletableFuture<Map<String, Object>>> IN_FLIGHT =
            new AtomicReference<>();
    private static final AtomicInteger WAITERS = new AtomicInteger();
    /** Probes of independent backend configurations run side by side here. */
    private static final ExecutorService PROBES =
            Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r, "storage-readiness-probe");
                t.setDaemon(true);
                return t;
            });

    private StorageReadiness() {
    }

    /** One storage probe with a time budget in ms. */
    public interface Probe {

        Map<String, Object> probe(long timeoutMs) throws Exception;
    }

    public static Map<String, Object> check(GraphManager manager, long timeoutMs,
                                            long cacheTtlMs, int maxWaiters) {
        // The graphs are auth proxies and the probe request carries no user,
        // so look the graphs up and probe them as the internal admin, the way
        // other internal paths do; the result carries no data or addresses
        List<Map<String, Object>> holder = new ArrayList<>(1);
        HugeGraphAuthProxy.runAsAdmin(() -> {
            List<RemoteGraph> remotes = remoteGraphs(manager);
            String storage = remotes.isEmpty() ? "embedded" : remotes.stream().map(r -> r.backend).distinct()
                                                                     .collect(Collectors.joining(","));
            // a configured graph that did not load is not served, whatever its
            // backend: GraphManager logs and skips it, so it is absent from the
            // graphs looked at above and would otherwise read as "embedded"
            int failed = manager.failedGraphs().size();
            if (failed > 0) {
                holder.add(failedBody(failed, storage));
                return;
            }
            if (remotes.isEmpty()) {
                Map<String, Object> body = new LinkedHashMap<>();
                body.put("ready", true);
                body.put("storage", "embedded");
                body.put("reason", "no graph on a remote storage");
                holder.add(body);
                return;
            }
            holder.add(check(storage, t -> probeAll(remotes, t), timeoutMs, cacheTtlMs, maxWaiters));
        });
        return holder.get(0);
    }

    /** Not ready: `failed` configured graphs could not be loaded (no names, the endpoint is public). */
    public static Map<String, Object> failedBody(int failed, String storage) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ready", false);
        body.put("storage", storage);
        body.put("reason", failed + " configured graph(s) failed to load");
        body.put("failed_graphs", failed);
        return body;
    }

    /** One graph per distinct remote backend configuration, in graph order; public for the unit test. */
    public static final class RemoteGraph {

        final String name;
        final String backend;
        final Probe probe;

        public RemoteGraph(String name, String backend, Probe probe) {
            this.name = name;
            this.backend = backend;
            this.probe = probe;
        }
    }

    /**
     * Readiness covers every remote backend configuration this server
     * uses: graphs that share a configuration (the same PD peers, the same
     * HBase hosts and namespace) share one probe; independent clusters are
     * probed side by side within the common budget, and ready means all of
     * them are ready.
     */
    static List<RemoteGraph> remoteGraphs(GraphManager manager) {
        List<RemoteGraph> out = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (String name : manager.graphs()) {
            try {
                HugeGraph graph = manager.graph(name);
                if (graph == null || !REMOTE_BACKENDS.contains(graph.backend())) {
                    continue;
                }
                String key = configKey(graph.backend(), graph.configuration(), graph.name());
                if (!seen.add(key)) {
                    continue;
                }
                out.add(new RemoteGraph(name, graph.backend(), probeOf(graph)));
            } catch (Throwable e) {
                LOG.debug("Skip graph {} while looking for remote storages", name, e);
            }
        }
        return out;
    }

    /**
     * The probe of one graph. `metadata()` auto-opens the thread-local graph
     * transaction (it goes through graphTransaction()), and a transaction
     * left open on a request or probe thread makes the graph's close() fail
     * its all-threads-closed check, so the probe closes it on the way out.
     */
    public static Probe probeOf(HugeGraph graph) {
        return t -> {
            try {
                return graph.metadata(null, STORAGE_READINESS_META, t);
            } finally {
                if (graph.tx().isOpen()) {
                    graph.tx().close();
                }
            }
        };
    }

    /**
     * What makes two graphs share one probe. hstore: the PD cluster, since
     * the probe pings the Stores PD lists. hbase: nothing, every graph is its
     * own scope, because the HBase provider derives the table namespace from
     * the graph name; the key still carries the full connection settings
     * (hosts, port, znode parent) so the body's grouping is honest.
     */
    public static String configKey(String backend, org.apache.commons.configuration2.Configuration conf,
                                   String graphName) {
        try {
            if (BACKEND_HSTORE.equals(backend)) {
                return backend + "|" + conf.getString("pd.peers");
            }
            return backend + "|" + conf.getString("hbase.hosts") + ":" + conf.getString("hbase.port") +
                   conf.getString("hbase.znode_parent") + "|" + graphName;
        } catch (Throwable e) {
            return backend + "|" + graphName;
        }
    }

    /** Every configuration's probe in parallel on the shared budget; the body merges them. */
    public static Map<String, Object> probeAll(List<RemoteGraph> remotes, long timeoutMs)
            throws Exception {
        if (remotes.size() == 1) {
            Map<String, Object> body = new LinkedHashMap<>(remotes.get(0).probe.probe(timeoutMs));
            body.put("probes", List.of(probeEntry(remotes.get(0), body)));
            return body;
        }
        long deadline = System.currentTimeMillis() + timeoutMs;
        List<CompletableFuture<Map<String, Object>>> futures = new ArrayList<>();
        for (RemoteGraph r : remotes) {
            futures.add(CompletableFuture.supplyAsync(() -> {
                // the auth context is a thread local of the request thread: the
                // probe runs as the internal admin on its own thread as well
                List<Map<String, Object>> holder = new ArrayList<>(1);
                HugeGraphAuthProxy.runAsAdmin(() -> {
                    try {
                        holder.add(r.probe.probe(timeoutMs));
                    } catch (Exception e) {
                        Map<String, Object> failed = new LinkedHashMap<>();
                        failed.put("ready", false);
                        failed.put("reason", "probe failed: " + e.getClass().getSimpleName());
                        holder.add(failed);
                    }
                });
                return holder.get(0);
            }, PROBES));
        }
        List<Map<String, Object>> entries = new ArrayList<>();
        Map<String, Object> first = null;
        boolean ready = true;
        String reason = "ok";
        for (int i = 0; i < remotes.size(); i++) {
            Map<String, Object> result;
            try {
                long remaining = Math.max(1L, deadline - System.currentTimeMillis());
                result = futures.get(i).get(remaining, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                futures.get(i).cancel(true);
                result = new LinkedHashMap<>();
                result.put("ready", false);
                result.put("reason", "did not answer within " + timeoutMs + " ms");
            }
            if (first == null) {
                first = result;
            }
            Map<String, Object> entry = probeEntry(remotes.get(i), result);
            entries.add(entry);
            if (!isReady(result)) {
                if (ready) {
                    // no graph name: the endpoint is unauthenticated
                    reason = remotes.get(i).backend + " configuration " + (i + 1) + " of " +
                             remotes.size() + ": " + result.get("reason");
                }
                ready = false;
            }
        }
        Map<String, Object> body = new LinkedHashMap<>(first);
        body.put("ready", ready);
        body.put("reason", reason);
        body.put("probes", entries);
        return body;
    }

    /** One probe in the public body: backend and outcome, no graph name (unauthenticated endpoint). */
    private static Map<String, Object> probeEntry(RemoteGraph r, Map<String, Object> result) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("storage", r.backend);
        entry.put("ready", isReady(result));
        entry.put("reason", result.get("reason"));
        return entry;
    }

    public static Map<String, Object> check(Probe probe, long timeoutMs, long cacheTtlMs) {
        return check(BACKEND_HSTORE, probe, timeoutMs, cacheTtlMs, Integer.MAX_VALUE);
    }

    /**
     * At most one probe runs at a time and every caller gets its answer:
     * the first caller runs the probe on its own thread, concurrent callers
     * wait for that result, each bounded by its own timeout, and at most
     * {@code maxWaiters} of them wait at once: the rest get an immediate
     * not-ready, so a burst of probes during slow storage cannot hold the
     * REST worker pool (the endpoint is unauthenticated and outside the
     * load-shedding filter). No monitor is held during the probe.
     */
    public static Map<String, Object> check(String storage, Probe probe, long timeoutMs,
                                            long cacheTtlMs, int maxWaiters) {
        Cached last = LAST.get();
        if (last != null && System.currentTimeMillis() - last.at < cacheTtlMs) {
            Map<String, Object> body = new LinkedHashMap<>(last.result);
            body.put("cached", true);
            return body;
        }
        CompletableFuture<Map<String, Object>> mine = new CompletableFuture<>();
        CompletableFuture<Map<String, Object>> running = IN_FLIGHT.get();
        if (running == null && IN_FLIGHT.compareAndSet(null, mine)) {
            Map<String, Object> body;
            try {
                body = probeOnce(storage, probe, timeoutMs);
                LAST.set(new Cached(body, System.currentTimeMillis()));
            } finally {
                IN_FLIGHT.set(null);
            }
            mine.complete(body);
            return new LinkedHashMap<>(body);
        }
        if (running == null) {
            running = IN_FLIGHT.get();
        }
        if (running == null) {
            // The owner finished between our two reads: its result is cached
            return check(storage, probe, timeoutMs, cacheTtlMs, maxWaiters);
        }
        if (WAITERS.incrementAndGet() > maxWaiters) {
            WAITERS.decrementAndGet();
            return notReady(storage, "too many readiness callers waiting for the probe (" +
                                     maxWaiters + ")");
        }
        try {
            Map<String, Object> body = new LinkedHashMap<>(
                    running.get(timeoutMs, TimeUnit.MILLISECONDS));
            body.put("shared", true);
            return body;
        } catch (TimeoutException e) {
            return notReady(storage, "a probe is still running after " + timeoutMs + " ms");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return notReady(storage, "interrupted");
        } catch (ExecutionException e) {
            return notReady(storage, "probe failed: " +
                                     e.getCause().getClass().getSimpleName());
        } finally {
            WAITERS.decrementAndGet();
        }
    }

    private static Map<String, Object> probeOnce(String storage, Probe probe, long timeoutMs) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ready", false);
        body.put("storage", storage);
        try {
            Map<String, Object> result = probe.probe(timeoutMs);
            body.putAll(result);
        } catch (Throwable e) {
            LOG.warn("Storage readiness probe failed", e);
            body.put("ready", false);
            body.put("reason", "probe failed: " + e.getClass().getSimpleName());
        }
        body.put("cached", false);
        return body;
    }

    private static Map<String, Object> notReady(String storage, String reason) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ready", false);
        body.put("storage", storage);
        body.put("reason", reason);
        body.put("cached", false);
        return body;
    }

    public static boolean isReady(Map<String, Object> body) {
        return Boolean.TRUE.equals(body.get("ready"));
    }

    public static void resetCache() {
        LAST.set(null);
        IN_FLIGHT.set(null);
        WAITERS.set(0);
    }

}
