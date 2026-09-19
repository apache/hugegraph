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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

/**
 * Whether this server can serve graph traffic, for a readiness probe.
 * Graphs on an embedded backend are ready as soon as the REST layer answers.
 * Graphs on hstore are probed through the backend's "storage_readiness"
 * metadata: at least one known Store answers a cheap direct call; PD is only
 * needed until the first Store list is known. The storage is shared by every
 * hstore graph of the process, so one graph is probed and the result is
 * reused for a short TTL to keep repeated probes cheap. The body never
 * carries raw exception text, since the endpoint is unauthenticated.
 */
public final class StorageReadiness {

    public static final String STORAGE_READINESS_META = "storage_readiness";
    public static final String BACKEND_HSTORE = "hstore";

    private static final Logger LOG = Log.logger(StorageReadiness.class);

    private static volatile Map<String, Object> lastResult;
    private static volatile long lastCheckedAt;

    private StorageReadiness() {
    }

    /** One storage probe with a time budget in ms. */
    public interface Probe {

        Map<String, Object> probe(long timeoutMs) throws Exception;
    }

    public static Map<String, Object> check(GraphManager manager,
                                            long timeoutMs, long cacheTtlMs) {
        // The graphs are auth proxies and the probe request carries no user,
        // so look the graph up and probe it as the internal admin, the way
        // other internal paths do; the result carries no data or addresses
        List<Map<String, Object>> holder = new ArrayList<>(1);
        HugeGraphAuthProxy.runAsAdmin(() -> {
            HugeGraph graph = firstHstoreGraph(manager);
            if (graph == null) {
                Map<String, Object> body = new LinkedHashMap<>();
                body.put("ready", true);
                body.put("storage", "embedded");
                body.put("reason", "no graph on a remote storage");
                holder.add(body);
                return;
            }
            holder.add(check(t -> graph.metadata(null, STORAGE_READINESS_META, t),
                             timeoutMs, cacheTtlMs));
        });
        return holder.get(0);
    }

    public static synchronized Map<String, Object> check(Probe probe, long timeoutMs,
                                                         long cacheTtlMs) {
        long now = System.currentTimeMillis();
        Map<String, Object> cached = lastResult;
        if (cached != null && now - lastCheckedAt < cacheTtlMs) {
            Map<String, Object> body = new LinkedHashMap<>(cached);
            body.put("cached", true);
            return body;
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ready", false);
        body.put("storage", BACKEND_HSTORE);
        try {
            Map<String, Object> result = probe.probe(timeoutMs);
            body.putAll(result);
        } catch (Throwable e) {
            LOG.warn("Storage readiness probe failed", e);
            body.put("ready", false);
            body.put("reason", "probe failed: " + e.getClass().getSimpleName());
        }
        body.put("cached", false);
        lastResult = body;
        lastCheckedAt = System.currentTimeMillis();
        return new LinkedHashMap<>(body);
    }

    public static boolean isReady(Map<String, Object> body) {
        return Boolean.TRUE.equals(body.get("ready"));
    }

    public static synchronized void resetCache() {
        lastResult = null;
        lastCheckedAt = 0L;
    }

    private static HugeGraph firstHstoreGraph(GraphManager manager) {
        for (String name : manager.graphs()) {
            try {
                HugeGraph graph = manager.graph(name);
                if (graph != null && BACKEND_HSTORE.equals(graph.backend())) {
                    return graph;
                }
            } catch (Throwable e) {
                LOG.debug("Skip graph {} while looking for a remote storage", name, e);
            }
        }
        return null;
    }
}
