/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.meta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.type.define.GraphStatus;

/**
 * The status of one graph across all the servers that reported it.
 * <p>
 * The aggregate is a readiness gate, so it fails closed: it answers READY
 * only when every server that is expected to serve the graph reported READY.
 * When the number of expected servers is unknown the aggregate can't tell a
 * complete cluster from a partial one, and answers LOADING.
 */
public final class GraphStatusAggregate {

    public static final String STATUS_KEY = "status";
    public static final String READY_COUNT_KEY = "ready_count";
    public static final String TOTAL_COUNT_KEY = "total_count";
    public static final String EXPECTED_COUNT_KEY = "expected_count";
    public static final String SERVERS_KEY = "servers";

    /**
     * Rendered instead of a status name when no server reported yet, it's not
     * a state a server can be in so it's kept out of {@link GraphStatus}.
     */
    private static final String STATUS_UNKNOWN = "UNKNOWN";

    /**
     * The number of servers expected to report is unknown
     */
    private static final int UNKNOWN_EXPECTED = 0;

    private static final Comparator<GraphStatusEntry> BY_SERVER =
            Comparator.comparing(GraphStatusEntry::server,
                                 Comparator.nullsLast(
                                         Comparator.naturalOrder()));

    private final GraphStatus status;
    private final int readyCount;
    private final int expected;
    private final List<GraphStatusEntry> entries;

    private GraphStatusAggregate(GraphStatus status, int readyCount,
                                 int expected,
                                 List<GraphStatusEntry> entries) {
        this.status = status;
        this.readyCount = readyCount;
        this.expected = expected;
        this.entries = entries;
    }

    /**
     * Aggregates the reported status against the servers that are currently
     * registered in the cluster. A server keeps no state of its own, so the
     * status it reported outlives it: an entry left behind by a server that
     * is gone would otherwise hold a healthy graph down forever when it reads
     * FAILED, or make up a quorum the running servers never reached when it
     * reads READY.
     * <p>
     * A server missing from the registration isn't taken as gone right away:
     * a registration is refreshed periodically and lapses for a while when a
     * server is merely slow, and dropping the entry of a server that is in
     * fact still loading would answer READY too early. Only an entry that is
     * both unregistered and older than {@code staleAfter} is dropped, so the
     * reading errs on the side of holding the graph back.
     *
     * @param entries     the status reported by each server, may be null
     * @param liveServers the ids of the servers registered for the graph
     *                    space, empty or null when they can't be listed, in
     *                    which case nothing is dropped and the aggregate
     *                    can't reach READY
     * @param staleAfter  how long an unregistered server's status is kept, in
     *                    milliseconds
     * @param now         the current time, in milliseconds
     */
    public static GraphStatusAggregate of(Collection<GraphStatusEntry> entries,
                                          Collection<String> liveServers,
                                          long staleAfter, long now) {
        if (liveServers == null || liveServers.isEmpty()) {
            return of(entries, UNKNOWN_EXPECTED);
        }
        Set<String> live = new HashSet<>(liveServers);
        List<GraphStatusEntry> reported = new ArrayList<>();
        if (entries != null) {
            for (GraphStatusEntry entry : entries) {
                if (entry == null) {
                    continue;
                }
                boolean registered = live.contains(entry.server());
                boolean stale = now - entry.updateTime() > staleAfter;
                if (registered || !stale) {
                    reported.add(entry);
                }
            }
        }
        /*
         * A server that reported but is no longer registered still has to be
         * accounted for, otherwise the graph could look ready while it is not
         */
        int expected = Math.max(live.size(), reported.size());
        return of(reported, expected);
    }

    /**
     * @param entries  the status reported by each server, may be null or empty
     * @param expected the number of servers expected to report, a value less
     *                 than or equal to zero means the number is unknown and
     *                 the aggregate can't reach READY
     */
    public static GraphStatusAggregate of(Collection<GraphStatusEntry> entries,
                                          int expected) {
        List<GraphStatusEntry> sorted = new ArrayList<>();
        if (entries != null) {
            for (GraphStatusEntry entry : entries) {
                if (entry != null) {
                    sorted.add(entry);
                }
            }
        }
        sorted.sort(BY_SERVER);

        int ready = 0;
        boolean failed = false;
        boolean loading = false;
        for (GraphStatusEntry entry : sorted) {
            GraphStatus entryStatus = entry.status();
            if (entryStatus == GraphStatus.FAILED) {
                failed = true;
            } else if (entryStatus == GraphStatus.READY) {
                ready++;
            } else {
                // An unset status is not a guarantee, treat it as loading
                loading = true;
            }
        }

        GraphStatus status;
        if (sorted.isEmpty()) {
            status = null;
        } else if (failed) {
            status = GraphStatus.FAILED;
        } else if (loading) {
            status = GraphStatus.LOADING;
        } else if (expected > 0 && ready >= expected) {
            status = GraphStatus.READY;
        } else {
            /*
             * Either a server has not reported yet, or the number of servers
             * that should report is unknown. Both mean the graph can't be
             * guaranteed to answer on every server of the cluster
             */
            status = GraphStatus.LOADING;
        }

        return new GraphStatusAggregate(status, ready, expected,
                                        Collections.unmodifiableList(sorted));
    }

    public GraphStatus status() {
        return this.status;
    }

    public int readyCount() {
        return this.readyCount;
    }

    public int totalCount() {
        return this.entries.size();
    }

    public int expected() {
        return this.expected;
    }

    public boolean expectedKnown() {
        return this.expected > 0;
    }

    public List<GraphStatusEntry> entries() {
        return this.entries;
    }

    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(STATUS_KEY, this.status == null ? STATUS_UNKNOWN :
                            this.status.name());
        map.put(READY_COUNT_KEY, this.readyCount);
        map.put(TOTAL_COUNT_KEY, this.totalCount());
        // Always rendered, null tells the client the number can't be known
        Integer expectedCount = this.expectedKnown() ? this.expected : null;
        map.put(EXPECTED_COUNT_KEY, expectedCount);

        List<Map<String, Object>> servers =
                new ArrayList<>(this.entries.size());
        for (GraphStatusEntry entry : this.entries) {
            servers.add(entry.asMap());
        }
        map.put(SERVERS_KEY, servers);
        return map;
    }
}
