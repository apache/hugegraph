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

package org.apache.hugegraph.backend.cache;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

/**
 * Keeps the schema cache of one graph on this server consistent with schema
 * changes made through other servers.
 * <p>
 * Every schema change writes a new opaque version of the graph to PD
 * ({@link #bump()}). Each tick ({@link #run()}) reads the version and, if it
 * differs from the one adopted last time, clears the schema cache of the
 * graph and adopts the version it read before the clear. Versions are only
 * compared for equality. The PD watch events are not involved, so a lost
 * event or a dead watch can delay a change by at most one interval.
 * <p>
 * Changes made through this server are not skipped: a writer could otherwise
 * adopt its own version after another server wrote a newer schema element,
 * and keep that element stale.
 */
public final class SchemaVersionReconciler implements Runnable {

    private static final Logger LOG = Log.logger(SchemaVersionReconciler.class);

    private static final AtomicLong VERSION_SEQ = new AtomicLong();
    private static final String NONE = "<none>";

    // One daemon thread runs the ticks of every graph in this JVM
    private static final ScheduledExecutorService SCHEDULER = newScheduler();

    private final String graphSpace;
    private final String graph;
    private final String spaceGraphName;
    private final String source;
    private final VersionStore store;
    private final Runnable clearCache;
    private final BooleanSupplier graphClosed;

    private final AtomicBoolean pendingWrite;
    // The version adopted by the last clear, null before the first tick
    private volatile String applied;
    private volatile boolean unreachable;
    private volatile boolean stopped;
    private ScheduledFuture<?> future;

    public SchemaVersionReconciler(String graphSpace, String graph,
                                   String source, VersionStore store,
                                   Runnable clearCache,
                                   BooleanSupplier graphClosed) {
        E.checkNotNull(graphSpace, "graphSpace");
        E.checkNotNull(graph, "graph");
        E.checkNotNull(source, "source");
        E.checkNotNull(store, "store");
        E.checkNotNull(clearCache, "clearCache");
        E.checkNotNull(graphClosed, "graphClosed");
        this.graphSpace = graphSpace;
        this.graph = graph;
        this.spaceGraphName = graphSpace + "-" + graph;
        this.source = source;
        this.store = store;
        this.clearCache = clearCache;
        this.graphClosed = graphClosed;
        this.pendingWrite = new AtomicBoolean(false);
    }

    private static ScheduledExecutorService newScheduler() {
        ScheduledThreadPoolExecutor scheduler =
                new ScheduledThreadPoolExecutor(1, r -> {
                    Thread thread = new Thread(r, "schema-version-reconciler");
                    thread.setDaemon(true);
                    return thread;
                });
        scheduler.setRemoveOnCancelPolicy(true);
        return scheduler;
    }

    static String newVersion(String source) {
        return System.currentTimeMillis() + "-" + source + "-" +
               VERSION_SEQ.incrementAndGet();
    }

    /**
     * Write a new version after a committed schema change. A failed write
     * doesn't fail the schema change: it's retried by the next tick, or by
     * the next schema change if the reconciler isn't scheduled.
     */
    public void bump() {
        try {
            this.store.write(this.graphSpace, this.graph,
                             newVersion(this.source));
        } catch (Exception e) {
            this.pendingWrite.set(true);
            LOG.warn("Schema version write failed for graph '{}', will retry: {}",
                     this.spaceGraphName, e.toString());
        }
    }

    @Override
    public void run() {
        if (this.stopped || this.graphClosed.getAsBoolean()) {
            this.stop();
            return;
        }
        // An exception escaping here would cancel the scheduled task
        try {
            // Reset before the write, so a bump() failing meanwhile isn't lost
            if (this.pendingWrite.getAndSet(false)) {
                try {
                    this.store.write(this.graphSpace, this.graph,
                                     newVersion(this.source));
                } catch (Exception e) {
                    this.pendingWrite.set(true);
                    throw e;
                }
                LOG.info("Schema version pending write landed for graph '{}'",
                         this.spaceGraphName);
            }
            String current = this.store.read(this.graphSpace, this.graph);
            if (current == null) {
                current = "";
            }
            if (this.unreachable) {
                this.unreachable = false;
                LOG.info("PD reachable again, schema version reconcile " +
                         "resumed for graph '{}'", this.spaceGraphName);
            }
            if (current.equals(this.applied)) {
                return;
            }
            // Clear after the read, so the adopted version never claims a
            // change that this clear didn't cover
            this.clearCache.run();
            String previous = this.applied == null ? NONE : this.applied;
            this.applied = current;
            LOG.info("Schema cache of graph '{}' cleared by version " +
                     "reconciler ({} -> {})", this.spaceGraphName, previous,
                     current.isEmpty() ? NONE : current);
        } catch (Exception e) {
            if (!this.unreachable) {
                this.unreachable = true;
                LOG.warn("PD unreachable, schema version reconcile skipped " +
                         "for graph '{}': {}", this.spaceGraphName,
                         e.toString());
            }
        }
    }

    public synchronized void start(long intervalMs) {
        E.checkArgument(intervalMs > 0L,
                        "The reconcile interval must be > 0, but got %s",
                        intervalMs);
        this.stopped = false;
        // Random first delay, so servers started together don't tick together
        long delay = ThreadLocalRandom.current().nextLong(intervalMs + 1L);
        this.future = SCHEDULER.scheduleWithFixedDelay(this, delay, intervalMs,
                                                       TimeUnit.MILLISECONDS);
    }

    /**
     * Restart the task if an Error escaped a tick and cancelled it. Called
     * when a schema transaction of the graph is created.
     */
    public synchronized void ensureScheduled(long intervalMs) {
        if (this.stopped || (this.future != null && !this.future.isDone())) {
            return;
        }
        LOG.warn("Schema version reconciler of graph '{}' is not running, " +
                 "restarting it", this.spaceGraphName);
        this.start(intervalMs);
    }

    public synchronized void stop() {
        this.stopped = true;
        if (this.future != null) {
            this.future.cancel(false);
        }
    }

    public boolean stopped() {
        return this.stopped;
    }

    public String applied() {
        return this.applied;
    }

    public boolean pendingWrite() {
        return this.pendingWrite.get();
    }

    synchronized ScheduledFuture<?> future() {
        return this.future;
    }

    public interface VersionStore {

        /**
         * @return the stored version, or "" if none was written
         */
        String read(String graphSpace, String graph);

        void write(String graphSpace, String graph, String version);
    }
}
