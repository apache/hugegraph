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

package org.apache.hugegraph.backend.store.cassandra;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.store.BackendSession.AbstractBackendSession;
import org.apache.hugegraph.backend.store.BackendSessionPool;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;

public class CassandraSessionPool extends BackendSessionPool {

    private static final Logger LOG = Log.logger(CassandraSessionPool.class);

    private static final int SECOND = 1000;
    private static final String HEALTH_CHECK_CQL =
            "SELECT now() FROM system.local";

    /**
     * Guards the one-time JVM-wide warning about {@code commitAsync()} not
     * being covered by query-time retries. {@link CassandraSessionPool} is
     * instantiated once per backend store per graph, so without this guard
     * the warning would fire many times on startup for a structural
     * limitation that does not change between instances.
     */
    private static final AtomicBoolean ASYNC_RETRY_WARNING_LOGGED =
            new AtomicBoolean(false);

    private Cluster cluster;
    private final String keyspace;
    private final int maxRetries;
    private final long retryInterval;
    private final long retryBaseDelay;
    private final long retryMaxDelay;

    public CassandraSessionPool(HugeConfig config,
                                String keyspace, String store) {
        super(config, keyspace + "/" + store);
        this.cluster = null;
        this.keyspace = keyspace;
        this.maxRetries = config.get(
                CassandraOptions.CASSANDRA_QUERY_RETRY_MAX_ATTEMPTS);
        this.retryInterval = config.get(
                CassandraOptions.CASSANDRA_QUERY_RETRY_INTERVAL);
        long reconnectBase = config.get(
                CassandraOptions.CASSANDRA_RECONNECT_BASE_DELAY);
        long reconnectMax = config.get(
                CassandraOptions.CASSANDRA_RECONNECT_MAX_DELAY);
        E.checkArgument(reconnectMax >= reconnectBase,
                        "'%s' (%s) must be >= '%s' (%s)",
                        CassandraOptions.CASSANDRA_RECONNECT_MAX_DELAY.name(),
                        reconnectMax,
                        CassandraOptions.CASSANDRA_RECONNECT_BASE_DELAY.name(),
                        reconnectBase);
        this.retryBaseDelay = reconnectBase;
        this.retryMaxDelay = reconnectMax;

        if (this.maxRetries > 0 &&
            ASYNC_RETRY_WARNING_LOGGED.compareAndSet(false, true)) {
            LOG.warn("cassandra.query_retry_max_attempts={} applies to sync commit()" +
                     " only. commitAsync() has no retry protection.", this.maxRetries);
        }
    }

    @Override
    public synchronized void open() {
        if (this.opened()) {
            throw new BackendException("Please close the old SessionPool " +
                                       "before opening a new one");
        }

        HugeConfig config = this.config();
        // Contact options
        String hosts = config.get(CassandraOptions.CASSANDRA_HOST);
        int port = config.get(CassandraOptions.CASSANDRA_PORT);

        assert this.cluster == null || this.cluster.isClosed();
        /*
         * We disable cassandra metrics through withoutMetrics(), due to
         * metrics versions are incompatible, java11 glassfish use metrics 4,
         * but cassandra use metrics 3.
         * TODO: fix it after after cassandra upgrade metrics version
         */
        Builder builder = Cluster.builder()
                                 .addContactPoints(hosts.split(","))
                                 .withoutMetrics()
                                 .withPort(port);

        // Timeout options
        int connTimeout = config.get(CassandraOptions.CASSANDRA_CONN_TIMEOUT);
        int readTimeout = config.get(CassandraOptions.CASSANDRA_READ_TIMEOUT);

        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setConnectTimeoutMillis(connTimeout * SECOND);
        socketOptions.setReadTimeoutMillis(readTimeout * SECOND);

        builder.withSocketOptions(socketOptions);

        // Reconnection policy: let driver keep retrying nodes in background
        // with exponential backoff after they go down (see issue #2740).
        builder.withReconnectionPolicy(
                new ExponentialReconnectionPolicy(this.retryBaseDelay,
                                                  this.retryMaxDelay));

        // Credential options
        String username = config.get(CassandraOptions.CASSANDRA_USERNAME);
        String password = config.get(CassandraOptions.CASSANDRA_PASSWORD);
        if (!username.isEmpty()) {
            builder.withCredentials(username, password);
        }

        // Compression options
        String compression = config.get(CassandraOptions.CASSANDRA_COMPRESSION);
        builder.withCompression(Compression.valueOf(compression.toUpperCase()));

        this.cluster = builder.build();
    }

    @Override
    public final synchronized boolean opened() {
        return (this.cluster != null && !this.cluster.isClosed());
    }

    protected final synchronized Cluster cluster() {
        E.checkState(this.cluster != null,
                     "Cassandra cluster has not been initialized");
        return this.cluster;
    }

    @Override
    public final Session session() {
        return (Session) super.getOrNewSession();
    }

    @Override
    protected Session newSession() {
        E.checkState(this.cluster != null,
                     "Cassandra cluster has not been initialized");
        return new Session();
    }

    @Override
    protected synchronized void doClose() {
        if (this.cluster != null && !this.cluster.isClosed()) {
            this.cluster.close();
        }
    }

    public final boolean clusterConnected() {
        E.checkState(this.cluster != null,
                     "Cassandra cluster has not been initialized");
        return !this.cluster.isClosed();
    }

    /**
     * The Session class is a wrapper of driver Session
     * Expect every thread hold its own session(wrapper)
     */
    public final class Session extends AbstractBackendSession {

        private com.datastax.driver.core.Session session;
        private BatchStatement batch;

        public Session() {
            this.session = null;
            this.batch = new BatchStatement(); // LOGGED
        }

        public BatchStatement add(Statement statement) {
            return this.batch.add(statement);
        }

        @Override
        public void rollback() {
            this.batch.clear();
        }

        @Override
        public ResultSet commit() {
            ResultSet rs = this.executeWithRetry(this.batch);
            // Clear batch if execute() successfully (retained if failed)
            this.batch.clear();
            return rs;
        }

        public void commitAsync() {
            Collection<Statement> statements = this.batch.getStatements();
            if (statements.isEmpty()) {
                this.batch.clear();
                return;
            }

            int count = 0;
            int processors = Math.min(statements.size(), 1023);
            List<ResultSetFuture> results = new ArrayList<>(processors + 1);
            com.datastax.driver.core.Session driverSession =
                    this.sessionForAsyncCommit();
            for (Statement s : statements) {
                // TODO: track async retry support in a follow-up issue.
                // commitAsync() bypasses executeWithRetry().
                // During a Cassandra restart, async writes may fail with
                // NoHostAvailableException even when maxRetries > 0. Callers
                // must handle ResultSetFuture failures surfaced by
                // getUninterruptibly(). A follow-up issue should wrap each
                // future with retry semantics.
                ResultSetFuture future = driverSession.executeAsync(s);
                results.add(future);

                if (++count > processors) {
                    results.forEach(ResultSetFuture::getUninterruptibly);
                    results.clear();
                    count = 0;
                }
            }
            for (ResultSetFuture future : results) {
                future.getUninterruptibly();
            }

            // Clear batch if execute() successfully (retained if failed)
            this.batch.clear();
        }

        public ResultSet query(Statement statement) {
            assert !this.hasChanges();
            return this.execute(statement);
        }

        public ResultSet execute(Statement statement) {
            return this.executeWithRetry(statement);
        }

        public ResultSet execute(String statement) {
            return this.executeWithRetry(new SimpleStatement(statement));
        }

        public ResultSet execute(String statement, Object... args) {
            return this.executeWithRetry(new SimpleStatement(statement, args));
        }

        /**
         * Execute a statement, retrying on transient connectivity failures
         * (NoHostAvailableException / OperationTimedOutException). The driver
         * itself keeps retrying connections in the background via the
         * reconnection policy, so once Cassandra comes back online, a
         * subsequent attempt here will succeed without restarting the server.
         *
         * <p>OperationTimedOutException is only retried for statements marked
         * idempotent; otherwise a timed-out mutation might be applied once by
         * Cassandra and then duplicated by a client-side retry.
         *
         * <p>If the driver session has been discarded (e.g. by
         * {@link #reconnectIfNeeded()} after a failed health-check) it is
         * lazily reopened at the start of each attempt.
         *
         * <p><b>Blocking note:</b> retries block the calling thread via
         * {@link Thread#sleep(long)}. Worst-case a single call blocks for
         * {@code maxRetries * retryMaxDelay} ms. Under high-throughput
         * workloads concurrent threads may pile up in {@code sleep()} during
         * a Cassandra outage. For such deployments lower
         * {@code cassandra.query_retry_max_attempts} (default 3) and
         * {@code cassandra.reconnect_max_delay} (default 10000ms) so the
         * request fails fast and pressure is released back to the caller.
         */
        private ResultSet executeWithRetry(Statement statement) {
            int retries = CassandraSessionPool.this.maxRetries;
            long interval = CassandraSessionPool.this.retryInterval;
            long maxDelay = CassandraSessionPool.this.retryMaxDelay;
            DriverException lastError = null;
            for (int attempt = 0; attempt <= retries; attempt++) {
                try {
                    if (this.session == null || this.session.isClosed()) {
                        // Lazy reopen: may itself throw NHAE while
                        // Cassandra is still unreachable; the catch below
                        // treats that as a transient failure.
                        this.session = null;
                        this.open();
                    }
                    return this.session.execute(statement);
                } catch (NoHostAvailableException | OperationTimedOutException e) {
                    lastError = e;
                    if (e instanceof OperationTimedOutException &&
                        !Boolean.TRUE.equals(statement.isIdempotent())) {
                        throw new BackendException(
                                "Cassandra query timed out and won't be " +
                                "retried because the statement is not " +
                                "marked idempotent", e);
                    }
                    if (attempt >= retries) {
                        break;
                    }
                    long cap = maxDelay > 0 ? maxDelay : interval;
                    long shift = 1L << Math.min(attempt, 20);
                    long delay;
                    try {
                        // Guard against Long overflow when retryInterval is huge.
                        delay = Math.min(Math.multiplyExact(interval, shift), cap);
                    } catch (ArithmeticException overflow) {
                        delay = cap;
                    }
                    LOG.warn("Cassandra temporarily unavailable ({}), " +
                             "retry {}/{} in {} ms",
                             e.getClass().getSimpleName(), attempt + 1,
                             retries, delay);
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new BackendException("Interrupted while " +
                                                   "waiting to retry " +
                                                   "Cassandra query", ie);
                    }
                }
            }
            // Preserve original exception as cause (stack trace + type) by
            // pre-formatting the message and using the (String, Throwable)
            // constructor explicitly to avoid ambiguity with varargs overloads.
            String msg = String.format(
                    "Failed to execute Cassandra query after %s retries: %s",
                    retries,
                    lastError == null ? "<null>" : lastError.getMessage());
            throw new BackendException(msg, lastError);
        }

        private void tryOpen() {
            assert this.session == null;
            try {
                this.open();
            } catch (InvalidQueryException ignored) {
                // ignore
            }
        }

        private com.datastax.driver.core.Session sessionForAsyncCommit() {
            if (this.session == null || this.session.isClosed()) {
                this.session = null;
                try {
                    this.open();
                } catch (DriverException e) {
                    throw new BackendException(
                            "Failed to open Cassandra session for async commit",
                            e);
                }
            }
            if (this.session == null) {
                throw new BackendException(
                        "Cassandra session is unavailable for async commit");
            }
            return this.session;
        }

        @Override
        public void open() {
            this.opened = true;
            assert this.session == null;
            this.session = cluster().connect(keyspace());
        }

        @Override
        public boolean opened() {
            if (this.opened && this.session == null) {
                this.tryOpen();
            }
            return this.opened && this.session != null;
        }

        @Override
        public boolean closed() {
            if (!this.opened || this.session == null) {
                return true;
            }
            return this.session.isClosed();
        }

        @Override
        public void close() {
            assert this.closeable();
            if (this.session == null) {
                return;
            }
            this.session.close();
            this.session = null;
        }

        @Override
        public boolean hasChanges() {
            return this.batch.size() > 0;
        }

        /**
         * Periodic liveness probe invoked by {@link BackendSessionPool} to
         * recover thread-local sessions after Cassandra has been restarted.
         * Reopens the driver session if it was closed and pings the cluster
         * with a lightweight query. On failure the session is discarded via
         * {@link #reset()} so the next call to
         * {@link #executeWithRetry(Statement)} reopens it; any exception
         * here is swallowed so the caller can still issue the real query.
         */
        @Override
        public void reconnectIfNeeded() {
            if (!this.opened) {
                return;
            }
            try {
                if (this.session == null || this.session.isClosed()) {
                    this.session = null;
                    this.tryOpen();
                }
                if (this.session != null) {
                    this.session.execute(new SimpleStatement(HEALTH_CHECK_CQL));
                }
            } catch (NoHostAvailableException | OperationTimedOutException e) {
                LOG.debug("Cassandra health-check failed, resetting session: {}",
                          e.getMessage());
                this.reset();
            }
        }

        /**
         * Force-close the driver session so it is re-opened on the next
         * {@link #opened()} call. Used when a failure is observed and we
         * want to start fresh on the next attempt.
         */
        @Override
        public void reset() {
            if (this.session == null) {
                return;
            }
            try {
                this.session.close();
            } catch (Exception e) {
                // Do not swallow Error (OOM / StackOverflow); only log
                // ordinary exceptions raised by the driver on close.
                LOG.warn("Failed to reset Cassandra session", e);
            } finally {
                this.session = null;
            }
        }

        public Collection<Statement> statements() {
            return this.batch.getStatements();
        }

        public String keyspace() {
            return CassandraSessionPool.this.keyspace;
        }

        public Metadata metadata() {
            return CassandraSessionPool.this.cluster.getMetadata();
        }

        public int aggregateTimeout() {
            HugeConfig conf = CassandraSessionPool.this.config();
            return conf.get(CassandraOptions.AGGR_TIMEOUT);
        }
    }
}
