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

    private Cluster cluster;
    private final String keyspace;
    private int maxRetries;
    private long retryInterval;
    private long retryMaxDelay;

    public CassandraSessionPool(HugeConfig config,
                                String keyspace, String store) {
        super(config, keyspace + "/" + store);
        this.cluster = null;
        this.keyspace = keyspace;
        this.maxRetries = 0;
        this.retryInterval = 0L;
        this.retryMaxDelay = 0L;
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
        builder.withReconnectionPolicy(
                new ExponentialReconnectionPolicy(reconnectBase, reconnectMax));
        this.retryMaxDelay = reconnectMax;
        this.maxRetries = config.get(
                CassandraOptions.CASSANDRA_RECONNECT_MAX_RETRIES);
        this.retryInterval = config.get(
                CassandraOptions.CASSANDRA_RECONNECT_INTERVAL);

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

            int count = 0;
            int processors = Math.min(statements.size(), 1023);
            List<ResultSetFuture> results = new ArrayList<>(processors + 1);
            for (Statement s : statements) {
                ResultSetFuture future = this.session.executeAsync(s);
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
         * See issue #2740.
         */
        private ResultSet executeWithRetry(Statement statement) {
            int retries = CassandraSessionPool.this.maxRetries;
            long interval = CassandraSessionPool.this.retryInterval;
            long maxDelay = CassandraSessionPool.this.retryMaxDelay;
            DriverException lastError = null;
            for (int attempt = 0; attempt <= retries; attempt++) {
                try {
                    return this.session.execute(statement);
                } catch (NoHostAvailableException | OperationTimedOutException e) {
                    lastError = e;
                    if (attempt >= retries) {
                        break;
                    }
                    long delay = Math.min(interval * (1L << Math.min(attempt, 20)),
                                          maxDelay > 0 ? maxDelay : interval);
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
            throw new BackendException("Failed to execute Cassandra query " +
                                       "after %s retries: %s",
                                       lastError, retries,
                                       lastError == null ? "<null>" :
                                               lastError.getMessage());
        }

        private void tryOpen() {
            assert this.session == null;
            try {
                this.open();
            } catch (InvalidQueryException ignored) {
                // ignore
            }
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
         * with a lightweight query. Any failure here is swallowed so the
         * caller can still issue the real query, which will drive retries via
         * {@link #executeWithRetry(Statement)}.
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
                    if (this.session == null) {
                        return;
                    }
                }
                this.session.execute(new SimpleStatement(HEALTH_CHECK_CQL));
            } catch (DriverException e) {
                LOG.debug("Cassandra health-check failed, " +
                          "will retry on next query: {}", e.getMessage());
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
            } catch (Throwable e) {
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
