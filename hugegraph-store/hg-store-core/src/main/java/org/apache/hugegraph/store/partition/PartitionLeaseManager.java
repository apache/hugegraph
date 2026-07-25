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

package org.apache.hugegraph.store.partition;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import lombok.Getter;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.pd.PdProvider;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import lombok.extern.slf4j.Slf4j;

/**
 * Manages partition leases for distributed rocksdb-cloud write fencing.
 * When running in distributed mode with rocksdb-cloud backend, stores acquire
 * leases from PD to establish exclusive write ownership over partitions.
 * This manager handles:
 * - Acquiring leases when a partition becomes a leader
 * - Periodically renewing active leases
 * - Releasing leases when ownership changes
 * - Mapping leases to S3 buckets for rocksdb-cloud writes
 */
@Slf4j
public class PartitionLeaseManager {

    private static final Logger LOG = Log.logger(PartitionLeaseManager.class);
    private static final int DEFAULT_LEASE_TTL_SECONDS = 30;
    private static final int DEFAULT_LEASE_RENEW_INTERVAL_SECONDS = 20;

    private final PdProvider pdProvider;
    private final long storeId;
    private final ScheduledExecutorService scheduledExecutor;
    private final Map<String, PartitionLeaseState> leaseStates = new ConcurrentHashMap<>();
    /**
     * -- GETTER --
     *  Check if this manager is enabled.
     */
    @Getter
    private final boolean enabled;
    private final int leaseTtlSeconds;
    private final int leaseRenewIntervalSeconds;

    // Partition key format: "graphName#partitionId"
    private static String partitionKey(String graphName, int partitionId) {
        return graphName + "#" + partitionId;
    }

    /**
     * Represents the state of an acquired partition lease.
     */
    static class PartitionLeaseState {
        final String graphName;
        final int partitionId;
        Metapb.PartitionLease lease;
        long nextRenewTime;

        PartitionLeaseState(String graphName, int partitionId, Metapb.PartitionLease lease) {
            this.graphName = graphName;
            this.partitionId = partitionId;
            this.lease = lease;
            this.nextRenewTime = System.currentTimeMillis() +
                                 DEFAULT_LEASE_RENEW_INTERVAL_SECONDS * 1000L;
        }

        boolean shouldRenew() {
            return System.currentTimeMillis() >= nextRenewTime;
        }
    }

    /**
     * Create a lease manager for distributed rocksdb-cloud mode.
     *
     * @param pdProvider PD client provider
     * @param storeId    this store's ID
     * @param enabled    whether lease management is enabled (feature flag)
     */
    public PartitionLeaseManager(PdProvider pdProvider, long storeId, boolean enabled) {
        this(pdProvider, storeId, enabled,
             DEFAULT_LEASE_TTL_SECONDS,
             DEFAULT_LEASE_RENEW_INTERVAL_SECONDS);
    }

    public PartitionLeaseManager(PdProvider pdProvider, long storeId, boolean enabled,
                                 int leaseTtlSeconds, int leaseRenewIntervalSeconds) {
        this.pdProvider = pdProvider;
        this.storeId = storeId;
        this.enabled = enabled;
        this.leaseTtlSeconds = leaseTtlSeconds > 0 ? leaseTtlSeconds : DEFAULT_LEASE_TTL_SECONDS;
        this.leaseRenewIntervalSeconds = leaseRenewIntervalSeconds > 0 ?
                                  leaseRenewIntervalSeconds : DEFAULT_LEASE_RENEW_INTERVAL_SECONDS;
        this.scheduledExecutor = new ScheduledThreadPoolExecutor(1,
                                                                 r -> {
                                                                     Thread t = new Thread(r,
                                                                                           "partition-lease-renewer");
                                                                     t.setDaemon(true);
                                                                     return t;
                                                                 });
        if (enabled) {
            startRenewalScheduler();
        }
    }

    /**
     * Start the background lease renewal scheduler.
     */
    private void startRenewalScheduler() {
        scheduledExecutor.scheduleAtFixedRate(
                this::renewExpiredLeases,
                leaseRenewIntervalSeconds,
                leaseRenewIntervalSeconds,
                TimeUnit.SECONDS
        );
    }

    /**
     * Acquire a lease for a partition becoming the leader.
     *
     * @param graphName   the graph name
     * @param partitionId the partition ID
     * @return the acquired PartitionLease, or null if acquisition fails
     */
    public Metapb.PartitionLease acquireLease(String graphName, int partitionId) {
        if (!enabled) {
            return null;
        }

        String key = partitionKey(graphName, partitionId);
        try {
            Metapb.PartitionLease lease = pdProvider.acquirePartitionLease(
                    graphName,
                    partitionId,
                    storeId,
                    leaseTtlSeconds
            );
            if (lease != null) {
                PartitionLeaseState state = new PartitionLeaseState(graphName, partitionId, lease);
                state.nextRenewTime = System.currentTimeMillis() +
                                      leaseRenewIntervalSeconds * 1000L;
                leaseStates.put(key, state);
                LOG.info("Acquired lease for partition {}: epoch={}, ttl={}s",
                         key, lease.getLeaseEpoch(), leaseTtlSeconds);
                return lease;
            }
        } catch (PDException e) {
            LOG.error("Failed to acquire lease for partition {}: {}", key, e.getMessage());
        }
        return null;
    }

    /**
     * Release a lease for a partition losing ownership.
     *
     * @param graphName   the graph name
     * @param partitionId the partition ID
     */
    public void releaseLease(String graphName, int partitionId) {
        if (!enabled) {
            return;
        }

        String key = partitionKey(graphName, partitionId);
        PartitionLeaseState state = leaseStates.get(key);
        if (state != null) {
            try {
                if (state.lease != null) {
                    pdProvider.releasePartitionLease(
                            graphName,
                            partitionId,
                            storeId,
                            state.lease.getLeaseEpoch()
                    );
                    LOG.info("Released lease for partition {}: epoch={}",
                             key, state.lease.getLeaseEpoch());
                }
            } catch (PDException e) {
                LOG.error("Failed to release lease for partition {}: {}", key, e.getMessage());
            } finally {
                leaseStates.remove(key);
            }
        }
    }

    /**
     * Get the current lease for a partition.
     *
     * @param graphName   the graph name
     * @param partitionId the partition ID
     * @return the current lease, or null if not acquired
     */
    public Metapb.PartitionLease getLease(String graphName, int partitionId) {
        String key = partitionKey(graphName, partitionId);
        PartitionLeaseState state = leaseStates.get(key);
        return state != null ? state.lease : null;
    }

    /**
     * Get the bucket name for a partition with a valid lease (for rocksdb-cloud writes).
     *
     * @param graphName   the graph name
     * @param partitionId the partition ID
     * @return the bucket name, or null if no valid lease
     */
    public String resolveBucket(String graphName, int partitionId) {
        if (!enabled) {
            return null;
        }

        Metapb.PartitionLease lease = getLease(graphName, partitionId);
        if (lease != null) {
            return pdProvider.resolvePartitionBucket(graphName, partitionId, storeId,
                                                    lease.getLeaseEpoch());
        }
        return null;
    }

    /**
     * Periodically renew leases that are about to expire.
     */
    private void renewExpiredLeases() {
        for (Map.Entry<String, PartitionLeaseState> entry : leaseStates.entrySet()) {
            PartitionLeaseState state = entry.getValue();
            if (state.shouldRenew()) {
                try {
                    Metapb.PartitionLease renewed = pdProvider.renewPartitionLease(
                            state.graphName,
                            state.partitionId,
                            storeId,
                            state.lease.getLeaseEpoch(),
                            leaseTtlSeconds
                    );
                    if (renewed != null) {
                        state.lease = renewed;
                        state.nextRenewTime = System.currentTimeMillis() +
                                              leaseRenewIntervalSeconds * 1000L;
                        LOG.debug("Renewed lease for partition {}#{}: new_epoch={}",
                                  state.graphName, state.partitionId,
                                  renewed.getLeaseEpoch());
                    }
                } catch (PDException e) {
                    LOG.warn("Failed to renew lease for partition {}#{}: {}",
                             state.graphName, state.partitionId, e.getMessage());
                }
            }
        }
    }

    /**
     * Clear all leases (typically before shutdown).
     */
    public void clearAll() {
        for (String key : leaseStates.keySet()) {
            PartitionLeaseState state = leaseStates.get(key);
            if (state != null) {
                releaseLease(state.graphName, state.partitionId);
            }
        }
        leaseStates.clear();
    }

    /**
     * Shutdown the lease manager.
     */
    public void shutdown() {
        clearAll();
        if (scheduledExecutor != null && !scheduledExecutor.isShutdown()) {
            scheduledExecutor.shutdown();
            try {
                if (!scheduledExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    scheduledExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduledExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Get the number of active leases.
     */
    public int getActiveLeaseCount() {
        return leaseStates.size();
    }

}
