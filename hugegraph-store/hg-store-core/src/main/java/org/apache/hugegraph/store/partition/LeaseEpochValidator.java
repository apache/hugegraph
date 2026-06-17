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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import lombok.extern.slf4j.Slf4j;

/**
 * Enforces write fencing using partition lease epochs in distributed rocksdb-cloud mode.
 * When a partition has a valid lease, all writes must use the same lease epoch.
 * This prevents stale leaders from writing data after losing the lease.
 * Lease epochs are incremented on every lease acquisition/renewal by PD.
 * Write requests that include a stale epoch are rejected with LeaseExpired error.
 */
@Slf4j
public class LeaseEpochValidator {

    private static final Logger LOG = Log.logger(LeaseEpochValidator.class);

    private final PartitionLeaseManager leaseManager;
    private final Map<String, EpochFencingState> partitionState = new ConcurrentHashMap<>();

    /**
     * State tracking for lease epochs on a per-partition basis.
     */
    static class EpochFencingState {
        String graphName;
        int partitionId;
        long currentEpoch;
        long epochUpdateTime;

        EpochFencingState(String graphName, int partitionId, long epoch) {
            this.graphName = graphName;
            this.partitionId = partitionId;
            this.currentEpoch = epoch;
            this.epochUpdateTime = System.currentTimeMillis();
        }

        void updateEpoch(long newEpoch) {
            this.currentEpoch = newEpoch;
            this.epochUpdateTime = System.currentTimeMillis();
        }

    }

    /**
     * Create a lease epoch validator.
     *
     * @param leaseManager the partition lease manager
     */
    public LeaseEpochValidator(PartitionLeaseManager leaseManager) {
        this.leaseManager = leaseManager;
    }

    /**
     * Validate a write operation's lease epoch.
     *
     * @param graphName    graph name
     * @param partitionId  partition ID
     * @param clientEpoch  epoch provided by the write client (may be 0 if no lease info)
     * @return true if the write is allowed; false if epoch mismatch (lease expired)
     */
    public boolean validateWriteEpoch(String graphName, int partitionId, long clientEpoch) {
        if (leaseManager == null || !leaseManager.isEnabled()) {
            // Lease fencing disabled, allow all writes
            return true;
        }

        String key = partitionKey(graphName, partitionId);

        // Check if current store has a valid lease for this partition
        var lease = leaseManager.getLease(graphName, partitionId);
        if (lease == null) {
            // No active lease for this partition
            // This is OK - may be a follower or partition just assigned
            LOG.debug("No active lease for partition {}, allowing write without epoch check", key);
            return true;
        }

        long leaseEpoch = lease.getLeaseEpoch();

        // If client provided epoch 0, this is a first write after becoming leader
        // Update our tracking state with the new epoch
        if (clientEpoch == 0) {
            updatePartitionEpoch(graphName, partitionId, leaseEpoch);
            LOG.debug("First write for partition {} with new lease epoch = {}",
                     key, leaseEpoch);
            return true;
        }

        // Validate the client's epoch matches what we're currently authorizing
        if (clientEpoch != leaseEpoch) {
            LOG.warn("Lease epoch mismatch for partition {}: client={}, lease={} " +
                    "(write rejected - client epoch is stale or from different store)",
                    key, clientEpoch, leaseEpoch);
            return false;
        }

        LOG.debug("Write epoch validated for partition {}: epoch = {}", key, leaseEpoch);
        return true;
    }

    /**
     * Get the current valid lease epoch for a partition on this store.
     *
     * @param graphName   graph name
     * @param partitionId partition ID
     * @return current lease epoch, or -1 if no valid lease
     */
    public long getCurrentLeaseEpoch(String graphName, int partitionId) {
        if (leaseManager == null || !leaseManager.isEnabled()) {
            return -1;
        }

        var lease = leaseManager.getLease(graphName, partitionId);
        if (lease != null) {
            return lease.getLeaseEpoch();
        }
        return -1;
    }

    /**
     * Validate snapshot write must use current lease epoch.
     *
     * @param graphName   graph name
     * @param partitionId partition ID
     * @return current lease epoch for snapshot, or 0 if no lease (snapshot allowed)
     */
    public long getSnapshotEpoch(String graphName, int partitionId) {
        long epoch = getCurrentLeaseEpoch(graphName, partitionId);
        if (epoch < 0) {
            // No lease, allow snapshot without epoch fencing
            return 0;
        }
        return epoch;
    }

    /**
     * Validate checkpoint can be written with current lease.
     *
     * @param graphName   graph name
     * @param partitionId partition ID
     * @return true if checkpoint is allowed
     */
    public boolean canCheckpoint(String graphName, int partitionId) {
        if (leaseManager == null || !leaseManager.isEnabled()) {
            return true;
        }

        var lease = leaseManager.getLease(graphName, partitionId);
        boolean allowed = lease != null;

        if (!allowed) {
            LOG.debug(
                    "Checkpoint rejected: no active lease for partition {}/{}",
                    graphName, partitionId);
        }
        return allowed;
    }

    /**
     * Handle lease expiration - clear cached epoch for partition.
     *
     * @param graphName   graph name
     * @param partitionId partition ID
     */
    public void onLeaseExpired(String graphName, int partitionId) {
        String key = partitionKey(graphName, partitionId);
        EpochFencingState state = partitionState.remove(key);
        if (state != null) {
            LOG.info("Lease expired for partition {}: cleared cached epoch {}",
                    key, state.currentEpoch);
        }
    }

    /**
     * Handle lease release - clear cached epoch for partition.
     *
     * @param graphName   graph name
     * @param partitionId partition ID
     */
    public void onLeaseReleased(String graphName, int partitionId) {
        String key = partitionKey(graphName, partitionId);
        EpochFencingState state = partitionState.remove(key);
        if (state != null) {
            LOG.info("Lease released for partition {}: cleared cached epoch {}",
                    key, state.currentEpoch);
        }
    }

    /**
     * Clear all cached epoch state (typically on shutdown).
     */
    public void clearAll() {
        partitionState.clear();
        LOG.info("Cleared all cached lease epoch states");
    }

    /**
     * Get epoch fencing statistics for monitoring.
     *
     * @return map of partition keys to current cached epochs
     */
    public Map<String, Long> getEpochStats() {
        Map<String, Long> stats = new HashMap<>();
        for (var entry : partitionState.entrySet()) {
            stats.put(entry.getKey(), entry.getValue().currentEpoch);
        }
        return stats;
    }

    private void updatePartitionEpoch(String graphName, int partitionId, long epoch) {
        String key = partitionKey(graphName, partitionId);
        partitionState.compute(key, (k, v) -> {
            if (v == null) {
                return new EpochFencingState(graphName, partitionId, epoch);
            } else {
                v.updateEpoch(epoch);
                return v;
            }
        });
    }

    private String partitionKey(String graphName, int partitionId) {
        return graphName + "#" + partitionId;
    }
}

