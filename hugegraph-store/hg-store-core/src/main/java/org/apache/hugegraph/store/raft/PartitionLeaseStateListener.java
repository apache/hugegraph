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

package org.apache.hugegraph.store.raft;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftException;

import lombok.extern.slf4j.Slf4j;

import org.apache.hugegraph.store.partition.PartitionLeaseManager;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

/**
 * Implements RaftStateListener to manage partition leases during leadership transitions.
 * When rocksdb-cloud is enabled in distributed mode:
 * - onLeaderStart(): Acquires leases when this store becomes the partition leader
 * - onLeaderStop(): Releases leases when this store loses leadership
 * - Other state changes are ignored for lease management
 */
@Slf4j
public class PartitionLeaseStateListener implements RaftStateListener {

    private static final Logger LOG = Log.logger(PartitionLeaseStateListener.class);

    private final String graphName;
    private final int partitionId;
    private final PartitionLeaseManager leaseManager;

    /**
     * Create a listener for a specific partition's lease lifecycle.
     *
     * @param graphName    the graph name
     * @param partitionId  the partition ID
     * @param leaseManager the lease manager for this partition
     */
    public PartitionLeaseStateListener(String graphName, int partitionId,
                                       PartitionLeaseManager leaseManager) {
        this.graphName = graphName;
        this.partitionId = partitionId;
        this.leaseManager = leaseManager;
    }

    /**
     * Called when current node becomes leader - acquire the partition lease.
     */
    @Override
    public void onLeaderStart(long newTerm) {
        if (leaseManager == null || !leaseManager.isEnabled()) {
            return;
        }

        try {
            LOG.info("Partition {}#{} became leader in term {}. Acquiring lease...",
                     graphName, partitionId, newTerm);
            var lease = leaseManager.acquireLease(graphName, partitionId);
            if (lease != null) {
                long ttlMs = lease.getLeaseExpireTimestamp() - System.currentTimeMillis();
                LOG.info("Successfully acquired lease for {}#{}: epoch={}, expires_in_ms={}",
                         graphName, partitionId, lease.getLeaseEpoch(), ttlMs);
            } else {
                LOG.warn("Failed to acquire lease for {}#{}", graphName, partitionId);
            }
        } catch (Exception e) {
            LOG.error("Exception while acquiring lease for {}#{}: {}",
                      graphName, partitionId, e.getMessage(), e);
        }
    }

    /**
     * Called when current node loses leadership - release the partition lease.
     */
    @Override
    public void onLeaderStop(long oldTerm) {
        if (leaseManager == null || !leaseManager.isEnabled()) {
            return;
        }

        try {
            LOG.info("Partition {}#{} lost leadership in term {}. Releasing lease...",
                     graphName, partitionId, oldTerm);
            leaseManager.releaseLease(graphName, partitionId);
            LOG.info("Released lease for {}#{}", graphName, partitionId);
        } catch (Exception e) {
            LOG.error("Exception while releasing lease for {}#{}: {}",
                      graphName, partitionId, e.getMessage(), e);
        }
    }

    /**
     * Called when starting to follow a new leader (partition loss event).
     * Release the lease if this store still holds it.
     */
    @Override
    public void onStartFollowing(PeerId newLeaderId, long newTerm) {
        if (leaseManager == null || !leaseManager.isEnabled()) {
            return;
        }

        var currentLease = leaseManager.getLease(graphName, partitionId);
        if (currentLease != null) {
            LOG.debug("Partition {}#{} starting to follow new leader {}. Releasing lease to avoid conflicts.",
                      graphName, partitionId, newLeaderId);
            try {
                leaseManager.releaseLease(graphName, partitionId);
            } catch (Exception e) {
                LOG.warn("Exception releasing lease during follow transition for {}#{}: {}",
                         graphName, partitionId, e.getMessage());
            }
        }
    }

    @Override
    public void onStopFollowing(PeerId oldLeaderId, long oldTerm) {
        // No action needed when stopping to follow a leader
    }

    @Override
    public void onConfigurationCommitted(Configuration conf) {
        // No action needed for configuration changes
    }

    @Override
    public void onDataCommitted(long index) {
        // No action needed for data commit milestones
    }

    @Override
    public void onError(RaftException e) {
        LOG.error("Raft error detected for partition {}#{}: {}",
                  graphName, partitionId, e.getMessage(), e);
    }
}

