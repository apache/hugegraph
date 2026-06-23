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

package org.apache.hugegraph.pd.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.hugegraph.pd.PartitionService;
import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.meta.PartitionBucketRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for partition lease operations:
 * - acquire/renew/release happy path
 * - renew-too-early rejection
 * - auto cleanup of expired lease
 */
public class PartitionLeaseServiceTest extends PDCoreTestBase {

    private PartitionService partitionService;
    private StoreNodeService storeNodeService;
    private final String graphName = "test_graph";
    private final int partitionId = 0;
    private final long storeId = 1L;

    @Before
    public void setUp() throws PDException {
        partitionService = getPartitionService();
        storeNodeService = getStoreNodeService();

        ensureStoreAlive(1L, "127.0.0.1:8501");
        ensureStoreAlive(2L, "127.0.0.1:8502");
        ensureStoreAlive(3L, "127.0.0.1:8503");

        // Ensure test partition exists by triggering lazy creation via getPartitionByCode
        if (partitionService.getPartitionById(graphName, partitionId) == null) {
            // Create ShardGroup for the partition
            Metapb.Shard shard = Metapb.Shard.newBuilder()
                                           .setStoreId(storeId)
                                           .setRole(Metapb.ShardRole.Leader)
                                           .build();

            Metapb.ShardGroup shardGroup = Metapb.ShardGroup.newBuilder()
                                                           .setId(partitionId)
                                                           .setState(Metapb.PartitionState.PState_Normal)
                                                           .addAllShards(List.of(shard))
                                                           .build();
            storeNodeService.getStoreInfoMeta().updateShardGroup(shardGroup);

            // Trigger partition creation via code lookup
            // This will create partition 1 with the range for hash code 5000
            partitionService.getPartitionByCode(graphName, 5000);
        }
    }

    private boolean storeExists(long id) {
        try {
            return storeNodeService.getStore(id) != null;
        } catch (PDException ignored) {
            return false;
        }
    }

    private void ensureStoreAlive(long id, String address) throws PDException {
        if (!storeExists(id)) {
            Metapb.Store store = Metapb.Store.newBuilder()
                                            .setId(id)
                                            .setAddress(address)
                                            .setState(Metapb.StoreState.Up)
                                            .build();
            storeNodeService.getStoreInfoMeta().updateStore(store);
            storeNodeService.getStoreInfoMeta().keepStoreAlive(store);
            return;
        }
        Metapb.Store store = Metapb.Store.newBuilder(storeNodeService.getStore(id))
                                        .setState(Metapb.StoreState.Up)
                                        .build();
        storeNodeService.getStoreInfoMeta().updateStore(store);
        storeNodeService.getStoreInfoMeta().keepStoreAlive(store);
    }

    @After
    public void tearDown() {
        // Keep suite tests isolated: this test class creates active stores that
        // would otherwise leak into StoreServiceTest expectations.
        cleanupStore(1L);
        cleanupStore(2L);
        cleanupStore(3L);
        try {
            storeNodeService.getStoreInfoMeta().deleteShardGroup(partitionId);
        } catch (PDException ignored) {
            // Best-effort cleanup for shared in-process test services.
        }
    }

    private void cleanupStore(long id) {
        try {
            Metapb.Store store = storeNodeService.getStore(id);
            if (store != null) {
                storeNodeService.getStoreInfoMeta().removeActiveStore(store);
            }
        } catch (PDException ignored) {
            // Store not present is acceptable during cleanup.
        }
        try {
            storeNodeService.removeStore(id);
        } catch (PDException ignored) {
            // Store not present is acceptable during cleanup.
        }
    }

    /**
     * Test happy path: acquire -> renew -> release
     */
    @Test
    public void testAcquireRenewReleaseHappyPath() throws PDException, InterruptedException {
        // Step 1: Acquire lease
        Metapb.PartitionLease acquiredLease = partitionService.acquirePartitionLease(
                graphName, partitionId, storeId, 10);

        assertNotNull("Acquired lease should not be null", acquiredLease);
        assertEquals("Graph name should match", graphName, acquiredLease.getGraphName());
        assertEquals("Partition ID should match", partitionId, acquiredLease.getPartitionId());
        assertEquals("Store ID should match", storeId, acquiredLease.getLeaseOwnerStoreId());
        assertEquals("Initial epoch should be 1", 1L, acquiredLease.getLeaseEpoch());

        long acquiredEpoch = acquiredLease.getLeaseEpoch();
        long expireAt1 = acquiredLease.getLeaseExpireTimestamp();

        // Step 2: Wait for renew window (> 5 seconds before expiration, per LEASE_RENEW_WINDOW_MS=15s)
        // With a 10-second TTL, we can renew after 5+ seconds
        Thread.sleep(6000);

        // Step 3: Renew lease
        Metapb.PartitionLease renewedLease = partitionService.renewPartitionLease(
                graphName, partitionId, storeId, acquiredEpoch, 10);

        assertNotNull("Renewed lease should not be null", renewedLease);
        assertEquals("Graph name should match", graphName, renewedLease.getGraphName());
        assertEquals("Partition ID should match", partitionId, renewedLease.getPartitionId());
        assertEquals("Store ID should match", storeId, renewedLease.getLeaseOwnerStoreId());
        assertEquals("Epoch should remain the same", acquiredEpoch, renewedLease.getLeaseEpoch());

        long expireAt2 = renewedLease.getLeaseExpireTimestamp();
        org.junit.Assert.assertTrue("Renewed expiration should be later than original",
                                    expireAt2 > expireAt1);

        // Step 4: Release lease
        partitionService.releasePartitionLease(graphName, partitionId, storeId, acquiredEpoch);

        // Verify lease is removed
        Metapb.PartitionLease releasedLease = storeNodeService.getStoreInfoMeta()
                                                             .getPartitionLease(graphName,
                                                                               partitionId);
        assertNull("Lease should be removed after release", releasedLease);
    }

    /**
     * Test renew-too-early rejection
     */
    @Test
    public void testRenewTooEarlyRejection() throws PDException, InterruptedException {
        // Acquire lease with long TTL
        Metapb.PartitionLease acquiredLease = partitionService.acquirePartitionLease(
                graphName, partitionId, storeId, 30);

        assertNotNull("Acquired lease should not be null", acquiredLease);
        long leaseEpoch = acquiredLease.getLeaseEpoch();

        // Immediately try to renew (without waiting for renew window)
        try {
            partitionService.renewPartitionLease(graphName, partitionId, storeId, leaseEpoch, 30);
            fail("Should reject renew too early");
        } catch (PDException e) {
            org.junit.Assert.assertTrue("Error message should indicate renew too early",
                                       e.getMessage().contains("renew is too early"));
        }

        // Wait a bit and try again (but still within renew window)
        Thread.sleep(2000);

        try {
            partitionService.renewPartitionLease(graphName, partitionId, storeId, leaseEpoch, 30);
            fail("Should still reject renew too early");
        } catch (PDException e) {
            org.junit.Assert.assertTrue("Error should be lease conflict",
                                       e.getMessage().contains("lease") ||
                                       e.getMessage().contains("conflict"));
        }

        // Wait for renew window (> 15 seconds before expiration)
        Thread.sleep(16000);

        // Now renew should succeed
        Metapb.PartitionLease renewedLease = partitionService.renewPartitionLease(
                graphName, partitionId, storeId, leaseEpoch, 30);
        assertNotNull("Renewed lease should succeed after renew window", renewedLease);

        // Clean up
        partitionService.releasePartitionLease(graphName, partitionId, storeId, leaseEpoch);
    }

    /**
     * Test lease conflict on acquire with a different store
     */
    @Test
    public void testLeaseConflictOnAcquire() throws PDException {
        // Create a second store
        long storeId2 = 2L;
        Metapb.Store store2 = Metapb.Store.newBuilder()
                                         .setId(storeId2)
                                         .setAddress("127.0.0.1:8502")
                                         .setState(Metapb.StoreState.Up)
                                         .build();
        storeNodeService.getStoreInfoMeta().updateStore(store2);
        storeNodeService.getStoreInfoMeta().keepStoreAlive(store2);

        // First store acquires lease
        Metapb.PartitionLease lease1 = partitionService.acquirePartitionLease(
                graphName, partitionId, storeId, 30);
        assertNotNull("First store should acquire lease", lease1);

        // Second store tries to acquire the same partition lease
        try {
            partitionService.acquirePartitionLease(graphName, partitionId, storeId2, 30);
            fail("Should reject acquire for different store");
        } catch (PDException e) {
            org.junit.Assert.assertTrue("Error message should mention conflict",
                                       e.getMessage().contains("lease conflict"));
        }

        // Clean up
        partitionService.releasePartitionLease(graphName, partitionId, storeId,
                                              lease1.getLeaseEpoch());
    }

    /**
     * Test renew with invalid epoch
     */
    @Test
    public void testRenewWithInvalidEpoch() throws PDException, InterruptedException {
        // Acquire lease
        Metapb.PartitionLease lease = partitionService.acquirePartitionLease(
                graphName, partitionId, storeId, 10);
        assertNotNull("Lease should be acquired", lease);

        for (int i = 0; i < 6; i++) {
            Thread.sleep(1000);
            long now = System.currentTimeMillis();
            long remaining = lease.getLeaseExpireTimestamp() - now;
            if (remaining < 15000) {
                break;
            }
        }

        // Try renew with wrong epoch
        long wrongEpoch = lease.getLeaseEpoch() + 1;
        try {
            partitionService.renewPartitionLease(graphName, partitionId, storeId, wrongEpoch, 10);
            fail("Should reject renew with wrong epoch");
        } catch (PDException e) {
            org.junit.Assert.assertTrue("Error should mention conflict or stale",
                                       e.getMessage().contains("conflict") ||
                                       e.getMessage().contains("stale"));
        }

        // Clean up
        partitionService.releasePartitionLease(graphName, partitionId, storeId,
                                              lease.getLeaseEpoch());
    }

    /**
     * Test lease not found errors
     */
    @Test
    public void testLeaseNotFound() {
        int nonExistentPartitionId = 999;

        // Try renew on non-existent lease
        try {
            partitionService.renewPartitionLease(graphName, nonExistentPartitionId, storeId, 1L,
                                                 10);
            fail("Should get LEASE_NOT_FOUND error");
        } catch (PDException e) {
            org.junit.Assert.assertTrue("Error should mention 'not found'",
                                       e.getMessage().contains("not found"));
        }

        // Try release on non-existent lease
        try {
            partitionService.releasePartitionLease(graphName, nonExistentPartitionId, storeId, 1L);
            fail("Should get LEASE_NOT_FOUND error");
        } catch (PDException e) {
            org.junit.Assert.assertTrue("Error should mention 'not found'",
                                       e.getMessage().contains("not found"));
        }
    }

    /**
     * Test acquire after lease expiration
     */
    @Test
    public void testAcquireAfterExpiration() throws PDException, InterruptedException {
        // Acquire with short TTL (2 seconds)
        Metapb.PartitionLease lease1 = partitionService.acquirePartitionLease(
                graphName, partitionId, storeId, 2);
        assertNotNull("First lease should be acquired", lease1);
        long epoch1 = lease1.getLeaseEpoch();

        // Wait for expiration
        Thread.sleep(2500);

        // Second acquire with different store should succeed
        long storeId2 = 2L;
        Metapb.Store store2 = Metapb.Store.newBuilder()
                                         .setId(storeId2)
                                         .setAddress("127.0.0.1:8502")
                                         .setState(Metapb.StoreState.Up)
                                         .build();
        storeNodeService.getStoreInfoMeta().updateStore(store2);
        storeNodeService.getStoreInfoMeta().keepStoreAlive(store2);

        Metapb.PartitionLease lease2 = partitionService.acquirePartitionLease(
                graphName, partitionId, storeId2, 2);
        assertNotNull("Second lease should be acquired after expiration", lease2);
        assertEquals("Second lease should have higher or equal epoch",
                    lease2.getLeaseEpoch(), epoch1 + 1);

        // Clean up
        partitionService.releasePartitionLease(graphName, partitionId, storeId2,
                                              lease2.getLeaseEpoch());
    }

    /**
     * Test multiple acquire/renew/release cycles
     */
    @Test
    public void testMultipleLeaseCycles() throws PDException, InterruptedException {
        for (int cycle = 0; cycle < 3; cycle++) {
            // Acquire
            Metapb.PartitionLease lease = partitionService.acquirePartitionLease(
                    graphName, partitionId, storeId, 10);
            assertNotNull("Lease should be acquired in cycle " + cycle, lease);

            long leaseEpoch = lease.getLeaseEpoch();

            // Wait and renew
            Thread.sleep(6000);
            Metapb.PartitionLease renewed = partitionService.renewPartitionLease(
                    graphName, partitionId, storeId, leaseEpoch, 10);
            assertNotNull("Lease should be renewed in cycle " + cycle, renewed);
            assertEquals("Epoch should be preserved", leaseEpoch, renewed.getLeaseEpoch());

            // Release
            partitionService.releasePartitionLease(graphName, partitionId, storeId, leaseEpoch);

            // Verify lease is removed
            Metapb.PartitionLease after = storeNodeService.getStoreInfoMeta()
                                                         .getPartitionLease(graphName,
                                                                           partitionId);
            assertNull("Lease should be removed after release in cycle " + cycle, after);
        }
    }

    @Test
    public void testResolvePartitionBucketWithLeaseFence() throws PDException {
        String oldLayout = getPdConfig().getStore().getCloudBucketLayout();
        String oldPrefix = getPdConfig().getStore().getPerStoreBucketPrefix();
        try {
            getPdConfig().getStore().setCloudBucketLayout("per_store_migrating");
            getPdConfig().getStore().setPerStoreBucketPrefix("test-store-");

            Metapb.PartitionLease lease = partitionService.acquirePartitionLease(
                    graphName, partitionId, storeId, 30);

            String bucket = partitionService.resolvePartitionBucket(graphName,
                                                                   partitionId,
                                                                   storeId,
                                                                   lease.getLeaseEpoch());
            assertEquals("test-store-" + storeId, bucket);

            PartitionBucketRecord record = partitionService.getPartitionBucketRecord(graphName,
                                                                                      partitionId);
            assertNotNull(record);
            assertEquals(storeId, record.getOwnerStoreId());
            assertEquals(lease.getLeaseEpoch(), record.getLeaseEpoch());
            assertEquals(bucket, record.getBucket());

            try {
                partitionService.resolvePartitionBucket(graphName,
                                                       partitionId,
                                                       storeId,
                                                       lease.getLeaseEpoch() + 1);
                fail("should reject stale lease epoch");
            } catch (PDException e) {
                assertTrue(e.getMessage().contains("fenced"));
            }

            partitionService.releasePartitionLease(graphName,
                                                  partitionId,
                                                  storeId,
                                                  lease.getLeaseEpoch());
            assertNull(partitionService.getPartitionBucketRecord(graphName, partitionId));
        } finally {
            getPdConfig().getStore().setCloudBucketLayout(oldLayout);
            getPdConfig().getStore().setPerStoreBucketPrefix(oldPrefix);
        }
    }
}
















