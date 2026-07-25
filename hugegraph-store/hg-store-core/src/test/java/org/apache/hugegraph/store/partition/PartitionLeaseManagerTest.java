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

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.pd.PdProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for PartitionLeaseManager distributed rocksdb-cloud write fencing.
 */
public class PartitionLeaseManagerTest {

    private PdProvider pdProvider;
    private PartitionLeaseManager leaseManager;
    private static final long STORE_ID = 1L;
    private static final String GRAPH_NAME = "hugegraph";
    private static final int PARTITION_ID = 0;

    @Before
    public void setUp() {
        pdProvider = Mockito.mock(PdProvider.class);
    }

    @Test
    public void testLeaseAcquisition() throws PDException {
        Metapb.PartitionLease mockLease = Metapb.PartitionLease.newBuilder()
                                                               .setGraphName(GRAPH_NAME)
                                                               .setPartitionId(PARTITION_ID)
                                                               .setLeaseOwnerStoreId(STORE_ID)
                                                               .setLeaseEpoch(1L)
                                                               .setLeaseExpireTimestamp(
                                                                       System.currentTimeMillis() +
                                                                       30000)
                                                               .build();
        Mockito.when(pdProvider.acquirePartitionLease(GRAPH_NAME, PARTITION_ID, STORE_ID, 30))
               .thenReturn(mockLease);

        leaseManager = new PartitionLeaseManager(pdProvider, STORE_ID, true);

        Metapb.PartitionLease lease = leaseManager.acquireLease(GRAPH_NAME, PARTITION_ID);

        assertNotNull(lease);
        assertEquals(1L, lease.getLeaseEpoch());
        assertEquals(PARTITION_ID, lease.getPartitionId());

        verify(pdProvider, times(1)).acquirePartitionLease(GRAPH_NAME, PARTITION_ID, STORE_ID, 30);
    }

    @Test
    public void testLeaseRelease() throws PDException {
        Metapb.PartitionLease mockLease = Metapb.PartitionLease.newBuilder()
                                                               .setGraphName(GRAPH_NAME)
                                                               .setPartitionId(PARTITION_ID)
                                                               .setLeaseOwnerStoreId(STORE_ID)
                                                               .setLeaseEpoch(1L)
                                                               .setLeaseExpireTimestamp(
                                                                       System.currentTimeMillis() +
                                                                       30000)
                                                               .build();
        Mockito.when(pdProvider.acquirePartitionLease(GRAPH_NAME, PARTITION_ID, STORE_ID, 30))
               .thenReturn(mockLease);

        leaseManager = new PartitionLeaseManager(pdProvider, STORE_ID, true);

        // Acquire first
        Metapb.PartitionLease lease = leaseManager.acquireLease(GRAPH_NAME, PARTITION_ID);
        assertNotNull(lease);

        // Then release
        leaseManager.releaseLease(GRAPH_NAME, PARTITION_ID);

        // Verify release was called on PD
        verify(pdProvider, times(1)).releasePartitionLease(GRAPH_NAME, PARTITION_ID, STORE_ID,
                                                           1L);

        // After release, lease should be removed
        Metapb.PartitionLease releasedLease = leaseManager.getLease(GRAPH_NAME, PARTITION_ID);
        assertNull(releasedLease);
    }

    @Test
    public void testBucketResolution() throws PDException {
        Metapb.PartitionLease mockLease = Metapb.PartitionLease.newBuilder()
                                                               .setGraphName(GRAPH_NAME)
                                                               .setPartitionId(PARTITION_ID)
                                                               .setLeaseOwnerStoreId(STORE_ID)
                                                               .setLeaseEpoch(1L)
                                                               .setLeaseExpireTimestamp(
                                                                       System.currentTimeMillis() +
                                                                       30000)
                                                               .build();
        Mockito.when(pdProvider.acquirePartitionLease(GRAPH_NAME, PARTITION_ID, STORE_ID, 30))
               .thenReturn(mockLease);
        Mockito.when(pdProvider.resolvePartitionBucket(GRAPH_NAME, PARTITION_ID, STORE_ID, 1L))
               .thenReturn("store-1-partition-0");

        leaseManager = new PartitionLeaseManager(pdProvider, STORE_ID, true);

        // Acquire lease
        Metapb.PartitionLease lease = leaseManager.acquireLease(GRAPH_NAME, PARTITION_ID);
        assertNotNull(lease);

        // Resolve bucket
        String bucket = leaseManager.resolveBucket(GRAPH_NAME, PARTITION_ID);
        assertEquals("store-1-partition-0", bucket);

        verify(pdProvider, times(1)).resolvePartitionBucket(GRAPH_NAME, PARTITION_ID, STORE_ID,
                                                            1L);
    }

    @Test
    public void testDisabledLeaseManager() throws PDException {
        leaseManager = new PartitionLeaseManager(pdProvider, STORE_ID, false);

        // When disabled, lease operations should be no-ops
        Metapb.PartitionLease lease = leaseManager.acquireLease(GRAPH_NAME, PARTITION_ID);
        assertNull(lease);

        leaseManager.releaseLease(GRAPH_NAME, PARTITION_ID);

        // No PD calls should be made
        Mockito.verify(pdProvider, times(0)).acquirePartitionLease(anyString(), anyInt(),
                                                                  anyLong(), anyInt());
        Mockito.verify(pdProvider, times(0)).releasePartitionLease(anyString(), anyInt(),
                                                                   anyLong(), anyLong());
    }

    @Test
    public void testLeaseAcquisitionException() throws PDException {
        Mockito.when(pdProvider.acquirePartitionLease(GRAPH_NAME, PARTITION_ID, STORE_ID, 30))
               .thenThrow(new PDException(1, "PD internal error"));

        leaseManager = new PartitionLeaseManager(pdProvider, STORE_ID, true);

        // Should handle exception gracefully
        Metapb.PartitionLease lease = leaseManager.acquireLease(GRAPH_NAME, PARTITION_ID);
        assertNull(lease);

        // Verify PD was called once
        verify(pdProvider, times(1)).acquirePartitionLease(GRAPH_NAME, PARTITION_ID, STORE_ID, 30);
    }

    @Test
    public void testActiveLeaseCount() throws PDException {
        Metapb.PartitionLease mockLease = Metapb.PartitionLease.newBuilder()
                                                               .setGraphName(GRAPH_NAME)
                                                               .setPartitionId(PARTITION_ID)
                                                               .setLeaseOwnerStoreId(STORE_ID)
                                                               .setLeaseEpoch(1L)
                                                               .setLeaseExpireTimestamp(
                                                                       System.currentTimeMillis() +
                                                                       30000)
                                                               .build();
        Mockito.when(pdProvider.acquirePartitionLease(anyString(), anyInt(), anyLong(),
                                                     anyInt()))
               .thenReturn(mockLease);

        leaseManager = new PartitionLeaseManager(pdProvider, STORE_ID, true);

        assertEquals(0, leaseManager.getActiveLeaseCount());

        // Acquire leases for 3 partitions
        leaseManager.acquireLease(GRAPH_NAME, 0);
        leaseManager.acquireLease(GRAPH_NAME, 1);
        leaseManager.acquireLease(GRAPH_NAME, 2);

        assertEquals(3, leaseManager.getActiveLeaseCount());

        // Release one
        leaseManager.releaseLease(GRAPH_NAME, 0);
        assertEquals(2, leaseManager.getActiveLeaseCount());

        // Clear all
        leaseManager.clearAll();
        assertEquals(0, leaseManager.getActiveLeaseCount());
    }
}

