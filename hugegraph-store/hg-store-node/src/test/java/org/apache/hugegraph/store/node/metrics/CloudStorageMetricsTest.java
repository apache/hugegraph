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

package org.apache.hugegraph.store.node.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.store.node.cloud.CloudStorageMetrics;
import org.apache.hugegraph.store.node.cloud.CloudStorageMetricsConst;
import org.apache.hugegraph.store.node.cloud.CloudSyncTracker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * Unit tests for {@link CloudStorageMetrics}.
 * Includes the former lifecycle/cardinality checks from
 * {@code CloudStorageMetricsLifecycleTest}.
 *
 * <p>Because {@link CloudStorageMetrics} uses static singleton state, each test
 * resets all static fields via reflection in {@link #tearDown()} so that tests
 * remain independent.
 */
public class CloudStorageMetricsTest {

    private SimpleMeterRegistry registry;
    private CloudSyncTracker syncTracker;

    @Before
    public void setUp() {
        registry = new SimpleMeterRegistry();
        syncTracker = new CloudSyncTracker();
        resetStaticState();
    }

    @After
    public void tearDown() {
        resetStaticState();
    }

    // -----------------------------------------------------------------------
    // init()
    // -----------------------------------------------------------------------

    @Test
    public void testInitRegistersGlobalMetrics() {
        CloudStorageMetrics.init(registry, syncTracker);

        assertNotNull("upload failures counter must be registered",
                registry.find(CloudStorageMetricsConst.UPLOAD_FAILURES).counter());
        assertNotNull("sync latency timer must be registered",
                registry.find(CloudStorageMetricsConst.SYNC_LATENCY_MS).timer());
        assertNotNull("retry queue size gauge must be registered",
                registry.find(CloudStorageMetricsConst.RETRY_QUEUE_SIZE).gauge());
    }

    @Test
    public void testInitIsIdempotentSecondCallIsNoOp() {
        CloudStorageMetrics.init(registry, syncTracker);

        // A second registry should be ignored
        SimpleMeterRegistry secondRegistry = new SimpleMeterRegistry();
        CloudStorageMetrics.init(secondRegistry, syncTracker);

        // Upload failures counter must still be registered on the first registry
        assertNotNull(registry.find(CloudStorageMetricsConst.UPLOAD_FAILURES).counter());
        // Nothing should be registered on the second registry
        assertEquals(0, secondRegistry.getMeters().size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitNullRegistryThrowsIllegalArgument() {
        CloudStorageMetrics.init(null, syncTracker);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitNullTrackerThrowsIllegalArgument() {
        CloudStorageMetrics.init(registry, null);
    }

    // -----------------------------------------------------------------------
    // registerDatabaseMetrics()
    // -----------------------------------------------------------------------

    @Test
    public void testRegisterDatabaseMetricsBeforeInitIsNoOp() {
        // No init — should not throw and registry must remain empty
        CloudStorageMetrics.registerDatabaseMetrics("db1");
        assertEquals(0, registry.getMeters().size());
    }

    @Test
    public void testRegisterDatabaseMetricsRegistersConfirmedFilesGauge() {
        CloudStorageMetrics.init(registry, syncTracker);
        CloudStorageMetrics.registerDatabaseMetrics("mydb");

        Gauge gauge = registry.find(CloudStorageMetricsConst.CONFIRMED_FILES)
                              .tag(CloudStorageMetricsConst.TAG_DB_NAME, "mydb")
                              .gauge();
        assertNotNull("confirmed_files gauge must be registered for the database", gauge);
    }

    @Test
    public void testRegisterDatabaseMetricsRegistersDeleteGuardReuploadGauge() {
        CloudStorageMetrics.init(registry, syncTracker);
        CloudStorageMetrics.registerDatabaseMetrics("mydb");

        Gauge gauge = registry.find(CloudStorageMetricsConst.DELETE_GUARD_REUPLOAD_COUNT)
                              .tag(CloudStorageMetricsConst.TAG_DB_NAME, "mydb")
                              .gauge();
        assertNotNull("delete_guard_reupload_count gauge must be registered for the database",
                gauge);
    }

    @Test
    public void testRegisterDatabaseMetricsIsIdempotentSecondCallIsNoOp() {
        CloudStorageMetrics.init(registry, syncTracker);
        CloudStorageMetrics.registerDatabaseMetrics("mydb");

        // Count meters after first registration
        int metersAfterFirst = registry.getMeters().size();

        // A second call for the same database should not add new meters
        CloudStorageMetrics.registerDatabaseMetrics("mydb");
        assertEquals(metersAfterFirst, registry.getMeters().size());
    }

    @Test
    public void testRegisterDatabaseMetricsMultipleDatabasesEachGetsSeparateGauges() {
        CloudStorageMetrics.init(registry, syncTracker);
        CloudStorageMetrics.registerDatabaseMetrics("db-alpha");
        CloudStorageMetrics.registerDatabaseMetrics("db-beta");

        assertNotNull(registry.find(CloudStorageMetricsConst.CONFIRMED_FILES)
                              .tag(CloudStorageMetricsConst.TAG_DB_NAME, "db-alpha").gauge());
        assertNotNull(registry.find(CloudStorageMetricsConst.CONFIRMED_FILES)
                              .tag(CloudStorageMetricsConst.TAG_DB_NAME, "db-beta").gauge());
    }

    // -----------------------------------------------------------------------
    // recordUploadFailure()
    // -----------------------------------------------------------------------

    @Test
    public void testRecordUploadFailureBeforeInitIsNoOp() {
        // Must not throw
        CloudStorageMetrics.recordUploadFailure("db1", "default", "timeout");
    }

    @Test
    public void testRecordUploadFailureIncrementsCounter() {
        CloudStorageMetrics.init(registry, syncTracker);

        CloudStorageMetrics.recordUploadFailure("db1", "default", "timeout");
        CloudStorageMetrics.recordUploadFailure("db1", "default", "auth");

        Counter counter = registry.find(CloudStorageMetricsConst.UPLOAD_FAILURES).counter();
        assertNotNull(counter);
        assertEquals(2.0, counter.count(), 0.001);
    }

    @Test
    public void testRecordUploadFailureMultipleCallsAccumulate() {
        CloudStorageMetrics.init(registry, syncTracker);

        for (int i = 0; i < 5; i++) {
            CloudStorageMetrics.recordUploadFailure("db1", "cf1", "error-" + i);
        }

        Counter counter = registry.find(CloudStorageMetricsConst.UPLOAD_FAILURES).counter();
        assertNotNull(counter);
        assertEquals(5.0, counter.count(), 0.001);
    }

    // -----------------------------------------------------------------------
    // recordSyncLatency()
    // -----------------------------------------------------------------------

    @Test
    public void testRecordSyncLatencyBeforeInitIsNoOp() {
        // Must not throw
        CloudStorageMetrics.recordSyncLatency("db1", 250L);
    }

    @Test
    public void testRecordSyncLatencyRecordsMeasurement() {
        CloudStorageMetrics.init(registry, syncTracker);

        CloudStorageMetrics.recordSyncLatency("db1", 100L);
        CloudStorageMetrics.recordSyncLatency("db1", 200L);

        Timer timer = registry.find(CloudStorageMetricsConst.SYNC_LATENCY_MS).timer();
        assertNotNull(timer);
        assertEquals(2, timer.count());
        assertEquals(300.0, timer.totalTime(TimeUnit.MILLISECONDS), 1.0);
    }

    @Test
    public void testRecordSyncLatencyZeroLatencyIsRecorded() {
        CloudStorageMetrics.init(registry, syncTracker);
        CloudStorageMetrics.recordSyncLatency("db1", 0L);

        Timer timer = registry.find(CloudStorageMetricsConst.SYNC_LATENCY_MS).timer();
        assertNotNull(timer);
        assertEquals(1, timer.count());
    }

    // -----------------------------------------------------------------------
    // getConfirmedFileCount()
    // -----------------------------------------------------------------------

    @Test
    public void testGetConfirmedFileCountBeforeInitReturnsZero() {
        assertEquals(0L, CloudStorageMetrics.getConfirmedFileCount("db1"));
    }

    @Test
    public void testGetConfirmedFileCountAfterInitDelegatesToSyncTracker() {
        // Confirm a file in the tracker then query via metrics
        syncTracker.markConfirmed("db1", "db1/000001.sst");
        syncTracker.markConfirmed("db1", "db1/000002.sst");

        CloudStorageMetrics.init(registry, syncTracker);

        assertEquals(2L, CloudStorageMetrics.getConfirmedFileCount("db1"));
    }

    @Test
    public void testGetConfirmedFileCountUnknownDbReturnsZero() {
        CloudStorageMetrics.init(registry, syncTracker);
        assertEquals(0L, CloudStorageMetrics.getConfirmedFileCount("no-such-db"));
    }

    // -----------------------------------------------------------------------
    // incrementDeleteGuardReupload() / getDeleteGuardReuploadCount()
    // -----------------------------------------------------------------------

    @Test
    public void testGetDeleteGuardReuploadCountUnknownDbReturnsZero() {
        assertEquals(0L, CloudStorageMetrics.getDeleteGuardReuploadCount("unknown-db"));
    }

    @Test
    public void testIncrementDeleteGuardReuploadIncrementsCountForDb() {
        CloudStorageMetrics.incrementDeleteGuardReupload("db1");
        CloudStorageMetrics.incrementDeleteGuardReupload("db1");
        CloudStorageMetrics.incrementDeleteGuardReupload("db1");

        assertEquals(3L, CloudStorageMetrics.getDeleteGuardReuploadCount("db1"));
    }

    @Test
    public void testIncrementDeleteGuardReuploadCountsAreIsolatedPerDb() {
        CloudStorageMetrics.incrementDeleteGuardReupload("db-a");
        CloudStorageMetrics.incrementDeleteGuardReupload("db-a");
        CloudStorageMetrics.incrementDeleteGuardReupload("db-b");

        assertEquals(2L, CloudStorageMetrics.getDeleteGuardReuploadCount("db-a"));
        assertEquals(1L, CloudStorageMetrics.getDeleteGuardReuploadCount("db-b"));
    }

    @Test
    public void testIncrementDeleteGuardReuploadBeforeRegisterDbStillWorks() {
        // Incrementing before registerDatabaseMetrics() should still track the count
        CloudStorageMetrics.incrementDeleteGuardReupload("db-new");
        assertEquals(1L, CloudStorageMetrics.getDeleteGuardReuploadCount("db-new"));
    }

    @Test
    public void testDeleteGuardReuploadGaugeReflectsLiveCount() {
        CloudStorageMetrics.init(registry, syncTracker);
        CloudStorageMetrics.registerDatabaseMetrics("mydb");

        CloudStorageMetrics.incrementDeleteGuardReupload("mydb");
        CloudStorageMetrics.incrementDeleteGuardReupload("mydb");

        Gauge gauge = registry.find(CloudStorageMetricsConst.DELETE_GUARD_REUPLOAD_COUNT)
                              .tag(CloudStorageMetricsConst.TAG_DB_NAME, "mydb")
                              .gauge();
        assertNotNull(gauge);
        assertEquals(2.0, gauge.value(), 0.001);
    }

    @Test
    public void testConfirmedFilesGaugeReflectsTrackerState() {
        CloudStorageMetrics.init(registry, syncTracker);
        CloudStorageMetrics.registerDatabaseMetrics("mydb");

        syncTracker.markConfirmed("mydb", "mydb/000001.sst");
        syncTracker.markConfirmed("mydb", "mydb/000002.sst");
        syncTracker.markConfirmed("mydb", "mydb/000003.sst");

        Gauge gauge = registry.find(CloudStorageMetricsConst.CONFIRMED_FILES)
                              .tag(CloudStorageMetricsConst.TAG_DB_NAME, "mydb")
                              .gauge();
        assertNotNull(gauge);
        assertEquals(3.0, gauge.value(), 0.001);
    }

    // -----------------------------------------------------------------------
    // setRetryQueueSize() — placeholder method, must not throw
    // -----------------------------------------------------------------------

    @Test
    public void testSetRetryQueueSizeDoesNotThrow() {
        CloudStorageMetrics.setRetryQueueSize(0);
        CloudStorageMetrics.setRetryQueueSize(42);
        CloudStorageMetrics.setRetryQueueSize(Integer.MAX_VALUE);
    }

    // -----------------------------------------------------------------------
    // unregisterDatabaseMetrics() — per-DB meter cleanup (cardinality bound)
    // -----------------------------------------------------------------------
    // Ensures per-database meters are removed when a DB is deleted, so meter cardinality does not
    // grow without bound as databases are created and destroyed (partition rebalancing, graph drops).

    @Test
    public void testPerDbMetricsAreRemovedOnUnregister() {
        CloudStorageMetrics.init(registry, syncTracker);
        CloudStorageMetrics.registerDatabaseMetrics("db-A");
        CloudStorageMetrics.registerDatabaseMetrics("db-B");

        assertEquals("both DBs must have per-DB meters registered",
                     2, registeredDbMetricCount());
        assertNotNull("db-A confirmed-files gauge must exist", confirmedFilesGaugeFor("db-A"));
        assertNotNull("db-B confirmed-files gauge must exist", confirmedFilesGaugeFor("db-B"));

        // Simulate DB deletion for db-A.
        CloudStorageMetrics.unregisterDatabaseMetrics("db-A");

        assertEquals("only db-B must remain after db-A is unregistered",
                     1, registeredDbMetricCount());
        assertNull("db-A gauge must be removed from the registry (no cardinality leak)",
                   confirmedFilesGaugeFor("db-A"));
        assertNotNull("db-B gauge must be untouched", confirmedFilesGaugeFor("db-B"));
    }

    @Test
    public void testUnregisterThenReregisterWorksForRecreatedDb() {
        CloudStorageMetrics.init(registry, syncTracker);
        CloudStorageMetrics.registerDatabaseMetrics("db-A");
        CloudStorageMetrics.unregisterDatabaseMetrics("db-A");
        assertNull(confirmedFilesGaugeFor("db-A"));

        // A DB recreated at the same name must get fresh metrics.
        CloudStorageMetrics.registerDatabaseMetrics("db-A");
        assertNotNull("recreated DB must re-register its metrics", confirmedFilesGaugeFor("db-A"));
        assertEquals(1, registeredDbMetricCount());
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Resets all static fields in {@link CloudStorageMetrics} to {@code null} / empty
     * via reflection so that each test starts from a clean slate.
     */
    @SuppressWarnings("unchecked")
    private static void resetStaticState() {
        try {
            setStaticField("registry");
            setStaticField("syncTracker");
            setStaticField("uploadFailuresCounter");
            setStaticField("syncLatencyTimer");

            Field mapField = CloudStorageMetrics.class
                    .getDeclaredField("deleteGuardReuploadPerDb");
            mapField.setAccessible(true);
            ((ConcurrentHashMap<String, AtomicLong>) mapField.get(null)).clear();

            Field perDbMetersField = CloudStorageMetrics.class.getDeclaredField("perDbMeters");
            perDbMetersField.setAccessible(true);
            ((java.util.Map<?, ?>) perDbMetersField.get(null)).clear();
        } catch (Exception e) {
            throw new RuntimeException("Failed to reset CloudStorageMetrics static state", e);
        }
    }

    private static void setStaticField(String fieldName)
            throws NoSuchFieldException, IllegalAccessException {
        Field field = CloudStorageMetrics.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(null, (Object) null);
    }

    private Gauge confirmedFilesGaugeFor(String dbName) {
        return registry.find(CloudStorageMetricsConst.CONFIRMED_FILES)
                       .tag(CloudStorageMetricsConst.TAG_DB_NAME, dbName)
                       .gauge();
    }

    /**
     * Number of databases with registered per-DB meters. Read via reflection on {@code perDbMeters}
     * because this test lives outside {@code CloudStorageMetrics}'s package and cannot call the
     * package-private {@code registeredDbMetricCountForTest()} helper.
     */
    private static int registeredDbMetricCount() {
        try {
            Field f = CloudStorageMetrics.class.getDeclaredField("perDbMeters");
            f.setAccessible(true);
            return ((java.util.Map<?, ?>) f.get(null)).size();
        } catch (Exception e) {
            throw new RuntimeException("Failed to read CloudStorageMetrics.perDbMeters", e);
        }
    }
}


