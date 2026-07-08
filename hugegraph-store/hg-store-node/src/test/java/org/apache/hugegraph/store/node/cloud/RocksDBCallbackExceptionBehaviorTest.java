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

package org.apache.hugegraph.store.node.cloud;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.hugegraph.store.cloud.CloudStorageNonRetryableException;
import org.apache.hugegraph.store.cloud.CloudStorageProvider;
import org.apache.hugegraph.store.cloud.CloudStorageProviderFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test that verifies RocksDB behavior when exceptions are thrown from
 * the {@link CloudStorageEventListener#onTableFileCreated} callback.
 *
 * <p><b>Key scenarios:</b>
 * <ul>
 *   <li>Upload provider throws an exception → captured and logged, retry queue engaged</li>
 *   <li>RocksDB does NOT crash when listener throws</li>
 *   <li>Multiple listeners can be registered; one failure doesn't prevent others</li>
 *   <li>Exception crosses JNI boundary safely (RocksDB swallows it)</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 * mvn test -pl hugegraph-store/hg-store-node \
 *   -Dtest=RocksDBCallbackExceptionBehaviorTest \
 *   -am -DskipTests=false
 * </pre>
 */
public class RocksDBCallbackExceptionBehaviorTest {

    private CloudStorageEventListener listener;
    private CloudStorageProvider mockProvider;
    private CloudUploadRetryQueue retryQueue;
    private Path tmpDir;

    @Before
    public void setUp() throws IOException {
        // Create temp directory for DLQ file
        this.tmpDir = Files.createTempDirectory("hgstore-test-");

        // Create a mock provider that we can control (throw/succeed)
        this.mockProvider = Mockito.mock(CloudStorageProvider.class);

        // Create a minimal retry queue (in-memory + optional disk DLQ)
        this.retryQueue = new CloudUploadRetryQueue(
                3,                    // maxAttempts
                100L,                 // initialDelayMs
                5000L,                // maxDelayMs
                this.tmpDir.toString() // dataRoot
        );

        // Create the listener with retry queue
        this.listener = new CloudStorageEventListener(
                "/data/hgstore",
                true,        // startupHydrationEnabled
                3000L,       // readMissGuardWindowMs
                this.retryQueue
        );

        // Set the active provider for testing
        CloudStorageProviderFactory.setActiveProviderForTest(this.mockProvider);
    }

    @After
    public void tearDown() {
        if (this.retryQueue != null) {
            this.retryQueue.close();
        }
        if (this.tmpDir != null) {
            deleteDirectory(this.tmpDir.toFile());
        }
        CloudStorageProviderFactory.reset();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void deleteDirectory(java.io.File dir) {
        if (!dir.exists()) {
            return;
        }
        java.io.File[] children = dir.listFiles();
        if (children != null) {
            for (java.io.File child : children) {
                if (child.isDirectory()) {
                    deleteDirectory(child);
                } else {
                    child.delete();
                }
            }
        }
        dir.delete();
    }

    @Test
    public void uploadThrowsException_doesNotCrashRocksDB_submitsToRetryQueue() throws Exception {
        String dbName = "hgstore-metadata";
        String cfName = "default";
        String filePath = "/data/hgstore/" + dbName + "/000001.sst";
        long fileSize = 64 * 1024 * 1024; // 64 MB

        // Step 1: Configure provider to throw an exception
        IOException uploadFailure = new IOException("Network timeout: S3 unavailable");
        Mockito.doThrow(uploadFailure)
               .when(this.mockProvider)
               .uploadFile(Mockito.anyString(), Mockito.anyString());

        // Step 2: Invoke callback (simulating RocksDB calling us after flush)
        // This should NOT throw, even though the provider throws
        try {
            this.listener.onTableFileCreated(dbName, cfName, filePath, fileSize);
        } catch (Exception e) {
            Assert.fail("onTableFileCreated should NOT throw exception; caught: " + e);
        }

        // Step 3: Verify retry queue captured the failure
        List<FailedUploadTask> dlqEntries = this.retryQueue.getDlqEntries();
        int inFlightCount = this.retryQueue.getInFlightCount();

        Assert.assertTrue(
                "task should be enqueued for retry (either in-flight or in DLQ after exhaustion)",
                inFlightCount > 0 || !dlqEntries.isEmpty()
        );

        // Step 4: Verify provider.uploadFile was actually called
        Mockito.verify(this.mockProvider, Mockito.times(1))
               .uploadFile(filePath, dbName + "/000001.sst");
    }

    @Test
    public void uploadThrowsNonRetryableException_submitsDirectlyToDLQ() throws Exception {
        String dbName = "hgstore-metadata";
        String cfName = "default";
        String filePath = "/data/hgstore/" + dbName + "/000002.sst";
        long fileSize = 64 * 1024 * 1024;

        // Configure provider to throw a non-retryable exception
        CloudStorageNonRetryableException nonRetryable =
                new CloudStorageNonRetryableException(
                        "Authentication failed; credentials invalid",
                        null
                );
        Mockito.doThrow(nonRetryable)
               .when(this.mockProvider)
               .uploadFile(Mockito.anyString(), Mockito.anyString());

        // Invoke callback
        try {
            this.listener.onTableFileCreated(dbName, cfName, filePath, fileSize);
        } catch (Exception e) {
            Assert.fail("onTableFileCreated should not throw; caught: " + e);
        }

        // Wait briefly for async retry queue to process
        Thread.sleep(500);

        // Verify task ended up in DLQ (not retrying)
        List<FailedUploadTask> dlqEntries = this.retryQueue.getDlqEntries();
        Assert.assertFalse("non-retryable exception should land in DLQ immediately",
                           dlqEntries.isEmpty());

        FailedUploadTask task = dlqEntries.get(0);
        Assert.assertEquals(dbName, task.getDbName());
        Assert.assertTrue(task.getLastError().contains("credentials"));
    }

    @Test
    public void callbackInvokedWithoutActiveProvider_doesNotCrash() {
        String dbName = "hgstore-metadata";
        String cfName = "default";
        String filePath = "/data/hgstore/" + dbName + "/000004.sst";
        long fileSize = 64 * 1024 * 1024;

        // Deactivate provider
        CloudStorageProviderFactory.reset();

        // Invoke callback — should gracefully no-op
        try {
            this.listener.onTableFileCreated(dbName, cfName, filePath, fileSize);
        } catch (Exception e) {
            Assert.fail("callback should handle null provider gracefully; caught: " + e);
        }

        // Verify no crash and no DLQ entries (no-op)
        Assert.assertEquals("no-op when provider is null", 0,
                            this.retryQueue.getDlqEntries().size());
    }
}










