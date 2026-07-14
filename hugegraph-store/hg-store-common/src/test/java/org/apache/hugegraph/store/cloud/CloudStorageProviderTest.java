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

package org.apache.hugegraph.store.cloud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

/**
 * Tests for CloudStorageProvider interface default implementations.
 */
@SuppressWarnings("resource")
public class CloudStorageProviderTest {

    /**
     * Test that the default listFiles() implementation returns an empty list
     */
    @Test
    public void testDefaultListFilesReturnsEmptyList() throws IOException {
        // Create a minimal implementation that uses default listFiles
        CloudStorageProvider provider = new CloudStorageProvider() {
            @Override
            public String providerName() {
                return "test";
            }

            @Override
            public void init(CloudStorageConfig config) {
                // No-op
            }

            @Override
            public void uploadFile(String localPath, String remoteKey) {
                // No-op
            }

            @Override
            public void deleteFile(String remoteKey) {
                // No-op
            }

            @Override
            public boolean fileExists(String remoteKey) {
                return false;
            }

            @Override
            public void downloadFile(String remoteKey, String localPath) {
                // No-op
            }

            @Override
            public void close() {
                // No-op
            }
        };

        // Call the default listFiles method
        List<String> result = provider.listFiles("some/prefix");

        // Verify it returns an empty list (the default implementation)
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    /**
     * Test that listFiles default implementation can be overridden
     */
    @Test
    public void testListFilesCanBeOverridden() throws IOException {
        CloudStorageProvider provider = new CloudStorageProvider() {
            @Override
            public String providerName() {
                return "test";
            }

            @Override
            public void init(CloudStorageConfig config) {
                // No-op
            }

            @Override
            public void uploadFile(String localPath, String remoteKey) {
                // No-op
            }

            @Override
            public void deleteFile(String remoteKey) {
                // No-op
            }

            @Override
            public boolean fileExists(String remoteKey) {
                return false;
            }

            @Override
            public List<String> listFiles(String remoteDirPrefix) {
                // Override with custom implementation
                java.util.List<String> result = new java.util.ArrayList<>();
                result.add("file1.sst");
                result.add("file2.sst");
                return result;
            }

            @Override
            public void downloadFile(String remoteKey, String localPath) {
                // No-op
            }

            @Override
            public void close() {
                // No-op
            }
        };

        List<String> result = provider.listFiles("some/prefix");

        assertEquals(2, result.size());
        assertTrue(result.contains("file1.sst"));
        assertTrue(result.contains("file2.sst"));
    }
}

