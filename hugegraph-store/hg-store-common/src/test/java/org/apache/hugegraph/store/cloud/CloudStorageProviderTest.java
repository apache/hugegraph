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
import static org.junit.Assert.assertThrows;
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

    @Test
    public void testDefaultDeletePrefixDeletesAllListedKeys() throws IOException {
        java.util.List<String> deleted = new java.util.ArrayList<>();

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
                deleted.add(remoteKey);
            }

            @Override
            public boolean fileExists(String remoteKey) {
                return false;
            }

            @Override
            public List<String> listFiles(String remoteDirPrefix) {
                return java.util.Arrays.asList("a.sst", "b.sst", "c.sst");
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

        int deletedCount = provider.deletePrefix("some/prefix");

        assertEquals(3, deletedCount);
        assertEquals(3, deleted.size());
        assertTrue(deleted.contains("a.sst"));
        assertTrue(deleted.contains("b.sst"));
        assertTrue(deleted.contains("c.sst"));
    }

     @Test
     public void testDefaultDeletePrefixContinuesOnDeleteFailure() throws IOException {
         java.util.List<String> attempted = new java.util.ArrayList<>();

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
             public void deleteFile(String remoteKey) throws IOException {
                 attempted.add(remoteKey);
                 if ("b.sst".equals(remoteKey)) {
                     throw new IOException("simulated delete failure");
                 }
             }

             @Override
             public boolean fileExists(String remoteKey) {
                 return false;
             }

             @Override
             public List<String> listFiles(String remoteDirPrefix) {
                 return java.util.Arrays.asList("a.sst", "b.sst", "c.sst");
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

         // The default impl attempts every key (continue-on-failure) but MUST surface a partial
         // failure by throwing, so callers such as purgeRemotePrefix can preserve the tombstone
         // guard instead of treating an incomplete purge as success.
         IOException ex = assertThrows(IOException.class,
                                       () -> provider.deletePrefix("some/prefix"));
         assertTrue("Exception must name the failed key: " + ex.getMessage(),
                    ex.getMessage().contains("b.sst"));

         // All three keys were still attempted (a failure did not short-circuit later deletes).
         assertEquals(3, attempted.size());
         assertEquals("a.sst", attempted.get(0));
         assertEquals("b.sst", attempted.get(1));
         assertEquals("c.sst", attempted.get(2));
     }

     @Test
     public void testDeletePrefixWithEmptyList() throws IOException {
         CloudStorageProvider provider = new CloudStorageProvider() {
             @Override
             public String providerName() {
                 return "test";
             }

             @Override
             public void init(CloudStorageConfig config) {
             }

             @Override
             public void uploadFile(String localPath, String remoteKey) {
             }

             @Override
             public void deleteFile(String remoteKey) {
             }

             @Override
             public boolean fileExists(String remoteKey) {
                 return false;
             }

             @Override
             public List<String> listFiles(String remoteDirPrefix) {
                 return java.util.Collections.emptyList();
             }

             @Override
             public void downloadFile(String remoteKey, String localPath) {
             }

             @Override
             public void close() {
             }
         };

         int deletedCount = provider.deletePrefix("some/prefix");

         assertEquals(0, deletedCount);
     }

     @Test
     public void testListFilesDefaultWithLargeDataset() throws IOException {
         CloudStorageProvider provider = new CloudStorageProvider() {
             @Override
             public String providerName() {
                 return "test";
             }

             @Override
             public void init(CloudStorageConfig config) {
             }

             @Override
             public void uploadFile(String localPath, String remoteKey) {
             }

             @Override
             public void deleteFile(String remoteKey) {
             }

             @Override
             public boolean fileExists(String remoteKey) {
                 return false;
             }

             @Override
             public List<String> listFiles(String remoteDirPrefix) {
                 // Simulate large dataset
                 java.util.List<String> files = new java.util.ArrayList<>();
                 for (int i = 0; i < 1000; i++) {
                     files.add("file" + i + ".sst");
                 }
                 return files;
             }

             @Override
             public void downloadFile(String remoteKey, String localPath) {
             }

             @Override
             public void close() {
             }
         };

         List<String> result = provider.listFiles("some/prefix");

         assertEquals(1000, result.size());
         assertTrue(result.contains("file0.sst"));
         assertTrue(result.contains("file999.sst"));
     }
}

