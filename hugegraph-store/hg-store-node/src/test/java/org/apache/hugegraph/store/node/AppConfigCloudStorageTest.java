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

package org.apache.hugegraph.store.node;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.hugegraph.store.cloud.CloudStorageConfig;
import org.junit.Before;
import org.junit.Test;
import org.springframework.mock.env.MockEnvironment;

public class AppConfigCloudStorageTest {

    private AppConfig.CloudStorageSpringConfig springConfig;
    private MockEnvironment mockEnv;

    @Before
    public void setUp() {
        AppConfig appConfig = new AppConfig();
        springConfig = appConfig.new CloudStorageSpringConfig();
        mockEnv = new MockEnvironment();
        springConfig.setEnvironment(mockEnv);
    }

    /**
     * Common fields are bound from {@code cloud.storage.*} and converted correctly.
     * Provider-specific properties flow from the environment through the provider sub-namespace.
     */
    @Test
    public void testCloudStorageSpringConfigConversion() {
        springConfig.setEnabled(true);
        springConfig.setProvider("s3");
        springConfig.setPathPrefix("test-prefix");
        springConfig.setStartupHydrationEnabled(false);
        springConfig.setReadMissGuardWindowMs(5000L);

        // Simulate cloud.storage.s3.* properties being present in the Spring Environment
        mockEnv.setProperty("cloud.storage.s3.bucket", "test-bucket");
        mockEnv.setProperty("cloud.storage.s3.region", "us-west-2");
        mockEnv.setProperty("cloud.storage.s3.endpoint", "https://s3.example.com");
        mockEnv.setProperty("cloud.storage.s3.access-key", "test-access-key");
        mockEnv.setProperty("cloud.storage.s3.secret-key", "test-secret-key");
        mockEnv.setProperty("cloud.storage.s3.multipart-part-retry-max-attempts", "5");
        mockEnv.setProperty("cloud.storage.s3.multipart-part-retry-base-backoff-ms", "1500");
        mockEnv.setProperty("cloud.storage.s3.multipart-exhausted-direct-dlq", "true");

        CloudStorageConfig cfg = springConfig.toCloudStorageConfig();

        // Common fields
        assertTrue(cfg.isEnabled());
        assertEquals("s3", cfg.getProvider());
        assertEquals("test-prefix", cfg.getPathPrefix());
        assertFalse(cfg.isStartupHydrationEnabled());
        assertEquals(5000L, cfg.getReadMissGuardWindowMs());

        // Provider-specific properties forwarded verbatim
        assertEquals("test-bucket", cfg.getProviderProperties().get("bucket"));
        assertEquals("us-west-2", cfg.getProviderProperties().get("region"));
        assertEquals("https://s3.example.com", cfg.getProviderProperties().get("endpoint"));
        assertEquals("test-access-key", cfg.getProviderProperties().get("access-key"));
        assertEquals("test-secret-key", cfg.getProviderProperties().get("secret-key"));
        assertEquals("5", cfg.getProviderProperties().get("multipart-part-retry-max-attempts"));
        assertEquals("1500",
                     cfg.getProviderProperties().get("multipart-part-retry-base-backoff-ms"));
        assertEquals("true",
                     cfg.getProviderProperties().get("multipart-exhausted-direct-dlq"));
    }

    /**
     * When no provider env properties exist, providerProperties is empty but not null.
     */
    @Test
    public void testCloudStorageSpringConfigDefaults() {
        CloudStorageConfig cfg = springConfig.toCloudStorageConfig();

        assertFalse(cfg.isEnabled());
        assertEquals("s3", cfg.getProvider());
        assertEquals("hugegraph", cfg.getPathPrefix());
        assertTrue(cfg.isStartupHydrationEnabled());
        assertEquals(3000L, cfg.getReadMissGuardWindowMs());
        assertEquals(3, cfg.getUploadRetryMaxAttempts());
        assertEquals(64, cfg.getUploadBackpressureHighWatermark());
        assertNotNull(cfg.getProviderProperties());
        assertTrue(cfg.getProviderProperties().isEmpty());
    }

    /**
     * Common property setters on CloudStorageSpringConfig work correctly.
     */
    @Test
    public void testCloudStorageSpringConfigCommonGettersSetters() {
        springConfig.setEnabled(true);
        assertTrue(springConfig.isEnabled());

        springConfig.setProvider("gcs");
        assertEquals("gcs", springConfig.getProvider());

        springConfig.setPathPrefix("my-prefix");
        assertEquals("my-prefix", springConfig.getPathPrefix());

        springConfig.setStartupHydrationEnabled(false);
        assertFalse(springConfig.isStartupHydrationEnabled());

        springConfig.setReadMissGuardWindowMs(7000L);
        assertEquals(7000L, springConfig.getReadMissGuardWindowMs());

        springConfig.setUploadRetryMaxAttempts(3);
        assertEquals(3, springConfig.getUploadRetryMaxAttempts());

        springConfig.setUploadRetryInitialDelayMs(500L);
        assertEquals(500L, springConfig.getUploadRetryInitialDelayMs());

        springConfig.setUploadRetryMaxDelayMs(30_000L);
        assertEquals(30_000L, springConfig.getUploadRetryMaxDelayMs());

        springConfig.setUploadBackpressureHighWatermark(128);
        assertEquals(128, springConfig.getUploadBackpressureHighWatermark());
        assertEquals(128,
                     springConfig.toCloudStorageConfig().getUploadBackpressureHighWatermark());
    }

    /**
     * Provider-specific sub-namespace is driven by the {@code provider} field,
     * so a different provider name reads from a different env sub-namespace.
     */
    @Test
    public void testProviderNamespaceIsDynamic() {
        springConfig.setProvider("gcs");
        mockEnv.setProperty("cloud.storage.gcs.bucket", "gcs-bucket");
        mockEnv.setProperty("cloud.storage.gcs.credentials-file-path", "/path/creds.json");
        // S3 keys should NOT appear
        mockEnv.setProperty("cloud.storage.s3.bucket", "s3-bucket");

        CloudStorageConfig cfg = springConfig.toCloudStorageConfig();

        assertEquals("gcs-bucket", cfg.getProviderProperties().get("bucket"));
        assertEquals("/path/creds.json",
                     cfg.getProviderProperties().get("credentials-file-path"));
        assertFalse("s3 keys must not bleed into gcs namespace",
                    cfg.getProviderProperties().containsKey("cloud.storage.s3.bucket"));
    }

    /**
     * Test AppConfig getRaftPath.
     */
    @Test
    public void testGetRaftPath() {
        AppConfig config = new AppConfig();
        // getRaftPath should not throw; actual value depends on initialization
        config.getRaftPath();
    }

    /**
     * Test CloudStorageSpringConfig creation and conversion with empty env.
     */
    @Test
    public void testCloudStorageSpringConfigCreation() {
        assertNotNull(springConfig);
        assertNotNull(springConfig.toCloudStorageConfig());
    }

    // -----------------------------------------------------------------------
    // Stable cloud key scope resolution (recovery after IP drift / disk loss)
    // -----------------------------------------------------------------------

    @Test
    public void resolveStoreScope_prefersConfiguredNodeId() throws Exception {
        AppConfig appConfig = new AppConfig();
        Path dataRoot = Files.createTempDirectory("hgstore-scope");
        try {
            String prefix = appConfig.resolveStableStoreScopePrefix("node-A", dataRoot.toString());
            assertEquals("store-node-A", prefix);
            // Persisted so a later removal of the config still resolves to the same scope.
            assertEquals("store-node-A",
                         Files.readString(dataRoot.resolve(AppConfig.CLOUD_SCOPE_MARKER_FILE))
                              .trim());
        } finally {
            deleteRecursively(dataRoot);
        }
    }

    @Test
    public void resolveStoreScope_sanitizesConfiguredNodeId() throws Exception {
        AppConfig appConfig = new AppConfig();
        Path dataRoot = Files.createTempDirectory("hgstore-scope");
        try {
            // Unsafe key characters must be replaced so the scope is a valid single path segment.
            assertEquals("store-pod_1_2",
                         appConfig.resolveStableStoreScopePrefix("pod/1:2", dataRoot.toString()));
        } finally {
            deleteRecursively(dataRoot);
        }
    }

    @Test
    public void resolveStoreScope_reusesPersistedMarkerAcrossIdentityDrift() throws Exception {
        AppConfig appConfig = new AppConfig();
        Path dataRoot = Files.createTempDirectory("hgstore-scope");
        try {
            // A marker written under a PREVIOUS network identity must be honored on a later start
            // (blank node-id), even though the current runtime address may differ — this is the
            // property that lets a node find its prior remote data after an IP/hostname change.
            Files.writeString(dataRoot.resolve(AppConfig.CLOUD_SCOPE_MARKER_FILE),
                              "store-original_1_1");
            String prefix = appConfig.resolveStableStoreScopePrefix("", dataRoot.toString());
            assertEquals("store-original_1_1", prefix);
        } finally {
            deleteRecursively(dataRoot);
        }
    }

    @Test
    public void resolveStoreScope_seedsAndPersistsOnFirstStart() throws Exception {
        AppConfig appConfig = new AppConfig();
        Path dataRoot = Files.createTempDirectory("hgstore-scope");
        try {
            // No node-id and no marker: seed from identity, persist, and reuse on the next start.
            String first = appConfig.resolveStableStoreScopePrefix(null, dataRoot.toString());
            assertTrue("Seeded scope must use the store- prefix", first.startsWith("store-"));
            assertTrue("First start must persist the marker",
                       Files.exists(dataRoot.resolve(AppConfig.CLOUD_SCOPE_MARKER_FILE)));
            String second = appConfig.resolveStableStoreScopePrefix(null, dataRoot.toString());
            assertEquals("Second start must reuse the persisted scope", first, second);
        } finally {
            deleteRecursively(dataRoot);
        }
    }

    @Test
    public void resolveStoreScope_configuredNodeIdOverridesPersistedMarker() throws Exception {
        AppConfig appConfig = new AppConfig();
        Path dataRoot = Files.createTempDirectory("hgstore-scope");
        try {
            Files.writeString(dataRoot.resolve(AppConfig.CLOUD_SCOPE_MARKER_FILE), "store-seeded");
            String prefix = appConfig.resolveStableStoreScopePrefix("explicit", dataRoot.toString());
            assertEquals("An explicit node-id must win over a persisted marker",
                         "store-explicit", prefix);
            assertEquals("The marker must be updated to the configured scope", "store-explicit",
                         Files.readString(dataRoot.resolve(AppConfig.CLOUD_SCOPE_MARKER_FILE))
                              .trim());
        } finally {
            deleteRecursively(dataRoot);
        }
    }

    private static void deleteRecursively(Path dir) throws Exception {
        if (!Files.exists(dir)) {
            return;
        }
        try (java.util.stream.Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (Exception ignore) {
                    // best-effort test cleanup
                }
            });
        }
    }
}
