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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.store.cloud.CloudStorageConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.mock.env.MockEnvironment;

public class AppConfigCloudStorageTest {

    private MockEnvironment mockEnv;

    @BeforeEach
    public void setUp() {
        mockEnv = new MockEnvironment();
    }

    /**
     * Common fields are bound from {@code cloud.storage.*} and provider-specific properties flow
     * from the environment through the provider sub-namespace.
     */
    @Test
    public void testBindCloudStorageConfig() {
        mockEnv.setProperty("cloud.storage.enabled", "true");
        mockEnv.setProperty("cloud.storage.provider", "s3");
        mockEnv.setProperty("cloud.storage.path-prefix", "test-prefix");
        mockEnv.setProperty("cloud.storage.startup-hydration-enabled", "false");
        mockEnv.setProperty("cloud.storage.read-miss-guard-window-ms", "5000");
        mockEnv.setProperty("cloud.storage.upload-retry-max-attempts", "5");
        mockEnv.setProperty("cloud.storage.upload-retry-initial-delay-ms", "500");
        mockEnv.setProperty("cloud.storage.upload-retry-max-delay-ms", "30000");
        mockEnv.setProperty("cloud.storage.metadata-sync-debounce-ms", "250");
        mockEnv.setProperty("cloud.storage.metadata-sync-max-unpublished", "8");
        mockEnv.setProperty("cloud.storage.node-id", "node-7");

        mockEnv.setProperty("cloud.storage.s3.bucket", "test-bucket");
        mockEnv.setProperty("cloud.storage.s3.region", "us-west-2");
        mockEnv.setProperty("cloud.storage.s3.endpoint", "https://s3.example.com");
        mockEnv.setProperty("cloud.storage.s3.access-key", "test-access-key");
        mockEnv.setProperty("cloud.storage.s3.secret-key", "test-secret-key");

        CloudStorageConfig cfg = AppConfig.bindCloudStorageConfig(mockEnv);

        // Common fields
        assertTrue(cfg.isEnabled());
        assertEquals("s3", cfg.getProvider());
        assertEquals("test-prefix", cfg.getPathPrefix());
        assertFalse(cfg.isStartupHydrationEnabled());
        assertEquals(5000L, cfg.getReadMissGuardWindowMs());
        assertEquals(5, cfg.getUploadRetryMaxAttempts());
        assertEquals(500L, cfg.getUploadRetryInitialDelayMs());
        assertEquals(30_000L, cfg.getUploadRetryMaxDelayMs());
        assertEquals(250L, cfg.getMetadataSyncDebounceMs());
        assertEquals(8, cfg.getMetadataSyncMaxUnpublished());
        assertEquals("node-7", cfg.getNodeId());

        // Provider-specific properties forwarded verbatim
        assertEquals("test-bucket", cfg.getProviderProperties().get("bucket"));
        assertEquals("us-west-2", cfg.getProviderProperties().get("region"));
        assertEquals("https://s3.example.com", cfg.getProviderProperties().get("endpoint"));
        assertEquals("test-access-key", cfg.getProviderProperties().get("access-key"));
        assertEquals("test-secret-key", cfg.getProviderProperties().get("secret-key"));
    }

    /**
     * With no {@code cloud.storage.*} properties, the {@link CloudStorageConfig} field defaults
     * apply and providerProperties is empty but not null.
     */
    @Test
    public void testBindCloudStorageConfigDefaults() {
        CloudStorageConfig cfg = AppConfig.bindCloudStorageConfig(mockEnv);

        assertFalse(cfg.isEnabled());
        assertEquals("s3", cfg.getProvider());
        assertEquals("hugegraph", cfg.getPathPrefix());
        assertTrue(cfg.isStartupHydrationEnabled());
        assertEquals(3000L, cfg.getReadMissGuardWindowMs());
        assertEquals(3, cfg.getUploadRetryMaxAttempts());
        assertNotNull(cfg.getProviderProperties());
        assertTrue(cfg.getProviderProperties().isEmpty());
    }

    /**
     * Unset fields keep their {@link CloudStorageConfig} defaults when only some are bound.
     */
    @Test
    public void testBindCloudStorageConfigPartialKeepsDefaults() {
        mockEnv.setProperty("cloud.storage.enabled", "true");

        CloudStorageConfig cfg = AppConfig.bindCloudStorageConfig(mockEnv);

        assertTrue(cfg.isEnabled());
        assertEquals("hugegraph", cfg.getPathPrefix());
        assertEquals(1_000L, cfg.getMetadataSyncDebounceMs());
        assertEquals(32, cfg.getMetadataSyncMaxUnpublished());
    }

    /**
     * Common fields resolve from environment-variable-style names via relaxed binding.
     */
    @Test
    public void testCommonFieldsResolveFromEnvVarStyleNames() {
        Map<String, Object> envVarProps = new HashMap<>();
        envVarProps.put("CLOUD_STORAGE_ENABLED", "true");
        envVarProps.put("CLOUD_STORAGE_PATH_PREFIX", "env-prefix");
        envVarProps.put("CLOUD_STORAGE_METADATA_SYNC_DEBOUNCE_MS", "0");
        mockEnv.getPropertySources().addFirst(new SystemEnvironmentPropertySource(
                StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME, envVarProps));

        CloudStorageConfig cfg = AppConfig.bindCloudStorageConfig(mockEnv);

        assertTrue(cfg.isEnabled());
        assertEquals("env-prefix", cfg.getPathPrefix());
        assertEquals(0L, cfg.getMetadataSyncDebounceMs());
    }

    /**
     * Provider-specific sub-namespace is driven by the {@code provider} field,
     * so a different provider name reads from a different env sub-namespace.
     */
    @Test
    public void testProviderNamespaceIsDynamic() {
        mockEnv.setProperty("cloud.storage.provider", "gcs");
        mockEnv.setProperty("cloud.storage.gcs.bucket", "gcs-bucket");
        mockEnv.setProperty("cloud.storage.gcs.credentials-file-path", "/path/creds.json");
        // S3 keys should NOT appear
        mockEnv.setProperty("cloud.storage.s3.bucket", "s3-bucket");

        CloudStorageConfig cfg = AppConfig.bindCloudStorageConfig(mockEnv);

        assertEquals("gcs", cfg.getProvider());
        assertEquals("gcs-bucket", cfg.getProviderProperties().get("bucket"));
        assertEquals("/path/creds.json",
                     cfg.getProviderProperties().get("credentials-file-path"));
        assertEquals(2, cfg.getProviderProperties().size(),
                     "s3 keys must not bleed into gcs namespace");
    }

    /**
     * Provider properties must also resolve from environment-variable-style property names
     * (e.g. {@code CLOUD_STORAGE_S3_SECRET_KEY}), as those are how credentials typically
     * arrive in a container deployment. Relaxed binding reconciles these with the kebab-case
     * key ({@code secret-key}) that {@code S3CloudStorageConfig.KEY_*} expects.
     */
    @Test
    public void testProviderPropertiesResolveFromEnvVarStyleNames() {
        Map<String, Object> envVarProps = new HashMap<>();
        envVarProps.put("CLOUD_STORAGE_S3_SECRET_KEY", "env-var-secret");
        envVarProps.put("CLOUD_STORAGE_S3_ACCESS_KEY", "env-var-access");
        mockEnv.getPropertySources().addFirst(new SystemEnvironmentPropertySource(
                StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME, envVarProps));

        CloudStorageConfig cfg = AppConfig.bindCloudStorageConfig(mockEnv);

        assertEquals("env-var-secret", cfg.getProviderProperties().get("secret-key"));
        assertEquals("env-var-access", cfg.getProviderProperties().get("access-key"));
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
            assertTrue(first.startsWith("store-"), "Seeded scope must use the store- prefix");
            assertTrue(Files.exists(dataRoot.resolve(AppConfig.CLOUD_SCOPE_MARKER_FILE)),
                       "First start must persist the marker");
            String second = appConfig.resolveStableStoreScopePrefix(null, dataRoot.toString());
            assertEquals(first, second, "Second start must reuse the persisted scope");
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
            assertEquals("store-explicit", prefix,
                         "An explicit node-id must win over a persisted marker");
            assertEquals("store-explicit",
                         Files.readString(dataRoot.resolve(AppConfig.CLOUD_SCOPE_MARKER_FILE))
                              .trim(),
                         "The marker must be updated to the configured scope");
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
