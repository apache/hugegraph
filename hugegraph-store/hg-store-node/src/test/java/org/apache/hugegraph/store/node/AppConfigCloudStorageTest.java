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
        assertEquals(5, cfg.getUploadRetryMaxAttempts());
        assertEquals(64, cfg.getUploadBackpressureHighWatermark());
        assertEquals("flush", cfg.getWalMode());
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


        springConfig.setWalMode("wal");
        assertEquals("wal", springConfig.getWalMode());
        assertEquals("wal", springConfig.toCloudStorageConfig().getWalMode());
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
}
