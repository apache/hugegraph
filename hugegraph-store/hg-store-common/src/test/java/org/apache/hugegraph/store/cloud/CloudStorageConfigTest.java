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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class CloudStorageConfigTest {

    private CloudStorageConfig config;

    @Before
    public void setUp() {
        config = new CloudStorageConfig();
    }

    // ---- Common fields ----

    @Test
    public void testDefaults() {
        assertFalse(config.isEnabled());
        assertEquals("s3", config.getProvider());
        assertEquals("hugegraph", config.getPathPrefix());
        assertTrue(config.isStartupHydrationEnabled());
        assertEquals(3000L, config.getReadMissGuardWindowMs());
        assertNotNull(config.getProviderProperties());
    }

    @Test
    public void testEnabledFlag() {
        assertFalse(config.isEnabled());

        config.setEnabled(true);
        assertTrue(config.isEnabled());

        config.setEnabled(false);
        assertFalse(config.isEnabled());
    }

    @Test
    public void testProvider() {
        assertEquals("s3", config.getProvider());
        config.setProvider("gcs");
        assertEquals("gcs", config.getProvider());
        config.setProvider("adls");
        assertEquals("adls", config.getProvider());
    }

    @Test
    public void testPathPrefix() {
        assertEquals("hugegraph", config.getPathPrefix());

        config.setPathPrefix("myapp/data");
        assertEquals("myapp/data", config.getPathPrefix());
    }

    @Test
    public void testStartupHydrationEnabled() {
        assertTrue(config.isStartupHydrationEnabled());

        config.setStartupHydrationEnabled(false);
        assertFalse(config.isStartupHydrationEnabled());

        config.setStartupHydrationEnabled(true);
        assertTrue(config.isStartupHydrationEnabled());
    }

    @Test
    public void testReadMissGuardWindowMs() {
        assertEquals(3000L, config.getReadMissGuardWindowMs());

        config.setReadMissGuardWindowMs(5000L);
        assertEquals(5000L, config.getReadMissGuardWindowMs());

        config.setReadMissGuardWindowMs(0L);
        assertEquals(0L, config.getReadMissGuardWindowMs());

        config.setReadMissGuardWindowMs(-1L);
        assertEquals(-1L, config.getReadMissGuardWindowMs());
    }

     @Test
     public void testUploadRetryDefaults() {
         // Default 3: whole-file retries enabled under the primary-durability model.
         assertEquals(3, config.getUploadRetryMaxAttempts());
         assertEquals(1_000L, config.getUploadRetryInitialDelayMs());
         assertEquals(60_000L, config.getUploadRetryMaxDelayMs());
         // Backpressure enabled by default.
         assertEquals(64, config.getUploadBackpressureHighWatermark());
     }

     @Test
     public void testMetadataSyncDefaults() {
         // Metadata mirroring defaults to flush mode.
         assertEquals("flush", config.getWalMode());
     }

     @Test
     public void testMetadataSyncSetters() {

         config.setWalMode("wal");
         assertEquals("wal", config.getWalMode());
     }

     @Test
     public void testUploadRetryMaxAttempts() {
         config.setUploadRetryMaxAttempts(3);
         assertEquals(3, config.getUploadRetryMaxAttempts());

         config.setUploadRetryMaxAttempts(0);
         assertEquals(0, config.getUploadRetryMaxAttempts());

         config.setUploadRetryMaxAttempts(10);
         assertEquals(10, config.getUploadRetryMaxAttempts());
     }

     @Test
     public void testUploadRetryInitialDelayMs() {
         config.setUploadRetryInitialDelayMs(500L);
         assertEquals(500L, config.getUploadRetryInitialDelayMs());

         config.setUploadRetryInitialDelayMs(2_000L);
         assertEquals(2_000L, config.getUploadRetryInitialDelayMs());
     }

     @Test
     public void testUploadRetryMaxDelayMs() {
         config.setUploadRetryMaxDelayMs(30_000L);
         assertEquals(30_000L, config.getUploadRetryMaxDelayMs());

         config.setUploadRetryMaxDelayMs(120_000L);
         assertEquals(120_000L, config.getUploadRetryMaxDelayMs());
     }

    // ---- Provider properties (cloud.storage.<provider>.* flattened) ----

    @Test
    public void testProviderPropertiesDefaults() {
        assertNotNull(config.getProviderProperties());
        assertTrue(config.getProviderProperties().isEmpty());
    }

    @Test
    public void testProviderPropertiesSetAndGet() {
        Map<String, String> props = new HashMap<>();
        props.put("bucket", "my-bucket");
        props.put("region", "us-east-1");
        props.put("endpoint", "https://s3.amazonaws.com");
        props.put("access-key", "AKIAIOSFODNN7EXAMPLE");
        props.put("secret-key", "secret");
        props.put("multipart-part-retry-max-attempts", "7");

        config.setProviderProperties(props);

        assertEquals("my-bucket", config.getProviderProperties().get("bucket"));
        assertEquals("us-east-1", config.getProviderProperties().get("region"));
        assertEquals("7", config.getProviderProperties()
                                  .get("multipart-part-retry-max-attempts"));
    }

    @Test
    public void testCompleteConfiguration() {
        config.setEnabled(true);
        config.setProvider("s3");
        config.setPathPrefix("production/data");
        config.setStartupHydrationEnabled(true);
        config.setReadMissGuardWindowMs(5000L);
        config.setUploadRetryMaxAttempts(3);
        config.setUploadRetryInitialDelayMs(500L);
        config.setUploadRetryMaxDelayMs(30_000L);

        Map<String, String> providerProps = new HashMap<>();
        providerProps.put("bucket", "hugegraph-backup");
        providerProps.put("region", "us-west-2");
        providerProps.put("endpoint", "https://s3-us-west-2.amazonaws.com");
        providerProps.put("access-key", "test-access-key");
        providerProps.put("secret-key", "test-secret-key");
        providerProps.put("multipart-part-retry-max-attempts", "5");
        providerProps.put("multipart-part-retry-base-backoff-ms", "1500");
        providerProps.put("multipart-exhausted-direct-dlq", "false");
        config.setProviderProperties(providerProps);

        // Common fields
        assertTrue(config.isEnabled());
        assertEquals("s3", config.getProvider());
        assertEquals("production/data", config.getPathPrefix());
        assertTrue(config.isStartupHydrationEnabled());
        assertEquals(5000L, config.getReadMissGuardWindowMs());
        assertEquals(3, config.getUploadRetryMaxAttempts());
        assertEquals(500L, config.getUploadRetryInitialDelayMs());
        assertEquals(30_000L, config.getUploadRetryMaxDelayMs());

        // Provider-specific fields (cloud.storage.<provider>.*)
        assertEquals("hugegraph-backup", config.getProviderProperties().get("bucket"));
        assertEquals("us-west-2", config.getProviderProperties().get("region"));
        assertEquals("https://s3-us-west-2.amazonaws.com",
                     config.getProviderProperties().get("endpoint"));
        assertEquals("test-access-key", config.getProviderProperties().get("access-key"));
        assertEquals("test-secret-key", config.getProviderProperties().get("secret-key"));
        assertEquals("5", config.getProviderProperties().get("multipart-part-retry-max-attempts"));
        assertEquals("1500",
                     config.getProviderProperties().get("multipart-part-retry-base-backoff-ms"));
        assertEquals("false", config.getProviderProperties().get("multipart-exhausted-direct-dlq"));
    }
}

