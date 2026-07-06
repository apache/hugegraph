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

    /**
     * Test CloudStorageConfig defaults
     */
    @Test
    public void testDefaults() {
        assertFalse(config.isEnabled());
        assertEquals("s3", config.getProvider());
        assertEquals("hugegraph", config.getPathPrefix());
        assertTrue(config.isStartupHydrationEnabled());
        assertEquals(3000L, config.getReadMissGuardWindowMs());
        assertNotNull(config.getExtraProperties());
    }

    /**
     * Test enabled flag setter and getter
     */
    @Test
    public void testEnabledFlag() {
        assertFalse(config.isEnabled());

        config.setEnabled(true);
        assertTrue(config.isEnabled());

        config.setEnabled(false);
        assertFalse(config.isEnabled());
    }

    /**
     * Test provider setter and getter
     */
    @Test
    public void testProvider() {
        assertEquals("s3", config.getProvider());

        config.setProvider("gcs");
        assertEquals("gcs", config.getProvider());

        config.setProvider("azure");
        assertEquals("azure", config.getProvider());
    }

    /**
     * Test bucket setter and getter
     */
    @Test
    public void testBucket() {
        config.setBucket("my-bucket");
        assertEquals("my-bucket", config.getBucket());

        config.setBucket("another-bucket-123");
        assertEquals("another-bucket-123", config.getBucket());
    }

    /**
     * Test region setter and getter
     */
    @Test
    public void testRegion() {
        config.setRegion("us-east-1");
        assertEquals("us-east-1", config.getRegion());

        config.setRegion("eu-west-1");
        assertEquals("eu-west-1", config.getRegion());
    }

    /**
     * Test endpoint setter and getter
     */
    @Test
    public void testEndpoint() {
        config.setEndpoint("https://s3.amazonaws.com");
        assertEquals("https://s3.amazonaws.com", config.getEndpoint());

        config.setEndpoint("https://minio.example.com");
        assertEquals("https://minio.example.com", config.getEndpoint());
    }

    /**
     * Test accessKey setter and getter
     */
    @Test
    public void testAccessKey() {
        config.setAccessKey("AKIAIOSFODNN7EXAMPLE");
        assertEquals("AKIAIOSFODNN7EXAMPLE", config.getAccessKey());
    }

    /**
     * Test secretKey setter and getter
     */
    @Test
    public void testSecretKey() {
        config.setSecretKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        assertEquals("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", config.getSecretKey());
    }

    /**
     * Test pathPrefix setter and getter
     */
    @Test
    public void testPathPrefix() {
        assertEquals("hugegraph", config.getPathPrefix());

        config.setPathPrefix("myapp/data");
        assertEquals("myapp/data", config.getPathPrefix());
    }

    /**
     * Test startupHydrationEnabled setter and getter
     */
    @Test
    public void testStartupHydrationEnabled() {
        assertTrue(config.isStartupHydrationEnabled());

        config.setStartupHydrationEnabled(false);
        assertFalse(config.isStartupHydrationEnabled());

        config.setStartupHydrationEnabled(true);
        assertTrue(config.isStartupHydrationEnabled());
    }

    /**
     * Test readMissGuardWindowMs setter and getter
     */
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

    /**
     * Test extraProperties setter and getter
     */
    @Test
    public void testExtraProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("key1", "value1");
        props.put("key2", "value2");

        config.setExtraProperties(props);

        assertNotNull(config.getExtraProperties());
        assertEquals(2, config.getExtraProperties().size());
        assertEquals("value1", config.getExtraProperties().get("key1"));
        assertEquals("value2", config.getExtraProperties().get("key2"));
    }

    /**
     * Test complete configuration
     */
    @Test
    public void testCompleteConfiguration() {
        config.setEnabled(true);
        config.setProvider("s3");
        config.setBucket("hugegraph-backup");
        config.setRegion("us-west-2");
        config.setEndpoint("https://s3-us-west-2.amazonaws.com");
        config.setAccessKey("test-access-key");
        config.setSecretKey("test-secret-key");
        config.setPathPrefix("production/data");
        config.setStartupHydrationEnabled(true);
        config.setReadMissGuardWindowMs(5000L);

        Map<String, String> extra = new HashMap<>();
        extra.put("enable-versioning", "true");
        extra.put("enable-encryption", "true");
        config.setExtraProperties(extra);

        // Verify all settings
        assertTrue(config.isEnabled());
        assertEquals("s3", config.getProvider());
        assertEquals("hugegraph-backup", config.getBucket());
        assertEquals("us-west-2", config.getRegion());
        assertEquals("https://s3-us-west-2.amazonaws.com", config.getEndpoint());
        assertEquals("test-access-key", config.getAccessKey());
        assertEquals("test-secret-key", config.getSecretKey());
        assertEquals("production/data", config.getPathPrefix());
        assertTrue(config.isStartupHydrationEnabled());
        assertEquals(5000L, config.getReadMissGuardWindowMs());
        assertEquals(2, config.getExtraProperties().size());
    }
}

