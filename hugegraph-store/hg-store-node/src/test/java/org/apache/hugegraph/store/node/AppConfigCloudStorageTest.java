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

import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.store.cloud.CloudStorageConfig;
import org.junit.Before;
import org.junit.Test;

public class AppConfigCloudStorageTest {

    private AppConfig appConfig;
    private AppConfig.CloudStorageSpringConfig springConfig;

    @Before
    public void setUp() {
        appConfig = new AppConfig();
        springConfig = appConfig.new CloudStorageSpringConfig();
    }

    /**
     * Test that CloudStorageSpringConfig correctly converts to CloudStorageConfig
     */
    @Test
    public void testCloudStorageSpringConfigConversion() {
        springConfig.setEnabled(true);
        springConfig.setProvider("s3");
        springConfig.setBucket("test-bucket");
        springConfig.setRegion("us-west-2");
        springConfig.setEndpoint("https://s3.example.com");
        springConfig.setAccessKey("test-access-key");
        springConfig.setSecretKey("test-secret-key");
        springConfig.setPathPrefix("test-prefix");
        springConfig.setStartupHydrationEnabled(false);
        springConfig.setReadMissGuardWindowMs(5000L);

        Map<String, String> extraProps = new HashMap<>();
        extraProps.put("custom-prop", "custom-value");
        springConfig.setExtraProperties(extraProps);

        CloudStorageConfig cfg = springConfig.toCloudStorageConfig();

        assertTrue(cfg.isEnabled());
        assertEquals("s3", cfg.getProvider());
        assertEquals("test-bucket", cfg.getBucket());
        assertEquals("us-west-2", cfg.getRegion());
        assertEquals("https://s3.example.com", cfg.getEndpoint());
        assertEquals("test-access-key", cfg.getAccessKey());
        assertEquals("test-secret-key", cfg.getSecretKey());
        assertEquals("test-prefix", cfg.getPathPrefix());
        assertFalse(cfg.isStartupHydrationEnabled());
        assertEquals(5000L, cfg.getReadMissGuardWindowMs());
        assertEquals(1, cfg.getExtraProperties().size());
        assertEquals("custom-value", cfg.getExtraProperties().get("custom-prop"));
    }

    /**
     * Test CloudStorageSpringConfig defaults
     */
    @Test
    public void testCloudStorageSpringConfigDefaults() {
        CloudStorageConfig cfg = springConfig.toCloudStorageConfig();

        assertFalse(cfg.isEnabled());
        assertEquals("s3", cfg.getProvider());
        assertEquals("hugegraph", cfg.getPathPrefix());
        assertTrue(cfg.isStartupHydrationEnabled());
        assertEquals(3000L, cfg.getReadMissGuardWindowMs());
    }

    /**
     * Test CloudStorageSpringConfig all properties set
     */
    @Test
    public void testCloudStorageSpringConfigAllPropertiesSet() {
        springConfig.setEnabled(true);
        springConfig.setProvider("gcs");
        springConfig.setBucket("gcs-bucket");
        springConfig.setRegion("us-central-1");
        springConfig.setEndpoint("https://storage.googleapis.com");
        springConfig.setAccessKey("gcs-access");
        springConfig.setSecretKey("gcs-secret");
        springConfig.setPathPrefix("data/prefix");
        springConfig.setStartupHydrationEnabled(false);
        springConfig.setReadMissGuardWindowMs(10000L);

        Map<String, String> extra = new HashMap<>();
        extra.put("project-id", "my-gcp-project");
        springConfig.setExtraProperties(extra);

        CloudStorageConfig cfg = springConfig.toCloudStorageConfig();

        assertTrue(cfg.isEnabled());
        assertEquals("gcs", cfg.getProvider());
        assertEquals("gcs-bucket", cfg.getBucket());
        assertEquals("us-central-1", cfg.getRegion());
        assertEquals("https://storage.googleapis.com", cfg.getEndpoint());
        assertEquals("gcs-access", cfg.getAccessKey());
        assertEquals("gcs-secret", cfg.getSecretKey());
        assertEquals("data/prefix", cfg.getPathPrefix());
        assertFalse(cfg.isStartupHydrationEnabled());
        assertEquals(10000L, cfg.getReadMissGuardWindowMs());
        assertNotNull(cfg.getExtraProperties());
    }

    /**
     * Test CloudStorageSpringConfig getters and setters
     */
    @Test
    public void testCloudStorageSpringConfigGettersSetters() {
        // Test each setter and getter independently
        springConfig.setEnabled(true);
        assertTrue(springConfig.isEnabled());

        springConfig.setProvider("azure");
        assertEquals("azure", springConfig.getProvider());

        springConfig.setBucket("my-bucket");
        assertEquals("my-bucket", springConfig.getBucket());

        springConfig.setRegion("westus");
        assertEquals("westus", springConfig.getRegion());

        springConfig.setEndpoint("https://my-storage.blob.core.windows.net");
        assertEquals("https://my-storage.blob.core.windows.net", springConfig.getEndpoint());

        springConfig.setAccessKey("my-access-key");
        assertEquals("my-access-key", springConfig.getAccessKey());

        springConfig.setSecretKey("my-secret-key");
        assertEquals("my-secret-key", springConfig.getSecretKey());

        springConfig.setPathPrefix("my-prefix");
        assertEquals("my-prefix", springConfig.getPathPrefix());

        springConfig.setStartupHydrationEnabled(false);
        assertFalse(springConfig.isStartupHydrationEnabled());

        springConfig.setReadMissGuardWindowMs(7000L);
        assertEquals(7000L, springConfig.getReadMissGuardWindowMs());

        Map<String, String> props = new HashMap<>();
        props.put("key", "value");
        springConfig.setExtraProperties(props);
        assertNotNull(springConfig.getExtraProperties());
    }

    /**
     * Test AppConfig getRaftPath
     */
    @Test
    public void testGetRaftPath() {
        AppConfig config = new AppConfig();
        // getRaftPath should not throw, value depends on initialization
        String result = config.getRaftPath();
        // Just verify it doesn't throw an exception
        // The actual value depends on whether dataPath and raftPath are initialized
    }

    /**
     * Test CloudStorageSpringConfig creation and conversion
     */
    @Test
    public void testCloudStorageSpringConfigCreation() {
        assertNotNull(springConfig);
        assertNotNull(springConfig.toCloudStorageConfig());
    }
}




