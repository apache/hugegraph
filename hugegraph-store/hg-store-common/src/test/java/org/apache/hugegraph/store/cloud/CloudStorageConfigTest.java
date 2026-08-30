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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("ObviousNullCheck")
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
         // Backpressure is opt-in (disabled by default): when > 0 it parks RocksDB's
         // flush/compaction thread, which can stall writes during a sustained cloud outage.
         assertEquals(0, config.getUploadBackpressureHighWatermark());
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

     @Test
     public void testUploadBackpressureHighWatermark() {
         config.setUploadBackpressureHighWatermark(128);
         assertEquals(128, config.getUploadBackpressureHighWatermark());

         config.setUploadBackpressureHighWatermark(0);
         assertEquals(0, config.getUploadBackpressureHighWatermark());
     }

     @Test
     public void testDlqMaxSize() {
         assertEquals(100_000, config.getDlqMaxSize());

         config.setDlqMaxSize(500);
         assertEquals(500, config.getDlqMaxSize());
     }

     @Test
     public void testMetadataSyncDebounceMs() {
         assertEquals(1_000L, config.getMetadataSyncDebounceMs());

         config.setMetadataSyncDebounceMs(250L);
         assertEquals(250L, config.getMetadataSyncDebounceMs());

         config.setMetadataSyncDebounceMs(0L);
         assertEquals(0L, config.getMetadataSyncDebounceMs());
     }

     @Test
     public void testMetadataSyncMaxUnpublished() {
         assertEquals(32, config.getMetadataSyncMaxUnpublished());

         config.setMetadataSyncMaxUnpublished(8);
         assertEquals(8, config.getMetadataSyncMaxUnpublished());

         config.setMetadataSyncMaxUnpublished(0);
         assertEquals(0, config.getMetadataSyncMaxUnpublished());
     }

     @Test
     public void testNodeId() {
         assertEquals("", config.getNodeId());

         config.setNodeId("store-node-7");
         assertEquals("store-node-7", config.getNodeId());
     }

     /**
      * Exercises the Lombok {@code @Data}-generated {@code equals}/{@code hashCode}/{@code toString}
      * so the generated methods on the class declaration are covered.
      */
     @Test
     public void testEqualsHashCodeAndToString() {
         CloudStorageConfig a = new CloudStorageConfig();
         CloudStorageConfig b = new CloudStorageConfig();

         assertEquals(a, b);
         assertEquals(a.hashCode(), b.hashCode());
         assertEquals(a, a);
         assertNotEquals(null, a);
         assertNotEquals("not-a-config", a);
         assertNotNull(a.toString());

         b.setProvider("gcs");
         assertNotEquals(a, b);
         assertNotEquals(a.hashCode(), b.hashCode());
     }

     /**
      * Drives every field comparison in the Lombok-generated {@code equals} to its
      * "not equal" branch. Lombok compares fields in declaration order and returns on the
      * first difference, so each field must be the <em>only</em> difference from a default
      * instance for its comparison branch to be reached — a single multi-field diff would
      * short-circuit at the first field and leave the rest uncovered.
      */
     @Test
     public void testEqualsDistinguishesEveryField() {
         assertNotEquals(new CloudStorageConfig(), withEnabled());
         assertNotEquals(new CloudStorageConfig(), withProvider("gcs"));
         assertNotEquals(new CloudStorageConfig(), withPathPrefix("other"));
         assertNotEquals(new CloudStorageConfig(), withStartupHydration());
         assertNotEquals(new CloudStorageConfig(), withReadMissGuardWindowMs());
         assertNotEquals(new CloudStorageConfig(), withUploadRetryMaxAttempts());
         assertNotEquals(new CloudStorageConfig(), withUploadRetryInitialDelayMs());
         assertNotEquals(new CloudStorageConfig(), withUploadRetryMaxDelayMs());
         assertNotEquals(new CloudStorageConfig(), withUploadBackpressureHighWatermark());
         assertNotEquals(new CloudStorageConfig(), withDlqMaxSize());
         assertNotEquals(new CloudStorageConfig(), withMetadataSyncDebounceMs());
         assertNotEquals(new CloudStorageConfig(), withMetadataSyncMaxUnpublished());
         assertNotEquals(new CloudStorageConfig(), withNodeId("node-x"));
         assertNotEquals(new CloudStorageConfig(), withProviderProperties(singleProp()));

         // hashCode is sensitive to representative fields of each primitive family.
         assertNotEquals(new CloudStorageConfig().hashCode(), withReadMissGuardWindowMs()
                 .hashCode());
         assertNotEquals(new CloudStorageConfig().hashCode(), withUploadRetryMaxAttempts()
                 .hashCode());

         // hashCode branches for the boolean fields: defaults are enabled=false /
         // startupHydrationEnabled=true, so hash a config that flips each to exercise the
         // opposite branch of the generated ternaries.
         assertNotEquals(0, withEnabled().hashCode());
         assertNotEquals(0, withStartupHydration().hashCode());
     }

     /**
      * Exercises the null-handling branches of the Lombok {@code equals}/{@code hashCode} for the
      * reference-typed fields: each is compared with one side {@code null} and the other non-null,
      * in both directions, plus both-null equality.
      */
     @Test
     public void testEqualsHandlesNullReferenceFields() {
         assertReferenceFieldNullBranches(withProvider(null), withProvider("s3"));
         assertReferenceFieldNullBranches(withPathPrefix(null), withPathPrefix("hugegraph"));
         assertReferenceFieldNullBranches(withNodeId(null), withNodeId("node-x"));
         assertReferenceFieldNullBranches(withProviderProperties(null),
                                          withProviderProperties(singleProp()));
     }

     /**
      * Covers the Lombok-generated {@code canEqual} guard, which a plain {@code equals} call between
      * two direct instances never drives to both outcomes: the {@code instanceof} check in
      * {@code canEqual} (true for a peer, false for a foreign type) and the
      * {@code !other.canEqual(this)} branch in {@code equals} (reached when the argument is a
      * subtype that declines equality).
      */
     @Test
     public void testCanEqualGuardBranches() {
         CloudStorageConfig a = new CloudStorageConfig();

         // canEqual: instanceof true branch (a peer) and false branch (a foreign type).
         assertTrue(a.canEqual(new CloudStorageConfig()));
         assertFalse(a.canEqual("not-a-config"));

         // equals reaches its `!other.canEqual(this)` == true branch when the argument passes the
         // instanceof check but rejects this instance from its own canEqual.
         CloudStorageConfig rejecting = new CloudStorageConfig() {
             @Override
             public boolean canEqual(Object other) {
                 return false;
             }
         };
         assertNotEquals(a, rejecting);
     }

     private static void assertReferenceFieldNullBranches(CloudStorageConfig withNull,
                                                          CloudStorageConfig withValue) {
         // this-null vs other-non-null and the reverse both reach the "not equal" branch.
         assertNotEquals(withNull, withValue);
         assertNotEquals(withValue, withNull);
         // both-null on that field collapses to equal (all other fields are defaults).
         assertEquals(withNull, cloneOf(withNull));
         assertNotNull(withNull.hashCode());
     }

     private static CloudStorageConfig cloneOf(CloudStorageConfig src) {
         CloudStorageConfig copy = new CloudStorageConfig();
         copy.setEnabled(src.isEnabled());
         copy.setProvider(src.getProvider());
         copy.setPathPrefix(src.getPathPrefix());
         copy.setStartupHydrationEnabled(src.isStartupHydrationEnabled());
         copy.setReadMissGuardWindowMs(src.getReadMissGuardWindowMs());
         copy.setUploadRetryMaxAttempts(src.getUploadRetryMaxAttempts());
         copy.setUploadRetryInitialDelayMs(src.getUploadRetryInitialDelayMs());
         copy.setUploadRetryMaxDelayMs(src.getUploadRetryMaxDelayMs());
         copy.setUploadBackpressureHighWatermark(src.getUploadBackpressureHighWatermark());
         copy.setDlqMaxSize(src.getDlqMaxSize());
         copy.setMetadataSyncDebounceMs(src.getMetadataSyncDebounceMs());
         copy.setMetadataSyncMaxUnpublished(src.getMetadataSyncMaxUnpublished());
         copy.setNodeId(src.getNodeId());
         copy.setProviderProperties(src.getProviderProperties());
         return copy;
     }

     private static Map<String, String> singleProp() {
         Map<String, String> props = new HashMap<>();
         props.put("bucket", "b");
         return props;
     }

     private static CloudStorageConfig withEnabled() {
         CloudStorageConfig c = new CloudStorageConfig();
         c.setEnabled(true);
         return c;
     }

     private static CloudStorageConfig withProvider(String v) {
         CloudStorageConfig c = new CloudStorageConfig();
         c.setProvider(v);
         return c;
     }

     private static CloudStorageConfig withPathPrefix(String v) {
         CloudStorageConfig c = new CloudStorageConfig();
         c.setPathPrefix(v);
         return c;
     }

     private static CloudStorageConfig withStartupHydration() {
         CloudStorageConfig c = new CloudStorageConfig();
         c.setStartupHydrationEnabled(false);
         return c;
     }

     private static CloudStorageConfig withReadMissGuardWindowMs() {
         CloudStorageConfig c = new CloudStorageConfig();
         c.setReadMissGuardWindowMs(9999L);
         return c;
     }

     private static CloudStorageConfig withUploadRetryMaxAttempts() {
         CloudStorageConfig c = new CloudStorageConfig();
         c.setUploadRetryMaxAttempts(9);
         return c;
     }

     private static CloudStorageConfig withUploadRetryInitialDelayMs() {
         CloudStorageConfig c = new CloudStorageConfig();
         c.setUploadRetryInitialDelayMs(9999L);
         return c;
     }

     private static CloudStorageConfig withUploadRetryMaxDelayMs() {
         CloudStorageConfig c = new CloudStorageConfig();
         c.setUploadRetryMaxDelayMs(9999L);
         return c;
     }

     private static CloudStorageConfig withUploadBackpressureHighWatermark() {
         CloudStorageConfig c = new CloudStorageConfig();
         c.setUploadBackpressureHighWatermark(9);
         return c;
     }

     private static CloudStorageConfig withDlqMaxSize() {
         CloudStorageConfig c = new CloudStorageConfig();
         c.setDlqMaxSize(9);
         return c;
     }

     private static CloudStorageConfig withMetadataSyncDebounceMs() {
         CloudStorageConfig c = new CloudStorageConfig();
         c.setMetadataSyncDebounceMs(9999L);
         return c;
     }

     private static CloudStorageConfig withMetadataSyncMaxUnpublished() {
         CloudStorageConfig c = new CloudStorageConfig();
         c.setMetadataSyncMaxUnpublished(9);
         return c;
     }

     private static CloudStorageConfig withNodeId(String v) {
         CloudStorageConfig c = new CloudStorageConfig();
         c.setNodeId(v);
         return c;
     }

     private static CloudStorageConfig withProviderProperties(Map<String, String> v) {
         CloudStorageConfig c = new CloudStorageConfig();
         c.setProviderProperties(v);
         return c;
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

     @Test
     public void testMultiplePropertyUpdates() {
         Map<String, String> props1 = new HashMap<>();
         props1.put("key1", "value1");
         config.setProviderProperties(props1);
         assertEquals("value1", config.getProviderProperties().get("key1"));

         Map<String, String> props2 = new HashMap<>();
         props2.put("key2", "value2");
         config.setProviderProperties(props2);
         assertEquals("value2", config.getProviderProperties().get("key2"));
     }

     @Test
     public void testBackpressureDisabled() {
         config.setUploadBackpressureHighWatermark(0);
         assertEquals(0, config.getUploadBackpressureHighWatermark());
     }

     @Test
     public void testNegativeReadMissGuardWindow() {
         config.setReadMissGuardWindowMs(-5000L);
         assertEquals(-5000L, config.getReadMissGuardWindowMs());
     }

     @Test
     public void testProviderPropertiesImmutabilityBehavior() {
         Map<String, String> props = new HashMap<>();
         props.put("bucket", "original");
         config.setProviderProperties(props);

         // Modify the original map
         props.put("bucket", "modified");

         // The config should have the modified value since it references the same map
         assertEquals("modified", config.getProviderProperties().get("bucket"));
     }
}
