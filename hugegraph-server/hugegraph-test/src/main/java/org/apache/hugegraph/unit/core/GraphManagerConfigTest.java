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

package org.apache.hugegraph.unit.core;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.Test;

public class GraphManagerConfigTest {

    @Test
    public void testAttachExplicitLocalResourceLimits() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(CoreOptions.SCHEMA_CACHE_CAPACITY.name(), 1000L);
        properties.setProperty(
                CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name(), 8388608);
        GraphManager manager = newManager(properties);

        try {
            Map<String, Object> attached = attach(manager, new HashMap<>());

            Assert.assertEquals(1000L, attached.get(
                    CoreOptions.SCHEMA_CACHE_CAPACITY.name()));
            Assert.assertEquals(8388608, attached.get(
                    CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name()));
        } finally {
            manager.close();
        }
    }

    @Test
    public void testKeepIncomingResourceLimits() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(CoreOptions.SCHEMA_CACHE_CAPACITY.name(), 1000L);
        properties.setProperty(
                CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name(), 8388608);
        GraphManager manager = newManager(properties);
        Map<String, Object> configs = new HashMap<>();
        configs.put(CoreOptions.SCHEMA_CACHE_CAPACITY.name(), 2000L);
        configs.put(CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name(),
                    16777216);

        try {
            Map<String, Object> attached = attach(manager, configs);

            Assert.assertEquals(2000L, attached.get(
                    CoreOptions.SCHEMA_CACHE_CAPACITY.name()));
            Assert.assertEquals(16777216, attached.get(
                    CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name()));
        } finally {
            manager.close();
        }
    }

    @Test
    public void testAttachDoesNotMutateSourceOrCopySensitiveOptions() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(CoreOptions.SCHEMA_CACHE_CAPACITY.name(), 1000L);
        properties.setProperty(
                CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name(), 8388608);
        properties.setProperty(CoreOptions.BACKEND.name(), "memory");
        properties.setProperty(CoreOptions.STORE.name(), "local_store");
        properties.setProperty(ServerOptions.ADMIN_PA.name(), "local_secret");
        GraphManager manager = newManager(properties);
        Map<String, Object> configs = new HashMap<>();
        configs.put("marker", "source");
        Map<String, Object> source = new HashMap<>(configs);

        try {
            Map<String, Object> attached = attach(manager, configs);

            Assert.assertEquals(source, configs);
            Assert.assertEquals("source", attached.get("marker"));
            Assert.assertFalse(attached.containsKey(CoreOptions.BACKEND.name()));
            Assert.assertFalse(attached.containsKey(CoreOptions.STORE.name()));
            Assert.assertFalse(attached.containsKey(ServerOptions.ADMIN_PA.name()));
        } finally {
            manager.close();
        }
    }

    @Test
    public void testDoNotAttachImplicitLocalDefaults() {
        GraphManager manager = newManager(new PropertiesConfiguration());

        try {
            Map<String, Object> attached = attach(manager, new HashMap<>());

            Assert.assertFalse(attached.containsKey(
                    CoreOptions.SCHEMA_CACHE_CAPACITY.name()));
            Assert.assertFalse(attached.containsKey(
                    CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name()));
        } finally {
            manager.close();
        }
    }

    @Test
    public void testDoNotAttachResourceLimitsToAliasGraph() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(CoreOptions.SCHEMA_CACHE_CAPACITY.name(), 1000L);
        properties.setProperty(
                CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name(), 8388608);
        GraphManager manager = newManager(properties);
        Map<String, Object> configs = new HashMap<>();
        configs.put(CoreOptions.ALIAS_NAME.name(), "source_graph");
        Map<String, Object> source = new HashMap<>(configs);

        try {
            Map<String, Object> attached = attach(manager, configs);

            Assert.assertEquals(source, attached);
            Assert.assertEquals(source, configs);
        } finally {
            manager.close();
        }
    }

    private static GraphManager newManager(PropertiesConfiguration properties) {
        return new GraphManager(new HugeConfig(properties),
                                new EventHub("config-test"));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> attach(GraphManager manager,
                                               Map<String, Object> configs) {
        return Whitebox.invoke(manager.getClass(), new Class<?>[]{Map.class},
                               "attachLocalCacheConfig", manager, configs);
    }
}
