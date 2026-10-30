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

package org.apache.hugegraph.store.node.listener;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.SimpleCommandLinePropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;

/**
 * PdConfigureListener puts the PD-managed application-pd.yml in front of every other
 * property source. An explicit per-Store rocksdb.total_memory_size, from a command-line
 * argument, the Docker entrypoint's SPRING_APPLICATION_JSON or -D, must still win when
 * RocksdbConfig binds, while the rest of the PD configuration keeps its precedence.
 */
public class PdConfigureListenerMemoryOverrideTest {

    private static final String PD_BUDGET = "32000000000";
    private static final String PD_WRITE_BUFFER = "64000000";

    @Test
    public void testDockerOverrideWinsOverPdConfig() {
        Map<String, String> rocksdb = bind(environment(json("268435456"), null));
        assertEquals("268435456", rocksdb.get("total_memory_size"));
        // Other PD-managed keys keep their precedence
        assertEquals(PD_WRITE_BUFFER, rocksdb.get("write_buffer_size"));
    }

    @Test
    public void testDockerAutoOverrideWinsOverPdConfig() {
        assertEquals("auto", bind(environment(json("auto"), null)).get("total_memory_size"));
    }

    @Test
    public void testSystemPropertyOverrideWinsOverPdConfig() {
        assertEquals("536870912",
                     bind(environment(null, "536870912")).get("total_memory_size"));
        // SPRING_APPLICATION_JSON outranks -D, as without PD configuration
        assertEquals("268435456", bind(environment(json("268435456"), "536870912"))
                .get("total_memory_size"));
    }

    @Test
    public void testCommandLineOverrideWinsOverPdConfig() {
        assertEquals("1073741824",
                     bind(withCommandLine(environment(null, null))).get("total_memory_size"));
        // Spring puts command-line arguments ahead of SPRING_APPLICATION_JSON
        assertEquals("1073741824", bind(withCommandLine(environment(json("268435456"), null)))
                .get("total_memory_size"));
    }

    private static StandardEnvironment withCommandLine(StandardEnvironment env) {
        env.getPropertySources().addFirst(new SimpleCommandLinePropertySource(
                "--" + PdConfigureListener.MEMORY_KEY + "=1073741824"));
        return env;
    }

    @Test
    public void testEnvironmentVariableKeepsPdConfig() {
        StandardEnvironment env = environment(null, null);
        Map<String, Object> variables = new HashMap<>();
        variables.put("ROCKSDB_TOTAL_MEMORY_SIZE", "268435456");
        env.getPropertySources().addBefore("applicationConfig: [application-pd.yml]",
                                           new SystemEnvironmentPropertySource(
                                                   StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                                                   variables));
        // The variable binds to the total.memory.size key, not the budget, so it must not
        // drop PD's value and leave the Store on its local file
        Map<String, String> rocksdb = bind(env);
        assertEquals(PD_BUDGET, rocksdb.get("total_memory_size"));
        assertEquals("268435456", rocksdb.get("total.memory.size"));
    }

    @Test
    public void testWithoutOverridePdConfigStillApplies() {
        Map<String, String> rocksdb = bind(environment(null, null));
        assertEquals(PD_BUDGET, rocksdb.get("total_memory_size"));
        assertEquals(PD_WRITE_BUFFER, rocksdb.get("write_buffer_size"));
    }

    @Test
    public void testOnlyTheMemoryKeyIsDropped() {
        StandardEnvironment env = environment(json("268435456"), null);
        Properties pd = pdConfig();
        PdConfigureListener.dropOverriddenMemory(pd, env.getPropertySources());
        assertFalse(pd.containsKey(PdConfigureListener.MEMORY_KEY));
        assertEquals(PD_WRITE_BUFFER, pd.getProperty("rocksdb.write_buffer_size"));
    }

    @Test
    public void testReapplyingKeepsOnePdSource() {
        StandardEnvironment env = environment(json("268435456"), null);
        applyPdConfig(env);
        int size = env.getPropertySources().size();
        // A PD config change re-runs updatePdConfig on the same sources, and the new
        // pd-config replaces the old one
        applyPdConfig(env);
        assertEquals(size, env.getPropertySources().size());
        assertEquals("268435456", env.getProperty(PdConfigureListener.MEMORY_KEY));
    }

    private static Map<String, String> bind(StandardEnvironment env) {
        applyPdConfig(env);
        return Binder.get(env).bind("rocksdb", Bindable.mapOf(String.class, String.class))
                     .orElseGet(HashMap::new);
    }

    // What updatePdConfig() does with the PD copy
    private static void applyPdConfig(StandardEnvironment env) {
        MutablePropertySources sources = env.getPropertySources();
        Properties pd = pdConfig();
        PdConfigureListener.dropOverriddenMemory(pd, sources);
        sources.addFirst(new PropertiesPropertySource("pd-config", pd));
    }

    /**
     * The sources as the listener sees them: SPRING_APPLICATION_JSON, system properties and
     * the local yml, in Spring Boot's order.
     */
    private static StandardEnvironment environment(MapPropertySource json,
                                                   String systemProperty) {
        StandardEnvironment env = new StandardEnvironment();
        MutablePropertySources sources = env.getPropertySources();
        // Keep the real JVM and process environment out of the test
        sources.remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
        sources.remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
        Map<String, Object> local = new HashMap<>();
        local.put(PdConfigureListener.MEMORY_KEY, "auto");
        local.put("rocksdb.write_buffer_size", "32000000");
        sources.addLast(new MapPropertySource("applicationConfig: [application-pd.yml]", local));
        if (systemProperty != null) {
            Map<String, Object> props = new HashMap<>();
            props.put(PdConfigureListener.MEMORY_KEY, systemProperty);
            sources.addFirst(new MapPropertySource(
                    StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME, props));
        }
        if (json != null) {
            sources.addFirst(json);
        }
        return env;
    }

    private static Properties pdConfig() {
        Properties pd = new Properties();
        pd.put(PdConfigureListener.MEMORY_KEY, PD_BUDGET);
        pd.put("rocksdb.write_buffer_size", PD_WRITE_BUFFER);
        return pd;
    }

    private static MapPropertySource json(String budget) {
        Map<String, Object> json = new HashMap<>();
        json.put(PdConfigureListener.MEMORY_KEY, budget);
        json.put("app.data-path", "/hugegraph-store/storage");
        return new MapPropertySource("spring.application.json", json);
    }
}
