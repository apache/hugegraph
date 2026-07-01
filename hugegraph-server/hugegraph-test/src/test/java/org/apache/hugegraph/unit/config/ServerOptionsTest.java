/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.unit.config;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.testutil.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ServerOptionsTest {

    @BeforeClass
    public static void init() {
        OptionSpace.register("server",
                             ServerOptions.class.getName());
    }

    @Test
    public void testUrlOptionNormalizeAddsDefaultScheme() {
        PropertiesConfiguration conf = new PropertiesConfiguration();
        conf.setProperty("restserver.url", "127.0.0.1:8080");
        conf.setProperty("gremlinserver.url", "127.0.0.1:8182");
        conf.setProperty("server.urls_to_pd", "0.0.0.0:8080");
        conf.setProperty("server.k8s_url", "127.0.0.1:8888");

        HugeConfig config = new HugeConfig(conf);

        Assert.assertEquals("http://127.0.0.1:8080",
                            config.get(ServerOptions.REST_SERVER_URL));
        Assert.assertEquals("http://127.0.0.1:8182",
                            config.get(ServerOptions.GREMLIN_SERVER_URL));
        Assert.assertEquals("http://0.0.0.0:8080",
                            config.get(ServerOptions.SERVER_URLS_TO_PD));
        Assert.assertEquals("https://127.0.0.1:8888",
                            config.get(ServerOptions.SERVER_K8S_URL));
    }

    @Test
    public void testUrlNormalizationEdgeCases() {
        // Whitespace trimming
        PropertiesConfiguration conf = new PropertiesConfiguration();
        conf.setProperty("restserver.url", "  127.0.0.1:8080  ");
        HugeConfig config = new HugeConfig(conf);
        Assert.assertEquals("http://127.0.0.1:8080",
                            config.get(ServerOptions.REST_SERVER_URL));

        // Case normalization
        conf = new PropertiesConfiguration();
        conf.setProperty("restserver.url", "HTTP://127.0.0.1:8080");
        config = new HugeConfig(conf);
        Assert.assertEquals("http://127.0.0.1:8080",
                            config.get(ServerOptions.REST_SERVER_URL));

        // IPv6 without scheme
        conf = new PropertiesConfiguration();
        conf.setProperty("restserver.url", "[::1]:8080");
        config = new HugeConfig(conf);
        Assert.assertEquals("http://[::1]:8080",
                            config.get(ServerOptions.REST_SERVER_URL));

        // IPv6 with existing scheme
        conf = new PropertiesConfiguration();
        conf.setProperty("restserver.url", "http://[::1]:8080");
        config = new HugeConfig(conf);
        Assert.assertEquals("http://[::1]:8080",
                            config.get(ServerOptions.REST_SERVER_URL));
    }

    @Test
    public void testUrlNormalizationPreservesHostnameCase() {
        // Uppercase scheme + mixed-case hostname
        PropertiesConfiguration conf = new PropertiesConfiguration();
        conf.setProperty("restserver.url", "HTTP://MyServer:8080");
        HugeConfig config = new HugeConfig(conf);
        // Should lowercase ONLY the scheme, preserve "MyServer"
        Assert.assertEquals("http://MyServer:8080",
                            config.get(ServerOptions.REST_SERVER_URL));

        // Use server.k8s_url for HTTPS test (it defaults to https://)
        conf = new PropertiesConfiguration();
        conf.setProperty("server.k8s_url", "HTTPS://MyHost:8888");
        config = new HugeConfig(conf);
        Assert.assertEquals("https://MyHost:8888",
                            config.get(ServerOptions.SERVER_K8S_URL));
    }

    @Test
    public void testUrlNormalizationPreservesPathCase() {
        PropertiesConfiguration conf = new PropertiesConfiguration();
        conf.setProperty("restserver.url", "http://127.0.0.1:8080/SomePath/CaseSensitive");
        HugeConfig config = new HugeConfig(conf);
        Assert.assertEquals("http://127.0.0.1:8080/SomePath/CaseSensitive",
                            config.get(ServerOptions.REST_SERVER_URL));
    }

    @Test
    public void testHttpsSchemeIsNotDowngraded() {
        PropertiesConfiguration conf = new PropertiesConfiguration();
        conf.setProperty("restserver.url", "https://127.0.0.1:8080");
        HugeConfig config = new HugeConfig(conf);
        Assert.assertEquals("https://127.0.0.1:8080",
                            config.get(ServerOptions.REST_SERVER_URL));
    }
}
