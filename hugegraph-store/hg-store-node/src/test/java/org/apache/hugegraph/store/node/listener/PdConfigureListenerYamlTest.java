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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.FileSystemResource;
import org.yaml.snakeyaml.error.YAMLException;

public class PdConfigureListenerYamlTest {

    @Test
    public void testFlattenMapsAndMultipleDocuments() throws Exception {
        Properties properties = parse("rocksdb:\n  max_background_jobs: 8\n" +
                                      "store:\n  enabled: true\n  peers: [one, two]\n" +
                                      "---\nserver:\n  port: 8500\n");
        Assertions.assertEquals(8, properties.get("rocksdb.max_background_jobs"));
        Assertions.assertEquals(true, properties.get("store.enabled"));
        Assertions.assertEquals(Arrays.asList("one", "two"), properties.get("store.peers"));
        Assertions.assertEquals(8500, properties.get("server.port"));
    }

    @Test
    public void testRejectJavaTypeBeforeConstruction() {
        UnexpectedType.constructions = 0;
        String tagged = "store: !!" + UnexpectedType.class.getName() + " {}\n";
        InvocationTargetException exception = Assertions.assertThrows(
                InvocationTargetException.class, () -> parse(tagged));
        Assertions.assertTrue(exception.getCause() instanceof YAMLException);
        Assertions.assertEquals(0, UnexpectedType.constructions);
    }

    @Test
    public void testBootLoadsShippedApplicationYaml() throws Exception {
        List<PropertySource<?>> sources = new YamlPropertySourceLoader().load(
                "store", new FileSystemResource(
                        "../hg-store-dist/src/assembly/static/conf/application.yml"));
        Assertions.assertFalse(sources.isEmpty());
        Assertions.assertNotNull(sources.get(0).getProperty("pdserver.address"));
    }

    @Test
    public void testBootKeepsDateLikeValuesAsStrings() throws Exception {
        List<PropertySource<?>> sources = new YamlPropertySourceLoader().load(
                "config", new ByteArrayResource("date: 2026-10-05\nport: 8500\n".getBytes(
                        java.nio.charset.StandardCharsets.UTF_8)));
        Assertions.assertEquals("2026-10-05", sources.get(0).getProperty("date"));
        Assertions.assertEquals(8500, sources.get(0).getProperty("port"));
    }

    @Test
    public void testBootStartsWithShippedYamlConfiguration() {
        SpringApplication application = new SpringApplication(YamlApplication.class);
        application.setWebApplicationType(WebApplicationType.NONE);
        application.setRegisterShutdownHook(false);
        try (ConfigurableApplicationContext context = application.run(
                "--spring.config.location=../hg-store-dist/src/assembly/static/conf/application.yml",
                "--logging.config=")) {
            Assertions.assertNotNull(context.getEnvironment().getProperty("pdserver.address"));
        }
    }

    @Configuration(proxyBeanMethods = false)
    public static class YamlApplication {
    }

    private static Properties parse(String yaml) throws Exception {
        Method method = PdConfigureListener.class.getDeclaredMethod("getYmlConfig", String.class);
        method.setAccessible(true);
        return (Properties) method.invoke(new PdConfigureListener(), yaml);
    }

    public static class UnexpectedType {

        private static int constructions;

        public UnexpectedType() {
            constructions++;
        }
    }
}
