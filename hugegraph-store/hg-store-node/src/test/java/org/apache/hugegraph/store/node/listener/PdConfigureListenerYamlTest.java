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
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.tools.ToolProvider;

import org.apache.hugegraph.pd.client.KvClient;
import org.apache.hugegraph.pd.grpc.kv.KResponse;
import org.apache.hugegraph.util.RocksDBRuntime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.FileSystemResource;
import org.yaml.snakeyaml.error.YAMLException;

public class PdConfigureListenerYamlTest {

    @TempDir
    public Path temporary;

    @Test
    public void testSelectedRuntimeSurvivesRetainedPdConfiguration() throws Exception {
        for (String provider : new String[]{"rocksdb", "topling"}) {
            StandardEnvironment environment = new StandardEnvironment();
            Properties image = new Properties();
            image.setProperty("rocksdb.provider", provider);
            environment.getPropertySources().addFirst(new PropertiesPropertySource("spring.application.json", image));
            String retained = "rocksdb:\n  provider: " +
                              ("topling".equals(provider) ? "rocksdb" : "topling") +
                              "\n  max_background_jobs: 8\n";
            Path file = this.temporary.resolve(provider + "-application-pd.yml");
            Files.writeString(file, retained);
            PdConfigureListener listener = new PdConfigureListener();
            set(listener, "pdConfFile", file.toFile());
            set(listener, "selectedProvider", provider);
            KvClient client = Mockito.mock(KvClient.class);
            Mockito.when(client.get("S:Timestamp")).thenReturn(
                    KResponse.newBuilder().setValue(Long.toString(System.currentTimeMillis() + 60000)).build());
            Method update = PdConfigureListener.class.getDeclaredMethod(
                    "updatePdConfig", org.springframework.core.env.MutablePropertySources.class,
                    KvClient.class, String.class);
            update.setAccessible(true);
            update.invoke(listener, environment.getPropertySources(), client, retained);
            Assertions.assertEquals(provider, environment.getProperty("rocksdb.provider"));
            Assertions.assertEquals("8", environment.getProperty("rocksdb.max_background_jobs"));
            Assertions.assertEquals(retained, Files.readString(file));
            verifyRuntime(provider, environment.getProperty("rocksdb.provider"));
        }
    }

    @Test
    public void testNoRuntimeSelectionDoesNotOverrideManagedProvider() throws Exception {
        StandardEnvironment environment = new StandardEnvironment();
        Properties configured = new Properties();
        configured.setProperty("rocksdb.provider", "topling");
        environment.getPropertySources().addFirst(new PropertiesPropertySource("pd-config", configured));
        Method prioritize = PdConfigureListener.class.getDeclaredMethod(
                "prioritizeSelectedProvider", org.springframework.core.env.MutablePropertySources.class);
        prioritize.setAccessible(true);
        prioritize.invoke(new PdConfigureListener(), environment.getPropertySources());
        Assertions.assertEquals("topling", environment.getProperty("rocksdb.provider"));
        Assertions.assertFalse(environment.getPropertySources().contains("rocksdb-runtime-provider"));
    }

    private static void set(PdConfigureListener listener, String name, Object value) throws Exception {
        Field field = PdConfigureListener.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(listener, value);
    }

    private void verifyRuntime(String runtime, String configured) throws Exception {
        Path classes = Files.createDirectory(this.temporary.resolve(runtime));
        Path source = classes.resolve("RocksDB.java");
        Files.writeString(source, "package org.rocksdb; public class RocksDB {}" +
                                  ("topling".equals(runtime) ? " class SidePluginRepo {}" : ""));
        Assertions.assertEquals(0, ToolProvider.getSystemJavaCompiler().run(
                null, null, null, "-d", classes.toString(), source.toString()));
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader loader = new URLClassLoader(new URL[]{classes.toUri().toURL()}, null)) {
            Thread.currentThread().setContextClassLoader(loader);
            RocksDBRuntime.verify(configured);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

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
