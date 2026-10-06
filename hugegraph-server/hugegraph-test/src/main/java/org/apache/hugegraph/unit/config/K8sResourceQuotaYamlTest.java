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

package org.apache.hugegraph.unit.config;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.k8s.K8sDriver;
import org.apache.hugegraph.k8s.K8sManager;
import org.apache.hugegraph.testutil.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;

public class K8sResourceQuotaYamlTest {

    private Path template;
    private byte[] originalTemplate;
    private boolean existingDirectory;
    private Field driverField;
    private Object originalDriver;
    private K8sDriver driver;

    @Before
    public void setUp() throws Exception {
        this.template = Paths.get(CoreOptions.K8S_QUOTA_TEMPLATE.defaultValue());
        this.existingDirectory = Files.exists(this.template.getParent());
        this.originalTemplate = Files.exists(this.template) ? Files.readAllBytes(this.template) : null;
        Files.createDirectories(this.template.getParent());
        this.driverField = K8sManager.class.getDeclaredField("k8sDriver");
        this.driverField.setAccessible(true);
        this.originalDriver = this.driverField.get(K8sManager.instance());
        this.driver = Mockito.mock(K8sDriver.class);
        this.driverField.set(K8sManager.instance(), this.driver);
        UnexpectedType.constructions = 0;
    }

    @After
    public void tearDown() throws Exception {
        this.driverField.set(K8sManager.instance(), this.originalDriver);
        if (this.originalTemplate == null) {
            Files.deleteIfExists(this.template);
        } else {
            Files.write(this.template, this.originalTemplate);
        }
        if (!this.existingDirectory) {
            Files.deleteIfExists(this.template.getParent());
        }
    }

    @Test
    public void testQuotaMapRoundTrip() throws Exception {
        Files.write(this.template, ("apiVersion: v1\nkind: ResourceQuota\n" +
                                    "metadata:\n  name: old-quota\n  labels: {team: graphs}\n" +
                                    "spec:\n  hard: {}\n").getBytes(StandardCharsets.UTF_8));
        K8sManager.instance().loadResourceQuota("Team_Graph", 4, 8);
        ArgumentCaptor<String> yaml = ArgumentCaptor.forClass(String.class);
        Mockito.verify(this.driver).createOrReplaceResourceQuota(Mockito.eq("team-graph"),
                                                                 yaml.capture());
        Map<String, Object> quota = new Yaml(new SafeConstructor(new LoaderOptions()))
                .load(yaml.getValue());
        Map<?, ?> metadata = (Map<?, ?>) quota.get("metadata");
        Map<?, ?> hard = (Map<?, ?>) ((Map<?, ?>) quota.get("spec")).get("hard");
        Assert.assertEquals("team-graph-resource-quota", metadata.get("name"));
        Assert.assertEquals("graphs", ((Map<?, ?>) metadata.get("labels")).get("team"));
        Assert.assertEquals("4", hard.get("requests.cpu"));
        Assert.assertEquals("4", hard.get("limits.cpu"));
        Assert.assertEquals("8Gi", hard.get("requests.memory"));
        Assert.assertEquals("8Gi", hard.get("limits.memory"));
    }

    @Test
    public void testConfigBuilderReadsKubeconfig() throws Exception {
        Path kubeconfig = Files.createTempFile("hugegraph-kubeconfig-", ".yaml");
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put(Config.KUBERNETES_KUBECONFIG_FILE, kubeconfig.toString());
        properties.put(Config.KUBERNETES_AUTH_TRYKUBECONFIG_SYSTEM_PROPERTY, "true");
        properties.put(Config.KUBERNETES_AUTH_TRYSERVICEACCOUNT_SYSTEM_PROPERTY, "false");
        properties.put(Config.KUBERNETES_DISABLE_AUTO_CONFIG_SYSTEM_PROPERTY, "false");
        Map<String, String> originals = new LinkedHashMap<>();
        try {
            Files.write(kubeconfig, ("apiVersion: v1\nkind: Config\n" +
                                     "clusters:\n- name: test\n" +
                                     "  cluster: {server: 'https://127.0.0.1:6443'}\n" +
                                     "users:\n- name: test-user\n" +
                                     "  user: {token: local-test-token}\n" +
                                     "contexts:\n- name: test-context\n" +
                                     "  context: {cluster: test, user: test-user, " +
                                     "namespace: hugegraph-test}\n" +
                                     "current-context: test-context\n" +
                                     "extensions:\n- name: fixture-extension\n" +
                                     "  extension: {ratio: 0.5, count: 1}\n").getBytes(StandardCharsets.UTF_8));
            properties.forEach((key, value) -> {
                originals.put(key, System.getProperty(key));
                System.setProperty(key, value);
            });
            // This is the same auto-configuration entry point as K8sDriver,
            // without constructing a client or contacting a Kubernetes server.
            Config config = new ConfigBuilder().build();
            Assert.assertEquals("https://127.0.0.1:6443/", config.getMasterUrl());
            Assert.assertEquals("hugegraph-test", config.getNamespace());
            Assert.assertEquals("local-test-token", config.getOauthToken());
            Config roundTrip = Serialization.unmarshal(Serialization.asYaml(config), Config.class);
            Assert.assertEquals(config.getMasterUrl(), roundTrip.getMasterUrl());
            Assert.assertEquals(config.getNamespace(), roundTrip.getNamespace());
            Assert.assertEquals(config.getOauthToken(), roundTrip.getOauthToken());
        } finally {
            originals.forEach((key, value) -> {
                if (value == null) {
                    System.clearProperty(key);
                } else {
                    System.setProperty(key, value);
                }
            });
            Files.deleteIfExists(kubeconfig);
        }
    }

    @Test
    public void testRejectJavaTypeBeforeConstruction() throws Exception {
        String tagged = "!!" + UnexpectedType.class.getName() + " {}\n";
        Files.write(this.template, tagged.getBytes(StandardCharsets.UTF_8));
        K8sManager.instance().loadResourceQuota("graphs", 4, 8);
        Assert.assertEquals(0, UnexpectedType.constructions);
        Mockito.verifyNoInteractions(this.driver);
    }

    public static class UnexpectedType {

        private static int constructions;

        public UnexpectedType() {
            constructions++;
        }
    }
}
