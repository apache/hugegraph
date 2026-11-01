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

package org.apache.hugegraph.unit;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.config.ConfigException;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.RpcOptions;
import org.apache.hugegraph.rpc.RpcClientProvider;
import org.apache.hugegraph.rpc.RpcConsumerConfig;
import org.apache.hugegraph.rpc.RpcProviderConfig;
import org.apache.hugegraph.rpc.RpcServer;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.ServerClientTest.HelloService;
import org.apache.hugegraph.unit.ServerClientTest.HelloServiceImpl;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alipay.sofa.rpc.bootstrap.ConsumerBootstrap;
import com.alipay.sofa.rpc.bootstrap.DefaultConsumerBootstrap;
import com.alipay.sofa.rpc.client.Cluster;
import com.alipay.sofa.rpc.client.FailoverCluster;
import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.alipay.sofa.rpc.config.ProviderConfig;
import com.alipay.sofa.rpc.ext.Extension;
import com.alipay.sofa.rpc.ext.ExtensionLoaderFactory;

public class RpcConfigTest extends BaseUnitTest {

    private static final String HELLO = HelloService.class.getName();

    private static ConsumerFailure consumerFailure;

    @BeforeClass
    public static void registerCleanupBootstrap() {
        ExtensionLoaderFactory.getExtensionLoader(ConsumerBootstrap.class)
                              .loadExtension(CleanupBootstrap.class);
        ExtensionLoaderFactory.getExtensionLoader(Cluster.class)
                              .loadExtension(CleanupCluster.class);
    }

    @Test
    public void testReferFailureRetainsCleanupOwner() {
        this.assertReferCleanup(new IllegalStateException("refer failure"), false, false);
    }

    @Test
    public void testReferErrorPreservedOnSelfSuppression() {
        this.assertReferCleanup(new AssertionError("refer error"), true, false);
    }

    @Test
    public void testCleanupErrorPromotedAboveReferException() {
        this.assertReferCleanup(new IllegalStateException("refer failure"), false, true);
    }

    @Test
    public void testReferFailureWithSuccessfulCleanupReleasesOwner() {
        RpcConsumerConfig owner = new RpcConsumerConfig(new HugeConfig(Collections.emptyMap()),
                                                       "127.0.0.1:1");
        ConsumerFailure control = new ConsumerFailure(owner, new IllegalStateException("refer failure"));
        control.disarm();
        try {
            Assert.assertSame(control.referFailure, captureFailure(() -> owner.serviceProxy(HELLO)));
            Assert.assertGt(0, control.destroyCalls.get());
            Assert.assertFalse(control.retained());
            Assert.assertNull(control.bootstrap.getProxyIns());
        } finally {
            control.close();
        }
    }

    private void assertReferCleanup(Throwable primary, boolean sameCleanup, boolean fatalCleanup) {
        RpcConsumerConfig owner = new RpcConsumerConfig(new HugeConfig(Collections.emptyMap()),
                                                       "127.0.0.1:1");
        ConsumerFailure control = new ConsumerFailure(owner, primary);
        control.unreferFailure = sameCleanup ? primary : new IllegalStateException("unrefer failure");
        if (fatalCleanup) {
            control.clusterFailure = new AssertionError("cluster error");
        } else if (sameCleanup) {
            control.clusterFailure = new AssertionError("secondary cluster error");
        }
        try {
            Throwable thrown = captureFailure(() -> owner.serviceProxy(HELLO));
            Assert.assertNotNull(control.bootstrap.getProxyIns());
            Assert.assertSame(fatalCleanup ? control.clusterFailure : primary, thrown);
            if (fatalCleanup) {
                Assert.assertSame(primary, thrown.getSuppressed()[0]);
            }
            Assert.assertGt(0, control.destroyCalls.get());
            Assert.assertTrue(control.retained());
            control.disarm();
            owner.destroy();
            Assert.assertFalse(control.retained());
            Assert.assertNull(control.bootstrap.getProxyIns());
        } finally {
            control.close();
        }
    }

    @Test
    public void testClusterCleanupFailureRetainsOwnerForRetry() {
        RpcConsumerConfig owner = new RpcConsumerConfig(new HugeConfig(Collections.emptyMap()),
                                                       "127.0.0.1:1");
        ConsumerFailure control = new ConsumerFailure(owner, new IllegalStateException("refer failure"));
        control.failUnrefer = false;
        control.clusterFailure = new IllegalStateException("cluster failure");
        try {
            Assert.assertSame(control.referFailure, captureFailure(() -> owner.serviceProxy(HELLO)));
            // Native unRefer swallows a cluster Exception, so the explicit destroy must also run.
            Assert.assertGte(2, control.destroyCalls.get());
            Assert.assertTrue(control.retained());
            control.disarm();
            owner.destroy();
            Assert.assertFalse(control.retained());
        } finally {
            control.close();
        }
    }

    @Test
    public void testDestroyContinuesRemainingConsumers() {
        RpcConsumerConfig owner = new RpcConsumerConfig(new HugeConfig(Collections.emptyMap()),
                                                       "127.0.0.1:1");
        ConsumerFailure first = new ConsumerFailure(owner, new IllegalStateException("first refer"));
        ConsumerFailure second = null;
        try {
            captureFailure(() -> owner.serviceProxy(HELLO));
            second = new ConsumerFailure(owner, new IllegalStateException("second refer"));
            captureFailure(() -> owner.serviceProxy(HELLO));
            second.disarm();
            Assert.assertSame(first.unreferFailure, captureFailure(owner::removeAllServiceProxy));
            Assert.assertNull(second.bootstrap.getProxyIns());
            // Unrefer-only cleanup retains owners for the subsequent explicit destroy.
            Assert.assertTrue(second.retained());
            Assert.assertSame(first.unreferFailure, captureFailure(owner::destroy));
            Assert.assertTrue(first.retained());
            Assert.assertFalse(second.retained());
            Assert.assertNull(second.bootstrap.getProxyIns());
            first.disarm();
            owner.destroy();
            Assert.assertFalse(first.retained());
        } finally {
            first.close();
            if (second != null) {
                second.close();
            }
        }
    }

    @Test
    public void testRemoveConsumerRetainsFailedCleanupOwner() {
        RpcConsumerConfig owner = new RpcConsumerConfig(new HugeConfig(Collections.emptyMap()),
                                                       "127.0.0.1:1");
        ConsumerFailure control = new ConsumerFailure(owner, null);
        try {
            HelloService proxy = owner.serviceProxy(HELLO);
            Assert.assertSame(control.unreferFailure, captureFailure(() -> {
                owner.removeServiceProxy(null, HELLO, proxy);
            }));
            Assert.assertGt(0, control.destroyCalls.get());
            Assert.assertTrue(control.retained());
            Map<String, ConsumerConfig<?>> configs = Whitebox.getInternalState(owner, "configs");
            Assert.assertSame(control.bootstrap.getConsumerConfig(), configs.get(HELLO));
            control.disarm();
            owner.destroy();
            Assert.assertFalse(control.retained());
        } finally {
            control.close();
        }
    }

    private static Throwable captureFailure(Runnable action) {
        try {
            action.run();
        } catch (RuntimeException | Error failure) {
            return failure;
        }
        throw new AssertionError("Expected consumer failure");
    }

    private static final class ConsumerFailure implements AutoCloseable {

        private static final AtomicInteger IDS = new AtomicInteger();
        private final RpcConsumerConfig owner;
        private final Throwable referFailure;
        private final AtomicInteger destroyCalls = new AtomicInteger();
        private Throwable unreferFailure = new IllegalStateException("unrefer failure");
        private Throwable clusterFailure;
        private boolean failUnrefer = true;
        private CleanupBootstrap bootstrap;

        private ConsumerFailure(RpcConsumerConfig owner, Throwable referFailure) {
            this.owner = owner;
            this.referFailure = referFailure;
            ConsumerConfig<HelloService> config = new ConsumerConfig<>();
            config.setInterfaceId(HELLO).setUniqueId("cleanup-" + IDS.incrementAndGet())
                  .setProtocol("bolt").setDirectUrl("127.0.0.1:1")
                  .setLazy(true).setCheck(false).setInJVM(false)
                  .setBootstrap("hugegraph_cleanup_test").setCluster("hugegraph_cleanup_test");
            Map<String, ConsumerConfig<?>> configs = Whitebox.getInternalState(owner, "configs");
            configs.put(HELLO, config);
            consumerFailure = this;
        }

        private boolean retained() {
            List<?> bootstraps = Whitebox.getInternalState(this.owner, "bootstraps");
            return bootstraps.contains(this.bootstrap);
        }

        private void disarm() {
            this.failUnrefer = false;
            this.clusterFailure = null;
        }

        @Override
        public void close() {
            this.disarm();
            if (this.bootstrap != null) {
                this.bootstrap.unRefer();
                this.bootstrap.getCluster().destroy();
            }
            consumerFailure = null;
        }
    }

    @Extension("hugegraph_cleanup_test")
    public static final class CleanupBootstrap extends DefaultConsumerBootstrap<HelloService> {

        private final ConsumerFailure control;

        public CleanupBootstrap(ConsumerConfig<HelloService> config) {
            super(config);
            this.control = consumerFailure;
            this.control.bootstrap = this;
        }

        @Override
        public HelloService refer() {
            HelloService proxy = super.refer();
            if (this.control.referFailure != null) {
                throwFailure(this.control.referFailure);
            }
            return proxy;
        }

        @Override
        public void unRefer() {
            if (this.control.failUnrefer) {
                throwFailure(this.control.unreferFailure);
            }
            super.unRefer();
        }

        private static void throwFailure(Throwable failure) {
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            throw (RuntimeException) failure;
        }
    }

    @Extension("hugegraph_cleanup_test")
    public static final class CleanupCluster extends FailoverCluster {

        private final ConsumerFailure control;

        public CleanupCluster(ConsumerBootstrap<?> bootstrap) {
            super(bootstrap);
            this.control = ((CleanupBootstrap) bootstrap).control;
        }

        @Override
        public void destroy() {
            // Use the native cluster lifecycle; control only the cleanup failure boundary.
            this.control.destroyCalls.incrementAndGet();
            if (this.control.clusterFailure != null) {
                CleanupBootstrap.throwFailure(this.control.clusterFailure);
            }
            super.destroy();
        }
    }

    @Test
    public void testExcludeSelfUrl() {
        Assert.assertEquals("127.0.0.1:8092,127.0.0.1:8093",
                            excludeSelfUrl("127.0.0.1:8091,127.0.0.1:8092;127.0.0.1:8093",
                                           "127.0.0.1:8091"));
        // Order is kept and duplicates are collapsed
        Assert.assertEquals("b:2,a:1",
                            excludeSelfUrl("b:2,a:1,b:2,self:0", "self:0"));
        Assert.assertEquals("a:1", excludeSelfUrl("a:1", "self:0"));
        Assert.assertEquals("", excludeSelfUrl("self:0,self:0", "self:0"));
        Assert.assertEquals("", excludeSelfUrl("", "self:0"));
    }

    @Test
    public void testClientDisabledWhenOnlySelfIsRemote() {
        Map<String, Object> options = new HashMap<>();
        options.put(RpcOptions.RPC_SERVER_HOST.name(), "127.0.0.1");
        options.put(RpcOptions.RPC_SERVER_PORT.name(), "8091");
        options.put(RpcOptions.RPC_REMOTE_URL.name(), "127.0.0.1:8091");
        RpcClientProvider selfOnly = new RpcClientProvider(new HugeConfig(options));
        Assert.assertFalse(selfOnly.enabled());
        Assert.assertThrows(IllegalArgumentException.class, selfOnly::config, e -> {
            Assert.assertContains("add an address other than self service", e.getMessage());
        });
        // Both are no-ops when the client is disabled
        selfOnly.unreferAll();
        selfOnly.destroy();

        options.put(RpcOptions.RPC_REMOTE_URL.name(), "127.0.0.1:8091,127.0.0.1:8092");
        RpcClientProvider withPeer = new RpcClientProvider(new HugeConfig(options));
        try {
            Assert.assertTrue(withPeer.enabled());
            // Self is dropped from the direct URL the SOFA consumer is built with
            Assert.assertEquals("127.0.0.1:8092",
                                consumerConfig(withPeer.config(), null, HELLO).getDirectUrl());
        } finally {
            withPeer.destroy();
        }
    }

    @Test
    public void testRpcOptionsRejectInvalidValues() {
        assertInvalidOption(RpcOptions.RPC_PROTOCOL.name(), "grpc");
        assertInvalidOption(RpcOptions.RPC_SERIALIZATION.name(), "json");
        assertInvalidOption(RpcOptions.RPC_CLIENT_LOAD_BALANCER.name(), "leastActive");
        assertInvalidOption(RpcOptions.RPC_SERVER_PORT.name(), "-1");
        assertInvalidOption(RpcOptions.RPC_SERVER_TIMEOUT.name(), "0");
        assertInvalidOption(RpcOptions.RPC_CLIENT_RETRIES.name(), "-1");
        assertInvalidOption(RpcOptions.RPC_CLIENT_READ_TIMEOUT.name(), "ten");
        assertInvalidOption(RpcOptions.RPC_LOGGER_IMPL.name(), " ");

        Map<String, Object> options = new HashMap<>();
        options.put(RpcOptions.RPC_SERVER_PORT.name(), "0");
        options.put(RpcOptions.RPC_CLIENT_RETRIES.name(), "0");
        options.put(RpcOptions.RPC_CLIENT_LOAD_BALANCER.name(), "roundRobin");
        HugeConfig config = new HugeConfig(options);
        Assert.assertEquals(0, config.get(RpcOptions.RPC_SERVER_PORT));
        Assert.assertEquals(0, config.get(RpcOptions.RPC_CLIENT_RETRIES));
        Assert.assertEquals("roundRobin", config.get(RpcOptions.RPC_CLIENT_LOAD_BALANCER));
    }

    @Test
    public void testProviderServiceIds() {
        RpcProviderConfig provider = new RpcProviderConfig();
        String plain = provider.addService(HelloService.class, new HelloServiceImpl());
        String graph1 = provider.addService("g1", HelloService.class, new HelloServiceImpl());
        String graph2 = provider.addService("g2", HelloService.class, new HelloServiceImpl());
        Assert.assertEquals(HELLO, plain);
        Assert.assertEquals(HELLO + ":g1", graph1);
        Assert.assertEquals(HELLO + ":g2", graph2);
        Assert.assertEquals(3, provider.configs().size());

        ProviderConfig<?> config = provider.configs().get(graph1);
        Assert.assertEquals(graph1, config.getId());
        Assert.assertEquals("g1", config.getUniqueId());
        Assert.assertEquals(HELLO, config.getInterfaceId());
        Assert.assertEquals("", provider.configs().get(plain).getUniqueId());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            provider.addService("g1", HelloService.class, new HelloServiceImpl());
        }, e -> {
            Assert.assertContains("Not allowed to add service already exist: '" +
                                  graph1 + "'", e.getMessage());
        });

        provider.removeService(graph1);
        Assert.assertFalse(provider.configs().containsKey(graph1));
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            provider.removeService(graph1);
        }, e -> {
            Assert.assertContains("doesn't exist", e.getMessage());
        });
        // A removed id can be registered again
        Assert.assertEquals(graph1, provider.addService("g1", HelloService.class,
                                                        new HelloServiceImpl()));

        provider.removeAllService();
        Assert.assertTrue(provider.configs().isEmpty());
    }

    @Test
    public void testConsumerConfigFromOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put(RpcOptions.RPC_CLIENT_READ_TIMEOUT.name(), "7");
        options.put(RpcOptions.RPC_CLIENT_CONNECT_TIMEOUT.name(), "3");
        options.put(RpcOptions.RPC_CLIENT_RECONNECT_PERIOD.name(), "5");
        options.put(RpcOptions.RPC_CLIENT_RETRIES.name(), "2");
        options.put(RpcOptions.RPC_CLIENT_LOAD_BALANCER.name(), "random");
        String remote = "127.0.0.1:8092,127.0.0.1:8093";
        RpcConsumerConfig consumer = new RpcConsumerConfig(new HugeConfig(options), remote);

        ConsumerConfig<?> plain = consumerConfig(consumer, null, HELLO);
        Assert.assertEquals(HELLO, plain.getInterfaceId());
        Assert.assertEquals("bolt", plain.getProtocol());
        Assert.assertEquals("hessian2", plain.getSerialization());
        Assert.assertEquals(remote, plain.getDirectUrl());
        Assert.assertEquals(7000, plain.getTimeout());
        Assert.assertEquals(3000, plain.getConnectTimeout());
        Assert.assertEquals(5000, plain.getReconnectPeriod());
        Assert.assertEquals(2, plain.getRetries());
        Assert.assertEquals("random", plain.getLoadBalancer());
        Assert.assertEquals("", plain.getUniqueId());
        Assert.assertNotEquals("fanout", plain.getCluster());

        // Graph scoped consumers broadcast to every provider of that graph
        ConsumerConfig<?> graph = consumerConfig(consumer, "g1", HELLO);
        Assert.assertEquals(HELLO + ":g1", graph.getId());
        Assert.assertEquals("g1", graph.getUniqueId());
        Assert.assertEquals("fanout", graph.getCluster());

        consumer.destroy();
    }

    @Test
    public void testConsumerConfigCachedThroughServiceProxy() {
        RpcServer server = new RpcServer(config("server-random"));
        RpcConsumerConfig consumer = null;
        try {
            server.config().addService(HelloService.class, new HelloServiceImpl());
            server.config().addService("g1", HelloService.class, new HelloServiceImpl());
            server.config().addService("g2", HelloService.class, new HelloServiceImpl());
            server.exportAll();
            consumer = new RpcConsumerConfig(config("client"), server.host() + ":" + server.port());
            ConsumerConfig<?> plain = referredConfig(consumer, null);
            ConsumerConfig<?> graph = referredConfig(consumer, "g1");
            Assert.assertSame(plain, referredConfig(consumer, null));
            Assert.assertSame(graph, referredConfig(consumer, "g1"));
            Assert.assertNotSame(graph, referredConfig(consumer, "g2"));
        } finally {
            if (consumer != null) {
                consumer.destroy();
            }
            stopServer(server);
        }
    }

    private static ConsumerConfig<?> referredConfig(RpcConsumerConfig consumer, String graph) {
        HelloService proxy = consumer.serviceProxy(graph, HELLO);
        Assert.assertEquals("hello tom!", proxy.hello("tom"));
        Map<String, ConsumerConfig<?>> configs = Whitebox.getInternalState(consumer, "configs");
        String id = graph == null ? HELLO : HELLO + ":" + graph;
        Assert.assertNotNull(configs.get(id));
        return configs.get(id);
    }

    @Test
    public void testServerDestroyClosesServices() {
        RpcServer server = new RpcServer(config("server-random"));
        Assert.assertTrue(server.enabled());
        // Host and port are only known once the server is bound
        Assert.assertNull(server.host());
        Assert.assertEquals(0, server.port());

        CloseableHelloService closeable = new CloseableHelloService(false);
        CloseableHelloService failing = new CloseableHelloService(true);
        server.config().addService(HelloService.class, closeable);
        server.config().addService("g1", HelloService.class, failing);
        try {
            server.exportAll();
            Assert.assertEquals("127.0.0.1", server.host());
            Assert.assertGt(0, server.port());
        } finally {
            server.destroy();
        }

        Assert.assertEquals(1, closeable.closed.get());
        // A failure while closing one service doesn't stop the shutdown
        Assert.assertEquals(1, failing.closed.get());
        Assert.assertTrue(server.config().configs().isEmpty());
    }

    private static String excludeSelfUrl(String rpcUrl, String selfUrl) {
        return Whitebox.invokeStatic(RpcClientProvider.class, "excludeSelfUrl",
                                     rpcUrl, selfUrl);
    }

    private static ConsumerConfig<?> consumerConfig(RpcConsumerConfig consumer,
                                                    String graph, String interfaceId) {
        return Whitebox.invoke(RpcConsumerConfig.class,
                               new Class<?>[]{String.class, String.class},
                               "consumerConfig", consumer, graph, interfaceId);
    }

    private static void assertInvalidOption(String key, String value) {
        Map<String, Object> options = new HashMap<>();
        options.put(key, value);
        Assert.assertThrows(ConfigException.class, () -> {
            new HugeConfig(options);
        }, e -> {
            Assert.assertContains(key, e.getMessage());
        });
    }

    public static class CloseableHelloService extends HelloServiceImpl
                                              implements AutoCloseable {

        private final boolean failOnClose;
        private final AtomicInteger closed = new AtomicInteger();

        public CloseableHelloService(boolean failOnClose) {
            this.failOnClose = failOnClose;
        }

        @Override
        public void close() {
            this.closed.incrementAndGet();
            if (this.failOnClose) {
                throw new IllegalStateException("close failure for test");
            }
        }
    }
}
