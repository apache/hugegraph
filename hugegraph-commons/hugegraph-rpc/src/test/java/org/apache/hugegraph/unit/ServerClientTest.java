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

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.rpc.RpcClientProvider;
import org.apache.hugegraph.rpc.RpcCommonConfig;
import org.apache.hugegraph.rpc.RpcConsumerConfig;
import org.apache.hugegraph.rpc.RpcProviderConfig;
import org.apache.hugegraph.rpc.RpcServer;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.util.E;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alipay.sofa.rpc.common.RpcOptions;
import com.alipay.sofa.rpc.config.ProviderConfig;
import com.alipay.sofa.rpc.config.ServerConfig;
import com.alipay.sofa.rpc.core.exception.SofaRpcException;
import com.alipay.sofa.rpc.core.exception.SofaRpcRuntimeException;
import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelFuture;

public class ServerClientTest extends BaseUnitTest {

    private static RpcServer rpcServer;
    private static RpcClientProvider rpcClient;

    @BeforeClass
    public static void init() {
        rpcServer = new RpcServer(config(true));
        rpcClient = new RpcClientProvider(config(false));
    }

    @AfterClass
    public static void clear() {
        if (rpcClient != null) {
            rpcClient.destroy();
        }
        if (rpcServer != null) {
            stopServer(rpcServer);
        }
    }

    @After
    public void teardown() {
        if (rpcClient != null) {
            rpcClient.unreferAll();
        }
        if (rpcServer != null) {
            unexportServer(rpcServer);
        }
    }

    @Test
    public void testUnexportWaitsBeforeReusingListenerPort() throws Exception {
        rpcServer.config().addService(HelloService.class, new HelloServiceImpl());
        startServer(rpcServer);
        ChannelFuture binding = listener(rpcServer);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        binding.channel().eventLoop().execute(() -> {
            entered.countDown();
            try {
                E.checkState(release.await(5, TimeUnit.SECONDS), "RPC close barrier timed out");
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(failure);
            }
        });
        try {
            Assert.assertTrue(entered.await(5, TimeUnit.SECONDS));
            Future<?> closing = worker.submit(() -> unexportServer(rpcServer));
            Assert.assertThrows(TimeoutException.class, () -> closing.get(1, TimeUnit.SECONDS));
            Assert.assertTrue(rpcServer.config().configs().isEmpty());
            Assert.assertTrue(binding.channel().isOpen());
            Assert.assertFalse(binding.channel().closeFuture().isDone());
            release.countDown();
            closing.get(5, TimeUnit.SECONDS);
            Assert.assertTrue(binding.channel().closeFuture().isSuccess());
            rpcServer.config().addService(HelloService.class, new HelloServiceImpl());
            startServer(rpcServer);
            HelloService client = rpcClient.config().serviceProxy(HelloService.class);
            Assert.assertEquals("hello tom!", client.hello("tom"));
        } finally {
            release.countDown();
            worker.shutdownNow();
            Assert.assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testSimpleService() {
        // Init server
        RpcProviderConfig serverConfig = rpcServer.config();
        serverConfig.addService(HelloService.class, new HelloServiceImpl());
        startServer(rpcServer);

        // Init client
        RpcConsumerConfig clientConfig = rpcClient.config();
        HelloService client = clientConfig.serviceProxy(HelloService.class);

        // Test call
        Assert.assertEquals("hello tom!", client.hello("tom"));
        Assert.assertEquals("tom", client.echo("tom"));
        Assert.assertEquals(5.14, client.sum(2, 3.14), 0.00000001d);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            client.hello("");
        }, e -> {
            Assert.assertContains("empty hello parameter", e.getMessage());
        });
    }

    @Test
    public void testMultiService() {
        // Init server
        GraphHelloServiceImpl g1 = new GraphHelloServiceImpl("g1");
        GraphHelloServiceImpl g2 = new GraphHelloServiceImpl("g2");
        GraphHelloServiceImpl g3 = new GraphHelloServiceImpl("g3");

        RpcProviderConfig serverConfig = rpcServer.config();
        serverConfig.addService(g1.graph(), HelloService.class, g1);
        serverConfig.addService(g2.graph(), HelloService.class, g2);
        serverConfig.addService(g3.graph(), HelloService.class, g3);
        startServer(rpcServer);

        // Init client
        RpcConsumerConfig clientConfig = rpcClient.config();
        HelloService c1 = clientConfig.serviceProxy("g1", HelloService.class);
        HelloService c2 = clientConfig.serviceProxy("g2", HelloService.class);
        HelloService c3 = clientConfig.serviceProxy("g3", HelloService.class);

        // Test call
        Assert.assertEquals("g1: hello tom!", c1.hello("tom"));
        Assert.assertEquals("g1: tom", c1.echo("tom"));
        Assert.assertEquals(5.14, c1.sum(2, 3.14), 0.00000001d);

        Assert.assertEquals("g2: hello tom!", c2.hello("tom"));
        Assert.assertEquals("g2: tom", c2.echo("tom"));
        Assert.assertEquals(6.14, c2.sum(3, 3.14), 0.00000001d);

        Assert.assertEquals("g3: hello tom!", c3.hello("tom"));
        Assert.assertEquals("g3: tom", c3.echo("tom"));
        Assert.assertEquals(103.14, c3.sum(100, 3.14), 0.00000001d);

        Assert.assertEquals(5.14, g1.result(), 0.00000001d);
        Assert.assertEquals(6.14, g2.result(), 0.00000001d);
        Assert.assertEquals(103.14, g3.result(), 0.00000001d);
    }

    @Test
    public void testStartServerWithRandomPort() {
        // Init server
        RpcServer rpcServerRandom = new RpcServer(config("server-random"));
        RpcProviderConfig serverConfig = rpcServerRandom.config();
        serverConfig.addService(HelloService.class, new HelloServiceImpl());
        startServer(rpcServerRandom);

        Assert.assertNotEquals(0, rpcServerRandom.port());
        Assert.assertNotEquals(8090, rpcServerRandom.port());

        // Init client
        HugeConfig config = config(false);
        String url = rpcServerRandom.host() + ":" + rpcServerRandom.port();
        String remoteUrlKey = org.apache.hugegraph.config.RpcOptions.RPC_REMOTE_URL.name();
        config.setProperty(remoteUrlKey, url);
        RpcClientProvider rpcClientRandom = new RpcClientProvider(config);

        RpcConsumerConfig clientConfig = rpcClientRandom.config();
        HelloService client = clientConfig.serviceProxy(HelloService.class);

        // Test call
        Assert.assertEquals("hello tom!", client.hello("tom"));
        Assert.assertEquals("tom", client.echo("tom"));
        Assert.assertEquals(5.14, client.sum(2, 3.14), 0.00000001d);

        // Destroy all
        rpcClientRandom.destroy();
        stopServer(rpcServerRandom);
    }

    @Test
    public void testStartServerWithAdaptivePort() throws IOException {
        // Init server
        RpcServer rpcServerAdaptive = new RpcServer(config("server-adaptive"));
        RpcProviderConfig serverConfig = rpcServerAdaptive.config();
        serverConfig.addService(HelloService.class, new HelloServiceImpl());

        // Start another server bound the port
        int usedPort = rpcServerAdaptive.port();
        InetAddress ip = InetAddress.getByName(rpcServerAdaptive.host());
        ServerSocket inUse = new ServerSocket(usedPort,50, ip);

        // Start server after the port in use
        startServer(rpcServerAdaptive);

        Assert.assertNotEquals(0, rpcServerAdaptive.port());
        Assert.assertNotEquals(usedPort, rpcServerAdaptive.port());

        // Init client
        HugeConfig config = config(false);
        String url = rpcServerAdaptive.host() + ":" + rpcServerAdaptive.port();
        String remoteUrlKey = org.apache.hugegraph.config.RpcOptions.RPC_REMOTE_URL.name();
        config.setProperty(remoteUrlKey, url);
        RpcClientProvider rpcClientAdaptive = new RpcClientProvider(config);

        RpcConsumerConfig clientConfig = rpcClientAdaptive.config();
        HelloService client = clientConfig.serviceProxy(HelloService.class);

        // Test call
        Assert.assertEquals("hello tom!", client.hello("tom"));
        Assert.assertEquals("tom", client.echo("tom"));
        Assert.assertEquals(5.14, client.sum(2, 3.14), 0.00000001d);

        // Destroy all
        rpcClientAdaptive.destroy();
        stopServer(rpcServerAdaptive);

        inUse.close();
    }

    @Test
    public void testStartBothServerAndClientThroughSameConfig() {
        // Init server1
        HugeConfig server1 = config("server1-client");
        RpcServer rpcServer1 = new RpcServer(server1);
        RpcClientProvider rpcClient1 = new RpcClientProvider(server1);

        GraphHelloServiceImpl s1g1 = new GraphHelloServiceImpl("g1");
        GraphHelloServiceImpl s1g2 = new GraphHelloServiceImpl("g2");

        rpcServer1.config().addService(s1g1.graph(), HelloService.class, s1g1);
        rpcServer1.config().addService(s1g2.graph(), HelloService.class, s1g2);

        startServer(rpcServer1);

        // Init server2
        HugeConfig server2 = config("server2-client");
        RpcServer rpcServer2 = new RpcServer(server2);
        RpcClientProvider rpcClient2 = new RpcClientProvider(server2);

        GraphHelloServiceImpl s2g1 = new GraphHelloServiceImpl("g1");
        GraphHelloServiceImpl s2g2 = new GraphHelloServiceImpl("g2");

        rpcServer2.config().addService(s2g1.graph(), HelloService.class, s2g1);
        rpcServer2.config().addService(s2g2.graph(), HelloService.class, s2g2);

        startServer(rpcServer2);

        // Init client1
        HelloService s2g1Client = rpcClient1.config().serviceProxy(
                                  "g1", HelloService.class);
        HelloService s2g2Client = rpcClient1.config().serviceProxy(
                                  "g2", HelloService.class);

        // Init client2
        HelloService s1g1Client = rpcClient2.config().serviceProxy(
                                  "g1", HelloService.class);
        HelloService s1g2Client = rpcClient2.config().serviceProxy(
                                  "g2", HelloService.class);

        // Test call
        Assert.assertEquals(2.1, s2g1Client.sum(1, 1.1), 0.00000001d);
        Assert.assertEquals(2.2, s2g2Client.sum(1, 1.2), 0.00000001d);

        Assert.assertEquals(1.1, s1g1Client.sum(1, 0.1), 0.00000001d);
        Assert.assertEquals(1.2, s1g2Client.sum(0, 1.2), 0.00000001d);

        Assert.assertEquals(1.1, s1g1.result(), 0.00000001d);
        Assert.assertEquals(1.2, s1g2.result(), 0.00000001d);
        Assert.assertEquals(2.1, s2g1.result(), 0.00000001d);
        Assert.assertEquals(2.2, s2g2.result(), 0.00000001d);

        // Destroy all
        rpcClient1.destroy();
        rpcClient2.destroy();
        stopServer(rpcServer1);
        stopServer(rpcServer2);
    }

    @Test
    public void testFanoutCallService() {
        // Init 3 servers
        HugeConfig server3 = config("server3");
        RpcServer rpcServer3 = new RpcServer(server3);

        HugeConfig server4 = config("server4");
        RpcServer rpcServer4 = new RpcServer(server4);

        HugeConfig server5 = config("server5");
        RpcServer rpcServer5 = new RpcServer(server5);

        GraphHelloServiceImpl s3g1 = new GraphHelloServiceImpl("g1");
        GraphHelloServiceImpl s4g1 = new GraphHelloServiceImpl("g1");
        GraphHelloServiceImpl s5g1 = new GraphHelloServiceImpl("g1");

        rpcServer3.config().addService(s3g1.graph(), HelloService.class, s3g1);
        rpcServer4.config().addService(s4g1.graph(), HelloService.class, s4g1);
        rpcServer5.config().addService(s5g1.graph(), HelloService.class, s5g1);

        startServer(rpcServer3);
        startServer(rpcServer4);
        startServer(rpcServer5);

        // Init client
        HugeConfig client345 = config("client345");
        RpcClientProvider rpcClient345 = new RpcClientProvider(client345);

        HelloService g1 = rpcClient345.config()
                                      .serviceProxy("g1", HelloService.class);

        // Test fanout
        Assert.assertEquals("g1: fanout", g1.echo("fanout"));
        Assert.assertEquals(16.8, g1.sum(10, 6.8), 0.00000001d);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            g1.hello("");
        }, e -> {
            Assert.assertContains("empty hello parameter", e.getMessage());
        });

        Assert.assertEquals(16.8, s3g1.result(), 0.00000001d);
        Assert.assertEquals(16.8, s4g1.result(), 0.00000001d);
        Assert.assertEquals(16.8, s5g1.result(), 0.00000001d);

        // Destroy all
        rpcClient345.destroy();

        stopServer(rpcServer3);
        stopServer(rpcServer4);
        stopServer(rpcServer5);
    }

    @Test
    public void testFanoutCallServiceWithError() {
        // Init 3 servers
        HugeConfig server3 = config("server3");
        RpcServer rpcServer3 = new RpcServer(server3);

        HugeConfig server4 = config("server4");
        RpcServer rpcServer4 = new RpcServer(server4);

        HugeConfig server5 = config("server5");
        RpcServer rpcServer5 = new RpcServer(server5);

        GraphHelloServiceImpl s3g1 = new GraphHelloServiceImpl("g1");
        GraphHelloServiceImpl s4g1 = new GraphHelloServiceImpl("g1");
        GraphHelloServiceImpl s5g1 = new GraphHelloServiceImpl("g1");

        rpcServer3.config().addService(s3g1.graph(), HelloService.class, s3g1);
        rpcServer4.config().addService(s4g1.graph(), HelloService.class, s4g1);
        rpcServer5.config().addService(s5g1.graph(), HelloService.class, s5g1);

        startServer(rpcServer3);
        startServer(rpcServer4);
        startServer(rpcServer5);

        // Init client with one server unavailable
        HugeConfig client346 = config("client346");
        RpcClientProvider rpcClient346 = new RpcClientProvider(client346);

        HelloService g1 = rpcClient346.config()
                                      .serviceProxy("g1", HelloService.class);

        // Test fanout with one failed
        Assert.assertEquals("g1: fanout", g1.echo("fanout"));
        Assert.assertEquals(16.8, g1.sum(10, 6.8), 0.00000001d);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            g1.hello("");
        }, e -> {
            Assert.assertContains("empty hello parameter", e.getMessage());
        });

        Assert.assertEquals(16.8, s3g1.result(), 0.00000001d);
        Assert.assertEquals(16.8, s4g1.result(), 0.00000001d);
        Assert.assertEquals(0.0, s5g1.result(), 0.00000001d);

        s3g1.resetResult();
        s4g1.resetResult();
        s5g1.resetResult();

        // Init client with all servers unavailable
        HugeConfig client67 = config("client67");
        RpcClientProvider rpcClient67 = new RpcClientProvider(client67);

        HelloService g67 = rpcClient67.config()
                                      .serviceProxy("g1", HelloService.class);

        // Test fanout with all failed
        Assert.assertThrows(SofaRpcException.class, () -> {
            g67.echo("fanout");
        }, e -> {
            Assert.assertContains("Failed to call", e.getMessage());
            Assert.assertContains("echo() on remote server", e.getMessage());
        });

        Assert.assertEquals(0.0, s3g1.result(), 0.00000001d);
        Assert.assertEquals(0.0, s4g1.result(), 0.00000001d);
        Assert.assertEquals(0.0, s5g1.result(), 0.00000001d);

        // Init client with none service provider
        RpcClientProvider rpcClient0 = new RpcClientProvider(client67);
        Whitebox.setInternalState(rpcClient0, "consumerConfig.remoteUrls",
                                  "");
        HelloService g0 = rpcClient0.config()
                                    .serviceProxy("g1", HelloService.class);

        Assert.assertThrows(SofaRpcException.class, () -> {
            g0.echo("fanout");
        }, e -> {
            Assert.assertContains("No service provider for", e.getMessage());
        });

        // Destroy all
        rpcClient346.destroy();
        rpcClient67.destroy();

        stopServer(rpcServer3);
        stopServer(rpcServer4);
        stopServer(rpcServer5);
    }

    @Test
    public void testLoadBalancer() {
        List<Runnable> cleanup = new ArrayList<>();
        Throwable failure = null;
        try {
            // Init 3 servers
            HugeConfig server3 = config("server3");
            RpcServer rpcServer3 = new RpcServer(server3);
            cleanup.add(() -> stopServer(rpcServer3));

            HugeConfig server4 = config("server4");
            RpcServer rpcServer4 = new RpcServer(server4);
            cleanup.add(() -> stopServer(rpcServer4));

            HugeConfig server5 = config("server5");
            RpcServer rpcServer5 = new RpcServer(server5);
            cleanup.add(() -> stopServer(rpcServer5));

            GraphHelloServiceImpl s3g1 = new GraphHelloServiceImpl("g1");
            GraphHelloServiceImpl s4g1 = new GraphHelloServiceImpl("g1");
            GraphHelloServiceImpl s5g1 = new GraphHelloServiceImpl("g1");

            rpcServer3.config().addService(HelloService.class, s3g1);
            rpcServer4.config().addService(HelloService.class, s4g1);
            rpcServer5.config().addService(HelloService.class, s5g1);

            startServer(rpcServer3);
            startServer(rpcServer4);
            startServer(rpcServer5);

            // Test LB "consistentHash"
            HugeConfig clientLB = config("client-lb");
            RpcClientProvider rpcClientCHash = new RpcClientProvider(clientLB);
            cleanup.add(rpcClientCHash::destroy);
            HelloService cHash = rpcClientCHash.config()
                                               .serviceProxy(HelloService.class);

            Assert.assertEquals("g1: load", cHash.echo("load"));
            Assert.assertEquals(16.8, cHash.sum(10, 6.8), 0.00000001d);
            Assert.assertEquals(16.8, s3g1.result() + s4g1.result() + s5g1.result(),
                                0.00000001d);

            Assert.assertEquals("g1: load", cHash.echo("load"));
            Assert.assertEquals(16.8, cHash.sum(10, 6.8), 0.00000001d);
            Assert.assertEquals(16.8, s3g1.result() + s4g1.result() + s5g1.result(),
                                0.00000001d);

            Assert.assertEquals("g1: load", cHash.echo("load"));
            Assert.assertEquals(16.8, cHash.sum(10, 6.8), 0.00000001d);
            Assert.assertEquals(16.8, s3g1.result() + s4g1.result() + s5g1.result(),
                                0.00000001d);

            s3g1.resetResult();
            s4g1.resetResult();
            s5g1.resetResult();

            // Test LB "roundRobin"
            String lbKey = org.apache.hugegraph.config.RpcOptions.RPC_CLIENT_LOAD_BALANCER.name();
            clientLB.setProperty(lbKey, "roundRobin");
            RpcClientProvider rpcClientRound = new RpcClientProvider(clientLB);
            cleanup.add(rpcClientRound::destroy);
            HelloService round = rpcClientRound.config()
                                               .serviceProxy(HelloService.class);

            Assert.assertEquals("g1: load", round.echo("load"));
            Assert.assertEquals(1.1, round.sum(1, 0.1), 0.00000001d);
            Assert.assertEquals(1.1, s3g1.result() + s4g1.result() + s5g1.result(),
                                0.00000001d);

            Assert.assertEquals("g1: load", round.echo("load"));
            Assert.assertEquals(1.1, round.sum(1, 0.1), 0.00000001d);
            Assert.assertEquals(2.2, s3g1.result() + s4g1.result() + s5g1.result(),
                                0.00000001d);

            Assert.assertEquals("g1: load", round.echo("load"));
            Assert.assertEquals(1.1, round.sum(1, 0.1), 0.00000001d);
            Assert.assertEquals(3.3, s3g1.result() + s4g1.result() + s5g1.result(),
                                0.00000001d);

            s3g1.resetResult();
            s4g1.resetResult();
            s5g1.resetResult();

            // Test LB "random"
            clientLB.setProperty(lbKey, "random");
            RpcClientProvider rpcClientRandom = new RpcClientProvider(clientLB);
            cleanup.add(rpcClientRandom::destroy);
            HelloService random = rpcClientRandom.config()
                                                 .serviceProxy(HelloService.class);

            // Random routing can legitimately leave some providers unvisited.
            GraphHelloServiceImpl[] services = {s3g1, s4g1, s5g1};
            int visited = 0;
            for (int i = 0; i < 12; i++) {
                if (i < 3) {
                    Assert.assertEquals("g1: load", random.echo("load"));
                }
                Assert.assertEquals(1.1, random.sum(1, 0.1), 0.00000001d);
                int currentVisited = 0;
                for (GraphHelloServiceImpl service : services) {
                    double result = service.result();
                    Assert.assertTrue(result == 0.0 || result == 1.1);
                    if (result != 0.0) {
                        currentVisited++;
                    }
                }
                Assert.assertTrue(currentVisited > 0);
                Assert.assertTrue(currentVisited >= visited && currentVisited <= visited + 1);
                visited = currentVisited;
            }

            s3g1.resetResult();
            s4g1.resetResult();
            s5g1.resetResult();
        } catch (RuntimeException | Error e) {
            failure = e;
            throw e;
        } finally {
            RuntimeException cleanupFailure = null;
            for (int i = cleanup.size() - 1; i >= 0; i--) {
                try {
                    cleanup.get(i).run();
                } catch (RuntimeException e) {
                    if (failure != null) {
                        failure.addSuppressed(e);
                    } else if (cleanupFailure == null) {
                        cleanupFailure = e;
                    } else {
                        cleanupFailure.addSuppressed(e);
                    }
                }
            }
            if (cleanupFailure != null) {
                throw cleanupFailure;
            }
        }
    }

    @Test
    public void testServiceProxy() {
        // Init server
        RpcProviderConfig serverConfig = rpcServer.config();
        serverConfig.addService(HelloService.class, new HelloServiceImpl());
        serverConfig.addService("graph", HelloService.class,
                                new GraphHelloServiceImpl("graph"));
        startServer(rpcServer);

        // Init client
        RpcConsumerConfig clientConfig = rpcClient.config();
        HelloService client = clientConfig.serviceProxy(HelloService.class);
        HelloService client2 = clientConfig.serviceProxy(HelloService.class);
        HelloService clientG = clientConfig.serviceProxy("graph",
                                                         HelloService.class);

        // Test call
        Assert.assertNotEquals(client, client2);
        Assert.assertEquals("hello tom!", client.hello("tom"));
        Assert.assertEquals("hello tom!", client2.hello("tom"));
        Assert.assertEquals("graph: hello tom!", clientG.hello("tom"));

        // Test call after unrefer
        rpcClient.unreferAll();

        Assert.assertThrows(SofaRpcRuntimeException.class, () -> {
            client.hello("tom");
        });
        Assert.assertThrows(SofaRpcRuntimeException.class, () -> {
            client2.hello("tom");
        });
        Assert.assertThrows(SofaRpcRuntimeException.class, () -> {
            clientG.hello("tom");
        });
    }

    @Test
    public void testAddServiceMultiTimesOfSameService() {
        RpcServer rpcServerExport = new RpcServer(config("server-random"));

        rpcServerExport.config().addService(HelloService.class,
                                            new HelloServiceImpl());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            rpcServerExport.config().addService(HelloService.class,
                                                new HelloServiceImpl());
        }, e -> {
            Assert.assertContains("Not allowed to add service already exist",
                                  e.getMessage());
        });

        rpcServerExport.exportAll();

        stopServer(rpcServerExport);
    }

    @Test
    public void testExportMultiTimesOfSameServer() {
        RpcServer rpcServerExport = new RpcServer(config("server-random"));
        try {
            rpcServerExport.config().addService(HelloService.class, new HelloServiceImpl());
            rpcServerExport.exportAll();
            rpcServerExport.exportAll();
            Assert.assertEquals(1, rpcServerExport.config().configs().get(
                    HelloService.class.getName()).getServer().size());
        } finally {
            stopServer(rpcServerExport);
        }
    }

    @Test
    public void testExportMultiTimesOfSameService() {
        RpcServer rpcServerExport = new RpcServer(config("server-random"));
        try {
            rpcServerExport.config().addService(HelloService.class, new HelloServiceImpl());
            rpcServerExport.exportAll();
            rpcServerExport.config().addService("graph", HelloService.class, new HelloServiceImpl());
            rpcServerExport.exportAll();
            rpcServerExport.exportAll();
            for (ProviderConfig<?> provider : rpcServerExport.config().configs().values()) {
                Assert.assertEquals(1, provider.getServer().size());
            }
        } finally {
            stopServer(rpcServerExport);
        }
    }

    @Test
    public void testExportRetriesWithoutDuplicatingConfiguredServers() {
        RpcServer server = new RpcServer(config("server-random"));
        AtomicInteger exports = new AtomicInteger();
        IllegalStateException failure = new IllegalStateException("export failed before binding");
        ProviderConfig<HelloService> provider = new ProviderConfig<>() {
            @Override
            public void export() {
                if (exports.incrementAndGet() == 1) {
                    throw failure;
                }
            }
        };
        // Sofa equality ignores settings such as serialization; preserve distinct config objects.
        ServerConfig existing = new ServerConfig().setProtocol("bolt").setHost("127.0.0.1")
                                                  .setPort(0).setSerialization("java");
        provider.setServer(existing);
        server.config().configs().put(HelloService.class.getName(), provider);
        try {
            Assert.assertThrows(IllegalStateException.class, server::exportAll,
                                caught -> Assert.assertSame(failure, caught));
            server.exportAll();
            server.exportAll();
            Assert.assertEquals(3, exports.get());
            Assert.assertEquals(2, provider.getServer().size());
            Assert.assertSame(existing, provider.getServer().get(0));
            Assert.assertSame(Whitebox.getInternalState(server, "serverConfig"), provider.getServer().get(1));
        } finally {
            stopServer(server);
        }
    }

    @Test
    public void testExportNoneService() {
        RpcServer rpcServerNoneService = new RpcServer(config("server-random"));

        // Will be ignored if none service added
        rpcServerNoneService.exportAll();

        stopServer(rpcServerNoneService);
    }

    private static RpcClientProvider clientFor(RpcServer server) {
        HugeConfig config = config(false);
        String remoteUrlKey = org.apache.hugegraph.config.RpcOptions.RPC_REMOTE_URL.name();
        config.setProperty(remoteUrlKey, server.host() + ":" + server.port());
        return new RpcClientProvider(config);
    }

    @Test
    public void testTemporaryServerDoesNotShareMainListener() {
        rpcServer.config().addService(HelloService.class, new HelloServiceImpl());
        startServer(rpcServer);
        RpcServer temporary = new RpcServer(config("server-random"));
        try {
            temporary.config().addService(HelloService.class, new HelloServiceImpl());
            startServer(temporary);
            Assert.assertNotEquals(rpcServer.port(), temporary.port());
        } finally {
            stopServer(temporary);
        }
        HelloService client = rpcClient.config().serviceProxy(HelloService.class);
        Assert.assertEquals("hello tom!", client.hello("tom"));
    }

    @Test
    public void testUnexportService() {
        RpcServer rpcServerUnexport = new RpcServer(config("server-random"));
        RpcProviderConfig serverConfig = rpcServerUnexport.config();
        String service = serverConfig.addService(HelloService.class,
                                                 new HelloServiceImpl());
        String graphService = serverConfig.addService("graph", HelloService.class,
                                                      new GraphHelloServiceImpl("graph"));
        rpcServerUnexport.exportAll();
        RpcClientProvider isolatedClient = clientFor(rpcServerUnexport);
        try {
            HelloService client = isolatedClient.config().serviceProxy(HelloService.class);
            HelloService graphClient = isolatedClient.config().serviceProxy("graph", HelloService.class);
            ChannelFuture binding = listener(rpcServerUnexport);
            Assert.assertEquals("hello tom!", client.hello("tom"));
            rpcServerUnexport.unexport(service);
            Assert.assertThrows(SofaRpcException.class, () -> client.hello("tom"));
            Assert.assertEquals("graph: hello tom!", graphClient.hello("tom"));
            Assert.assertTrue(binding.channel().isOpen());
            rpcServerUnexport.unexport(graphService);
            Assert.assertTrue(binding.channel().closeFuture().awaitUninterruptibly(5, TimeUnit.SECONDS));
            Assert.assertTrue(binding.channel().closeFuture().isSuccess());
            Assert.assertTrue(serverConfig.configs().isEmpty());
            Assert.assertThrows(SofaRpcException.class, () -> graphClient.hello("tom"));
        } finally {
            isolatedClient.destroy();
            stopServer(rpcServerUnexport);
        }
    }

    @Test
    public void testUnexportAllService() {
        RpcServer rpcServerUnexport = new RpcServer(config("server-random"));
        RpcProviderConfig serverConfig = rpcServerUnexport.config();
        serverConfig.addService(HelloService.class, new HelloServiceImpl());
        serverConfig.addService("graph", HelloService.class,
                                new GraphHelloServiceImpl("graph"));
        rpcServerUnexport.exportAll();
        RpcClientProvider isolatedClient = clientFor(rpcServerUnexport);
        try {
            RpcConsumerConfig clientConfig = isolatedClient.config();
            HelloService client = clientConfig.serviceProxy(HelloService.class);
            HelloService clientG = clientConfig.serviceProxy("graph", HelloService.class);
            Assert.assertEquals("hello tom!", client.hello("tom"));
            Assert.assertEquals("graph: hello tom!", clientG.hello("tom"));
            rpcServerUnexport.unexportAll();
            Assert.assertThrows(SofaRpcException.class, () -> client.hello("tom"));
            Assert.assertThrows(SofaRpcException.class, () -> clientG.hello("tom"));
        } finally {
            isolatedClient.destroy();
            stopServer(rpcServerUnexport);
        }
    }

    @Test
    public void testUnexportNotExistService() {
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            rpcServer.unexport("fake");
        }, e -> {
            Assert.assertContains("The service 'fake' doesn't exist",
                                  e.getMessage());
        });
    }

    @Test
    public void testRemoveServiceUsesImplementationIdentity() throws Exception {
        RpcServer server = new RpcServer(config("server-random"));
        GraphHelloServiceImpl original = new GraphHelloServiceImpl("graph");
        RpcProviderConfig providers = server.config();
        String serviceId = providers.addService("graph", HelloService.class, original);
        PausedProvider provider = new PausedProvider();
        provider.setId(serviceId).setUniqueId("graph");
        provider.setInterfaceId(HelloService.class.getName()).setRef(original);
        providers.configs().put(serviceId, provider);
        server.exportAll();
        RpcClientProvider client = clientFor(server);
        ExecutorService worker = Executors.newFixedThreadPool(2);
        try {
            HelloService proxy = client.config().serviceProxy("graph", HelloService.class);
            Assert.assertFalse(providers.removeService(serviceId, new GraphHelloServiceImpl("graph")));
            Assert.assertEquals("graph: hello tom!", proxy.hello("tom"));
            provider.armed = true;
            Future<?> exporting = worker.submit(server::exportAll);
            Assert.assertTrue(provider.entered.await(5, TimeUnit.SECONDS));
            CountDownLatch removing = new CountDownLatch(1);
            Future<Boolean> removal = worker.submit(() -> {
                removing.countDown();
                return providers.removeService(serviceId, original);
            });
            Assert.assertTrue(removing.await(5, TimeUnit.SECONDS));
            Assert.assertThrows(TimeoutException.class, () -> removal.get(1, TimeUnit.SECONDS));
            Assert.assertSame(provider, providers.configs().get(serviceId));
            provider.release.countDown();
            exporting.get(5, TimeUnit.SECONDS);
            Assert.assertTrue(removal.get(5, TimeUnit.SECONDS));
            Assert.assertFalse(providers.configs().containsKey(serviceId));
            GraphHelloServiceImpl replacement = new GraphHelloServiceImpl("replacement");
            providers.addService("graph", HelloService.class, replacement);
            server.exportAll();
            Assert.assertFalse(providers.removeService(serviceId, original));
            Assert.assertSame(replacement, providers.configs().get(serviceId).getRef());
            Assert.assertEquals("replacement: hello tom!", proxy.hello("tom"));
        } finally {
            provider.release.countDown();
            worker.shutdownNow();
            try {
                Assert.assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
            } finally {
                client.destroy();
                stopServer(server);
            }
        }
    }

    private static class PausedProvider extends ProviderConfig<HelloService> {

        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private volatile boolean armed;

        @Override
        public ProviderConfig<HelloService> setTimeout(int timeout) {
            if (this.armed) {
                this.entered.countDown();
                try {
                    E.checkState(this.release.await(5, TimeUnit.SECONDS), "RPC export barrier timed out");
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(failure);
                }
                this.armed = false;
            }
            return super.setTimeout(timeout);
        }
    }

    @Test
    public void testRemoveServiceProxyUsesProxyIdentity() {
        RpcServer server = new RpcServer(config("server-random"));
        server.config().addService("graph", HelloService.class, new GraphHelloServiceImpl("graph"));
        server.config().addService("other", HelloService.class, new GraphHelloServiceImpl("other"));
        server.exportAll();
        RpcClientProvider client = clientFor(server);
        try {
            RpcConsumerConfig consumers = client.config();
            HelloService original = consumers.serviceProxy("graph", HelloService.class);
            HelloService second = consumers.serviceProxy("graph", HelloService.class);
            HelloService other = consumers.serviceProxy("other", HelloService.class);
            Assert.assertNotSame(original, second);
            Assert.assertTrue(consumers.removeServiceProxy("graph", HelloService.class.getName(), original));
            Assert.assertThrows(SofaRpcRuntimeException.class, () -> original.hello("tom"));
            Assert.assertEquals("graph: hello tom!", second.hello("tom"));
            Assert.assertFalse(consumers.removeServiceProxy("graph", HelloService.class.getName(), original));
            Assert.assertTrue(consumers.removeServiceProxy("graph", HelloService.class.getName(), second));
            List<?> bootstraps = Whitebox.getInternalState(consumers, "bootstraps");
            Assert.assertEquals(1, bootstraps.size());
            HelloService replacement = consumers.serviceProxy("graph", HelloService.class);
            Assert.assertFalse(consumers.removeServiceProxy("graph", HelloService.class.getName(), original));
            Assert.assertEquals("graph: hello tom!", replacement.hello("tom"));
            Assert.assertEquals("other: hello tom!", other.hello("tom"));
        } finally {
            client.destroy();
            stopServer(server);
        }
    }

    @Test
    public void testServerDisabled() {
        HugeConfig clientConf = config(false);
        RpcServer rpcServerDisabled = new RpcServer(clientConf);

        Assert.assertFalse(rpcServerDisabled.enabled());
        Assert.assertThrows(IllegalArgumentException.class, rpcServerDisabled::config, e -> {
            Assert.assertContains("RpcServer is not enabled", e.getMessage());
        });

        stopServer(rpcServerDisabled);
    }

    @Test
    public void testClientDisabled() {
        HugeConfig serverConf = config(true);
        RpcClientProvider rpcClientDisabled = new RpcClientProvider(serverConf);

        Assert.assertFalse(rpcClientDisabled.enabled());
        Assert.assertThrows(IllegalArgumentException.class, rpcClientDisabled::config, e -> {
            Assert.assertContains("RpcClient is not enabled", e.getMessage());
        });

        rpcClientDisabled.destroy();
    }

    @Test
    public void testInitRpcConfigs() {
        ImmutableMap<String, Object> fixedOptions = ImmutableMap.of(
                RpcOptions.PROVIDER_REPEATED_EXPORT_LIMIT, 1);
        RpcCommonConfig.initRpcConfigs(fixedOptions);

        RpcCommonConfig.initRpcConfigs(RpcOptions.CONSUMER_RETRIES, 2);
    }

    public interface HelloService {

        String hello(String string);

        String echo(String string);

        double sum(long a, double b);
    }

    public static class HelloServiceImpl implements HelloService {

        @Override
        public String hello(String string) {
            E.checkArgument(!string.isEmpty(), "empty hello parameter");
            return "hello " + string + "!";
        }

        @Override
        public String echo(String string) {
            return string;
        }

        @Override
        public double sum(long a, double b) {
            return a + b;
        }
    }

    public static class GraphHelloServiceImpl implements HelloService {

        private final String graph;
        private double result;

        public GraphHelloServiceImpl(String graph) {
            this.graph = graph;
        }

        public String graph() {
            return this.graph;
        }

        public void resetResult() {
            this.result = 0.0;
        }

        public double result() {
            return this.result;
        }

        @Override
        public String hello(String string) {
            E.checkArgument(!string.isEmpty(), "empty hello parameter");
            return this.graph + ": hello " + string + "!";
        }

        @Override
        public String echo(String string) {
            return this.graph + ": " + string;
        }

        @Override
        public double sum(long a, double b) {
            this.result = a + b;
            return this.result;
        }
    }
}
