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
import com.alipay.sofa.rpc.core.exception.SofaRpcException;
import com.alipay.sofa.rpc.core.exception.SofaRpcRuntimeException;
import com.google.common.collect.ImmutableMap;

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
            rpcServer.destroy();
        }
    }

    @After
    public void teardown() {
        if (rpcClient != null) {
            rpcClient.unreferAll();
        }
        if (rpcServer != null) {
            rpcServer.unexportAll();
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

        rpcServer3.config().addService(HelloService.class, s3g1);
        rpcServer4.config().addService(HelloService.class, s4g1);
        rpcServer5.config().addService(HelloService.class, s5g1);

        startServer(rpcServer3);
        startServer(rpcServer4);
        startServer(rpcServer5);

        // Test LB "consistentHash"
        HugeConfig clientLB = config("client-lb");
        RpcClientProvider rpcClientCHash = new RpcClientProvider(clientLB);
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
        HelloService random = rpcClientRandom.config()
                                             .serviceProxy(HelloService.class);

        Assert.assertEquals("g1: load", random.echo("load"));
        Assert.assertEquals(1.1, random.sum(1, 0.1), 0.00000001d);
        Assert.assertEquals(1.1, s3g1.result() + s4g1.result() + s5g1.result(),
                            0.00000001d);

        Assert.assertEquals("g1: load", random.echo("load"));
        Assert.assertEquals(1.1, random.sum(1, 0.1), 0.00000001d);
        double sum = s3g1.result() + s4g1.result() + s5g1.result();
        Assert.assertTrue(2.2 == sum || 1.1 == sum);

        Assert.assertEquals("g1: load", random.echo("load"));
        Assert.assertEquals(1.1, random.sum(1, 0.1), 0.00000001d);
        double sum2 = s3g1.result() + s4g1.result() + s5g1.result();
        Assert.assertTrue(sum == sum2 || sum + 1.1 == sum2);

        for (int i = 0; i < 9; i++) {
            Assert.assertEquals(1.1, random.sum(1, 0.1), 0.00000001d);
        }
        Assert.assertEquals(3.3, s3g1.result() + s4g1.result() + s5g1.result(),
                            0.00000001d);

        s3g1.resetResult();
        s4g1.resetResult();
        s5g1.resetResult();

        // Destroy all
        rpcClientCHash.destroy();
        rpcClientRound.destroy();
        rpcClientRandom.destroy();

        stopServer(rpcServer3);
        stopServer(rpcServer4);
        stopServer(rpcServer5);
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
        rpcServerExport.config().addService(HelloService.class,
                                            new HelloServiceImpl());
        rpcServerExport.exportAll();
        rpcServerExport.exportAll();

        stopServer(rpcServerExport);
    }

    @Test
    public void testExportMultiTimesOfSameService() {
        RpcServer rpcServerExport = new RpcServer(config("server-random"));
        rpcServerExport.config().addService(HelloService.class,
                                            new HelloServiceImpl());
        rpcServerExport.exportAll();

        rpcServerExport.config().addService("graph", HelloService.class,
                                            new HelloServiceImpl());
        rpcServerExport.exportAll();

        stopServer(rpcServerExport);
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
        rpcServerUnexport.exportAll();
        RpcClientProvider isolatedClient = clientFor(rpcServerUnexport);
        try {
            HelloService client = isolatedClient.config().serviceProxy(HelloService.class);
            Assert.assertEquals("hello tom!", client.hello("tom"));
            rpcServerUnexport.unexport(service);
            Assert.assertThrows(SofaRpcException.class, () -> client.hello("tom"));
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
