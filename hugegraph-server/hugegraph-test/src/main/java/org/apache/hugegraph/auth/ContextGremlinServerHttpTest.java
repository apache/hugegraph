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

package org.apache.hugegraph.auth;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.server.HugeGraphWsAndHttpChannelizer;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer;
import org.apache.tinkerpop.gremlin.server.channel.WsAndHttpChannelizer;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONUntypedMessageSerializerV1;
import org.junit.Test;

import io.netty.channel.Channel;

public class ContextGremlinServerHttpTest {

    @Test
    public void testOnlyStandardChannelizerIsAdapted() {
        try {
            Settings settings = new Settings();
            settings.channelizer = WsAndHttpChannelizer.class.getName();
            Assert.assertSame(settings,
                              ContextGremlinServer.configureChannelizer(settings));
            Assert.assertEquals(HugeGraphWsAndHttpChannelizer.class.getName(),
                                settings.channelizer);
            settings.channelizer = "example.CustomChannelizer";
            ContextGremlinServer.configureChannelizer(settings);
            Assert.assertEquals("example.CustomChannelizer", settings.channelizer);
            settings.channelizer = WebSocketChannelizer.class.getName();
            ContextGremlinServer.configureChannelizer(settings);
            Assert.assertEquals(WebSocketChannelizer.class.getName(),
                                settings.channelizer);
        } finally {
            HugeGraphAuthProxy.resetContext();
        }
    }

    @Test
    public void testUnknownAliasThenValidQueriesOnSameTcpConnection()
            throws Exception {
        Settings settings = new Settings();
        settings.host = "127.0.0.1";
        settings.port = 0;
        settings.gremlinPool = 1;
        settings.threadPoolBoss = 1;
        settings.threadPoolWorker = 1;
        settings.channelizer = WsAndHttpChannelizer.class.getName();
        Settings.SerializerSettings serializer = new Settings.SerializerSettings();
        serializer.className = GraphSONUntypedMessageSerializerV1.class.getName();
        Settings.SerializerSettings webSocketSerializer = new Settings.SerializerSettings();
        webSocketSerializer.className = GraphSONMessageSerializerV3.class.getName();
        settings.serializers = Arrays.asList(serializer, webSocketSerializer);
        ContextGremlinServer server = new ContextGremlinServer(
                settings, new EventHub("http-lifecycle-test", 1));
        try {
            server.getServerGremlinExecutor().getGraphManager()
                  .putGraph("known", EmptyGraph.instance());
            server.start().get(30, TimeUnit.SECONDS);
            Assert.assertTrue(server.getChannelizer() instanceof
                              HugeGraphWsAndHttpChannelizer);
            Channel listening = Whitebox.getInternalState(server,
                                                         "serverSocketChannel");
            int port = ((InetSocketAddress) listening.localAddress()).getPort();
            try (Socket socket = new Socket("127.0.0.1", port)) {
                socket.setSoTimeout(10000);
                InputStream input = new BufferedInputStream(socket.getInputStream());
                send(socket, "missing", "101");
                Map<?, ?> rejected = read(input, 400);
                Assert.assertTrue(rejected.get("message").toString()
                                          .contains("Could not rebind"));
                send(socket, "known", "202");
                Map<?, ?> accepted = read(input, 200);
                Assert.assertEquals(Collections.singletonList(202),
                                    ((Map<?, ?>) accepted.get("result")).get("data"));
                send(socket, "known", "303");
                Map<?, ?> next = read(input, 200);
                Assert.assertEquals(Collections.singletonList(303),
                                    ((Map<?, ?>) next.get("result")).get("data"));
            }
            Cluster cluster = Cluster.build().addContactPoint("127.0.0.1")
                                     .port(port)
                                     .serializer(new GraphSONMessageSerializerV3())
                                     .create();
            try {
                Client client = cluster.connect();
                try {
                    Assert.assertEquals(404, client.submit("404").all()
                                                   .get(10, TimeUnit.SECONDS)
                                                   .get(0).getInt());
                } finally {
                    client.close();
                }
            } finally {
                cluster.close();
            }
        } finally {
            server.stop().get(30, TimeUnit.SECONDS);
            HugeGraphAuthProxy.resetContext();
        }
    }

    private static void send(Socket socket, String alias, String script)
            throws IOException {
        String body = "{\"gremlin\":\"" + script + "\",\"language\":" +
                      "\"gremlin-groovy\",\"aliases\":{\"graph\":\"" + alias + "\"}}";
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        String headers = "POST /gremlin HTTP/1.1\r\n" +
                         "Host: 127.0.0.1\r\n" +
                         "Content-Type: application/json\r\n" +
                         "Accept: application/json\r\n" +
                         "Connection: keep-alive\r\n" +
                         "Content-Length: " + bytes.length + "\r\n\r\n";
        socket.getOutputStream().write(headers.getBytes(StandardCharsets.US_ASCII));
        socket.getOutputStream().write(bytes);
        socket.getOutputStream().flush();
    }

    private static Map<?, ?> read(InputStream input, int expectedStatus)
            throws IOException {
        String status = line(input);
        Assert.assertTrue(status, status.startsWith("HTTP/1.1 " + expectedStatus + " "));
        int length = -1;
        String header;
        while (!(header = line(input)).isEmpty()) {
            if (header.toLowerCase(Locale.ROOT).startsWith("content-length:")) {
                length = Integer.parseInt(header.substring(header.indexOf(':') + 1).trim());
            }
        }
        Assert.assertTrue("Expected a bounded response body", length >= 0 && length < 65536);
        byte[] bytes = input.readNBytes(length);
        Assert.assertEquals(length, bytes.length);
        return JsonUtil.fromJson(new String(bytes, StandardCharsets.UTF_8), Map.class);
    }

    private static String line(InputStream input) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        int value;
        while ((value = input.read()) != '\n') {
            if (value < 0 || bytes.size() >= 8192) {
                throw new IOException("Incomplete or excessive HTTP header");
            }
            if (value != '\r') {
                bytes.write(value);
            }
        }
        return bytes.toString(StandardCharsets.US_ASCII);
    }
}
