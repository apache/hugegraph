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

package org.apache.hugegraph.dist;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.security.HugeSecurityManager;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ColdGremlinQueryTest {

    @Test
    public void testEarlyInitializationOnConfigurationFailure() throws Exception {
        Path sourceRoot = Path.of(System.getProperty("basedir", System.getProperty("user.dir")))
                              .getParent().getParent();
        Path target = sourceRoot.resolve("hugegraph-server/hugegraph-dist/target");
        Path fixture = Files.createTempDirectory(Files.createDirectories(target), "cold-constructor-");
        Path output = fixture.resolve("constructor.log");
        Process process = null;
        try {
            process = startJvm(sourceRoot, output, "early", fixture.resolve("missing.yaml").toString());
            Assert.assertTrue("Constructor JVM timed out", process.waitFor(60, TimeUnit.SECONDS));
            Assert.assertEquals(Files.readString(output), 0, process.exitValue());
            Assert.assertTrue(Files.readString(output), Files.readString(output).contains("early-initialization-ok"));
        } finally {
            stop(process);
            FileUtils.deleteDirectory(fixture.toFile());
        }
    }

    @Test
    public void testFirstGremlinQueryThroughServerStartup() throws Exception {
        Path sourceRoot = Path.of(System.getProperty("basedir", System.getProperty("user.dir")))
                              .getParent().getParent();
        Path target = sourceRoot.resolve("hugegraph-server/hugegraph-dist/target");
        Path fixture = Files.createTempDirectory(Files.createDirectories(target), "cold-gremlin-");
        Path output = fixture.resolve("server.log");
        Process server = null;
        try {
            int restPort;
            int gremlinPort;
            try (ServerSocket rest = new ServerSocket(0, 0, InetAddress.getLoopbackAddress());
                 ServerSocket gremlin = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
                restPort = rest.getLocalPort();
                gremlinPort = gremlin.getLocalPort();
            }
            Path graphs = Files.createDirectory(fixture.resolve("graphs"));
            Path graphConfig = graphs.resolve("hugegraph.properties");
            Files.writeString(graphConfig, "gremlin.graph=org.apache.hugegraph.HugeFactory\n" +
                                           "backend=rocksdb\nserializer=binary\nstore=hugegraph\n" +
                                           "rocksdb.data_path=" + fixture.resolve("data") + "\n" +
                                           "rocksdb.wal_path=" + fixture.resolve("data") + "\n");
            Files.writeString(fixture.resolve("java.security"), "networkaddress.cache.ttl=60\n");
            // Seed in another JVM so neither schema creation nor a REST request
            // can initialize ConditionQuery in the server JVM under test.
            Path seedOutput = fixture.resolve("seed.log");
            Process seed = startJvm(sourceRoot, seedOutput, "seed", graphConfig.toString());
            try {
                Assert.assertTrue("Seed JVM timed out", seed.waitFor(60, TimeUnit.SECONDS));
                Assert.assertEquals(Files.readString(seedOutput), 0, seed.exitValue());
            } finally {
                stop(seed);
            }

            Path staticDir = sourceRoot.resolve("hugegraph-server/hugegraph-dist/src/assembly/static");
            Path gremlinConfig = fixture.resolve("gremlin-server.yaml");
            String yaml = Files.readString(staticDir.resolve("conf/gremlin-server.yaml"))
                               .replace("scripts/empty-sample.groovy",
                                        staticDir.resolve("scripts/empty-sample.groovy").toString());
            Files.writeString(gremlinConfig, yaml + "\nhost: 127.0.0.1\nport: " + gremlinPort + "\n");
            Path restConfig = fixture.resolve("rest-server.properties");
            Files.writeString(restConfig, "restserver.url=http://127.0.0.1:" + restPort + "\n" +
                                          "gremlinserver.url=127.0.0.1:" + gremlinPort + "\n" +
                                          "graphs=" + graphs + "\n" +
                                          "memory_monitor.threshold=1.0\n");
            server = startJvm(sourceRoot, output, "serve", gremlinConfig.toString(), restConfig.toString());
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
            String restUrl = "http://127.0.0.1:" + restPort;
            waitForServer(client, restUrl, server, output);

            // /versions is a readiness probe, not a graph query. The first graph
            // request reaches real Gremlin workers after the production constructor.
            ObjectMapper mapper = new ObjectMapper();
            String gremlinUrl = "http://127.0.0.1:" + gremlinPort + "/gremlin";
            Response response = gremlin(client, gremlinUrl, "g.V().hasLabel('cold_start').count()");
            Assert.assertEquals(response.body() + "\n" + Files.readString(output), 200, response.status());
            JsonNode result = mapper.readTree(response.body());
            Assert.assertEquals(response.body(), 200, result.path("status").path("code").asInt());
            Assert.assertEquals(response.body(), 1, result.path("result").path("data").get(0).asInt());

            response = send(client, HttpRequest.newBuilder(URI.create(restUrl +
                    "/graphspaces/DEFAULT/graphs/hugegraph/graph/vertices?label=cold_start"))
                    .timeout(Duration.ofSeconds(10)).GET().build());
            Assert.assertEquals(response.body(), 200, response.status());
            JsonNode vertices = mapper.readTree(response.body()).path("vertices");
            Assert.assertEquals(response.body(), 1, vertices.size());
            Assert.assertEquals(response.body(), "cold_start", vertices.get(0).path("label").asText());

            assertDenied(client, gremlinUrl, "System.getProperty('java.version')", "java.version");
            assertDenied(client, gremlinUrl, "System.setSecurityManager(null)", "denied permission");
        } finally {
            stop(server);
            FileUtils.deleteDirectory(fixture.toFile());
        }
    }

    public static void main(String[] args) throws Exception {
        if ("early".equals(args[0])) {
            assertEarlyInitialization(args[1]);
        } else if ("seed".equals(args[0])) {
            HugeGraphServer.register();
            HugeGraph graph = HugeFactory.open(args[1]);
            try {
                graph.initBackend();
                graph.schema().vertexLabel("cold_start").useCustomizeStringId().create();
                graph.addVertex(T.id, "cold_start", T.label, "cold_start");
                graph.tx().commit();
            } finally {
                graph.close();
                HugeFactory.shutdown(30L);
            }
        } else {
            // Exercise the production constructor, including its
            // temporary suspension and restoration of HugeSecurityManager.
            HugeGraphServer.register();
            System.setSecurityManager(new HugeSecurityManager());
            HugeGraphServer server = new HugeGraphServer(args[1], args[2]);
            Assert.assertTrue(System.getSecurityManager() instanceof HugeSecurityManager);
            HugeFactory.removeShutdownHook();
            Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
            // REST starts before Gremlin inside the constructor. Publish readiness
            // only after the constructor returns and restores the sandbox.
            Files.writeString(Path.of(args[2]).getParent().resolve("ready"), "ready");
            new CountDownLatch(1).await();
        }
    }

    private static void assertEarlyInitialization(String missingConfig) throws Exception {
        GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        SecurityManager previous = System.getSecurityManager();
        String threadName = Thread.currentThread().getName();
        try {
            engine.eval("1 + 1");
            Log.logger(ColdGremlinQueryTest.class).warn("Initialize logging for {}", "cold constructor fixture");
            HugeSecurityManager security = new HugeSecurityManager();
            System.setSecurityManager(security);
            HugeException error = Assert.assertThrows(HugeException.class,
                    () -> new HugeGraphServer(missingConfig, missingConfig));
            Assert.assertEquals("Failed to load yaml config file '" + missingConfig + "'", error.getMessage());
            Assert.assertSame(security, System.getSecurityManager());

            // Stop before graph preparation can incidentally initialize query
            // classes. Only the production constructor can warm them here.
            Thread.currentThread().setName("gremlin-server-exec-cold-constructor");
            Assert.assertEquals(Boolean.TRUE,
                                engine.eval("org.apache.hugegraph.dist.ColdGremlinQueryTest.constructQuery()"));
            Assert.assertNotNull(new ConditionQuery(HugeType.VERTEX));
        } finally {
            Thread.currentThread().setName(threadName);
            System.setSecurityManager(previous);
            engine.reset();
        }
        System.out.println("early-initialization-ok");
    }

    public static boolean constructQuery() {
        return new ConditionQuery(HugeType.VERTEX).resultType() == HugeType.VERTEX;
    }

    private static Process startJvm(Path sourceRoot, Path output, String... args) throws IOException {
        Path staticDir = sourceRoot.resolve("hugegraph-server/hugegraph-dist/src/assembly/static");
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-Xms128m");
        command.add("-Xmx512m");
        command.add("-Djava.security.manager=allow");
        command.add("-Djava.security.properties=" + output.getParent().resolve("java.security"));
        command.add("@" + staticDir.resolve("bin/jvm-module.options"));
        command.add("-cp");
        command.add(System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
        command.add(ColdGremlinQueryTest.class.getName());
        command.addAll(List.of(args));
        return new ProcessBuilder(command).directory(sourceRoot.toFile())
                   .redirectErrorStream(true).redirectOutput(output.toFile()).start();
    }

    private static void waitForServer(HttpClient client, String url, Process server, Path output) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (System.nanoTime() < deadline) {
            Assert.assertTrue(Files.readString(output), server.isAlive());
            try {
                HttpResponse<Void> response = client.send(HttpRequest.newBuilder(URI.create(url + "/versions"))
                        .timeout(Duration.ofSeconds(3)).GET().build(), HttpResponse.BodyHandlers.discarding());
                if (response.statusCode() == 200 && Files.exists(output.getParent().resolve("ready"))) {
                    return;
                }
            } catch (IOException ignored) {
                // The listener is not ready yet.
            }
            Thread.sleep(100L);
        }
        Assert.fail("Server startup timed out\n" + Files.readString(output));
    }

    private static Response gremlin(HttpClient client, String url, String script) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String body = mapper.createObjectNode().put("gremlin", script).put("language", "gremlin-groovy")
                            .set("aliases", mapper.createObjectNode().put("g", "__g_DEFAULT-hugegraph")).toString();
        return send(client, HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build());
    }

    private static Response send(HttpClient client, HttpRequest request) throws Exception {
        HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
        byte[] body = response.body();
        if ("gzip".equalsIgnoreCase(response.headers().firstValue("Content-Encoding").orElse(""))) {
            try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(body))) {
                body = gzip.readAllBytes();
            }
        }
        return new Response(response.statusCode(), new String(body, StandardCharsets.UTF_8));
    }

    private static void assertDenied(HttpClient client, String url, String script, String message) throws Exception {
        Response response = gremlin(client, url, script);
        Assert.assertNotEquals(response.body(), 200, response.status());
        Assert.assertTrue(response.body(), response.body().contains(message));
    }

    private static void stop(Process process) throws InterruptedException {
        if (process != null && process.isAlive()) {
            process.destroy();
            if (!process.waitFor(45, TimeUnit.SECONDS)) {
                process.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
            }
        }
    }

    private record Response(int status, String body) {
    }
}
