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

package org.apache.hugegraph.api.job;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;

import org.apache.hugegraph.api.filter.CompressInterceptor;
import org.apache.hugegraph.api.filter.CompressInterceptor.Compress;
import org.apache.hugegraph.metrics.MetricsUtil;
import org.apache.hugegraph.testutil.Assert;
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.Test;

import com.codahale.metrics.Meter;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

public class TaskResultJerseyGrizzlyIntegrationTest {

    private static final String RESULT = "[1,2,3]";

    @Test
    public void testJerseyCompressionProducesCompleteGzipEntity()
            throws Exception {
        Scenario scenario = Scenario.complete();

        try (ServerHarness server = new ServerHarness(scenario, 5)) {
            HttpResponse response = server.request();

            Assert.assertEquals(200, response.status);
            Assert.assertEquals(CompressInterceptor.GZIP,
                                response.contentEncoding);
            Assert.assertEquals(RESULT, decompress(response.body));
            Assert.assertEquals(0, scenario.permitsDuringWrite.get());
            Assert.assertEquals(1, scenario.limiter.availablePermits());
        }
    }

    @Test
    public void testJerseyHeadReturnsHeadersWithoutEntity() throws Exception {
        Scenario scenario = Scenario.complete();

        try (ServerHarness server = new ServerHarness(scenario, 5)) {
            HttpResponse response = server.head();

            Assert.assertEquals(200, response.status);
            Assert.assertEquals(CompressInterceptor.GZIP,
                                response.contentEncoding);
            Assert.assertEquals(0, response.body.length);
            Assert.assertEquals(0, scenario.limiter.availablePermits());
            Assert.assertEquals(1L, scenario.writerStarted.getCount());
        }
    }

    @Test
    public void testSlowClientHoldsPermitUntilDisconnect() throws Exception {
        Scenario scenario = Scenario.slowStream();

        try (ServerHarness server = new ServerHarness(scenario, 30);
             Socket client = server.slowRequest()) {
            Assert.assertTrue(scenario.writerStarted.await(
                    2L, TimeUnit.SECONDS));
            Assert.assertEquals(0, scenario.limiter.availablePermits());
            Assert.assertFalse(scenario.writerFinished.await(
                    200L, TimeUnit.MILLISECONDS));
        }

        Assert.assertTrue(scenario.writerFinished.await(
                3L, TimeUnit.SECONDS));
        Assert.assertTrue(awaitPermit(scenario.limiter, 3L));
    }

    @Test
    public void testGrizzlyTransactionTimeoutInterruptsJerseyStream()
            throws Exception {
        Scenario scenario = Scenario.waitForInterrupt();
        long timeout = meter("timeout").getCount();

        try (ServerHarness server = new ServerHarness(scenario, 1)) {
            try {
                server.request();
            } catch (IOException ignored) {
                // The transaction timeout may abort the HTTP connection.
            }

            Assert.assertTrue(scenario.writerStarted.await(
                    2L, TimeUnit.SECONDS));
            Assert.assertTrue(awaitPermit(scenario.limiter, 3L));
        }

        Assert.assertTrue(scenario.interrupted.get());
        Assert.assertEquals(timeout + 1L, meter("timeout").getCount());
    }

    @Test
    public void testPostCommitTimeoutTerminatesConnection() throws Exception {
        Scenario scenario = Scenario.waitForPostCommitInterrupt();
        long postCommit = meter("postcommit-failed").getCount();

        try (ServerHarness server = new ServerHarness(scenario, 1)) {
            try {
                server.request();
            } catch (IOException ignored) {
                // A terminated post-commit response is intentionally partial.
            }

            Assert.assertTrue(scenario.writerStarted.await(
                    2L, TimeUnit.SECONDS));
            Assert.assertTrue(awaitPermit(scenario.limiter, 3L));
            Connection<?> connection = scenario.connection.get();
            Assert.assertNotNull(connection);
            Assert.assertFalse(connection.isOpen());
        }

        Assert.assertTrue(scenario.interrupted.get());
        Assert.assertEquals(postCommit + 1L,
                            meter("postcommit-failed").getCount());
    }

    private static Meter meter(String name) {
        return MetricsUtil.registerMeter(
                TaskAPI.class, "task-result-stream-" + name);
    }

    private static boolean awaitPermit(Semaphore limiter, long seconds)
            throws InterruptedException {
        long deadline = System.nanoTime() +
                        TimeUnit.SECONDS.toNanos(seconds);
        while (System.nanoTime() < deadline) {
            if (limiter.availablePermits() == 1) {
                return true;
            }
            Thread.sleep(10L);
        }
        return limiter.availablePermits() == 1;
    }

    private static String decompress(byte[] bytes) throws IOException {
        try (GZIPInputStream input = new GZIPInputStream(
                new ByteArrayInputStream(bytes))) {
            return new String(read(input), StandardCharsets.UTF_8);
        }
    }

    private static byte[] read(InputStream input) throws IOException {
        try (InputStream source = input;
             ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int length;
            while ((length = source.read(buffer)) != -1) {
                output.write(buffer, 0, length);
            }
            return output.toByteArray();
        }
    }

    private static String readHeaders(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        int matched = 0;
        while (matched < 4) {
            int value = input.read();
            if (value == -1) {
                throw new IOException("Connection closed before HTTP headers");
            }
            output.write(value);
            if (output.size() > 16 * 1024) {
                throw new IOException("HTTP response headers are too large");
            }
            int expected = matched == 0 || matched == 2 ? '\r' : '\n';
            matched = value == expected ? matched + 1 :
                      value == '\r' ? 1 : 0;
        }
        return output.toString(StandardCharsets.US_ASCII);
    }

    @Path("stream")
    public static final class StreamingResource {

        private final Scenario scenario;

        @Inject
        public StreamingResource(Scenario scenario) {
            this.scenario = scenario;
        }

        @GET
        @Compress
        @Produces(MediaType.APPLICATION_JSON)
        public Response stream(@Context Request request) {
            Connection<?> connection = request.getContext().getConnection();
            this.scenario.connection.set(connection);
            TaskResultStreamMetrics.RequestTrace trace =
                    TaskResultStreamMetrics.request(
                            "DEFAULT", "hugegraph", 123L,
                            TaskResultStreamMetrics.Mode.COMPLETE);
            trace.backend("memory");
            TaskResultStreamingOutput output =
                    new TaskResultStreamingOutput(
                            connection,
                            request.getResponse()::isCommitted,
                            10, this.scenario.limiter, trace,
                            RESULT.length(), this.scenario::write);
            return Response.ok(output, MediaType.APPLICATION_JSON_TYPE)
                           .build();
        }

        @HEAD
        @Compress
        @Produces(MediaType.APPLICATION_JSON)
        public Response head() {
            return Response.ok()
                           .type(MediaType.APPLICATION_JSON_TYPE)
                           .header("Cache-Control", "no-store")
                           .header("Content-Encoding",
                                   CompressInterceptor.GZIP)
                           .build();
        }
    }

    private static final class Scenario {

        private final boolean waitForInterrupt;
        private final boolean writeBeforeInterrupt;
        private final boolean slowStream;
        private final Semaphore limiter;
        private final CountDownLatch writerStarted;
        private final CountDownLatch writerFinished;
        private final AtomicBoolean interrupted;
        private final AtomicInteger permitsDuringWrite;
        private final AtomicReference<Connection<?>> connection;

        private Scenario(boolean waitForInterrupt,
                         boolean writeBeforeInterrupt,
                         boolean slowStream) {
            this.waitForInterrupt = waitForInterrupt;
            this.writeBeforeInterrupt = writeBeforeInterrupt;
            this.slowStream = slowStream;
            this.limiter = new Semaphore(0);
            this.writerStarted = new CountDownLatch(1);
            this.writerFinished = new CountDownLatch(1);
            this.interrupted = new AtomicBoolean(false);
            this.permitsDuringWrite = new AtomicInteger(-1);
            this.connection = new AtomicReference<>();
        }

        private static Scenario complete() {
            return new Scenario(false, false, false);
        }

        private static Scenario waitForInterrupt() {
            return new Scenario(true, false, false);
        }

        private static Scenario waitForPostCommitInterrupt() {
            return new Scenario(true, true, false);
        }

        private static Scenario slowStream() {
            return new Scenario(false, false, true);
        }

        private void write(java.io.OutputStream output, long deadlineNanos)
                           throws IOException {
            this.writerStarted.countDown();
            this.permitsDuringWrite.set(this.limiter.availablePermits());
            try {
                if (this.slowStream) {
                    writeNoise(output, 4096);
                    return;
                }
                if (!this.waitForInterrupt) {
                    output.write(RESULT.getBytes(StandardCharsets.UTF_8));
                    return;
                }
                if (this.writeBeforeInterrupt) {
                    writeNoise(output, 8);
                    output.flush();
                }
                Thread.sleep(TimeUnit.SECONDS.toMillis(10L));
            } catch (InterruptedException e) {
                this.interrupted.set(true);
                Thread.currentThread().interrupt();
            } finally {
                this.writerFinished.countDown();
            }
        }

        private static void writeNoise(java.io.OutputStream output,
                                       int chunks) throws IOException {
            byte[] bytes = new byte[16 * 1024];
            int state = 0x13579BDF;
            for (int chunk = 0; chunk < chunks; chunk++) {
                for (int i = 0; i < bytes.length; i++) {
                    state = state * 1103515245 + 12345;
                    bytes[i] = (byte) (state >>> 16);
                }
                output.write(bytes);
            }
        }
    }

    private static final class ServerHarness implements AutoCloseable {

        private final HttpServer server;
        private final int port;

        private ServerHarness(Scenario scenario, int transactionTimeout)
                throws IOException {
            ResourceConfig config = new ResourceConfig();
            config.register(StreamingResource.class);
            config.register(new AbstractBinder() {
                @Override
                protected void configure() {
                    this.bind(scenario).to(Scenario.class);
                }
            });
            config.register(CompressInterceptor.class);
            this.server = GrizzlyHttpServerFactory.createHttpServer(
                    URI.create("http://127.0.0.1:0/"), config, false);
            NetworkListener listener = this.server.getListeners()
                                                  .iterator().next();
            listener.setTransactionTimeout(transactionTimeout);
            this.server.start();
            InetSocketAddress address = (InetSocketAddress)
                    listener.getServerConnection().getLocalAddress();
            this.port = address.getPort();
        }

        private HttpResponse request() throws IOException {
            return this.request("GET");
        }

        private HttpResponse head() throws IOException {
            return this.request("HEAD");
        }

        private HttpResponse request(String method) throws IOException {
            HttpURLConnection connection = (HttpURLConnection)
                    URI.create("http://127.0.0.1:" + this.port + "/stream")
                       .toURL().openConnection();
            connection.setRequestMethod(method);
            connection.setRequestProperty("Accept-Encoding",
                                          CompressInterceptor.GZIP);
            connection.setConnectTimeout(2000);
            connection.setReadTimeout(5000);
            int status = connection.getResponseCode();
            InputStream input = status >= 400 ? connection.getErrorStream() :
                                connection.getInputStream();
            byte[] body = input == null ? new byte[0] : read(input);
            String contentEncoding = connection.getHeaderField(
                    "Content-Encoding");
            connection.disconnect();
            return new HttpResponse(status, contentEncoding, body);
        }

        private Socket slowRequest() throws IOException {
            Socket socket = new Socket();
            try {
                socket.setReceiveBufferSize(1024);
                socket.setSoTimeout(5000);
                socket.connect(new InetSocketAddress("127.0.0.1", this.port),
                               2000);
                String request = "GET /stream HTTP/1.1\r\n" +
                                 "Host: 127.0.0.1:" + this.port + "\r\n" +
                                 "Accept-Encoding: gzip\r\n" +
                                 "Connection: close\r\n\r\n";
                socket.getOutputStream().write(
                        request.getBytes(StandardCharsets.US_ASCII));
                socket.getOutputStream().flush();
                String headers = readHeaders(socket.getInputStream());
                if (!headers.contains("Content-Encoding: gzip")) {
                    throw new IOException("Missing gzip response header");
                }
                return socket;
            } catch (IOException e) {
                socket.close();
                throw e;
            }
        }

        @Override
        public void close() {
            this.server.shutdownNow();
        }
    }

    private static final class HttpResponse {

        private final int status;
        private final String contentEncoding;
        private final byte[] body;

        private HttpResponse(int status, String contentEncoding,
                             byte[] body) {
            this.status = status;
            this.contentEncoding = contentEncoding;
            this.body = body;
        }
    }
}
