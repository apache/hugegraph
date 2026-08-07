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

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.metrics.MetricsUtil;
import org.apache.hugegraph.testutil.Assert;
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.PortRange;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.threadpool.ThreadPoolConfig;
import org.junit.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;

public class TaskResultGrizzlyIntegrationTest {

    private static final String METRIC_PREFIX = "task-result-stream-";
    private static final String WRITE_TIMEOUT_MESSAGE =
            "Write timeout exceeded when trying to flush the data";
    private static final byte[] CHUNK = new byte[8192];

    @Test
    public void testSlowReaderTriggersWriteTimeout() throws Exception {
        Scenario scenario = new Scenario(1);
        long active = counter("active").getCount();
        long timeout = meter("timeout").getCount();
        long disconnected = meter("disconnected").getCount();
        long postCommit = meter("postcommit-failed").getCount();

        try (ServerHarness server = new ServerHarness(scenario);
             Socket client = server.connect()) {
            client.setReceiveBufferSize(1024);
            client.setSoTimeout(5000);
            server.request(client);

            Assert.assertTrue(scenario.finished.await(8L, TimeUnit.SECONDS));
            Assert.assertTrue(connectionTerminated(client));
        }

        Assert.assertTrue(hasMessage(scenario.error.get(),
                                     WRITE_TIMEOUT_MESSAGE));
        Assert.assertTrue(scenario.committed.get());
        Assert.assertEquals(1, scenario.limiter.availablePermits());
        Assert.assertEquals(active, counter("active").getCount());
        Assert.assertEquals(timeout + 1L, meter("timeout").getCount());
        Assert.assertEquals(disconnected,
                            meter("disconnected").getCount());
        Assert.assertEquals(postCommit + 1L,
                            meter("postcommit-failed").getCount());
        Assert.assertEquals(scenario.previousTimeout.get(),
                            scenario.restoredTimeout.get());
        Assert.assertTrue(scenario.elapsedMillis.get() < 5000L);
    }

    @Test
    public void testClientDisconnectReleasesPermit() throws Exception {
        Scenario scenario = new Scenario(10);
        long active = counter("active").getCount();
        long timeout = meter("timeout").getCount();
        long disconnected = meter("disconnected").getCount();
        long postCommit = meter("postcommit-failed").getCount();

        try (ServerHarness server = new ServerHarness(scenario)) {
            try (Socket client = server.connect()) {
                client.setSoTimeout(3000);
                server.request(client);
                Assert.assertTrue(client.getInputStream().read() != -1);
                client.setSoLinger(true, 0);
            }

            Assert.assertTrue(scenario.finished.await(8L, TimeUnit.SECONDS));
        }

        Assert.assertInstanceOf(IOException.class, scenario.error.get());
        Assert.assertFalse(hasMessage(scenario.error.get(),
                                      WRITE_TIMEOUT_MESSAGE));
        Assert.assertTrue(scenario.committed.get());
        Assert.assertEquals(1, scenario.limiter.availablePermits());
        Assert.assertEquals(active, counter("active").getCount());
        Assert.assertEquals(disconnected + 1L,
                            meter("disconnected").getCount());
        Assert.assertEquals(timeout, meter("timeout").getCount());
        Assert.assertEquals(postCommit + 1L,
                            meter("postcommit-failed").getCount());
        Assert.assertEquals(scenario.previousTimeout.get(),
                            scenario.restoredTimeout.get());
    }

    private static Meter meter(String name) {
        return MetricsUtil.registerMeter(
                TaskAPI.class, METRIC_PREFIX + name);
    }

    private static Counter counter(String name) {
        return MetricsUtil.registerCounter(
                TaskAPI.class, METRIC_PREFIX + name);
    }

    private static boolean hasMessage(Throwable error, String expected) {
        for (Throwable current = error; current != null;
             current = current.getCause()) {
            if (current.getMessage() != null &&
                current.getMessage().contains(expected)) {
                return true;
            }
        }
        return false;
    }

    private static boolean connectionTerminated(Socket socket) {
        byte[] buffer = new byte[8192];
        try {
            while (socket.getInputStream().read(buffer) != -1) {
                // Drain data already queued before the write timeout.
            }
            return true;
        } catch (SocketTimeoutException e) {
            return false;
        } catch (IOException e) {
            return true;
        }
    }

    private static final class Scenario extends HttpHandler {

        private final int timeoutSeconds;
        private final Semaphore limiter;
        private final CountDownLatch finished;
        private final AtomicReference<Throwable> error;
        private final AtomicBoolean committed;
        private final AtomicLong previousTimeout;
        private final AtomicLong restoredTimeout;
        private final AtomicLong elapsedMillis;

        private Scenario(int timeoutSeconds) {
            this.timeoutSeconds = timeoutSeconds;
            this.limiter = new Semaphore(0);
            this.finished = new CountDownLatch(1);
            this.error = new AtomicReference<>();
            this.committed = new AtomicBoolean(false);
            this.previousTimeout = new AtomicLong(Long.MIN_VALUE);
            this.restoredTimeout = new AtomicLong(Long.MIN_VALUE);
            this.elapsedMillis = new AtomicLong(Long.MAX_VALUE);
        }

        @Override
        public void service(Request request, Response response)
                            throws Exception {
            response.setBufferSize(1024);
            response.setContentType("application/octet-stream");
            Connection<?> connection =
                    request.getContext().getConnection();
            this.previousTimeout.set(connection.getWriteTimeout(
                    TimeUnit.MILLISECONDS));
            TaskResultStreamMetrics.RequestTrace trace =
                    TaskResultStreamMetrics.request(
                            "DEFAULT", "hugegraph", 123L,
                            TaskResultStreamMetrics.Mode.COMPLETE);
            trace.backend("memory");
            TaskResultStreamingOutput streaming =
                    new TaskResultStreamingOutput(
                            connection, response::isCommitted,
                            this.timeoutSeconds, this.limiter, trace,
                            CHUNK.length, Scenario::writeUntilFailure);
            long start = System.nanoTime();
            try {
                streaming.write(response.getOutputStream());
            } catch (IOException | RuntimeException e) {
                this.error.set(e);
                throw e;
            } catch (Error e) {
                this.error.set(e);
                throw e;
            } finally {
                this.elapsedMillis.set(TimeUnit.NANOSECONDS.toMillis(
                        System.nanoTime() - start));
                this.committed.set(response.isCommitted());
                this.restoredTimeout.set(connection.getWriteTimeout(
                        TimeUnit.MILLISECONDS));
                this.finished.countDown();
            }
        }

        private static void writeUntilFailure(OutputStream output,
                                              long deadlineNanos)
                                              throws IOException {
            for (int i = 0; i < 1_000_000; i++) {
                output.write(CHUNK);
                output.flush();
            }
        }
    }

    private static final class ServerHarness implements AutoCloseable {

        private final HttpServer server;
        private final int port;

        private ServerHarness(HttpHandler handler) throws IOException {
            this.server = new HttpServer();
            NetworkListener listener = new NetworkListener(
                    "task-result-stream-test", "127.0.0.1",
                    new PortRange(20000, 50000));
            listener.setMaxPendingBytes(1024);
            listener.getTransport().setWriteBufferSize(1024);
            listener.getTransport().setSelectorRunnersCount(1);
            listener.getTransport().setWorkerThreadPoolConfig(
                    ThreadPoolConfig.defaultConfig()
                                    .setPoolName("task-result-stream-test")
                                    .setCorePoolSize(2)
                                    .setMaxPoolSize(2));
            this.server.addListener(listener);
            this.server.getServerConfiguration()
                       .addHttpHandler(handler, "/stream");
            this.server.start();
            InetSocketAddress address = (InetSocketAddress)
                    listener.getServerConnection().getLocalAddress();
            this.port = address.getPort();
        }

        private Socket connect() throws IOException {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress("127.0.0.1", this.port));
            return socket;
        }

        private void request(Socket socket) throws IOException {
            String request = "GET /stream HTTP/1.1\r\n" +
                             "Host: 127.0.0.1:" + this.port + "\r\n" +
                             "Connection: keep-alive\r\n\r\n";
            socket.getOutputStream().write(
                    request.getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();
        }

        @Override
        public void close() {
            this.server.shutdownNow();
        }
    }
}
