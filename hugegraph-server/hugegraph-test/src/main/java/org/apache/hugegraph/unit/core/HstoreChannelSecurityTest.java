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

package org.apache.hugegraph.unit.core;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.script.ScriptException;

import org.apache.hugegraph.security.HugeSecurityManager;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class HstoreChannelSecurityTest {

    private static final String CLIENT = "org.apache.hugegraph.store.client.grpc.AbstractGrpcClient";
    private static final AtomicBoolean USER_CALLBACK = new AtomicBoolean();
    private static Thread callbackThread;

    @Test
    public void testColdSessionChannelsCanBeCreatedAndRecreatedFromGremlin() throws Exception {
        assumeHstoreAvailable();
        try (Fixture fixture = new Fixture()) {
            fixture.engine.put("access", fixture.access);
            fixture.enableSandbox();
            Assert.assertEquals(Boolean.TRUE, fixture.engine.eval("access.getStub() != null"));
            Assert.assertEquals(Boolean.TRUE, fixture.engine.eval("access.getStub() != null"));
        }
        // A fresh client and target exercise lazy initialization again.
        try (Fixture fixture = new Fixture()) {
            fixture.engine.put("access", fixture.access);
            fixture.enableSandbox();
            Assert.assertEquals(Boolean.TRUE, fixture.engine.eval("access.getStub() != null"));
        }
    }

    @Test
    public void testColdStreamStubCanBeCreatedAndRecreatedFromGremlin() throws Exception {
        assumeHstoreAvailable();
        // Isolate the common pool: idle workers from another test can hide thread creation.
        Path output = Files.createTempFile("hstore-stream-stub-", ".log");
        Process child = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Djava.util.concurrent.ForkJoinPool.common.parallelism=2",
                "-cp", System.getProperty("java.class.path"),
                ColdStreamStub.class.getName()).redirectErrorStream(true)
                                         .redirectOutput(output.toFile()).start();
        try {
            Assert.assertTrue("Stream stub process timed out", child.waitFor(30, TimeUnit.SECONDS));
            Assert.assertEquals(Files.readString(output, StandardCharsets.UTF_8), 0, child.exitValue());
        } finally {
            if (child.isAlive()) {
                child.destroyForcibly();
            }
            Files.deleteIfExists(output);
        }
    }

    public static final class ColdStreamStub {

        public static void main(String[] args) throws Exception {
            try (Fixture fixture = new Fixture(true)) {
                Assert.assertEquals("The common pool must start cold", 0,
                                    ForkJoinPool.commonPool().getPoolSize());
                fixture.engine.put("access", fixture.access);
                fixture.enableSandbox();
                Object first = fixture.engine.eval("access.getStub()");
                Assert.assertNotNull(first);
                Assert.assertNotNull(fixture.engine.eval("access.getStub()"));
                Class.forName(CLIENT).getMethod("closeChannel", String.class)
                     .invoke(null, fixture.target);
                Object rebuilt = fixture.engine.eval("access.getStub()");
                Assert.assertNotNull(rebuilt);
                Assert.assertNotSame(first, rebuilt);
                Assert.assertEquals("Stub construction must not start common-pool workers", 0,
                                    ForkJoinPool.commonPool().getPoolSize());
            }
        }
    }

    @Test
    public void testUserThreadAndFactoryAccessRemainDenied() throws Exception {
        assumeHstoreAvailable();
        try (Fixture fixture = new Fixture()) {
            fixture.enableSandbox();
            denied(fixture.engine, "new Thread()", "Not allowed to access thread group");
            denied(fixture.engine, "new ThreadGroup('user')", "Not allowed to access thread group");
            denied(fixture.engine, "new org.apache.hugegraph.store.client.util.ExecutorPool" +
                   ".DefaultThreadFactory('user').newThread({ -> } as Runnable)",
                   "Not allowed to access thread group");
            denied(fixture.engine, "System.getProperty('user.home')",
                   "Not allowed to access system property(user.home)");
            denied(fixture.engine, "System.setProperty('io.grpc.netty.shaded.io.grpc.netty." +
                   "useCustomAllocator', 'false')", "Not allowed to modify system property");
        }
    }

    @Test
    public void testCustomChannelBuilderRetainsGremlinThreadRestrictions() throws Exception {
        assumeHstoreAvailable();
        USER_CALLBACK.set(false);
        try (Fixture fixture = new Fixture()) {
            Object userClient = fixture.engine.eval("new " + CLIENT + "() {" +
                    " protected io.grpc.ManagedChannel createChannel(String target) {" +
                    " org.apache.hugegraph.unit.core.HstoreChannelSecurityTest.userCallback();" +
                    " return null; }\n" +
                    " public io.grpc.stub.AbstractBlockingStub getBlockingStub(" +
                    "io.grpc.ManagedChannel channel) { return null; } }");
            fixture.engine.put("userClient", userClient);
            fixture.enableSandbox();
            denied(fixture.engine, "userClient.getBlockingStub('127.0.0.1:18499')",
                   "Not allowed to access thread group");
            Assert.assertTrue("The custom builder ran on the caller", USER_CALLBACK.get());
            Assert.assertSame(Thread.currentThread(), callbackThread);
        }
    }

    public static void userCallback() {
        USER_CALLBACK.set(true);
        callbackThread = Thread.currentThread();
        new Thread();
    }

    public static final class Access {

        private final Object client;
        private final Object session;
        private final Method getStub;

        private Access(Object client, Object session, Method getStub) {
            this.client = client;
            this.session = session;
            this.getStub = getStub;
        }

        public Object getStub() throws Exception {
            return this.getStub.invoke(this.client, this.session);
        }
    }

    private static final class Fixture implements AutoCloseable {

        private final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        private final SecurityManager previous = System.getSecurityManager();
        private final String threadName = Thread.currentThread().getName();
        private final String target = "127.0.0.1:" + (20000 + Math.floorMod(UUID.randomUUID().hashCode(), 30000));
        private final Access access;

        private Fixture() throws Exception {
            this(false);
        }

        private Fixture(boolean stream) throws Exception {
            // PD/framework bootstrap initializes the shared gRPC and generated stub classes
            // before Server accepts Gremlin. No Store pool or RPC is created by this setup.
            io.grpc.ManagedChannel bootstrap = io.grpc.ManagedChannelBuilder
                    .forTarget("127.0.0.1:18498").usePlaintext().build();
            io.grpc.stub.AbstractStub<?> bootstrapStub = (io.grpc.stub.AbstractStub<?>)
                    Class.forName(stream ? "org.apache.hugegraph.store.grpc.stream.HgStoreStreamGrpc" :
                                  "org.apache.hugegraph.store.grpc.session.HgStoreSessionGrpc")
                         .getMethod(stream ? "newStub" : "newBlockingStub", io.grpc.Channel.class).invoke(null, bootstrap);
            bootstrapStub.withDeadlineAfter(1, java.util.concurrent.TimeUnit.SECONDS)
                         .withMaxInboundMessageSize(1024).withMaxOutboundMessageSize(1024);
            bootstrap.shutdownNow();
            // In reactor tests these SDK classes are in sibling target directories;
            // load their class metadata before the sandbox, as Server bootstrap does.
            Class.forName("org.apache.hugegraph.store.client.util.ExecutorPool$DefaultThreadFactory");
            Class.forName("org.apache.hugegraph.store.term.HgPair");
            Class<?> sessionType = Class.forName("org.apache.hugegraph.store.client.HgStoreNodeSession");
            Class<?> nodeType = Class.forName("org.apache.hugegraph.store.client.HgStoreNode");
            Object node = Proxy.newProxyInstance(nodeType.getClassLoader(), new Class<?>[]{nodeType},
                    (p, method, args) -> method.getName().equals("getAddress") ? target : null);
            Object session = Proxy.newProxyInstance(sessionType.getClassLoader(), new Class<?>[]{sessionType},
                    (p, method, args) -> method.getName().equals("getStoreNode") ? node : null);
            Class<?> clientType = Class.forName(stream ? "org.apache.hugegraph.store.client.grpc.GrpcStoreStreamClient" :
                                  "org.apache.hugegraph.store.client.grpc.GrpcStoreSessionClient");
            Constructor<?> constructor = clientType.getDeclaredConstructor();
            constructor.setAccessible(true);
            Object client = constructor.newInstance();
            Method getStub = clientType.getDeclaredMethod(stream ? "getStub" : "getBlockingStub", sessionType);
            getStub.setAccessible(true);
            this.access = new Access(client, session, getStub);
            org.apache.hugegraph.util.Log.logger(HugeSecurityManager.class);
            this.engine.eval("1 + 1");
        }

        private void enableSandbox() {
            System.setSecurityManager(new HugeSecurityManager());
            Thread.currentThread().setName("gremlin-server-exec-hstore-test");
        }

        @Override
        public void close() throws Exception {
            Thread.currentThread().setName(this.threadName);
            System.setSecurityManager(this.previous);
            Class<?> client = Class.forName(CLIENT);
            client.getMethod("closeChannel", String.class).invoke(null, this.target);
            client.getMethod("closeChannel", String.class).invoke(null, "127.0.0.1:18499");
        }

    }

    private static void assumeHstoreAvailable() {
        try {
            Class.forName(CLIENT);
        } catch (ClassNotFoundException e) {
            Assume.assumeNoException("HStore is absent from the RocksDB-only distribution", e);
        }
    }

    private static void denied(GremlinGroovyScriptEngine engine, String script, String message)
            throws Exception {
        try {
            engine.eval(script);
            Assert.fail("Expected denied Gremlin access: " + script);
        } catch (ScriptException e) {
            Assert.assertTrue(e.toString(), e.toString().contains(message));
        }
    }
}
