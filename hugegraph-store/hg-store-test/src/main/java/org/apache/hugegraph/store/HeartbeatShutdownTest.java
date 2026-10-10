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

package org.apache.hugegraph.store;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb.ErrorType;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.meta.PartitionManager;
import org.apache.hugegraph.store.meta.Store;
import org.apache.hugegraph.store.meta.StoreMetadata;
import org.apache.hugegraph.store.metric.HgMetricService;
import org.apache.hugegraph.store.options.HgStoreEngineOptions;
import org.apache.hugegraph.store.pd.PdProvider;
import org.apache.hugegraph.store.util.PartitionMetaStoreWrapper;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.alipay.sofa.jraft.rpc.RpcServer;

public class HeartbeatShutdownTest {

    @Test
    public void testInvalidRecoveredShardExitsAfterNodeDestroy() throws Exception {
        assertFatalExit("recovery", 0);
    }

    @Test
    public void testFatalRegistrationExitsAfterShutdownHooks() throws Exception {
        for (int code : new int[]{ErrorType.STORE_ID_NOT_EXIST_VALUE,
                                 ErrorType.STORE_HAS_BEEN_REMOVED_VALUE,
                                 ErrorType.STORE_PROHIBIT_DUPLICATE_VALUE}) {
            assertFatalExit("register", code);
        }
    }

    @Test
    public void testFatalHeartbeatExitsAfterShutdownHooks() throws Exception {
        for (int code : new int[]{ErrorType.STORE_ID_NOT_EXIST_VALUE, ErrorType.STORE_HAS_BEEN_REMOVED_VALUE}) {
            assertFatalExit("heartbeat", code);
        }
    }

    private static void assertFatalExit(String phase, int code) throws Exception {
        Path directory = Files.createTempDirectory(Path.of("target"), "heartbeat-fatal-");
        Path marker = directory.resolve("hook-complete");
        Path output = directory.resolve("child.log");
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        if ("recovery".equals(phase)) {
            // Node's packaged artifact stores classes under Spring Boot's BOOT-INF.
            Path nodeClasses = Path.of("../hg-store-node/target/classes").toAbsolutePath();
            Assert.assertTrue("Missing Node reactor classes: " + nodeClasses,
                              Files.isRegularFile(nodeClasses.resolve(
                                      "org/apache/hugegraph/store/node/grpc/HgStoreNodeService.class")));
            classpath += File.pathSeparator + nodeClasses;
        }
        Process process = new ProcessBuilder(new File(System.getProperty("java.home"), "bin/java").toString(),
                                            "-cp", classpath, HeartbeatShutdownTest.class.getName(),
                                            phase, Integer.toString(code), marker.toString())
                          .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            boolean exited = process.waitFor(20, TimeUnit.SECONDS);
            if (!exited) {
                new ProcessBuilder(new File(System.getProperty("java.home"), "bin/jstack").toString(),
                                   Long.toString(process.pid()))
                        .redirectErrorStream(true).redirectOutput(directory.resolve("thread-dump.log").toFile())
                        .start().waitFor(5, TimeUnit.SECONDS);
            }
            Assert.assertTrue("Fatal heartbeat did not exit; child log: " + output + "\n" + Files.readString(output),
                              exited);
            Assert.assertEquals(Files.readString(output), "recovery".equals(phase) ? 0 : 255, process.exitValue());
            Assert.assertEquals("joined", Files.readString(marker));
            if ("recovery".equals(phase)) {
                String log = Files.readString(output);
                Assert.assertTrue(log, log.contains("not in valid shard group"));
                Assert.assertTrue(log, log.contains("NODE_DESTROY_COMPLETE"));
            }
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                Assert.assertTrue(process.waitFor(5, TimeUnit.SECONDS));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String phase = args[0];
        int code = Integer.parseInt(args[1]);
        Path marker = Path.of(args[2]);
        if ("recovery".equals(phase)) {
            runInvalidShardRecovery(marker);
            return;
        }
        CountDownLatch producersStarted = new CountDownLatch(1);
        PdProvider provider = Mockito.mock(PdProvider.class);
        Mockito.when(provider.getClusterStats()).thenReturn(Metapb.ClusterStats.newBuilder()
                                                                  .setState(Metapb.ClusterState.Cluster_OK).build());
        Mockito.when(provider.registerStore(Mockito.any())).thenAnswer(invocation -> {
            producersStarted.await();
            if ("register".equals(phase)) {
                System.out.println("FATAL_REGISTER " + code);
                throw new PDException(code, "fatal registration fixture");
            }
            return 1L;
        });
        Mockito.when(provider.storeHeartbeat(Mockito.any())).thenAnswer(invocation -> {
            System.out.println("FATAL_HEARTBEAT " + code);
            throw new PDException(code, "fatal heartbeat fixture");
        });
        HeartbeatService service = new HeartbeatService(null);
        service.setStoreMetadata(Mockito.mock(StoreMetadata.class));
        HgStoreEngineOptions options = Mockito.mock(HgStoreEngineOptions.class);
        Mockito.when(options.getPdProvider()).thenReturn(provider);
        Mockito.when(options.getGrpcAddress()).thenReturn("127.0.0.1:8500");
        Mockito.when(options.getRaftAddress()).thenReturn("127.0.0.1:8510");
        Mockito.when(options.getStoreHBInterval()).thenReturn(1);
        Mockito.when(options.getPartitionHBInterval()).thenReturn(60);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("SHUTDOWN_HOOK_JOIN");
            service.shutdown();
            try {
                Files.writeString(marker, "joined");
                System.out.println("SHUTDOWN_HOOK_COMPLETE");
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }, "fatal-fixture-shutdown"));
        service.init(options);
        producersStarted.countDown();
    }

    private static void runInvalidShardRecovery(Path marker) throws Exception {
        Path data = marker.getParent().resolve("data");
        Path raft = marker.getParent().resolve("raft");
        Files.createDirectories(data.resolve(HgStoreEngineOptions.DB_Path_Prefix).resolve("00001"));
        Files.createDirectories(raft.resolve(HgStoreEngineOptions.Raft_Path_Prefix).resolve("00001/log"));
        HgStoreEngineOptions options = new HgStoreEngineOptions();
        options.setDataPath(data.toString());
        options.setRaftPath(raft.toString());
        PdProvider provider = Mockito.mock(PdProvider.class);
        Metapb.Partition partition = Metapb.Partition.newBuilder().setId(1).setGraphName("graph").build();
        Mockito.when(provider.getShardGroup(1)).thenReturn(Metapb.ShardGroup.newBuilder()
                .setId(1).addShards(Metapb.Shard.newBuilder().setStoreId(2)
                                                       .setRole(Metapb.ShardRole.Leader)).build());
        Mockito.when(provider.getPartitionByID("graph", 1)).thenReturn(new Partition(partition));
        Store remote = new Store();
        remote.setRaftAddress("127.0.0.1:8511");
        Mockito.when(provider.getStoreByID(2L)).thenReturn(remote);
        PartitionManager manager = new PartitionManager(provider, options);
        StoreMetadata metadata = Mockito.mock(StoreMetadata.class);
        Store local = new Store();
        local.setId(1);
        Mockito.when(metadata.getStore()).thenReturn(local);
        Mockito.when(metadata.getPartitionStore(1)).thenReturn(Metapb.PartitionStore.newBuilder()
                    .setPartitionId(1).setStoreLocation(data.toString()).build());
        Mockito.when(metadata.getPartitionRaft(1)).thenReturn(Metapb.PartitionRaft.newBuilder()
                    .setPartitionId(1).setRaftLocation(raft.toString()).build());
        setField(PartitionManager.class, manager, "storeMetadata", metadata);
        PartitionMetaStoreWrapper wrapper = Mockito.mock(PartitionMetaStoreWrapper.class);
        Mockito.when(wrapper.scan(Mockito.eq(1), Mockito.eq(Metapb.Partition.parser()), Mockito.any()))
               .thenReturn(List.of(partition));
        setField(PartitionManager.class, manager, "wrapper", wrapper);
        Constructor<HgStoreEngine> constructor = HgStoreEngine.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        HgStoreEngine engine = constructor.newInstance();
        Fixture service = new Fixture(false);
        RpcServer rpc = Mockito.mock(RpcServer.class);
        HgMetricService metrics = Mockito.mock(HgMetricService.class);
        setField(HgStoreEngine.class, engine, "partitionManager", manager);
        setField(HgStoreEngine.class, engine, "heartbeatService", service);
        setField(HgStoreEngine.class, engine, "rpcServer", rpc);
        setField(HgStoreEngine.class, engine, "metricService", metrics);
        Class<?> nodeClass = Class.forName("org.apache.hugegraph.store.node.grpc.HgStoreNodeService");
        Class<?> configClass = Class.forName("org.apache.hugegraph.store.node.AppConfig");
        Object node = nodeClass.getConstructor(configClass).newInstance((Object) null);
        setField(nodeClass, node, "storeEngine", engine);
        service.addStateListener(engine);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("NODE_DESTROY_ENTERED");
            try {
                nodeClass.getMethod("destroy").invoke(node);
                Mockito.verify(metrics).shutdown();
                Mockito.verify(rpc).shutdown();
                Files.writeString(marker, "joined");
                System.out.println("NODE_DESTROY_COMPLETE");
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }, "recovery-fixture-shutdown"));
        service.start();
        Assert.assertTrue(service.entered.await(5, TimeUnit.SECONDS));
        service.onStateChanged(Metapb.StoreState.Up);
    }

    private static void setField(Class<?> owner, Object object, String name, Object value) throws Exception {
        Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        field.set(object, value);
    }

    @Test
    public void testShutdownJoinsBothInFlightProducersBeforeReturning() throws Exception {
        Fixture service = new Fixture(true);
        ExecutorService closer = Executors.newSingleThreadExecutor();
        try {
            service.start();
            Assert.assertTrue(service.entered.await(5, TimeUnit.SECONDS));
            Future<?> shutdown = closer.submit(service::shutdown);
            assertStillClosing(shutdown);
            service.storeRelease.countDown();
            assertStillClosing(shutdown);
            service.partitionRelease.countDown();
            shutdown.get(5, TimeUnit.SECONDS);
            Assert.assertFalse(service.storeThread.isAlive());
            Assert.assertFalse(service.partitionThread.isAlive());
        } finally {
            service.storeRelease.countDown();
            service.partitionRelease.countDown();
            service.shutdown();
            closer.shutdownNow();
        }
    }

    @Test
    public void testShutdownWakesBothIdleProducers() throws Exception {
        Fixture service = new Fixture(false);
        ExecutorService closer = Executors.newSingleThreadExecutor();
        try {
            service.start();
            Assert.assertTrue(service.entered.await(5, TimeUnit.SECONDS));
            awaitSleeping(service.storeThread);
            awaitSleeping(service.partitionThread);
            closer.submit(service::shutdown).get(5, TimeUnit.SECONDS);
            Assert.assertFalse(service.storeThread.isAlive());
            Assert.assertFalse(service.partitionThread.isAlive());
        } finally {
            service.shutdown();
            closer.shutdownNow();
        }
    }

    @Test
    public void testShutdownWaitsForDerivedStateCallbackAndRejectsLateUpdates() throws Exception {
        Fixture service = new Fixture(false);
        ExecutorService closer = Executors.newSingleThreadExecutor();
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch callbackRelease = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        service.addStateListener((store, previous, current) -> {
            calls.incrementAndGet();
            callbackEntered.countDown();
            try {
                callbackRelease.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        });
        try {
            service.start();
            Assert.assertTrue(service.entered.await(5, TimeUnit.SECONDS));
            service.onStateChanged(Metapb.StoreState.Up);
            Assert.assertTrue(callbackEntered.await(5, TimeUnit.SECONDS));
            Future<?> shutdown = closer.submit(service::shutdown);
            assertStillClosing(shutdown);
            callbackRelease.countDown();
            shutdown.get(5, TimeUnit.SECONDS);
            service.onStateChanged(Metapb.StoreState.Offline);
            Assert.assertEquals(1, calls.get());
            Assert.assertEquals(Metapb.StoreState.Up, service.getStoreInfo().getState());
        } finally {
            callbackRelease.countDown();
            service.shutdown();
            closer.shutdownNow();
        }
    }

    @Test
    public void testInterruptedShutdownDoesNotAllowDatabaseClose() throws Exception {
        Fixture service = new Fixture(true);
        CountDownLatch closing = new CountDownLatch(1);
        AtomicBoolean databaseClosed = new AtomicBoolean();
        AtomicBoolean interruptedFailure = new AtomicBoolean();
        Thread closer = new Thread(() -> {
            closing.countDown();
            try {
                service.shutdown();
                // HgStoreEngine only closes native owners after this call succeeds.
                databaseClosed.set(true);
            } catch (IllegalStateException expected) {
                interruptedFailure.set(Thread.currentThread().isInterrupted());
            }
        });
        try {
            service.start();
            Assert.assertTrue(service.entered.await(5, TimeUnit.SECONDS));
            closer.start();
            Assert.assertTrue(closing.await(5, TimeUnit.SECONDS));
            closer.interrupt();
            closer.join(5000);
            Assert.assertFalse(closer.isAlive());
            Assert.assertTrue(interruptedFailure.get());
            Assert.assertFalse(databaseClosed.get());
        } finally {
            service.storeRelease.countDown();
            service.partitionRelease.countDown();
            service.shutdown();
            closer.join(5000);
        }
    }

    private static void assertStillClosing(Future<?> shutdown) throws Exception {
        try {
            shutdown.get(100, TimeUnit.MILLISECONDS);
            Assert.fail("Shutdown returned while heartbeat still owns work");
        } catch (TimeoutException expected) {
            // A successful return would let HgStoreEngine close the DB too soon.
        }
    }

    private static void awaitSleeping(Thread thread) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        Assert.assertEquals(Thread.State.TIMED_WAITING, thread.getState());
    }

    private static final class Fixture extends HeartbeatService {

        private final CountDownLatch entered = new CountDownLatch(2);
        private final CountDownLatch storeRelease;
        private final CountDownLatch partitionRelease;
        private volatile Thread storeThread;
        private volatile Thread partitionThread;

        private Fixture(boolean blocked) {
            super(null);
            this.storeRelease = new CountDownLatch(blocked ? 1 : 0);
            this.partitionRelease = new CountDownLatch(blocked ? 1 : 0);
        }

        private void start() throws Exception {
            this.setStoreMetadata(Mockito.mock(StoreMetadata.class));
            HgStoreEngineOptions options = Mockito.mock(HgStoreEngineOptions.class);
            Mockito.when(options.getPartitionHBInterval()).thenReturn(60);
            Field delay = HeartbeatService.class.getDeclaredField("timerNextDelay");
            delay.setAccessible(true);
            delay.setInt(this, 60000);
            this.init(options);
        }

        @Override
        protected void registerStore() {
            this.storeThread = Thread.currentThread();
            this.awaitWork(this.storeRelease);
        }

        @Override
        protected void partitionHeartbeat() {
            this.partitionThread = Thread.currentThread();
            this.awaitWork(this.partitionRelease);
        }

        private void awaitWork(CountDownLatch release) {
            this.entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }
}
