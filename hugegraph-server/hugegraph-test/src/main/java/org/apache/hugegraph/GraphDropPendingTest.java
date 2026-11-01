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

package org.apache.hugegraph;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.job.SysJob;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.hugegraph.util.Events;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;
import org.mockito.Mockito;

public class GraphDropPendingTest {

    @Test
    public void testLocalDropKeepsDataAndRegistrationUntilCancelledWorkerExits() throws Exception {
        try (Fixture fixture = new Fixture("local_pending", false)) {
            BlockingJob job = fixture.start();
            fixture.scheduler.cancel(job.task());
            Assert.assertTrue(job.task().isCancelled());
            HugeException refusal = Assert.assertThrows(HugeException.class, fixture::drop);
            Assert.assertContains("Tasks are still active", refusal.getMessage());
            Assert.assertContains("task admission remains closed", refusal.getMessage());
            Assert.assertContains("restart to resume task admission", refusal.getMessage());
            BlockingJob late = new BlockingJob();
            late.release.countDown();
            Assert.assertThrows(IllegalStateException.class, () -> fixture.scheduler.schedule(
                    new HugeTask<>(IdGenerator.of(9997001L), null, late)));
            fixture.assertRetained();
            job.release.countDown();
            fixture.scheduler.waitUntilAllTasksCompleted(10L);
            fixture.drop();
            Assert.assertTrue(fixture.graph.closed());
            Assert.assertNull(fixture.manager.graph(fixture.graph.spaceGraphName()));
            Assert.assertFalse(Files.exists(fixture.file));
            Assert.assertEquals(1, fixture.dropEvents);
        }
    }

    @Test
    public void testPdDropPreservesMetadataUntilLocalTasksDrain() throws Exception {
        try (Fixture fixture = new Fixture("pd_pending", true)) {
            BlockingJob job = fixture.start();
            HugeException refusal = Assert.assertThrows(HugeException.class, fixture::drop);
            Assert.assertContains("Tasks are still active", refusal.getMessage());
            Assert.assertContains("task admission remains closed", refusal.getMessage());
            Assert.assertContains("restart to resume task admission", refusal.getMessage());
            BlockingJob late = new BlockingJob();
            late.release.countDown();
            Assert.assertThrows(IllegalStateException.class, () -> fixture.scheduler.schedule(
                    new HugeTask<>(IdGenerator.of(9997001L), null, late)));
            fixture.assertRetained();
            Assert.assertTrue(fixture.metadataPresent.get());
            job.release.countDown();
            fixture.scheduler.waitUntilAllTasksCompleted(10L);
            fixture.drop();
            Assert.assertFalse(fixture.metadataPresent.get());
            Assert.assertTrue(fixture.graph.closed());
            Assert.assertNull(fixture.manager.graph(fixture.graph.spaceGraphName()));
        }
    }

    @Test
    public void testConfiguredWaitAllowsShortTaskToFinishBeforePdDrop() throws Exception {
        try (Fixture fixture = new Fixture("pd_wait", true)) {
            fixture.graph.configuration().setProperty(CoreOptions.TASK_WAIT_TIMEOUT.name(), 2L);
            BlockingJob job = fixture.start();
            CountDownLatch fenced = new CountDownLatch(1);
            TaskScheduler scheduler = Mockito.mock(TaskScheduler.class,
                    org.mockito.AdditionalAnswers.delegatesTo(fixture.scheduler));
            Mockito.doAnswer(invocation -> {
                boolean drained = fixture.scheduler.close();
                if (!drained) {
                    fenced.countDown();
                }
                return drained;
            }).when(scheduler).close();
            HugeGraph proxy = Mockito.mock(HugeGraph.class, org.mockito.AdditionalAnswers.delegatesTo(fixture.graph));
            Mockito.doReturn(scheduler).when(proxy).taskScheduler();
            fixture.graphs.put(fixture.graph.spaceGraphName(), proxy);
            ExecutorService caller = Executors.newSingleThreadExecutor();
            try {
                Future<?> deletion = caller.submit(fixture::drop);
                Assert.assertTrue(fenced.await(10L, TimeUnit.SECONDS));
                Assert.assertTrue(fixture.metadataPresent.get());
                job.release.countDown();
                deletion.get(10L, TimeUnit.SECONDS);
                Assert.assertTrue(fixture.graph.closed());
                Assert.assertFalse(fixture.metadataPresent.get());
            } finally {
                job.release.countDown();
                caller.shutdown();
                Assert.assertTrue(caller.awaitTermination(10L, TimeUnit.SECONDS));
                fixture.graphs.replace(fixture.graph.spaceGraphName(), proxy, fixture.graph);
            }
        }
    }

    @Test
    public void testSchedulerCloseFailureDoesNotRemovePdMetadata() throws Exception {
        try (Fixture fixture = new Fixture("pd_close_failure", true)) {
            Graph proxy = Mockito.mock(HugeGraph.class, Mockito.withSettings().defaultAnswer(
                    org.mockito.AdditionalAnswers.delegatesTo(fixture.graph)));
            TaskScheduler failed = Mockito.spy(fixture.scheduler);
            Mockito.doThrow(new IllegalStateException("scheduler close failed")).when(failed).close();
            Mockito.doReturn(failed).when((HugeGraph) proxy).taskScheduler();
            fixture.graphs.put(fixture.graph.spaceGraphName(), proxy);
            try {
                RuntimeException error = Assert.assertThrows(RuntimeException.class, fixture::drop);
                Assert.assertContains("scheduler close failed", error.getMessage());
                Assert.assertTrue(fixture.metadataPresent.get());
                Assert.assertFalse(fixture.graph.closed());
                Assert.assertTrue(Files.exists(fixture.file));
            } finally {
                fixture.graphs.put(fixture.graph.spaceGraphName(), fixture.graph);
            }
        }
    }

    @Test
    public void testMissingGraphReportsArgumentErrorFromInitializedManager() throws Exception {
        try (Fixture fixture = new Fixture("missing_graph", true)) {
            IllegalArgumentException error = Assert.assertThrows(IllegalArgumentException.class,
                    () -> fixture.manager.dropGraph("DEFAULT", "missing", true));
            Assert.assertContains("doesn't exist", error.getMessage());
            fixture.assertRetained();
            Assert.assertTrue(fixture.metadataPresent.get());
        }
    }

    @Test
    public void testGraphSpaceSharedMetadataSurvivesRefusedDrop() throws Exception {
        try (Fixture fixture = new Fixture("space_pending", true)) {
            BlockingJob job = fixture.start();
            HugeException refusal = Assert.assertThrows(HugeException.class,
                    () -> fixture.manager.clearGraphSpace("DEFAULT"));
            Assert.assertContains("Tasks are still active", refusal.getMessage());
            Assert.assertFalse(fixture.sharedMetadataCleared.get());
            Assert.assertSame(fixture.graph, fixture.manager.graph(fixture.graph.spaceGraphName()));
            job.release.countDown();
        }
    }

    private static final class Fixture implements AutoCloseable {

        private final Path directory;
        private final Path file;
        private final GraphManager manager;
        private final Map<String, Graph> graphs;
        private final HugeGraph graph;
        private final HugeGraph neighbor;
        private final HugeGraphParams params;
        private final TaskScheduler scheduler;
        private final boolean pd;
        private final AtomicBoolean metadataPresent = new AtomicBoolean(true);
        private final AtomicBoolean sharedMetadataCleared = new AtomicBoolean();
        private final List<BlockingJob> jobs = new ArrayList<>();
        private int dropEvents;

        private Fixture(String name, boolean pd) throws Exception {
            RegisterUtil.registerBackends();
            this.pd = pd;
            this.directory = Files.createTempDirectory("graph-drop-core-");
            HugeConfig config = FakeObjects.newConfig();
            config.setProperty(ServerOptions.USE_PD.name(), false);
            config.setProperty(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name(), false);
            config.setProperty(ServerOptions.ENABLE_DYNAMIC_CREATE_DROP.name(), true);
            config.setProperty(ServerOptions.GRAPHS.name(), this.directory.toString());
            EventHub events = new EventHub("drop-core-" + name);
            this.manager = new GraphManager(config, events);
            this.graphs = Whitebox.getInternalState(this.manager, "graphs");
            this.graph = this.open(name);
            this.neighbor = this.open(name + "_neighbor");
            this.params = Whitebox.getInternalState(this.graph, "params");
            this.scheduler = this.graph.taskScheduler();
            this.file = this.directory.resolve(name + ".properties");
            events.listen(Events.GRAPH_DROP, event -> {
                if (event.args()[0] == this.graph) {
                    this.dropEvents++;
                }
                return null;
            });
            if (pd) {
                // Replace the external PD service, never bypass manager initialization.
                Whitebox.setInternalState(this.manager, "PDExist", true);
                MetaManager meta = Mockito.mock(MetaManager.class);
                Whitebox.setInternalState(this.manager, "metaManager", meta);
                Mockito.when(meta.graphConfigs("DEFAULT")).thenReturn(java.util.Collections.emptyMap());
                Mockito.doAnswer(invocation -> {
                    this.metadataPresent.set(false);
                    return null;
                }).when(meta).removeGraphConfig(Mockito.eq("DEFAULT"), Mockito.eq(name));
                Mockito.doAnswer(invocation -> {
                    this.sharedMetadataCleared.set(true);
                    return null;
                }).when(meta).clearGraphAuth("DEFAULT");
                Map<String, GraphSpace> spaces = Whitebox.getInternalState(this.manager, "graphSpaces");
                spaces.put("DEFAULT", Mockito.mock(GraphSpace.class));
            }
        }

        private HugeGraph open(String name) throws Exception {
            HugeConfig config = FakeObjects.newConfig();
            config.setProperty(Graph.GRAPH, HugeFactory.class.getName());
            config.setProperty("backend", "memory");
            config.setProperty("serializer", "text");
            config.setProperty("store", name);
            config.setProperty(CoreOptions.TASK_WAIT_TIMEOUT.name(), 0L);
            Path path = this.directory.resolve(name + ".properties");
            config.save(path.toFile());
            config.file(path.toString());
            HugeGraph result = HugeFactory.open(config);
            result.initBackend();
            result.serverStarted(GlobalMasterInfo.master("drop-core-test"));
            result.schema().vertexLabel("person").useCustomizeStringId().create();
            result.addVertex(T.id, "kept", T.label, "person");
            result.tx().commit();
            ((HugeGraphParams) Whitebox.getInternalState(result, "params")).closeTx();
            this.graphs.put(result.spaceGraphName(), result);
            return result;
        }

        private BlockingJob start() throws Exception {
            BlockingJob job = new BlockingJob();
            this.jobs.add(job);
            HugeTask<Object> task = new HugeTask<>(IdGenerator.of(9997000L), null, job);
            task.type("test");
            task.name("drop-owner");
            this.scheduler.schedule(task);
            Assert.assertTrue(job.started.await(10L, TimeUnit.SECONDS));
            return job;
        }

        private void drop() {
            if (this.pd) {
                this.manager.dropGraph("DEFAULT", this.graph.name(), true);
            } else {
                this.manager.dropGraphLocal(this.graph.name());
            }
        }

        private void assertRetained() {
            Assert.assertSame(this.graph, this.manager.graph(this.graph.spaceGraphName()));
            Assert.assertFalse(this.graph.closed());
            Assert.assertTrue(Files.exists(this.file));
            Assert.assertEquals(IdGenerator.of("kept"), this.graph.vertices("kept").next().id());
            this.params.closeTx();
            Map<String, ?> registered = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
            Assert.assertSame(this.graph, registered.get(this.graph.spaceGraphName()));
            Assert.assertEquals(0, this.dropEvents);
        }

        @Override
        public void close() throws Exception {
            for (BlockingJob job : this.jobs) {
                job.release.countDown();
            }
            this.scheduler.waitUntilAllTasksCompleted(10L);
            this.manager.close();
            for (HugeGraph current : new HugeGraph[]{this.graph, this.neighbor}) {
                if (!current.closed()) {
                    current.close();
                }
                HugeFactory.remove(current);
            }
            this.graphs.clear();
            FileUtils.deleteDirectory(this.directory.toFile());
        }
    }

    public static class BlockingJob extends SysJob<Object> {

        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        @Override
        public String type() {
            return "test";
        }

        @Override
        public Object execute() {
            this.graph().vertices("kept").next();
            this.started.countDown();
            boolean interrupted = false;
            while (this.release.getCount() != 0L) {
                try {
                    this.release.await();
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
    }
}
