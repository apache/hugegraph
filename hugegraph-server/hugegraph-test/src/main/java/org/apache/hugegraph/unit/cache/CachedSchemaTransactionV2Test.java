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

package org.apache.hugegraph.unit.cache;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.cache.Cache;
import org.apache.hugegraph.backend.cache.CacheManager;
import org.apache.hugegraph.backend.cache.CachedSchemaTransactionV2;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.event.Event;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.event.EventListener;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.meta.PdMetaDriver;
import org.apache.hugegraph.meta.managers.SchemaMetaManager;
import org.apache.hugegraph.pd.client.KvClient;
import org.apache.hugegraph.pd.grpc.kv.WatchResponse;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.SchemaElement;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.SchemaStatus;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.hugegraph.util.Events;
import org.apache.hugegraph.util.LockUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Tests of the V2 schema cache against an in-memory store that stands in for
 * PD. The store can stop a call after it read or wrote its data and before it
 * returns, which is how each test forces the order of a read, a write and a
 * clear. "Another Server" is a second schema manager that writes to the same
 * store without touching the cache under test, followed by the clear this
 * Server would run when that Server's event arrives.
 */
public class CachedSchemaTransactionV2Test extends BaseUnitTest {

    private static final String GRAPH_SPACE = "DEFAULT";
    private static final String CLUSTER = "hg";
    private static final String VERSION = "version";
    private static final long WAIT_SECONDS = 20L;
    private static final AtomicInteger GRAPHS = new AtomicInteger();

    private String graphName;
    private String spaceGraph;
    private FakeObjects objects;
    private HugeGraphParams params;
    private BackendStoreProvider provider;
    private EventHub schemaEventHub;
    private FakeStore store;
    private SchemaMetaManager otherServer;
    private RecordingPublisher publisher;
    private Object originalPublisher;
    private boolean originalRegistered;
    private ExecutorService executor;
    private CachedSchemaTransactionV2 tx;

    @Before
    public void setup() throws Exception {
        this.graphName = "v2cache" + GRAPHS.incrementAndGet();
        this.spaceGraph = GRAPH_SPACE + "-" + this.graphName;
        this.objects = new FakeObjects(this.graphName);
        HugeGraph graph = this.objects.graph();
        Mockito.doReturn(GRAPH_SPACE).when(graph).graphSpace();
        Mockito.doReturn(this.spaceGraph).when(graph).spaceGraphName();

        this.provider = Mockito.mock(BackendStoreProvider.class);
        BackendStore backendStore = Mockito.mock(BackendStore.class);
        Mockito.doReturn(this.provider).when(backendStore).provider();
        this.schemaEventHub = new EventHub("schema-" + this.graphName);

        this.params = Mockito.mock(HugeGraphParams.class);
        Mockito.doReturn(graph).when(this.params).graph();
        Mockito.doReturn(this.graphName).when(this.params).name();
        Mockito.doReturn(this.spaceGraph).when(this.params).spaceGraphName();
        Mockito.doReturn(graph.configuration()).when(this.params).configuration();
        Mockito.doReturn(this.schemaEventHub).when(this.params).schemaEventHub();
        Mockito.doReturn(backendStore).when(this.params).loadGraphStore();

        LockUtil.init(this.spaceGraph);
        this.store = new FakeStore();
        this.otherServer = new SchemaMetaManager(this.store.driver, CLUSTER, graph);
        this.executor = Executors.newCachedThreadPool();

        // There is no PD: don't register the meta watch, and record publishes
        this.originalRegistered = registeredFlag().getAndSet(true);
        // Other test classes open V2 graphs without a PD and leave failed publishes
        resetPendingClearEvents();
        this.publisher = new RecordingPublisher();
        this.originalPublisher = Whitebox.getInternalState(
                CachedSchemaTransactionV2.class, "clearEventPublisher");
        Whitebox.setInternalState(CachedSchemaTransactionV2.class,
                                  "clearEventPublisher", this.publisher);

        this.tx = new CachedSchemaTransactionV2(this.store.driver, CLUSTER,
                                                this.params);
    }

    @After
    public void teardown() throws Exception {
        try {
            this.store.open();
            this.executor.shutdownNow();
            Assert.assertTrue(this.executor.awaitTermination(WAIT_SECONDS,
                                                             TimeUnit.SECONDS));
            this.tx.close();
            CachedSchemaTransactionV2.closeGraph(this.params);
        } finally {
            // Static state of the class under test: always put it back
            resetPendingClearEvents();
            Whitebox.setInternalState(CachedSchemaTransactionV2.class,
                                      "clearEventPublisher", this.originalPublisher);
            registeredFlag().set(this.originalRegistered);
            LockUtil.destroy(this.spaceGraph);
        }
    }

    // ------------------------------------------------------------ publish

    @Test
    public void testGraphOpenPublishesOnce() {
        // setup() opened the graph
        Assert.assertEquals(1, this.publisher.count(this.graphName));

        CachedSchemaTransactionV2 second =
                new CachedSchemaTransactionV2(this.store.driver, CLUSTER, this.params);
        try {
            Assert.assertEquals(1, this.publisher.count(this.graphName));
        } finally {
            second.close();
        }
    }

    @Test
    public void testAddPublishes() {
        this.publisher.reset();
        this.tx.addPropertyKey(this.propertyKey(1, "name", 1));
        Assert.assertEquals(1, this.publisher.count(this.graphName));
    }

    @Test
    public void testUpdatePublishes() {
        this.tx.addPropertyKey(this.propertyKey(1, "name", 1));
        this.publisher.reset();

        this.tx.updatePropertyKey(this.propertyKey(1, "name", 2));

        Assert.assertEquals(1, this.publisher.count(this.graphName));
        Assert.assertEquals(GRAPH_SPACE, this.publisher.events.get(0)[0]);
    }

    @Test
    public void testStatusChangePublishes() {
        PropertyKey pk = this.propertyKey(1, "name", 1);
        this.tx.addPropertyKey(pk);
        this.publisher.reset();

        this.tx.updateSchemaStatus(pk, SchemaStatus.INVALID);

        Assert.assertEquals(1, this.publisher.count(this.graphName));
        Assert.assertEquals(SchemaStatus.INVALID,
                            this.otherServer.getPropertyKey(GRAPH_SPACE, this.graphName,
                                                            IdGenerator.of(1)).status());
    }

    @Test
    public void testRemovePublishesWhateverSyncDeletionSays() {
        for (boolean syncDeletion : new boolean[]{false, true}) {
            this.params.configuration().setProperty(
                    CoreOptions.TASK_SYNC_DELETION.name(), String.valueOf(syncDeletion));
            PropertyKey pk = this.propertyKey(1, "name", 1);
            this.tx.addPropertyKey(pk);
            this.publisher.reset();

            this.tx.removeSchema(pk);

            Assert.assertEquals("task.sync_deletion=" + syncDeletion,
                                1, this.publisher.count(this.graphName));
        }
    }

    @Test
    public void testStoreClearAndTruncatePublishButInitDoesNot() {
        ArgumentCaptor<EventListener> captor =
                ArgumentCaptor.forClass(EventListener.class);
        Mockito.verify(this.provider).listen(captor.capture());
        EventListener storeListener = captor.getValue();
        this.params.configuration().setProperty(
                CoreOptions.TASK_SYNC_DELETION.name(), "true");

        for (String event : new String[]{Events.STORE_CLEAR, Events.STORE_TRUNCATE}) {
            this.tx.addPropertyKey(this.propertyKey(1, "name", 1));
            this.publisher.reset();
            long reads = this.store.reads();

            storeListener.event(new Event(this.provider, event));

            Assert.assertEquals(event, 1, this.publisher.count(this.graphName));
            // The local cache is dropped too
            Assert.assertNotNull(this.tx.getPropertyKey(IdGenerator.of(1)));
            Assert.assertEquals(event, reads + 1, this.store.reads());
        }

        this.publisher.reset();
        storeListener.event(new Event(this.provider, Events.STORE_INIT));
        Assert.assertEquals(0, this.publisher.count(this.graphName));
    }

    @Test
    public void testSchemaClearPublishes() {
        this.tx.addPropertyKey(this.propertyKey(1, "name", 1));
        this.publisher.reset();

        this.tx.clear();

        Assert.assertEquals(1, this.publisher.count(this.graphName));
        Assert.assertNull(this.tx.getPropertyKey(IdGenerator.of(1)));
    }

    @Test
    public void testEveryPublishComesAfterItsStoreWrite() {
        // What another Server would read from the store when the event arrives
        List<Integer> seen = new ArrayList<>();
        this.publisher.intercept = () -> {
            PropertyKey stored = this.otherServer.getPropertyKey(GRAPH_SPACE, this.graphName,
                                                                 IdGenerator.of(1));
            seen.add(stored == null ? -1 : this.version(stored));
        };
        PropertyKey pk = this.propertyKey(1, "name", 1);
        this.tx.addPropertyKey(pk);
        this.tx.updatePropertyKey(this.propertyKey(1, "name", 2));
        this.tx.removeSchema(pk);
        this.tx.addPropertyKey(this.propertyKey(1, "name", 3));
        this.tx.clear();
        this.publisher.intercept = null;

        // added, updated, removed, added again, cleared
        Assert.assertEquals(Arrays.asList(1, 2, -1, 3, -1), seen);
    }

    // ------------------------------------------------------------ failed write

    @Test
    public void testWriteThatFailsHalfwayPublishesAndDropsTheElement() {
        Id id = IdGenerator.of(1);
        this.tx.addPropertyKey(this.propertyKey(1, "name", 1));
        this.tx.getPropertyKeys();
        this.publisher.reset();

        // The id key is replaced, then the store fails before the name key
        this.store.hook = (operation, key) -> {
            if (operation.equals("put") && key.contains("/ID/1")) {
                throw new IllegalStateException("PD is unreachable");
            }
        };
        long generation = this.generation();
        Assert.assertThrows(IllegalStateException.class, () -> {
            this.tx.updatePropertyKey(this.propertyKey(1, "name", 2));
        });
        this.store.hook = null;

        // The store changed, so the other Servers are told although the call failed
        Assert.assertEquals(1, this.publisher.count(this.graphName));
        // A read in flight can't cache what it fetched before the failed write
        Assert.assertTrue(this.generation() > generation);
        // And this Server asks the store instead of serving the copy it had
        Assert.assertNull(this.arrayCached(id));
        long reads = this.store.reads();
        Assert.assertEquals(2, this.version(this.tx.getPropertyKey(id)));
        Assert.assertTrue(this.store.reads() > reads);
    }

    @Test
    public void testFailedWriteKeepsItsEventWhenThePublishFailsToo() throws Exception {
        this.tx.addPropertyKey(this.propertyKey(1, "name", 1));
        this.publisher.reset();
        this.publisher.fail = true;
        this.store.hook = (operation, key) -> {
            if (operation.equals("put") && key.contains("/ID/1")) {
                throw new IllegalStateException("PD is unreachable");
            }
        };
        Assert.assertThrows(IllegalStateException.class, () -> {
            this.tx.updatePropertyKey(this.propertyKey(1, "name", 2));
        });
        this.store.hook = null;
        Assert.assertTrue(this.pending());

        this.publisher.fail = false;
        this.await(() -> this.publisher.count(this.graphName) == 1);
        this.await(() -> !this.pending());
    }

    @Test
    public void testRemoveThatFailsHalfwayPublishesAndDropsTheElement() {
        PropertyKey pk = this.propertyKey(1, "name", 1);
        this.tx.addPropertyKey(pk);
        Assert.assertNotNull(this.tx.getPropertyKey("name"));
        this.publisher.reset();

        // The name key is deleted, then the store fails before the id key
        this.store.hook = (operation, key) -> {
            if (operation.equals("delete") && key.contains("/NAME/name")) {
                throw new IllegalStateException("PD is unreachable");
            }
        };
        Assert.assertThrows(IllegalStateException.class, () -> {
            this.tx.removeSchema(pk);
        });
        this.store.hook = null;

        Assert.assertEquals(1, this.publisher.count(this.graphName));
        // Not served by name from the cache after the store lost the name
        long reads = this.store.reads();
        Assert.assertNull(this.tx.getPropertyKey("name"));
        Assert.assertTrue(this.store.reads() > reads);
    }

    // ------------------------------------------------------------ failed publish

    @Test
    public void testFailedPublishDoesNotFailTheWriteAndIsSentAgain() throws Exception {
        this.publisher.reset();
        this.publisher.fail = true;

        // Committed in the store although the event could not be sent
        this.tx.addPropertyKey(this.propertyKey(1, "name", 1));
        Assert.assertNotNull(this.otherServer.getPropertyKey(GRAPH_SPACE, this.graphName,
                                                             IdGenerator.of(1)));
        Assert.assertEquals(0, this.publisher.count(this.graphName));
        Assert.assertTrue(this.pending());

        this.publisher.fail = false;
        // Sent again by the retry with no further schema change
        this.await(() -> this.publisher.count(this.graphName) == 1);
        this.await(() -> !this.pending());
    }

    @Test
    public void testWatchStartSendsFailedPublishesAtOnce() throws Exception {
        this.publisher.reset();
        this.publisher.fail = true;
        this.tx.updatePropertyKey(this.propertyKey(1, "name", 1));
        // Let the first retry fail too, so the next one is two seconds away
        this.await(() -> this.publisher.attempts.get() >= 2);

        this.publisher.fail = false;
        long start = System.nanoTime();
        watchStarted();

        this.await(() -> this.publisher.count(this.graphName) == 1);
        long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        Assert.assertTrue("sent after " + millis + " ms", millis < 1500L);
        this.await(() -> !this.pending());
    }

    @Test
    public void testFailureDuringARetryIsKept() throws Exception {
        this.publisher.reset();
        this.publisher.fail = true;
        this.tx.updatePropertyKey(this.propertyKey(1, "name", 1));
        Assert.assertTrue(this.pending());

        /*
         * The retry sends its event, and before it marks the graph as done a
         * newer schema change fails to publish. That newer change is not
         * covered by the event the retry sent, so the graph must stay pending.
         */
        CountDownLatch retryInPublish = new CountDownLatch(1);
        CountDownLatch finishRetry = new CountDownLatch(1);
        Thread testThread = Thread.currentThread();
        this.publisher.fail = false;
        this.publisher.intercept = () -> {
            if (Thread.currentThread() == testThread) {
                throw new IllegalStateException("PD is unreachable");
            }
            retryInPublish.countDown();
            awaitLatch(finishRetry);
        };
        watchStarted();
        awaitLatch(retryInPublish);

        this.tx.updatePropertyKey(this.propertyKey(1, "name", 2));
        this.publisher.intercept = null;
        this.publisher.fail = true;
        finishRetry.countDown();

        this.await(() -> this.publisher.count(this.graphName) == 1);
        Thread.sleep(200L);
        Assert.assertTrue(this.pending());

        this.publisher.fail = false;
        this.await(() -> !this.pending());
        Assert.assertEquals(2, this.publisher.count(this.graphName));
    }

    @Test
    public void testWatchStartDuringARetryLeavesOneRetry() throws Exception {
        long epoch = pendingState("retryEpoch");
        this.publisher.reset();
        this.publisher.fail = true;
        this.tx.updatePropertyKey(this.propertyKey(1, "name", 1));

        // The first retry is inside its publish when the watch starts again
        CountDownLatch retryInPublish = new CountDownLatch(1);
        CountDownLatch finishRetry = new CountDownLatch(1);
        Thread testThread = Thread.currentThread();
        this.publisher.intercept = () -> {
            if (Thread.currentThread() != testThread && retryInPublish.getCount() > 0L) {
                retryInPublish.countDown();
                awaitLatch(finishRetry);
            }
        };
        awaitLatch(retryInPublish);
        watchStarted();
        finishRetry.countDown();
        // That retry fails, and so does the one the watch start asked for
        this.await(() -> this.publisher.attempts.get() >= 3);
        Thread.sleep(300L);

        /*
         * Three retries were scheduled: the first, the one of the watch start
         * and one follow-up. The retry that was overtaken must not schedule a
         * follow-up of its own, or two would run from here on and the delay
         * would double twice per round.
         */
        Assert.assertEquals(epoch + 3L, pendingState("retryEpoch"));
        Assert.assertEquals(2000L, pendingState("retryDelay"));
        Assert.assertEquals(3, this.publisher.attempts.get());

        // The delay starts over once the event got through
        this.publisher.intercept = null;
        this.publisher.fail = false;
        watchStarted();
        this.await(() -> !this.pending());
        this.await(() -> pendingState("retryDelay") == 1000L);
    }

    // ------------------------------------------------------------ watch start

    @Test
    public void testPdMetaDriverPassesTheWatchStartedCallback() throws Exception {
        PdMetaDriver driver = Mockito.mock(PdMetaDriver.class, Mockito.CALLS_REAL_METHODS);
        @SuppressWarnings("unchecked")
        KvClient<WatchResponse> client = Mockito.mock(KvClient.class);
        Whitebox.setInternalState(driver, "client", client);
        Consumer<WatchResponse> consumer = response -> { };
        Runnable started = () -> { };

        driver.listen("key", consumer, started);

        // Without it a Server would never learn that its watch started again
        Mockito.verify(client).listen(Mockito.eq("key"), Mockito.same(consumer),
                                      Mockito.any(), Mockito.same(started));
    }

    @Test
    public void testWatchStartClearsEveryGraph() throws Exception {
        this.otherServer.addPropertyKey(GRAPH_SPACE, this.graphName,
                                        this.propertyKey(1, "name", 1));
        Assert.assertEquals(1, this.version(this.tx.getPropertyKey(IdGenerator.of(1))));
        Assert.assertEquals(1, this.tx.getPropertyKeys().size());
        // A cache of another graph of this JVM
        Cache<Id, Object> otherId = CacheManager.instance()
                                                .cache("schema-v2-id-DEFAULT-other", 10L);
        Cache<Id, Object> otherName = CacheManager.instance()
                                                  .cache("schema-v2-name-DEFAULT-other", 10L);
        otherId.update(IdGenerator.of(9), "id");
        otherName.update(IdGenerator.of("nine"), "name");

        // Changed and added by another Server while the watch was down
        this.otherServer.addPropertyKey(GRAPH_SPACE, this.graphName,
                                        this.propertyKey(1, "name", 2));
        this.otherServer.addPropertyKey(GRAPH_SPACE, this.graphName,
                                        this.propertyKey(2, "age", 1));
        Assert.assertEquals(1, this.version(this.tx.getPropertyKey(IdGenerator.of(1))));

        watchStarted();

        Assert.assertEquals(2, this.version(this.tx.getPropertyKey(IdGenerator.of(1))));
        Assert.assertEquals(2, this.version(this.tx.getPropertyKey("name")));
        Assert.assertEquals(2, this.tx.getPropertyKeys().size());
        Assert.assertEquals(0L, otherId.size());
        Assert.assertEquals(0L, otherName.size());
    }

    // ------------------------------------------------------------ guard: reads

    @Test
    public void testReadByIdStartedBeforeAClearIsNotCached() throws Exception {
        Id id = IdGenerator.of(1);
        this.otherServer.addPropertyKey(GRAPH_SPACE, this.graphName,
                                        this.propertyKey(1, "name", 1));

        Gate gate = this.store.stopNext("get", "/ID/1");
        Future<PropertyKey> read = this.executor.submit(() -> this.tx.getPropertyKey(id));
        gate.awaitReached();
        // The read holds version 1. Another Server replaces it and its event arrives.
        this.otherServer.addPropertyKey(GRAPH_SPACE, this.graphName,
                                        this.propertyKey(1, "name", 2));
        this.clearByEvent();
        gate.open();
        Assert.assertEquals(1, this.version(read.get(WAIT_SECONDS, TimeUnit.SECONDS)));

        long reads = this.store.reads();
        Assert.assertEquals(2, this.version(this.tx.getPropertyKey(id)));
        Assert.assertTrue("the old value must not be served from the cache",
                          this.store.reads() > reads);
        // And what is cached now is the new one
        reads = this.store.reads();
        Assert.assertEquals(2, this.version(this.tx.getPropertyKey(id)));
        Assert.assertEquals(reads, this.store.reads());
    }

    @Test
    public void testReadByNameStartedBeforeAClearIsNotCached() throws Exception {
        this.otherServer.addPropertyKey(GRAPH_SPACE, this.graphName,
                                        this.propertyKey(1, "name", 1));

        Gate gate = this.store.stopNext("scan", "PROPERTY_KEY");
        Future<PropertyKey> read = this.executor.submit(() -> this.tx.getPropertyKey("name"));
        gate.awaitReached();
        // The reload that follows the name miss has scanned version 1
        this.otherServer.addPropertyKey(GRAPH_SPACE, this.graphName,
                                        this.propertyKey(1, "name", 2));
        this.clearByEvent();
        gate.open();
        read.get(WAIT_SECONDS, TimeUnit.SECONDS);

        Assert.assertEquals(2, this.version(this.tx.getPropertyKey("name")));
        Assert.assertEquals(2, this.version(this.tx.getPropertyKey(IdGenerator.of(1))));
        Assert.assertEquals(2, this.version(this.tx.getPropertyKeys().get(0)));
    }

    @Test
    public void testListingStartedBeforeAClearIsNotCached() throws Exception {
        this.otherServer.addPropertyKey(GRAPH_SPACE, this.graphName,
                                        this.propertyKey(1, "name", 1));

        Gate gate = this.store.stopNext("scan", "PROPERTY_KEY");
        Future<List<PropertyKey>> listing = this.executor.submit(() -> this.tx.getPropertyKeys());
        gate.awaitReached();
        this.otherServer.addPropertyKey(GRAPH_SPACE, this.graphName,
                                        this.propertyKey(2, "age", 1));
        this.clearByEvent();
        gate.open();
        Assert.assertEquals(1, listing.get(WAIT_SECONDS, TimeUnit.SECONDS).size());

        // The list of one must not be kept as "all property keys"
        Assert.assertEquals(2, this.tx.getPropertyKeys().size());
        Assert.assertNotNull(this.tx.getPropertyKey("age"));
    }

    @Test
    public void testReadStartedBeforeALocalWriteIsNotCached() throws Exception {
        Id id = IdGenerator.of(1);
        this.otherServer.addPropertyKey(GRAPH_SPACE, this.graphName,
                                        this.propertyKey(1, "name", 1));

        Gate gate = this.store.stopNext("get", "/ID/1");
        Future<PropertyKey> read = this.executor.submit(() -> this.tx.getPropertyKey(id));
        gate.awaitReached();
        // This Server itself replaces the element while the read is in flight
        this.tx.updatePropertyKey(this.propertyKey(1, "name", 2));
        gate.open();
        Assert.assertEquals(1, this.version(read.get(WAIT_SECONDS, TimeUnit.SECONDS)));

        Assert.assertEquals(2, this.version(this.tx.getPropertyKey(id)));
        Assert.assertEquals(2, this.version(this.tx.getPropertyKey("name")));
    }

    @Test
    public void testReadStartedBeforeARemoveIsNotCached() throws Exception {
        Id id = IdGenerator.of(1);
        PropertyKey pk = this.propertyKey(1, "name", 1);
        this.otherServer.addPropertyKey(GRAPH_SPACE, this.graphName, pk);

        Gate gate = this.store.stopNext("get", "/ID/1");
        Future<PropertyKey> read = this.executor.submit(() -> this.tx.getPropertyKey(id));
        gate.awaitReached();
        this.tx.removeSchema(pk);
        gate.open();
        Assert.assertNotNull(read.get(WAIT_SECONDS, TimeUnit.SECONDS));

        Assert.assertNull(this.tx.getPropertyKey(id));
        Assert.assertNull(this.tx.getPropertyKey("name"));
        Assert.assertEquals(0, this.tx.getPropertyKeys().size());
    }

    @Test
    public void testReadStartedBeforeALocalInvalidEventIsNotCached() throws Exception {
        Id id = IdGenerator.of(1);
        this.otherServer.addPropertyKey(GRAPH_SPACE, this.graphName,
                                        this.propertyKey(1, "name", 1));

        Gate gate = this.store.stopNext("get", "/ID/1");
        Future<PropertyKey> read = this.executor.submit(() -> this.tx.getPropertyKey(id));
        gate.awaitReached();
        this.otherServer.addPropertyKey(GRAPH_SPACE, this.graphName,
                                        this.propertyKey(1, "name", 2));
        // The element is invalidated through the event hub of the graph
        this.schemaEventHub.notify(Events.CACHE, Cache.ACTION_INVALID,
                                   HugeType.PROPERTY_KEY, id).get();
        gate.open();
        Assert.assertEquals(1, this.version(read.get(WAIT_SECONDS, TimeUnit.SECONDS)));

        Assert.assertEquals(2, this.version(this.tx.getPropertyKey(id)));
    }

    @Test
    public void testReadDuringALocalWriteDoesNotPutTheOldValueBack() throws Exception {
        Id id = IdGenerator.of(1);
        this.otherServer.addPropertyKey(GRAPH_SPACE, this.graphName,
                                        this.propertyKey(1, "name", 1));
        // Version 1 is in the id cache and not in the array cache
        this.tx.getPropertyKeys();
        Assert.assertNull(this.arrayCached(id));

        /*
         * Hold the write of version 2 while it updates the caches: the store
         * has version 2 and the id cache still has version 1.
         */
        CountDownLatch inCaches = new CountDownLatch(1);
        CountDownLatch finishWrite = new CountDownLatch(1);
        AtomicBoolean armed = new AtomicBoolean(false);
        PropertyKey update = Mockito.spy(this.propertyKey(1, "name", 2));
        Mockito.doAnswer(invocation -> {
            if (armed.compareAndSet(true, false)) {
                inCaches.countDown();
                awaitLatch(finishWrite);
            }
            return HugeType.PROPERTY_KEY;
        }).when(update).type();
        this.store.hook = (operation, key) -> {
            if (operation.equals("put") && key.contains("/NAME/name")) {
                armed.set(true);
            }
        };
        Future<?> write = this.executor.submit(() -> this.tx.updatePropertyKey(update));
        awaitLatch(inCaches);

        // A read takes version 1 from the id cache and wants to copy it
        AtomicReference<Thread> reader = new AtomicReference<>();
        Future<PropertyKey> read = this.executor.submit(() -> {
            reader.set(Thread.currentThread());
            return this.tx.getPropertyKey(id);
        });
        this.await(() -> reader.get() != null &&
                         reader.get().getState() == Thread.State.WAITING);
        finishWrite.countDown();
        write.get(WAIT_SECONDS, TimeUnit.SECONDS);
        Assert.assertEquals(1, this.version(read.get(WAIT_SECONDS, TimeUnit.SECONDS)));

        // It did not: every later read gets version 2 without asking the store
        long reads = this.store.reads();
        Assert.assertEquals(2, this.version((PropertyKey) this.arrayCached(id)));
        Assert.assertEquals(2, this.version(this.tx.getPropertyKey(id)));
        Assert.assertEquals(2, this.version(this.tx.getPropertyKey("name")));
        Assert.assertEquals(reads, this.store.reads());
    }

    @Test
    public void testGuardIsSharedByTheTransactionsOfAGraph() throws Exception {
        Id id = IdGenerator.of(1);
        this.otherServer.addPropertyKey(GRAPH_SPACE, this.graphName,
                                        this.propertyKey(1, "name", 1));
        // Each request has its own transaction; they share the caches
        CachedSchemaTransactionV2 second = new CachedSchemaTransactionV2(
                this.store.driver, CLUSTER, this.params);
        try {
            Gate gate = this.store.stopNext("get", "/ID/1");
            Future<PropertyKey> read = this.executor.submit(() -> this.tx.getPropertyKey(id));
            gate.awaitReached();
            second.updatePropertyKey(this.propertyKey(1, "name", 2));
            gate.open();
            Assert.assertEquals(1, this.version(read.get(WAIT_SECONDS, TimeUnit.SECONDS)));

            Assert.assertEquals(2, this.version(this.tx.getPropertyKey(id)));
            Assert.assertEquals(2, this.version(second.getPropertyKey("name")));
        } finally {
            second.close();
        }
    }

    // ------------------------------------------------------------ guard: write-through

    @Test
    public void testUpdateWriteThroughAfterAClearIsNotCached() throws Exception {
        Id id = IdGenerator.of(1);
        this.otherServer.addPropertyKey(GRAPH_SPACE, this.graphName,
                                        this.propertyKey(1, "name", 1));
        this.tx.getPropertyKeys();

        // The write of version 2 reaches the store and its answer is delayed
        Gate gate = this.store.stopNext("put", "/NAME/name");
        Future<?> write = this.executor.submit(
                () -> this.tx.updatePropertyKey(this.propertyKey(1, "name", 2)));
        gate.awaitReached();
        // Another Server writes version 3 after it, and its event arrives first
        this.otherServer.addPropertyKey(GRAPH_SPACE, this.graphName,
                                        this.propertyKey(1, "name", 3));
        this.clearByEvent();
        gate.open();
        write.get(WAIT_SECONDS, TimeUnit.SECONDS);

        Assert.assertEquals(3, this.version(this.tx.getPropertyKey(id)));
        Assert.assertEquals(3, this.version(this.tx.getPropertyKey("name")));
        Assert.assertEquals(3, this.version(this.tx.getPropertyKeys().get(0)));
    }

    @Test
    public void testAddWriteThroughAfterAClearIsNotCached() throws Exception {
        Id id = IdGenerator.of(1);

        Gate gate = this.store.stopNext("put", "/NAME/name");
        Future<?> write = this.executor.submit(
                () -> this.tx.addPropertyKey(this.propertyKey(1, "name", 1)));
        gate.awaitReached();
        this.otherServer.addPropertyKey(GRAPH_SPACE, this.graphName,
                                        this.propertyKey(1, "name", 2));
        this.clearByEvent();
        gate.open();
        write.get(WAIT_SECONDS, TimeUnit.SECONDS);

        Assert.assertEquals(2, this.version(this.tx.getPropertyKey(id)));
        Assert.assertEquals(2, this.version(this.tx.getPropertyKey("name")));
    }

    @Test
    public void testRefusedWriteThroughEvictsWhatWasCachedMeanwhile() throws Exception {
        Id id = IdGenerator.of(1);
        this.otherServer.addPropertyKey(GRAPH_SPACE, this.graphName,
                                        this.propertyKey(1, "name", 1));

        // The write of version 2 noted the generation and has not reached the store
        Gate gate = this.store.stopBeforePut("/ID/1");
        Future<?> write = this.executor.submit(
                () -> this.tx.updatePropertyKey(this.propertyKey(1, "name", 2)));
        gate.awaitReached();
        // A clear arrives, and readers cache version 1 before the write lands
        this.clearByEvent();
        Assert.assertEquals(1, this.version(this.tx.getPropertyKey(id)));
        Assert.assertEquals(1, this.version(this.tx.getPropertyKeys().get(0)));
        gate.open();
        write.get(WAIT_SECONDS, TimeUnit.SECONDS);

        // The write-through is not cached, and it removed the older copy
        Assert.assertNull(this.arrayCached(id));
        Assert.assertEquals(2, this.version(this.tx.getPropertyKey(id)));
        Assert.assertEquals(2, this.version(this.tx.getPropertyKey("name")));
        Assert.assertEquals(2, this.version(this.tx.getPropertyKeys().get(0)));

        // That listing is the whole list again: the next one needs no store
        long scans = this.store.scans.get();
        Assert.assertEquals(1, this.tx.getPropertyKeys().size());
        Assert.assertEquals("served from the cache", scans, this.store.scans.get());
    }

    @Test
    public void testWriteThroughWithoutAClearIsCached() {
        Id id = IdGenerator.of(1);
        this.tx.addPropertyKey(this.propertyKey(1, "name", 1));
        long reads = this.store.reads();
        Assert.assertEquals(1, this.version(this.tx.getPropertyKey(id)));
        Assert.assertEquals(1, this.version(this.tx.getPropertyKey("name")));

        this.tx.updatePropertyKey(this.propertyKey(1, "name", 2));
        Assert.assertEquals(2, this.version(this.tx.getPropertyKey(id)));
        Assert.assertEquals(2, this.version(this.tx.getPropertyKey("name")));
        Assert.assertEquals("served by the write-through", reads, this.store.reads());
    }

    // ------------------------------------------------------------ guard: each insertion

    @Test
    public void testEveryInsertionChecksTheGeneration() throws Exception {
        Id id = IdGenerator.of(1);
        PropertyKey old = this.propertyKey(1, "name", 1);
        this.otherServer.addPropertyKey(GRAPH_SPACE, this.graphName,
                                        this.propertyKey(1, "name", 2));

        long before = this.generation();
        this.clearByEvent();
        Assert.assertTrue(this.generation() > before);

        // A value read from the store before the clear
        this.invoke("cacheIfCurrent", new Class<?>[]{SchemaElement.class, long.class},
                    old, before);
        Assert.assertNull(this.arrayCached(id));
        Assert.assertEquals(2, this.version(this.tx.getPropertyKey(id)));

        // An id-cache hit taken before the clear, copied to the array cache
        this.clearByEvent();
        this.invoke("promoteIfCurrent", new Class<?>[]{SchemaElement.class, long.class},
                    old, before);
        Assert.assertNull(this.arrayCached(id));

        // A write sent before the clear
        this.invoke("cacheWritten", new Class<?>[]{SchemaElement.class, long.class},
                    old, before);
        Assert.assertNull(this.arrayCached(id));
        Assert.assertEquals(2, this.version(this.tx.getPropertyKey(id)));
        Assert.assertEquals(2, this.version(this.tx.getPropertyKey("name")));

        // With the current generation each of them caches
        this.clearByEvent();
        long current = this.generation();
        PropertyKey fresh = this.propertyKey(1, "name", 2);
        this.invoke("cacheIfCurrent", new Class<?>[]{SchemaElement.class, long.class},
                    fresh, current);
        Assert.assertSame(fresh, this.arrayCached(id));
        long reads = this.store.reads();
        Assert.assertSame(fresh, this.tx.getPropertyKey(id));
        Assert.assertSame(fresh, this.tx.getPropertyKey("name"));
        Assert.assertEquals(reads, this.store.reads());
    }

    @Test
    public void testClearsRemovesAndWritesAdvanceTheGeneration() {
        PropertyKey pk = this.propertyKey(1, "name", 1);

        long generation = this.generation();
        this.tx.addPropertyKey(pk);
        Assert.assertTrue("add", this.generation() > generation);

        generation = this.generation();
        this.tx.updatePropertyKey(this.propertyKey(1, "name", 2));
        Assert.assertTrue("update", this.generation() > generation);

        generation = this.generation();
        this.clearByEvent();
        Assert.assertTrue("clear by event", this.generation() > generation);

        generation = this.generation();
        this.tx.clearCache(false);
        Assert.assertTrue("local clear", this.generation() > generation);

        generation = this.generation();
        watchStarted();
        Assert.assertTrue("watch start", this.generation() > generation);

        generation = this.generation();
        this.tx.removeSchema(pk);
        Assert.assertTrue("remove", this.generation() > generation);

        // A plain read changes nothing
        generation = this.generation();
        this.tx.getPropertyKey(IdGenerator.of(1));
        this.tx.getPropertyKeys();
        Assert.assertEquals(generation, this.generation());
    }

    // ------------------------------------------------------------ name miss

    @Test
    public void testConcurrentNameMissesReloadOnce() throws Exception {
        for (int i = 1; i <= 3; i++) {
            this.otherServer.addPropertyKey(GRAPH_SPACE, this.graphName,
                                            this.propertyKey(i, "pk" + i, 1));
        }
        int threads = 6;
        // Every lookup misses the cache and finds its name in the store first
        CountDownLatch missed = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        this.store.hook = (operation, key) -> {
            if (operation.equals("get") && key.endsWith("/NAME/pk1")) {
                missed.countDown();
                awaitLatch(go);
            }
        };
        List<Future<PropertyKey>> lookups = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            lookups.add(this.executor.submit(() -> this.tx.getPropertyKey("pk1")));
        }
        awaitLatch(missed);
        this.store.hook = null;
        long scans = this.store.scans.get();
        go.countDown();
        for (Future<PropertyKey> lookup : lookups) {
            Assert.assertNotNull(lookup.get(WAIT_SECONDS, TimeUnit.SECONDS));
        }

        // One reload reads the four schema types, the other lookups reuse it
        Assert.assertEquals(4L, this.store.scans.get() - scans);
        long reads = this.store.reads();
        Assert.assertNotNull(this.tx.getPropertyKey("pk2"));
        Assert.assertNotNull(this.tx.getPropertyKey(IdGenerator.of(3)));
        Assert.assertEquals(3, this.tx.getPropertyKeys().size());
        Assert.assertEquals("the cache must stay filled after the misses",
                            reads, this.store.reads());
    }

    // ------------------------------------------------------------ listing

    @Test
    public void testAClearWaitsForAListingReadFromTheCache() throws Exception {
        this.otherServer.addPropertyKey(GRAPH_SPACE, this.graphName,
                                        this.propertyKey(1, "name", 1));
        this.tx.getPropertyKeys();

        // A cached element that can hold a listing in the middle of its pass
        CountDownLatch inListing = new CountDownLatch(1);
        CountDownLatch finishListing = new CountDownLatch(1);
        AtomicBoolean armed = new AtomicBoolean(false);
        PropertyKey slow = Mockito.spy(this.propertyKey(2, "slow", 1));
        Mockito.doAnswer(invocation -> {
            if (armed.get()) {
                inListing.countDown();
                awaitLatch(finishListing);
            }
            return HugeType.PROPERTY_KEY;
        }).when(slow).type();
        this.invoke("cacheIfCurrent", new Class<?>[]{SchemaElement.class, long.class},
                    slow, this.generation());
        armed.set(true);

        Future<List<PropertyKey>> listing = this.executor.submit(
                () -> this.tx.getPropertyKeys());
        awaitLatch(inListing);
        Future<?> clear = this.executor.submit(this::clearByEvent);
        try {
            clear.get(300L, TimeUnit.MILLISECONDS);
            Assert.fail("the clear ran while a listing was reading the cache");
        } catch (TimeoutException expected) {
            // The clear waits for the listing
        } finally {
            finishListing.countDown();
        }
        Assert.assertEquals(2, listing.get(WAIT_SECONDS, TimeUnit.SECONDS).size());
        clear.get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    public void testListingIsNeverPartialWhileClearsRun() throws Exception {
        int count = 50;
        int listings = 3000;
        for (int i = 1; i <= count; i++) {
            this.otherServer.addPropertyKey(GRAPH_SPACE, this.graphName,
                                            this.propertyKey(i, "pk" + i, 1));
        }
        AtomicBoolean stop = new AtomicBoolean(false);
        Future<?> clears = this.executor.submit(() -> {
            while (!stop.get()) {
                this.clearByEvent();
                // Leave room for a listing to fill the cache and for others to use it
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1L));
            }
        });
        long scans = this.store.scans.get();
        try {
            for (int i = 0; i < listings; i++) {
                Assert.assertEquals("listing " + i, count, this.tx.getPropertyKeys().size());
            }
        } finally {
            stop.set(true);
        }
        clears.get(WAIT_SECONDS, TimeUnit.SECONDS);
        Assert.assertTrue("some listings must have come from the cache",
                          this.store.scans.get() - scans < listings);
    }

    @Test
    public void testReadsNeverGoBackBehindAWriteUnderConcurrentClears() throws Exception {
        Id id = IdGenerator.of(1);
        this.tx.addPropertyKey(this.propertyKey(1, "name", 0));
        // The last version whose update has returned
        AtomicInteger written = new AtomicInteger(0);
        AtomicBoolean stop = new AtomicBoolean(false);
        List<Future<?>> noise = new ArrayList<>();
        noise.add(this.executor.submit(() -> {
            while (!stop.get()) {
                this.clearByEvent();
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1L));
            }
        }));
        for (int i = 0; i < 3; i++) {
            noise.add(this.executor.submit(() -> {
                while (!stop.get()) {
                    int floor = written.get();
                    this.assertNotOlder(floor, this.tx.getPropertyKey(id));
                    floor = written.get();
                    this.assertNotOlder(floor, this.tx.getPropertyKey("name"));
                    floor = written.get();
                    this.assertNotOlder(floor, this.tx.getPropertyKeys().get(0));
                }
            }));
        }
        Future<?> writer = this.executor.submit(() -> {
            for (int version = 1; version <= 300; version++) {
                this.tx.updatePropertyKey(this.propertyKey(1, "name", version));
                written.set(version);
            }
        });
        try {
            // A deadlock fails here instead of hanging the run
            writer.get(3L * WAIT_SECONDS, TimeUnit.SECONDS);
        } finally {
            stop.set(true);
        }
        for (Future<?> future : noise) {
            future.get(WAIT_SECONDS, TimeUnit.SECONDS);
        }

        Assert.assertEquals(300, this.version(this.tx.getPropertyKey(id)));
        Assert.assertEquals(300, this.version(this.tx.getPropertyKey("name")));
        Assert.assertEquals(300, this.version(this.tx.getPropertyKeys().get(0)));
    }

    // ------------------------------------------------------------ helpers

    private PropertyKey propertyKey(int id, String name, int version) {
        PropertyKey pk = this.objects.newPropertyKey(IdGenerator.of(id), name);
        pk.userdata(VERSION, version);
        return pk;
    }

    private int version(PropertyKey pk) {
        Assert.assertNotNull(pk);
        return ((Number) pk.userdata().get(VERSION)).intValue();
    }

    // A read that began after a version was written must not return an older one
    private void assertNotOlder(int floor, PropertyKey pk) {
        int version = this.version(pk);
        if (version < floor) {
            throw new AssertionError("read version " + version + " after version " +
                                     floor + " was written");
        }
    }

    /** What this Server does when another Server's clear event arrives. */
    private void clearByEvent() {
        Whitebox.invokeStatic(CachedSchemaTransactionV2.class,
                              new Class<?>[]{String.class},
                              "clearSchemaCache", this.spaceGraph);
    }

    private static void watchStarted() {
        try {
            Method method = CachedSchemaTransactionV2.class
                    .getDeclaredMethod("handleSchemaCacheWatchStarted");
            method.setAccessible(true);
            method.invoke(null);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private Object guard() {
        Object guard = Whitebox.getInternalState(this.tx, "arrayCaches");
        Assert.assertNotNull(guard);
        return guard;
    }

    private Object callGuard(String name, Object... args) {
        Object guard = this.guard();
        try {
            for (Method method : guard.getClass().getDeclaredMethods()) {
                if (method.getName().equals(name) &&
                    method.getParameterCount() == args.length) {
                    method.setAccessible(true);
                    return method.invoke(guard, args);
                }
            }
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
        throw new AssertionError("No method " + name);
    }

    private long generation() {
        return (Long) this.callGuard("generation");
    }

    private Object arrayCached(Id id) {
        return this.callGuard("get", HugeType.PROPERTY_KEY, id);
    }

    private void invoke(String name, Class<?>[] types, Object... args) {
        try {
            Method method = CachedSchemaTransactionV2.class.getDeclaredMethod(name, types);
            method.setAccessible(true);
            method.invoke(this.tx, args);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static AtomicBoolean registeredFlag() throws Exception {
        Field field = CachedSchemaTransactionV2.class
                .getDeclaredField("metaEventListenerRegistered");
        field.setAccessible(true);
        return (AtomicBoolean) field.get(null);
    }

    private static Object pendingClearEvents() {
        return Whitebox.getInternalState(CachedSchemaTransactionV2.class,
                                         "PENDING_CLEAR_EVENTS");
    }

    private static Map<?, ?> pendingMap() {
        return Whitebox.getInternalState(pendingClearEvents(), "pending");
    }

    private static long pendingState(String field) {
        Object pending = pendingClearEvents();
        synchronized (pending) {
            Long value = Whitebox.getInternalState(pending, field);
            return value;
        }
    }

    /**
     * Forget the failed publishes and their scheduled retry. They are static
     * state of the class under test, so without this a test that opens a V2
     * graph without a PD leaves a retry behind that fires in a later test.
     */
    static void resetPendingClearEvents() {
        Object pending = pendingClearEvents();
        synchronized (pending) {
            pendingMap().clear();
            ScheduledFuture<?> retry = Whitebox.getInternalState(pending, "retry");
            if (retry != null) {
                retry.cancel(false);
            }
            Whitebox.setInternalState(pending, "retryDelay", 1000L);
        }
    }

    // Whether this graph has a clear event waiting to be published again
    private boolean pending() {
        for (Object graph : pendingMap().keySet()) {
            if (graph.toString().contains(this.graphName + "]")) {
                return true;
            }
        }
        return false;
    }

    private void await(Condition condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (!condition.met()) {
            Assert.assertTrue("timed out", System.nanoTime() < deadline);
            Thread.sleep(10L);
        }
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting for the other thread");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    @FunctionalInterface
    private interface Condition {

        boolean met() throws Exception;
    }

    /** Stops one store call after it did its work and before it returns. */
    private static final class Gate {

        private final CountDownLatch reached = new CountDownLatch(1);
        private final CountDownLatch opened = new CountDownLatch(1);

        void awaitReached() {
            awaitLatch(this.reached);
        }

        void open() {
            this.opened.countDown();
        }

        void stop() {
            this.reached.countDown();
            awaitLatch(this.opened);
        }
    }

    private static final class RecordingPublisher implements BiConsumer<String, String> {

        private final List<String[]> events = new CopyOnWriteArrayList<>();
        private final AtomicInteger attempts = new AtomicInteger();
        private volatile boolean fail;
        private volatile Runnable intercept;

        @Override
        public void accept(String graphSpace, String graph) {
            this.attempts.incrementAndGet();
            // Decided when the call starts, like a request already on its way
            boolean failed = this.fail;
            Runnable current = this.intercept;
            if (current != null) {
                current.run();
            }
            if (failed) {
                throw new IllegalStateException("PD is unreachable");
            }
            this.events.add(new String[]{graphSpace, graph});
        }

        int count(String graph) {
            int count = 0;
            for (String[] event : this.events) {
                if (event[1].equals(graph)) {
                    count++;
                }
            }
            return count;
        }

        void reset() {
            this.events.clear();
            this.attempts.set(0);
        }
    }

    /** An in-memory stand-in for the PD key-value store. */
    private static final class FakeStore {

        private final ConcurrentSkipListMap<String, String> data =
                new ConcurrentSkipListMap<>();
        private final AtomicInteger gets = new AtomicInteger();
        private final AtomicInteger scans = new AtomicInteger();
        private final List<Gate> gates = new CopyOnWriteArrayList<>();
        private final PdMetaDriver driver = Mockito.mock(PdMetaDriver.class);
        private volatile StoreHook hook;
        private volatile Consumer<String> beforePut;

        FakeStore() {
            Mockito.doAnswer(invocation -> {
                String key = invocation.getArgument(0);
                Consumer<String> before = this.beforePut;
                if (before != null) {
                    before.accept(key);
                }
                this.data.put(key, invocation.getArgument(1));
                this.after("put", key);
                return null;
            }).when(this.driver).put(Mockito.anyString(), Mockito.anyString());
            Mockito.doAnswer(invocation -> {
                String key = invocation.getArgument(0);
                String value = this.data.get(key);
                this.gets.incrementAndGet();
                this.after("get", key);
                return value;
            }).when(this.driver).get(Mockito.anyString());
            Mockito.doAnswer(invocation -> {
                String key = invocation.getArgument(0);
                this.data.remove(key);
                this.after("delete", key);
                return null;
            }).when(this.driver).delete(Mockito.anyString());
            Mockito.doAnswer(invocation -> {
                String prefix = invocation.getArgument(0);
                this.data.keySet().removeIf(key -> key.startsWith(prefix));
                this.after("delete", prefix);
                return null;
            }).when(this.driver).deleteWithPrefix(Mockito.anyString());
            Mockito.doAnswer(invocation -> {
                String prefix = invocation.getArgument(0);
                Map<String, String> result = new TreeMap<>();
                this.data.forEach((key, value) -> {
                    if (key.startsWith(prefix)) {
                        result.put(key, value);
                    }
                });
                this.scans.incrementAndGet();
                this.after("scan", prefix);
                return result;
            }).when(this.driver).scanWithPrefix(Mockito.anyString());
        }

        long reads() {
            return this.gets.get() + this.scans.get();
        }

        /** Stop the next call of this operation whose key contains the text. */
        Gate stopNext(String operation, String keyPart) {
            Gate gate = new Gate();
            AtomicBoolean used = new AtomicBoolean(false);
            BiPredicate<String, String> matches =
                    (op, key) -> op.equals(operation) && key.contains(keyPart);
            this.gates.add(gate);
            this.hook = (op, key) -> {
                if (matches.test(op, key) && used.compareAndSet(false, true)) {
                    gate.stop();
                }
            };
            return gate;
        }

        /** Stop the next put whose key contains the text, before it changes the store. */
        Gate stopBeforePut(String keyPart) {
            Gate gate = new Gate();
            AtomicBoolean used = new AtomicBoolean(false);
            this.gates.add(gate);
            this.beforePut = key -> {
                if (key.contains(keyPart) && used.compareAndSet(false, true)) {
                    gate.stop();
                }
            };
            return gate;
        }

        void open() {
            this.hook = null;
            this.beforePut = null;
            for (Gate gate : this.gates) {
                gate.open();
            }
        }

        private void after(String operation, String key) {
            StoreHook current = this.hook;
            if (current != null) {
                current.after(operation, key);
            }
        }
    }

    @FunctionalInterface
    private interface StoreHook {

        void after(String operation, String key);
    }
}
