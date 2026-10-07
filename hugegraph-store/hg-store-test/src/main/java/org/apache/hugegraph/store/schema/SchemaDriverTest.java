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

package org.apache.hugegraph.store.schema;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.exception.NotAllowException;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.pd.client.KvClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.kv.KResponse;
import org.apache.hugegraph.pd.grpc.kv.ScanPrefixResponse;
import org.apache.hugegraph.pd.grpc.kv.WatchEvent;
import org.apache.hugegraph.pd.grpc.kv.WatchKv;
import org.apache.hugegraph.pd.grpc.kv.WatchResponse;
import org.apache.hugegraph.pd.grpc.kv.WatchType;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.business.BusinessHandlerImpl;
import org.apache.hugegraph.store.options.HgStoreEngineOptions;
import org.apache.hugegraph.store.util.HgStoreException;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SchemaDriverTest {

    private AtomicReference<SchemaDriver> instance;
    private SchemaDriver previousInstance;

    @Before
    public void setUp() throws Exception {
        this.instance = instanceReference();
        this.previousInstance = this.instance.getAndSet(null);
    }

    @After
    public void tearDown() {
        this.instance.set(this.previousInstance);
    }

    @Test
    public void testDestroyClosesOwnedKvClient() {
        TrackingKvClient client = new TrackingKvClient();
        SchemaDriver driver = new SchemaDriver(client, 10, 60_000L);
        this.instance.set(driver);

        try {
            SchemaDriver.destroy();

            Assert.assertTrue(client.closed);
            Assert.assertNull(SchemaDriver.getInstance());
        } finally {
            client.close();
        }
    }

    @Test
    public void testDestroyUnpublishesInstanceBeforeClosingResources() throws Exception {
        BlockingCloseKvClient client = new BlockingCloseKvClient();
        SchemaDriver driver = new SchemaDriver(client, 10, 60_000L);
        this.instance.set(driver);
        Thread destroyThread = new Thread(SchemaDriver::destroy, "schema-driver-destroy");
        try {
            destroyThread.start();
            client.awaitCloseStarted();

            Assert.assertNull(SchemaDriver.getInstance());
        } finally {
            client.allowClose.countDown();
            destroyThread.join(5000L);
            client.close();
        }

        Assert.assertFalse(destroyThread.isAlive());
        Assert.assertTrue(client.closed);
    }

    @Test
    public void testInitDoesNotWaitForDestroyCleanup() throws Exception {
        BlockingCloseKvClient client = new BlockingCloseKvClient();
        SchemaDriver driver = new SchemaDriver(client, 10, 60_000L);
        this.instance.set(driver);
        AtomicReference<Throwable> initFailure = new AtomicReference<>();
        CountDownLatch initFinished = new CountDownLatch(1);
        Thread destroyThread = new Thread(SchemaDriver::destroy, "schema-driver-destroy");
        Thread initThread = new Thread(() -> {
            try {
                SchemaDriver.init(PDConfig.of("127.0.0.1:8686"), 10, 60_000L);
            } catch (Throwable throwable) {
                initFailure.set(throwable);
            } finally {
                initFinished.countDown();
            }
        }, "schema-driver-init");
        boolean initCompletedDuringCleanup;
        try {
            destroyThread.start();
            client.awaitCloseStarted();
            initThread.start();
            initCompletedDuringCleanup = initFinished.await(1L, TimeUnit.SECONDS);
        } finally {
            client.allowClose.countDown();
            destroyThread.join(5000L);
            initThread.join(5000L);
            client.close();
        }

        Assert.assertTrue(initCompletedDuringCleanup);
        Assert.assertTrue(initFailure.get() instanceof NotAllowException);
        Assert.assertFalse(destroyThread.isAlive());
        Assert.assertFalse(initThread.isAlive());
    }

    @Test
    public void testConcurrentDestroyWaitsForCleanup() throws Exception {
        BlockingCloseKvClient client = new BlockingCloseKvClient();
        SchemaDriver driver = new SchemaDriver(client, 10, 60_000L);
        this.instance.set(driver);
        CountDownLatch secondDestroyStarted = new CountDownLatch(1);
        CountDownLatch secondDestroyReturned = new CountDownLatch(1);
        Thread firstDestroy = new Thread(SchemaDriver::destroy, "schema-driver-destroy-first");
        Thread secondDestroy = new Thread(() -> {
            secondDestroyStarted.countDown();
            SchemaDriver.destroy();
            secondDestroyReturned.countDown();
        }, "schema-driver-destroy-second");
        boolean returnedBeforeCleanup;
        try {
            firstDestroy.start();
            client.awaitCloseStarted();
            secondDestroy.start();
            Assert.assertTrue(secondDestroyStarted.await(5L, TimeUnit.SECONDS));
            returnedBeforeCleanup = secondDestroyReturned.await(500L, TimeUnit.MILLISECONDS);
        } finally {
            client.allowClose.countDown();
            firstDestroy.join(5000L);
            secondDestroy.join(5000L);
            client.close();
        }

        Assert.assertFalse(returnedBeforeCleanup);
        Assert.assertFalse(firstDestroy.isAlive());
        Assert.assertFalse(secondDestroy.isAlive());
    }

    @Test
    public void testConstructorFailureClosesOwnedKvClient() {
        FailingListenKvClient client = new FailingListenKvClient();

        try {
            new SchemaDriver(client, 10, 60_000L);
            Assert.fail("SchemaDriver construction should fail");
        } catch (HugeException expected) {
            Assert.assertEquals(2, client.listenCalls);
            Assert.assertTrue(client.closed);
        } finally {
            client.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testStoreCleanupClosesDriverAndReleasesGraphWrappers() throws Exception {
        Field cacheField = BusinessHandlerImpl.class.getDeclaredField("GRAPH_SUPPLIER_CACHE");
        cacheField.setAccessible(true);
        Map<String, HugeGraphSupplier> cache = (Map<String, HugeGraphSupplier>) cacheField.get(null);
        Map<String, HugeGraphSupplier> previousCache = new HashMap<>(cache);
        HugeGraphSupplier supplier = (HugeGraphSupplier) Proxy.newProxyInstance(
                HugeGraphSupplier.class.getClassLoader(), new Class<?>[]{HugeGraphSupplier.class},
                (proxy, method, args) -> null);
        TrackingKvClient client = new TrackingKvClient();
        SchemaDriver driver = new SchemaDriver(client, 10, 60_000L);
        this.instance.set(driver);
        try {
            cache.put("space/graph", supplier);
            BusinessHandlerImpl.clearCache();
            Assert.assertTrue(cache.isEmpty());
            Assert.assertFalse(client.closed);
            Assert.assertSame(driver, SchemaDriver.getInstance());

            cache.put("space/graph", supplier);
            BusinessHandlerImpl.closeSchemaResources();
            Assert.assertTrue(client.closed);
            Assert.assertNull(SchemaDriver.getInstance());
            Assert.assertTrue(cache.isEmpty());
            BusinessHandlerImpl.closeSchemaResources();

            TrackingKvClient replacementClient = new TrackingKvClient();
            this.instance.set(new SchemaDriver(replacementClient, 10, 60_000L));
            BusinessHandlerImpl.closeSchemaResources();
            Assert.assertTrue(replacementClient.closed);
        } finally {
            SchemaDriver.destroy();
            cache.putAll(previousCache);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testStoreCleanupReleasesWrappersWhenClientCloseFails() throws Exception {
        Field cacheField = BusinessHandlerImpl.class.getDeclaredField("GRAPH_SUPPLIER_CACHE");
        cacheField.setAccessible(true);
        Map<String, HugeGraphSupplier> cache = (Map<String, HugeGraphSupplier>) cacheField.get(null);
        Map<String, HugeGraphSupplier> previousCache = new HashMap<>(cache);
        HugeGraphSupplier supplier = (HugeGraphSupplier) Proxy.newProxyInstance(
                HugeGraphSupplier.class.getClassLoader(), new Class<?>[]{HugeGraphSupplier.class},
                (proxy, method, args) -> null);
        TrackingKvClient client = new TrackingKvClient() {
            @Override
            public void close() {
                super.close();
                throw new IllegalStateException("close failed");
            }
        };
        this.instance.set(new SchemaDriver(client, 10, 60_000L));
        try {
            cache.put("space/graph", supplier);
            try {
                BusinessHandlerImpl.closeSchemaResources();
                Assert.fail("Client close failure must propagate");
            } catch (IllegalStateException expected) {
                Assert.assertEquals("close failed", expected.getMessage());
            }
            Assert.assertTrue(client.closed);
            Assert.assertNull(SchemaDriver.getInstance());
            Assert.assertTrue(cache.isEmpty());
            BusinessHandlerImpl.closeSchemaResources();
        } finally {
            SchemaDriver.destroy();
            cache.putAll(previousCache);
        }
    }

    @Test
    public void testClosingStoreRejectsNewGraphSupplierWithoutOpeningPdClient() {
        HgStoreEngine engine = HgStoreEngine.getInstance();
        boolean previousClosing = engine.isClosing().getAndSet(true);
        try {
            BusinessHandlerImpl.getGraphSupplier("schema-driver-test/closing");
            Assert.fail("Closing Store must reject schema client creation");
        } catch (HgStoreException expected) {
            Assert.assertEquals("Store is closing", expected.getMessage());
            Assert.assertNull(SchemaDriver.getInstance());
        } finally {
            engine.isClosing().set(previousClosing);
        }
    }

    @Test
    public void testSchemaLookupCachesByIdAndNameAndWatchClearsOnlyTargetGraph() {
        CachingKvClient client = new CachingKvClient();
        SchemaDriver driver = new SchemaDriver(client, 10, 60_000L);
        this.instance.set(driver);
        try {
            Assert.assertEquals(4, client.listeners.size());
            PropertyKey first = driver.propertyKey("space", "graph", IdGenerator.of(1), null);
            Assert.assertSame(first, driver.propertyKey("space", "graph", "name", null));
            PropertyKey other = driver.propertyKey("space", "other", IdGenerator.of(1), null);
            Assert.assertEquals(2, client.getCalls);

            client.publish("HUGEGRAPH/hg/EVENT/GRAPH/SCHEMA/CLEAR", "space-graph");
            Assert.assertNotSame(first, driver.propertyKey("space", "graph", IdGenerator.of(1), null));
            Assert.assertSame(other, driver.propertyKey("space", "other", IdGenerator.of(1), null));
            Assert.assertEquals(3, client.getCalls);
        } finally {
            SchemaDriver.destroy();
        }
    }

    @Test
    public void testSchemaClearJsonFromServerEvictsOnlyNamedGraphAndKeepsLegacyFormat() {
        CachingKvClient client = new CachingKvClient();
        SchemaDriver driver = new SchemaDriver(client, "alternate", 10, 60_000L);
        this.instance.set(driver);
        String eventKey = "HUGEGRAPH/alternate/EVENT/GRAPH/SCHEMA/CLEAR";
        try {
            PropertyKey first = driver.propertyKey("space", "graph-with-hyphen", IdGenerator.of(1), null);
            PropertyKey other = driver.propertyKey("space", "other", IdGenerator.of(1), null);
            // Current Server CachedSchemaTransactionV2 publishes a source-bearing JSON event.
            client.publish(eventKey, "{\"graph\":\"space-graph-with-hyphen\",\"source\":\"server-process-1\"}");
            PropertyKey refreshed = driver.propertyKey("space", "graph-with-hyphen", IdGenerator.of(1), null);
            Assert.assertNotSame(first, refreshed);
            Assert.assertSame(other, driver.propertyKey("space", "other", IdGenerator.of(1), null));
            Assert.assertEquals(3, client.getCalls);

            client.publish(eventKey, "space-graph-with-hyphen");
            Assert.assertNotSame(refreshed,
                                 driver.propertyKey("space", "graph-with-hyphen", IdGenerator.of(1), null));
            Assert.assertSame(other, driver.propertyKey("space", "other", IdGenerator.of(1), null));
            Assert.assertEquals(4, client.getCalls);
        } finally {
            SchemaDriver.destroy();
        }
    }

    @Test
    public void testMalformedSchemaClearPayloadDoesNotInvalidateUnrelatedCaches() {
        CachingKvClient client = new CachingKvClient();
        SchemaDriver driver = new SchemaDriver(client, 10, 60_000L);
        this.instance.set(driver);
        String eventKey = "HUGEGRAPH/hg/EVENT/GRAPH/SCHEMA/CLEAR";
        try {
            PropertyKey cached = driver.propertyKey("space", "graph", IdGenerator.of(1), null);
            for (String payload : new String[]{"{broken", "{\"source\":\"server-process-1\"}", "{\"graph\":null}"}) {
                client.publish(eventKey, payload);
            }
            Assert.assertSame(cached, driver.propertyKey("space", "graph", IdGenerator.of(1), null));
            Assert.assertEquals(1, client.getCalls);
        } finally {
            SchemaDriver.destroy();
        }
    }

    @Test
    public void testGraphAndGraphSpaceRemovalInvalidateSchemaCaches() {
        CachingKvClient client = new CachingKvClient();
        SchemaDriver driver = new SchemaDriver(client, 10, 60_000L);
        this.instance.set(driver);
        try {
            PropertyKey first = driver.propertyKey("space", "graph", IdGenerator.of(1), null);
            PropertyKey other = driver.propertyKey("space", "other", IdGenerator.of(1), null);
            PropertyKey elsewhere = driver.propertyKey("elsewhere", "graph", IdGenerator.of(1), null);

            client.publish("HUGEGRAPH/hg/EVENT/GRAPH/REMOVE", "space-graph");
            Assert.assertNotSame(first, driver.propertyKey("space", "graph", IdGenerator.of(1), null));
            Assert.assertSame(other, driver.propertyKey("space", "other", IdGenerator.of(1), null));
            Assert.assertEquals(4, client.getCalls);

            client.publish("HUGEGRAPH/hg/EVENT/GRAPHSPACE/REMOVE", "space");
            Assert.assertNotSame(other, driver.propertyKey("space", "other", IdGenerator.of(1), null));
            Assert.assertSame(elsewhere, driver.propertyKey("elsewhere", "graph", IdGenerator.of(1), null));
            Assert.assertEquals(5, client.getCalls);
        } finally {
            SchemaDriver.destroy();
        }
    }

    @Test
    public void testMetadataKeysAndWatchSubscriptionsUseConfiguredCluster() {
        CachingKvClient client = new CachingKvClient();
        SchemaDriver driver = new SchemaDriver(client, "alternate", 10, 60_000L);
        this.instance.set(driver);
        try {
            Assert.assertEquals(4, client.listeners.size());
            for (String key : client.listeners.keySet()) {
                Assert.assertTrue(key, key.startsWith("HUGEGRAPH/alternate/EVENT/"));
            }
            SchemaGraph graph = new SchemaGraph("DEFAULT", "hugegraph", PDConfig.of("127.0.0.1:8686"),
                                                "alternate");
            Assert.assertEquals("HUGEGRAPH/alternate/GRAPHSPACE/DEFAULT/GRAPH_CONF/hugegraph",
                                client.lastGetKey);
            graph.propertyKey(IdGenerator.of(1));
            Assert.assertEquals("HUGEGRAPH/alternate/GRAPHSPACE/DEFAULT/hugegraph/SCHEMA/PROPERTY_KEY/ID/1",
                                client.lastGetKey);
            driver.propertyKeys("DEFAULT", "hugegraph", graph);
            Assert.assertEquals("HUGEGRAPH/alternate/GRAPHSPACE/DEFAULT/hugegraph/SCHEMA/PROPERTY_KEY/NAME",
                                client.lastScanPrefix);
        } finally {
            SchemaDriver.destroy();
        }
    }

    @Test
    public void testStoreSupplierMapsRpcNamespaceToSchemaGraphAndConfiguredCluster() throws Exception {
        CachingKvClient client = new CachingKvClient();
        this.instance.set(new SchemaDriver(client, "alternate", 10, 60_000L));
        HgStoreEngine engine = HgStoreEngine.getInstance();
        Field optionsField = HgStoreEngine.class.getDeclaredField("options");
        optionsField.setAccessible(true);
        Object previousOptions = optionsField.get(engine);
        boolean previousClosing = engine.isClosing().getAndSet(false);
        Field cacheField = BusinessHandlerImpl.class.getDeclaredField("GRAPH_SUPPLIER_CACHE");
        cacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, HugeGraphSupplier> cache = (Map<String, HugeGraphSupplier>) cacheField.get(null);
        Map<String, HugeGraphSupplier> previousCache = new HashMap<>(cache);
        HgStoreEngineOptions options = new HgStoreEngineOptions();
        options.setPdAddress("127.0.0.1:8686");
        options.setPdCluster("alternate");
        optionsField.set(engine, options);
        cache.clear();
        try {
            HugeGraphSupplier graph = BusinessHandlerImpl.getGraphSupplier("CODEC_TEST/row_graph/g");
            Assert.assertEquals("CODEC_TEST-row_graph", graph.name());
            Assert.assertEquals("HUGEGRAPH/alternate/GRAPHSPACE/CODEC_TEST/GRAPH_CONF/row_graph",
                                client.lastGetKey);
            graph.propertyKey(IdGenerator.of(1));
            Assert.assertEquals("HUGEGRAPH/alternate/GRAPHSPACE/CODEC_TEST/row_graph/SCHEMA/PROPERTY_KEY/ID/1",
                                client.lastGetKey);
            Assert.assertSame(graph, BusinessHandlerImpl.getGraphSupplier("CODEC_TEST/row_graph/g"));
            for (String invalid : new String[]{"plain", "/row_graph/g", "CODEC_TEST//g"}) {
                try {
                    BusinessHandlerImpl.getGraphSupplier(invalid);
                    Assert.fail("Invalid graph namespace must be rejected: " + invalid);
                } catch (HgStoreException expected) {
                    Assert.assertTrue(expected.getMessage().contains("Graph must include"));
                }
            }
        } finally {
            BusinessHandlerImpl.closeSchemaResources();
            cache.putAll(previousCache);
            optionsField.set(engine, previousOptions);
            engine.isClosing().set(previousClosing);
        }
    }

    @Test
    public void testSharedDriverRejectsConflictingClusterAndReusesMatchingCluster() {
        CachingKvClient client = new CachingKvClient();
        SchemaDriver driver = new SchemaDriver(client, "alternate", 10, 60_000L);
        this.instance.set(driver);
        PDConfig config = PDConfig.of("127.0.0.1:8686");
        try {
            Assert.assertSame(driver, SchemaDriver.getOrInit(config, "alternate"));
            try {
                SchemaDriver.getOrInit(config, "hg");
                Assert.fail("Conflicting metadata cluster must fail before reusing the driver");
            } catch (NotAllowException expected) {
                Assert.assertTrue(expected.getMessage().contains("alternate"));
            }
            Assert.assertSame(driver, SchemaDriver.getInstance());
        } finally {
            SchemaDriver.destroy();
        }
    }

    @SuppressWarnings("unchecked")
    private static AtomicReference<SchemaDriver> instanceReference() throws Exception {
        Field field = SchemaDriver.class.getDeclaredField("INSTANCE");
        field.setAccessible(true);
        return (AtomicReference<SchemaDriver>) field.get(null);
    }

    private static class TrackingKvClient extends KvClient<WatchResponse> {

        protected volatile boolean closed;

        TrackingKvClient() {
            super(PDConfig.of("127.0.0.1:8686"));
        }

        @Override
        public void listen(String key, Consumer<WatchResponse> consumer) throws PDException {
            // Avoid opening a real PD stream while constructing SchemaDriver.
        }

        @Override
        public void close() {
            this.closed = true;
            super.close();
        }
    }

    private static class CachingKvClient extends TrackingKvClient {

        private final Map<String, Consumer<WatchResponse>> listeners = new HashMap<>();
        private int getCalls;
        private String lastGetKey;
        private String lastScanPrefix;

        @Override
        public void listen(String key, Consumer<WatchResponse> consumer) {
            this.listeners.put(key, consumer);
        }

        @Override
        public KResponse get(String key) {
            this.getCalls++;
            this.lastGetKey = key;
            return KResponse.newBuilder().setValue("{\"id\":1,\"name\":\"name\"}").build();
        }

        @Override
        public ScanPrefixResponse scanPrefix(String prefix) {
            this.lastScanPrefix = prefix;
            return ScanPrefixResponse.getDefaultInstance();
        }

        private void publish(String key, String graph) {
            WatchEvent event = WatchEvent.newBuilder()
                                         .setType(WatchType.Put)
                                         .setCurrent(WatchKv.newBuilder().setValue(graph))
                                         .build();
            this.listeners.get(key).accept(WatchResponse.newBuilder().addEvents(event).build());
        }
    }

    private static class BlockingCloseKvClient extends TrackingKvClient {

        private final CountDownLatch closeStarted = new CountDownLatch(1);
        private final CountDownLatch allowClose = new CountDownLatch(1);

        @Override
        public void close() {
            this.closeStarted.countDown();
            try {
                this.allowClose.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Client close was interrupted", e);
            }
            super.close();
        }

        private void awaitCloseStarted() throws InterruptedException {
            Assert.assertTrue(this.closeStarted.await(5L, TimeUnit.SECONDS));
        }
    }

    private static class FailingListenKvClient extends TrackingKvClient {

        private int listenCalls;

        @Override
        public void listen(String key, Consumer<WatchResponse> consumer) throws PDException {
            if (++this.listenCalls == 2) {
                throw new PDException(-1, "listener startup failed");
            }
        }
    }
}
