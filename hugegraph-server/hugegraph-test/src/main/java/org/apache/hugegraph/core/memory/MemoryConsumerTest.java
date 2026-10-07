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

package org.apache.hugegraph.core.memory;

import java.nio.file.Paths;
import java.util.UUID;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.FileHandler;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.StandardHugeGraph;
import org.apache.hugegraph.backend.cache.CachedBackendStore;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.backend.BinaryId;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.memory.consumer.OffHeapObject;
import org.apache.hugegraph.memory.consumer.factory.IdFactory;
import org.apache.hugegraph.memory.consumer.factory.PropertyFactory;
import org.apache.hugegraph.memory.consumer.impl.id.StringIdOffHeap;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeEdgeProperty;
import org.apache.hugegraph.structure.HugeProperty;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Directions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.util.IllegalReferenceCountException;

public class MemoryConsumerTest extends MemoryManageTest {

    private static HugeGraph graph;
    private static boolean registered = false;

    public static HugeGraph buildGraph() throws ConfigurationException {
        String projectRoot = Paths.get("").toAbsolutePath().getParent().getParent().toString();
        String CONF = projectRoot + "/hugegraph-commons/hugegraph-common/src/test/java/org/apache" +
                      "/hugegraph/unit/config/test.conf";
        PropertiesConfiguration configuration = new PropertiesConfiguration();
        FileHandler fileHandler = new FileHandler(configuration);
        fileHandler.load(CONF);
        HugeConfig config = new HugeConfig(configuration);
        return new StandardHugeGraph(config);
    }

    @BeforeClass
    public static void setup() throws ConfigurationException {
        graph = buildGraph();
        graph.clearBackend();
        graph.initBackend();
        graph.serverStarted(GlobalMasterInfo.master("server-test"));
        if (registered) {
            return;
        }
        RegisterUtil.registerBackends();
        registered = true;

        SchemaManager schema = graph.schema();
        schema.propertyKey("name").asText().create();
        schema.vertexLabel("book")
              .properties("name")
              .primaryKeys("name")
              .enableLabelIndex(false)
              .create();
    }

    @AfterClass
    public static void clear() throws Exception {
        if (graph == null) {
            return;
        }

        try {
            graph.clearBackend();
        } finally {
            graph.close();
            graph = null;
        }
    }

    @Test
    public void testStringId() {
        Id stringIdOffHeap = IdFactory.getInstance().newStringId("java");
        Id stringId = IdGenerator.of("java");
        Assert.assertNotNull(stringIdOffHeap);
        Assert.assertEquals("java", stringIdOffHeap.asString());
        Assert.assertEquals(stringId, ((OffHeapObject) stringIdOffHeap).zeroCopyReadFromByteBuf());
        // Test release memoryBlock
        memoryManager.getCorrespondingTaskMemoryPool(Thread.currentThread().getName())
                     .releaseSelf("test", false);
        Assert.assertThrows(IllegalReferenceCountException.class,
                            ((StringIdOffHeap) stringIdOffHeap).getIdOffHeap()::memoryAddress);
    }

    @Test
    public void testLongId() {
        Id idOffHeap = IdFactory.getInstance().newLongId(1);
        Id id = IdGenerator.of(1);
        Assert.assertNotNull(idOffHeap);
        Assert.assertEquals(1, idOffHeap.asLong());
        Assert.assertEquals(id, ((OffHeapObject) idOffHeap).zeroCopyReadFromByteBuf());
    }

    @Test
    public void testUuidId() {
        UUID uuidEncoding = UUID.randomUUID();
        Id idOffHeap = IdFactory.getInstance().newUuidId(uuidEncoding);
        Id id = IdGenerator.of(uuidEncoding);
        Assert.assertNotNull(idOffHeap);
        Assert.assertArrayEquals(id.asBytes(), idOffHeap.asBytes());
        Assert.assertEquals(id, ((OffHeapObject) idOffHeap).zeroCopyReadFromByteBuf());
    }

    @Test
    public void testBinaryId() {
        Id stringIdOffHeap = IdFactory.getInstance().newStringId("java");
        Id idOffHeap =
                IdFactory.getInstance().newBinaryId(stringIdOffHeap.asBytes(), stringIdOffHeap);
        Id id = new BinaryId(stringIdOffHeap.asBytes(), stringIdOffHeap);
        Assert.assertNotNull(idOffHeap);
        Assert.assertArrayEquals(stringIdOffHeap.asBytes(), idOffHeap.asBytes());
        Assert.assertEquals(id, ((OffHeapObject) idOffHeap).zeroCopyReadFromByteBuf());
    }

    @Test
    public void testObjectId() {
        TestObject object = new TestObject();
        object.x = 1;
        object.y = "test";
        Id idOffHeap =
                IdFactory.getInstance().newObjectId(object);
        Id id = new IdGenerator.ObjectId(object);
        Assert.assertNotNull(idOffHeap);
        Assert.assertEquals(id.asObject(), idOffHeap.asObject());
        Assert.assertEquals(id, ((OffHeapObject) idOffHeap).zeroCopyReadFromByteBuf());
    }

    @Test
    public void testQueryId() {
        Query q = new Query(HugeType.VERTEX);
        Id idOffHeap =
                IdFactory.getInstance().newQueryId(q);
        Id id = new CachedBackendStore.QueryId(q);
        Assert.assertNotNull(idOffHeap);
        Assert.assertEquals(id.toString(), idOffHeap.toString());
        Assert.assertEquals(id, ((OffHeapObject) idOffHeap).zeroCopyReadFromByteBuf());
    }

    @Test
    public void testEdgeId() {
        Id stringIdOffHeap = IdFactory.getInstance().newStringId("java");
        HugeVertex java = new HugeVertex(graph, stringIdOffHeap, graph.vertexLabel("book"));
        Id edgeLabelIdOffHeap = IdFactory.getInstance().newLongId(1);
        Id subLabelIdOffHeap = IdFactory.getInstance().newLongId(2);
        Id edgeIdOffHeap =
                IdFactory.getInstance().newEdgeId(java.id(), Directions.OUT, edgeLabelIdOffHeap,
                                                  subLabelIdOffHeap,
                                                  "test", java.id());
        Id edgeId = new EdgeId(java.id(),
                               Directions.OUT,
                               (Id) ((OffHeapObject) edgeLabelIdOffHeap).zeroCopyReadFromByteBuf(),
                               (Id) ((OffHeapObject) subLabelIdOffHeap).zeroCopyReadFromByteBuf(),
                               "test",
                               java.id());
        Assert.assertNotNull(edgeIdOffHeap);
        // TODO: adopt equals method
        Assert.assertEquals(edgeId.asString(), edgeIdOffHeap.asString());
    }

    @Test
    public void testEdgeIdFormattingUsesSharedComponentsAndCache() {
        Id owner = IdFactory.getInstance().newStringId("owner>vertex");
        Id other = IdFactory.getInstance().newStringId("other`vertex");
        Id label = IdFactory.getInstance().newLongId(1);
        Id subLabel = IdFactory.getInstance().newLongId(2);
        String[] sortValues = {"value", "a>b`c!d:北京"};
        for (Directions direction : new Directions[]{Directions.OUT, Directions.IN}) {
            for (boolean directed : new boolean[]{false, true}) {
                for (String sortValue : sortValues) {
                    EdgeId offHeap = IdFactory.getInstance().newEdgeId(
                            owner, direction, label, subLabel, sortValue, other, directed);
                    EdgeId onHeap = new EdgeId(IdGenerator.of("owner>vertex"), direction,
                                               IdGenerator.of(1), IdGenerator.of(2), sortValue,
                                               IdGenerator.of("other`vertex"), directed);
                    OffHeapObject consumer = (OffHeapObject) offHeap;
                    int memoryBlocks = consumer.getAllMemoryBlock().size();
                    String formatted = offHeap.asString();
                    Assert.assertEquals(onHeap.asString(), formatted);
                    Assert.assertEquals(memoryBlocks + 1, consumer.getAllMemoryBlock().size());
                    Assert.assertEquals(formatted, offHeap.asString());
                    Assert.assertEquals(memoryBlocks + 1, consumer.getAllMemoryBlock().size());
                    Assert.assertArrayEquals(onHeap.asBytes(), offHeap.asBytes());
                    Assert.assertEquals(sortValue, offHeap.sortValues());
                }
            }
        }
    }

    @Test
    public void testProperty() {
        Id stringIdOffHeap = IdFactory.getInstance().newStringId("java");
        HugeVertex java = new HugeVertex(graph, stringIdOffHeap, graph.vertexLabel("book"));
        Id edgeLabelIdOffHeap = IdFactory.getInstance().newLongId(1);
        Id subLabelIdOffHeap = IdFactory.getInstance().newLongId(2);

        Id edgeId = new EdgeId(java.id(),
                               Directions.OUT,
                               (Id) ((OffHeapObject) edgeLabelIdOffHeap).zeroCopyReadFromByteBuf(),
                               (Id) ((OffHeapObject) subLabelIdOffHeap).zeroCopyReadFromByteBuf(),
                               "test",
                               java.id());
        HugeEdge testEdge = new HugeEdge(graph, edgeId, EdgeLabel.NONE);
        PropertyKey propertyKey = new PropertyKey(null, IdFactory.getInstance().newLongId(3),
                                                  "fake");

        String propertyValue = "test";
        HugeProperty<String> propertyOffHeap =
                PropertyFactory.getInstance(String.class).newHugeEdgeProperty(testEdge,
                                                                              propertyKey,
                                                                              propertyValue);
        HugeEdgeProperty<String> property = new HugeEdgeProperty<>(testEdge,
                                                                   propertyKey,
                                                                   propertyValue);
        Assert.assertNotNull(propertyOffHeap);
        Assert.assertEquals(property.value(), propertyOffHeap.value());
        Assert.assertEquals(property, propertyOffHeap);
    }

    static class TestObject {

        long x;
        String y;

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TestObject)) {
                return false;
            }
            TestObject other = (TestObject) obj;
            return this.x == other.x && this.y.equals(other.y);
        }
    }
}
