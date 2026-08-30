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

package org.apache.hugegraph.tinkerpop;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration2.Configuration;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.store.BackendStoreInfo;
import org.apache.hugegraph.io.HugeGraphIoRegistry;
import org.apache.hugegraph.io.HugeGraphSONModule;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io;

import com.google.common.collect.ImmutableSet;

@Graph.OptIn("org.apache.hugegraph.tinkerpop.StructureBasicSuite")
@Graph.OptIn("org.apache.hugegraph.tinkerpop.ProcessBasicSuite")
@Graph.OptIn("org.apache.hugegraph.tinkerpop.StructurePerformanceSuite")
@Graph.OptIn("org.apache.hugegraph.tinkerpop.ProcessPerformanceSuite")
public class TestGraph implements Graph {

    public static final String DEFAULT_VL = "vertex";

    public static final Set<String> TRUNCATE_BACKENDS = ImmutableSet.of("rocksdb");

    private static volatile int id = 666;

    private HugeGraph graph;
    private String loadedGraph = null;
    private boolean initedBackend = false;
    private boolean isLastIdCustomized = false;
    private boolean autoPerson = false;
    private boolean ioTest = false;

    public TestGraph(HugeGraph graph) {
        this.graph = graph;
    }

    public HugeGraph hugegraph() {
        return this.graph;
    }

    @Watched
    protected void initBackend() {
        BackendStoreInfo sysInfo = this.graph.backendStoreInfo();
        if (!sysInfo.exists()) {
            this.graph.initBackend();
        } else {
            // May reopen a closed graph
            assert sysInfo.exists() && !this.graph.closed();
        }

        this.graph.serverStarted(GlobalMasterInfo.master("server-tinkerpop"));

        this.initedBackend = true;
    }

    @Watched
    protected void clearBackend() {
        this.graph.clearBackend();
        this.initedBackend = false;
    }

    @Watched
    protected void clearAll(String testClass) {
        List<PropertyKey> pks = this.graph.schema().getPropertyKeys();
        if (pks.isEmpty()) {
            // No need to clear if there is no PKs(that's no schema and data)
            return;
        }

        if (TRUNCATE_BACKENDS.contains(this.graph.backend())) {
            // Delete all data by truncating tables
            this.truncateBackend();
        } else {
            // Clear schema (also include data)
            this.clearSchema();

            // Clear variables if needed (would not clear when clearing schema)
            if (testClass.endsWith("VariableAsMapTest")) {
                this.clearVariables();
                this.tx().commit();
            }
        }
    }

    @Watched
    protected void truncateBackend() {
        this.graph.truncateBackend();
    }

    @Watched
    protected void clearSchema() {
        // Clear schema and graph data will be cleared at same time
        SchemaManager schema = this.graph.schema();

        schema.getIndexLabels().forEach(elem -> {
            schema.indexLabel(elem.name()).remove();
        });

        schema.getEdgeLabels().forEach(elem -> {
            schema.edgeLabel(elem.name()).remove();
        });

        schema.getVertexLabels().forEach(elem -> {
            schema.vertexLabel(elem.name()).remove();
        });

        schema.getPropertyKeys().forEach(elem -> {
            schema.propertyKey(elem.name()).remove();
        });

        TaskScheduler scheduler = this.graph.taskScheduler();
        scheduler.tasks(null, -1, null).forEachRemaining(elem -> {
            scheduler.delete(elem.id(), false);
        });
    }

    @Watched
    protected void clearVariables() {
        Variables variables = this.variables();
        variables.keys().forEach(variables::remove);
    }

    protected boolean closed() {
        return this.graph.closed();
    }

    @Watched
    @Override
    public Vertex addVertex(Object... keyValues) {
        boolean needRedefineSchema = false;
        boolean hasId = false;
        IdStrategy idStrategy = IdStrategy.AUTOMATIC;
        String defaultVL = DEFAULT_VL;

        for (int i = 0; i < keyValues.length; i += 2) {
            if (keyValues[i] == null) {
                continue;
            }

            if (keyValues[i].equals(T.id)) {
                hasId = true;
            }

            if (keyValues[i].equals(T.label) &&
                i + 1 < keyValues.length &&
                "person".equals(keyValues[i + 1]) &&
                this.loadedGraph == null &&
                !this.autoPerson) {
                needRedefineSchema = true;
                defaultVL = "person";
            }
        }

        if (needRedefineSchema && this.loadedGraph == null) {
            this.clearSchema();
            this.tx().commit();
            this.initBasicSchema(idStrategy, defaultVL);
            this.tx().commit();
            if (!this.autoPerson &&
                "person".equals(defaultVL) &&
                idStrategy == IdStrategy.AUTOMATIC) {
                this.autoPerson = true;
            }

            this.isLastIdCustomized = idStrategy == IdStrategy.CUSTOMIZE_STRING;
        }

        if (!hasId && (this.isLastIdCustomized || needAddIdToLoadGraph())) {
            List<Object> kvs = new ArrayList<>(Arrays.asList(keyValues));
            kvs.add(T.id);
            kvs.add(String.valueOf(id++));
            keyValues = kvs.toArray();
        }

        return this.graph.addVertex(keyValues);
    }

    @Watched
    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        return this.graph.vertices(vertexIds);
    }

    @Watched
    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        return this.graph.edges(edgeIds);
    }

    @Override
    public Transaction tx() {
        return this.graph.tx();
    }

    @Watched
    @Override
    public void close() throws Exception {
        this.graph.close();
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass)
            throws IllegalArgumentException {
        return this.graph.compute(graphComputerClass);
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return this.graph.compute();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        Whitebox.setInternalState(HugeGraphSONModule.class, "OPTIMIZE_SERIALIZE", false);
        return (I) builder.graph(this).onMapper(mapper ->
                                                        mapper.addRegistry(
                                                                HugeGraphIoRegistry.instance())
        ).create();
    }

    @Override
    public Variables variables() {
        return this.graph.variables();
    }

    @Override
    public Configuration configuration() {
        return this.graph.configuration();
    }

    @Override
    public Features features() {
        return this.graph.features();
    }

    @Override
    public String toString() {
        return this.graph.toString();
    }

    public void loadedGraph(String loadedGraph) {
        this.loadedGraph = loadedGraph;
    }

    public String loadedGraph() {
        return this.loadedGraph;
    }

    public boolean initedBackend() {
        return this.initedBackend;
    }

    public void autoPerson(boolean autoPerson) {
        this.autoPerson = autoPerson;
    }

    public void ioTest(boolean ioTest) {
        this.ioTest = ioTest;
    }

    public boolean ioTest() {
        return this.ioTest;
    }

    private boolean needAddIdToLoadGraph() {
        if (this.graph.name().endsWith("standard") ||
            this.loadedGraph == null) {
            return false;
        }
        switch (this.loadedGraph) {
            case "gryo":
            case "graphsonv2d0":
                return true;
            case "graphml":
            case "graphsonv1d0":
            case "regularLoad":
                return false;
            default:
                throw new AssertionError(String.format("Wrong IO type %s", this.loadedGraph));
        }
    }

    @Watched
    public void initPropertyKey(String key, String type) {
        SchemaManager schema = this.graph.schema();

        switch (type) {
            case "Boolean":
                schema.propertyKey(key).asBoolean().ifNotExist().create();
                break;
            case "Integer":
                schema.propertyKey(key).asInt().ifNotExist().create();
                break;
            case "Long":
                schema.propertyKey(key).asLong().ifNotExist().create();
                break;
            case "Float":
                schema.propertyKey(key).asFloat().ifNotExist().create();
                break;
            case "Double":
                schema.propertyKey(key).asDouble().ifNotExist().create();
                break;
            case "String":
                schema.propertyKey(key).ifNotExist().create();
                break;
            case "BooleanArray":
                schema.propertyKey(key).asBoolean().valueList().ifNotExist().create();
                break;
            case "IntegerArray":
                schema.propertyKey(key).asInt().valueList().ifNotExist().create();
                break;
            case "LongArray":
                schema.propertyKey(key).asLong().valueList().ifNotExist().create();
                break;
            case "FloatArray":
                schema.propertyKey(key).asFloat().valueList().ifNotExist().create();
                break;
            case "DoubleArray":
                schema.propertyKey(key).asDouble().valueList().ifNotExist().create();
                break;
            case "StringArray":
                schema.propertyKey(key).valueList().ifNotExist().create();
                break;
            case "UniformList":
                schema.propertyKey(key).valueList().ifNotExist().create();
                break;
            case "MixedList":
            case "Map":
            case "Serializable":
                break;
            default:
                throw new RuntimeException(String.format("Wrong type %s for %s", type, key));
        }
    }

    @Watched
    public void initGratefulSchema(IdStrategy idStrategy) {
        SchemaManager schema = this.graph.schema();

        schema.propertyKey("id").asInt().ifNotExist().create();
        schema.propertyKey("weight").asInt().ifNotExist().create();
        schema.propertyKey("name").ifNotExist().create();
        schema.propertyKey("songType").asText().ifNotExist().create();
        schema.propertyKey("performances").asInt().ifNotExist().create();

        switch (idStrategy) {
            case AUTOMATIC:
                schema.vertexLabel("song")
                      .properties("id", "name", "songType", "performances")
                      .nullableKeys("id", "name", "songType", "performances")
                      .ifNotExist().create();
                schema.vertexLabel("artist")
                      .properties("id", "name")
                      .nullableKeys("id", "name")
                      .ifNotExist().create();
                break;
            case CUSTOMIZE_STRING:
                schema.vertexLabel("song")
                      .properties("id", "name", "songType", "performances")
                      .nullableKeys("id", "name", "songType", "performances")
                      .useCustomizeStringId().ifNotExist().create();
                schema.vertexLabel("artist")
                      .properties("id", "name")
                      .nullableKeys("id", "name")
                      .useCustomizeStringId().ifNotExist().create();
                break;
            default:
                throw new AssertionError("Id strategy must be customize or automatic");
        }

        schema.edgeLabel("followedBy")
              .link("song", "song")
              .properties("weight")
              .nullableKeys("weight")
              .ifNotExist().create();
        schema.edgeLabel("sungBy").link("song", "artist")
              .ifNotExist().create();
        schema.edgeLabel("writtenBy").link("song", "artist")
              .ifNotExist().create();

        schema.indexLabel("songByName").onV("song").by("name")
              .ifNotExist().create();
        schema.indexLabel("songBySongType").onV("song").by("songType")
              .ifNotExist().create();
        schema.indexLabel("songByPerf").onV("song").by("performances")
              .ifNotExist().create();
        schema.indexLabel("artistByName").onV("artist").by("name")
              .ifNotExist().create();
        schema.indexLabel("followedByByWeight").onE("followedBy").by("weight")
              .range().ifNotExist().create();
    }

    @Watched
    public void initModernSchema(IdStrategy idStrategy) {
        SchemaManager schema = this.graph.schema();

        schema.propertyKey("weight").asDouble().ifNotExist().create();
        schema.propertyKey("name").ifNotExist().create();
        schema.propertyKey("lang").ifNotExist().create();
        schema.propertyKey("age").asInt().ifNotExist().create();
        schema.propertyKey("year").asInt().ifNotExist().create();
        schema.propertyKey("acl").ifNotExist().create();
        schema.propertyKey("temp").ifNotExist().create();
        schema.propertyKey("peter").ifNotExist().create();
        schema.propertyKey("vadas").ifNotExist().create();
        schema.propertyKey("josh").ifNotExist().create();
        schema.propertyKey("marko").ifNotExist().create();
        schema.propertyKey("ripple").ifNotExist().create();
        schema.propertyKey("lop").ifNotExist().create();
        schema.propertyKey("test").ifNotExist().create();
        schema.propertyKey("p").ifNotExist().create();

        switch (idStrategy) {
            case AUTOMATIC:
                schema.vertexLabel("name")
                      .ifNotExist().create();
                schema.vertexLabel("person")
                      .properties("name", "age", "test")
                      .nullableKeys("name", "age", "test")
                      .ifNotExist().create();
                schema.vertexLabel("software")
                      .properties("name", "lang", "temp")
                      .nullableKeys("name", "lang", "temp")
                      .ifNotExist().create();
                schema.vertexLabel("dog")
                      .properties("name")
                      .nullableKeys("name")
                      .ifNotExist().create();
                schema.vertexLabel(DEFAULT_VL)
                      .properties("name", "age", "p")
                      .nullableKeys("name", "age", "p")
                      .ifNotExist().create();
                schema.vertexLabel("animal")
                      .properties("name", "age", "peter", "josh", "marko",
                                  "vadas", "ripple", "lop")
                      .nullableKeys("name", "age", "peter", "josh", "marko",
                                    "vadas", "ripple", "lop")
                      .ifNotExist().create();
                break;
            case CUSTOMIZE_STRING:
                schema.vertexLabel("person")
                      .properties("name", "age")
                      .nullableKeys("name", "age")
                      .useCustomizeStringId().ifNotExist().create();
                schema.vertexLabel("software")
                      .properties("name", "lang")
                      .nullableKeys("name", "lang")
                      .useCustomizeStringId().ifNotExist().create();
                schema.vertexLabel("dog")
                      .properties("name")
                      .nullableKeys("name")
                      .useCustomizeStringId().ifNotExist().create();
                schema.vertexLabel(DEFAULT_VL)
                      .properties("name", "age", "p")
                      .nullableKeys("name", "age", "p")
                      .useCustomizeStringId().ifNotExist().create();
                schema.vertexLabel("animal")
                      .properties("name", "age")
                      .nullableKeys("name", "age")
                      .useCustomizeStringId().ifNotExist().create();
                break;
            default:
                throw new AssertionError("Id strategy must be customize or automatic");
        }

        schema.edgeLabel("knows").link("person", "person")
              .properties("weight", "year")
              .nullableKeys("weight", "year")
              .ifNotExist().create();
        schema.edgeLabel("created").link("person", "software")
              .properties("weight")
              .nullableKeys("weight")
              .ifNotExist().create();
        schema.edgeLabel("codeveloper").link("person", "person")
              .properties("year")
              .nullableKeys("year")
              .ifNotExist().create();
        schema.edgeLabel("createdBy").link("software", "person")
              .properties("weight", "year", "acl")
              .nullableKeys("weight", "year", "acl")
              .ifNotExist().create();
        schema.edgeLabel("uses").link("person", "software")
              .ifNotExist().create();
        schema.edgeLabel("likes").link("person", "software")
              .ifNotExist().create();
        schema.edgeLabel("foo").link("person", "software")
              .ifNotExist().create();
        schema.edgeLabel("bar").link("person", "software")
              .ifNotExist().create();

        schema.indexLabel("personByName").onV("person").by("name")
              .ifNotExist().create();
        schema.indexLabel("personByAge").onV("person").by("age").range()
              .ifNotExist().create();
        schema.indexLabel("softwareByName").onV("software").by("name")
              .ifNotExist().create();
        schema.indexLabel("softwareByLang").onV("software").by("lang")
              .ifNotExist().create();
        schema.indexLabel("dogByName").onV("dog").by("name")
              .ifNotExist().create();
        schema.indexLabel("vertexByName").onV("vertex").by("name")
              .ifNotExist().create();
        schema.indexLabel("vertexByAge").onV("vertex").by("age").range()
              .ifNotExist().create();
        schema.indexLabel("knowsByWeight").onE("knows").by("weight").range()
              .ifNotExist().create();
        schema.indexLabel("createdByWeight").onE("created").by("weight")
              .range().ifNotExist().create();
        schema.indexLabel("personByNameAge").onV("person").by("name", "age")
              .ifNotExist().create();
        schema.indexLabel("vertexByP").onV("vertex").by("p")
              .ifNotExist().create();
    }

    @Watched
    public void initClassicSchema(IdStrategy idStrategy) {
        SchemaManager schema = this.graph.schema();

        schema.propertyKey("id").asInt().ifNotExist().create();
        schema.propertyKey("weight").asFloat().ifNotExist().create();
        schema.propertyKey("name").ifNotExist().create();
        schema.propertyKey("lang").ifNotExist().create();
        schema.propertyKey("age").asInt().ifNotExist().create();

        switch (idStrategy) {
            case AUTOMATIC:
                schema.vertexLabel("vertex")
                      .properties("id", "name", "age", "lang")
                      .nullableKeys("id", "name", "age", "lang")
                      .ifNotExist().create();
                break;
            case CUSTOMIZE_STRING:
                schema.vertexLabel("vertex")
                      .properties("id", "name", "age", "lang")
                      .nullableKeys("id", "name", "age", "lang")
                      .useCustomizeStringId().ifNotExist().create();
                break;
            default:
                throw new AssertionError("Id strategy must be customize or automatic");
        }

        schema.edgeLabel("knows").link("vertex", "vertex")
              .properties("weight")
              .nullableKeys("weight")
              .ifNotExist().create();
        schema.edgeLabel("created").link("vertex", "vertex")
              .properties("weight")
              .nullableKeys("weight")
              .ifNotExist().create();

        schema.indexLabel("vertexByName").onV("vertex").by("name").secondary()
              .ifNotExist().create();
        schema.indexLabel("vertexByAge").onV("vertex").by("age").range()
              .ifNotExist().create();
    }

    @Watched
    public void initBasicSchema(IdStrategy idStrategy, String defaultVL) {
        this.initBasicPropertyKey();
        this.initBasicVertexLabelV(idStrategy, defaultVL);
        this.initBasicVertexLabelAndEdgeLabelExceptV(defaultVL);
    }

    @Watched
    private void initBasicPropertyKey() {
        SchemaManager schema = this.graph.schema();

        schema.propertyKey("__id").ifNotExist().create();
        schema.propertyKey("oid").asInt().ifNotExist().create();
        schema.propertyKey("communityIndex").asInt().ifNotExist().create();
        schema.propertyKey("test").ifNotExist().create();
        schema.propertyKey("testing").ifNotExist().create();
        schema.propertyKey("data").ifNotExist().create();
        schema.propertyKey("name").ifNotExist().create();
        schema.propertyKey("location").ifNotExist().create();
        schema.propertyKey("status").ifNotExist().create();
        schema.propertyKey("boolean").asBoolean().ifNotExist().create();
        schema.propertyKey("float").asFloat().ifNotExist().create();
        schema.propertyKey("since").asInt().ifNotExist().create();
        schema.propertyKey("double").asDouble().ifNotExist().create();
        schema.propertyKey("string").ifNotExist().create();
        schema.propertyKey("integer").asInt().ifNotExist().create();
        /*
         * The method shouldHaveStandardStringRepresentationForEdgeProperty()
         * in PropertyTest$BasicPropertyTest need 'short' property with
         * datatype String
         */
        schema.propertyKey("short").asText().ifNotExist().create();
        schema.propertyKey("x").asInt().ifNotExist().create();
        schema.propertyKey("y").asInt().ifNotExist().create();
        schema.propertyKey("age").asInt().ifNotExist().create();
        schema.propertyKey("lang").ifNotExist().create();
        schema.propertyKey("some").ifNotExist().create();
        schema.propertyKey("that").ifNotExist().create();
        schema.propertyKey("any").ifNotExist().create();
        schema.propertyKey("this").ifNotExist().create();
        schema.propertyKey("year").asInt().ifNotExist().create();
        schema.propertyKey("xxx").ifNotExist().create();
        schema.propertyKey("yyy").ifNotExist().create();
        schema.propertyKey("favoriteColor").ifNotExist().create();
        schema.propertyKey("uuid").asUUID().ifNotExist().create();
        schema.propertyKey("myId").asInt().ifNotExist().create();
        schema.propertyKey("myEdgeId").asInt().ifNotExist().create();
        schema.propertyKey("state").ifNotExist().create();
        schema.propertyKey("acl").ifNotExist().create();
        schema.propertyKey("stars").asInt().ifNotExist().create();
        schema.propertyKey("b").asBoolean().ifNotExist().create();
        schema.propertyKey("s").ifNotExist().create();
        schema.propertyKey("n").ifNotExist().create();
        schema.propertyKey("d").asDouble().ifNotExist().create();
        schema.propertyKey("f").asFloat().ifNotExist().create();
        schema.propertyKey("i").asInt().ifNotExist().create();
        schema.propertyKey("l").asLong().ifNotExist().create();
        schema.propertyKey("p").ifNotExist().create();
        schema.propertyKey("k").ifNotExist().create();
        schema.propertyKey("here").ifNotExist().create();
        schema.propertyKey("to-change").ifNotExist().create();
        schema.propertyKey("to-remove").ifNotExist().create();
        schema.propertyKey("to-keep").ifNotExist().create();
        schema.propertyKey("to-drop").ifNotExist().create();
        schema.propertyKey("dropped").ifNotExist().create();
        schema.propertyKey("not-dropped").ifNotExist().create();
        schema.propertyKey("old").ifNotExist().create();
        schema.propertyKey("new").ifNotExist().create();
        schema.propertyKey("color").ifNotExist().create();
        schema.propertyKey("every").ifNotExist().create();
        schema.propertyKey("gremlin.partitionGraphStrategy.partition").ifNotExist().create();
        schema.propertyKey("blah").asDouble().ifNotExist().create();
        schema.propertyKey("bloop").asInt().ifNotExist().create();

        if (!this.graph.existsPropertyKey("long")) {
            schema.propertyKey("long").asLong().ifNotExist().create();
        }
        if (!this.graph.existsPropertyKey("aKey")) {
            schema.propertyKey("aKey").asDouble().ifNotExist().create();
        }

        if (this.ioTest) {
            schema.propertyKey("weight").asFloat().ifNotExist().create();
        } else {
            schema.propertyKey("weight").asDouble().ifNotExist().create();
        }
    }

    @Watched
    private void initBasicVertexLabelV(IdStrategy idStrategy, String defaultVL) {
        SchemaManager schema = this.graph.schema();

        switch (idStrategy) {
            case CUSTOMIZE_STRING:
                this.isLastIdCustomized = true;
                schema.vertexLabel(defaultVL)
                      .properties("__id", "oid", "name", "state", "status",
                                  "some", "that", "any", "this", "lang", "b",
                                  "communityIndex", "test", "testing", "acl",
                                  "favoriteColor", "aKey", "age", "boolean",
                                  "float", "double", "string", "integer",
                                  "long", "myId", "location", "x", "y", "s",
                                  "n", "d", "f", "i", "l", "p", "k", "to-change",
                                  "to-remove", "to-keep", "old", "new",
                                  "gremlin.partitionGraphStrategy.partition",
                                  "color", "blah")
                      .nullableKeys("__id", "oid", "name", "state", "status",
                                    "some", "that", "any", "this", "lang", "b",
                                    "communityIndex", "test", "testing", "acl",
                                    "favoriteColor", "aKey", "age", "boolean",
                                    "float", "double", "string", "integer",
                                    "long", "myId", "location", "x", "y", "s",
                                    "n", "d", "f", "i", "l", "p", "k", "to-change",
                                    "to-remove", "to-keep", "old", "new",
                                    "gremlin.partitionGraphStrategy.partition",
                                    "color", "blah")
                      .useCustomizeStringId().ifNotExist().create();
                break;
            case AUTOMATIC:
                this.isLastIdCustomized = false;
                schema.vertexLabel(defaultVL)
                      .properties("__id", "oid", "name", "state", "status",
                                  "some", "that", "any", "this", "lang", "b",
                                  "communityIndex", "test", "testing", "acl",
                                  "favoriteColor", "aKey", "age", "boolean",
                                  "float", "double", "string", "integer",
                                  "long", "myId", "location", "x", "y", "s",
                                  "n", "d", "f", "i", "l", "p", "k", "to-change",
                                  "to-remove", "to-keep", "old", "new",
                                  "gremlin.partitionGraphStrategy.partition",
                                  "color", "blah")
                      .nullableKeys("__id", "oid", "name", "state", "status",
                                    "some", "that", "any", "this", "lang", "b",
                                    "communityIndex", "test", "testing", "acl",
                                    "favoriteColor", "aKey", "age", "boolean",
                                    "float", "double", "string", "integer",
                                    "long", "myId", "location", "x", "y", "s",
                                    "n", "d", "f", "i", "l", "p", "k", "to-change",
                                    "to-remove", "to-keep", "old", "new",
                                    "gremlin.partitionGraphStrategy.partition",
                                    "color", "blah")
                      .ifNotExist().create();
                break;
            default:
                throw new AssertionError("Only customize and automatic " +
                                         "is legal in tinkerpop tests");
        }

        schema.indexLabel("defaultVLBy__id").onV(defaultVL).by("__id")
              .ifNotExist().create();
        schema.indexLabel("defaultVLByName").onV(defaultVL).by("name")
              .ifNotExist().create();
        schema.indexLabel("defaultVLBySome").onV(defaultVL).by("some")
              .ifNotExist().create();
        schema.indexLabel("defaultVLByK").onV(defaultVL).by("k")
              .ifNotExist().create();
    }

    @Watched
    private void initBasicVertexLabelAndEdgeLabelExceptV(String defaultVL) {
        SchemaManager schema = this.graph.schema();

        if (!"person".equals(defaultVL)) {
            schema.vertexLabel("person")
                  .properties("name", "age")
                  .nullableKeys("name", "age")
                  .ifNotExist().create();
        }
        schema.vertexLabel("software")
              .properties("name", "lang")
              .nullableKeys("name", "lang")
              .ifNotExist().create();
        schema.vertexLabel("thing")
              .properties("here")
              .nullableKeys("here")
              .ifNotExist().create();
        schema.vertexLabel("blah")
              .properties("test")
              .nullableKeys("test")
              .ifNotExist().create();

        schema.edgeLabel("self").link(defaultVL, defaultVL)
              .properties("__id", "test", "name", "some", "acl", "weight",
                          "here", "to-change", "dropped", "not-dropped", "new",
                          "to-drop", "short", "long")
              .nullableKeys("__id", "test", "name", "some", "acl", "weight",
                            "here", "to-change", "dropped", "not-dropped",
                            "new", "to-drop", "short", "long")
              .ifNotExist().create();
        schema.edgeLabel("aTOa").link(defaultVL, defaultVL)
              .properties("gremlin.partitionGraphStrategy.partition")
              .nullableKeys("gremlin.partitionGraphStrategy.partition")
              .ifNotExist().create();
        schema.edgeLabel("aTOb").link(defaultVL, defaultVL)
              .properties("gremlin.partitionGraphStrategy.partition")
              .nullableKeys("gremlin.partitionGraphStrategy.partition")
              .ifNotExist().create();
        schema.edgeLabel("bTOc").link(defaultVL, defaultVL)
              .properties("gremlin.partitionGraphStrategy.partition")
              .nullableKeys("gremlin.partitionGraphStrategy.partition")
              .ifNotExist().create();
        schema.edgeLabel("aTOc").link(defaultVL, defaultVL)
              .properties("gremlin.partitionGraphStrategy.partition")
              .nullableKeys("gremlin.partitionGraphStrategy.partition")
              .ifNotExist().create();
        schema.edgeLabel("connectsTo").link(defaultVL, defaultVL)
              .properties("gremlin.partitionGraphStrategy.partition", "every")
              .nullableKeys("gremlin.partitionGraphStrategy.partition", "every")
              .ifNotExist().create();
        schema.edgeLabel("relatesTo").link(defaultVL, defaultVL)
              .properties("gremlin.partitionGraphStrategy.partition", "every")
              .nullableKeys("gremlin.partitionGraphStrategy.partition", "every")
              .ifNotExist().create();
        schema.edgeLabel("test").link(defaultVL, defaultVL)
              .properties("test", "xxx", "yyy")
              .nullableKeys("test", "xxx", "yyy")
              .ifNotExist().create();
        schema.edgeLabel("friend").link(defaultVL, defaultVL)
              .properties("name", "location", "status", "uuid", "weight",
                          "acl", "bloop")
              .nullableKeys("name", "location", "status", "uuid", "weight",
                            "acl", "bloop")
              .ifNotExist().create();
        schema.edgeLabel("pets").link(defaultVL, defaultVL).ifNotExist().create();
        schema.edgeLabel("walks").link(defaultVL, defaultVL)
              .properties("location")
              .nullableKeys("location")
              .ifNotExist().create();
        schema.edgeLabel("livesWith").link(defaultVL, defaultVL).ifNotExist().create();
        schema.edgeLabel("friends").link(defaultVL, defaultVL)
              .properties("weight")
              .nullableKeys("weight")
              .ifNotExist().create();
        schema.edgeLabel("collaborator").link(defaultVL, defaultVL)
              .properties("location")
              .nullableKeys("location")
              .ifNotExist().create();
        schema.edgeLabel("hate").link(defaultVL, defaultVL)
              .ifNotExist().create();
        schema.edgeLabel("hates").link(defaultVL, defaultVL)
              .ifNotExist().create();
        schema.edgeLabel("test1").link(defaultVL, defaultVL)
              .ifNotExist().create();
        schema.edgeLabel("link").link(defaultVL, defaultVL)
              .ifNotExist().create();
        schema.edgeLabel("test2").link(defaultVL, defaultVL)
              .ifNotExist().create();
        schema.edgeLabel("test3").link(defaultVL, defaultVL)
              .ifNotExist().create();
        schema.edgeLabel("l").link(defaultVL, defaultVL)
              .properties("name")
              .nullableKeys("name")
              .ifNotExist().create();
        schema.edgeLabel("CONTROL").link(defaultVL, defaultVL)
              .ifNotExist().create();
        schema.edgeLabel("SELFLOOP").link(defaultVL, defaultVL)
              .ifNotExist().create();
        schema.edgeLabel("edge").link(defaultVL, defaultVL)
              .properties("weight")
              .nullableKeys("weight")
              .ifNotExist().create();
        schema.edgeLabel("next").link(defaultVL, defaultVL)
              .ifNotExist().create();

        schema.indexLabel("selfByName").onE("self").by("name")
              .ifNotExist().create();
        schema.indexLabel("selfBy__id").onE("self").by("__id")
              .ifNotExist().create();
        schema.indexLabel("selfBySome").onE("self").by("some")
              .ifNotExist().create();
        schema.indexLabel("selfByThis").onV(defaultVL).by("this")
              .ifNotExist().create();
        schema.indexLabel("selfByAny").onV(defaultVL).by("any")
              .ifNotExist().create();
        schema.indexLabel("selfByGremlinPartition").onV(defaultVL)
              .by("gremlin.partitionGraphStrategy.partition")
              .ifNotExist().create();
        schema.indexLabel("aTOaByGremlinPartition").onE("aTOa")
              .by("gremlin.partitionGraphStrategy.partition")
              .ifNotExist().create();
        schema.indexLabel("aTOcByGremlinPartition").onE("aTOc")
              .by("gremlin.partitionGraphStrategy.partition")
              .ifNotExist().create();
        schema.indexLabel("bTOcByGremlinPartition").onE("bTOc")
              .by("gremlin.partitionGraphStrategy.partition")
              .ifNotExist().create();
        schema.edgeLabel("blah1").link(defaultVL, defaultVL)
              .ifNotExist().create();
        schema.edgeLabel("blah2").link(defaultVL, defaultVL)
              .ifNotExist().create();
    }

    public void initEdgeLabelDefaultKnowsDefault(String defaultVL) {
        SchemaManager schema = this.graph.schema();
        schema.edgeLabel("knows").link(defaultVL, defaultVL)
              .properties("data", "test", "year", "boolean", "float",
                          "double", "string", "integer", "long", "weight",
                          "myEdgeId", "since", "acl", "stars", "aKey",
                          "gremlin.partitionGraphStrategy.partition", "color")
              .nullableKeys("data", "test", "year", "boolean", "float",
                            "double", "string", "integer", "long", "weight",
                            "myEdgeId", "since", "acl", "stars", "aKey",
                            "gremlin.partitionGraphStrategy.partition", "color")
              .ifNotExist().create();
    }

    public void initEdgeLabelDefaultCreatedDefault(String defaultVL) {
        SchemaManager schema = this.graph.schema();
        schema.edgeLabel("created").link(defaultVL, defaultVL)
              .properties("weight", "color")
              .nullableKeys("weight", "color")
              .ifNotExist().create();
    }

    public void initEdgeLabelPersonKnowsPerson() {
        SchemaManager schema = this.graph.schema();
        schema.edgeLabel("knows").link("person", "person")
              .properties("weight")
              .nullableKeys("weight")
              .ifNotExist().create();
    }

    public void initEdgeLabelPersonCreatedSoftware() {
        SchemaManager schema = this.graph.schema();
        schema.edgeLabel("created").link("person", "software")
              .properties("weight")
              .nullableKeys("weight")
              .ifNotExist().create();
    }

    public void initSinkSchema() {
        SchemaManager schema = this.graph.schema();

        schema.propertyKey("name").ifNotExist().create();

        schema.vertexLabel("message")
              .properties("name")
              .nullableKeys("name")
              .ifNotExist().create();

        schema.vertexLabel("loops")
              .properties("name")
              .nullableKeys("name")
              .ifNotExist().create();

        schema.edgeLabel("link").link("message", "message")
              .ifNotExist().create();
        schema.edgeLabel("self").link("loops", "loops")
              .ifNotExist().create();

        schema.indexLabel("loopsByName").onV("loops")
              .secondary().by("name")
              .ifNotExist().create();
    }
}
