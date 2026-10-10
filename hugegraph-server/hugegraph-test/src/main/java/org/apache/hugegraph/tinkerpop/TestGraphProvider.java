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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.structure.HugeProperty;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.structure.HugeVertexProperty;
import org.apache.hugegraph.testutil.Utils;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.FeatureRequirements;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeEdgeTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.LazyBarrierStrategy;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.TransactionMultiThreadedTest;
import org.junit.Assert;
import org.junit.Assume;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;

public class TestGraphProvider extends AbstractGraphProvider {

    private static final Logger LOG = Log.logger(TestGraphProvider.class);

    @SuppressWarnings("rawtypes")
    private static final Set<Class> IMPLEMENTATIONS = ImmutableSet.of(
            HugeEdge.class,
            HugeElement.class,
            HugeGraph.class,
            HugeProperty.class,
            HugeVertex.class,
            HugeVertexProperty.class);

    private static final String FILTER = "test.tinkerpop.filter";
    private static final String DEFAULT_FILTER = "methods.filter";

    private static final String TEST_CLASS = "testClass";
    private static final String TEST_METHOD = "testMethod";

    private static final String LOAD_GRAPH = "loadGraph";
    private static final String STANDARD = "standard";
    private static final String REGULAR_LOAD = "regularLoad";

    private static final String GREMLIN_GRAPH_KEY = "gremlin.graph";
    private static final String GREMLIN_GRAPH_VALUE =
            "org.apache.hugegraph.tinkerpop.TestGraphFactory";
    private static final String BACKEND = "backend";
    private static final String BACKEND_ROCKSDB = "rocksdb";
    private static final String ROCKSDB_DATA_PATH = "rocksdb.data_path";
    private static final String ROCKSDB_WAL_PATH = "rocksdb.wal_path";
    private static final String ROCKSDB_DATA_DISKS = "rocksdb.data_disks";
    private static final String TEST_PATH_SEPARATOR = "/";
    private static final int MAX_ROCKSDB_PATH_SUFFIX_PREFIX_LENGTH = 80;

    private static final String AKEY_CLASS_PREFIX =
            "org.apache.tinkerpop.gremlin.structure." +
            "PropertyTest.PropertyFeatureSupportTest";
    private static final String SUPPORTS_PREFIX = "supports";
    private static final String FEATURE_VALUES_SUFFIX = "Values";
    private static final String IO_CLASS_PREFIX =
            "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest";
    private static final String IO_TEST_PREFIX =
            "org.apache.tinkerpop.gremlin.structure.io.IoTest";

    private static final String EXPECT_CUSTOMIZED_ID = "expectCustomizedId";
    private static final Set<String> CUSTOMIZED_ID_METHODS = ImmutableSet.of(
            "shouldReadWriteSelfLoopingEdges",
            "shouldEvaluateConnectivityPatterns");

    private static final Set<String> ID_TYPES = ImmutableSet.of(
            VertexPropertyFeatures.FEATURE_USER_SUPPLIED_IDS,
            VertexPropertyFeatures.FEATURE_NUMERIC_IDS,
            VertexPropertyFeatures.FEATURE_STRING_IDS);

    private static final RateLimiter LOG_RATE_LIMITER =
            RateLimiter.create(1.0 / 300);

    private Map<String, String> blackMethods = new HashMap<>();
    private Map<String, TestGraph> graphs = new HashMap<>();
    private final String suite;

    public TestGraphProvider(String suite) throws IOException {
        super();
        this.initBlackList();
        this.suite = suite;
    }

    private void initBlackList() throws IOException {
        String filter = Utils.getConf().getString(FILTER);
        if (filter == null || filter.isEmpty()) {
            filter = DEFAULT_FILTER;
        }

        URL blackList = TestGraphProvider.class.getClassLoader()
                                               .getResource(filter);
        E.checkArgument(blackList != null,
                        "Can't find tests filter '%s' in resource directory",
                        filter);
        File file = new File(blackList.getPath());
        E.checkArgument(
                file.exists() && file.isFile() && file.canRead(),
                "Need to specify a readable filter file rather than: %s",
                file.toString());
        try (FileReader fr = new FileReader(file);
             BufferedReader reader = new BufferedReader(fr)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty() || line.startsWith("#")) {
                    // Empty line or comment line
                    continue;
                }
                String[] parts = line.split(":");
                Assert.assertEquals("methods.filter proper format is: " +
                                    "'testMethodName: ignore reason'",
                                    2, parts.length);
                Assert.assertTrue(
                        "Test method name in methods.filter can't be empty",
                        parts[0] != null && !parts[0].trim().isEmpty());
                Assert.assertTrue(
                        "Reason why ignore in methods.filter can't be empty",
                        parts[1] != null && !parts[1].trim().isEmpty());
                this.blackMethods.putIfAbsent(parts[0], parts[1]);
            }
        }
    }

    @Override
    public Map<String, Object> getBaseConfiguration(
            String graphName,
            Class<?> testClass, String testMethod,
            LoadGraphWith.GraphData graphData) {
        // Check if test in blackList
        String testFullName = testClass.getCanonicalName() + "." + testMethod;
        int index = testFullName.indexOf('@') == -1 ?
                    testFullName.length() : testFullName.indexOf('@');

        testFullName = testFullName.substring(0, index);
        Assume.assumeFalse(
                String.format("Test %s will be ignored with reason: %s",
                              testFullName, this.blackMethods.get(testFullName)),
                this.blackMethods.containsKey(testFullName));

        LOG.debug("Full name of test is: {}", testFullName);
        LOG.debug("Prefix of test is: {}", testFullName.substring(0, index));
        HashMap<String, Object> confMap = new HashMap<>();
        PropertiesConfiguration config = Utils.getConf();
        Iterator<String> keys = config.getKeys();
        while (keys.hasNext()) {
            String key = keys.next();
            confMap.put(key, config.getProperty(key));
        }
        String storePrefix = config.getString(CoreOptions.STORE.name());
        String store = storePrefix + "_" + this.suite + "_" + graphName;
        if (isTransactionMultiThreadedPropertyTest(testClass, testMethod)) {
            store += "_txprop";
        } else if (isMergeEdgeSelfTest(testClass, testMethod)) {
            store += "_meself";
        }
        confMap.put(CoreOptions.STORE.name(), store);
        if (isRocksDBBackend(config)) {
            this.isolateRocksDBPaths(confMap, graphName, testClass,
                                     testMethod);
        }
        confMap.put(GREMLIN_GRAPH_KEY, GREMLIN_GRAPH_VALUE);
        confMap.put(TEST_CLASS, testClass);
        confMap.put(TEST_METHOD, testMethod);
        confMap.put(LOAD_GRAPH, graphData);
        confMap.put(EXPECT_CUSTOMIZED_ID, customizedId(testClass, testMethod));

        return confMap;
    }

    private void isolateRocksDBPaths(Map<String, Object> confMap,
                                     String graphName, Class<?> testClass,
                                     String testMethod) {
        String testClassName = testClass.getName();
        String rawSuffix = this.suite + "_" + graphName + "_" +
                           testClassName + "_" + testMethod;
        String prefix = sanitizePathPart(this.suite + "_" + graphName + "_" +
                                         testClass.getSimpleName() + "_" +
                                         testMethod);
        if (prefix.length() > MAX_ROCKSDB_PATH_SUFFIX_PREFIX_LENGTH) {
            prefix = prefix.substring(0,
                                      MAX_ROCKSDB_PATH_SUFFIX_PREFIX_LENGTH);
        }
        String pathSuffix = prefix + "_" + shortHash(rawSuffix);
        isolatePath(confMap, ROCKSDB_DATA_PATH, pathSuffix);
        isolatePath(confMap, ROCKSDB_WAL_PATH, pathSuffix);

        Object dataDisks = confMap.get(ROCKSDB_DATA_DISKS);
        if (dataDisks != null) {
            confMap.put(ROCKSDB_DATA_DISKS,
                        isolateDataDisks(dataDisks, pathSuffix));
        }
    }

    private static void isolatePath(Map<String, Object> confMap, String key,
                                    String pathSuffix) {
        Object path = confMap.get(key);
        if (path == null) {
            return;
        }
        confMap.put(key, appendPath(path.toString(), pathSuffix));
    }

    private static String isolateDataDisks(Object dataDisks,
                                           String pathSuffix) {
        String value = dataDisks.toString().trim();
        if (value.isEmpty()) {
            return value;
        }

        boolean wrapped = value.startsWith("[") && value.endsWith("]");
        String body = wrapped ? value.substring(1, value.length() - 1) : value;
        String[] entries = body.split(",");
        StringBuilder builder = new StringBuilder();
        for (String entry : entries) {
            String item = entry.trim();
            int index = item.indexOf(':');
            if (index < 0) {
                return value;
            }
            String table = item.substring(0, index).trim();
            String path = item.substring(index + 1).trim();
            if (table.isEmpty() || path.isEmpty()) {
                return value;
            }
            if (builder.length() > 0) {
                builder.append(',');
            }
            builder.append(table).append(':')
                   .append(appendPath(path, pathSuffix));
        }
        return wrapped ? "[" + builder + "]" : builder.toString();
    }

    private static String appendPath(String path, String suffix) {
        if (path.endsWith("/") || path.endsWith("\\")) {
            return path + suffix;
        }
        return path + TEST_PATH_SEPARATOR + suffix;
    }

    private static String sanitizePathPart(String value) {
        return value.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    private static String shortHash(String value) {
        return Integer.toHexString(value.hashCode());
    }

    private static boolean isRocksDBBackend(Configuration config) {
        return config != null &&
               BACKEND_ROCKSDB.equals(config.getString(BACKEND, ""));
    }

    private static boolean customizedId(Class<?> test, String testMethod) {
        Method method;
        try {
            method = test.getDeclaredMethod(testMethod);
        } catch (NoSuchMethodException ignored) {
            return false;
        }
        FeatureRequirements features =
                method.getAnnotation(FeatureRequirements.class);
        if (features == null) {
            return false;
        }
        for (FeatureRequirement feature : features.value()) {
            if (feature.featureClass() == Graph.Features.VertexFeatures.class &&
                ID_TYPES.contains(feature.feature())) {
                // Expect CUSTOMIZED_ID if want to pass id to create vertex
                return true;
            }
        }
        return false;
    }

    private static boolean isTransactionMultiThreadedPropertyTest(
            Class<?> testClass, String testMethod) {
        return testClass == TransactionMultiThreadedTest.class &&
               testMethod.equals("shouldChangeVertexProperty");
    }

    private static boolean isMergeEdgeSelfTest(Class<?> testClass,
                                               String testMethod) {
        return testClass == MergeEdgeTest.Traversals.class &&
               testMethod.equals("g_V_mergeEXlabel_self_weight_05X");
    }

    private static String getAKeyType(Class<?> clazz, String method) {
        if (clazz.getCanonicalName().startsWith(AKEY_CLASS_PREFIX)) {
            String feature = method;
            int featureStart = method.indexOf('[');
            int featureEnd = method.indexOf(']');
            if (featureStart >= 0 && featureEnd > featureStart) {
                feature = method.substring(featureStart + 1, featureEnd);
            }

            if (!feature.startsWith(SUPPORTS_PREFIX)) {
                return null;
            }
            feature = feature.substring(SUPPORTS_PREFIX.length());

            int valueStart = feature.indexOf('(');
            if (valueStart >= 0) {
                feature = feature.substring(0, valueStart);
            }
            if (!feature.endsWith(FEATURE_VALUES_SUFFIX)) {
                return null;
            }
            return feature.substring(0, feature.length() -
                                        FEATURE_VALUES_SUFFIX.length());
        }
        return null;
    }

    private static String getIoType(Class<?> clazz, String method) {
        if (clazz.getCanonicalName().startsWith(IO_CLASS_PREFIX)) {
            return method.substring(method.indexOf('[') + 1,
                                    method.indexOf(']'));
        }
        return null;
    }

    private static boolean isIoTest(Class<?> clazz) {
        return clazz.getCanonicalName().startsWith(IO_TEST_PREFIX);
    }

    private static IdStrategy idStrategy(Configuration config) {
        Class<?> testClass = (Class<?>) config.getProperty(TEST_CLASS);
        String testMethod = config.getString(TEST_METHOD);

        // Set id strategy
        IdStrategy idStrategy = IdStrategy.AUTOMATIC;
        if (config.getBoolean(EXPECT_CUSTOMIZED_ID) || isIoTest(testClass) ||
            CUSTOMIZED_ID_METHODS.contains(testMethod)) {
            /*
             * Use CUSTOMIZED_ID if the following case are met:
             *  1.Expect CUSTOMIZED_ID for some specific features
             *  2.IoTest, it will copy vertex from IO or other graph
             *  3.Some tests that need to pass vertex id manually
             */
            idStrategy = IdStrategy.CUSTOMIZE_STRING;
        }
        return idStrategy;
    }

    @Watched
    private TestGraph newTestGraph(final Configuration config) {
        TestGraph testGraph = ((TestGraph) super.openTestGraph(config));
        testGraph.initBackend();
        return testGraph;
    }

    @Watched
    @Override
    public Graph openTestGraph(final Configuration config) {
        String graphName = config.getString(CoreOptions.STORE.name());
        Class<?> testClass = (Class<?>) config.getProperty(TEST_CLASS);
        String testMethod = config.getString(TEST_METHOD);

        TestGraph testGraph = this.graphs.get(graphName);
        if (testGraph == null) {
            this.graphs.putIfAbsent(graphName, this.newTestGraph(config));
        } else if (testGraph.closed()) {
            this.graphs.put(graphName, this.newTestGraph(config));
        }
        testGraph = this.graphs.get(graphName);

        // Ensure tx clean
        testGraph.tx().rollback();

        // Define property key 'aKey' based on specified type in test name
        String aKeyType = getAKeyType(testClass, testMethod);
        if (aKeyType != null) {
            testGraph.initPropertyKey("aKey", aKeyType);
        }

        if (testMethod.equals(
                "shouldHaveTruncatedStringRepresentationForEdgeProperty")) {
            testGraph.initPropertyKey("long", "String");
        } else {
            testGraph.initPropertyKey("long", "Long");
        }

        if (isTransactionMultiThreadedPropertyTest(testClass, testMethod)) {
            testGraph.initPropertyKey("test", "Integer");
        }

        // Basic schema is initiated by default once a graph is open
        String selfVL = isMergeEdgeSelfTest(testClass, testMethod) ?
                        "person" : TestGraph.DEFAULT_VL;
        testGraph.initBasicSchema(idStrategy(config), TestGraph.DEFAULT_VL,
                                  selfVL);
        if (testClass.getName().equals(
                "org.apache.tinkerpop.gremlin.process.traversal.step.map.ReadTest$Traversals")) {
            testGraph.initEdgeLabelPersonKnowsPerson();
            testGraph.initEdgeLabelPersonCreatedSoftware();
        } else {
            testGraph.initEdgeLabelDefaultKnowsDefault(TestGraph.DEFAULT_VL);
            testGraph.initEdgeLabelDefaultCreatedDefault(TestGraph.DEFAULT_VL);
        }
        testGraph.tx().commit();

        testGraph.loadedGraph(getIoType(testClass, testMethod));
        testGraph.autoPerson(false);
        testGraph.ioTest(isIoTest(testClass));

        Object loadGraph = config.getProperty(LOAD_GRAPH);
        if (loadGraph != null && !graphName.endsWith(STANDARD)) {
            this.loadGraphData(testGraph, (LoadGraphWith.GraphData) loadGraph);
        }

        // Used for travis ci output log
        if (LOG_RATE_LIMITER.tryAcquire(1)) {
            LOG.info("Open graph '{}' for test '{}'", graphName, testMethod);
        }
        return testGraph;
    }

    @Watched
    @Override
    public void clear(Graph graph, Configuration config) throws Exception {
        TestGraph testGraph = (TestGraph) graph;
        if (testGraph == null) {
            return;
        }
        String graphName = config.getString(CoreOptions.STORE.name());
        if (!testGraph.initedBackend()) {
            testGraph.close();
            if (this.graphs.get(graphName) == testGraph) {
                this.graphs.remove(graphName);
            }
            return;
        }
        if (testGraph.closed()) {
            if (this.graphs.get(graphName) == testGraph) {
                this.graphs.remove(graphName);
            }
            return;
        }

        // Reset consumers
        graph.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.AUTO);
        graph.tx().onClose(Transaction.CLOSE_BEHAVIOR.ROLLBACK);

        // Ensure tx clean
        graph.tx().rollback();

        // Clear all data
        Class<?> testClass = (Class<?>) config.getProperty(TEST_CLASS);
        testGraph.clearAll(testClass.getCanonicalName());

        if (isRocksDBBackend(config)) {
            testGraph.close();
            if (this.graphs.get(graphName) == testGraph) {
                this.graphs.remove(graphName);
            }
        }

        LOG.debug("Clear graph '{}'", graphName);
    }

    public void clear() {
        for (TestGraph graph : this.graphs.values()) {
            graph.clearBackend();
        }
        for (TestGraph graph : this.graphs.values()) {
            try {
                graph.close();
            } catch (Exception e) {
                LOG.error("Error while closing graph '{}'", graph, e);
            }
        }
        this.graphs.clear();
    }

    @Watched
    @SuppressWarnings("rawtypes")
    @Override
    public void loadGraphData(final Graph graph,
                              final LoadGraphWith loadGraphWith,
                              final Class testClass,
                              final String testName) {
        if (loadGraphWith == null) {
            super.loadGraphData(graph, loadGraphWith, testClass, testName);
            return;
        }

        this.loadGraphData(graph, loadGraphWith.value());

        super.loadGraphData(graph, loadGraphWith, testClass, testName);
    }

    @Watched
    public void loadGraphData(final Graph graph,
                              final LoadGraphWith.GraphData loadGraphWith) {
        TestGraph testGraph = (TestGraph) graph;

        // Clear basic schema initiated in openTestGraph
        testGraph.clearForLoad();

        if (testGraph.loadedGraph() == null) {
            testGraph.loadedGraph(REGULAR_LOAD);
        }

        boolean standard = testGraph.hugegraph().name().endsWith(STANDARD);
        IdStrategy idStrategy = standard && !testGraph.ioTest() ?
                                IdStrategy.AUTOMATIC :
                                IdStrategy.CUSTOMIZE_STRING;

        switch (loadGraphWith) {
            case GRATEFUL:
                testGraph.initGratefulSchema(idStrategy);
                break;
            case MODERN:
                testGraph.initModernSchema(idStrategy);
                break;
            case CLASSIC:
                testGraph.initClassicSchema(idStrategy);
                break;
            case CREW:
                break;
            case SINK:
                testGraph.initSinkSchema();
                break;
            default:
                throw new AssertionError(String.format(
                        "Only support GRATEFUL, MODERN and CLASSIC " +
                        "for @LoadGraphWith(), but '%s' is used ",
                        loadGraphWith));
        }
        LOG.debug("Load graph with {} schema", loadGraphWith);
        testGraph.tx().commit();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATIONS;
    }

    @SuppressWarnings("unchecked")
    @Override
    public GraphTraversalSource traversal(Graph graph) {
        HugeGraph hugegraph = ((TestGraph) graph).hugegraph();
        return hugegraph.traversal()
                        .withoutStrategies(LazyBarrierStrategy.class);
    }

    @Override
    public String convertId(Object id, Class<? extends Element> c) {
        if (id instanceof Number) {
            return id.toString();
        }
        return String.format("\"%s\"", StringEscapeUtils.escapeJava(
                id.toString()));
    }
}
