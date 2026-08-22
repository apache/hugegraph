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

package org.apache.hugegraph.unit.config;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeFeatures;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.MessageTextSerializer;
import org.junit.Test;
import org.mockito.Mockito;
import org.yaml.snakeyaml.Yaml;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class GremlinConfigCompatibilityTest extends BaseUnitTest {

    private static final Pattern CLASS_NAME =
            Pattern.compile("className:\\s*([^,}\\s]+)");
    private static final Pattern XML_COMMENT =
            Pattern.compile("<!--.*?-->", Pattern.DOTALL);
    private static final Pattern TINKERPOP_DEPENDENCY = Pattern.compile(
            "<dependency>\\s*<groupId>org\\.apache\\.tinkerpop</groupId>" +
            "(.*?)</dependency>", Pattern.DOTALL);
    private static final Pattern DEPENDENCY_VERSION =
            Pattern.compile("<version>(.*?)</version>", Pattern.DOTALL);
    private static final Pattern TINKERPOP_VERSION_PROPERTY = Pattern.compile(
            "<tinkerpop\\.version>(.*?)</tinkerpop\\.version>");
    private static final Pattern GUICE_DEPENDENCY = Pattern.compile(
            "<dependency>\\s*<groupId>com\\.google\\.inject</groupId>" +
            "(.*?)</dependency>", Pattern.DOTALL);
    private static final String SUPPORTED_TINKERPOP_VERSION = "3.8.1";
    /* Must match the guice.version property of the TinkerPop parent pom
     * (gremlin-test declares guice as a provided dependency, so HugeGraph
     * must supply the version TinkerPop itself was built against). */
    private static final String SUPPORTED_GUICE_VERSION = "4.2.3";
    private static final String SERIALIZER_PACKAGE =
            "org.apache.tinkerpop.gremlin.util.ser.";
    private static final String GRAPHSON_UNTYPED_V1 =
            SERIALIZER_PACKAGE + "GraphSONUntypedMessageSerializerV1";
    private static final String GRAPHBINARY_BUILDER =
            "org.apache.hugegraph.io.HugeGraphTypeSerializerRegistryBuilder";
    private static final String IO_REGISTRY =
            "org.apache.hugegraph.io.HugeGraphIoRegistry";
    private static final String GREMLIN_SERVER_CONFIG = "gremlin-server.yaml";
    private static final String REMOTE_OBJECTS_CONFIG = "remote-objects.yaml";
    private static final List<String> GREMLIN_SERVER_CONFIG_VARIANTS =
            Arrays.asList(
                    "static/conf/gremlin-server.yaml",
                    "travis/conf-raft1/gremlin-server.yaml",
                    "travis/conf-raft2/gremlin-server.yaml",
                    "travis/conf-raft3/gremlin-server.yaml"
            );
    private static final List<String> REMOTE_CONFIGS = Arrays.asList(
            "gremlin-driver-settings.yaml",
            "remote.yaml",
            REMOTE_OBJECTS_CONFIG
    );
    private static final List<String> TYPED_FALLBACK_SERIALIZERS =
            Arrays.asList(
                    SERIALIZER_PACKAGE + "GraphSONMessageSerializerV1",
                    SERIALIZER_PACKAGE + "GraphSONMessageSerializerV2",
                    SERIALIZER_PACKAGE + "GraphSONMessageSerializerV3"
            );
    private static final List<String> TYPED_GRAPHSON_MIME_TYPES =
            Arrays.asList(
                    "application/vnd.gremlin-v1.0+json",
                    "application/vnd.gremlin-v2.0+json",
                    "application/vnd.gremlin-v3.0+json"
            );
    private static final List<String> UNTYPED_GRAPHSON_MIME_TYPES =
            Arrays.asList(
                    "application/vnd.gremlin-v1.0+json;types=false",
                    "application/vnd.gremlin-v2.0+json;types=false",
                    "application/vnd.gremlin-v3.0+json;types=false"
            );

    @Test
    public void testGremlinServerSerializersUseTinkerPopUtilPackage() throws IOException {
        String content = readConfig(GREMLIN_SERVER_CONFIG);

        assertUsesHugeGraphIoRegistry(GREMLIN_SERVER_CONFIG, content);
        assertSerializerClassNamesUseUtilPackage(GREMLIN_SERVER_CONFIG,
                                                 content);
    }

    @Test
    public void testTinkerPopPomVersionsUseSupportedVersion()
            throws IOException {
        List<String> mismatches = new ArrayList<>();
        try (Stream<Path> files = Files.walk(repositoryRoot())) {
            files.filter(path -> "pom.xml".equals(
                         path.getFileName().toString()))
                 .forEach(path -> collectTinkerPopVersionMismatches(
                         path, mismatches));
        }

        Assert.assertTrue("TinkerPop dependencies must use " +
                          SUPPORTED_TINKERPOP_VERSION + " and guice must " +
                          "use " + SUPPORTED_GUICE_VERSION + ": " + mismatches,
                          mismatches.isEmpty());
    }

    @Test
    public void testRemoteSerializersUseTinkerPopUtilPackage() throws IOException {
        for (String file : REMOTE_CONFIGS) {
            String content = readConfig(file);

            assertUsesHugeGraphIoRegistry(file, content);
            assertSerializerClassNamesUseUtilPackage(file, content);
        }
    }

    @Test
    public void testConfiguredSerializerClassesAreLoadable() throws Exception {
        assertConfiguredSerializerClassesAreLoadable(
                GREMLIN_SERVER_CONFIG, readConfig(GREMLIN_SERVER_CONFIG));
        for (String file : REMOTE_CONFIGS) {
            assertConfiguredSerializerClassesAreLoadable(file,
                                                         readConfig(file));
        }
    }

    @Test
    public void testGremlinServerConfigVariantsSupportGraphSONMimeTypes()
            throws Exception {
        Path assembly = serverAssemblyPath();

        for (String variant : GREMLIN_SERVER_CONFIG_VARIANTS) {
            Settings settings = Settings.read(assembly.resolve(variant)
                                                   .toString());

            assertSupportsTypedAndUntypedGraphSONMimeTypes(variant,
                                                           graphSONMimeTypes(settings));
        }
    }

    @Test
    public void testGremlinServerConfigVariantsUseHugeGraphBinaryBuilder()
            throws Exception {
        Path assembly = serverAssemblyPath();

        for (String variant : GREMLIN_SERVER_CONFIG_VARIANTS) {
            Settings settings = Settings.read(assembly.resolve(variant)
                                                   .toString());
            boolean found = false;
            for (Settings.SerializerSettings serializer :
                 settings.serializers) {
                if (!serializer.className.startsWith(SERIALIZER_PACKAGE +
                                                     "GraphBinary")) {
                    continue;
                }
                Assert.assertNotNull(variant, serializer.config);
                Assert.assertEquals(variant, GRAPHBINARY_BUILDER,
                                    serializer.config.get("builder"));
                found = true;
            }
            Assert.assertTrue("No GraphBinary serializer in " + variant,
                              found);
        }
    }

    private static void assertSupportsTypedAndUntypedGraphSONMimeTypes(
            String fileName, Map<String, String> graphSONMimeTypes) {
        for (String mimeType : UNTYPED_GRAPHSON_MIME_TYPES) {
            Assert.assertTrue(fileName + " should support untyped " +
                              "GraphSON MIME " + mimeType,
                              graphSONMimeTypes.containsKey(mimeType));
        }
        for (String mimeType : TYPED_GRAPHSON_MIME_TYPES) {
            Assert.assertTrue(fileName + " should support typed GraphSON " +
                              "MIME " + mimeType,
                              graphSONMimeTypes.containsKey(mimeType));
        }
        Assert.assertEquals(fileName + " should keep application/json " +
                            "mapped to the untyped V1 serializer",
                            GRAPHSON_UNTYPED_V1,
                            graphSONMimeTypes.get("application/json"));
    }

    @Test
    public void testConfiguredGraphSONSerializersCanSerializeHugeGraphTypes()
            throws Exception {
        Settings settings = readGremlinServerSettings();
        List<String> typedSerializers = new ArrayList<>();
        boolean foundUntyped = false;

        for (Settings.SerializerSettings serializerSettings :
             settings.serializers) {
            if (!serializerSettings.className.startsWith(SERIALIZER_PACKAGE +
                                                         "GraphSON")) {
                continue;
            }

            MessageTextSerializer<?> serializer =
                    newTextSerializer(serializerSettings.className);
            serializer.configure(config(serializerSettings.config),
                                 Collections.emptyMap());
            boolean typed = !serializerSettings.className.startsWith(
                    SERIALIZER_PACKAGE + "GraphSONUntyped");
            if (typed) {
                typedSerializers.add(serializerSettings.className);
            } else {
                foundUntyped = true;
            }
            assertCanSerializeHugeGraphTypes(
                    serializer,
                    typed && usesStableGraphSONTypes(
                            serializerSettings.className));
        }

        Assert.assertTrue("No untyped GraphSON serializer settings found in " +
                          GREMLIN_SERVER_CONFIG, foundUntyped);
        Assert.assertEquals("Configured typed GraphSON serializers should " +
                            "match the fallback set",
                            TYPED_FALLBACK_SERIALIZERS, typedSerializers);
    }

    @Test
    public void testTypedFallbackSerializersCanRoundTripHugeGraphIds()
            throws Exception {
        Map<String, Object> config = graphSONV1Config(
                readGremlinServerSettings());

        for (String serializer : TYPED_FALLBACK_SERIALIZERS) {
            MessageTextSerializer<?> textSerializer =
                    newTextSerializer(serializer);

            textSerializer.configure(config(config), Collections.emptyMap());
            assertCanRoundTripHugeGraphIds(serializer, textSerializer);
        }
    }

    @Test
    public void testConfiguredGraphBinarySerializersCanRoundTripStandardPredicate()
            throws Exception {
        Settings settings = readGremlinServerSettings();
        boolean found = false;

        for (Settings.SerializerSettings serializerSettings :
             settings.serializers) {
            if (!serializerSettings.className.startsWith(SERIALIZER_PACKAGE +
                                                         "GraphBinary")) {
                continue;
            }

            MessageSerializer<?> serializer =
                    newMessageSerializer(serializerSettings.className);
            serializer.configure(config(serializerSettings.config),
                                 Collections.emptyMap());
            assertCanRoundTripStandardPredicate(serializerSettings.className,
                                                serializer);
            found = true;
        }

        Assert.assertTrue("No GraphBinary serializer settings found in " +
                          GREMLIN_SERVER_CONFIG, found);
    }

    @Test
    public void testConfiguredGraphBinarySerializersCanRoundTripElementProperties()
            throws Exception {
        Settings settings = readGremlinServerSettings();
        boolean found = false;

        for (Settings.SerializerSettings serializerSettings :
             settings.serializers) {
            if (!serializerSettings.className.startsWith(SERIALIZER_PACKAGE +
                                                         "GraphBinary")) {
                continue;
            }

            MessageSerializer<?> serializer =
                    newMessageSerializer(serializerSettings.className);
            serializer.configure(config(serializerSettings.config),
                                 Collections.emptyMap());
            assertCanRoundTripElementProperties(serializerSettings.className,
                                                serializer);
            found = true;
        }

        Assert.assertTrue("No GraphBinary serializer settings found in " +
                          GREMLIN_SERVER_CONFIG, found);
    }

    @Test
    public void testConfiguredGraphBinarySerializersCanRoundTripHugeGraphElements()
            throws Exception {
        Settings settings = readGremlinServerSettings();
        boolean found = false;

        for (Settings.SerializerSettings serializerSettings :
             settings.serializers) {
            if (!serializerSettings.className.startsWith(SERIALIZER_PACKAGE +
                                                         "GraphBinary")) {
                continue;
            }

            MessageSerializer<?> serializer =
                    newMessageSerializer(serializerSettings.className);
            serializer.configure(config(serializerSettings.config),
                                 Collections.emptyMap());
            assertCanRoundTripHugeGraphElements(serializerSettings.className,
                                                serializer);
            found = true;
        }

        Assert.assertTrue("No GraphBinary serializer settings found in " +
                          GREMLIN_SERVER_CONFIG, found);
    }

    @Test
    public void testConfiguredGraphBinarySerializersUsePrimitiveHugeGraphIds()
            throws Exception {
        Settings settings = readGremlinServerSettings();
        boolean found = false;

        for (Settings.SerializerSettings serializerSettings :
             settings.serializers) {
            if (!serializerSettings.className.startsWith(SERIALIZER_PACKAGE +
                                                         "GraphBinary")) {
                continue;
            }

            MessageSerializer<?> serializer =
                    newMessageSerializer(serializerSettings.className);
            serializer.configure(config(serializerSettings.config),
                                 Collections.emptyMap());
            assertUsesPrimitiveHugeGraphIds(serializerSettings.className,
                                            serializer);
            found = true;
        }

        Assert.assertTrue("No GraphBinary serializer settings found in " +
                          GREMLIN_SERVER_CONFIG, found);
    }

    @Test
    public void testConfiguredGraphBinarySerializersCanRoundTripReferenceElements()
            throws Exception {
        Settings settings = readGremlinServerSettings();
        boolean found = false;

        for (Settings.SerializerSettings serializerSettings :
             settings.serializers) {
            if (!serializerSettings.className.startsWith(SERIALIZER_PACKAGE +
                                                         "GraphBinary")) {
                continue;
            }

            MessageSerializer<?> serializer =
                    newMessageSerializer(serializerSettings.className);
            serializer.configure(config(serializerSettings.config),
                                 Collections.emptyMap());
            assertCanRoundTripReferenceElements(serializerSettings.className,
                                                serializer);
            found = true;
        }

        Assert.assertTrue("No GraphBinary serializer settings found in " +
                          GREMLIN_SERVER_CONFIG, found);
    }

    @Test
    public void testConfiguredGraphSONSerializersIncludeElementProperties()
            throws Exception {
        Settings settings = readGremlinServerSettings();
        boolean found = false;

        for (Settings.SerializerSettings serializerSettings :
             settings.serializers) {
            if (!serializerSettings.className.startsWith(SERIALIZER_PACKAGE +
                                                         "GraphSON")) {
                continue;
            }

            MessageTextSerializer<?> serializer =
                    newTextSerializer(serializerSettings.className);
            serializer.configure(config(serializerSettings.config),
                                 Collections.emptyMap());
            assertIncludesElementProperties(serializerSettings.className,
                                            serializer);
            found = true;
        }

        Assert.assertTrue("No GraphSON serializer settings found in " +
                          GREMLIN_SERVER_CONFIG, found);
    }

    @Test
    public void testConfiguredGraphSONSerializersIncludeTreeElementProperties()
            throws Exception {
        Settings settings = readGremlinServerSettings();
        boolean found = false;

        for (Settings.SerializerSettings serializerSettings :
             settings.serializers) {
            if (!serializerSettings.className.startsWith(SERIALIZER_PACKAGE +
                                                         "GraphSON")) {
                continue;
            }

            MessageTextSerializer<?> serializer =
                    newTextSerializer(serializerSettings.className);
            serializer.configure(config(serializerSettings.config),
                                 Collections.emptyMap());
            String tree = serializeResponse(serializer, elementTree());
            Assert.assertTrue(serializerSettings.className,
                              tree.contains("properties"));
            Assert.assertTrue(serializerSettings.className,
                              tree.contains("marko"));
            Assert.assertTrue(serializerSettings.className,
                              tree.contains("weight"));
            found = true;
        }

        Assert.assertTrue("No GraphSON serializer settings found in " +
                          GREMLIN_SERVER_CONFIG, found);
    }

    @Test
    public void testDefaultGraphSONSerializerIncludesHugeGraphElementProperties()
            throws Exception {
        Settings settings = readGremlinServerSettings();
        Map<String, Object> config = graphSONV1Config(settings);
        MessageTextSerializer<?> serializer =
                newTextSerializer(GRAPHSON_UNTYPED_V1);
        serializer.configure(config(config), Collections.emptyMap());

        HugeEdge edge = hugeGraphEdgeWithProperties();
        HugeVertex vertex = (HugeVertex) edge.outVertex();
        String vertexJson = serializeResponse(serializer, vertex);
        String edgeJson = serializeResponse(serializer, edge);
        String pathJson = serializeResponse(
                serializer, elementPath(vertex, edge));

        Assert.assertContains("properties", vertexJson);
        Assert.assertContains("name", vertexJson);
        Assert.assertContains("tom", vertexJson);
        Assert.assertContains("age", vertexJson);
        Assert.assertContains("18", vertexJson);
        Assert.assertContains("properties", edgeJson);
        Assert.assertContains("weight", edgeJson);
        Assert.assertContains("0.75", edgeJson);
        Assert.assertContains("properties", pathJson);
        Assert.assertContains("tom", pathJson);
        Assert.assertContains("0.75", pathJson);
    }

    @Test
    public void testRemoteObjectsSerializerCanSerializePathShape()
            throws Exception {
        RemoteSerializerSettings settings =
                readRemoteSerializerSettings(REMOTE_OBJECTS_CONFIG);
        MessageTextSerializer<?> serializer =
                newTextSerializer(settings.className);

        serializer.configure(config(settings.config), Collections.emptyMap());

        String json = serializeResponse(serializer, testPath());

        Assert.assertContains("\"labels\"", json);
        Assert.assertContains("\"objects\"", json);
        Assert.assertContains("marko", json);
        Assert.assertContains("lop", json);
        Assert.assertContains("\"a\"", json);
        Assert.assertContains("\"b\"", json);
        Assert.assertContains("\"software\"", json);
    }

    private static Settings readGremlinServerSettings() throws Exception {
        return Settings.read(configPath(GREMLIN_SERVER_CONFIG).toString());
    }

    private static String readConfig(String fileName) throws IOException {
        return Files.readString(configPath(fileName), StandardCharsets.UTF_8);
    }

    private static Path configPath(String fileName) {
        return findConfDir().resolve(fileName);
    }

    private static Path serverAssemblyPath() {
        return findConfDir().getParent().getParent();
    }

    private static Path repositoryRoot() {
        Path current = Paths.get(System.getProperty("user.dir"))
                            .toAbsolutePath();
        while (current != null) {
            if (Files.isDirectory(current.resolve("hugegraph-server")) &&
                Files.isDirectory(current.resolve("hugegraph-pd"))) {
                return current;
            }
            current = current.getParent();
        }

        Assert.fail("Can't find HugeGraph repository root from " +
                    System.getProperty("user.dir"));
        return Paths.get(System.getProperty("user.dir"));
    }

    private static void collectTinkerPopVersionMismatches(
            Path pom, List<String> mismatches) {
        try {
            String content = Files.readString(pom, StandardCharsets.UTF_8);
            content = XML_COMMENT.matcher(content).replaceAll("");

            Matcher property = TINKERPOP_VERSION_PROPERTY.matcher(content);
            while (property.find()) {
                collectVersionMismatch(pom, property.group(1), mismatches);
            }

            Matcher dependency = TINKERPOP_DEPENDENCY.matcher(content);
            while (dependency.find()) {
                Matcher version = DEPENDENCY_VERSION.matcher(
                        dependency.group(1));
                if (version.find()) {
                    collectVersionMismatch(pom, version.group(1), mismatches);
                }
            }

            Matcher guice = GUICE_DEPENDENCY.matcher(content);
            while (guice.find()) {
                Matcher version = DEPENDENCY_VERSION.matcher(
                        guice.group(1));
                if (version.find()) {
                    collectGuiceVersionMismatch(pom, version.group(1),
                                                mismatches);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read " + pom, e);
        }
    }

    private static void collectGuiceVersionMismatch(Path pom, String version,
                                                    List<String> mismatches) {
        String actual = version.trim();
        if (SUPPORTED_GUICE_VERSION.equals(actual)) {
            return;
        }
        mismatches.add(repositoryRoot().relativize(pom) + "=guice:" + actual);
    }

    private static void collectVersionMismatch(Path pom, String version,
                                               List<String> mismatches) {
        String actual = version.trim();
        if (SUPPORTED_TINKERPOP_VERSION.equals(actual) ||
            "${tinkerpop.version}".equals(actual)) {
            return;
        }
        mismatches.add(repositoryRoot().relativize(pom) + "=" + actual);
    }

    private static Path findConfDir() {
        String configuredDir = System.getProperty("hugegraph.conf.dir");
        Path configuredPath = resolveConfiguredDir(configuredDir);
        if (configuredPath != null) {
            return configuredPath;
        }

        String envDir = System.getenv("HUGEGRAPH_CONF_DIR");
        Path envPath = resolveConfiguredDir(envDir);
        if (envPath != null) {
            return envPath;
        }

        Path userDir = Paths.get(System.getProperty("user.dir"));
        List<Path> candidates = new ArrayList<>();

        Path parent = userDir.getParent();
        if (parent != null) {
            candidates.add(parent.resolve("hugegraph-dist")
                                 .resolve("src")
                                 .resolve("assembly")
                                 .resolve("static")
                                 .resolve("conf"));
        }
        candidates.add(userDir.resolve("hugegraph-server")
                              .resolve("hugegraph-dist")
                              .resolve("src")
                              .resolve("assembly")
                              .resolve("static")
                              .resolve("conf"));

        for (Path candidate : candidates) {
            if (Files.isDirectory(candidate)) {
                return candidate;
            }
        }

        Assert.fail(String.format("Can't find hugegraph-dist static conf from" +
                                  " %s (hugegraph.conf.dir=%s," +
                                  " HUGEGRAPH_CONF_DIR=%s, candidates=%s)",
                                  userDir, configuredDir, envDir, candidates));
        return userDir;
    }

    private static Path resolveConfiguredDir(String path) {
        if (path == null || path.isEmpty()) {
            return null;
        }
        Path configured = Paths.get(path);
        if (Files.isDirectory(configured)) {
            return configured;
        }
        return null;
    }

    private static void assertUsesHugeGraphIoRegistry(String fileName,
                                                      String content) {
        Assert.assertTrue(fileName + " should keep HugeGraphIoRegistry",
                          content.contains(IO_REGISTRY));
    }

    private static void assertSerializerClassNamesUseUtilPackage(
            String fileName, String content) {
        Assert.assertFalse(content.contains(
                "org.apache.tinkerpop.gremlin.driver.ser."));
        Assert.assertFalse(content.contains(
                "org.apache.tinkerpop.gremlin.server.ser."));

        Matcher matcher = CLASS_NAME.matcher(content);
        boolean found = false;
        while (matcher.find()) {
            found = true;
            String className = matcher.group(1);
            Assert.assertTrue(fileName + " has outdated serializer " +
                              className,
                              className.startsWith(SERIALIZER_PACKAGE));
        }
        Assert.assertTrue("No serializer className found in " + fileName,
                          found);
    }

    private static void assertConfiguredSerializerClassesAreLoadable(
            String fileName, String content) throws ClassNotFoundException {
        Matcher matcher = CLASS_NAME.matcher(content);
        boolean found = false;
        while (matcher.find()) {
            found = true;
            Class.forName(matcher.group(1));
        }
        Assert.assertTrue("No serializer className found in " + fileName,
                          found);
    }

    private static Map<String, String> graphSONMimeTypes(Settings settings)
            throws Exception {
        Map<String, String> mimeTypes = new HashMap<>();

        for (Settings.SerializerSettings serializerSettings :
             settings.serializers) {
            if (!serializerSettings.className.startsWith(SERIALIZER_PACKAGE +
                                                         "GraphSON")) {
                continue;
            }

            MessageSerializer<?> serializer =
                    newMessageSerializer(serializerSettings.className);
            for (String mimeType : serializer.mimeTypesSupported()) {
                mimeTypes.putIfAbsent(mimeType, serializerSettings.className);
            }
        }

        return mimeTypes;
    }

    private static MessageTextSerializer<?> newTextSerializer(String className)
            throws Exception {
        MessageSerializer<?> serializer = newMessageSerializer(className);

        Assert.assertTrue(className + " should be a MessageTextSerializer",
                          serializer instanceof MessageTextSerializer);
        return (MessageTextSerializer<?>) serializer;
    }

    private static MessageSerializer<?> newMessageSerializer(String className)
            throws Exception {
        Object serializer = Class.forName(className)
                                 .getDeclaredConstructor()
                                 .newInstance();

        Assert.assertTrue(className + " should be a MessageSerializer",
                          serializer instanceof MessageSerializer);
        return (MessageSerializer<?>) serializer;
    }

    private static String serializeResponse(MessageTextSerializer<?> serializer,
                                            Object result)
            throws Exception {
        ResponseMessage response = ResponseMessage.build(UUID.randomUUID())
                                                  .code(ResponseStatusCode.SUCCESS)
                                                  .result(result)
                                                  .create();

        return serializer.serializeResponseAsString(response,
                                                    ByteBufAllocator.DEFAULT);
    }

    private static ResponseMessage roundTripResponse(
            MessageTextSerializer<?> serializer, Object result)
            throws Exception {
        ResponseMessage response = ResponseMessage.build(UUID.randomUUID())
                                                  .code(ResponseStatusCode.SUCCESS)
                                                  .result(result)
                                                  .create();
        String json = serializer.serializeResponseAsString(
                response, ByteBufAllocator.DEFAULT);
        return serializer.deserializeResponse(json);
    }

    private static ResponseMessage roundTripBinaryResponse(
            MessageSerializer<?> serializer, Object result)
            throws Exception {
        return roundTripBinaryResponse(serializer, serializer, result);
    }

    private static ResponseMessage roundTripBinaryResponse(
            MessageSerializer<?> serverSerializer,
            MessageSerializer<?> clientSerializer, Object result)
            throws Exception {
        ResponseMessage response = ResponseMessage.build(UUID.randomUUID())
                                                  .code(ResponseStatusCode.SUCCESS)
                                                  .result(result)
                                                  .create();
        ByteBuf buffer = serverSerializer.serializeResponseAsBinary(
                response, ByteBufAllocator.DEFAULT);
        try {
            return clientSerializer.deserializeResponse(buffer);
        } finally {
            buffer.release();
        }
    }

    @SuppressWarnings("unchecked")
    private static RemoteSerializerSettings readRemoteSerializerSettings(
            String fileName) throws IOException {
        try (InputStream input = Files.newInputStream(configPath(fileName))) {
            Map<String, Object> root = new Yaml().load(input);
            Map<String, Object> serializer =
                    (Map<String, Object>) root.get("serializer");

            Assert.assertNotNull("No serializer in " + fileName, serializer);
            String className = (String) serializer.get("className");
            Map<String, Object> config =
                    (Map<String, Object>) serializer.get("config");

            Assert.assertNotNull("No serializer className in " + fileName,
                                 className);
            Assert.assertNotNull("No serializer config in " + fileName,
                                 config);
            return new RemoteSerializerSettings(className, config);
        }
    }

    private static Map<String, Object> graphSONV1Config(Settings settings) {
        for (Settings.SerializerSettings serializer : settings.serializers) {
            if (GRAPHSON_UNTYPED_V1.equals(serializer.className)) {
                Assert.assertNotNull(serializer.config);
                return serializer.config;
            }
        }

        Assert.fail("No " + GRAPHSON_UNTYPED_V1 + " found in " +
                    GREMLIN_SERVER_CONFIG);
        return Collections.emptyMap();
    }

    private static Map<String, Object> config(Map<String, Object> config) {
        if (config == null) {
            return Collections.emptyMap();
        }
        return new HashMap<>(config);
    }

    private static org.apache.tinkerpop.gremlin.process.traversal.Path testPath() {
        return MutablePath.make()
                          .extend(IdGenerator.of("marko"), Set.of("a"))
                          .extend(IdGenerator.of("lop"),
                                  Set.of("b", "software"));
    }

    private static void assertCanSerializeHugeGraphTypes(
            MessageTextSerializer<?> serializer, boolean typed)
            throws Exception {
        Object id = IdGenerator.of("marko");
        Object uuidId = IdGenerator.of(
                UUID.fromString("3cfcafc8-7906-4ab7-a207-4ded056f58de"));
        Object edgeId = EdgeId.parse("S1>2>3>4>L6");
        String fileJson = serializeResponse(serializer, new File("test.text"));
        String idJson = serializeResponse(serializer, id);
        String uuidJson = serializeResponse(serializer, uuidId);
        String edgeJson = serializeResponse(serializer, edgeId);

        Assert.assertContains("\"file\"", fileJson);
        Assert.assertContains("test.text", fileJson);
        Assert.assertContains("marko", idJson);
        Assert.assertContains("3cfcafc8-7906-4ab7-a207-4ded056f58de",
                              uuidJson);
        Assert.assertContains("S1>2>3>4>L6", edgeJson);

        if (typed) {
            assertContainsGraphSONType(fileJson, "hugegraph:File");
            assertContainsGraphSONType(idJson, "hugegraph:StringId");
            assertContainsGraphSONType(uuidJson, "hugegraph:UuidId");
            assertContainsGraphSONType(edgeJson, "hugegraph:EdgeId");
        }
    }

    private static boolean usesStableGraphSONTypes(String serializer) {
        // GraphSON V1 uses legacy @class wrapping; assert stable
        // hugegraph:* @type names for V2/V3 typed fallback serializers.
        return !serializer.endsWith("GraphSONMessageSerializerV1");
    }

    private static void assertCanRoundTripHugeGraphIds(
            String serializerName, MessageTextSerializer<?> serializer)
            throws Exception {
        List<Object> ids = Arrays.asList(
                IdGenerator.of("marko"),
                IdGenerator.of(123L),
                IdGenerator.of(UUID.fromString(
                        "3cfcafc8-7906-4ab7-a207-4ded056f58de")),
                EdgeId.parse("S1>2>3>4>L6")
        );

        for (Object expected : ids) {
            ResponseMessage response = roundTripResponse(serializer, expected);
            Object actual = response.getResult().getData();
            String message = serializerName + " should round-trip " +
                             expected.getClass().getSimpleName();
            Assert.assertEquals(message, expected.getClass(),
                                actual.getClass());
            Assert.assertEquals(message, expected, actual);
        }
    }

    private static void assertCanRoundTripStandardPredicate(
            String serializerName, MessageSerializer<?> serializer)
            throws Exception {
        P<String> expected = P.eq("marko");
        ResponseMessage response = roundTripBinaryResponse(serializer,
                                                           expected);
        Object actual = response.getResult().getData();
        String message = serializerName +
                         " should round-trip a standard predicate";
        Assert.assertInstanceOf(P.class, actual);
        Assert.assertEquals(message, expected, actual);
    }

    private static void assertCanRoundTripElementProperties(
            String serializerName, MessageSerializer<?> serializer)
            throws Exception {
        ResponseMessage vertexResponse = roundTripBinaryResponse(
                serializer, vertexWithProperties());
        Object vertexResult = vertexResponse.getResult().getData();
        Assert.assertInstanceOf(Vertex.class, vertexResult);
        Vertex vertex = (Vertex) vertexResult;
        Assert.assertEquals(serializerName, 1, vertex.id());
        Assert.assertEquals(serializerName, "person", vertex.label());
        Assert.assertEquals(serializerName, "marko", vertex.value("name"));
        Assert.assertEquals(serializerName, 29,
                            ((Number) vertex.value("age")).intValue());

        ResponseMessage edgeResponse = roundTripBinaryResponse(
                serializer, edgeWithProperties());
        Object edgeResult = edgeResponse.getResult().getData();
        Assert.assertInstanceOf(Edge.class, edgeResult);
        Edge edge = (Edge) edgeResult;
        Assert.assertEquals(serializerName, 7, edge.id());
        Assert.assertEquals(serializerName, "knows", edge.label());
        Assert.assertEquals(serializerName, 0.5D,
                            (Double) edge.value("weight"), 0.0D);

        org.apache.tinkerpop.gremlin.process.traversal.Path sourcePath =
                elementPath(vertexWithProperties(), edgeWithProperties());
        Object pathResult = roundTripBinaryResponse(serializer, sourcePath)
                            .getResult().getData();
        Assert.assertInstanceOf(
                org.apache.tinkerpop.gremlin.process.traversal.Path.class,
                pathResult);
        org.apache.tinkerpop.gremlin.process.traversal.Path path =
                (org.apache.tinkerpop.gremlin.process.traversal.Path)
                        pathResult;
        Vertex pathVertex = path.get(0);
        Edge pathEdge = path.get(1);
        Assert.assertEquals(serializerName, "marko",
                            pathVertex.value("name"));
        Assert.assertEquals(serializerName, 0.5D,
                            (Double) pathEdge.value("weight"), 0.0D);
    }

    private static void assertCanRoundTripHugeGraphElements(
            String serializerName, MessageSerializer<?> serializer)
            throws Exception {
        HugeEdge expectedEdge = hugeGraphEdgeWithProperties();
        HugeVertex expectedVertex = (HugeVertex) expectedEdge.outVertex();
        MessageSerializer<?> standardClient =
                new GraphBinaryMessageSerializerV1();

        Object vertexResult = roundTripBinaryResponse(serializer,
                                                      standardClient,
                                                      expectedVertex)
                              .getResult().getData();
        Assert.assertInstanceOf(Vertex.class, vertexResult);
        Vertex vertex = (Vertex) vertexResult;
        Assert.assertEquals(serializerName, expectedVertex.id().asLong(),
                            ((Number) vertex.id()).longValue());
        Assert.assertEquals(serializerName, "person", vertex.label());
        Assert.assertEquals(serializerName, "tom", vertex.value("name"));
        Assert.assertEquals(serializerName, 18,
                            ((Number) vertex.value("age")).intValue());

        Object edgeResult = roundTripBinaryResponse(serializer, standardClient,
                                                    expectedEdge)
                            .getResult().getData();
        Assert.assertInstanceOf(Edge.class, edgeResult);
        Edge edge = (Edge) edgeResult;
        Assert.assertEquals(serializerName, expectedEdge.id().asString(),
                            edge.id().toString());
        Assert.assertEquals(serializerName, "knows", edge.label());
        Assert.assertEquals(serializerName, 0.75D,
                            (Double) edge.value("weight"), 0.0D);

        Object pathResult = roundTripBinaryResponse(
                serializer, standardClient,
                elementPath(expectedVertex, expectedEdge))
                .getResult().getData();
        Assert.assertInstanceOf(
                org.apache.tinkerpop.gremlin.process.traversal.Path.class,
                pathResult);
        org.apache.tinkerpop.gremlin.process.traversal.Path path =
                (org.apache.tinkerpop.gremlin.process.traversal.Path)
                        pathResult;
        Vertex pathVertex = path.get(0);
        Edge pathEdge = path.get(1);
        Assert.assertEquals(serializerName, "tom",
                            pathVertex.value("name"));
        Assert.assertEquals(serializerName, 0.75D,
                            (Double) pathEdge.value("weight"), 0.0D);
    }

    private static void assertUsesPrimitiveHugeGraphIds(
            String serializerName, MessageSerializer<?> serializer)
            throws Exception {
        List<Id> ids = Arrays.asList(
                IdGenerator.of("marko"),
                IdGenerator.of(123L),
                IdGenerator.of(UUID.fromString(
                        "3cfcafc8-7906-4ab7-a207-4ded056f58de")),
                EdgeId.parse("S1>2>3>4>L6")
        );
        MessageSerializer<?> standardClient =
                new GraphBinaryMessageSerializerV1();

        for (Id id : ids) {
            Object actual = roundTripBinaryResponse(serializer, standardClient,
                                                    id)
                            .getResult().getData();
            Assert.assertEquals(serializerName, id.asObject(), actual);
        }
    }

    private static void assertCanRoundTripReferenceElements(
            String serializerName, MessageSerializer<?> serializer)
            throws Exception {
        ReferenceVertex marko = new ReferenceVertex(1, "person");
        ReferenceVertex vadas = new ReferenceVertex(2, "person");
        ReferenceEdge knows = new ReferenceEdge(7, "knows", marko, vadas);

        Object vertexResult = roundTripBinaryResponse(serializer, marko)
                              .getResult().getData();
        Assert.assertInstanceOf(Vertex.class, vertexResult);
        Assert.assertFalse(serializerName,
                           ((Vertex) vertexResult).properties().hasNext());

        Object edgeResult = roundTripBinaryResponse(serializer, knows)
                            .getResult().getData();
        Assert.assertInstanceOf(Edge.class, edgeResult);
        Assert.assertFalse(serializerName,
                           ((Edge) edgeResult).properties().hasNext());

        org.apache.tinkerpop.gremlin.process.traversal.Path sourcePath =
                elementPath(marko, knows);
        Object pathResult = roundTripBinaryResponse(serializer, sourcePath)
                            .getResult().getData();
        Assert.assertInstanceOf(
                org.apache.tinkerpop.gremlin.process.traversal.Path.class,
                pathResult);
        org.apache.tinkerpop.gremlin.process.traversal.Path path =
                (org.apache.tinkerpop.gremlin.process.traversal.Path)
                        pathResult;
        Assert.assertFalse(serializerName,
                           ((Vertex) path.get(0)).properties().hasNext());
        Assert.assertFalse(serializerName,
                           ((Edge) path.get(1)).properties().hasNext());
    }

    private static void assertIncludesElementProperties(
            String serializerName, MessageTextSerializer<?> serializer)
            throws Exception {
        String vertex = serializeResponse(serializer, vertexWithProperties());
        Assert.assertTrue(serializerName, vertex.contains("properties"));
        Assert.assertTrue(serializerName, vertex.contains("name"));
        Assert.assertTrue(serializerName, vertex.contains("marko"));
        Assert.assertTrue(serializerName, vertex.contains("age"));
        Assert.assertTrue(serializerName, vertex.contains("29"));

        String edge = serializeResponse(serializer, edgeWithProperties());
        Assert.assertTrue(serializerName, edge.contains("properties"));
        Assert.assertTrue(serializerName, edge.contains("weight"));
        Assert.assertTrue(serializerName, edge.contains("0.5"));

        String path = serializeResponse(
                serializer,
                elementPath(vertexWithProperties(), edgeWithProperties()));
        Assert.assertTrue(serializerName, path.contains("properties"));
        Assert.assertTrue(serializerName, path.contains("marko"));
        Assert.assertTrue(serializerName, path.contains("weight"));
    }

    private static DetachedVertex vertexWithProperties() {
        return DetachedVertex.build()
                             .setId(1)
                             .setLabel("person")
                             .addProperty(new DetachedVertexProperty<>(
                                     11, "name", "marko",
                                     Collections.emptyMap()))
                             .addProperty(new DetachedVertexProperty<>(
                                     12, "age", 29,
                                     Collections.emptyMap()))
                             .create();
    }

    private static org.apache.tinkerpop.gremlin.process.traversal.Path
            elementPath(Vertex vertex, Edge edge) {
        return MutablePath.make()
                          .extend(vertex, Set.of("v"))
                          .extend(edge, Set.of("e"));
    }

    private static Tree<Object> elementTree() {
        Tree<Object> tree = new Tree<>();
        Tree<Object> children = new Tree<>();
        children.put(edgeWithProperties(), new Tree<>());
        tree.put(vertexWithProperties(), children);
        return tree;
    }

    private static HugeEdge hugeGraphEdgeWithProperties() {
        FakeObjects objects = new FakeObjects();
        Mockito.doReturn(new HugeFeatures(objects.graph(), false))
               .when(objects.graph()).features();
        return objects.newEdge(123, 456);
    }

    private static DetachedEdge edgeWithProperties() {
        DetachedVertex marko = DetachedVertex.build()
                                              .setId(1)
                                              .setLabel("person")
                                              .create();
        DetachedVertex vadas = DetachedVertex.build()
                                              .setId(2)
                                              .setLabel("person")
                                              .create();
        return DetachedEdge.build()
                           .setId(7)
                           .setLabel("knows")
                           .setOutV(marko)
                           .setInV(vadas)
                           .addProperty(new DetachedProperty<>("weight", 0.5D))
                           .create();
    }

    private static void assertContainsGraphSONType(String json,
                                                   String graphSONType) {
        Assert.assertContains("\"@type\"", json);
        Assert.assertContains(graphSONType, json);
    }

    private static final class RemoteSerializerSettings {

        private final String className;
        private final Map<String, Object> config;

        private RemoteSerializerSettings(String className,
                                         Map<String, Object> config) {
            this.className = className;
            this.config = config;
        }
    }
}
