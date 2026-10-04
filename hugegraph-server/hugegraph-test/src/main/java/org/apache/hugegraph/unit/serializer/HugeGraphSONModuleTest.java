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

package org.apache.hugegraph.unit.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.id.IdGenerator.LongId;
import org.apache.hugegraph.backend.id.IdGenerator.StringId;
import org.apache.hugegraph.backend.id.IdGenerator.UuidId;
import org.apache.hugegraph.io.HugeGraphIoRegistry;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.junit.Test;

public class HugeGraphSONModuleTest extends BaseUnitTest {

    @Test
    public void testSerializeFileWithGraphSONTypeInfo() throws IOException {
        String json = writeTyped(new File("test.text"));
        Map<?, ?> typedFile = JsonUtil.fromJson(json, Map.class);
        Object value = typedFile.get("@value");

        Assert.assertEquals("hugegraph:File", typedFile.get("@type"));
        Assert.assertInstanceOf(Map.class, value);
        Assert.assertEquals("test.text", ((Map<?, ?>) value).get("file"));
        Assert.assertContains("hugegraph:File", json);
        Assert.assertContains("\"file\"", json);
        Assert.assertContains("test.text", json);

        File file = readTyped(json, File.class);
        Assert.assertEquals("test.text", file.getName());
    }

    @Test
    public void testRoundTripIdWithGraphSONTypeInfo() throws IOException {
        StringId expectedStringId = (StringId) IdGenerator.of("marko");
        LongId expectedLongId = (LongId) IdGenerator.of(123L);
        UuidId expectedUuidId = (UuidId) IdGenerator.of(
                UUID.fromString("3cfcafc8-7906-4ab7-a207-4ded056f58de"));
        EdgeId expectedEdgeId = EdgeId.parse("S1>2>3>4>L6");

        String stringId = writeTyped(expectedStringId);
        String longId = writeTyped(expectedLongId);
        String uuidId = writeTyped(expectedUuidId);
        String edgeId = writeTyped(expectedEdgeId);

        Assert.assertContains("hugegraph:StringId", stringId);
        Assert.assertContains("marko", stringId);
        Assert.assertContains("hugegraph:LongId", longId);
        Assert.assertContains("123", longId);
        Assert.assertContains("hugegraph:UuidId", uuidId);
        Assert.assertContains("3cfcafc8-7906-4ab7-a207-4ded056f58de",
                              uuidId);
        Assert.assertContains("hugegraph:EdgeId", edgeId);
        Assert.assertContains("S1>2>3>4>L6", edgeId);

        Assert.assertEquals(expectedStringId,
                            readTyped(stringId, StringId.class));
        Assert.assertEquals(expectedLongId,
                            readTyped(longId, LongId.class));
        Assert.assertEquals(expectedUuidId,
                            readTyped(uuidId, UuidId.class));
        Assert.assertEquals(expectedEdgeId,
                            readTyped(edgeId, EdgeId.class));
    }

    @Test
    public void testSerializeSchemaWithUntypedGraphSONModule() throws IOException {
        FakeObjects objects = new FakeObjects();
        PropertyKey propertyKey = objects.newPropertyKey(IdGenerator.of(1L),
                                                         "name");

        String json = writeUntyped(propertyKey);

        Assert.assertContains("\"name\"", json);
    }

    private static String writeTyped(Object object) throws IOException {
        GraphSONMapper mapper = mapper(TypeInfo.PARTIAL_TYPES);
        GraphSONWriter writer = GraphSONWriter.build().mapper(mapper).create();
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        writer.writeObject(output, object);

        return output.toString(StandardCharsets.UTF_8.name());
    }

    private static <T> T readTyped(String json, Class<T> clazz)
            throws IOException {
        GraphSONMapper mapper = mapper(TypeInfo.PARTIAL_TYPES);
        GraphSONReader reader = GraphSONReader.build().mapper(mapper).create();
        ByteArrayInputStream input = new ByteArrayInputStream(
                json.getBytes(StandardCharsets.UTF_8));

        return reader.readObject(input, clazz);
    }

    private static String writeUntyped(Object object) throws IOException {
        GraphSONMapper mapper = mapper(TypeInfo.NO_TYPES);
        GraphSONWriter writer = GraphSONWriter.build().mapper(mapper).create();
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        writer.writeObject(output, object);

        return output.toString(StandardCharsets.UTF_8.name());
    }

    private static GraphSONMapper mapper(TypeInfo typeInfo) {
        GraphSONMapper mapper = GraphSONMapper.build()
                                              .version(GraphSONVersion.V3_0)
                                              .typeInfo(typeInfo)
                                              .addRegistry(HugeGraphIoRegistry.instance())
                                              .create();

        return mapper;
    }
}
