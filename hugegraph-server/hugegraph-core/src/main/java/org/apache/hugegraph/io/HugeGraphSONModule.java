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

package org.apache.hugegraph.io;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.id.IdGenerator.LongId;
import org.apache.hugegraph.backend.id.IdGenerator.StringId;
import org.apache.hugegraph.backend.id.IdGenerator.UuidId;
import org.apache.hugegraph.backend.store.Shard;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.space.Service;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.structure.HugeProperty;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.Blob;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.SafeDateUtil;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TinkerPopJacksonModule;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.core.type.WritableTypeId;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.JsonSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.DateDeserializers.DateDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.UUIDDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.DateSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.UUIDSerializer;
import org.slf4j.Logger;

@SuppressWarnings("serial")
public class HugeGraphSONModule extends TinkerPopJacksonModule {

    private static final Logger LOG = Log.logger(HugeGraphSONModule.class);

    private static final long serialVersionUID = 6480426922914059122L;

    private static final String TYPE_NAMESPACE = "hugegraph";

    private static boolean OPTIMIZE_SERIALIZE = true;

    @SuppressWarnings("rawtypes")
    private static final Map<Class, String> TYPE_DEFINITIONS;

    private static final GraphSONSchemaSerializer SCHEMA_SERIALIZER =
            new GraphSONSchemaSerializer();

    // NOTE: jackson will synchronize DateFormat
    private static final String DF = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final DateFormat DATE_FORMAT = new SimpleDateFormat(DF);

    static {
        TYPE_DEFINITIONS = new ConcurrentHashMap<>();

        TYPE_DEFINITIONS.put(Optional.class, "Optional");
        TYPE_DEFINITIONS.put(Date.class, "Date");
        TYPE_DEFINITIONS.put(UUID.class, "UUID");

        // HugeGraph id serializer
        TYPE_DEFINITIONS.put(StringId.class, "StringId");
        TYPE_DEFINITIONS.put(LongId.class, "LongId");
        TYPE_DEFINITIONS.put(EdgeId.class, "EdgeId");

        // HugeGraph schema serializer
        TYPE_DEFINITIONS.put(PropertyKey.class, "PropertyKey");
        TYPE_DEFINITIONS.put(VertexLabel.class, "VertexLabel");
        TYPE_DEFINITIONS.put(EdgeLabel.class, "EdgeLabel");
        TYPE_DEFINITIONS.put(IndexLabel.class, "IndexLabel");

        // HugeGraph vertex serializer
        TYPE_DEFINITIONS.put(HugeVertex.class, "HugeVertex");
        // TYPE_DEFINITIONS.put(HugeEdge.class, "HugeEdge");

        // HugeGraph shard serializer
        TYPE_DEFINITIONS.put(Shard.class, "Shard");

        // HugeGraph space and service serializer
        TYPE_DEFINITIONS.put(GraphSpace.class, "GraphSpace");
        TYPE_DEFINITIONS.put(Service.class, "Service");
    }

    public static void register(HugeGraphIoRegistry io) {
        io.register(GraphSONIo.class, null, new HugeGraphSONModule());
    }

    private HugeGraphSONModule() {
        super(TYPE_NAMESPACE);

        registerCommonSerializers(this);

        // HugeGraph id serializer
        registerIdSerializers(this);

        // HugeGraph schema serializer
        registerSchemaSerializers(this);

        // HugeGraph vertex/edge serializer
        if (OPTIMIZE_SERIALIZE) {
            registerGraphSerializers(this);
        }

        // HugeGraph space and service serializer
        registerGraphSpaceSerializers(this);
        registerServiceSerializers(this);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<Class, String> getTypeDefinitions() {
        return TYPE_DEFINITIONS;
    }

    @Override
    public String getTypeNamespace() {
        return TYPE_NAMESPACE;
    }

    public static void registerCommonSerializers(SimpleModule module) {
        module.addSerializer(Optional.class, new OptionalSerializer());

        module.addSerializer(Shard.class, new ShardSerializer());

        module.addSerializer(File.class, new FileSerializer());

        boolean useTimestamp = false;
        module.addSerializer(Date.class,
                             new DateSerializer(useTimestamp, DATE_FORMAT));
        module.addDeserializer(Date.class, new DateDeserializer(
                new DateDeserializer(), DATE_FORMAT, DF));

        module.addSerializer(UUID.class, new UUIDSerializer());
        module.addDeserializer(UUID.class, new UUIDDeserializer());

        module.addSerializer(Blob.class, new BlobSerializer());
        module.addDeserializer(Blob.class, new BlobDeserializer());
    }

    public static void registerIdSerializers(SimpleModule module) {
        module.addSerializer(StringId.class,
                             new IdSerializer<>(StringId.class));
        module.addDeserializer(StringId.class,
                               new IdDeserializer<>(StringId.class));

        module.addSerializer(LongId.class,
                             new IdSerializer<>(LongId.class));
        module.addDeserializer(LongId.class,
                               new IdDeserializer<>(LongId.class));

        module.addSerializer(UuidId.class,
                             new IdSerializer<>(UuidId.class));
        module.addDeserializer(UuidId.class,
                               new IdDeserializer<>(UuidId.class));

        module.addSerializer(EdgeId.class,
                             new IdSerializer<>(EdgeId.class));
        module.addDeserializer(EdgeId.class,
                               new IdDeserializer<>(EdgeId.class));
    }

    public static void registerSchemaSerializers(SimpleModule module) {
        module.addSerializer(PropertyKey.class, new PropertyKeySerializer());
        module.addSerializer(VertexLabel.class, new VertexLabelSerializer());
        module.addSerializer(EdgeLabel.class, new EdgeLabelSerializer());
        module.addSerializer(IndexLabel.class, new IndexLabelSerializer());
    }

    public static void registerGraphSerializers(SimpleModule module) {
        /*
         * Use customized serializer need to be compatible with V1 and V2
         * Graphson, and seems need to implement edge deserializer, it is
         * a little complicated.
         */
        module.addSerializer(HugeVertex.class, new HugeVertexSerializer());
        module.addSerializer(HugeEdge.class, new HugeEdgeSerializer());

        module.addSerializer(Path.class, new PathSerializer());
        module.addSerializer(Tree.class, new TreeSerializer());
    }

    public static void registerGraphSpaceSerializers(SimpleModule module) {
        module.addSerializer(GraphSpace.class, new GraphSpaceSerializer());
        module.addDeserializer(GraphSpace.class, new GraphSpaceDeserializer());
    }

    public static void registerServiceSerializers(SimpleModule module) {
        module.addSerializer(Service.class, new ServiceSerializer());
        module.addDeserializer(Service.class, new ServiceDeserializer());
    }

    @SuppressWarnings("rawtypes")
    private static class OptionalSerializer extends StdSerializer<Optional> {

        public OptionalSerializer() {
            super(Optional.class);
        }

        @Override
        public void serialize(Optional optional,
                              JsonGenerator jsonGenerator,
                              SerializerProvider provider)
                throws IOException {
            if (optional.isPresent()) {
                jsonGenerator.writeObject(optional.get());
            } else {
                jsonGenerator.writeObject(null);
            }
        }
    }

    private static class GraphSpaceSerializer
            extends StdSerializer<GraphSpace> {

        public GraphSpaceSerializer() {
            super(GraphSpace.class);
        }

        @Override
        public void serialize(GraphSpace gs,
                              JsonGenerator jsonGenerator,
                              SerializerProvider provider)
                throws IOException {
            jsonGenerator.writeStartObject();
            for (Map.Entry<String, Object> entry : gs.info().entrySet()) {
                jsonGenerator.writeFieldName(entry.getKey());
                jsonGenerator.writeObject(entry.getValue());
            }
            jsonGenerator.writeEndObject();
        }
    }

    private static class GraphSpaceDeserializer
            extends StdDeserializer<GraphSpace> {

        public GraphSpaceDeserializer() {
            super(GraphSpace.class);
        }

        @Override
        public GraphSpace deserialize(JsonParser jsonParser,
                                      DeserializationContext ctxt)
                throws IOException {
            if (jsonParser.getCurrentToken() != JsonToken.START_OBJECT) {
                throw new HugeException("Invalid start marker");
            }

            String name = null;
            String nickname = null;
            String description = null;

            Number maxGraphNumber = 0;
            Number maxRoleNumber = 0;

            Number cpuLimit = 0;
            Number memoryLimit = 0;
            Number storageLimit = 0;

            Number computeCpuLimit = 0;
            Number computeMemoryLimit = 0;

            String oltpNamespace = null;
            String olapNamespace = null;
            String storageNamespace = null;

            Number cpuUsed = 0;
            Number memoryUsed = 0;
            Number storageUsed = 0;
            Number graphNumberUsed = 0;
            Number roleNumberUsed = 0;
            Boolean auth = false;

            String operatorImagePath = "";
            String internalAlgorithmImageUrl = "";

            String creator = GraphSpace.DEFAULT_CREATOR_NAME;
            Date create = null;
            Date update = null;

            Map<String, Object> configs = new HashMap<>();
            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                String fieldName = jsonParser.getCurrentName();
                jsonParser.nextToken();
                if ("name".equals(fieldName)) {
                    name = jsonParser.getText();
                } else if ("nickname".equals(fieldName)) {
                    nickname = jsonParser.getText();
                } else if ("description".equals(fieldName)) {
                    description = jsonParser.getText();
                } else if ("max_graph_number".equals(fieldName)) {
                    maxGraphNumber = jsonParser.getNumberValue();
                } else if ("max_role_number".equals(fieldName)) {
                    maxRoleNumber = jsonParser.getNumberValue();
                } else if ("cpu_limit".equals(fieldName)) {
                    cpuLimit = jsonParser.getNumberValue();
                } else if ("memory_limit".equals(fieldName)) {
                    memoryLimit = jsonParser.getNumberValue();
                } else if ("compute_cpu_limit".equals(fieldName)) {
                    computeCpuLimit = jsonParser.getNumberValue();
                } else if ("compute_memory_limit".equals(fieldName)) {
                    computeMemoryLimit = jsonParser.getNumberValue();
                } else if ("storage_limit".equals(fieldName)) {
                    storageLimit = jsonParser.getNumberValue();
                } else if ("oltp_namespace".equals(fieldName)) {
                    oltpNamespace = jsonParser.getText();
                } else if ("olap_namespace".equals(fieldName)) {
                    olapNamespace = jsonParser.getText();
                } else if ("storage_namespace".equals(fieldName)) {
                    storageNamespace = jsonParser.getText();
                } else if ("cpu_used".equals(fieldName)) {
                    cpuUsed = jsonParser.getNumberValue();
                } else if ("memory_used".equals(fieldName)) {
                    memoryUsed = jsonParser.getNumberValue();
                } else if ("storage_used".equals(fieldName)) {
                    storageUsed = jsonParser.getNumberValue();
                } else if ("graph_number_used".equals(fieldName)) {
                    graphNumberUsed = jsonParser.getNumberValue();
                } else if ("role_number_used".equals(fieldName)) {
                    roleNumberUsed = jsonParser.getNumberValue();
                } else if ("auth".equals(fieldName)) {
                    auth = jsonParser.getBooleanValue();
                } else if ("operator_image_path".equals(fieldName)) {
                    operatorImagePath = jsonParser.getText();
                } else if ("internal_algorithm_image_url".equals(fieldName)) {
                    internalAlgorithmImageUrl = jsonParser.getText();
                } else if ("creator".equals(fieldName)) {
                    creator = jsonParser.getText();
                } else if ("create_time".equals(fieldName)) {
                    String val = jsonParser.getValueAsString();
                    if (val == null) {
                        create = new Date();
                    } else {
                        try {
                            create = SafeDateUtil.parse(val, DF);
                        } catch (ParseException e) {
                            LOG.warn("Failed to parse date: {}", val, e);
                            create = new Date();
                        }
                    }
                } else if ("update_time".equals(fieldName)) {
                    String val = jsonParser.getValueAsString();
                    if (val == null) {
                        update = new Date();
                    } else {
                        try {
                            update = SafeDateUtil.parse(val, DF);
                        } catch (ParseException e) {
                            e.printStackTrace();
                            update = new Date();
                        }
                    }
                } else {
                    configs.put(fieldName, jsonParser.getValueAsString());
                }
            }
            jsonParser.close();

            GraphSpace space = new GraphSpace(name, nickname, description,
                                              cpuLimit.intValue(),
                                              memoryLimit.intValue(),
                                              storageLimit.intValue(),
                                              maxGraphNumber.intValue(),
                                              maxRoleNumber.intValue(),
                                              oltpNamespace,
                                              olapNamespace,
                                              storageNamespace,
                                              cpuUsed.intValue(),
                                              memoryUsed.intValue(),
                                              storageUsed.intValue(),
                                              graphNumberUsed.intValue(),
                                              roleNumberUsed.intValue(),
                                              auth,
                                              creator,
                                              configs);

            space.updateTime(update);
            space.createTime(create);
            space.computeCpuLimit(computeCpuLimit.intValue());
            space.computeMemoryLimit(computeMemoryLimit.intValue());
            space.operatorImagePath(operatorImagePath);
            space.internalAlgorithmImageUrl(internalAlgorithmImageUrl);
            return space;
        }
    }

    private static class ServiceSerializer
            extends StdSerializer<Service> {

        public ServiceSerializer() {
            super(Service.class);
        }

        @Override
        public void serialize(Service service,
                              JsonGenerator jsonGenerator,
                              SerializerProvider provider)
                throws IOException {
            jsonGenerator.writeStartObject();
            for (Map.Entry<String, Object> entry : service.info().entrySet()) {
                jsonGenerator.writeFieldName(entry.getKey());
                jsonGenerator.writeObject(entry.getValue());
            }
            jsonGenerator.writeEndObject();
        }
    }

    private static class ServiceDeserializer
            extends StdDeserializer<Service> {

        public ServiceDeserializer() {
            super(Service.class);
        }

        @Override
        public Service deserialize(JsonParser jsonParser,
                                   DeserializationContext ctxt)
                throws IOException {
            if (jsonParser.getCurrentToken() != JsonToken.START_OBJECT) {
                throw new HugeException("Invalid start marker");
            }

            String name = null;
            String description = null;
            String type = null;
            String deploymentType = null;
            String status = "UNKNOWN";

            Number count = 0;
            Number running = 0;

            Number cpuLimit = 0;
            Number memoryLimit = 0;
            Number storageLimit = 0;

            String routeType = null;
            Number port = 8080;

            Set<String> urls = new HashSet<>();
            Set<String> serverDdsUrls = new HashSet<>();
            Set<String> serverNodePortUrls = new HashSet<>();

            String serviceId = null;
            String pdServiceId = null;

            String creator = null;
            Date createTime = null;
            Date updateTime = null;

            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                String fieldName = jsonParser.getCurrentName();
                jsonParser.nextToken();
                if ("name".equals(fieldName)) {
                    name = jsonParser.getText();
                } else if ("description".equals(fieldName)) {
                    description = jsonParser.getText();
                } else if ("type".equals(fieldName)) {
                    type = jsonParser.getText();
                } else if ("deployment_type".equals(fieldName)) {
                    deploymentType = jsonParser.getText();
                } else if ("status".equals(fieldName)) {
                    status = jsonParser.getText();
                } else if ("count".equals(fieldName)) {
                    count = jsonParser.getNumberValue();
                } else if ("running".equals(fieldName)) {
                    running = jsonParser.getNumberValue();
                } else if ("cpu_limit".equals(fieldName)) {
                    cpuLimit = jsonParser.getNumberValue();
                } else if ("memory_limit".equals(fieldName)) {
                    memoryLimit = jsonParser.getNumberValue();
                } else if ("storage_limit".equals(fieldName)) {
                    storageLimit = jsonParser.getNumberValue();
                } else if ("route_type".equals(fieldName)) {
                    routeType = jsonParser.getText();
                } else if ("port".equals(fieldName)) {
                    port = jsonParser.getNumberValue();
                } else if ("urls".equals(fieldName)) {
                    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                        String urlString = jsonParser.getText();
                        urls.addAll(Arrays.asList(urlString.split(",")));
                    }
                } else if ("server_dds_urls".equals(fieldName)) {
                    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                        String urlString = jsonParser.getText();
                        serverDdsUrls.addAll(Arrays.asList(urlString.split(",")));
                    }
                } else if ("server_node_port_urls".equals(fieldName)) {
                    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                        String urlString = jsonParser.getText();
                        serverNodePortUrls.addAll(Arrays.asList(urlString.split(",")));
                    }
                } else if ("service_id".equals(fieldName)) {
                    serviceId = jsonParser.getText();
                } else if ("pd_service_id".equals(fieldName)) {
                    pdServiceId = jsonParser.getText();
                } else if ("creator".equals(fieldName)) {
                    creator = jsonParser.getText();
                } else if ("create_time".equals(fieldName)) {
                    String val = jsonParser.getValueAsString();
                    try {
                        createTime = SafeDateUtil.parse(val, DF);
                    } catch (ParseException e) {
                        e.printStackTrace();
                        createTime = new Date();
                    }
                } else if ("update_time".equals(fieldName)) {
                    String val = jsonParser.getValueAsString();
                    try {
                        updateTime = SafeDateUtil.parse(val, DF);
                    } catch (ParseException e) {
                        e.printStackTrace();
                        updateTime = new Date();
                    }
                } else {
                    // throw new HugeException("Invalid field '%s'", fieldName);
                    LOG.error("Deserialize Service",
                              new HugeException("Invalid field %s", fieldName));

                }
            }
            jsonParser.close();

            Service service = new Service(name, creator, description,
                                          Service.ServiceType.valueOf(type),
                                          Service.DeploymentType.valueOf(deploymentType),
                                          count.intValue(),
                                          running.intValue(),
                                          cpuLimit.intValue(),
                                          memoryLimit.intValue(),
                                          storageLimit.intValue(),
                                          routeType,
                                          port.intValue(),
                                          urls);
            service.serverDdsUrls(serverDdsUrls);
            service.serverNodePortUrls(serverNodePortUrls);
            service.status(Service.Status.valueOf(status));
            service.serviceId(serviceId);
            service.pdServiceId(pdServiceId);
            service.createTime(createTime);
            service.updateTime(updateTime);
            return service;
        }
    }

    private static class IdSerializer<T extends Id> extends StdSerializer<T> {

        public IdSerializer(Class<T> clazz) {
            super(clazz);
        }

        @Override
        public void serialize(T value,
                              JsonGenerator jsonGenerator,
                              SerializerProvider provider)
                throws IOException {
            if (value.number()) {
                jsonGenerator.writeNumber(value.asLong());
            } else {
                jsonGenerator.writeString(value.asString());
            }
        }

        @Override
        public void serializeWithType(T value,
                                      JsonGenerator jsonGenerator,
                                      SerializerProvider provider,
                                      TypeSerializer typeSer)
                throws IOException {
            // https://github.com/FasterXML/jackson-databind/issues/2320
            WritableTypeId typeId = typeSer.typeId(value, JsonToken.VALUE_STRING);
            typeSer.writeTypePrefix(jsonGenerator, typeId);
            this.serialize(value, jsonGenerator, provider);
            typeSer.writeTypeSuffix(jsonGenerator, typeId);
        }
    }

    private static class IdDeserializer<T extends Id>
            extends StdDeserializer<T> {

        public IdDeserializer(Class<T> clazz) {
            super(clazz);
        }

        @SuppressWarnings("unchecked")
        @Override
        public T deserialize(JsonParser jsonParser,
                             DeserializationContext ctxt)
                throws IOException {
            Class<?> clazz = this.handledType();
            if (clazz.equals(LongId.class)) {
                Number idValue = ctxt.readValue(jsonParser, Number.class);
                return (T) IdGenerator.of(idValue.longValue());
            } else if (clazz.equals(StringId.class)) {
                String idValue = ctxt.readValue(jsonParser, String.class);
                return (T) IdGenerator.of(idValue);
            } else if (clazz.equals(UuidId.class)) {
                UUID idValue = ctxt.readValue(jsonParser, UUID.class);
                return (T) IdGenerator.of(idValue);
            } else {
                assert clazz.equals(EdgeId.class);
                String idValue = ctxt.readValue(jsonParser, String.class);
                return (T) EdgeId.parse(idValue);
            }
        }
    }

    private static class PropertyKeySerializer
            extends StdSerializer<PropertyKey> {

        public PropertyKeySerializer() {
            super(PropertyKey.class);
        }

        @Override
        public void serialize(PropertyKey pk,
                              JsonGenerator jsonGenerator,
                              SerializerProvider provider)
                throws IOException {
            writeEntry(jsonGenerator, SCHEMA_SERIALIZER.writePropertyKey(pk));
        }
    }

    private static class VertexLabelSerializer
            extends StdSerializer<VertexLabel> {

        public VertexLabelSerializer() {
            super(VertexLabel.class);
        }

        @Override
        public void serialize(VertexLabel vl,
                              JsonGenerator jsonGenerator,
                              SerializerProvider provider)
                throws IOException {
            writeEntry(jsonGenerator, SCHEMA_SERIALIZER.writeVertexLabel(vl));
        }
    }

    private static class EdgeLabelSerializer extends StdSerializer<EdgeLabel> {

        public EdgeLabelSerializer() {
            super(EdgeLabel.class);
        }

        @Override
        public void serialize(EdgeLabel el,
                              JsonGenerator jsonGenerator,
                              SerializerProvider provider)
                throws IOException {
            writeEntry(jsonGenerator, SCHEMA_SERIALIZER.writeEdgeLabel(el));
        }
    }

    private static class IndexLabelSerializer
            extends StdSerializer<IndexLabel> {

        public IndexLabelSerializer() {
            super(IndexLabel.class);
        }

        @Override
        public void serialize(IndexLabel il,
                              JsonGenerator jsonGenerator,
                              SerializerProvider provider)
                throws IOException {
            writeEntry(jsonGenerator, SCHEMA_SERIALIZER.writeIndexLabel(il));
        }
    }

    private static void writeEntry(JsonGenerator jsonGenerator,
                                   Map<HugeKeys, Object> schema)
            throws IOException {
        jsonGenerator.writeStartObject();
        for (Map.Entry<HugeKeys, Object> entry : schema.entrySet()) {
            jsonGenerator.writeFieldName(entry.getKey().string());
            jsonGenerator.writeObject(entry.getValue());
        }
        jsonGenerator.writeEndObject();
    }

    protected abstract static class HugeElementSerializer<T extends HugeElement>
            extends StdSerializer<T> {

        public HugeElementSerializer(Class<T> clazz) {
            super(clazz);
        }

        public void writeIdField(String fieldName, Id id,
                                 JsonGenerator generator)
                throws IOException {
            generator.writeFieldName(fieldName);
            if (id.number()) {
                generator.writeNumber(id.asLong());
            } else {
                generator.writeString(id.asString());
            }
        }

        public void writePropertiesField(Collection<HugeProperty<?>> properties,
                                         JsonGenerator generator,
                                         SerializerProvider provider)
                throws IOException {
            // Start write properties
            generator.writeFieldName("properties");
            generator.writeStartObject();

            for (HugeProperty<?> property : properties) {
                String key = property.key();
                Object val = property.value();
                try {
                    generator.writeFieldName(key);
                    if (val != null) {
                        JsonSerializer<Object> serializer =
                                provider.findValueSerializer(val.getClass());
                        serializer.serialize(val, generator, provider);
                    } else {
                        generator.writeNull();
                    }
                } catch (IOException e) {
                    throw new HugeException(
                            "Failed to serialize property(%s: %s) " +
                            "for vertex '%s'", key, val, property.element());
                }
            }
            // End write properties
            generator.writeEndObject();
        }
    }

    private static class HugeVertexSerializer
            extends HugeElementSerializer<HugeVertex> {

        public HugeVertexSerializer() {
            super(HugeVertex.class);
        }

        @Override
        public void serialize(HugeVertex vertex, JsonGenerator generator,
                              SerializerProvider provider)
                throws IOException {
            vertex.forceLoad();
            generator.writeStartObject();

            this.writeIdField("id", vertex.id(), generator);
            generator.writeStringField("label", vertex.label());
            generator.writeStringField("type", "vertex");

            this.writePropertiesField(vertex.getFilledProperties(),
                                      generator, provider);

            generator.writeEndObject();
        }

        @Override
        public void serializeWithType(HugeVertex value, JsonGenerator generator,
                                      SerializerProvider provider,
                                      TypeSerializer typeSer)
                throws IOException {
            WritableTypeId typeId = typeSer.typeId(value, JsonToken.VALUE_STRING);
            typeSer.writeTypePrefix(generator, typeId);
            this.serialize(value, generator, provider);
            typeSer.writeTypeSuffix(generator, typeId);
        }
    }

    private static class HugeEdgeSerializer
            extends HugeElementSerializer<HugeEdge> {

        public HugeEdgeSerializer() {
            super(HugeEdge.class);
        }

        @Override
        public void serialize(HugeEdge edge, JsonGenerator generator,
                              SerializerProvider provider)
                throws IOException {
            edge.forceLoad();
            generator.writeStartObject();

            // Write id, label, type
            this.writeIdField("id", edge.id(), generator);
            generator.writeStringField("label", edge.label());
            generator.writeStringField("type", "edge");

            HugeVertex outVertex = (HugeVertex) edge.outVertex();
            HugeVertex inVertex = (HugeVertex) edge.inVertex();
            this.writeIdField("outV", outVertex.id(), generator);
            generator.writeStringField("outVLabel", outVertex.label());
            this.writeIdField("inV", inVertex.id(), generator);
            generator.writeStringField("inVLabel", inVertex.label());

            this.writePropertiesField(edge.getFilledProperties(),
                                      generator, provider);

            generator.writeEndObject();
        }

        @Override
        public void serializeWithType(HugeEdge value, JsonGenerator generator,
                                      SerializerProvider provider,
                                      TypeSerializer typeSer)
                throws IOException {
            WritableTypeId typeId = typeSer.typeId(value, JsonToken.VALUE_STRING);
            typeSer.writeTypePrefix(generator, typeId);
            this.serialize(value, generator, provider);
            typeSer.writeTypeSuffix(generator, typeId);
        }
    }

    private static class PathSerializer extends StdSerializer<Path> {

        public PathSerializer() {
            super(Path.class);
        }

        @Override
        public void serialize(Path path, JsonGenerator jsonGenerator,
                              SerializerProvider provider) throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeObjectField(GraphSONTokens.LABELS,
                                           path.labels());
            jsonGenerator.writeObjectField(GraphSONTokens.OBJECTS,
                                           path.objects());
            jsonGenerator.writeEndObject();
        }
    }

    @SuppressWarnings("rawtypes") // Tree<T>
    private static class TreeSerializer extends StdSerializer<Tree> {

        public TreeSerializer() {
            super(Tree.class);
        }

        @Override
        public void serialize(Tree tree, JsonGenerator jsonGenerator,
                              SerializerProvider provider) throws IOException {
            jsonGenerator.writeStartArray();
            @SuppressWarnings("unchecked")
            Set<Map.Entry<Element, Tree>> set = tree.entrySet();
            for (Map.Entry<Element, Tree> entry : set) {
                jsonGenerator.writeStartObject();
                jsonGenerator.writeObjectField(GraphSONTokens.KEY,
                                               entry.getKey());
                jsonGenerator.writeObjectField(GraphSONTokens.VALUE,
                                               entry.getValue());
                jsonGenerator.writeEndObject();
            }
            jsonGenerator.writeEndArray();
        }
    }

    private static class ShardSerializer extends StdSerializer<Shard> {

        public ShardSerializer() {
            super(Shard.class);
        }

        @Override
        public void serialize(Shard shard, JsonGenerator jsonGenerator,
                              SerializerProvider provider) throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("start", shard.start());
            jsonGenerator.writeStringField("end", shard.end());
            jsonGenerator.writeNumberField("length", shard.length());
            jsonGenerator.writeEndObject();
        }
    }

    private static class FileSerializer extends StdSerializer<File> {

        public FileSerializer() {
            super(File.class);
        }

        @Override
        public void serialize(File file, JsonGenerator jsonGenerator,
                              SerializerProvider provider) throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("file", file.getName());
            jsonGenerator.writeEndObject();
        }
    }

    private static class BlobSerializer extends StdSerializer<Blob> {

        public BlobSerializer() {
            super(Blob.class);
        }

        @Override
        public void serialize(Blob blob, JsonGenerator jsonGenerator,
                              SerializerProvider provider) throws IOException {
            jsonGenerator.writeBinary(blob.bytes());
        }
    }

    private static class BlobDeserializer extends StdDeserializer<Blob> {

        public BlobDeserializer() {
            super(Blob.class);
        }

        @Override
        public Blob deserialize(JsonParser jsonParser,
                                DeserializationContext ctxt)
                throws IOException {
            byte[] bytes = jsonParser.getBinaryValue();
            return Blob.wrap(bytes);
        }
    }
}
