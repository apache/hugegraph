/*
 * Copyright 2017 HugeGraph Authors
 *
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

package org.apache.hugegraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.exception.NotAllowException;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.pd.client.KvClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.kv.KResponse;
import org.apache.hugegraph.pd.grpc.kv.ScanPrefixResponse;
import org.apache.hugegraph.pd.grpc.kv.WatchEvent;
import org.apache.hugegraph.pd.grpc.kv.WatchResponse;
import org.apache.hugegraph.pd.grpc.kv.WatchType;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.SchemaElement;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.slf4j.Logger;

public class SchemaDriver {
    private static final Logger log = Log.logger(SchemaDriver.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final String DELIMITER = "-";
    public static final String META_PATH_DELIMITER = "/";
    public static final String META_PATH_HUGEGRAPH = "HUGEGRAPH";
    public static final String META_PATH_GRAPHSPACE = "GRAPHSPACE";
    public static final String META_PATH_GRAPH = "GRAPH";
    public static final String META_PATH_CLUSTER = "hg";
    public static final String META_PATH_SCHEMA = "SCHEMA";
    public static final String META_PATH_GRAPH_CONF = "GRAPH_CONF";
    public static final String META_PATH_PROPERTY_KEY = "PROPERTY_KEY";
    public static final String META_PATH_VERTEX_LABEL = "VERTEX_LABEL";
    public static final String META_PATH_EDGE_LABEL = "EDGE_LABEL";
    public static final String META_PATH_INDEX_LABEL = "INDEX_LABEL";
    public static final String META_PATH_NAME = "NAME";
    public static final String META_PATH_ID = "ID";
    public static final String META_PATH_EVENT = "EVENT";
    public static final String META_PATH_REMOVE = "REMOVE";
    public static final String META_PATH_CLEAR = "CLEAR";

    private static final AtomicReference<SchemaDriver> INSTANCE =
            new AtomicReference<>();
    private static final Object LIFECYCLE_LOCK = new Object();
    private static boolean destroying;
    private static CountDownLatch destroyCompletion = new CountDownLatch(0);
    // Client for accessing PD
    private final KvClient<WatchResponse> client;

    private final SchemaCaches caches;

    private SchemaDriver(PDConfig pdConfig, int cacheSize,
                         long expiration) {
        this(new KvClient<>(pdConfig), cacheSize, expiration);
    }

    SchemaDriver(KvClient<WatchResponse> client, int cacheSize,
                 long expiration) {
        this.client = Objects.requireNonNull(client, "client");
        this.caches = new SchemaCaches(cacheSize, expiration);
        try {
            this.listenMetaChanges();
        } catch (RuntimeException e) {
            try {
                this.closeResources();
            } catch (RuntimeException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
        log.info(String.format(
                "The SchemaDriver initialized successfully, cacheSize = %s," +
                " expiration = %s s", cacheSize, expiration / 1000));
    }


    public static void init(PDConfig pdConfig) {
        init(pdConfig, 300, 300 * 1000);
    }

    public static void init(PDConfig pdConfig, int cacheSize, long expiration) {
        synchronized (LIFECYCLE_LOCK) {
            SchemaDriver instance = INSTANCE.get();
            if (instance != null) {
                throw new NotAllowException(
                        "The SchemaDriver [cacheSize=%s, expiration=%s, " +
                        "client=%s] has already been initialized and is not " +
                        "allowed to be initialized again", instance.caches.limit(),
                        instance.caches.expiration(), instance.client);
            }
            INSTANCE.set(new SchemaDriver(pdConfig, cacheSize, expiration));
        }
    }

    public static void destroy() {
        SchemaDriver instance = null;
        CountDownLatch completion;
        boolean closeResources = false;
        synchronized (LIFECYCLE_LOCK) {
            if (destroying) {
                completion = destroyCompletion;
            } else {
                instance = INSTANCE.get();
                if (instance == null) {
                    return;
                }
                destroying = true;
                completion = new CountDownLatch(1);
                destroyCompletion = completion;
                closeResources = true;
            }
        }
        if (!closeResources) {
            awaitDestroy(completion);
            return;
        }
        try {
            instance.closeResources();
        } finally {
            synchronized (LIFECYCLE_LOCK) {
                INSTANCE.compareAndSet(instance, null);
                destroying = false;
            }
            completion.countDown();
        }
    }

    private static void awaitDestroy(CountDownLatch completion) {
        boolean interrupted = false;
        while (true) {
            try {
                completion.await();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void closeResources() {
        try {
            this.client.close();
        } finally {
            this.caches.cancelScheduleCacheClean();
            this.caches.destroyAll();
        }
    }

    public SchemaCaches schemaCaches() {
        return this.caches;
    }

    public static SchemaDriver getInstance() {
        return INSTANCE.get();
    }

    private void listenMetaChanges() {
        this.listen(graphSpaceRemoveKey(), this::graphSpaceRemoveHandler);
        this.listen(graphRemoveKey(), this::graphRemoveHandler);
        this.listen(graphClearKey(), this::graphClearHandler);
        this.listen(schemaCacheClearKey(), this::schemaCacheClearHandler);
    }

    private <T> void schemaCacheClearHandler(T response) {
        List<String> names = this.extractValuesFromResponse(response);
        for (String gs : names) {
            String[] arr = gs.split(DELIMITER);
            assert arr.length == 2;
            this.caches.clear(arr[0], arr[1]);
            log.info(String.format(
                    "Graph '%s' schema clear event is received, deleting all " +
                    "schema caches under '%s'", gs, gs));
        }
    }

    private <T> void graphClearHandler(T response) {
        List<String> names = this.extractValuesFromResponse(response);
        for (String gs : names) {
            String[] arr = gs.split(DELIMITER);
            assert arr.length == 2;
            this.caches.clear(arr[0], arr[1]);
            log.info(String.format(
                    "Graph '%s' clear event is received, deleting all " +
                    "schema caches under '%s'", gs, gs));
        }
    }

    private <T> void graphRemoveHandler(T response) {
        List<String> names = this.extractValuesFromResponse(response);
        for (String gs : names) {
            String[] arr = gs.split(DELIMITER);
            assert arr.length == 2;
            this.caches.destroy(arr[0], arr[1]);
            log.info(String.format(
                    "Graph '%s' delete event is received, deleting all " +
                    "schema caches under '%s'", gs, gs));
        }
    }

    private <T> void graphSpaceRemoveHandler(T response) {
        List<String> names = this.extractValuesFromResponse(response);
        for (String gs : names) {
            this.caches.destroy(gs);
            log.info(String.format(
                    "graph space '%s' delete event is received, deleting all " +
                    "schema caches under '%s'", gs, gs));
        }
    }


    public <T> List<String> extractValuesFromResponse(T response) {
        List<String> values = new ArrayList<>();
        WatchResponse res = (WatchResponse) response;
        for (WatchEvent event : res.getEventsList()) {
            // Skip if not PUT event
            if (!event.getType().equals(WatchType.Put)) {
                return null;
            }
            String value = event.getCurrent().getValue();
            values.add(value);
        }
        return values;
    }


    public <T> void listen(String key, Consumer<T> consumer) {
        try {
            this.client.listen(key, (Consumer<WatchResponse>) consumer);
        } catch (PDException e) {
            throw new HugeException("Failed to listen '%s' to pd", e, key);
        }
    }

    public  Map<String, Object> graphConfig(String graphSpace, String graph) {
        String content = this.get(graphConfKey(graphSpace, graph));
        if (content == null || content.length() == 0) {
            return new HashMap<>();
        } else {
            return fromJson(content, Map.class);
        }
    }

    public PropertyKey propertyKey(String graphSpace, String graph, Id id,
                                   HugeGraphSupplier schemaGraph) {
        SchemaElement pk =
                this.caches.get(graphSpace, graph, HugeType.PROPERTY_KEY, id);
        if (pk == null) {
            pk = getPropertyKey(graphSpace, graph, id, schemaGraph);
            E.checkArgument(pk != null, "no such propertyKey: id = '%s'", id);
            this.caches.set(graphSpace, graph, HugeType.PROPERTY_KEY, pk.id(), pk);
            this.caches.set(graphSpace, graph, HugeType.PROPERTY_KEY, pk.name(), pk);
        }
        return (PropertyKey) pk;
    }

    public PropertyKey propertyKey(String graphSpace, String graph,
                                   String name, HugeGraphSupplier schemaGraph) {
        SchemaElement pk =
                this.caches.get(graphSpace, graph, HugeType.PROPERTY_KEY, name);
        if (pk == null) {
            pk = getPropertyKey(graphSpace, graph, name, schemaGraph);
            E.checkArgument(pk != null, "no such propertyKey: name = '%s'",
                            name);
            this.caches.set(graphSpace, graph, HugeType.PROPERTY_KEY, pk.id(), pk);
            this.caches.set(graphSpace, graph, HugeType.PROPERTY_KEY, pk.name(), pk);
        }
        return (PropertyKey) pk;
    }

    public List<PropertyKey> propertyKeys(String graphSpace, String graph,
                                          HugeGraphSupplier schemaGraph) {
        Map<String, String> propertyKeysKvs =
                this.scanWithPrefix(propertyKeyPrefix(graphSpace, graph));
        List<PropertyKey> propertyKeys =
                new ArrayList<>(propertyKeysKvs.size());
        for (String value : propertyKeysKvs.values()) {
            PropertyKey pk =
                    PropertyKey.fromMap(fromJson(value, Map.class), schemaGraph);
            this.caches.set(graphSpace, graph, HugeType.PROPERTY_KEY, pk.id(), pk);
            this.caches.set(graphSpace, graph, HugeType.PROPERTY_KEY, pk.name(), pk);
            propertyKeys.add(pk);
        }
        return propertyKeys;
    }

    public List<VertexLabel> vertexLabels(String graphSpace, String graph,
                                          HugeGraphSupplier schemaGraph) {
        Map<String, String> vertexLabelKvs = this.scanWithPrefix(
                vertexLabelPrefix(graphSpace, graph));
        List<VertexLabel> vertexLabels =
                new ArrayList<>(vertexLabelKvs.size());
        for (String value : vertexLabelKvs.values()) {
            VertexLabel vl =
                    VertexLabel.fromMap(fromJson(value, Map.class),
                                        schemaGraph);
            this.caches.set(graphSpace, graph, HugeType.VERTEX_LABEL, vl.id(), vl);
            this.caches.set(graphSpace, graph, HugeType.VERTEX_LABEL, vl.name(), vl);
            vertexLabels.add(vl);
        }
        return vertexLabels;
    }

    public List<EdgeLabel> edgeLabels(String graphSpace, String graph,
                                          HugeGraphSupplier schemaGraph) {
        Map<String, String> edgeLabelKvs = this.scanWithPrefix(
                edgeLabelPrefix(graphSpace, graph));
        List<EdgeLabel> edgeLabels =
                new ArrayList<>(edgeLabelKvs.size());
        for (String value : edgeLabelKvs.values()) {
            EdgeLabel el =
                    EdgeLabel.fromMap(fromJson(value, Map.class), schemaGraph);
            this.caches.set(graphSpace, graph, HugeType.EDGE_LABEL, el.id(), el);
            this.caches.set(graphSpace, graph, HugeType.EDGE_LABEL, el.name(), el);
            edgeLabels.add(el);
        }
        return edgeLabels;
    }

    public List<IndexLabel> indexLabels(String graphSpace, String graph,
                                          HugeGraphSupplier schemaGraph) {
        Map<String, String> indexLabelKvs = this.scanWithPrefix(
                indexLabelPrefix(graphSpace, graph));
        List<IndexLabel> indexLabels =
                new ArrayList<>(indexLabelKvs.size());
        for (String value : indexLabelKvs.values()) {
            IndexLabel il =
                    IndexLabel.fromMap(fromJson(value, Map.class), schemaGraph);
            this.caches.set(graphSpace, graph, HugeType.INDEX_LABEL, il.id(), il);
            this.caches.set(graphSpace, graph, HugeType.INDEX_LABEL, il.name(), il);
            indexLabels.add(il);
        }
        return indexLabels;
    }

    private String propertyKeyPrefix(String graphSpace, String graph) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/GRAPH/{graph
        // }/SCHEMA/PROPERTY_KEY/NAME
        return stringJoin(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           META_PATH_CLUSTER,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           graph,
                           META_PATH_SCHEMA,
                           META_PATH_PROPERTY_KEY,
                           META_PATH_NAME);
    }

    private String vertexLabelPrefix(String graphSpace, String graph) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/GRAPH/{graph
        // }/SCHEMA/VERTEX_LABEL/NAME
        return stringJoin(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           META_PATH_CLUSTER,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           graph,
                           META_PATH_SCHEMA,
                           META_PATH_VERTEX_LABEL,
                           META_PATH_NAME);
    }

    private String edgeLabelPrefix(String graphSpace, String graph) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/GRAPH/{graph
        // }/SCHEMA/EDGELABEL/NAME
        return stringJoin(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           META_PATH_CLUSTER,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           graph,
                           META_PATH_SCHEMA,
                           META_PATH_EDGE_LABEL,
                           META_PATH_NAME);
    }

    private String indexLabelPrefix(String graphSpace, String graph) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/GRAPH/{graph
        // }/SCHEMA/INDEX_LABEL/NAME
        return stringJoin(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           META_PATH_CLUSTER,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           graph,
                           META_PATH_SCHEMA,
                           META_PATH_INDEX_LABEL,
                           META_PATH_NAME);
    }

    public VertexLabel vertexLabel(String graphSpace, String graph, Id id,
                                   HugeGraphSupplier schemaGraph) {
        SchemaElement vl =
                this.caches.get(graphSpace, graph, HugeType.VERTEX_LABEL, id);
        if (vl == null) {
            vl = getVertexLabel(graphSpace, graph, id, schemaGraph);
            E.checkArgument(vl != null, "no such vertex label: id = '%s'", id);
            this.caches.set(graphSpace, graph, HugeType.VERTEX_LABEL, vl.id(), vl);
            this.caches.set(graphSpace, graph, HugeType.VERTEX_LABEL, vl.name(), vl);
        }
        return (VertexLabel) vl;
    }

    public VertexLabel vertexLabel(String graphSpace, String graph,
                                   String name, HugeGraphSupplier schemaGraph) {
        SchemaElement vl =
                this.caches.get(graphSpace, graph, HugeType.VERTEX_LABEL, name);
        if (vl == null) {
            vl = getVertexLabel(graphSpace, graph, name, schemaGraph);
            E.checkArgument(vl != null, "no such vertex label: name = '%s'",
                            name);
            this.caches.set(graphSpace, graph, HugeType.VERTEX_LABEL, vl.id(), vl);
            this.caches.set(graphSpace, graph, HugeType.VERTEX_LABEL, vl.name(), vl);
        }
        return (VertexLabel) vl;
    }

    public EdgeLabel edgeLabel(String graphSpace, String graph, Id id,
                               HugeGraphSupplier schemaGraph) {
        SchemaElement el =
                this.caches.get(graphSpace, graph, HugeType.EDGE_LABEL, id);
        if (el == null) {
            el = getEdgeLabel(graphSpace, graph, id, schemaGraph);
            E.checkArgument(el != null, "no such edge label: id = '%s'", id);
            this.caches.set(graphSpace, graph, HugeType.EDGE_LABEL, el.id(), el);
            this.caches.set(graphSpace, graph, HugeType.EDGE_LABEL, el.name(), el);
        }
        return (EdgeLabel) el;
    }

    public EdgeLabel edgeLabel(String graphSpace, String graph, String name,
                               HugeGraphSupplier schemaGraph) {
        SchemaElement el =
                this.caches.get(graphSpace, graph, HugeType.EDGE_LABEL, name);
        if (el == null) {
            el = getEdgeLabel(graphSpace, graph, name, schemaGraph);
            E.checkArgument(el != null, "no such edge label: name = '%s'",
                            name);
            this.caches.set(graphSpace, graph, HugeType.EDGE_LABEL, el.id(), el);
            this.caches.set(graphSpace, graph, HugeType.EDGE_LABEL, el.name(), el);
        }
        return (EdgeLabel) el;
    }

    public IndexLabel indexLabel(String graphSpace, String graph, Id id,
                                 HugeGraphSupplier schemaGraph) {
        SchemaElement il =
                this.caches.get(graphSpace, graph, HugeType.INDEX_LABEL, id);
        if (il == null) {
            il = getIndexLabel(graphSpace, graph, id, schemaGraph);
            E.checkArgument(il != null, "no such index label: id = '%s'", id);
            this.caches.set(graphSpace, graph, HugeType.INDEX_LABEL, il.id(), il);
            this.caches.set(graphSpace, graph, HugeType.INDEX_LABEL, il.name(), il);
        }
        return (IndexLabel) il;
    }

    public IndexLabel indexLabel(String graphSpace, String graph, String name,
                                 HugeGraphSupplier schemaGraph) {
        SchemaElement il =
                this.caches.get(graphSpace, graph, HugeType.INDEX_LABEL, name);
        if (il == null) {
            il = getIndexLabel(graphSpace, graph, name, schemaGraph);
            E.checkArgument(il != null, "no such index label: name = '%s'",
                            name);
            this.caches.set(graphSpace, graph, HugeType.INDEX_LABEL, il.id(), il);
            this.caches.set(graphSpace, graph, HugeType.INDEX_LABEL, il.name(), il);
        }
        return (IndexLabel) il;
    }

    private String get(String key) {
        try {
            KResponse response = this.client.get(key);
            return response.getValue();
        } catch (PDException e) {
            throw new HugeException("Failed to get '%s' from pd", e, key);
        }
    }

    private Map<String, String> scanWithPrefix(String prefix) {
        try {
            ScanPrefixResponse response = this.client.scanPrefix(prefix);
            return response.getKvsMap();
        } catch (PDException e) {
            throw new HugeException("Failed to scanWithPrefix '%s' from pd", e, prefix);
        }
    }

    private PropertyKey getPropertyKey(String graphSpace, String graph,
                                       Id propertyKey, HugeGraphSupplier schemaGraph) {
        String content =
                this.get(propertyKeyIdKey(graphSpace, graph, propertyKey));
        if (content == null || content.length() == 0) {
            return null;
        } else {
            return PropertyKey.fromMap(fromJson(content, Map.class), schemaGraph);
        }
    }

    private PropertyKey getPropertyKey(String graphSpace, String graph,
                                       String propertyKey, HugeGraphSupplier schemaGraph) {
        String content =
                this.get(propertyKeyNameKey(graphSpace, graph, propertyKey));
        if (content == null || content.length() == 0) {
            return null;
        } else {
            return PropertyKey.fromMap(fromJson(content, Map.class), schemaGraph);
        }
    }

    private VertexLabel getVertexLabel(String graphSpace, String graph,
                                       Id vertexLabel, HugeGraphSupplier schemaGraph) {
        String content =
                this.get(vertexLabelIdKey(graphSpace, graph, vertexLabel));
        if (content == null || content.length() == 0) {
            return null;
        } else {
            return VertexLabel.fromMap(fromJson(content, Map.class), schemaGraph);
        }
    }

    private VertexLabel getVertexLabel(String graphSpace, String graph,
                                       String vertexLabel, HugeGraphSupplier schemaGraph) {
        String content =
                this.get(vertexLabelNameKey(graphSpace, graph, vertexLabel));
        if (content == null || content.length() == 0) {
            return null;
        } else {
            return VertexLabel.fromMap(fromJson(content, Map.class), schemaGraph);
        }
    }

    private EdgeLabel getEdgeLabel(String graphSpace, String graph,
                                   Id edgeLabel, HugeGraphSupplier schemaGraph) {
        String content =
                this.get(edgeLabelIdKey(graphSpace, graph, edgeLabel));
        if (content == null || content.length() == 0) {
            return null;
        } else {
            return EdgeLabel.fromMap(fromJson(content, Map.class), schemaGraph);
        }
    }

    private EdgeLabel getEdgeLabel(String graphSpace, String graph,
                                   String edgeLabel, HugeGraphSupplier schemaGraph) {
        String content =
                this.get(edgeLabelNameKey(graphSpace, graph, edgeLabel));
        if (content == null || content.length() == 0) {
            return null;
        } else {
            return EdgeLabel.fromMap(fromJson(content, Map.class), schemaGraph);
        }
    }


    private IndexLabel getIndexLabel(String graphSpace, String graph,
                                     Id indexLabel, HugeGraphSupplier schemaGraph) {
        String content =
                this.get(indexLabelIdKey(graphSpace, graph, indexLabel));
        if (content == null || content.length() == 0) {
            return null;
        } else {
            return IndexLabel.fromMap(fromJson(content, Map.class), schemaGraph);
        }
    }

    private IndexLabel getIndexLabel(String graphSpace, String graph,
                                     String indexLabel,
                                     HugeGraphSupplier schemaGraph) {
        String content =
                this.get(indexLabelNameKey(graphSpace, graph, indexLabel));
        if (content == null || content.length() == 0) {
            return null;
        } else {
            return IndexLabel.fromMap(fromJson(content, Map.class),
                                      schemaGraph);
        }
    }


    private <T> T fromJson(String json, Class<T> clazz) {
        E.checkState(json != null, "Json value can't be null for '%s'",
                     clazz.getSimpleName());
        try {
            return MAPPER.readValue(json, clazz);
        } catch (IOException e) {
            throw new HugeException("Can't read json: %s", e, e.getMessage());
        }
    }

    private String toJson(Object object) {
        try {
            return MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new HugeException("Can't write json: %s", e, e.getMessage());
        }
    }

    private String propertyKeyIdKey(String graphSpace, String graph, Id id) {
        return idKey(graphSpace, graph, id, HugeType.PROPERTY_KEY);
    }

    private String propertyKeyNameKey(String graphSpace, String graph,
                                      String name) {
        return nameKey(graphSpace, graph, name, HugeType.PROPERTY_KEY);
    }


    private String vertexLabelIdKey(String graphSpace, String graph, Id id) {
        return idKey(graphSpace, graph, id, HugeType.VERTEX_LABEL);
    }

    private String vertexLabelNameKey(String graphSpace, String graph,
                                      String name) {
        return nameKey(graphSpace, graph, name, HugeType.VERTEX_LABEL);
    }

    private String edgeLabelIdKey(String graphSpace, String graph, Id id) {
        return idKey(graphSpace, graph, id, HugeType.EDGE_LABEL);
    }

    private String edgeLabelNameKey(String graphSpace, String graph,
                                    String name) {
        return nameKey(graphSpace, graph, name, HugeType.EDGE_LABEL);
    }

    private String indexLabelIdKey(String graphSpace, String graph, Id id) {
        return idKey(graphSpace, graph, id, HugeType.INDEX_LABEL);
    }

    private String indexLabelNameKey(String graphSpace, String graph,
                                     String name) {
        return nameKey(graphSpace, graph, name, HugeType.INDEX_LABEL);
    }

    private String graphSpaceRemoveKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPHSPACE/REMOVE
        return stringJoin(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           META_PATH_CLUSTER,
                           META_PATH_EVENT,
                           META_PATH_GRAPHSPACE,
                           META_PATH_REMOVE);
    }

    private String graphConfKey(String graphSpace, String graph) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/GRAPH_CONF/{graph}
        return stringJoin(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           META_PATH_CLUSTER,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           META_PATH_GRAPH_CONF,
                           graph);
    }

    private String nameKey(String graphSpace, String graph,
                           String name, HugeType type) {
        // HUGEGRAPH/hg/GRAPHSPACE/{graphspace}/{graph}/SCHEMA
        // /{META_PATH_TYPE}/NAME/{name}
        return stringJoin(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           META_PATH_CLUSTER,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           graph,
                           META_PATH_SCHEMA,
                           hugeType2MetaPath(type),
                           META_PATH_NAME,
                           name);
    }

    private String idKey(String graphSpace, String graph,
                         Id id, HugeType type) {
        // HUGEGRAPH/hg/GRAPHSPACE/{graphspace}/{graph}/SCHEMA
        // /{META_PATH_TYPE}/ID/{id}
        return stringJoin(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           META_PATH_CLUSTER,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           graph,
                           META_PATH_SCHEMA,
                           hugeType2MetaPath(type),
                           META_PATH_ID,
                           id.asString());
    }

    private String schemaCacheClearKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPH/SCHEMA/CLEAR
        return stringJoin(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           META_PATH_CLUSTER,
                           META_PATH_EVENT,
                           META_PATH_GRAPH,
                           META_PATH_SCHEMA,
                           META_PATH_CLEAR);
    }

    private String graphClearKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPH/CLEAR
        return stringJoin(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           META_PATH_CLUSTER,
                           META_PATH_EVENT,
                           META_PATH_GRAPH,
                           META_PATH_CLEAR);
    }

    private String graphRemoveKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPH/REMOVE
        return stringJoin(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           META_PATH_CLUSTER,
                           META_PATH_EVENT,
                           META_PATH_GRAPH,
                           META_PATH_REMOVE);
    }

    private String hugeType2MetaPath(HugeType type) {
        String schemaType = null;
        switch (type) {
            case PROPERTY_KEY:
                schemaType = META_PATH_PROPERTY_KEY;
                break;
            case VERTEX_LABEL:
                schemaType = META_PATH_VERTEX_LABEL;
                break;
            case EDGE_LABEL:
                schemaType = META_PATH_EDGE_LABEL;
                break;
            case INDEX_LABEL:
                schemaType = META_PATH_INDEX_LABEL;
                break;
            default:
                throw new AssertionError(String.format(
                        "Invalid HugeType : %s", type));
        }
        return schemaType;
    }

    private static String stringJoin(String delimiter, String... parts) {
        StringBuilder builder = new StringBuilder();
        int size = parts.length;
        for (int i = 0; i < size; i++) {
            builder.append(parts[i]);
            if (i < size - 1) {
                builder.append(delimiter);
            }
        }
        return builder.toString();
    }

    private static final class SchemaCaches {
        private final int limit;
        private final long expiration;
        private final Timer timer;

        private final ConcurrentHashMap<String, ConcurrentHashMap<String,
                SchemaElement>> caches;

        public SchemaCaches(int limit, long expiration) {
            this.expiration = expiration;
            this.limit = limit;
            this.timer = new Timer();
            this.caches = new ConcurrentHashMap<>();
            scheduleCacheCleanup();
        }

        public int limit() {
            return this.limit;
        }

        public long expiration() {
            return this.expiration;
        }

        private void scheduleCacheCleanup() {
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    log.debug("schedule clear schema caches");
                    clearAll();
                }
            }, expiration, expiration);
        }

        public void cancelScheduleCacheClean() {
            timer.cancel();
        }

        public SchemaElement get(String graphSpace, String graph, HugeType type,
                                 Id id) {
            return get(graphSpace, graph, type, id.asString());
        }

        public SchemaElement get(String graphSpace, String graph, HugeType type,
                                 String name) {
            String graphName = stringJoin(DELIMITER, graphSpace, graph);
            if (this.caches.get(graphName) == null) {
                this.caches.put(graphName, new ConcurrentHashMap<>(this.limit));
            }
            return this.caches.get(graphName)
                              .get(stringJoin(DELIMITER, type.string(), name));
        }

        public void set(String graphSpace, String graph, HugeType type, Id id,
                        SchemaElement value) {
            set(graphSpace, graph, type, id.asString(), value);
        }

        public void set(String graphSpace, String graph, HugeType type,
                        String name, SchemaElement value) {
            String graphName = stringJoin(DELIMITER, graphSpace, graph);
            ConcurrentHashMap<String, SchemaElement>
                    schemaCaches = this.caches.get(graphName);
            if (schemaCaches == null) {
                schemaCaches = this.caches.put(graphName, new ConcurrentHashMap<>(this.limit));
            }
            if (schemaCaches.size() >= limit) {
                log.info(String.format(
                        "The current '%s''s schemaCaches size '%s' reached " +
                        "limit '%s'", graphName, schemaCaches.size(), limit));
                return;
            }
            schemaCaches.put(stringJoin(DELIMITER, type.string(), name),
                             value);
            log.debug(String.format("graph '%s' add schema caches '%s'",
                                    graphName,
                                    stringJoin(DELIMITER, type.string(),
                                                name)));
        }

        public void remove(String graphSpace, String graph, HugeType type,
                           Id id) {
            remove(graphSpace, graph, type, id.asString());
        }

        public void remove(String graphSpace, String graph, HugeType type,
                           String name) {
            String graphName = stringJoin(DELIMITER, graphSpace, graph);

            ConcurrentHashMap<String, SchemaElement>
                    schemaCaches = this.caches.get(graphName);
            schemaCaches.remove(stringJoin(DELIMITER, type.string(), name));

        }

        public void clearAll() {
            for (String key : this.caches.keySet()) {
                log.debug(String.format("graph in '%s' schema caches clear",
                                        key));
                this.caches.get(key).clear();
            }
        }

        public void clear(String graphSpace, String graph) {
            ConcurrentHashMap<String, SchemaElement>
                    schemaCaches =
                    this.caches.get(stringJoin(DELIMITER, graphSpace, graph));
            if (schemaCaches != null) {
                schemaCaches.clear();
            }
        }

        public void destroyAll() {
            this.caches.clear();
        }

        public void destroy(String graphSpace, String graph) {
            this.caches.remove(stringJoin(DELIMITER, graphSpace, graph));

        }

        public void destroy(String graphSpace) {
            for (String key : this.caches.keySet()) {
                String gs = key.split(DELIMITER)[0];
                if (gs.equals(graphSpace)) {
                    this.caches.remove(key);
                }
            }
        }

    }
}
