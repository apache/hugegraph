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

package org.apache.hugegraph.serializer;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.auth.SchemaDefine.AuthElement;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.page.PageInfo;
import org.apache.hugegraph.iterator.Metadatable;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.space.SchemaTemplate;
import org.apache.hugegraph.space.Service;
import org.apache.hugegraph.traversal.algorithm.CustomizedCrosspointsTraverser.CrosspointsPaths;
import org.apache.hugegraph.traversal.algorithm.FusiformSimilarityTraverser.SimilarsMap;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.algorithm.SingleSourceShortestPathTraverser.NodeWithWeight;
import org.apache.hugegraph.traversal.algorithm.SingleSourceShortestPathTraverser.WeightedPaths;
import org.apache.hugegraph.traversal.optimize.TraversalUtil;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class JsonSerializer implements Serializer {

    private static final int LBUF_SIZE = 1024;
    private static final String MEASURE_KEY = "measure";
    private static final JsonSerializer INSTANCE = new JsonSerializer();
    private Map<String, Object> apiMeasure = null;

    private JsonSerializer() {
    }

    private JsonSerializer(Map<String, Object> apiMeasure) {
        this.apiMeasure = apiMeasure;
    }

    public static JsonSerializer instance() {
        return INSTANCE;
    }

    public static JsonSerializer instance(Map<String, Object> apiMeasure) {
        return new JsonSerializer(apiMeasure);
    }

    @Override
    public String writeMap(Map<?, ?> map) {
        ImmutableMap.Builder<Object, Object> builder = ImmutableMap.builder();
        builder.putAll(map);
        if (this.apiMeasure != null) {
            builder.put(MEASURE_KEY, this.apiMeasure);
        }
        return JsonUtil.toJson(builder.build());
    }

    @Override
    public String writeList(String label, Collection<?> list) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(LBUF_SIZE)) {
            out.write(String.format("{\"%s\": ", label).getBytes(API.CHARSET));
            out.write(JsonUtil.toJson(list).getBytes(API.CHARSET));
            if (this.apiMeasure != null) {
                out.write(String.format(",\"%s\": ", MEASURE_KEY).getBytes(API.CHARSET));
                out.write(JsonUtil.toJson(this.apiMeasure).getBytes(API.CHARSET));
            }
            out.write("}".getBytes(API.CHARSET));
            return out.toString(API.CHARSET);
        } catch (Exception e) {
            throw new HugeException("Failed to serialize %s", e, label);
        }
    }

    private String writeIterator(String label, Iterator<?> iter,
                                 boolean paging) {
        // Early throw if needed
        iter.hasNext();

        // Serialize Iterator
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(LBUF_SIZE)) {
            out.write("{".getBytes(API.CHARSET));

            out.write(String.format("\"%s\":[", label).getBytes(API.CHARSET));

            // Write data
            boolean first = true;
            while (iter.hasNext()) {
                if (!first) {
                    out.write(",".getBytes(API.CHARSET));
                } else {
                    first = false;
                }
                out.write(JsonUtil.toJson(iter.next()).getBytes(API.CHARSET));
            }
            out.write("]".getBytes(API.CHARSET));

            // Write page
            if (paging) {
                String page;
                if (iter instanceof GraphTraversal<?, ?>) {
                    page = TraversalUtil.page((GraphTraversal<?, ?>) iter);
                } else if (iter instanceof Metadatable) {
                    page = PageInfo.pageInfo(iter);
                } else {
                    throw new HugeException("Invalid paging iterator: %s",
                                            iter.getClass());
                }
                if (page != null) {
                    page = String.format(",\"page\": \"%s\"", page);
                } else {
                    page = ",\"page\": null";
                }
                out.write(page.getBytes(API.CHARSET));
            }

            if (this.apiMeasure != null) {
                out.write(String.format(",\"%s\":[", MEASURE_KEY).getBytes(API.CHARSET));
                out.write(JsonUtil.toJson(this.apiMeasure).getBytes(API.CHARSET));
            }

            out.write("}".getBytes(API.CHARSET));
            return out.toString(API.CHARSET);
        } catch (HugeException e) {
            throw e;
        } catch (Exception e) {
            throw new HugeException("Failed to serialize %s", e, label);
        } finally {
            try {
                CloseableIterator.closeIterator(iter);
            } catch (Exception e) {
                throw new HugeException("Failed to close for %s", e, label);
            }
        }
    }

    @Override
    public String writePropertyKey(PropertyKey propertyKey) {
        return JsonUtil.toJson(propertyKey);
    }

    @Override
    public String writeTaskWithSchema(
            SchemaElement.TaskWithSchema taskWithSchema) {
        StringBuilder builder = new StringBuilder();
        long id = taskWithSchema.task() == null ?
                  0L : taskWithSchema.task().asLong();
        SchemaElement schemaElement = taskWithSchema.schemaElement();
        String type;
        String schema;
        if (schemaElement instanceof PropertyKey) {
            type = "property_key";
            schema = this.writePropertyKey((PropertyKey) schemaElement);
        } else if (schemaElement instanceof IndexLabel) {
            type = "index_label";
            schema = this.writeIndexlabel((IndexLabel) schemaElement);
        } else {
            throw new HugeException("Invalid schema element '%s' in " +
                                    "TaskWithSchema, only support " +
                                    "[PropertyKey, IndexLabel]", schemaElement);
        }
        builder.append("{\"").append(type).append("\": ")
               .append(schema).append(", \"task_id\": ")
               .append(id);
        if (this.apiMeasure != null) {
            builder.append(String.format(",\"%s\":[", MEASURE_KEY));
            builder.append(JsonUtil.toJson(this.apiMeasure));
        }
        return builder.append("}").toString();
    }

    @Override
    public String writePropertyKeys(List<PropertyKey> propertyKeys) {
        return writeList("propertykeys", propertyKeys);
    }

    @Override
    public String writeVertexLabel(VertexLabel vertexLabel) {
        return JsonUtil.toJson(vertexLabel);
    }

    @Override
    public String writeVertexLabels(List<VertexLabel> vertexLabels) {
        return writeList("vertexlabels", vertexLabels);
    }

    @Override
    public String writeEdgeLabel(EdgeLabel edgeLabel) {
        return JsonUtil.toJson(edgeLabel);
    }

    @Override
    public String writeEdgeLabels(List<EdgeLabel> edgeLabels) {
        return writeList("edgelabels", edgeLabels);
    }

    @Override
    public String writeIndexlabel(IndexLabel indexLabel) {
        return JsonUtil.toJson(indexLabel);
    }

    @Override
    public String writeIndexlabels(List<IndexLabel> indexLabels) {
        return writeList("indexlabels", indexLabels);
    }

    @Override
    public String writeVertex(Vertex vertex) {
        return JsonUtil.toJson(vertex);
    }

    @Override
    public String writeVertices(Iterator<Vertex> vertices, boolean paging) {
        return this.writeIterator("vertices", vertices, paging);
    }

    @Override
    public String writeEdge(Edge edge) {
        return JsonUtil.toJson(edge);
    }

    @Override
    public String writeEdges(Iterator<Edge> edges, boolean paging) {
        return this.writeIterator("edges", edges, paging);
    }

    @Override
    public String writeIds(List<Id> ids) {
        return JsonUtil.toJson(ids);
    }

    @Override
    public String writeAuthElement(AuthElement elem) {
        return this.writeMap(elem.asMap());
    }

    @Override
    public <V extends AuthElement> String writeAuthElements(String label,
                                                            List<V> elems) {
        List<Object> list = new ArrayList<>(elems.size());
        for (V elem : elems) {
            list.add(elem.asMap());
        }
        return this.writeList(label, list);
    }

    @Override
    public String writePaths(String name, Collection<HugeTraverser.Path> paths,
                             boolean withCrossPoint, Iterator<?> vertices,
                             Iterator<?> edges) {
        List<Map<String, Object>> pathList = new ArrayList<>(paths.size());
        for (HugeTraverser.Path path : paths) {
            pathList.add(path.toMap(withCrossPoint));
        }

        ImmutableMap.Builder<Object, Object> builder = ImmutableMap.builder();
        builder.put(name, pathList);

        if (vertices != null) {
            builder.put("vertices", vertices);
        }

        if (edges != null) {
            builder.put("edges", edges);
        }

        if (this.apiMeasure != null) {
            builder.put(MEASURE_KEY, this.apiMeasure);
        }

        return JsonUtil.toJson(builder.build());
    }

    @Override
    public String writeCrosspoints(CrosspointsPaths paths,
                                   Iterator<?> vertices,
                                   Iterator<?> edges,
                                   boolean withPath) {
        List<Map<String, Object>> pathList;
        if (withPath) {
            pathList = new ArrayList<>();
            for (HugeTraverser.Path path : paths.paths()) {
                pathList.add(path.toMap(false));
            }
        } else {
            pathList = ImmutableList.of();
        }
        ImmutableMap.Builder<Object, Object> builder = ImmutableMap.builder()
                                                                   .put("crosspoints",
                                                                        paths.crosspoints())
                                                                   .put("paths", pathList)
                                                                   .put("vertices", vertices)
                                                                   .put("edges", edges);
        if (this.apiMeasure != null) {
            builder.put(MEASURE_KEY, this.apiMeasure);
        }
        return JsonUtil.toJson(builder.build());
    }

    @Override
    public String writeSimilars(SimilarsMap similars,
                                Iterator<?> vertices) {
        ImmutableMap.Builder<Object, Object> builder = ImmutableMap.builder()
                                                                   .put("similars",
                                                                        similars.toMap())
                                                                   .put("vertices", vertices);
        if (this.apiMeasure != null) {
            builder.put(MEASURE_KEY, this.apiMeasure);
        }
        return JsonUtil.toJson(builder.build());
    }

    @Override
    public String writeWeightedPath(NodeWithWeight path, Iterator<?> vertices,
                                    Iterator<?> edges) {
        Map<String, Object> pathMap = path == null ?
                                      ImmutableMap.of() : path.toMap();
        ImmutableMap.Builder<Object, Object> builder = ImmutableMap.builder()
                                                                   .put("path", pathMap)
                                                                   .put("vertices", vertices)
                                                                   .put("edges", edges);
        if (this.apiMeasure != null) {
            builder.put(MEASURE_KEY, this.apiMeasure);
        }
        return JsonUtil.toJson(builder.build());
    }

    @Override
    public String writeWeightedPaths(WeightedPaths paths, Iterator<?> vertices,
                                     Iterator<?> edges) {
        Map<Id, Map<String, Object>> pathMap = paths == null ?
                                               ImmutableMap.of() :
                                               paths.toMap();
        ImmutableMap.Builder<Object, Object> builder = ImmutableMap.builder()
                                                                   .put("paths", pathMap)
                                                                   .put("vertices", vertices)
                                                                   .put("edges", edges);
        if (this.apiMeasure != null) {
            builder.put(MEASURE_KEY, this.apiMeasure);
        }
        return JsonUtil.toJson(builder.build());
    }

    @Override
    public String writeNodesWithPath(String name, List<Id> nodes, long size,
                                     Collection<HugeTraverser.Path> paths,
                                     Iterator<?> vertices, Iterator<?> edges) {
        List<Map<String, Object>> pathList = new ArrayList<>();
        for (HugeTraverser.Path path : paths) {
            pathList.add(path.toMap(false));
        }

        ImmutableMap.Builder<Object, Object> builder = ImmutableMap.builder()
                                                                   .put(name, nodes)
                                                                   .put("size", size)
                                                                   .put("paths", pathList)
                                                                   .put("vertices", vertices)
                                                                   .put("edges", edges);
        if (this.apiMeasure != null) {
            builder.put(MEASURE_KEY, this.apiMeasure);
        }

        return JsonUtil.toJson(builder.build());
    }

    @Override
    public String writeGraphSpace(GraphSpace graphSpace) {
        return JsonUtil.toJson(graphSpace);
    }

    @Override
    public String writeService(Service service) {
        return JsonUtil.toJson(service);
    }

    @Override
    public String writeSchemaTemplate(SchemaTemplate template) {
        return JsonUtil.toJson(template.asMap());
    }

}
