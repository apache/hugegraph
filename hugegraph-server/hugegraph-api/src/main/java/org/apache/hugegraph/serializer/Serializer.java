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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.auth.SchemaDefine.AuthElement;
import org.apache.hugegraph.backend.id.Id;
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
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public interface Serializer {

    String writeMap(Map<?, ?> map);

    String writeList(String label, Collection<?> list);

    String writePropertyKey(PropertyKey propertyKey);

    String writePropertyKeys(List<PropertyKey> propertyKeys);

    String writeVertexLabel(VertexLabel vertexLabel);

    String writeVertexLabels(List<VertexLabel> vertexLabels);

    String writeEdgeLabel(EdgeLabel edgeLabel);

    String writeEdgeLabels(List<EdgeLabel> edgeLabels);

    String writeIndexlabel(IndexLabel indexLabel);

    String writeIndexlabels(List<IndexLabel> indexLabels);

    String writeTaskWithSchema(SchemaElement.TaskWithSchema tws);

    String writeVertex(Vertex v);

    String writeVertices(Iterator<Vertex> vertices, boolean paging);

    String writeEdge(Edge e);

    String writeEdges(Iterator<Edge> edges, boolean paging);

    String writeIds(List<Id> ids);

    String writeAuthElement(AuthElement elem);

    <V extends AuthElement> String writeAuthElements(String label, List<V> users);

    String writePaths(String name, Collection<HugeTraverser.Path> paths,
                      boolean withCrossPoint, Iterator<?> vertices,
                      Iterator<?> edges);

    default String writePaths(String name, Collection<HugeTraverser.Path> paths,
                              boolean withCrossPoint) {
        return this.writePaths(name, paths, withCrossPoint, null, null);
    }

    String writeCrosspoints(CrosspointsPaths paths, Iterator<?> vertices,
                            Iterator<?> edges, boolean withPath);

    String writeSimilars(SimilarsMap similars, Iterator<?> vertices);

    String writeWeightedPath(NodeWithWeight path, Iterator<?> vertices,
                             Iterator<?> edges);

    String writeWeightedPaths(WeightedPaths paths, Iterator<?> vertices,
                              Iterator<?> edges);

    String writeNodesWithPath(String name, List<Id> nodes, long size,
                              Collection<HugeTraverser.Path> paths,
                              Iterator<?> vertices, Iterator<?> edges);

    String writeGraphSpace(GraphSpace graphSpace);

    String writeService(Service service);

    String writeSchemaTemplate(SchemaTemplate template);
}
