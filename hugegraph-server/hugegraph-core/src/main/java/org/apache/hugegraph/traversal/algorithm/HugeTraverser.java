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

package org.apache.hugegraph.traversal.algorithm;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.Aggregate;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.EdgesQueryIterator;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.iterator.ExtendableIterator;
import org.apache.hugegraph.iterator.FilterIterator;
import org.apache.hugegraph.iterator.LimitIterator;
import org.apache.hugegraph.iterator.MapperIterator;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.schema.SchemaLabel;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.iterator.NestedIterator;
import org.apache.hugegraph.traversal.algorithm.steps.EdgeStep;
import org.apache.hugegraph.traversal.algorithm.steps.Steps;
import org.apache.hugegraph.traversal.optimize.TraversalUtil;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.CollectionUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.collection.CollectionFactory;
import org.apache.hugegraph.util.collection.ObjectIntMapping;
import org.apache.hugegraph.util.collection.ObjectIntMappingFactory;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;

public class HugeTraverser {

    public static final String DEFAULT_CAPACITY = "10000000";
    public static final String DEFAULT_ELEMENTS_LIMIT = "10000000";
    public static final String DEFAULT_PATHS_LIMIT = "10";
    public static final String DEFAULT_LIMIT = "100";
    public static final String DEFAULT_MAX_DEGREE = "10000";
    public static final String DEFAULT_SKIP_DEGREE = "100000";
    public static final String DEFAULT_SAMPLE = "100";
    public static final String DEFAULT_WEIGHT = "0";
    public static final int DEFAULT_MAX_DEPTH = 5000;
    // Empirical value of scan limit, with which results can be returned in 3s
    public static final String DEFAULT_PAGE_LIMIT = "100000";
    public static final long NO_LIMIT = -1L;
    // traverse mode of kout algorithm: bfs and dfs
    public static final String TRAVERSE_MODE_BFS = "breadth_first_search";
    public static final String TRAVERSE_MODE_DFS = "depth_first_search";
    protected static final Logger LOG = Log.logger(HugeTraverser.class);
    protected static final int MAX_VERTICES = 10;
    private static CollectionFactory collectionFactory;
    private final HugeGraph graph;
    // for apimeasure
    public AtomicLong edgeIterCounter = new AtomicLong(0);
    public AtomicLong vertexIterCounter = new AtomicLong(0);

    public HugeTraverser(HugeGraph graph) {
        this.graph = graph;
        if (collectionFactory == null) {
            collectionFactory = new CollectionFactory(this.collectionType());
        }
    }

    public static void checkDegree(long degree) {
        checkPositiveOrNoLimit(degree, "max degree");
    }

    public static void checkCapacity(long capacity) {
        checkPositiveOrNoLimit(capacity, "capacity");
    }

    public static void checkLimit(long limit) {
        checkPositiveOrNoLimit(limit, "limit");
    }

    public static void checkPositive(long value, String name) {
        E.checkArgument(value > 0,
                        "The %s parameter must be > 0, but got %s",
                        name, value);
    }

    public static void checkPositiveOrNoLimit(long value, String name) {
        E.checkArgument(value > 0L || value == NO_LIMIT,
                        "The %s parameter must be > 0 or == %s, but got: %s",
                        name, NO_LIMIT, value);
    }

    public static void checkNonNegative(long value, String name) {
        E.checkArgument(value >= 0L,
                        "The %s parameter must be >= 0, but got: %s",
                        name, value);
    }

    public static void checkNonNegativeOrNoLimit(long value, String name) {
        E.checkArgument(value >= 0L || value == NO_LIMIT,
                        "The %s parameter must be >= 0 or == %s, but got: %s",
                        name, NO_LIMIT, value);
    }

    public static void checkCapacity(long capacity, long access,
                                     String traverse) {
        if (capacity != NO_LIMIT && access > capacity) {
            throw new HugeException("Exceed capacity '%s' while finding %s",
                                    capacity, traverse);
        }
    }

    public static void checkSkipDegree(long skipDegree, long degree,
                                       long capacity) {
        E.checkArgument(skipDegree >= 0L &&
                        skipDegree <= Query.DEFAULT_CAPACITY,
                        "The skipped degree must be in [0, %s], but got '%s'",
                        Query.DEFAULT_CAPACITY, skipDegree);
        if (capacity != NO_LIMIT) {
            E.checkArgument(degree != NO_LIMIT && degree < capacity,
                            "The max degree must be < capacity");
            E.checkArgument(skipDegree < capacity,
                            "The skipped degree must be < capacity");
        }
        if (skipDegree > 0L) {
            E.checkArgument(degree != NO_LIMIT && skipDegree >= degree,
                            "The skipped degree must be >= max degree, " +
                            "but got skipped degree '%s' and max degree '%s'",
                            skipDegree, degree);
        }
    }

    public static void checkTraverseMode(String traverseMode) {
        E.checkArgument(traverseMode.compareToIgnoreCase(TRAVERSE_MODE_BFS) == 0 ||
                        traverseMode.compareToIgnoreCase(TRAVERSE_MODE_DFS) == 0,
                        "The traverse mode must be one of '%s' or '%s', but got '%s'",
                        TRAVERSE_MODE_BFS, TRAVERSE_MODE_DFS, traverseMode);
    }

    public static boolean isTraverseModeDFS(String traverseMode) {
        return traverseMode.compareToIgnoreCase(TRAVERSE_MODE_DFS) == 0;
    }

    public static <K, V extends Comparable<? super V>> Map<K, V> topN(
            Map<K, V> map,
            boolean sorted,
            long limit) {
        if (sorted) {
            map = CollectionUtil.sortByValue(map, false);
        }
        if (limit == NO_LIMIT || map.size() <= limit) {
            return map;
        }
        Map<K, V> results = InsertionOrderUtil.newMap();
        long count = 0L;
        for (Map.Entry<K, V> entry : map.entrySet()) {
            results.put(entry.getKey(), entry.getValue());
            if (++count >= limit) {
                break;
            }
        }
        return results;
    }

    public static Iterator<Edge> skipSuperNodeIfNeeded(Iterator<Edge> edges,
                                                       long degree,
                                                       long skipDegree) {
        if (skipDegree <= 0L) {
            return edges;
        }
        List<Edge> edgeList = newList();
        for (int i = 1; edges.hasNext(); i++) {
            Edge edge = edges.next();
            if (i <= degree) {
                edgeList.add(edge);
            }
            if (i >= skipDegree) {
                return QueryResults.emptyIterator();
            }
        }
        return edgeList.iterator();
    }

    protected static Set<Id> newIdSet() {
        return collectionFactory.newIdSet();
    }

    protected static <V> Set<V> newSet() {
        return newSet(false);
    }

    protected static <V> Set<V> newSet(boolean concurrent) {
        if (concurrent) {
            return ConcurrentHashMap.newKeySet();
        } else {
            return collectionFactory.newSet();
        }
    }

    protected static <V> Set<V> newSet(int initialCapacity) {
        return collectionFactory.newSet(initialCapacity);
    }

    protected static <V> Set<V> newSet(Collection<V> collection) {
        return collectionFactory.newSet(collection);
    }

    protected static <V> List<V> newList() {
        return collectionFactory.newList();
    }

    protected static <V> List<V> newList(int initialCapacity) {
        return collectionFactory.newList(initialCapacity);
    }

    protected static <V> List<V> newList(Collection<V> collection) {
        return collectionFactory.newList(collection);
    }

    protected static <K, V> Map<K, V> newMap() {
        return collectionFactory.newMap();
    }

    protected static <K, V> Map<K, V> newMap(int initialCapacity) {
        return collectionFactory.newMap(initialCapacity);
    }

    protected static <K, V> MultivaluedMap<K, V> newMultivalueMap() {
        return new MultivaluedHashMap<>();
    }

    protected static List<Id> joinPath(Node prev, Node back, boolean ring) {
        // Get self path
        List<Id> path = prev.path();

        // Get reversed other path
        List<Id> backPath = back.path();
        Collections.reverse(backPath);

        if (!ring) {
            // Avoid loop in path
            if (CollectionUtils.containsAny(path, backPath)) {
                return ImmutableList.of();
            }
        }

        // Append other path behind self path
        path.addAll(backPath);
        return path;
    }

    public static List<HugeEdge> pathEdges(Iterator<Edge> iterator, HugeEdge edge) {
        List<HugeEdge> edges = new ArrayList<>();
        if (iterator instanceof NestedIterator) {
            edges = ((NestedIterator) iterator).pathEdges();
        }
        edges.add(edge);
        return edges;
    }

    public HugeGraph graph() {
        return this.graph;
    }

    protected int concurrentDepth() {
        return this.graph.option(CoreOptions.OLTP_CONCURRENT_DEPTH);
    }

    private CollectionType collectionType() {
        return this.graph.option(CoreOptions.OLTP_COLLECTION_TYPE);
    }

    protected Set<Id> adjacentVertices(Id sourceV, Set<Id> vertices,
                                       Directions dir, Id label,
                                       Set<Id> excluded, long degree,
                                       long limit) {
        if (limit == 0) {
            return ImmutableSet.of();
        }

        Set<Id> neighbors = newIdSet();
        for (Id source : vertices) {
            Iterator<Edge> edges = this.edgesOfVertex(source, dir,
                                                      label, degree);
            while (edges.hasNext()) {
                HugeEdge e = (HugeEdge) edges.next();
                Id target = e.id().otherVertexId();
                boolean matchExcluded = (excluded != null &&
                                         excluded.contains(target));
                if (matchExcluded || neighbors.contains(target) ||
                    sourceV.equals(target)) {
                    continue;
                }
                neighbors.add(target);
                if (limit != NO_LIMIT && neighbors.size() >= limit) {
                    return neighbors;
                }
            }
        }
        return neighbors;
    }

    protected Iterator<Id> adjacentVertices(Id source, Directions dir,
                                            Id label, long limit) {
        Iterator<Edge> edges = this.edgesOfVertex(source, dir, label, limit);
        return new MapperIterator<>(edges, e -> {
            HugeEdge edge = (HugeEdge) e;
            return edge.id().otherVertexId();
        });
    }

    protected Set<Id> adjacentVertices(Id source, EdgeStep step) {
        Set<Id> neighbors = newSet();
        Iterator<Edge> edges = this.edgesOfVertex(source, step);
        while (edges.hasNext()) {
            neighbors.add(((HugeEdge) edges.next()).id().otherVertexId());
        }
        return neighbors;
    }

    protected Iterator<Id> adjacentVertices(Id source, Directions dir,
                                            List<Id> labels, long limit) {
        Iterator<Edge> edges = this.edgesOfVertex(source, dir, labels, limit);
        return new MapperIterator<>(edges, e -> {
            HugeEdge edge = (HugeEdge) e;
            return edge.id().otherVertexId();
        });
    }

    @Watched
    protected Iterator<Edge> edgesOfVertex(Id source, Directions dir,
                                           Id label, long limit) {
        Id[] labels = {};
        if (label != null) {
            labels = new Id[]{label};
        }

        Query query = GraphTransaction.constructEdgesQuery(source, dir, labels);
        if (limit != NO_LIMIT) {
            query.limit(limit);
        }
        return this.graph.edges(query);
    }

    @Watched
    protected Iterator<Edge> edgesOfVertex(Id source, Directions dir,
                                           Map<Id, String> labels, long limit) {
        if (labels == null || labels.isEmpty()) {
            return this.edgesOfVertex(source, dir, (Id) null, limit);
        }
        ExtendableIterator<Edge> results = new ExtendableIterator<>();
        for (Id label : labels.keySet()) {
            E.checkNotNull(label, "edge label");
            results.extend(this.edgesOfVertex(source, dir, label, limit));
        }

        if (limit == NO_LIMIT) {
            return results;
        }

        long[] count = new long[1];
        return new LimitIterator<>(results, e -> count[0]++ >= limit);
    }

    protected Iterator<Edge> edgesOfVertex(Id source, Directions dir,
                                           List<Id> labels, long limit) {
        if (labels == null || labels.isEmpty()) {
            return this.edgesOfVertex(source, dir, (Id) null, limit);
        }
        ExtendableIterator<Edge> results = new ExtendableIterator<>();
        for (Id label : labels) {
            E.checkNotNull(label, "edge label");
            results.extend(this.edgesOfVertex(source, dir, label, limit));
        }

        if (limit == NO_LIMIT) {
            return results;
        }

        long[] count = new long[1];
        return new LimitIterator<>(results, e -> count[0]++ >= limit);
    }

    protected Iterator<Edge> edgesOfVertex(Id source, EdgeStep edgeStep) {
        if (edgeStep.properties() == null || edgeStep.properties().isEmpty()) {
            Iterator<Edge> edges = this.edgesOfVertex(source,
                                                      edgeStep.direction(),
                                                      edgeStep.labels(),
                                                      edgeStep.limit());
            return edgeStep.skipSuperNodeIfNeeded(edges);
        }
        return this.edgesOfVertex(source, edgeStep, false);
    }

    protected Iterator<Edge> edgesOfVertexWithSK(Id source, EdgeStep edgeStep) {
        assert edgeStep.properties() != null && !edgeStep.properties().isEmpty();
        return this.edgesOfVertex(source, edgeStep, true);
    }

    private Iterator<Edge> edgesOfVertex(Id source, EdgeStep edgeStep,
                                         boolean mustAllSK) {
        Id[] edgeLabels = edgeStep.edgeLabels();
        Query query = GraphTransaction.constructEdgesQuery(source,
                                                           edgeStep.direction(),
                                                           edgeLabels);
        ConditionQuery filter = null;
        if (mustAllSK) {
            this.fillFilterBySortKeys(query, edgeLabels, edgeStep.properties());
        } else {
            filter = (ConditionQuery) query.copy();
            this.fillFilterByProperties(filter, edgeStep.properties());
        }
        query.capacity(Query.NO_CAPACITY);
        if (edgeStep.limit() != NO_LIMIT) {
            query.limit(edgeStep.limit());
        }
        Iterator<Edge> edges = this.graph().edges(query);
        if (filter != null) {
            ConditionQuery finalFilter = filter;
            edges = new FilterIterator<>(edges, (e) -> {
                return finalFilter.test((HugeEdge) e);
            });
        }
        return edgeStep.skipSuperNodeIfNeeded(edges);
    }

    public EdgesIterator edgesOfVertices(Iterator<Id> sources,
                                         Directions dir,
                                         List<Id> labelIds,
                                         long degree) {
        return new EdgesIterator(new EdgesQueryIterator(sources, dir, labelIds, degree));
    }

    public Iterator<Edge> edgesOfVertex(Id source, Steps steps) {
        List<Id> edgeLabels = steps.edgeLabels();
        ConditionQuery cq = GraphTransaction.constructEdgesQuery(
                source, steps.direction(), edgeLabels.toArray(new Id[0]));
        cq.capacity(Query.NO_CAPACITY);
        if (steps.limit() != NO_LIMIT) {
            cq.limit(steps.limit());
        }

        if (steps.isEdgeEmpty()) {
            Iterator<Edge> edges = this.graph().edges(cq);
            return edgesOfVertexStep(edges, steps);
        }

        Map<Id, ConditionQuery> edgeConditions =
                getFilterQueryConditions(steps.edgeSteps(), HugeType.EDGE);

        Iterator<Edge> filteredEdges =
                new FilterIterator<>(this.graph().edges(cq),
                                     edge -> validateEdge(edgeConditions, (HugeEdge) edge));

        return edgesOfVertexStep(filteredEdges, steps);
    }

    protected Iterator<Edge> edgesOfVertexStep(Iterator<Edge> edges, Steps steps) {
        if (steps.isVertexEmpty()) {
            return edges;
        }

        Map<Id, ConditionQuery> vertexConditions =
                getFilterQueryConditions(steps.vertexSteps(), HugeType.VERTEX);

        return new FilterIterator<>(edges,
                                    edge -> validateVertex(vertexConditions, (HugeEdge) edge));
    }

    private Boolean validateVertex(Map<Id, ConditionQuery> conditions,
                                   HugeEdge edge) {
        HugeVertex sourceV = edge.sourceVertex();
        HugeVertex targetV = edge.targetVertex();
        if (!conditions.containsKey(sourceV.schemaLabel().id()) ||
            !conditions.containsKey(targetV.schemaLabel().id())) {
            return false;
        }

        ConditionQuery cq = conditions.get(sourceV.schemaLabel().id());
        if (cq != null) {
            sourceV = (HugeVertex) this.graph.vertex(sourceV.id());
            if (!cq.test(sourceV)) {
                return false;
            }
        }

        cq = conditions.get(targetV.schemaLabel().id());
        if (cq != null) {
            targetV = (HugeVertex) this.graph.vertex(targetV.id());
            return cq.test(targetV);
        }
        return true;
    }

    private Boolean validateEdge(Map<Id, ConditionQuery> conditions,
                                 HugeEdge edge) {
        if (!conditions.containsKey(edge.schemaLabel().id())) {
            return false;
        }

        ConditionQuery cq = conditions.get(edge.schemaLabel().id());
        if (cq != null) {
            return cq.test(edge);
        }
        return true;
    }

    private Map<Id, ConditionQuery> getFilterQueryConditions(
            Map<Id, Steps.StepEntity> idStepEntityMap, HugeType type) {
        Map<Id, ConditionQuery> conditions = new HashMap<>();

        for (Map.Entry<Id, Steps.StepEntity> entry : idStepEntityMap.entrySet()) {
            Steps.StepEntity stepEntity = entry.getValue();
            if (stepEntity.properties() != null && !stepEntity.properties().isEmpty()) {
                ConditionQuery cq = new ConditionQuery(type);
                Map<Id, Object> properties = stepEntity.properties();
                TraversalUtil.fillConditionQuery(cq, properties, this.graph);
                conditions.put(entry.getKey(), cq);
            } else {
                conditions.put(entry.getKey(), null);
            }
        }
        return conditions;
    }

    private void fillFilterBySortKeys(Query query, Id[] edgeLabels,
                                      Map<Id, Object> properties) {
        if (properties == null || properties.isEmpty()) {
            return;
        }

        E.checkArgument(edgeLabels.length == 1,
                        "The properties filter condition can be set " +
                        "only if just set one edge label");

        this.fillFilterByProperties(query, properties);

        ConditionQuery condQuery = (ConditionQuery) query;
        if (!GraphTransaction.matchFullEdgeSortKeys(condQuery, this.graph())) {
            Id label = condQuery.conditionValue(HugeKeys.LABEL);
            E.checkArgument(false, "The properties %s does not match " +
                                   "sort keys of edge label '%s'",
                            this.graph().mapPkId2Name(properties.keySet()),
                            this.graph().edgeLabel(label).name());
        }
    }

    private void fillFilterByProperties(Query query,
                                        Map<Id, Object> properties) {
        if (properties == null || properties.isEmpty()) {
            return;
        }

        ConditionQuery condQuery = (ConditionQuery) query;
        TraversalUtil.fillConditionQuery(condQuery, properties, this.graph);
    }

    protected long edgesCount(Id source, EdgeStep edgeStep) {
        Id[] edgeLabels = edgeStep.edgeLabels();
        Query query = GraphTransaction.constructEdgesQuery(source,
                                                           edgeStep.direction(),
                                                           edgeLabels);
        this.fillFilterBySortKeys(query, edgeLabels, edgeStep.properties());
        query.aggregate(Aggregate.AggregateFunc.COUNT, null);
        query.capacity(Query.NO_CAPACITY);
        query.limit(Query.NO_LIMIT);
        long count = graph().queryNumber(query).longValue();
        if (edgeStep.degree() == NO_LIMIT || count < edgeStep.degree()) {
            return count;
        } else if (edgeStep.skipDegree() != 0L &&
                   count >= edgeStep.skipDegree()) {
            return 0L;
        } else {
            return edgeStep.degree();
        }
    }

    protected Object getVertexLabelIdOrNull(Object label) {
        if (label == null) {
            return null;
        }
        return SchemaLabel.getVertexLabelId(this.graph, label);
    }

    protected Id getEdgeLabelIdOrNull(Object label) {
        if (label == null) {
            return null;
        }
        return SchemaLabel.getEdgeLabelId(this.graph, label);
    }

    protected void checkVertexExist(Id vertexId, String name) {
        try {
            this.graph.vertex(vertexId);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException(String.format(
                    "The %s with id '%s' does not exist", name, vertexId), e);
        }
    }

    public Iterator<Edge> createNestedIterator(Id sourceV, Steps steps,
                                               int depth, Set<Id> visited, boolean nearest) {
        E.checkArgument(depth > 0, "The depth should large than 0 for nested iterator");
        visited.add(sourceV);

        // build a chained iterator path with length of depth
        Iterator<Edge> iterator = this.edgesOfVertex(sourceV, steps);
        for (int i = 1; i < depth; i++) {
            iterator = new NestedIterator(this, iterator, steps, visited, nearest);
        }
        return iterator;
    }

    public static class Node {

        private final Id id;
        private final Node parent;

        public Node(Id id) {
            this(id, null);
        }

        public Node(Id id, Node parent) {
            E.checkArgumentNotNull(id, "Id of Node can't be null");
            this.id = id;
            this.parent = parent;
        }

        public Id id() {
            return this.id;
        }

        public Node parent() {
            return this.parent;
        }

        public List<Id> path() {
            List<Id> ids = newList();
            Node current = this;
            do {
                ids.add(current.id);
                current = current.parent;
            } while (current != null);
            Collections.reverse(ids);
            return ids;
        }

        public List<Id> joinPath(Node back) {
            return HugeTraverser.joinPath(this, back, false);
        }

        public boolean contains(Id id) {
            Node node = this;
            do {
                if (node.id.equals(id)) {
                    return true;
                }
                node = node.parent;
            } while (node != null);
            return false;
        }

        @Override
        public int hashCode() {
            return this.id.hashCode();
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof Node)) {
                return false;
            }
            Node other = (Node) object;
            return Objects.equals(this.id, other.id) &&
                   Objects.equals(this.parent, other.parent);
        }

        @Override
        public String toString() {
            return this.id.toString();
        }
    }

    public static class Path {

        public static final Path EMPTY = new Path(ImmutableList.of());

        private final Id crosspoint;
        private final List<Id> vertices;
        private Set<Edge> edges = Collections.emptySet();

        public Path(List<Id> vertices) {
            this(null, vertices);
        }

        public Path(Id crosspoint, List<Id> vertices) {
            this.crosspoint = crosspoint;
            this.vertices = vertices;
        }

        public Path(List<Id> vertices, Set<Edge> edges) {
            this(null, vertices);
            this.edges = edges;
        }

        public Set<Edge> getEdges() {
            return edges;
        }

        public void setEdges(Set<Edge> edges) {
            this.edges = edges;
        }

        public Id crosspoint() {
            return this.crosspoint;
        }

        public void addToLast(Id id) {
            this.vertices.add(id);
        }

        public List<Id> vertices() {
            return this.vertices;
        }

        public void reverse() {
            Collections.reverse(this.vertices);
        }

        public Map<String, Object> toMap(boolean withCrossPoint) {
            if (withCrossPoint) {
                return ImmutableMap.of("crosspoint", this.crosspoint,
                                       "objects", this.vertices);
            } else {
                return ImmutableMap.of("objects", this.vertices);
            }
        }

        public boolean ownedBy(Id source) {
            E.checkNotNull(source, "source");
            Id min = null;
            for (Id id : this.vertices) {
                if (min == null || id.compareTo(min) < 0) {
                    min = id;
                }
            }
            return source.equals(min);
        }

        @Override
        public int hashCode() {
            return this.vertices.hashCode();
        }

        /**
         * Compares the specified object with this path for equality.
         * Returns <tt>true</tt> if and only if both have same vertices list
         * without regard of crosspoint.
         *
         * @param other the object to be compared for equality with this path
         * @return <tt>true</tt> if the specified object is equal to this path
         */
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Path)) {
                return false;
            }
            return this.vertices.equals(((Path) other).vertices);
        }

        @Override
        public String toString() {
            return this.vertices.toString();
        }
    }

    public static class PathSet implements Set<Path> {

        public static final PathSet EMPTY = new PathSet(ImmutableSet.of());

        private final Set<Path> paths;

        private Set<Edge> edges = Collections.emptySet();

        public PathSet(Set<Path> paths, Set<Edge> edges) {
            this(paths);
            this.edges = edges;
        }

        public PathSet() {
            this(newSet());
        }

        private PathSet(Set<Path> paths) {
            this.paths = paths;
        }

        public Set<Path> getPaths() {
            return this.paths;
        }

        public Set<Edge> getEdges() {
            return edges;
        }

        public void setEdges(Set<Edge> edges) {
            this.edges = edges;
        }

        @Override
        public boolean add(Path path) {
            return this.paths.add(path);
        }

        @Override
        public boolean remove(Object o) {
            return this.paths.remove(o);
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return this.paths.containsAll(c);
        }

        @Override
        public boolean addAll(Collection<? extends Path> c) {
            return this.paths.addAll(c);
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return this.paths.retainAll(c);
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return this.paths.removeAll(c);
        }

        @Override
        public void clear() {
            this.paths.clear();
        }

        @Override
        public boolean isEmpty() {
            return this.paths.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return this.paths.contains(o);
        }

        @Override
        public int size() {
            return this.paths.size();
        }

        @Override
        public Iterator<Path> iterator() {
            return this.paths.iterator();
        }

        @Override
        public Object[] toArray() {
            return this.paths.toArray();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return this.paths.toArray(a);
        }

        public boolean addAll(PathSet paths) {
            return this.paths.addAll(paths.paths);
        }

        public Set<Id> vertices() {
            Set<Id> vertices = newIdSet();
            for (Path path : this.paths) {
                vertices.addAll(path.vertices());
            }
            return vertices;
        }

        @Override
        public String toString() {
            return this.paths.toString();
        }

        public void append(Id current) {
            for (Iterator<Path> iter = paths.iterator(); iter.hasNext(); ) {
                Path path = iter.next();
                if (path.vertices().contains(current)) {
                    iter.remove();
                    continue;
                }
                path.addToLast(current);
            }
        }
    }

    public static class EdgeRecord {

        private final Map<Long, Edge> edgeMap;
        private final ObjectIntMapping<Id> idMapping;

        public EdgeRecord(boolean concurrent) {
            this.edgeMap = new HashMap<>();
            this.idMapping = ObjectIntMappingFactory.newObjectIntMapping(concurrent);
        }

        private static Long makeVertexPairIndex(int source, int target) {
            return ((long) source & 0xFFFFFFFFL) |
                   (((long) target << 32) & 0xFFFFFFFF00000000L);
        }

        public static Set<Id> getEdgeIds(Set<Edge> edges) {
            return edges.stream().map(edge -> ((HugeEdge) edge).id()).collect(Collectors.toSet());
        }

        private int code(Id id) {
            if (id.number()) {
                long l = id.asLong();
                if (0 <= l && l <= Integer.MAX_VALUE) {
                    return (int) l;
                }
            }
            int code = this.idMapping.object2Code(id);
            assert code > 0;
            return -code;
        }

        public void addEdge(Id source, Id target, Edge edge) {
            Long index = makeVertexPairIndex(this.code(source), this.code(target));
            this.edgeMap.put(index, edge);
        }

        private Edge getEdge(Id source, Id target) {
            Long index = makeVertexPairIndex(this.code(source), this.code(target));
            return this.edgeMap.get(index);
        }

        public Set<Edge> getEdges(HugeTraverser.Path path) {
            if (path == null || path.vertices().isEmpty()) {
                return new HashSet<>();
            }
            Iterator<Id> vertexIter = path.vertices().iterator();
            return getEdges(vertexIter);
        }

        public Set<Edge> getEdges(Collection<HugeTraverser.Path> paths) {
            Set<Edge> edgeIds = new HashSet<>();
            for (HugeTraverser.Path path : paths) {
                edgeIds.addAll(getEdges(path));
            }
            return edgeIds;
        }

        public Set<Edge> getEdges(Iterator<Id> vertexIter) {
            Set<Edge> edges = new HashSet<>();
            Id first = vertexIter.next();
            Id second;
            while (vertexIter.hasNext()) {
                second = vertexIter.next();
                Edge edge = getEdge(first, second);
                if (edge == null) {
                    edge = getEdge(second, first);
                }
                if (edge != null) {
                    edges.add(edge);
                }
                first = second;
            }
            return edges;
        }
    }

    public class EdgesIterator implements Iterator<Iterator<Edge>>, Closeable {

        private final Iterator<Iterator<Edge>> currentIter;

        public EdgesIterator(EdgesQueryIterator queries) {
            List<Iterator<Edge>> iteratorList = new ArrayList<>();
            while (queries.hasNext()) {
                Iterator<Edge> edges = graph.edges(queries.next());
                iteratorList.add(edges);
            }
            this.currentIter = iteratorList.iterator();
        }

        @Override
        public boolean hasNext() {
            return this.currentIter.hasNext();
        }

        @Override
        public Iterator<Edge> next() {
            return this.currentIter.next();
        }

        @Override
        public void close() throws IOException {
            CloseableIterator.closeIterator(currentIter);
        }
    }
}
