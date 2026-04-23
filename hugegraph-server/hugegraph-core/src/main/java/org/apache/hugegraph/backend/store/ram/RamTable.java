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

package org.apache.hugegraph.backend.store.ram;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.ConditionQueryFlatten;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.iterator.FlatMapperIterator;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.Consumers;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;

public final class RamTable {

    public static final String USER_DIR = System.getProperty("user.dir");
    public static final String EXPORT_PATH = USER_DIR + "/export";

    private static final Logger LOG = Log.logger(RamTable.class);

    // max vertices count, include non exists vertex, default 2.4 billion
    private static final long VERTICES_CAPACITY = 2400000000L;
    // max edges count, include OUT and IN edges, default 2.1 billion
    private static final int EDGES_CAPACITY = 2100000000;

    private static final int NULL = 0;

    private static final Condition BOTH_COND = Condition.or(
            Condition.eq(HugeKeys.DIRECTION, Directions.OUT),
            Condition.eq(HugeKeys.DIRECTION, Directions.IN));

    private final HugeGraph graph;
    private final long verticesCapacity;
    private final int verticesCapacityHalf;
    private final int edgesCapacity;

    private IntIntMap verticesLow;
    private IntIntMap verticesHigh;
    private IntLongMap edges;

    private volatile boolean loading = false;

    public RamTable(HugeGraph graph) {
        this(graph, VERTICES_CAPACITY, EDGES_CAPACITY);
    }

    public RamTable(HugeGraph graph, long maxVertices, int maxEdges) {
        this.graph = graph;
        this.verticesCapacity = maxVertices + 2L;
        this.verticesCapacityHalf = (int) (this.verticesCapacity / 2L);
        this.edgesCapacity = maxEdges + 1;
        this.reset();
    }

    private void reset() {
        this.verticesLow = null;
        this.verticesHigh = null;
        this.edges = null;
        this.verticesLow = new IntIntMap(this.verticesCapacityHalf);
        this.verticesHigh = new IntIntMap(this.verticesCapacityHalf);
        this.edges = new IntLongMap(this.edgesCapacity);
        // Set the first element as null edge
        this.edges.add(0L);
    }

    public void reload(boolean loadFromFile, String file) {
        if (this.loading) {
            throw new HugeException("There is one loading task, " +
                                    "please wait for it to complete");
        }

        this.loading = true;
        try {
            this.reset();
            if (loadFromFile) {
                this.loadFromFile(file);
            } else {
                this.loadFromDB();
                if (file != null) {
                    LOG.info("Export graph to file '{}'", file);
                    if (!this.exportToFile(file)) {
                        LOG.warn("Can't export graph to file '{}'", file);
                    }
                }
            }
            LOG.info("Loaded {} edges", this.edgesSize());
        } catch (Throwable e) {
            this.reset();
            throw new HugeException("Failed to load ramtable", e);
        } finally {
            this.loading = false;
        }
    }

    private void loadFromFile(String fileName) throws Exception {
        File file = Paths.get(EXPORT_PATH, fileName).toFile();
        if (!file.exists() || !file.isFile() || !file.canRead()) {
            throw new IllegalArgumentException(String.format(
                    "File '%s' does not existed or readable", fileName));
        }
        try (FileInputStream fis = new FileInputStream(file);
             BufferedInputStream bis = new BufferedInputStream(fis);
             DataInputStream input = new DataInputStream(bis)) {
            // read vertices
            this.verticesLow.readFrom(input);
            this.verticesHigh.readFrom(input);
            // read edges
            this.edges.readFrom(input);
        }
    }

    private boolean exportToFile(String fileName) throws Exception {
        File file = Paths.get(EXPORT_PATH, fileName).toFile();
        if (!file.exists()) {
            FileUtils.forceMkdir(file.getParentFile());
            if (!file.createNewFile()) {
                return false;
            }
        }
        try (FileOutputStream fos = new FileOutputStream(file);
             BufferedOutputStream bos = new BufferedOutputStream(fos);
             DataOutputStream output = new DataOutputStream(bos)) {
            // write vertices
            this.verticesLow.writeTo(output);
            this.verticesHigh.writeTo(output);
            // write edges
            this.edges.writeTo(output);
        }
        return true;
    }

    private void loadFromDB() throws Exception {
        Query query = new Query(HugeType.VERTEX);
        query.capacity(this.verticesCapacityHalf * 2L);
        query.limit(Query.NO_LIMIT);
        Iterator<Vertex> vertices = this.graph.vertices(query);

        // switch concurrent loading here
        boolean concurrent = true;
        if (concurrent) {
            try (LoadTraverser traverser = new LoadTraverser()) {
                traverser.load(vertices);
            }
            return;
        }

        Iterator<Edge> adjEdges;
        Id lastId = IdGenerator.ZERO;
        while (vertices.hasNext()) {
            Id vertex = (Id) vertices.next().id();
            if (vertex.compareTo(lastId) < 0) {
                throw new HugeException("The ramtable feature is not " +
                                        "supported by %s backend",
                                        this.graph.backend());
            }
            ensureNumberId(vertex);
            lastId = vertex;

            adjEdges = this.graph.adjacentEdges(vertex);
            if (adjEdges.hasNext()) {
                HugeEdge edge = (HugeEdge) adjEdges.next();
                this.addEdge(true, edge);
            }
            while (adjEdges.hasNext()) {
                HugeEdge edge = (HugeEdge) adjEdges.next();
                this.addEdge(false, edge);
            }
        }
    }

    public void addEdge(boolean newVertex, HugeEdge edge) {
        if (edge.schemaLabel().existSortKeys()) {
            throw new HugeException("Only edge label without sortkey is " +
                                    "supported by ramtable, but got '%s'",
                                    edge.schemaLabel());
        }
        ensureNumberId(edge.id().ownerVertexId());
        ensureNumberId(edge.id().otherVertexId());

        this.addEdge(newVertex,
                     edge.id().ownerVertexId().asLong(),
                     edge.id().otherVertexId().asLong(),
                     edge.direction(),
                     (int) edge.schemaLabel().id().asLong());
    }

    public void addEdge(boolean newVertex, long owner, long target,
                        Directions direction, int label) {
        long value = encode(target, direction, label);
        this.addEdge(newVertex, owner, value);
    }

    public void addEdge(boolean newVertex, long owner, long value) {
        int position = this.edges.add(value);
        if (newVertex) {
            assert this.vertexAdjPosition(owner) <= NULL : owner;
            this.vertexAdjPosition(owner, position);
        }
        // maybe there is no edges of the next vertex, set -position first
        this.vertexAdjPosition(owner + 1, -position);
    }

    public long edgesSize() {
        // -1 means the first is NULL edge
        return this.edges.size() - 1L;
    }

    @Watched
    public boolean matched(Query query) {
        if (this.edgesSize() == 0L || this.loading) {
            return false;
        }
        if (!query.resultType().isEdge() ||
            !(query instanceof ConditionQuery)) {
            return false;
        }

        ConditionQuery cq = (ConditionQuery) query;

        int conditionsSize = cq.conditionsSize();
        Object owner = cq.condition(HugeKeys.OWNER_VERTEX);
        Directions direction = cq.condition(HugeKeys.DIRECTION);
        Id label = cq.uniqueConditionValue(HugeKeys.LABEL);

        if (direction == null && conditionsSize > 1) {
            for (Condition cond : cq.conditions()) {
                if (cond.equals(BOTH_COND)) {
                    direction = Directions.BOTH;
                    break;
                }
            }
        }

        int matchedConds = 0;
        if (owner != null) {
            matchedConds++;
        } else {
            return false;
        }
        if (direction != null) {
            matchedConds++;
        }
        if (label != null) {
            matchedConds++;
        }
        return matchedConds == cq.conditionsSize();
    }

    @Watched
    public Iterator<HugeEdge> query(Query query) {
        assert this.matched(query);
        assert this.edgesSize() > 0;

        List<ConditionQuery> cqs = ConditionQueryFlatten.flatten(
                (ConditionQuery) query);
        if (cqs.size() == 1) {
            ConditionQuery cq = cqs.get(0);
            return this.query(cq);
        }
        return new FlatMapperIterator<>(cqs.iterator(), this::query);
    }

    private Iterator<HugeEdge> query(ConditionQuery query) {
        Id owner = query.condition(HugeKeys.OWNER_VERTEX);
        assert owner != null;
        Directions dir = query.condition(HugeKeys.DIRECTION);
        if (dir == null) {
            dir = Directions.BOTH;
        }
        Id label = query.uniqueConditionValue(HugeKeys.LABEL);
        if (label == null) {
            label = IdGenerator.ZERO;
        }
        return this.query(owner.asLong(), dir, (int) label.asLong());
    }

    @Watched
    public Iterator<HugeEdge> query(long owner, Directions dir, int label) {
        if (this.loading) {
            // don't query when loading
            return Collections.emptyIterator();
        }

        int start = this.vertexAdjPosition(owner);
        if (start <= NULL) {
            return Collections.emptyIterator();
        }
        int end = this.vertexAdjPosition(owner + 1);
        assert start != NULL;
        if (end < NULL) {
            // The next vertex does not exist edges
            end = 1 - end;
        }
        return new EdgeRangeIterator(start, end, dir, label, owner);
    }

    private void vertexAdjPosition(long vertex, int position) {
        if (vertex < this.verticesCapacityHalf) {
            this.verticesLow.put(vertex, position);
        } else if (vertex < this.verticesCapacity) {
            vertex -= this.verticesCapacityHalf;
            assert vertex < Integer.MAX_VALUE;
            this.verticesHigh.put(vertex, position);
        } else {
            throw new HugeException("Out of vertices capacity %s",
                                    this.verticesCapacity);
        }
    }

    private int vertexAdjPosition(long vertex) {
        if (vertex < this.verticesCapacityHalf) {
            return this.verticesLow.get(vertex);
        } else if (vertex < this.verticesCapacity) {
            vertex -= this.verticesCapacityHalf;
            assert vertex < Integer.MAX_VALUE;
            return this.verticesHigh.get(vertex);
        } else {
            throw new HugeException("Out of vertices capacity %s: %s",
                                    this.verticesCapacity, vertex);
        }
    }

    private static void ensureNumberId(Id id) {
        if (!id.number()) {
            throw new HugeException("Only number id is supported by " +
                                    "ramtable, but got %s id '%s'",
                                    id.type().name().toLowerCase(), id);
        }
    }

    private static long encode(long target, Directions direction, int label) {
        // TODO: support property
        assert (label & 0x0fffffff) == label;
        assert target < 2L * Integer.MAX_VALUE : target;
        long value = target & 0xffffffff;
        long dir = direction == Directions.OUT ?
                   0x00000000L : 0x80000000L;
        value = (value << 32) | (dir | label);
        return value;
    }

    private class EdgeRangeIterator implements Iterator<HugeEdge> {

        private final int end;
        private final Directions dir;
        private final int label;
        private final HugeVertex owner;
        private int current;
        private HugeEdge currentEdge;

        public EdgeRangeIterator(int start, int end,
                                 Directions dir, int label, long owner) {
            assert 0 < start && start < end;
            this.end = end;
            this.dir = dir;
            this.label = label;
            this.owner = new HugeVertex(RamTable.this.graph,
                                        IdGenerator.of(owner),
                                        VertexLabel.NONE);
            this.current = start;
            this.currentEdge = null;
        }

        @Override
        public boolean hasNext() {
            if (this.currentEdge != null) {
                return true;
            }
            while (this.current < this.end) {
                this.currentEdge = this.fetch();
                if (this.currentEdge != null) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public HugeEdge next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            assert this.currentEdge != null;
            HugeEdge edge = this.currentEdge;
            this.currentEdge = null;
            return edge;
        }

        private HugeEdge fetch() {
            if (this.current >= this.end) {
                return null;
            }
            long value = RamTable.this.edges.get(this.current++);
            long otherV = value >>> 32;
            assert otherV >= 0L : otherV;
            Directions actualDir = (value & 0x80000000L) == 0L ?
                                   Directions.OUT : Directions.IN;
            int label = (int) value & 0x7fffffff;
            assert label >= 0;

            if (this.dir != actualDir && this.dir != Directions.BOTH) {
                return null;
            }
            if (this.label != label && this.label != 0) {
                return null;
            }

            HugeGraph graph = RamTable.this.graph;
            this.owner.correctVertexLabel(VertexLabel.NONE);
            boolean direction = actualDir == Directions.OUT;
            Id labelId = IdGenerator.of(label);
            Id otherVertexId = IdGenerator.of(otherV);
            String sortValues = "";
            EdgeLabel edgeLabel = graph.edgeLabel(labelId);

            HugeEdge edge = HugeEdge.constructEdge(this.owner, direction,
                                                   edgeLabel, sortValues,
                                                   otherVertexId);
            edge.propNotLoaded();
            return edge;
        }
    }

    private class LoadTraverser implements AutoCloseable {

        private final HugeGraph graph;
        private final ExecutorService executor;
        private final List<Id> vertices;
        private final Map<Id, List<Edge>> edges;

        private static final int ADD_BATCH = Consumers.QUEUE_WORKER_SIZE;

        public LoadTraverser() {
            this.graph = RamTable.this.graph;
            this.executor = Consumers.newThreadPool("ramtable-load",
                                                    Consumers.THREADS);
            this.vertices = new ArrayList<>(ADD_BATCH);
            this.edges = new ConcurrentHashMap<>();
        }

        @Override
        public void close() throws Exception {
            if (this.executor != null) {
                for (int i = 0; i < Consumers.THREADS; i++) {
                    this.executor.execute(() -> {
                        this.graph.tx().commit();
                    });
                }
                this.executor.shutdown();
            }
        }

        protected long load(Iterator<Vertex> vertices) {
            Consumers<Id> consumers = new Consumers<>(this.executor, vertex -> {
                Iterator<Edge> adjEdges = this.graph.adjacentEdges(vertex);
                this.edges.put(vertex, IteratorUtils.list(adjEdges));
            }, null);

            consumers.start("ramtable-loading");

            long total = 0L;
            try {
                while (vertices.hasNext()) {
                    if (++total % 10000000 == 0) {
                        LOG.info("Loaded {} vertices", total);
                    }

                    Id vertex = (Id) vertices.next().id();
                    this.addVertex(vertex);

                    consumers.provide(vertex);
                }
            } catch (Consumers.StopExecution e) {
                // pass
            } catch (Throwable e) {
                throw Consumers.wrapException(e);
            } finally {
                try {
                    consumers.await();
                } catch (Throwable e) {
                    throw Consumers.wrapException(e);
                } finally {
                    CloseableIterator.closeIterator(vertices);
                }
            }
            this.addEdgesByBatch();
            return total;
        }

        private void addVertex(Id vertex) {
            Id lastId = IdGenerator.ZERO;
            if (!this.vertices.isEmpty()) {
                lastId = this.vertices.get(this.vertices.size() - 1);
            }
            LOG.info("scan from hbase source {} lastId value: {} compare {} size {}",
                     vertex, lastId, vertex.compareTo(lastId), this.vertices.size());

            if (vertex.compareTo(lastId) < 0) {
                throw new HugeException("The ramtable feature is not " +
                                        "supported by %s backend",
                                        this.graph.backend());
            }
            if (!vertex.number()) {
                throw new HugeException("Only number id is supported " +
                                        "by ramtable, but got %s id '%s'",
                                        vertex.type().name().toLowerCase(),
                                        vertex);
            }

            if (this.vertices.size() >= ADD_BATCH) {
                this.addEdgesByBatch();
            }
            this.vertices.add(vertex);
        }

        private void addEdgesByBatch() {
            int waitTimes = 0;
            for (Id vertex : this.vertices) {
                List<Edge> adjEdges = this.edges.remove(vertex);
                while (adjEdges == null) {
                    waitTimes++;
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException ignored) {
                        // pass
                    }
                    adjEdges = this.edges.remove(vertex);
                }
                for (int i = 0; i < adjEdges.size(); i++) {
                    HugeEdge edge = (HugeEdge) adjEdges.get(i);
                    assert edge.id().ownerVertexId().equals(vertex);
                    addEdge(i == 0, edge);
                }
            }

            if (waitTimes > this.vertices.size()) {
                LOG.info("Loading wait times is {}", waitTimes);
            }

            this.vertices.clear();
        }
    }
}
