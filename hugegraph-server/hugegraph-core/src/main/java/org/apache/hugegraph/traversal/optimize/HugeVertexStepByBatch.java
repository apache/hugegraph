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

package org.apache.hugegraph.traversal.optimize;

import java.util.Iterator;
import java.util.List;

import org.apache.hugegraph.query.BatchConditionQuery;
import org.apache.hugegraph.query.ConditionQuery;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.iterator.BatchMapperIterator;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

public class HugeVertexStepByBatch<E extends Element>
        extends HugeVertexStep<E> {

    private static final long serialVersionUID = -3609787815053052222L;

    private BatchMapperIterator<Traverser.Admin<Vertex>, E> batchIterator;
    private Traverser.Admin<Vertex> head;
    private Iterator<E> iterator;

    public HugeVertexStepByBatch(final VertexStep<E> originalVertexStep) {
        super(originalVertexStep);
        this.batchIterator = null;
        this.head = null;
        this.iterator = null;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        /* Override super.processNextStart() */
        if (this.batchIterator == null) {
            int batchSize = (int) Query.QUERY_BATCH;
            this.batchIterator = new BatchMapperIterator<>(
                    batchSize, this.starts, this::flatMap);
        }

        if (this.batchIterator.hasNext()) {
            assert this.head != null;
            E item = this.batchIterator.next();
            // TODO: find the parent node accurately instead the head
            return this.head.split(item, this);
        }

        throw FastNoSuchElementException.instance();
    }

    @Override
    public void reset() {
        super.reset();
        this.closeIterator();
        this.batchIterator = null;
        this.head = null;
    }

    @Override
    public Iterator<?> lastTimeResults() {
        /*
         * NOTE: fetch page from this iterator, can only get page info of
         * the lowest level, may lost info of upper levels.
         */
        return this.iterator;
    }

    @Override
    protected void closeIterator() {
        CloseableIterator.closeIterator(this.batchIterator);
    }

    @SuppressWarnings("unchecked")
    private Iterator<E> flatMap(List<Traverser.Admin<Vertex>> traversers) {
        if (this.head == null && !traversers.isEmpty()) {
            this.head = traversers.get(0);
        }
        boolean queryVertex = this.returnsVertex();
        boolean queryEdge = this.returnsEdge();
        assert queryVertex || queryEdge;
        if (queryVertex) {
            this.iterator = (Iterator<E>) this.vertices(traversers);
        } else {
            assert queryEdge;
            this.iterator = (Iterator<E>) this.edges(traversers);
        }
        return this.iterator;
    }

    private Iterator<Vertex> vertices(
            List<Traverser.Admin<Vertex>> traversers) {
        assert !traversers.isEmpty();

        Iterator<Edge> edges = this.edges(traversers);
        return this.queryAdjacentVertices(edges);
    }

    private Iterator<Edge> edges(List<Traverser.Admin<Vertex>> traversers) {
        assert !traversers.isEmpty();

        BatchConditionQuery batchQuery = new BatchConditionQuery(
                HugeType.EDGE, traversers.size());

        for (Traverser.Admin<Vertex> traverser : traversers) {
            ConditionQuery query = this.constructEdgesQuery(traverser);
            /*
             * Merge each query into batch query through IN condition
             * NOTE: duplicated results may be removed by backend store
             */
            batchQuery.mergeToIN(query, HugeKeys.OWNER_VERTEX);
        }

        this.injectQueryInfo(batchQuery);
        return this.queryEdges(batchQuery);
    }
}
