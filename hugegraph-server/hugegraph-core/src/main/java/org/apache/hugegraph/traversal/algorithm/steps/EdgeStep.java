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

package org.apache.hugegraph.traversal.algorithm.steps;

import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEGREE;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.NO_LIMIT;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.optimize.TraversalUtil;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Edge;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class EdgeStep {

    protected Directions direction;
    protected final Map<Id, String> labels;
    protected final Map<Id, Object> properties;
    protected final long degree;
    protected final long skipDegree;

    public EdgeStep(HugeGraph g, Directions direction) {
        this(g, direction, ImmutableList.of());
    }

    public EdgeStep(HugeGraph g, List<String> labels) {
        this(g, Directions.BOTH, labels);
    }

    public EdgeStep(HugeGraph g, Map<String, Object> properties) {
        this(g, Directions.BOTH, ImmutableList.of(), properties);
    }

    public EdgeStep(HugeGraph g, Directions direction, List<String> labels) {
        this(g, direction, labels, ImmutableMap.of());
    }

    public EdgeStep(HugeGraph g, Directions direction, List<String> labels,
                    Map<String, Object> properties) {
        this(g, direction, labels, properties,
             Long.parseLong(DEFAULT_MAX_DEGREE), 0L);
    }

    public EdgeStep(HugeGraph g, Directions direction, List<String> labels,
                    Map<String, Object> properties,
                    long degree, long skipDegree) {
        E.checkArgumentNotNull(g, "The graph can't be null");
        E.checkArgument(degree == NO_LIMIT || degree > 0L,
                        "The max degree must be > 0 or == -1, but got: %s",
                        degree);
        HugeTraverser.checkSkipDegree(skipDegree, degree,
                                      HugeTraverser.NO_LIMIT);
        this.direction = direction;

        // Parse edge labels
        Map<Id, String> labelIds = new HashMap<>();
        if (labels != null) {
            for (String label : labels) {
                EdgeLabel el = g.edgeLabel(label);
                labelIds.put(el.id(), label);
            }
        }
        this.labels = labelIds;

        // Parse properties
        if (properties == null || properties.isEmpty()) {
            this.properties = null;
        } else {
            this.properties = TraversalUtil.transProperties(g, properties);
        }

        this.degree = degree;
        this.skipDegree = skipDegree;
    }

    public Directions direction() {
        return this.direction;
    }

    public Map<Id, String> labels() {
        return this.labels;
    }

    public Map<Id, Object> properties() {
        return this.properties;
    }

    public long degree() {
        return this.degree;
    }

    public long skipDegree() {
        return this.skipDegree;
    }

    public Id[] edgeLabels() {
        int elsSize = this.labels.size();
        return this.labels.keySet().toArray(new Id[elsSize]);
    }

    public void swithDirection() {
        this.direction = this.direction.opposite();
    }

    public long limit() {
        return this.skipDegree > 0L ? this.skipDegree : this.degree;
    }

    @Override
    public String toString() {
        return String.format("EdgeStep{direction=%s,labels=%s,properties=%s}",
                             this.direction, this.labels, this.properties);
    }

    public Iterator<Edge> skipSuperNodeIfNeeded(Iterator<Edge> edges) {
        return HugeTraverser.skipSuperNodeIfNeeded(edges, this.degree,
                                                   this.skipDegree);
    }
}
