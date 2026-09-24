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

package org.apache.hugegraph.api.traversers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.schema.SchemaLabel;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.optimize.TraversalUtil;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.v3.oas.annotations.media.Schema;

public class Vertices {

    @JsonProperty("ids")
    @Schema(description = "The vertex IDs", example = "[\"1:Tom\", \"2:Mary\"]")
    public Set<Object> ids;
    @JsonProperty("label")
    @Schema(description = "The vertex label", example = "person")
    public String label;
    @JsonProperty("properties")
    @Schema(description = "The vertex properties in key-value format")
    public Map<String, Object> properties;

    public Iterator<Vertex> vertices(HugeGraph g) {
        Map<String, Object> props = this.properties;
        E.checkArgument(!((this.ids == null || this.ids.isEmpty()) &&
                          (props == null || props.isEmpty()) &&
                          this.label == null), "No source vertices provided");
        Iterator<Vertex> iterator;
        if (this.ids != null && !this.ids.isEmpty()) {
            E.checkArgument(this.label == null,
                            "Just provide one of ids or label of source vertices");
            E.checkArgument(props == null || props.isEmpty(),
                            "Just provide one of ids or properties of source vertices");
            List<Id> sourceIds = new ArrayList<>(this.ids.size());
            for (Object id : this.ids) {
                sourceIds.add(HugeVertex.getIdValue(id));
            }
            iterator = g.vertices(sourceIds.toArray());
            E.checkArgument(iterator.hasNext(),
                            "Not exist source vertices with ids %s",
                            this.ids);
        } else {
            ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
            if (this.label != null) {
                Id label = SchemaLabel.getVertexLabelId(g, this.label);
                query.eq(HugeKeys.LABEL, label);
            }
            if (props != null && !props.isEmpty()) {
                Map<Id, Object> pks = TraversalUtil.transProperties(g, props);
                TraversalUtil.fillConditionQuery(query, pks, g);
            }
            assert !query.empty();
            iterator = g.vertices(query);
            E.checkArgument(iterator.hasNext(), "Not exist source vertex " +
                                                "with label '%s' and properties '%s'",
                            this.label, props);
        }
        return iterator;
    }

    @Override
    public String toString() {
        return String.format("SourceVertex{ids=%s,label=%s,properties=%s}",
                             this.ids, this.label, this.properties);
    }
}
