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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.struct.schema.SchemaElement;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.util.DateUtil;

/**
 * Provides shared schema, configuration and time access without requiring a graph engine.
 */
public interface HugeGraphSupplier {

    List<String> mapPkId2Name(Collection<Id> ids);

    List<String> mapIlId2Name(Collection<Id> ids);

    PropertyKey propertyKey(Id key);

    Collection<PropertyKey> propertyKeys();

    VertexLabel vertexLabelOrNone(Id id);

    boolean existsLinkLabel(Id vertexLabel);

    VertexLabel vertexLabel(Id label);

    VertexLabel vertexLabel(String label);


    default EdgeLabel edgeLabelOrNone(Id id) {
        EdgeLabel el = this.edgeLabel(id);
        if (el == null) {
            el = EdgeLabel.undefined(this, id);
        }
        return el;
    }
    EdgeLabel edgeLabel(Id label);

    EdgeLabel edgeLabel(String label);

    IndexLabel indexLabel(Id id);

    Collection<IndexLabel> indexLabels();

    String name();

    default String spaceGraphName() {
        return this.name();
    }

    HugeConfig configuration();

    default long now() {
        return DateUtil.now().getTime();
    }


    default List<String> mapElId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.edgeLabel(id);
            names.add(schema.name());
        }
        return names;
    }

    default Set<Pair<String, String>> mapPairId2Name(
            Set<Pair<Id, Id>> pairs) {
        Set<Pair<String, String>> results = new HashSet<>(pairs.size());
        for (Pair<Id, Id> pair : pairs) {
            results.add(Pair.of(this.vertexLabel(pair.getLeft()).name(),
                                this.vertexLabel(pair.getRight()).name()));
        }
        return results;
    }
}
