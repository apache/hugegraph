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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.struct.schema.EdgeLabel;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.struct.schema.SchemaElement;
import org.apache.hugegraph.struct.schema.VertexLabel;
import org.apache.hugegraph.util.E;

public class SchemaGraph implements HugeGraphSupplier {

    private final String graphSpace;
    private final String graph;
    private final PDConfig pdConfig;
    private final HugeConfig config;

    private final SchemaDriver schemaDriver;

    public SchemaGraph(String graphSpace, String graph, PDConfig pdConfig) {
        this.graphSpace = graphSpace;
        this.graph = graph;
        this.pdConfig = pdConfig;
        this.schemaDriver = schemaDriverInit();
        this.config = this.loadConfig();
    }

    private  SchemaDriver schemaDriverInit() {
        if (SchemaDriver.getInstance() == null) {
            synchronized (SchemaDriver.class) {
                if (SchemaDriver.getInstance() == null) {
                    SchemaDriver.init(this.pdConfig);
                }
            }
        }
        return SchemaDriver.getInstance();
    }

    private HugeConfig loadConfig() {
        // Load configuration from PD
        Map<String, Object> configs =
                schemaDriver.graphConfig(this.graphSpace, this.graph);
        Configuration propConfig = new MapConfiguration(configs);
        return new HugeConfig(propConfig);
    }

    @Override
    public List<String> mapPkId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.propertyKey(id);
            names.add(schema.name());
        }
        return names;
    }

    @Override
    public List<String> mapIlId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.indexLabel(id);
            names.add(schema.name());
        }
        return names;
    }

    @Override
    public HugeConfig configuration(){
        return this.config;
    }

    @Override
    public PropertyKey propertyKey(Id id) {
        return schemaDriver.propertyKey(this.graphSpace, this.graph, id, this);
    }

    public PropertyKey propertyKey(String name) {
        return schemaDriver.propertyKey(this.graphSpace, this.graph, name, this);
    }

    @Override
    public Collection<PropertyKey> propertyKeys() {
        // TODO
        return null;
    }

    @Override
    public VertexLabel vertexLabelOrNone(Id id) {
        VertexLabel vl = vertexLabel(id);
        if (vl == null) {
            vl = VertexLabel.undefined(null, id);
        }
        return vl;
    }

    @Override
    public boolean existsLinkLabel(Id vertexLabel) {
        List<EdgeLabel> edgeLabels =
                schemaDriver.edgeLabels(this.graphSpace, this.graph, this);
        for (EdgeLabel edgeLabel : edgeLabels) {
            if (edgeLabel.linkWithLabel(vertexLabel)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public VertexLabel vertexLabel(Id id) {
        E.checkArgumentNotNull(id, "Vertex label id can't be null");
        if (SchemaElement.OLAP_ID.equals(id)) {
            return VertexLabel.OLAP_VL;
        }
        return schemaDriver.vertexLabel(this.graphSpace, this.graph, id, this);
    }

    @Override
    public VertexLabel vertexLabel(String name) {
        E.checkArgumentNotNull(name, "Vertex label name can't be null");
        E.checkArgument(!name.isEmpty(), "Vertex label name can't be empty");
        if (SchemaElement.OLAP.equals(name)) {
            return VertexLabel.OLAP_VL;
        }
        return schemaDriver.vertexLabel(this.graphSpace, this.graph, name, this);
    }

    @Override
    public EdgeLabel edgeLabel(Id id) {
        return schemaDriver.edgeLabel(this.graphSpace, this.graph, id, this);
    }

    @Override
    public EdgeLabel edgeLabel(String name) {
        return schemaDriver.edgeLabel(this.graphSpace, this.graph, name, this);
    }

    @Override
    public IndexLabel indexLabel(Id id) {
        return schemaDriver.indexLabel(this.graphSpace, this.graph, id, this);
    }

    @Override
    public Collection<IndexLabel> indexLabels() {
        return schemaDriver.indexLabels(this.graphSpace, this.graph, this);
    }

    public IndexLabel indexLabel(String name) {
        return schemaDriver.indexLabel(this.graphSpace, this.graph, name, this);
    }

    @Override
    public String name() {
        return String.join("-", this.graphSpace, this.graph);
    }
}
