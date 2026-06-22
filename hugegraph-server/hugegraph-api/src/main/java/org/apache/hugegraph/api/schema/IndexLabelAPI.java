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

package org.apache.hugegraph.api.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.RedirectFilter;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.Checkable;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.schema.Userdata;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.GraphMode;
import org.apache.hugegraph.type.define.IndexType;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/graphs/{graph}/schema/indexlabels")
@Singleton
@Tag(name = "IndexLabelAPI")
public class IndexLabelAPI extends API {

    private static final Logger LOG = Log.logger(IndexLabelAPI.class);

    @POST
    @Timed
    @Status(Status.ACCEPTED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$graphspace=$graphspace $owner=$graph " +
                            "$action=index_label_write"})
    @RedirectFilter.RedirectMasterRole
    public String create(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("graph") String graph,
                         JsonIndexLabel jsonIndexLabel) {
        LOG.debug("Graph [{}] create index label: {}", graph, jsonIndexLabel);
        checkCreatingBody(jsonIndexLabel);

        HugeGraph g = graph(manager, graphSpace, graph);
        IndexLabel.Builder builder = jsonIndexLabel.convert2Builder(g);
        SchemaElement.TaskWithSchema il = builder.createWithTask();
        il.indexLabel(mapIndexLabel(il.indexLabel()));
        return manager.serializer().writeTaskWithSchema(il);
    }

    @PUT
    @Timed
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RedirectFilter.RedirectMasterRole
    public String update(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("graph") String graph,
                         @PathParam("name") String name,
                         @QueryParam("action") String action,
                         IndexLabelAPI.JsonIndexLabel jsonIndexLabel) {
        LOG.debug("Graph [{}] {} index label: {}",
                  graph, action, jsonIndexLabel);
        checkUpdatingBody(jsonIndexLabel);
        E.checkArgument(name.equals(jsonIndexLabel.name),
                        "The name in url(%s) and body(%s) are different",
                        name, jsonIndexLabel.name);
        // Parse action parameter
        boolean append = checkAndParseAction(action);

        HugeGraph g = graph(manager, graphSpace, graph);
        IndexLabel.Builder builder = jsonIndexLabel.convert2Builder(g);
        IndexLabel indexLabel = append ? builder.append() : builder.eliminate();
        return manager.serializer().writeIndexlabel(mapIndexLabel(indexLabel));
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$graphspace=$graphspace $owner=$graph " +
                            "$action=index_label_read"})
    public String list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("graph") String graph,
                       @QueryParam("names") List<String> names) {
        boolean listAll = CollectionUtils.isEmpty(names);
        if (listAll) {
            LOG.debug("Graph [{}] list index labels", graph);
        } else {
            LOG.debug("Graph [{}] get index labels by names {}", graph, names);
        }

        HugeGraph g = graph(manager, graphSpace, graph);
        List<IndexLabel> labels;
        if (listAll) {
            labels = g.schema().getIndexLabels();
        } else {
            labels = new ArrayList<>(names.size());
            for (String name : names) {
                labels.add(g.schema().getIndexLabel(name));
            }
        }
        return manager.serializer().writeIndexlabels(mapIndexLabels(labels));
    }

    @GET
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$graphspace=$graphspace $owner=$graph " +
                            "$action=index_label_read"})
    public String get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("graph") String graph,
                      @PathParam("name") String name) {
        LOG.debug("Graph [{}] get index label by name '{}'", graph, name);

        HugeGraph g = graph(manager, graphSpace, graph);
        IndexLabel indexLabel = g.schema().getIndexLabel(name);
        return manager.serializer().writeIndexlabel(mapIndexLabel(indexLabel));
    }

    @DELETE
    @Timed
    @Path("{name}")
    @Status(Status.ACCEPTED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$graphspace=$graphspace $owner=$graph " +
                            "$action=index_label_delete"})
    @RedirectFilter.RedirectMasterRole
    public Map<String, Id> delete(@Context GraphManager manager,
                                  @PathParam("graphspace") String graphSpace,
                                  @PathParam("graph") String graph,
                                  @PathParam("name") String name) {
        LOG.debug("Graph [{}] remove index label by name '{}'", graph, name);

        HugeGraph g = graph(manager, graphSpace, graph);
        // Throw 404 if not exists
        g.schema().getIndexLabel(name);
        return ImmutableMap.of("task_id",
                               g.schema().indexLabel(name).remove());
    }

    private static List<IndexLabel> mapIndexLabels(List<IndexLabel> labels) {
        List<IndexLabel> results = new ArrayList<>(labels.size());
        for (IndexLabel il : labels) {
            results.add(mapIndexLabel(il));
        }
        return results;
    }

    /**
     * Map RANGE_INT/RANGE_FLOAT/RANGE_LONG/RANGE_DOUBLE to RANGE
     */
    private static IndexLabel mapIndexLabel(IndexLabel label) {
        if (label.indexType().isRange()) {
            label = (IndexLabel) label.copy();
            label.indexType(IndexType.RANGE);
        }
        return label;
    }

    /**
     * JsonIndexLabel is only used to receive create and append requests
     */
    @JsonIgnoreProperties(value = {"status"})
    private static class JsonIndexLabel implements Checkable {

        @JsonProperty("id")
        public long id;
        @JsonProperty("name")
        public String name;
        @JsonProperty("base_type")
        public HugeType baseType;
        @JsonProperty("base_value")
        public String baseValue;
        @JsonProperty("index_type")
        public IndexType indexType;
        @JsonProperty("fields")
        public String[] fields;
        @JsonProperty("user_data")
        public Userdata userdata;
        @JsonProperty("check_exist")
        public Boolean checkExist;
        @JsonProperty("rebuild")
        public Boolean rebuild;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.name,
                                   "The name of index label can't be null");
            E.checkArgumentNotNull(this.baseType,
                                   "The base type of index label '%s' " +
                                   "can't be null", this.name);
            E.checkArgument(this.baseType == HugeType.VERTEX_LABEL ||
                            this.baseType == HugeType.EDGE_LABEL,
                            "The base type of index label '%s' can only be " +
                            "either VERTEX_LABEL or EDGE_LABEL", this.name);
            E.checkArgumentNotNull(this.baseValue,
                                   "The base value of index label '%s' " +
                                   "can't be null", this.name);
            E.checkArgumentNotNull(this.indexType,
                                   "The index type of index label '%s' " +
                                   "can't be null", this.name);
            if (this.indexType == IndexType.VECTOR) {
                E.checkArgumentNotNull(this.userdata,
                                       "The user_data(dimension and metric) of " +
                                       "vector index label '%s' " + "can't be null", this.name);
            }

        }

        @Override
        public void checkUpdate() {
            E.checkArgumentNotNull(this.name,
                                   "The name of index label can't be null");
            E.checkArgument(this.baseType == null,
                            "The base type of index label '%s' must be null",
                            this.name);
            E.checkArgument(this.baseValue == null,
                            "The base value of index label '%s' must be null",
                            this.name);
            E.checkArgument(this.indexType == null,
                            "The index type of index label '%s' must be null",
                            this.name);
        }

        private IndexLabel.Builder convert2Builder(HugeGraph g) {
            IndexLabel.Builder builder = g.schema().indexLabel(this.name);
            if (this.id != 0) {
                E.checkArgument(this.id > 0,
                                "Only positive number can be assign as " +
                                "index label id");
                E.checkArgument(g.mode() == GraphMode.RESTORING,
                                "Only accept index label id when graph in " +
                                "RESTORING mode, but '%s' is in mode '%s'",
                                g, g.mode());
                builder.id(this.id);
            }
            if (this.baseType != null) {
                assert this.baseValue != null;
                builder.on(this.baseType, this.baseValue);
            }
            if (this.indexType != null) {
                builder.indexType(this.indexType);
            }
            if (this.fields != null && this.fields.length > 0) {
                builder.by(this.fields);
            }
            if (this.userdata != null) {
                builder.userdata(this.userdata);
            }
            if (this.checkExist != null) {
                builder.checkExist(this.checkExist);
            }
            if (this.rebuild != null) {
                builder.rebuild(this.rebuild);
            }
            if (this.indexType == IndexType.VECTOR) {
                builder.rebuild(false);
            }
            return builder;
        }

        @Override
        public String toString() {
            return String.format("JsonIndexLabel{name=%s, baseType=%s," +
                                 "baseValue=%s, indexType=%s, fields=%s}",
                                 this.name, this.baseType, this.baseValue,
                                 this.indexType, Arrays.toString(this.fields));
        }
    }
}
