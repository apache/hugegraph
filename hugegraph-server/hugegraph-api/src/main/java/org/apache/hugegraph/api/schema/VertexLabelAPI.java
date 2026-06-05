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
import org.apache.hugegraph.schema.Userdata;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.type.define.GraphMode;
import org.apache.hugegraph.type.define.IdStrategy;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
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

@Path("graphspaces/{graphspace}/graphs/{graph}/schema/vertexlabels")
@Singleton
@Tag(name = "VertexLabelAPI")
public class VertexLabelAPI extends API {

    private static final Logger LOG = Log.logger(VertexLabelAPI.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$graphspace=$graphspace $owner=$graph " +
                            "$action=vertex_label_write"})
    @RedirectFilter.RedirectMasterRole
    public String create(@Context GraphManager manager,
                         @Parameter(description = "The graph space name")
                         @PathParam("graphspace") String graphSpace,
                         @Parameter(description = "The graph name")
                         @PathParam("graph") String graph,
                         JsonVertexLabel jsonVertexLabel) {
        LOG.debug("Graph [{}] create vertex label: {}",
                  graph, jsonVertexLabel);
        checkCreatingBody(jsonVertexLabel);

        HugeGraph g = graph(manager, graphSpace, graph);
        VertexLabel.Builder builder = jsonVertexLabel.convert2Builder(g);
        VertexLabel vertexLabel = builder.create();
        return manager.serializer().writeVertexLabel(vertexLabel);
    }

    @PUT
    @Timed
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$graphspace=$graphspace $owner=$graph " +
                            "$action=vertex_label_write"})
    @RedirectFilter.RedirectMasterRole
    public String update(@Context GraphManager manager,
                         @Parameter(description = "The graph space name")
                         @PathParam("graphspace") String graphSpace,
                         @Parameter(description = "The graph name")
                         @PathParam("graph") String graph,
                         @Parameter(description = "The vertex label name")
                         @PathParam("name") String name,
                         @Parameter(description = "Action to perform: 'append' or 'remove'")
                         @QueryParam("action") String action,
                         JsonVertexLabel jsonVertexLabel) {
        LOG.debug("Graph [{}] {} vertex label: {}",
                  graph, action, jsonVertexLabel);

        checkUpdatingBody(jsonVertexLabel);
        E.checkArgument(name.equals(jsonVertexLabel.name),
                        "The name in url(%s) and body(%s) are different",
                        name, jsonVertexLabel.name);
        // Parse action parameter
        boolean append = checkAndParseAction(action);

        HugeGraph g = graph(manager, graphSpace, graph);
        VertexLabel.Builder builder = jsonVertexLabel.convert2Builder(g);
        VertexLabel vertexLabel = append ?
                                  builder.append() :
                                  builder.eliminate();
        return manager.serializer().writeVertexLabel(vertexLabel);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$graphspace=$graphspace $owner=$graph " +
                            "$action=vertex_label_read"})
    public String list(@Context GraphManager manager,
                       @Parameter(description = "The graph space name")
                       @PathParam("graphspace") String graphSpace,
                       @Parameter(description = "The graph name")
                       @PathParam("graph") String graph,
                       @Parameter(description = "Filter vertex labels by names")
                       @QueryParam("names") List<String> names) {
        boolean listAll = CollectionUtils.isEmpty(names);
        if (listAll) {
            LOG.debug("Graph [{}] list vertex labels", graph);
        } else {
            LOG.debug("Graph [{}] get vertex labels by names {}", graph, names);
        }

        HugeGraph g = graph(manager, graphSpace, graph);
        List<VertexLabel> labels;
        if (listAll) {
            labels = g.schema().getVertexLabels();
        } else {
            labels = new ArrayList<>(names.size());
            for (String name : names) {
                labels.add(g.schema().getVertexLabel(name));
            }
        }
        return manager.serializer().writeVertexLabels(labels);
    }

    @GET
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$graphspace=$graphspace $owner=$graph " +
                            "$action=vertex_label_read"})
    public String get(@Context GraphManager manager,
                      @Parameter(description = "The graph space name")
                      @PathParam("graphspace") String graphSpace,
                      @Parameter(description = "The graph name")
                      @PathParam("graph") String graph,
                      @Parameter(description = "The vertex label name")
                      @PathParam("name") String name) {
        LOG.debug("Graph [{}] get vertex label by name '{}'", graph, name);

        HugeGraph g = graph(manager, graphSpace, graph);
        VertexLabel vertexLabel = g.schema().getVertexLabel(name);
        return manager.serializer().writeVertexLabel(vertexLabel);
    }

    @DELETE
    @Timed
    @Path("{name}")
    @Status(Status.ACCEPTED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$graphspace=$graphspace $owner=$graph " +
                            "$action=vertex_label_delete"})
    @RedirectFilter.RedirectMasterRole
    public Map<String, Id> delete(@Context GraphManager manager,
                                  @Parameter(description = "The graph space name")
                                  @PathParam("graphspace") String graphSpace,
                                  @Parameter(description = "The graph name")
                                  @PathParam("graph") String graph,
                                  @Parameter(description = "The vertex label name to delete")
                                  @PathParam("name") String name) {
        LOG.debug("Graph [{}] remove vertex label by name '{}'", graph, name);

        HugeGraph g = graph(manager, graphSpace, graph);
        // Throw 404 if not exists
        g.schema().getVertexLabel(name);
        return ImmutableMap.of("task_id",
                               g.schema().vertexLabel(name).remove());
    }

    /**
     * JsonVertexLabel is only used to receive create and append requests
     */
    @JsonIgnoreProperties(value = {"index_labels", "status"})
    @Schema(description = "Vertex label creation/update request")
    private static class JsonVertexLabel implements Checkable {

        @Schema(description = "The vertex label ID (only used in RESTORING mode)")
        @JsonProperty("id")
        public long id;
        @Schema(description = "The vertex label name", required = true)
        @JsonProperty("name")
        public String name;
        @Schema(description = "The ID strategy: AUTOMATIC, PRIMARY_KEY, " +
                               "CUSTOMIZE_STRING, CUSTOMIZE_NUMBER, CUSTOMIZE_UUID")
        @JsonProperty("id_strategy")
        public IdStrategy idStrategy;
        @Schema(description = "The property key names associated with this vertex label")
        @JsonProperty("properties")
        public String[] properties;
        @Schema(description = "The primary key names (used with PRIMARY_KEY strategy)")
        @JsonProperty("primary_keys")
        public String[] primaryKeys;
        @Schema(description = "The nullable property key names")
        @JsonProperty("nullable_keys")
        public String[] nullableKeys;
        @Schema(description = "Time-to-live in seconds")
        @JsonProperty("ttl")
        public long ttl;
        @Schema(description = "The property key name to use as TTL start time")
        @JsonProperty("ttl_start_time")
        public String ttlStartTime;
        @Schema(description = "Whether to enable label indexing")
        @JsonProperty("enable_label_index")
        public Boolean enableLabelIndex;
        @Schema(description = "User-defined metadata")
        @JsonProperty("user_data")
        public Userdata userdata;
        @Schema(description = "Whether to check if vertex label exists before creation")
        @JsonProperty("check_exist")
        public Boolean checkExist;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.name,
                                   "The name of vertex label can't be null");
        }

        private VertexLabel.Builder convert2Builder(HugeGraph g) {
            VertexLabel.Builder builder = g.schema().vertexLabel(this.name);
            if (this.id != 0) {
                E.checkArgument(this.id > 0,
                                "Only positive number can be assign as " +
                                "vertex label id");
                E.checkArgument(g.mode() == GraphMode.RESTORING,
                                "Only accept vertex label id when graph in " +
                                "RESTORING mode, but '%s' is in mode '%s'",
                                g, g.mode());
                builder.id(this.id);
            }
            if (this.idStrategy != null) {
                builder.idStrategy(this.idStrategy);
            }
            if (this.properties != null) {
                builder.properties(this.properties);
            }
            if (this.primaryKeys != null) {
                builder.primaryKeys(this.primaryKeys);
            }
            if (this.nullableKeys != null) {
                builder.nullableKeys(this.nullableKeys);
            }
            if (this.enableLabelIndex != null) {
                builder.enableLabelIndex(this.enableLabelIndex);
            }
            if (this.userdata != null) {
                builder.userdata(this.userdata);
            }
            if (this.checkExist != null) {
                builder.checkExist(this.checkExist);
            }
            if (this.ttl != 0) {
                builder.ttl(this.ttl);
            }
            if (this.ttlStartTime != null) {
                E.checkArgument(this.ttl > 0,
                                "Only set ttlStartTime when ttl is " +
                                "positive,  but got ttl: %s", this.ttl);
                builder.ttlStartTime(this.ttlStartTime);
            }
            return builder;
        }

        @Override
        public String toString() {
            return String.format("JsonVertexLabel{" +
                                 "name=%s, idStrategy=%s, primaryKeys=%s, nullableKeys=%s, " +
                                 "properties=%s, ttl=%s, ttlStartTime=%s}",
                                 this.name, this.idStrategy, Arrays.toString(this.primaryKeys),
                                 Arrays.toString(this.nullableKeys),
                                 Arrays.toString(this.properties), this.ttl, this.ttlStartTime);
        }
    }
}
