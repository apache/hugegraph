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
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.RedirectFilter;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.Checkable;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.Userdata;
import org.apache.hugegraph.type.define.EdgeLabelType;
import org.apache.hugegraph.type.define.Frequency;
import org.apache.hugegraph.type.define.GraphMode;
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

@Path("graphspaces/{graphspace}/graphs/{graph}/schema/edgelabels")
@Singleton
@Tag(name = "EdgeLabelAPI")
public class EdgeLabelAPI extends API {

    private static final Logger LOG = Log.logger(EdgeLabelAPI.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$graphspace=$graphspace $owner=$graph " +
                            "$action=edge_label_write"})
    @RedirectFilter.RedirectMasterRole
    public String create(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("graph") String graph,
                         JsonEdgeLabel jsonEdgeLabel) {
        LOG.debug("Graph [{}] create edge label: {}", graph, jsonEdgeLabel);
        checkCreatingBody(jsonEdgeLabel);

        HugeGraph g = graph(manager, graphSpace, graph);
        EdgeLabel.Builder builder = jsonEdgeLabel.convert2Builder(g);
        EdgeLabel edgeLabel = builder.create();
        return manager.serializer().writeEdgeLabel(edgeLabel);
    }

    @PUT
    @Timed
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$graphspace=$graphspace $owner=$graph " +
                            "$action=edge_label_write"})
    @RedirectFilter.RedirectMasterRole
    public String update(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("graph") String graph,
                         @PathParam("name") String name,
                         @QueryParam("action") String action,
                         JsonEdgeLabel jsonEdgeLabel) {
        LOG.debug("Graph [{}] {} edge label: {}",
                  graph, action, jsonEdgeLabel);

        checkUpdatingBody(jsonEdgeLabel);
        E.checkArgument(name.equals(jsonEdgeLabel.name),
                        "The name in url(%s) and body(%s) are different",
                        name, jsonEdgeLabel.name);
        // Parse action param
        boolean append = checkAndParseAction(action);

        HugeGraph g = graph(manager, graphSpace, graph);
        EdgeLabel.Builder builder = jsonEdgeLabel.convert2Builder(g);
        EdgeLabel edgeLabel = append ? builder.append() : builder.eliminate();
        return manager.serializer().writeEdgeLabel(edgeLabel);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$graphspace=$graphspace $owner=$graph " +
                            "$action=edge_label_read"})
    public String list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("graph") String graph,
                       @QueryParam("names") List<String> names) {
        boolean listAll = CollectionUtils.isEmpty(names);
        if (listAll) {
            LOG.debug("Graph [{}] list edge labels", graph);
        } else {
            LOG.debug("Graph [{}] get edge labels by names {}", graph, names);
        }

        HugeGraph g = graph(manager, graphSpace, graph);
        List<EdgeLabel> labels;
        if (listAll) {
            labels = g.schema().getEdgeLabels();
        } else {
            labels = new ArrayList<>(names.size());
            for (String name : names) {
                labels.add(g.schema().getEdgeLabel(name));
            }
        }
        return manager.serializer().writeEdgeLabels(labels);
    }

    @GET
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$graphspace=$graphspace $owner=$graph " +
                            "$action=edge_label_read"})
    public String get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("graph") String graph,
                      @PathParam("name") String name) {
        LOG.debug("Graph [{}] get edge label by name '{}'", graph, name);

        HugeGraph g = graph(manager, graphSpace, graph);
        EdgeLabel edgeLabel = g.schema().getEdgeLabel(name);
        return manager.serializer().writeEdgeLabel(edgeLabel);
    }

    @DELETE
    @Timed
    @Path("{name}")
    @Status(Status.ACCEPTED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$graphspace=$graphspace $owner=$graph " +
                            "$action=edge_label_delete"})
    @RedirectFilter.RedirectMasterRole
    public Map<String, Id> delete(@Context GraphManager manager,
                                  @PathParam("graphspace") String graphSpace,
                                  @PathParam("graph") String graph,
                                  @PathParam("name") String name) {
        LOG.debug("Graph [{}] remove edge label by name '{}'", graph, name);

        HugeGraph g = graph(manager, graphSpace, graph);
        // Throw 404 if not exists
        g.schema().getEdgeLabel(name);
        return ImmutableMap.of("task_id",
                               g.schema().edgeLabel(name).remove());
    }

    /**
     * JsonEdgeLabel is only used to receive create and append requests
     */
    @JsonIgnoreProperties(value = {"index_labels", "status"})
    private static class JsonEdgeLabel implements Checkable {

        @JsonProperty("id")
        public long id;
        @JsonProperty("name")
        public String name;
        @JsonProperty("edgelabel_type")
        public EdgeLabelType edgeLabelType;
        @JsonProperty("parent_label")
        public String fatherLabel;
        @JsonProperty("source_label")
        public String sourceLabel;
        @JsonProperty("target_label")
        public String targetLabel;
        @JsonProperty("links")
        public Set<Map<String, String>> links;
        @JsonProperty("frequency")
        public Frequency frequency;
        @JsonProperty("properties")
        public String[] properties;
        @JsonProperty("sort_keys")
        public String[] sortKeys;
        @JsonProperty("nullable_keys")
        public String[] nullableKeys;
        @JsonProperty("ttl")
        public long ttl;
        @JsonProperty("ttl_start_time")
        public String ttlStartTime;
        @JsonProperty("enable_label_index")
        public Boolean enableLabelIndex;
        @JsonProperty("user_data")
        public Userdata userdata;
        @JsonProperty("check_exist")
        public Boolean checkExist;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.name, "The name of edge label can't be null");
        }

        private EdgeLabel.Builder convert2Builder(HugeGraph g) {
            EdgeLabel.Builder builder = g.schema().edgeLabel(this.name);
            if (this.id != 0) {
                E.checkArgument(this.id > 0,
                                "Only positive number can be assign as " +
                                "edge label id");
                E.checkArgument(g.mode() == GraphMode.RESTORING,
                                "Only accept edge label id when graph in " +
                                "RESTORING mode, but '%s' is in mode '%s'",
                                g, g.mode());
                builder.id(this.id);
            }
            if (this.edgeLabelType == null) {
                this.edgeLabelType = EdgeLabelType.NORMAL;
            } else if (this.edgeLabelType.parent()) {
                builder.asBase();
            } else if (this.edgeLabelType.sub()) {
                builder.withBase(this.fatherLabel);
            } else {
                E.checkArgument(this.edgeLabelType.normal(),
                                "Please enter a valid edge_label_type value " +
                                "in [NORMAL, PARENT, SUB]");
            }
            if (this.sourceLabel != null) {
                builder.sourceLabel(this.sourceLabel);
            }
            if (this.targetLabel != null) {
                builder.targetLabel(this.targetLabel);
            }
            if (this.links != null && !this.links.isEmpty()) {
                for (Map<String, String> map : this.links) {
                    E.checkArgument(map.size() == 1,
                                    "The map size must be 1, due to it is a " +
                                    "pair");
                    Map.Entry<String, String> entry =
                            map.entrySet().iterator().next();
                    builder.link(entry.getKey(), entry.getValue());
                }
            }
            if (this.frequency != null) {
                builder.frequency(this.frequency);
            }
            if (this.properties != null) {
                builder.properties(this.properties);
            }
            if (this.sortKeys != null) {
                builder.sortKeys(this.sortKeys);
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
            return String.format("JsonEdgeLabel{" +
                                 "name=%s, sourceLabel=%s, targetLabel=%s, frequency=%s, " +
                                 "sortKeys=%s, nullableKeys=%s, properties=%s, ttl=%s, " +
                                 "ttlStartTime=%s}",
                                 this.name, this.sourceLabel, this.targetLabel,
                                 this.frequency, Arrays.toString(this.sortKeys),
                                 Arrays.toString(this.nullableKeys),
                                 Arrays.toString(this.properties), this.ttl, this.ttlStartTime);
        }
    }
}
