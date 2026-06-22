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
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.Checkable;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.schema.Userdata;
import org.apache.hugegraph.type.define.AggregateType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.GraphMode;
import org.apache.hugegraph.type.define.WriteType;
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

@Path("graphspaces/{graphspace}/graphs/{graph}/schema/propertykeys")
@Singleton
@Tag(name = "PropertyKeyAPI")
public class PropertyKeyAPI extends API {

    private static final Logger LOG = Log.logger(PropertyKeyAPI.class);

    @POST
    @Timed
    @Status(Status.ACCEPTED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$graphspace=$graphspace $owner=$graph " +
                            "$action=property_key_write"})
    @RedirectFilter.RedirectMasterRole
    public String create(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("graph") String graph,
                         JsonPropertyKey jsonPropertyKey) {
        LOG.debug("Graph [{}] create property key: {}", graph, jsonPropertyKey);
        checkCreatingBody(jsonPropertyKey);

        HugeGraph g = graph(manager, graphSpace, graph);
        PropertyKey.Builder builder = jsonPropertyKey.convert2Builder(g);
        SchemaElement.TaskWithSchema pk = builder.createWithTask();
        return manager.serializer().writeTaskWithSchema(pk);
    }

    @PUT
    @Timed
    @Status(Status.ACCEPTED)
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$graphspace=$graphspace $owner=$graph " +
                            "$action=property_key_write"})
    @RedirectFilter.RedirectMasterRole
    public String update(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("graph") String graph,
                         @PathParam("name") String name,
                         @QueryParam("action") String action,
                         PropertyKeyAPI.JsonPropertyKey jsonPropertyKey) {
        LOG.debug("Graph [{}] {} property key: {}",
                  graph, action, jsonPropertyKey);
        checkUpdatingBody(jsonPropertyKey);
        E.checkArgument(name.equals(jsonPropertyKey.name),
                        "The name in url(%s) and body(%s) are different",
                        name, jsonPropertyKey.name);

        HugeGraph g = graph(manager, graphSpace, graph);
        if (ACTION_CLEAR.equals(action)) {
            PropertyKey propertyKey = g.propertyKey(name);
            E.checkArgument(propertyKey.olap(),
                            "Only olap property key can do action clear, " +
                            "but got '%s'", propertyKey);
            Id id = g.clearPropertyKey(propertyKey);
            SchemaElement.TaskWithSchema pk =
                    new SchemaElement.TaskWithSchema(propertyKey, id);
            return manager.serializer().writeTaskWithSchema(pk);
        }

        // Parse action parameter
        boolean append = checkAndParseAction(action);

        PropertyKey.Builder builder = jsonPropertyKey.convert2Builder(g);
        PropertyKey propertyKey = append ?
                                  builder.append() :
                                  builder.eliminate();
        SchemaElement.TaskWithSchema pk =
                new SchemaElement.TaskWithSchema(propertyKey, IdGenerator.ZERO);
        return manager.serializer().writeTaskWithSchema(pk);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$graphspace=$graphspace $owner=$graph " +
                            "$action=property_key_read"})
    public String list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("graph") String graph,
                       @QueryParam("names") List<String> names) {
        boolean listAll = CollectionUtils.isEmpty(names);
        if (listAll) {
            LOG.debug("Graph [{}] list property keys", graph);
        } else {
            LOG.debug("Graph [{}] get property keys by names {}", graph, names);
        }

        HugeGraph g = graph(manager, graphSpace, graph);
        List<PropertyKey> propKeys;
        if (listAll) {
            propKeys = g.schema().getPropertyKeys();
        } else {
            propKeys = new ArrayList<>(names.size());
            for (String name : names) {
                propKeys.add(g.schema().getPropertyKey(name));
            }
        }
        return manager.serializer().writePropertyKeys(propKeys);
    }

    @GET
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$graphspace=$graphspace $owner=$graph " +
                            "$action=property_key_read"})
    public String get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("graph") String graph,
                      @PathParam("name") String name) {
        LOG.debug("Graph [{}] get property key by name '{}'", graph, name);

        HugeGraph g = graph(manager, graphSpace, graph);
        PropertyKey propertyKey = g.schema().getPropertyKey(name);
        return manager.serializer().writePropertyKey(propertyKey);
    }

    @DELETE
    @Timed
    @Status(Status.ACCEPTED)
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$graphspace=$graphspace $owner=$graph " +
                            "$action=property_key_delete"})
    @RedirectFilter.RedirectMasterRole
    public Map<String, Id> delete(@Context GraphManager manager,
                                  @PathParam("graphspace") String graphSpace,
                                  @PathParam("graph") String graph,
                                  @PathParam("name") String name) {
        LOG.debug("Graph [{}] remove property key by name '{}'", graph, name);

        HugeGraph g = graph(manager, graphSpace, graph);
        // Throw 404 if not exists
        g.schema().getPropertyKey(name);
        return ImmutableMap.of("task_id",
                               g.schema().propertyKey(name).remove());
    }

    /**
     * JsonPropertyKey is only used to receive create and append requests
     */
    @JsonIgnoreProperties(value = {"status"})
    private static class JsonPropertyKey implements Checkable {

        @JsonProperty("id")
        public long id;
        @JsonProperty("name")
        public String name;
        @JsonProperty("cardinality")
        public Cardinality cardinality;
        @JsonProperty("data_type")
        public DataType dataType;
        @JsonProperty("aggregate_type")
        public AggregateType aggregateType;
        @JsonProperty("write_type")
        public WriteType writeType;
        @JsonProperty("properties")
        public String[] properties;
        @JsonProperty("user_data")
        public Userdata userdata;
        @JsonProperty("check_exist")
        public Boolean checkExist;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.name,
                                   "The name of property key can't be null");
            E.checkArgument(this.properties == null ||
                            this.properties.length == 0,
                            "Not allowed to pass properties when " +
                            "creating property key since it doesn't " +
                            "support meta properties currently");
        }

        private PropertyKey.Builder convert2Builder(HugeGraph g) {
            PropertyKey.Builder builder = g.schema().propertyKey(this.name);
            if (this.id != 0) {
                E.checkArgument(this.id > 0,
                                "Only positive number can be assign as " +
                                "property key id");
                E.checkArgument(g.mode() == GraphMode.RESTORING,
                                "Only accept property key id when graph in " +
                                "RESTORING mode, but '%s' is in mode '%s'",
                                g, g.mode());
                builder.id(this.id);
            }
            if (this.cardinality != null) {
                builder.cardinality(this.cardinality);
            }
            if (this.dataType != null) {
                builder.dataType(this.dataType);
            }
            if (this.aggregateType != null) {
                builder.aggregateType(this.aggregateType);
            }
            if (this.writeType != null) {
                builder.writeType(this.writeType);
            }
            if (this.userdata != null) {
                builder.userdata(this.userdata);
            }
            if (this.checkExist != null) {
                builder.checkExist(this.checkExist);
            }
            return builder;
        }

        @Override
        public String toString() {
            return String.format("JsonPropertyKey{name=%s, cardinality=%s, " +
                                 "dataType=%s, aggregateType=%s, " +
                                 "writeType=%s, properties=%s}",
                                 this.name, this.cardinality,
                                 this.dataType, this.aggregateType,
                                 this.writeType, Arrays.toString(this.properties));
        }
    }
}
