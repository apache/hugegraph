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

package org.apache.hugegraph.api.space;

import java.util.Date;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.Checkable;
import org.apache.hugegraph.space.SchemaTemplate;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.SecurityContext;

@Path("graphspaces/{graphspace}/schematemplates")
@Singleton
@Tag(name = "SchemaTemplateAPI")
public class SchemaTemplateAPI extends API {

    private static final Logger LOG = Log.logger(SchemaTemplateAPI.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$dynamic"})
    public Object list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace) {
        LOG.debug("List all schema templates for graph space {}", graphSpace);
        ensurePdModeEnabled(manager);

        Set<String> templates = manager.schemaTemplates(graphSpace);
        return ImmutableMap.of("schema_templates", templates);
    }

    @GET
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$dynamic"})
    public Object get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("name") String name) {
        LOG.debug("Get schema template by name '{}' for graph space {}",
                  name, graphSpace);
        ensurePdModeEnabled(manager);

        return manager.serializer().writeSchemaTemplate(
                       schemaTemplate(manager, graphSpace, name));
    }

    @POST
    @Timed
    @StatusFilter.Status(StatusFilter.Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$dynamic"})
    public String create(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         JsonSchemaTemplate jsonSchemaTemplate) {
        LOG.debug("Create schema template {} for graph space: '{}'",
                  jsonSchemaTemplate, graphSpace);
        ensurePdModeEnabled(manager);
        E.checkArgumentNotNull(jsonSchemaTemplate,
                               "Request body cannot be null or empty");
        jsonSchemaTemplate.checkCreate(false);

        E.checkArgument(manager.graphSpace(graphSpace) != null,
                        "The graph space '%s' is not exist", graphSpace);

        SchemaTemplate template = jsonSchemaTemplate.toSchemaTemplate();
        template.create(new Date());
        template.creator(HugeGraphAuthProxy.username());
        manager.createSchemaTemplate(graphSpace, template);
        return manager.serializer().writeSchemaTemplate(template);
    }

    @DELETE
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$dynamic"})
    public void delete(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("name") String name,
                       @Context SecurityContext sc) {
        LOG.debug("Remove schema template by name '{}' for graph space '{}'",
                  name, graphSpace);
        ensurePdModeEnabled(manager);
        E.checkArgument(manager.graphSpace(graphSpace) != null,
                        "The graph space '%s' is not exist", graphSpace);

        SchemaTemplate st = schemaTemplate(manager, graphSpace, name);
        E.checkArgument(st != null,
                        "Schema template '%s' does not exist", name);

        String username = HugeGraphAuthProxy.username();
        boolean isSpace = manager.authManager()
                                 .isSpaceManager(graphSpace, username);
        if (Objects.equals(st.creator(), username) || isSpace) {
            manager.dropSchemaTemplate(graphSpace, name);
        } else {
            throw new ForbiddenException("No permission to delete schema template");
        }
    }

    @PUT
    @Timed
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$dynamic"})
    public String update(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("name") String name,
                       @Context SecurityContext sc,
                       JsonSchemaTemplate jsonSchemaTemplate) {
        ensurePdModeEnabled(manager);
        E.checkArgumentNotNull(jsonSchemaTemplate,
                               "Request body cannot be null or empty");
        jsonSchemaTemplate.checkUpdate();

        SchemaTemplate old = schemaTemplate(manager, graphSpace, name);
        if (null == old) {
            throw new HugeException("Schema template '%s' does not exist", name);
        }

        String username = HugeGraphAuthProxy.username();
        boolean isSpace = manager.authManager()
                                 .isSpaceManager(graphSpace, username);
        if (Objects.equals(old.creator(), username) || isSpace) {
            SchemaTemplate template = jsonSchemaTemplate.build(old);
            template.creator(old.creator());
            template.create(old.create());
            template.refreshUpdateTime();

            manager.updateSchemaTemplate(graphSpace, template);
            return manager.serializer().writeSchemaTemplate(template);
        }
        throw new ForbiddenException("No permission to update schema template");

    }

    private static class JsonSchemaTemplate implements Checkable {

        @JsonProperty("name")
        public String name;
        @JsonProperty("schema")
        public String schema;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgument(this.name != null && !this.name.isEmpty(),
                            "The name of schema template can't be null or " +
                            "empty");

            E.checkArgument(this.schema != null && !this.schema.isEmpty(),
                            "The schema can't be null or empty");
        }

        @Override
        public void checkUpdate() {
            E.checkArgument(this.schema != null && !this.schema.isEmpty(),
                            "The schema can't be null or empty");
        }

        public SchemaTemplate toSchemaTemplate() {
            return new SchemaTemplate(this.name, this.schema);
        }

        public SchemaTemplate build(SchemaTemplate old) {
            E.checkArgument(StringUtils.isEmpty(this.name) || old.name().equals(this.name),
                            "The name of template can't be updated");
            return new SchemaTemplate(old.name(), this.schema);
        }

        public String toString() {
            return String.format("JsonSchemaTemplate{name=%s, schema=%s}",
                                 this.name, this.schema);
        }
    }
}
