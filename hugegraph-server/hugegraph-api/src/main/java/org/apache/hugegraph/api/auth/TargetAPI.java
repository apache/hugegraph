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

package org.apache.hugegraph.api.auth;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeTarget;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.Checkable;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/auth/targets")
@Singleton
@Tag(name = "TargetAPI")
public class TargetAPI extends API {

    private static final Logger LOG = Log.logger(TargetAPI.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @Parameter(description = "The graph space name")
                         @PathParam("graphspace") String graphSpace,
                         JsonTarget jsonTarget) {
        LOG.debug("GraphSpace [{}] create target: {}", graphSpace, jsonTarget);
        GraphSpaceGroupAPI.ensureAuthManager(manager, graphSpace);
        checkCreatingBody(jsonTarget);

        HugeTarget target = jsonTarget.build(graphSpace);
        target.id(manager.authManager().createTarget(graphSpace, target));
        return manager.serializer().writeAuthElement(target);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @Parameter(description = "The graph space name")
                         @PathParam("graphspace") String graphSpace,
                         @Parameter(description = "The target id")
                         @PathParam("id") String id,
                         JsonTarget jsonTarget) {
        LOG.debug("GraphSpace [{}] update target: {}", graphSpace, jsonTarget);
        GraphSpaceGroupAPI.ensureAuthManager(manager, graphSpace);
        checkUpdatingBody(jsonTarget);

        HugeTarget target;
        try {
            target = manager.authManager().getTarget(graphSpace,
                                                     UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid target id: " + id);
        }
        checkGraphSpace(graphSpace, target);
        target = jsonTarget.build(target);
        manager.authManager().updateTarget(graphSpace, target);
        return manager.serializer().writeAuthElement(target);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @Parameter(description = "The graph space name")
                       @PathParam("graphspace") String graphSpace,
                       @Parameter(description = "The limit of results to return")
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("GraphSpace [{}] list targets", graphSpace);
        GraphSpaceGroupAPI.ensureAuthManager(manager, graphSpace);

        List<HugeTarget> targets = listScopedTargets(manager.authManager(),
                                                     graphSpace, limit);
        return manager.serializer().writeAuthElements("targets", targets);
    }

    static List<HugeTarget> listScopedTargets(AuthManager authManager,
                                               String graphSpace,
                                               long limit) {
        List<HugeTarget> targets = authManager.listAllTargets(graphSpace, -1L);
        targets = targets.stream()
                         .filter(target -> graphSpace.equals(
                                 target.graphSpace()))
                         .collect(Collectors.toList());
        return GraphSpaceGroupAPI.applyLimit(targets, limit);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @Parameter(description = "The graph space name")
                      @PathParam("graphspace") String graphSpace,
                      @Parameter(description = "The target id")
                      @PathParam("id") String id) {
        LOG.debug("GraphSpace [{}] get target: {}", graphSpace, id);
        GraphSpaceGroupAPI.ensureAuthManager(manager, graphSpace);

        HugeTarget target = manager.authManager().getTarget(
                graphSpace, UserAPI.parseId(id));
        checkGraphSpace(graphSpace, target);
        return manager.serializer().writeAuthElement(target);
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @Parameter(description = "The graph space name")
                       @PathParam("graphspace") String graphSpace,
                       @Parameter(description = "The target id")
                       @PathParam("id") String id) {
        LOG.debug("GraphSpace [{}] delete target: {}", graphSpace, id);
        GraphSpaceGroupAPI.ensureAuthManager(manager, graphSpace);

        try {
            HugeTarget target = manager.authManager().getTarget(
                    graphSpace, UserAPI.parseId(id));
            checkGraphSpace(graphSpace, target);
            manager.authManager().deleteTarget(graphSpace,
                                               UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid target id: " + id);
        }
    }

    static void checkGraphSpace(String graphSpace, HugeTarget target) {
        E.checkArgumentNotNull(target, "The target can't be null");
        if (!graphSpace.equals(target.graphSpace())) {
            throw new jakarta.ws.rs.ForbiddenException(
                    "Permission denied: target belongs to another graphspace");
        }
    }

    @JsonIgnoreProperties(value = {"id", "target_creator",
                                   "target_create", "target_update"})
    static class JsonTarget implements Checkable {

        @JsonProperty("target_name")
        @Schema(description = "The target name", required = true)
        private String name;
        @JsonProperty("target_graph")
        @Schema(description = "The target graph name", required = true)
        private String graph;
        @JsonProperty("target_url")
        @Schema(description = "The target URL")
        private String url;
        @JsonProperty("target_description")
        @Schema(description = "The target description")
        private String description;
        @JsonProperty("target_resources") // error when List<HugeResource>
        @Schema(description = "The target resources")
        private List<Map<String, Object>> resources;

        public HugeTarget build(HugeTarget target) {
            E.checkArgument(this.name == null ||
                            target.name().equals(this.name),
                            "The name of target can't be updated");
            E.checkArgument(this.graph == null ||
                            target.graph().equals(this.graph),
                            "The graph of target can't be updated");
            if (this.url != null) {
                target.url(this.url);
            }
            if (this.resources != null) {
                target.resources(JsonUtil.toJson(this.resources));
            }
            if (this.description != null) {
                target.description(this.description);
            }
            return target;
        }

        public HugeTarget build(String graphSpace) {
            String targetUrl = this.url == null ? "" : this.url;
            HugeTarget target = new HugeTarget(this.name, this.graph,
                                               targetUrl);
            target.graphSpace(graphSpace);
            target.description(this.description);
            if (this.resources != null) {
                target.resources(JsonUtil.toJson(this.resources));
            }
            return target;
        }

        @Override
        public String toString() {
            return "JsonTarget{" +
                   "name='" + name + '\'' +
                   ", graph='" + graph + '\'' +
                   ", url='" + url + '\'' +
                   ", resources=" + resources +
                   '}';
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.name,
                                   "The name of target can't be null");
            E.checkArgumentNotNull(this.graph,
                                   "The graph of target can't be null");
        }

        @Override
        public void checkUpdate() {
            E.checkArgument(this.url != null || this.resources != null ||
                            this.description != null,
                            "Expect one of target url/resources/description");

        }
    }
}
