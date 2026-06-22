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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeProject;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.Checkable;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

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

@Path("graphspaces/{graphspace}/auth/projects")
@Singleton
@Tag(name = "ProjectAPI")
public class ProjectAPI extends API {

    private static final Logger LOG = Log.logger(ProjectAPI.class);
    private static final String ACTION_ADD_GRAPH = "add_graph";
    private static final String ACTION_REMOVE_GRAPH = "remove_graph";

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         JsonProject jsonProject) {
        LOG.debug("GraphSpace [{}] create project: {}", graphSpace, jsonProject);
        checkCreatingBody(jsonProject);

        HugeProject project = jsonProject.build();
        Id projectId = manager.authManager().createProject(project);
        /*
         * Some fields of project(like admin_group) can only be known after
         * created
         */
        project = manager.authManager().getProject(projectId);
        return manager.serializer().writeAuthElement(project);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("id") String id,
                         @QueryParam("action") String action,
                         JsonProject jsonProject) {
        LOG.debug("GraphSpace [{}] update {} project: {}", graphSpace, action,
                  jsonProject);
        checkUpdatingBody(jsonProject);

        HugeProject project;
        Id projectId = UserAPI.parseId(id);
        AuthManager authManager = manager.authManager();
        try {
            project = authManager.getProject(projectId);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid project id: " + id);
        }
        if (ProjectAPI.isAddGraph(action)) {
            project = jsonProject.buildAddGraph(project);
        } else if (ProjectAPI.isRemoveGraph(action)) {
            project = jsonProject.buildRemoveGraph(project);
        } else {
            E.checkArgument(StringUtils.isEmpty(action),
                            "The action parameter can only be either " +
                            "%s or %s or '', but got '%s'",
                            ProjectAPI.ACTION_ADD_GRAPH,
                            ProjectAPI.ACTION_REMOVE_GRAPH,
                            action);
            project = jsonProject.buildUpdateDescription(project);
        }
        authManager.updateProject(project);
        return manager.serializer().writeAuthElement(project);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("GraphSpace [{}] list project", graphSpace);

        List<HugeProject> projects = manager.authManager()
                                            .listAllProject(limit);
        return manager.serializer().writeAuthElements("projects", projects);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("id") String id) {
        LOG.debug("GraphSpace [{}] get project: {}", graphSpace, id);

        HugeProject project;
        try {
            project = manager.authManager().getProject(UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid project id: " + id);
        }
        return manager.serializer().writeAuthElement(project);
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("id") String id) {
        LOG.debug("GraphSpace [{}] delete project: {}", graphSpace, id);

        try {
            manager.authManager().deleteProject(UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid project id: " + id);
        }
    }

    public static boolean isAddGraph(String action) {
        return ACTION_ADD_GRAPH.equals(action);
    }

    public static boolean isRemoveGraph(String action) {
        return ACTION_REMOVE_GRAPH.equals(action);
    }

    @JsonIgnoreProperties(value = {"id", "target_creator",
                                   "target_create", "target_update",
                                   "project_admin_group", "project_op_group",
                                   "project_target"})
    private static class JsonProject implements Checkable {

        @JsonProperty("project_name")
        private String name;
        @JsonProperty("project_graphs")
        private Set<String> graphs;
        @JsonProperty("project_description")
        private String description;

        public HugeProject build() {
            HugeProject project = new HugeProject(this.name, this.description);
            return project;
        }

        private HugeProject buildAddGraph(HugeProject project) {
            E.checkArgument(this.name == null ||
                            this.name.equals(project.name()),
                            "The name of project can't be updated");
            E.checkArgument(!CollectionUtils.isEmpty(this.graphs),
                            "The graphs of project can't be empty " +
                            "when adding graphs");
            E.checkArgument(StringUtils.isEmpty(this.description),
                            "The description of project can't be updated " +
                            "when adding graphs");
            Set<String> sourceGraphs = new HashSet<>(project.graphs());
            E.checkArgument(!sourceGraphs.containsAll(this.graphs),
                            "There are graphs '%s' of project '%s' that " +
                            "have been added in the graph collection",
                            this.graphs, project.id());
            sourceGraphs.addAll(this.graphs);
            project.graphs(sourceGraphs);
            return project;
        }

        private HugeProject buildRemoveGraph(HugeProject project) {
            E.checkArgument(this.name == null ||
                            this.name.equals(project.name()),
                            "The name of project can't be updated");
            E.checkArgument(!CollectionUtils.isEmpty(this.graphs),
                            "The graphs of project can't be empty " +
                            "when removing graphs");
            E.checkArgument(StringUtils.isEmpty(this.description),
                            "The description of project can't be updated " +
                            "when removing graphs");
            Set<String> sourceGraphs = new HashSet<>(project.graphs());
            sourceGraphs.removeAll(this.graphs);
            project.graphs(sourceGraphs);
            return project;
        }

        private HugeProject buildUpdateDescription(HugeProject project) {
            E.checkArgument(this.name == null ||
                            this.name.equals(project.name()),
                            "The name of project can't be updated");
            E.checkArgumentNotNull(this.description,
                                   "The description of project " +
                                   "can't be null");
            E.checkArgument(CollectionUtils.isEmpty(this.graphs),
                            "The graphs of project can't be updated");
            project.description(this.description);
            return project;
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.name,
                                   "The name of project can't be null");
            E.checkArgument(CollectionUtils.isEmpty(this.graphs),
                            "The graphs '%s' of project can't be added when" +
                            "creating the project '%s'",
                            this.graphs, this.name);
        }

        @Override
        public void checkUpdate() {
            E.checkArgument(!CollectionUtils.isEmpty(this.graphs) ||
                            this.description != null,
                            "Must specify 'graphs' or 'description' " +
                            "field that need to be updated");
        }
    }
}
