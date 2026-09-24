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

package org.apache.hugegraph.api.variables;

import java.util.Map;
import java.util.Optional;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/graphs/{graph}/variables")
@Singleton
@Tag(name = "VariablesAPI")
public class VariablesAPI extends API {

    private static final Logger LOG = Log.logger(VariablesAPI.class);

    @PUT
    @Timed
    @Path("{key}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> update(@Context GraphManager manager,
                                      @Parameter(description = "The name of the graph space")
                                      @PathParam("graphspace") String graphSpace,
                                      @Parameter(description = "The name of the graph")
                                      @PathParam("graph") String graph,
                                      @Parameter(description = "The key of the variable")
                                      @PathParam("key") String key,
                                      JsonVariableValue value) {
        E.checkArgument(value != null && value.data != null,
                        "The variable value can't be empty");
        LOG.debug("Graph [{}] set variable for {}: {}", graph, key, value);

        HugeGraph g = graph(manager, graphSpace, graph);
        commit(g, () -> g.variables().set(key, value.data));
        return ImmutableMap.of(key, value.data);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> list(@Context GraphManager manager,
                                    @Parameter(description = "The name of the graph space")
                                    @PathParam("graphspace") String graphSpace,
                                    @Parameter(description = "The name of the graph")
                                    @PathParam("graph") String graph) {
        LOG.debug("Graph [{}] get variables", graph);

        HugeGraph g = graph(manager, graphSpace, graph);
        return g.variables().asMap();
    }

    @GET
    @Timed
    @Path("{key}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> get(@Context GraphManager manager,
                                   @Parameter(description = "The name of the graph space")
                                   @PathParam("graphspace") String graphSpace,
                                   @Parameter(description = "The name of the graph")
                                   @PathParam("graph") String graph,
                                   @Parameter(description = "The key of the variable")
                                   @PathParam("key") String key) {
        LOG.debug("Graph [{}] get variable by key '{}'", graph, key);

        HugeGraph g = graph(manager, graphSpace, graph);
        Optional<?> object = g.variables().get(key);
        if (!object.isPresent()) {
            throw new NotFoundException(String.format(
                    "Variable '%s' does not exist", key));
        }
        return ImmutableMap.of(key, object.get());
    }

    @DELETE
    @Timed
    @Path("{key}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @Parameter(description = "The name of the graph space")
                       @PathParam("graphspace") String graphSpace,
                       @Parameter(description = "The name of the graph")
                       @PathParam("graph") String graph,
                       @Parameter(description = "The key of the variable")
                       @PathParam("key") String key) {
        LOG.debug("Graph [{}] remove variable by key '{}'", graph, key);

        HugeGraph g = graph(manager, graphSpace, graph);
        commit(g, () -> g.variables().remove(key));
    }

    private static class JsonVariableValue {

        @Schema(description = "The value of the variable", required = true)
        public Object data;

        @Override
        public String toString() {
            return String.format("JsonVariableValue{data=%s}", this.data);
        }
    }
}
