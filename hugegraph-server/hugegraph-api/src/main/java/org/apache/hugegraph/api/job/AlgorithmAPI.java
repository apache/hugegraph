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

package org.apache.hugegraph.api.job;

import java.util.Map;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.RedirectFilter;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.job.AlgorithmJob;
import org.apache.hugegraph.job.JobBuilder;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/graphs/{graph}/jobs/algorithm")
@Singleton
@Tag(name = "AlgorithmAPI")
public class AlgorithmAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Path("/{name}")
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RedirectFilter.RedirectMasterRole
    public Map<String, Id> post(@Context GraphManager manager,
                                @Parameter(description = "The graphspace name")
                                @PathParam("graphspace") String graphSpace,
                                @Parameter(description = "The graph name")
                                @PathParam("graph") String graph,
                                @Parameter(description = "The algorithm name")
                                @PathParam("name") String algorithm,
                                @Parameter(description = "The algorithm parameters")
                                Map<String, Object> parameters) {
        LOG.debug("Graph [{}] schedule algorithm job: {}", graph, parameters);
        E.checkArgument(algorithm != null && !algorithm.isEmpty(),
                        "The algorithm name can't be empty");
        if (parameters == null) {
            parameters = ImmutableMap.of();
        }
        if (!AlgorithmJob.check(algorithm, parameters)) {
            throw new NotFoundException("Not found algorithm: " + algorithm);
        }

        HugeGraph g = graph(manager, graphSpace, graph);
        Map<String, Object> input = ImmutableMap.of("algorithm", algorithm,
                                                    "parameters", parameters);
        JobBuilder<Object> builder = JobBuilder.of(g);
        builder.name("algorithm:" + algorithm)
               .input(JsonUtil.toJson(input))
               .job(new AlgorithmJob());
        return ImmutableMap.of("task_id", builder.schedule().id());
    }
}
