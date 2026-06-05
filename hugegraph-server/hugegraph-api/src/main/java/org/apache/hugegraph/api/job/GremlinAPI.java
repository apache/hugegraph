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

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.RedirectFilter;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.Checkable;
import org.apache.hugegraph.job.GremlinJob;
import org.apache.hugegraph.job.JobBuilder;
import org.apache.hugegraph.metrics.MetricsUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/graphs/{graph}/jobs/gremlin")
@Singleton
@Tag(name = "GremlinAPI")
public class GremlinAPI extends API {

    private static final Logger LOG = Log.logger(GremlinAPI.class);

    private static final int MAX_NAME_LENGTH = 256;

    private static final Histogram GREMLIN_JOB_INPUT_HISTOGRAM =
            MetricsUtil.registerHistogram(GremlinAPI.class, "gremlin-input");

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"space_member", "$graphspace=$graphspace $owner=$graph " +
                            "$action=gremlin_execute"})
    @RedirectFilter.RedirectMasterRole
    public Map<String, Id> post(@Context GraphManager manager,
                                @Parameter(description = "The graphspace name")
                                @PathParam("graphspace") String graphSpace,
                                @Parameter(description = "The graph name")
                                @PathParam("graph") String graph,
                                @Parameter(description = "The Gremlin job request")
                                GremlinRequest request) {
        LOG.debug("Graph [{}] schedule gremlin job: {}", graph, request);
        checkCreatingBody(request);
        GREMLIN_JOB_INPUT_HISTOGRAM.update(request.gremlin.length());

        HugeGraph g = graph(manager, graphSpace, graph);
        request.aliase(graph, "graph");
        JobBuilder<Object> builder = JobBuilder.of(g);
        builder.name(request.name())
               .input(request.toJson())
               //todo: auth
               //.context(HugeGraphAuthProxy.getContextString())
               .job(new GremlinJob());
        return ImmutableMap.of("task_id", builder.schedule().id());
    }

    public static class GremlinRequest implements Checkable {

        // See org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer
        @JsonProperty
        @Schema(description = "The Gremlin script to execute", required = true)
        private String gremlin;
        @JsonProperty
        @Schema(description = "The bindings for the Gremlin script")
        private Map<String, Object> bindings = new HashMap<>();
        @JsonProperty
        @Schema(description = "The language of the Gremlin script", example = "gremlin-groovy")
        private String language = "gremlin-groovy";
        @JsonProperty
        @Schema(description = "The aliases for graph references")
        private Map<String, String> aliases = new HashMap<>();

        public String gremlin() {
            return this.gremlin;
        }

        public void gremlin(String gremlin) {
            this.gremlin = gremlin;
        }

        public Map<String, Object> bindings() {
            return this.bindings;
        }

        public void bindings(Map<String, Object> bindings) {
            this.bindings = bindings;
        }

        public void binding(String name, Object value) {
            this.bindings.put(name, value);
        }

        public String language() {
            return this.language;
        }

        public void language(String language) {
            this.language = language;
        }

        public Map<String, String> aliases() {
            return this.aliases;
        }

        public void aliases(Map<String, String> aliases) {
            this.aliases = aliases;
        }

        public void aliase(String key, String value) {
            this.aliases.put(key, value);
        }

        public String name() {
            // Get the first line of script as the name
            String firstLine = this.gremlin.split("\r\n|\r|\n", 2)[0];
            final Charset charset = Charset.forName(CHARSET);
            final byte[] bytes = firstLine.getBytes(charset);
            if (bytes.length <= MAX_NAME_LENGTH) {
                return firstLine;
            }

            CharsetDecoder decoder = charset.newDecoder();
            decoder.onMalformedInput(CodingErrorAction.IGNORE);
            decoder.reset();

            ByteBuffer buffer = ByteBuffer.wrap(bytes, 0, MAX_NAME_LENGTH);
            try {
                return decoder.decode(buffer).toString();
            } catch (CharacterCodingException e) {
                throw new HugeException("Failed to decode truncated bytes of " +
                                        "gremlin first line", e);
            }
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.gremlin,
                                   "The gremlin parameter can't be null");
            E.checkArgumentNotNull(this.language,
                                   "The language parameter can't be null");
            E.checkArgument(this.aliases == null || this.aliases.isEmpty(),
                            "There is no need to pass gremlin aliases");
        }

        public String toJson() {
            Map<String, Object> map = new HashMap<>();
            map.put("gremlin", this.gremlin);
            map.put("bindings", this.bindings);
            map.put("language", this.language);
            map.put("aliases", this.aliases);
            return JsonUtil.toJson(map);
        }

        public static GremlinRequest fromJson(String json) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = JsonUtil.fromJson(json, Map.class);
            String gremlin = (String) map.get("gremlin");
            @SuppressWarnings("unchecked")
            Map<String, Object> bindings = (Map<String, Object>)
                    map.get("bindings");
            String language = (String) map.get("language");
            @SuppressWarnings("unchecked")
            Map<String, String> aliases = (Map<String, String>)
                    map.get("aliases");

            GremlinRequest request = new GremlinRequest();
            request.gremlin(gremlin);
            request.bindings(bindings);
            request.language(language);
            request.aliases(aliases);
            return request;
        }
    }
}
