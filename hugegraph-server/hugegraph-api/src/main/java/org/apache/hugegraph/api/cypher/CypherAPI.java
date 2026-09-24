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

package org.apache.hugegraph.api.cypher;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.CompressInterceptor;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;

@Path("graphspaces/{graphspace}/graphs/{graph}/cypher")
@Singleton
@Tag(name = "CypherAPI")
public class CypherAPI extends API {

    private static final Logger LOG = Log.logger(CypherAPI.class);
    private static final Charset UTF8 = StandardCharsets.UTF_8;
    private static final String CLIENT_CONF = "conf/remote-objects.yaml";
    private final Base64.Decoder decoder = Base64.getUrlDecoder();
    private final String basic = "Basic ";
    private final String bearer = "Bearer ";

    private CypherManager cypherManager;

    private CypherManager cypherManager() {
        if (this.cypherManager == null) {
            this.cypherManager = CypherManager.configOf(CLIENT_CONF);
        }
        return this.cypherManager;
    }

    @GET
    @Timed
    @CompressInterceptor.Compress(buffer = (1024 * 40))
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public CypherModel query(@Context HttpHeaders headers,
                             @Parameter(description = "The graph space name")
                             @PathParam("graphspace") String graphspace,
                             @Parameter(description = "The graph name")
                             @PathParam("graph") String graph,
                             @Parameter(description = "The cypher query string")
                             @QueryParam("cypher") String cypher) {

        return this.queryByCypher(headers, graphspace, graph, cypher);
    }


    @POST
    @Timed
    @CompressInterceptor.Compress
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public CypherModel post(@Context HttpHeaders headers,
                            @Parameter(description = "The graph space name")
                            @PathParam("graphspace") String graphspace,
                            @Parameter(description = "The graph name")
                            @PathParam("graph") String graph,
                            @Parameter(description = "The cypher query string")
                            String cypher) {

        return this.queryByCypher(headers, graphspace, graph, cypher);
    }

    private CypherModel queryByCypher(HttpHeaders headers, String graphspace,
                                      String graph, String cypher) {
        E.checkArgument(graphspace != null && !graphspace.isEmpty(),
                        "The graphspace parameter can't be null or empty");
        E.checkArgument(graph != null && !graph.isEmpty(),
                        "The graph parameter can't be null or empty");
        E.checkArgument(cypher != null && !cypher.isEmpty(),
                        "The cypher parameter can't be null or empty");

        String graphInfo = graphspace + "-" + graph;
        Map<String, String> aliases = new HashMap<>(2, 1);
        aliases.put("graph", graphInfo);
        aliases.put("g", "__g_" + graphInfo);

        return this.client(headers).submitQuery(cypher, aliases);
    }

    private CypherClient client(HttpHeaders headers) {
        String auth = headers.getHeaderString(HttpHeaders.AUTHORIZATION);

        if (auth != null && !auth.isEmpty()) {
            auth = auth.split(",")[0];
        }

        if (auth != null) {
            if (auth.startsWith(basic)) {
                return this.clientViaBasic(auth);
            } else if (auth.startsWith(bearer)) {
                return this.clientViaToken(auth);
            }
        }

        throw new NotAuthorizedException("The Cypher-API called without any authorization.");
    }

    private CypherClient clientViaBasic(String auth) {
        Pair<String, String> userPass = this.toUserPass(auth);
        E.checkNotNull(userPass, "user-password-pair");

        return this.cypherManager().getClient(userPass.getLeft(), userPass.getRight());
    }

    private CypherClient clientViaToken(String auth) {
        return this.cypherManager().getClient(auth.substring(bearer.length()));
    }

    private Pair<String, String> toUserPass(String auth) {
        if (auth == null || auth.isEmpty()) {
            return null;
        }
        if (!auth.startsWith(basic)) {
            return null;
        }

        String[] split;
        try {
            String encoded = auth.substring(basic.length());
            byte[] userPass = this.decoder.decode(encoded);
            String authorization = new String(userPass, UTF8);
            split = authorization.split(":");
        } catch (Exception e) {
            LOG.error("Failed convert auth to credential.", e);
            return null;
        }

        if (split.length != 2) {
            return null;
        }
        return ImmutablePair.of(split[0], split[1]);
    }
}
