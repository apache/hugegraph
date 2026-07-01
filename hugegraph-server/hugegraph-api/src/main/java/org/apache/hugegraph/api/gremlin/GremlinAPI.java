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

package org.apache.hugegraph.api.gremlin;

import org.apache.hugegraph.api.filter.CompressInterceptor.Compress;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.metrics.MetricsUtil;
import org.apache.hugegraph.util.E;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.annotation.Timed;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;

@Path("gremlin")
@Singleton
@Tag(name = "GremlinAPI")
public class GremlinAPI extends GremlinQueryAPI {

    private static final Histogram GREMLIN_INPUT_HISTOGRAM =
            MetricsUtil.registerHistogram(GremlinAPI.class, "gremlin-input");
    private static final Histogram GREMLIN_OUTPUT_HISTOGRAM =
            MetricsUtil.registerHistogram(GremlinAPI.class, "gremlin-output");

    @POST
    @Timed
    @Compress
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Response post(@Context HugeConfig conf,
                         @Context HttpHeaders headers,
                         @Parameter(description = "The Gremlin query request body")
                         String request) {
        /* The following code is reserved for forwarding request */
        // context.getRequestDispatcher(location).forward(request, response);
        // return Response.seeOther(UriBuilder.fromUri(location).build())
        // .build();
        // Response.temporaryRedirect(UriBuilder.fromUri(location).build())
        // .build();
        String auth = headers.getHeaderString(HttpHeaders.AUTHORIZATION);
        Response response = this.client().doPostRequest(auth, request);
        GREMLIN_INPUT_HISTOGRAM.update(request.length());
        GREMLIN_OUTPUT_HISTOGRAM.update(response.getLength());
        return transformResponseIfNeeded(response);
    }

    @GET
    @Timed
    @Compress(buffer = (1024 * 40))
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Response get(@Context HugeConfig conf,
                        @Context HttpHeaders headers,
                        @Context UriInfo uriInfo) {
        String auth = headers.getHeaderString(HttpHeaders.AUTHORIZATION);
        String query = uriInfo.getRequestUri().getRawQuery();
        E.checkArgumentNotNull(query, "The request query can't be empty");
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        Response response = this.client().doGetRequest(auth, params);
        GREMLIN_INPUT_HISTOGRAM.update(query.length());
        GREMLIN_OUTPUT_HISTOGRAM.update(response.getLength());
        return transformResponseIfNeeded(response);
    }
}
