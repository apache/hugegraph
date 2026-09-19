/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.api.profile;

import java.util.Map;

import org.apache.hugegraph.api.API;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.util.JsonUtil;

import com.codahale.metrics.annotation.Timed;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.PermitAll;
import jakarta.inject.Singleton;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;

/**
 * Storage-aware readiness for Kubernetes and load balancers: 200 while at
 * least one known Store answers this server, 503 while none does (or, before
 * any Store list is known, while PD does not answer). Unauthenticated, like
 * /versions, so that an httpGet probe needs no credential; the body carries
 * no addresses and no raw exception text.
 */
@Path("readiness")
@Singleton
@Tag(name = "ReadinessAPI")
public class ReadinessAPI extends API {

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @PermitAll
    public Response get(@Context GraphManager manager, @Context HugeConfig conf) {
        Map<String, Object> body = StorageReadiness.check(
                manager, conf.get(ServerOptions.READINESS_TIMEOUT),
                conf.get(ServerOptions.READINESS_CACHE_TTL));
        Response.Status status = StorageReadiness.isReady(body) ?
                                 Response.Status.OK :
                                 Response.Status.SERVICE_UNAVAILABLE;
        return Response.status(status)
                       .type(APPLICATION_JSON_WITH_CHARSET)
                       .entity(JsonUtil.toJson(body))
                       .build();
    }
}
