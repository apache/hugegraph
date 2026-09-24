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

package org.apache.hugegraph.api.filter;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.exception.HugeGremlinException;
import org.apache.hugegraph.exception.NotFoundException;
import org.glassfish.hk2.api.MultiException;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

public class ExceptionFilter {

    private static final int BAD_REQUEST_ERROR =
            Response.Status.BAD_REQUEST.getStatusCode();
    private static final int NOT_FOUND_ERROR =
            Response.Status.NOT_FOUND.getStatusCode();
    private static final int INTERNAL_SERVER_ERROR =
            Response.Status.INTERNAL_SERVER_ERROR.getStatusCode();

    public static class TracedExceptionMapper extends API {

        private static boolean forcedTrace = false;

        @Context
        private jakarta.inject.Provider<HugeConfig> configProvider;

        protected boolean trace() {
            if (forcedTrace) {
                return true;
            }
            HugeConfig config = this.configProvider.get();
            if (config == null) {
                return false;
            }
            return config.get(ServerOptions.ALLOW_TRACE);
        }
    }

    @Path("exception/trace")
    @Singleton
    @Tag(name = "TracedExceptionAPI")
    public static class TracedExceptionAPI extends API {

        @GET
        @Timed
        @Produces(APPLICATION_JSON_WITH_CHARSET)
        @RolesAllowed({"space_member"})
        public Object get() {
            return ImmutableMap.of("trace", TracedExceptionMapper.forcedTrace);
        }

        @PUT
        @Timed
        @Consumes(APPLICATION_JSON)
        @Produces(APPLICATION_JSON_WITH_CHARSET)
        @RolesAllowed({"space_member"})
        public Object trace(boolean trace) {
            TracedExceptionMapper.forcedTrace = trace;
            return ImmutableMap.of("trace", TracedExceptionMapper.forcedTrace);
        }
    }

    @Provider
    public static class HugeExceptionMapper
            extends TracedExceptionMapper
            implements ExceptionMapper<HugeException> {

        @Override
        public Response toResponse(HugeException exception) {
            return Response.status(BAD_REQUEST_ERROR)
                           .type(MediaType.APPLICATION_JSON)
                           .entity(formatException(exception, this.trace()))
                           .build();
        }
    }

    @Provider
    public static class IllegalArgumentExceptionMapper
            extends TracedExceptionMapper
            implements ExceptionMapper<IllegalArgumentException> {

        @Override
        public Response toResponse(IllegalArgumentException exception) {
            return Response.status(BAD_REQUEST_ERROR)
                           .type(MediaType.APPLICATION_JSON)
                           .entity(formatException(exception, this.trace()))
                           .build();
        }
    }

    @Provider
    public static class NotFoundExceptionMapper
            extends TracedExceptionMapper
            implements ExceptionMapper<NotFoundException> {

        @Override
        public Response toResponse(NotFoundException exception) {
            return Response.status(NOT_FOUND_ERROR)
                           .type(MediaType.APPLICATION_JSON)
                           .entity(formatException(exception, this.trace()))
                           .build();
        }
    }

    @Provider
    public static class NoSuchElementExceptionMapper
            extends TracedExceptionMapper
            implements ExceptionMapper<NoSuchElementException> {

        @Override
        public Response toResponse(NoSuchElementException exception) {
            return Response.status(NOT_FOUND_ERROR)
                           .type(MediaType.APPLICATION_JSON)
                           .entity(formatException(exception, this.trace()))
                           .build();
        }
    }

    @Provider
    public static class WebApplicationExceptionMapper
            extends TracedExceptionMapper
            implements ExceptionMapper<WebApplicationException> {

        @Override
        public Response toResponse(WebApplicationException exception) {
            Response response = exception.getResponse();
            if (response.hasEntity()) {
                return response;
            }
            MultivaluedMap<String, Object> headers = response.getHeaders();
            boolean trace = this.trace(response.getStatus());
            response = Response.status(response.getStatus())
                               .type(MediaType.APPLICATION_JSON)
                               .entity(formatException(exception, trace))
                               .build();
            response.getHeaders().putAll(headers);
            return response;
        }

        private boolean trace(int status) {
            return this.trace() && status == INTERNAL_SERVER_ERROR;
        }
    }

    @Provider
    public static class HugeGremlinExceptionMapper
            extends TracedExceptionMapper
            implements ExceptionMapper<HugeGremlinException> {

        @Override
        public Response toResponse(HugeGremlinException exception) {
            return Response.status(exception.statusCode())
                           .type(MediaType.APPLICATION_JSON)
                           .entity(formatGremlinException(exception,
                                                          this.trace()))
                           .build();
        }
    }

    @Provider
    public static class AssertionErrorMapper extends TracedExceptionMapper
            implements ExceptionMapper<AssertionError> {

        @Override
        public Response toResponse(AssertionError exception) {
            return Response.status(INTERNAL_SERVER_ERROR)
                           .type(MediaType.APPLICATION_JSON)
                           .entity(formatException(exception, true))
                           .build();
        }
    }

    @Provider
    public static class UnknownExceptionMapper extends TracedExceptionMapper
            implements ExceptionMapper<Throwable> {

        @Override
        public Response toResponse(Throwable exception) {
            if (exception instanceof MultiException &&
                ((MultiException) exception).getErrors().size() == 1) {
                exception = ((MultiException) exception).getErrors().get(0);
            }
            return Response.status(INTERNAL_SERVER_ERROR)
                           .type(MediaType.APPLICATION_JSON)
                           .entity(formatException(exception, this.trace()))
                           .build();
        }
    }

    public static String formatException(Throwable exception, boolean trace) {
        String clazz = exception.getClass().toString();
        String message = exception.getMessage() != null ?
                         exception.getMessage() : "";
        String cause = exception.getCause() != null ?
                       exception.getCause().toString() : "";

        JsonObjectBuilder json = Json.createObjectBuilder()
                                     .add("exception", clazz)
                                     .add("message", message)
                                     .add("cause", cause);

        if (trace) {
            JsonArrayBuilder traces = Json.createArrayBuilder();
            for (StackTraceElement i : exception.getStackTrace()) {
                traces.add(i.toString());
            }
            json.add("trace", traces);
        }

        return json.build().toString();
    }

    public static String formatGremlinException(HugeGremlinException exception,
                                                boolean trace) {
        Map<String, Object> map = exception.response();
        String message = (String) map.get("message");
        String exClassName = (String) map.get("Exception-Class");
        @SuppressWarnings("unchecked")
        List<String> exceptions = (List<String>) map.get("exceptions");
        String stackTrace = (String) map.get("stackTrace");

        message = message != null ? message : "";
        exClassName = exClassName != null ? exClassName : "";
        String cause = exceptions != null ? exceptions.toString() : "";

        JsonObjectBuilder json = Json.createObjectBuilder()
                                     .add("exception", exClassName)
                                     .add("message", message)
                                     .add("cause", cause);

        if (trace && stackTrace != null) {
            JsonArrayBuilder traces = Json.createArrayBuilder();
            for (String part : StringUtils.split(stackTrace, '\n')) {
                traces.add(part);
            }
            json.add("trace", traces);
        }

        return json.build().toString();
    }
}
