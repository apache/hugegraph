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

package org.apache.hugegraph.api.exceptionshandler;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.apache.hugegraph.rest.response.ApiResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Provider
public class GenericExceptionMapper implements ExceptionMapper<Exception> {

    private static final Logger LOG = LogManager.getLogger(GenericExceptionMapper.class);

    @Override
    public Response toResponse(Exception exception) {
        LOG.error("Unexpected error occurred", exception);

        int code = 500;
        ApiResponse<Object> apiResponse = new ApiResponse<>(
                code,
                "An unexpected error occurred",
                null,
                Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase());

        return Response.status(code)
                       .type(MediaType.APPLICATION_JSON)
                       .entity(apiResponse)
                       .build();
    }
}
