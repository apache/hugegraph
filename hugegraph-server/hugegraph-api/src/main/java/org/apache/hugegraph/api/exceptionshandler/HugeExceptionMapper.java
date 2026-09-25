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

import com.alipay.sofa.rpc.log.Logger;
import com.alipay.sofa.rpc.log.LoggerFactory;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.rest.response.ApiResponse;

@Provider
public class HugeExceptionMapper implements ExceptionMapper<HugeException> {

    private static final Logger LOG =
            LoggerFactory.getLogger(HugeExceptionMapper.class);

    @Override
    public Response toResponse(HugeException exception) {

        Throwable rootCause = HugeException.rootCause(exception);
        int code;

        if (rootCause instanceof InterruptedException) {
            code = 500;
        } else if (rootCause instanceof IllegalArgumentException) {
            code = 400;
        } else if (rootCause instanceof java.util.NoSuchElementException) {
            code = 404;
        } else {
            code = 400;
        }

        if (code >= 500) {
            LOG.error("Unexpected server error", exception);
        } else {
            LOG.warn("Request error: {}", exception.getMessage());
        }

        Response.Status statusCode = Response.Status.fromStatusCode(code);
        String statusText = (statusCode != null ) ? statusCode.getReasonPhrase() : "BAD REQUEST";

        ApiResponse<Object> apiResponse = new ApiResponse<>(
                code,
                exception.getMessage(),
                null,
                statusText
        );

        return Response.status(code)
                       .type(MediaType.APPLICATION_JSON)
                       .entity(apiResponse)
                       .build();
    }
}

