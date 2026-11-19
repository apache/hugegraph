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

package org.apache.hugegraph.pd.rest.exceptions;

import org.junit.Test;
import org.junit.Assert;

import org.springframework.http.ResponseEntity;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.rest.exceptionshandler.GenericExceptionMapper;
import org.apache.hugegraph.pd.rest.exceptionshandler.PDExceptionMapper;
import org.apache.hugegraph.rest.response.ApiResponse;

public class PDExceptionMapperTest {

    @Test
    public void testPDExceptionMapping() {
        PDExceptionMapper mapper = new PDExceptionMapper();

        PDException exception = new PDException(4001, "test error");

        ResponseEntity<ApiResponse<Object>> response =
                mapper.toResponse(exception);

        Assert.assertEquals(400, response.getStatusCodeValue());
        Assert.assertEquals(4001, response.getBody().getCode());
        Assert.assertEquals("test error", response.getBody().getMessage());
        Assert.assertEquals("Bad Request", response.getBody().getStatus());
    }

    @Test
    public void testGenericExceptionFallback() {
        PDExceptionMapper mapper = new PDExceptionMapper();

        PDException exception = new PDException(9999, "Erro desconhecido");

        ResponseEntity<ApiResponse<Object>> response =
                mapper.toResponse(exception);

        Assert.assertEquals(500, response.getStatusCodeValue());
        Assert.assertEquals(9999, response.getBody().getCode());
        Assert.assertEquals("Internal Server Error", response.getBody().getStatus());
    }

    @Test
    public void testGenericExceptionMapping() {
        GenericExceptionMapper mapper = new GenericExceptionMapper();

        Exception exception = new RuntimeException("Erro genérico");

        ResponseEntity<ApiResponse<Object>> response =
                mapper.handleGenericException(exception);

        Assert.assertEquals(500, response.getStatusCodeValue());
        Assert.assertEquals(500, response.getBody().getCode());
        Assert.assertEquals("An unexpected error occurred", response.getBody().getMessage());
        Assert.assertEquals("Internal Server Error", response.getBody().getStatus());
    }
}
