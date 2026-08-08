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

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

import org.apache.hugegraph.api.filter.CompressInterceptor.Compress;
import org.apache.hugegraph.api.filter.ExceptionFilter;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.testutil.Assert;
import org.glassfish.grizzly.http.server.Request;
import org.junit.Test;

import jakarta.ws.rs.QueryParam;

public class TaskResultExceptionsTest {

    @Test
    public void testGetResultUsesLimitQueryParameter() throws Exception {
        Method method = getResultMethod();
        QueryParam parameter = null;
        for (Parameter item : method.getParameters()) {
            if (item.getType() == Integer.class) {
                parameter = item.getAnnotation(QueryParam.class);
                break;
            }
        }

        Assert.assertNotNull(parameter);
        Assert.assertEquals("limit", parameter.value());
    }

    @Test
    public void testGetResultUsesCompressInterceptor() throws Exception {
        Assert.assertNotNull(getResultMethod().getAnnotation(Compress.class));
    }

    @Test
    public void testTaskResultErrorsUseStandardEnvelope() {
        assertException(new TaskResultNotReadyException("not ready"), 409);
        assertException(new TaskResultNotReadableException("not readable"),
                        409);
        assertException(new TaskResultUnavailableException("unavailable"),
                        409);
        assertException(new InvalidTaskResultPageTokenException(
                                "invalid token",
                                new IllegalArgumentException("invalid")),
                        400);
        assertException(new TaskResultChangedException("changed"), 409);
        assertException(new TaskResultNotPageableException(
                                "not pageable",
                                new IllegalArgumentException("invalid")),
                        400);
    }

    private static void assertException(TaskResultException exception,
                                        int status) {
        Assert.assertEquals(status, exception.getResponse().getStatus());
        String envelope = ExceptionFilter.formatException(exception, false);
        Assert.assertContains(exception.getClass().getSimpleName(), envelope);
        Assert.assertFalse(envelope.contains("\"reason\":"));
    }

    private static Method getResultMethod() throws Exception {
        return TaskAPI.class.getMethod("getResult", GraphManager.class,
                                       HugeConfig.class, Request.class,
                                       String.class, String.class, long.class,
                                       Integer.class, String.class);
    }
}
