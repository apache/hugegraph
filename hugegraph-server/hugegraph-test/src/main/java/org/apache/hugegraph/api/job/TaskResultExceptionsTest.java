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

import org.apache.hugegraph.api.filter.ExceptionFilter;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class TaskResultExceptionsTest {

    @Test
    public void testStableTaskResultErrorContract() {
        assertException(new TaskResultNotReadyException("not ready"),
                        409, "not_ready");
        assertException(new TaskResultNotReadableException("not readable"),
                        409, "not_readable");
        assertException(new TaskResultUnavailableException("unavailable"),
                        409, "unavailable");
        assertException(new InvalidTaskResultPageTokenException(
                                "invalid token",
                                new IllegalArgumentException("invalid")),
                        400, "invalid_page_token");
        assertException(new TaskResultChangedException("changed"),
                        409, "changed");
        assertException(new TaskResultNotPageableException(
                                "not pageable",
                                new IllegalArgumentException("invalid")),
                        400, "not_pageable");
    }

    private static void assertException(TaskResultException exception,
                                        int status, String reason) {
        Assert.assertEquals(status, exception.getResponse().getStatus());
        Assert.assertEquals(reason, exception.reason());
        String envelope = ExceptionFilter.formatException(exception, false);
        Assert.assertContains(exception.getClass().getSimpleName(), envelope);
    }
}
