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

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;

abstract class TaskResultException extends WebApplicationException {

    private static final long serialVersionUID = -2051021373981832080L;

    private final String metricReason;

    protected TaskResultException(String metricReason, String message,
                                  Response.Status status) {
        super(message, status);
        this.metricReason = metricReason;
    }

    protected TaskResultException(String metricReason, String message,
                                  Throwable cause,
                                  Response.Status status) {
        super(message, cause, status);
        this.metricReason = metricReason;
    }

    String metricReason() {
        return this.metricReason;
    }
}
