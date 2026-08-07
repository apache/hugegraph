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

    private final String reason;

    protected TaskResultException(String reason, String message,
                                  Response.Status status) {
        super(message, status);
        this.reason = reason;
    }

    protected TaskResultException(String reason, String message,
                                  Throwable cause,
                                  Response.Status status) {
        super(message, cause, status);
        this.reason = reason;
    }

    String reason() {
        return this.reason;
    }
}

final class TaskResultNotReadyException extends TaskResultException {

    private static final long serialVersionUID = -1287013904761638797L;

    TaskResultNotReadyException(String message) {
        super("not_ready", message, Response.Status.CONFLICT);
    }
}

final class TaskResultNotReadableException extends TaskResultException {

    private static final long serialVersionUID = 3457480673376361295L;

    TaskResultNotReadableException(String message) {
        super("not_readable", message, Response.Status.CONFLICT);
    }
}

final class TaskResultUnavailableException extends TaskResultException {

    private static final long serialVersionUID = 8265456498718990105L;

    TaskResultUnavailableException(String message) {
        super("unavailable", message, Response.Status.CONFLICT);
    }
}

final class InvalidTaskResultPageTokenException extends TaskResultException {

    private static final long serialVersionUID = 2172643179597336947L;

    InvalidTaskResultPageTokenException(String message, Throwable cause) {
        super("invalid_page_token", message, cause,
              Response.Status.BAD_REQUEST);
    }
}

final class TaskResultChangedException extends TaskResultException {

    private static final long serialVersionUID = 3665240132205019826L;

    TaskResultChangedException(String message) {
        super("changed", message, Response.Status.CONFLICT);
    }
}

final class TaskResultNotPageableException extends TaskResultException {

    private static final long serialVersionUID = 8491764614369801642L;

    TaskResultNotPageableException(String message, Throwable cause) {
        super("not_pageable", message, cause, Response.Status.BAD_REQUEST);
    }
}
