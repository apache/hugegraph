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

package org.apache.hugegraph.task;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.util.E;

public final class TaskResultStreamException extends HugeException {

    private static final long serialVersionUID = -1460410885612151698L;

    private final Reason reason;

    public TaskResultStreamException(Reason reason, String message,
                                     Object... args) {
        super(message, args);
        E.checkNotNull(reason, "reason");
        this.reason = reason;
    }

    public TaskResultStreamException(Reason reason, Throwable cause,
                                     String message, Object... args) {
        super(message, cause, args);
        E.checkNotNull(reason, "reason");
        this.reason = reason;
    }

    public Reason reason() {
        return this.reason;
    }

    public enum Reason {
        INVALID_JSON,
        NOT_PAGEABLE,
        INVALID_OFFSET,
        ROOT_MISMATCH,
        SCAN_LIMIT_EXCEEDED,
        TIMEOUT
    }
}
