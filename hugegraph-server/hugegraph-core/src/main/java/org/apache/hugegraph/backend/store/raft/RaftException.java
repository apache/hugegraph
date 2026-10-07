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

package org.apache.hugegraph.backend.store.raft;

import org.apache.hugegraph.exception.HugeException;

public class RaftException extends HugeException {

    private static final long serialVersionUID = 594903805213423817L;

    public RaftException(String message) {
        super(message);
    }

    public RaftException(String message, Throwable cause) {
        super(message, cause);
    }

    public RaftException(String message, Object... args) {
        super(message, args);
    }

    public RaftException(String message, Throwable cause, Object... args) {
        super(message, cause, args);
    }

    public RaftException(Throwable cause) {
        this("Exception in raft", cause);
    }

    public static final void check(boolean expression,
                                   String message, Object... args)
            throws RaftException {
        if (!expression) {
            throw new RaftException(message, args);
        }
    }
}
