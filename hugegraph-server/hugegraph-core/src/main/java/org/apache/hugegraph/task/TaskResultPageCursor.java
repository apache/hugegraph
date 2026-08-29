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

import org.apache.hugegraph.util.E;

public final class TaskResultPageCursor {

    private final RootType rootType;
    private final long nextOffset;
    private final int pageSize;
    private final String fingerprint;

    public TaskResultPageCursor(RootType rootType, long nextOffset,
                                int pageSize, String fingerprint) {
        E.checkNotNull(rootType, "root type");
        E.checkArgument(nextOffset >= 0L,
                        "The next offset must be non-negative");
        E.checkArgument(pageSize > 0, "The page size must be positive");
        E.checkNotNull(fingerprint, "result fingerprint");
        this.rootType = rootType;
        this.nextOffset = nextOffset;
        this.pageSize = pageSize;
        this.fingerprint = fingerprint;
    }

    public RootType rootType() {
        return this.rootType;
    }

    public long nextOffset() {
        return this.nextOffset;
    }

    public int pageSize() {
        return this.pageSize;
    }

    public String fingerprint() {
        return this.fingerprint;
    }

    public enum RootType {

        ARRAY("array"),
        OBJECT("object");

        private final String text;

        RootType(String text) {
            this.text = text;
        }

        public String text() {
            return this.text;
        }
    }
}
