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

package org.apache.hugegraph.core;

import java.util.Objects;

/**
 * Graph identity within the Server registry. Combined names remain in use at
 * metadata, RPC and Gremlin boundaries.
 */
public record GraphKey(String graphSpace, String graphName) {

    public GraphKey {
        Objects.requireNonNull(graphSpace, "graphSpace");
        Objects.requireNonNull(graphName, "graphName");
    }

    public static GraphKey fromString(String name) {
        // Graph names cannot contain '-', so the last separator belongs to the
        // graph name even when a graph-space name contains separators.
        int separator = name.lastIndexOf(GraphManager.DELIMITER);
        if (separator < 0) {
            // An unqualified name must not resolve to a graph in DEFAULT.
            return new GraphKey("", name);
        }
        return new GraphKey(name.substring(0, separator), name.substring(separator + 1));
    }

    public String spaceGraphName() {
        return this.graphSpace + GraphManager.DELIMITER + this.graphName;
    }
}
