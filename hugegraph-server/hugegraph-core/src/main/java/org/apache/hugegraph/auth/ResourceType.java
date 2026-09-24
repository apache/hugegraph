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

package org.apache.hugegraph.auth;

import java.util.Objects;

import org.apache.hugegraph.type.HugeType;

public enum ResourceType {

    NONE,

    STATUS,

    VERTEX,

    EDGE,

    VERTEX_AGGR,

    EDGE_AGGR,

    VAR,

    GREMLIN,

    TASK,

    PROPERTY_KEY,

    VERTEX_LABEL,

    EDGE_LABEL,

    INDEX_LABEL, // include create/rebuild/delete index

    SCHEMA,

    META,

    ALL,

    GRANT,

    USER_GROUP,

    PROJECT,

    TARGET,

    METRICS,

    ROOT;

    public boolean match(ResourceType required) {
        if (this == required) {
            return true;
        }

        if (Objects.requireNonNull(required) == ResourceType.NONE) {
            return this != NONE;
        }

        switch (this) {
            case ROOT:
            case ALL:
                return this.ordinal() >= required.ordinal();
            case SCHEMA:
                return required.isSchema();
            default:
                break;
        }

        return false;
    }

    public boolean isGraph() {
        int ord = this.ordinal();
        return VERTEX.ordinal() <= ord && ord <= EDGE.ordinal();
    }

    public boolean isSchema() {
        int ord = this.ordinal();
        return PROPERTY_KEY.ordinal() <= ord && ord <= SCHEMA.ordinal();
    }

    public boolean isGraphOrSchema() {
        return this.isSchema() || this.isGraph();
    }

    public boolean isAuth() {
        int ord = this.ordinal();
        return GRANT.ordinal() <= ord && ord <= TARGET.ordinal();
    }

    public boolean isGrantOrUser() {
        return this == GRANT || this == USER_GROUP;
    }

    public boolean isRepresentative() {
        return this == ROOT || this == ALL || this == SCHEMA;
    }

    public static ResourceType from(HugeType type) {
        switch (type) {
            case VERTEX:
                return VERTEX;
            case EDGE:
            case EDGE_OUT:
            case EDGE_IN:
                return EDGE;
            case PROPERTY_KEY:
                return PROPERTY_KEY;
            case VERTEX_LABEL:
                return VERTEX_LABEL;
            case EDGE_LABEL:
                return EDGE_LABEL;
            case INDEX_LABEL:
                return INDEX_LABEL;
            default:
                // pass
                break;
        }
        return NONE;
    }
}
