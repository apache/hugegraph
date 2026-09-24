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

package org.apache.hugegraph.store.constant;

import java.util.Map;
import java.util.Objects;

public class HugeServerTables {

    public static final String UNKNOWN_TABLE = "unknown";
    public static final String VERTEX_TABLE = "g+v";
    public static final String OUT_EDGE_TABLE = "g+oe";
    public static final String IN_EDGE_TABLE = "g+ie";
    public static final String INDEX_TABLE = "g+index";
    public static final String TASK_TABLE = "g+task";
    public static final String OLAP_TABLE = "g+olap";
    //FIXME Is this necessary?
    public static final String SERVER_TABLE = "g+server";

    public static final String[] TABLES = new String[]{UNKNOWN_TABLE, VERTEX_TABLE,
            OUT_EDGE_TABLE, IN_EDGE_TABLE,
            INDEX_TABLE, TASK_TABLE, OLAP_TABLE, SERVER_TABLE};

    public static final Map<String, Integer> TABLES_MAP = Map.of(
            UNKNOWN_TABLE, 0,
            VERTEX_TABLE, 1,
            OUT_EDGE_TABLE, 2,
            IN_EDGE_TABLE, 3,
            INDEX_TABLE, 4,
            TASK_TABLE, 5,
            OLAP_TABLE, 6,
            SERVER_TABLE,7
    );

    public static boolean isEdgeTable(String table) {
        return Objects.equals(IN_EDGE_TABLE, table) || Objects.equals(OUT_EDGE_TABLE, table);
    }
}
