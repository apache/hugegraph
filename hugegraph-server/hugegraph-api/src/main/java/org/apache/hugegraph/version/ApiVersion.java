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

package org.apache.hugegraph.version;

import org.apache.hugegraph.util.VersionUtil;
import org.apache.hugegraph.util.VersionUtil.Version;

/**
 * This class represents the API version of the HugeGraph system.
 * It contains a version number and a method to check the compatibility
 * with the core version of the system.
 */
public final class ApiVersion {

    /*
     * API Version change log
     * <p>
     * version 0.2:
     * [0.2] HugeGraph-527: First add the version to the hugegraph module
     * [0.3] HugeGraph-525: Add versions check of components and api
     * [0.4] HugeGraph-162: Add schema builder to separate client and inner interface.
     * [0.5] HugeGraph-498: Support three kinds of id strategy
     * <p>
     * version 0.3:
     * <p>
     * [0.6] HugeGraph-614: Add update api of VL/EL to support append and eliminate action
     * [0.7] HugeGraph-245: Add nullable-props for vertex label and edge label
     * [0.8] HugeGraph-396: Continue to improve variables implementation
     * [0.9] HugeGraph-894: Add vertex/edge update api to add property and remove property
     * [0.10] HugeGraph-919: Add condition query for vertex/edge list API
     * <p>
     * version 0.4:
     * [0.11] HugeGraph-938: Remove useless index-names field in VL/EL API
     * [0.12] HugeGraph-589: Add schema id for all schema elements
     * [0.13] HugeGraph-956: Support customize string/number id strategy
     * <p>
     * version 0.5:
     * [0.14] HugeGraph-1085: Add enable_label_index to VL/EL
     * [0.15] HugeGraph-1105: Support paging for large numbers of records
     * [0.16] HugeGraph-944: Support rest shortest path, k-out, k-neighbor
     * [0.17] HugeGraph-944: Support rest shortest path, k-out, k-neighbor
     * [0.18] HugeGraph-81: Change argument "checkVertex" to "check_vertex"
     * <p>
     * version 0.6:
     * [0.19] HugeGraph-1195: Support eliminate userdata on schema
     * [0.20] HugeGraph-1210: Add paths api to find paths between two nodes
     * [0.21] HugeGraph-1197: Expose scan api for hugegraph-spark
     * [0.22] HugeGraph-1162: Support authentication and permission control
     * [0.23] HugeGraph-1176: Support degree and capacity for traverse api
     * [0.24] HugeGraph-1261: Add param offset for vertex/edge list API
     * [0.25] HugeGraph-1272: Support set/clear restore status of graph
     * [0.26] HugeGraph-1273: Add some monitoring counters to integrate with
     *        gremlin's monitoring framework
     * [0.27] HugeGraph-889: Use asynchronous mechanism to do schema deletion
     * <p>
     * version 0.8:
     * [0.28] Issue-153: Add task-cancel API
     * [0.29] Issue-39: Add rays and rings RESTful API
     * [0.30] Issue-32: Change index create API to return indexLabel and task id
     * [0.31] Issue-182: Support restore graph in restoring and merging mode
     * <p>
     * version 0.9:
     * [0.32] Issue-250: Keep depth and degree consistent for traverser api
     * [0.33] Issue-305: Implement customized paths and crosspoints RESTful API
     * [0.34] Issue-307: Let VertexAPI use simplified property serializer
     * [0.35] Issue-287: Support pagination when do index query
     * [0.36] Issue-360: Support paging for scan api
     * [0.37] Issue-391: Add skip_super_node for the shortest path
     * [0.38] Issue-274: Add personal-rank and neighbor-rank RESTful API
     * <p>
     * version 0.10:
     * [0.39] Issue-522: Add profile RESTful API
     * [0.40] Issue-523: Add source_in_ring args for rings RESTful API
     * [0.41] Issue-493: Support batch updating properties by multiple strategy
     * [0.42] Issue-176: Let gremlin error response consistent with RESTful's
     * [0.43] Issue-270 & 398: support shard-index and vertex + sort-key prefix,
     *        and split range to rangeInt, rangeFloat, rangeLong and rangeDouble
     * [0.44] Issue-633: Support unique index
     * [0.45] Issue-673: Add 'OVERRIDE' update strategy
     * [0.46] Issue-618 & 694: Support UUID id type
     * [0.47] Issue-691: Support aggregate property
     * [0.48] Issue-686: Support get schema by names
     * <p>
     * version 0.11:
     * [0.49] Issue-670: Support fusiform similarity API
     * [0.50] Issue-746: Support userdata for index label
     * [0.51] Issue-929: Support 5 TP RESTful API
     * [0.52] Issue-781: Support range query for rest api like P.gt(18)
     * [0.53] Issue-985: Add grant permission API
     * [0.54] Issue-295: Support ttl for vertex and edge
     * [0.55] Issue-994: Support results count for kneighbor/kout/rings
     * [0.56] Issue-800: Show schema status in schema API
     * [0.57] Issue-1105: Allow not rebuild index when create index label
     * [0.58] Issue-1173: Supports customized kout/kneighbor,
     *        multi-node-shortest-path, jaccard-similar and template-paths
     * [0.59] Issue-1333: Support graph read mode for olap property
     * [0.60] Issue-1392: Support create and resume snapshot
     * [0.61] Issue-1433: Unify naming of degree for oltp algorithms
     * [0.62] Issue-1378: Add compact api for rocksdb/cassandra/hbase backend
     * [0.63] Issue-1500: Add user-login RESTful API
     * [0.64] Issue-1504: Add auth-project RESTful API
     * [0.65] Issue-1506: Support olap property key
     * [0.66] Issue-1567: Support get schema RESTful API
     * [0.67] Issue-1065: Support dynamically add/remove graph
     * [0.68] Issue-1763: Support adamic-adar & resource-allocation API
     * [0.69] Issue-1748: Support Cypher query RESTful API
     * [0.70] PR-2242: Add edge-existence RESTful API
     * [0.71] PR-2286: Support Arthas API & Metric API prometheus format
     * [0.72] PR-3159: Support GraphSpace-wide default-role management APIs
     */

    /**
     * The second parameter of Version.of() is for IDE running without JAR
     * Note: Also update the version number in hugegraph-server/hugegraph-api/pom.xml
     */
    public static final Version VERSION = Version.of(ApiVersion.class, VersionUtil.getApiVersionFromProperties());

    public static void check() {
        // Check the version of hugegraph-core. Do first check from version 0.3
        VersionUtil.check(CoreVersion.VERSION, VersionUtil.getApiCheckBeginVersionFromProperties(), VersionUtil.getApiCheckEndVersionFromProperties(), CoreVersion.NAME);
    }
}
