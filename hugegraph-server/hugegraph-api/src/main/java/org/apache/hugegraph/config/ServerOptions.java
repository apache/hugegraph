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

package org.apache.hugegraph.config;

import static org.apache.hugegraph.config.OptionChecker.allowValues;
import static org.apache.hugegraph.config.OptionChecker.disallowEmpty;
import static org.apache.hugegraph.config.OptionChecker.nonNegativeInt;
import static org.apache.hugegraph.config.OptionChecker.positiveInt;
import static org.apache.hugegraph.config.OptionChecker.rangeDouble;
import static org.apache.hugegraph.config.OptionChecker.rangeInt;

public class ServerOptions extends OptionHolder {

    public static final ConfigOption<String> REST_SERVER_URL =
            new ConfigOption<>(
                    "restserver.url",
                    "The url for listening of graph server.",
                    disallowEmpty(),
                    "http://127.0.0.1:8080"
            ).withUrlNormalization("http://");

    public static final ConfigOption<Integer> SERVER_EVENT_HUB_THREADS =
            new ConfigOption<>(
                    "server.event_hub_threads",
                    "The event hub threads of server.",
                    rangeInt(1, 2 * CoreOptions.CPUS),
                    1
            );

    public static final ConfigOption<Boolean> ENABLE_SERVER_ROLE_ELECTION =
            new ConfigOption<>(
                    "server.role_election",
                    "Whether to enable role election, if enabled, the server " +
                    "will elect a master node in the cluster.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Integer> MAX_WORKER_THREADS =
            new ConfigOption<>(
                    "restserver.max_worker_threads",
                    "The maximum worker threads of rest server.",
                    rangeInt(2, Integer.MAX_VALUE),
                    2 * CoreOptions.CPUS
            );

    public static final ConfigOption<Integer> MIN_FREE_MEMORY =
            new ConfigOption<>(
                    "restserver.min_free_memory",
                    "The minimum free memory(MB) of rest server, requests " +
                    "will be rejected when the available memory of system " +
                    "is lower than this value.",
                    nonNegativeInt(),
                    64
            );

    public static final ConfigOption<Integer> TASK_THREADS =
            new ConfigOption<>(
                    "restserver.task_threads",
                    "The task threads of rest server.",
                    rangeInt(1, Math.max(4, CoreOptions.CPUS * 2)),
                    Math.max(4, CoreOptions.CPUS / 2)
            );

    public static final ConfigOption<Integer> REQUEST_TIMEOUT =
            new ConfigOption<>(
                    "restserver.request_timeout",
                    "The time in seconds within which a request must complete, " +
                    "-1 means no timeout.",
                    rangeInt(-1, Integer.MAX_VALUE),
                    30
            );

    public static final ConfigOption<String> WHITE_IP_STATUS =
            new ConfigOption<>(
                    "white_ip.status",
                    "The status of whether enable white ip.",
                    disallowEmpty(),
                    "disable"
            );

    public static final ConfigOption<Integer> CONN_IDLE_TIMEOUT =
            new ConfigOption<>(
                    "restserver.connection_idle_timeout",
                    "The time in seconds to keep an inactive connection " +
                    "alive, -1 means no timeout.",
                    rangeInt(-1, Integer.MAX_VALUE),
                    30
            );

    public static final ConfigOption<Integer> CONN_MAX_REQUESTS =
            new ConfigOption<>(
                    "restserver.connection_max_requests",
                    "The max number of HTTP requests allowed to be processed " +
                    "on one keep-alive connection, -1 means unlimited.",
                    rangeInt(-1, Integer.MAX_VALUE),
                    256
            );

    public static final ConfigOption<String> GREMLIN_SERVER_URL =
            new ConfigOption<>(
                    "gremlinserver.url",
                    "The url of gremlin server.",
                    disallowEmpty(),
                    "http://127.0.0.1:8182"
            ).withUrlNormalization("http://");

    public static final ConfigOption<Integer> GREMLIN_SERVER_TIMEOUT =
            new ConfigOption<>(
                    "gremlinserver.timeout",
                    "The timeout in seconds of waiting for gremlin server.",
                    positiveInt(),
                    30
            );

    public static final ConfigOption<Integer> GREMLIN_SERVER_MAX_ROUTE =
            new ConfigOption<>(
                    "gremlinserver.max_route",
                    "The max route number for gremlin server.",
                    positiveInt(),
                    2 * CoreOptions.CPUS
            );

    public static final ConfigListOption<String> META_ENDPOINTS =
            new ConfigListOption<>(
                    "meta.endpoints",
                    "The URL of meta endpoints.",
                    disallowEmpty(),
                    "http://127.0.0.1:2379"
            );

    public static final ConfigOption<Boolean> META_USE_CA =
            new ConfigOption<>(
                    "meta.use_ca",
                    "Whether to use ca to meta server.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Boolean> METRICS_DATA_TO_PD =
            new ConfigOption<>(
                    "metrics.data_to_pd",
                    "Whether to report metrics data to pd.",
                    disallowEmpty(),
                    true
            );

    public static final ConfigOption<String> META_CA =
            new ConfigOption<>(
                    "meta.ca",
                    "The ca file of meta server.",
                    null,
                    ""
            );

    public static final ConfigOption<String> META_CLIENT_CA =
            new ConfigOption<>(
                    "meta.client_ca",
                    "The client ca file of meta server.",
                    null,
                    ""
            );

    public static final ConfigOption<String> META_CLIENT_KEY =
            new ConfigOption<>(
                    "meta.client_key",
                    "The client key file of meta server.",
                    null,
                    ""
            );

    public static final ConfigOption<String> CLUSTER =
            new ConfigOption<>(
                    "cluster",
                    "The cluster name.",
                    disallowEmpty(),
                    "hg-test"
            );

    public static final ConfigOption<String> PD_PEERS =
            new ConfigOption<>(
                    "pd.peers",
                    "The pd server peers.",
                    disallowEmpty(),
                    "127.0.0.1:8686"
            );

    public static final ConfigOption<Boolean> SERVER_USE_K8S =
            new ConfigOption<>(
                    "server.use_k8s",
                    "Whether to use k8s to support multiple tenancy.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigListOption<String> K8S_ALGORITHMS =
            new ConfigListOption<>(
                    "k8s.algorithms",
                    "K8s algorithms",
                    disallowEmpty(),
                    "page-rank:org.apache.hugegraph.computer.algorithm.centrality.pagerank" +
                    ".PageRankParams",
                    "degree-centrality:org.apache.hugegraph.computer.algorithm.centrality.degree" +
                    ".DegreeCentralityParams",
                    "wcc:org.apache.hugegraph.computer.algorithm.community.wcc.WccParams",
                    "triangle-count:org.apache.hugegraph.computer.algorithm.community" +
                    ".trianglecount.TriangleCountParams",
                    "rings:org.apache.hugegraph.computer.algorithm.path.rings.RingsDetectionParams",
                    "rings-with-filter:org.apache.hugegraph.computer.algorithm.path.rings.filter" +
                    ".RingsDetectionWithFilterParams",
                    "betweenness-centrality:org.apache.hugegraph.computer.algorithm.centrality" +
                    ".betweenness.BetweennessCentralityParams",
                    "closeness-centrality:org.apache.hugegraph.computer.algorithm.centrality" +
                    ".closeness.ClosenessCentralityParams",
                    "lpa:org.apache.hugegraph.computer.algorithm.community.lpa.LpaParams",
                    "links:org.apache.hugegraph.computer.algorithm.path.links.LinksParams",
                    "kcore:org.apache.hugegraph.computer.algorithm.community.kcore.KCoreParams",
                    "louvain:org.apache.hugegraph.computer.algorithm.community.louvain" +
                    ".LouvainParams",
                    "clustering-coefficient:org.apache.hugegraph.computer.algorithm.community.cc" +
                    ".ClusteringCoefficientParams",
                    "ppr:org.apache.hugegraph.computer.algorithm.centrality.ppr" +
                    ".PersonalPageRankParams",
                    "subgraph-match:org.apache.hugegraph.computer.algorithm.path.subgraph" +
                    ".SubGraphMatchParams"
            );

    public static final ConfigOption<Boolean> SERVER_DEPLOY_IN_K8S =
            new ConfigOption<>(
                    "server.deploy_in_k8s",
                    "Whether to deploy server in k8s",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<String> SERVICE_ACCESS_PD_NAME =
            new ConfigOption<>(
                    "service.access_pd_name",
                    "Service name for server to access pd service.",
                    disallowEmpty(),
                    "hg"
            );

    public static final ConfigOption<String> SERVICE_ACCESS_PD_TOKEN =
            new ConfigOption<>(
                    "service.access_pd_token",
                    "Service token for server to access pd service.",
                    ""
            );

    public static final ConfigOption<String> SERVER_URLS_TO_PD =
            new ConfigOption<>(
                    "server.urls_to_pd",
                    "used as the server address reserved for PD and provided " +
                    "to clients. only used when starting the server in k8s.",
                    disallowEmpty(),
                    "http://0.0.0.0:8080"
            ).withUrlNormalization("http://");

    public static final ConfigOption<String> SERVER_K8S_URL =
            new ConfigOption<>(
                    "server.k8s_url",
                    "The url of k8s.",
                    disallowEmpty(),
                    "https://127.0.0.1:8888"
            ).withUrlNormalization("https://");

    public static final ConfigOption<Boolean> SERVER_K8S_USE_CA =
            new ConfigOption<>(
                    "server.k8s_use_ca",
                    "Whether to use ca to k8s api server.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<String> SERVER_K8S_CA =
            new ConfigOption<>(
                    "server.k8s_ca",
                    "The ca file of k8s api server.",
                    null,
                    ""
            );

    public static final ConfigOption<String> SERVER_K8S_CLIENT_CA =
            new ConfigOption<>(
                    "server.k8s_client_ca",
                    "The client ca file of k8s api server.",
                    null,
                    ""
            );

    public static final ConfigOption<String> SERVER_K8S_CLIENT_KEY =
            new ConfigOption<>(
                    "server.k8s_client_key",
                    "The client key file of k8s api server.",
                    null,
                    ""
            );

    public static final ConfigOption<String> SERVER_K8S_OLTP_IMAGE =
            new ConfigOption<>(
                    "server.k8s_oltp_image",
                    "The oltp server image of k8s.",
                    disallowEmpty(),
                    "127.0.0.1/kgs_bd/hugegraphserver:3.0.0"
            );

    public static final ConfigOption<String> SERVER_K8S_OLAP_IMAGE =
            new ConfigOption<>(
                    "server.k8s_olap_image",
                    "The olap server image of k8s.",
                    disallowEmpty(),
                    "hugegraph/hugegraph-server:v1"
            );

    public static final ConfigOption<String> SERVER_K8S_STORAGE_IMAGE =
            new ConfigOption<>(
                    "server.k8s_storage_image",
                    "The storage server image of k8s.",
                    disallowEmpty(),
                    "hugegraph/hugegraph-server:v1"
            );

    public static final ConfigOption<String> SERVER_DEFAULT_OLTP_K8S_NAMESPACE =
            new ConfigOption<>(
                    "server.default_oltp_k8s_namespace",
                    "The default namespace for HugeGraph default graph space.",
                    disallowEmpty(),
                    "hugegraph-server"
            );

    public static final ConfigOption<String> SERVER_DEFAULT_OLAP_K8S_NAMESPACE =
            new ConfigOption<>(
                    "server.default_olap_k8s_namespace",
                    "The default namespace for HugeGraph default graph space.",
                    disallowEmpty(),
                    "hugegraph-computer-system"
            );

    public static final ConfigOption<Boolean> GRAPH_LOAD_FROM_LOCAL_CONFIG =
            new ConfigOption<>(
                    "graph.load_from_local_config",
                    "Whether to load graphs from local configs.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<String> GRAPHS =
            new ConfigOption<>(
                    "graphs",
                    "The directory store graphs' config file.",
                    disallowEmpty(),
                    "./conf/graphs"
            );

    public static final ConfigOption<Boolean> SERVER_START_IGNORE_SINGLE_GRAPH_ERROR =
            new ConfigOption<>(
                    "server.start_ignore_single_graph_error",
                    "Whether to start ignore single graph error.",
                    disallowEmpty(),
                    true
            );

    public static final ConfigOption<Boolean> USE_PD =
            new ConfigOption<>(
                    "usePD",
                    "Whether use pd",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Integer> MAX_VERTICES_PER_BATCH =
            new ConfigOption<>(
                    "batch.max_vertices_per_batch",
                    "The maximum number of vertices submitted per batch.",
                    positiveInt(),
                    2500
            );

    public static final ConfigOption<Integer> MAX_EDGES_PER_BATCH =
            new ConfigOption<>(
                    "batch.max_edges_per_batch",
                    "The maximum number of edges submitted per batch.",
                    positiveInt(),
                    2500
            );

    public static final ConfigOption<Integer> MAX_WRITE_RATIO =
            new ConfigOption<>(
                    "batch.max_write_ratio",
                    "The maximum thread ratio for batch writing, " +
                    "only take effect if the batch.max_write_threads is 0.",
                    rangeInt(0, 100),
                    70
            );

    public static final ConfigOption<Integer> MAX_WRITE_THREADS =
            new ConfigOption<>(
                    "batch.max_write_threads",
                    "The maximum threads for batch writing, " +
                    "if the value is 0, the actual value will be set to " +
                    "batch.max_write_ratio * restserver.max_worker_threads.",
                    nonNegativeInt(),
                    0);

    public static final ConfigOption<String> ARTHAS_TELNET_PORT =
            new ConfigOption<>(
                    "arthas.telnetPort",
                    "arthas provides telnet ports to the outside",
                    disallowEmpty(),
                    "8562"
            );

    public static final ConfigOption<String> ARTHAS_HTTP_PORT =
            new ConfigOption<>(
                    "arthas.httpPort",
                    "arthas provides http ports to the outside",
                    disallowEmpty(),
                    "8561"
            );

    public static final ConfigOption<String> ARTHAS_IP =
            new ConfigOption<>(
                    "arthas.ip",
                    "arthas bound ip",
                    disallowEmpty(),
                    "0.0.0.0"
            );

    public static final ConfigOption<String> ARTHAS_DISABLED_COMMANDS =
            new ConfigOption<>(
                    "arthas.disabledCommands",
                    "arthas disabled commands",
                    disallowEmpty(),
                    "jad"
            );

    public static final ConfigOption<Boolean> ALLOW_TRACE =
            new ConfigOption<>(
                    "exception.allow_trace",
                    "Whether to allow exception trace stack.",
                    disallowEmpty(),
                    true
            );

    public static final ConfigOption<String> AUTHENTICATOR =
            new ConfigOption<>(
                    "auth.authenticator",
                    "The class path of authenticator implementation. " +
                    "e.g., org.apache.hugegraph.auth.StandardAuthenticator.",
                    null,
                    ""
            );

    public static final ConfigOption<String> ADMIN_PA =
            new ConfigOption<>(
                    "auth.admin_pa",
                    "The default password for built-in admin account, " +
                    "takes effect on first startup.",
                    null,
                    "pa"
            );

    public static final ConfigOption<String> SSL_KEYSTORE_FILE =
            new ConfigOption<>(
                    "ssl.keystore_file",
                    "The path of server keystore file used when https " +
                    "protocol is enabled.",
                    disallowEmpty(),
                    "conf/hugegraph-server.keystore"
            );

    public static final ConfigOption<String> SSL_KEYSTORE_PASSWORD =
            new ConfigOption<>(
                    "ssl.keystore_password",
                    "The password of the server keystore file " +
                    "when the https protocol is enabled.",
                    null,
                    "hugegraph"
            );

    public static final ConfigOption<String> SERVICE_GRAPH_SPACE =
            new ConfigOption<>(
                    "server.graphspace",
                    "The graph space of the server.",
                    null,
                    "DEFAULT"
            );

    public static final ConfigOption<String> SERVICE_ID =
            new ConfigOption<>(
                    "server.service_id",
                    "The service id of the server.",
                    null,
                    "DEFAULT"
            );

    public static final ConfigOption<String> PATH_GRAPH_SPACE =
            new ConfigOption<>(
                    "server.path_graphspace",
                    "The default path graph space of the server.",
                    null,
                    "DEFAULT"
            );

    public static final ConfigOption<Boolean> K8S_API_ENABLE =
            new ConfigOption<>(
                    "k8s.api",
                    "The k8s api start status " +
                    "when the computer service is enabled.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<String> K8S_NAMESPACE =
            new ConfigOption<>(
                    "k8s.namespace",
                    "The hugegraph url for k8s work " +
                    "when the computer service is enabled.",
                    null,
                    "hugegraph-computer-system"
            );

    public static final ConfigOption<String> K8S_ENABLE_INTERNAL_ALGORITHM =
            new ConfigOption<>(
                    "k8s.enable_internal_algorithm",
                    "Open k8s internal algorithm",
                    null,
                    "true"
            );
    public static final ConfigOption<String> K8S_INTERNAL_ALGORITHM =
            new ConfigOption<>(
                    "k8s.internal_algorithm",
                    "K8s internal algorithm",
                    disallowEmpty(),
                    "[page-rank, degree-centrality, wcc, triangle-count, rings, " +
                    "rings-with-filter, betweenness-centrality, closeness-centrality, lpa, links," +
                    " kcore, louvain, clustering-coefficient, ppr, subgraph-match]"
            );

    public static final ConfigOption<String> SERVER_ID =
            new ConfigOption<>(
                    "server.id",
                    "The id of hugegraph-server.",
                    disallowEmpty(),
                    "server-1"
            );
    public static final ConfigOption<String> SERVER_ROLE =
            new ConfigOption<>(
                    "server.role",
                    "The role of nodes in the cluster, available types are " +
                    "[master, worker, computer]",
                    allowValues("master", "worker", "computer"),
                    "master"
            );

    public static final ConfigOption<String> RAFT_GROUP_PEERS =
            new ConfigOption<>(
                    "raft.group_peers",
                    "The rpc address of raft group initial peers.",
                    disallowEmpty(),
                    "127.0.0.1:8090"
            );

    public static final ConfigOption<String> AUTH_GRAPH_STORE =
            new ConfigOption<>(
                    "auth.graph_store",
                    "The name of graph used to store authentication information, " +
                    "like users, only for org.apache.hugegraph.auth.StandardAuthenticator.",
                    disallowEmpty(),
                    "hugegraph"
            );

    public static final ConfigOption<String> AUTH_REMOTE_URL =
            new ConfigOption<>(
                    "auth.remote_url",
                    "If the address is empty, it provide auth service, " +
                    "otherwise it is auth client and also provide auth service " +
                    "through rpc forwarding. The remote url can be set to " +
                    "multiple addresses, which are concat by ','.",
                    null,
                    ""
            );

    public static final ConfigOption<String> NODE_ID =
            new ConfigOption<>(
                    "server.node_id",
                    "The node id of the server.",
                    null,
                    "node-id1"
            );

    public static final ConfigOption<String> NODE_ROLE =
            new ConfigOption<>(
                    "server.node_role",
                    "The node role of the server.",
                    null,
                    "worker"
            );

    public static final ConfigOption<String> K8S_KUBE_CONFIG =
            new ConfigOption<>(
                    "k8s.kubeconfig",
                    "The k8s kube config file " +
                    "when the computer service is enabled.",
                    null,
                    ""
            );

    public static final ConfigOption<String> K8S_HUGEGRAPH_URL =
            new ConfigOption<>(
                    "k8s.hugegraph_url",
                    "The hugegraph url for k8s work " +
                    "when the computer service is enabled.",
                    null,
                    ""
            );
    public static final ConfigOption<Boolean> ENABLE_DYNAMIC_CREATE_DROP =
            new ConfigOption<>(
                    "graphs.enable_dynamic_create_drop",
                    "Whether to enable create or drop graph dynamically.",
                    disallowEmpty(),
                    true
            );

    public static final ConfigOption<Long> SLOW_QUERY_LOG_TIME_THRESHOLD =
            new ConfigOption<>(
                    "log.slow_query_threshold",
                    "The threshold time(ms) of logging slow query, " +
                    "0 means logging slow query is disabled.",
                    nonNegativeInt(),
                    1000L
            );
    public static final ConfigOption<Double> JVM_MEMORY_MONITOR_THRESHOLD =
            new ConfigOption<>(
                    "memory_monitor.threshold",
                    "Threshold for JVM memory usage monitoring, 1 means disabling the memory " +
                    "monitoring task.",
                    rangeDouble(0.0, 1.0),
                    0.85
            );
    public static final ConfigOption<Integer> JVM_MEMORY_MONITOR_DETECT_PERIOD =
            new ConfigOption<>(
                    "memory_monitor.period",
                    "The period in ms of JVM memory usage monitoring, in each period we will " +
                    "detect the jvm memory usage and take corresponding actions.",
                    nonNegativeInt(),
                    2000
            );
    public static ConfigOption<String> K8S_INTERNAL_ALGORITHM_IMAGE_URL =
            new ConfigOption<>(
                    "k8s.internal_algorithm_image_url",
                    "K8s internal algorithm image url",
                    null,
                    ""
            );
    private static volatile ServerOptions instance;

    private ServerOptions() {
        super();
    }

    public static synchronized ServerOptions instance() {
        if (instance == null) {
            instance = new ServerOptions();
            instance.registerOptions();
        }
        return instance;
    }
}
