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

import static org.apache.hugegraph.HugeFactory.SYS_GRAPH;
import static org.apache.hugegraph.space.GraphSpace.DEFAULT_GRAPH_SPACE_DESCRIPTION;
import static org.apache.hugegraph.space.GraphSpace.DEFAULT_GRAPH_SPACE_SERVICE_NAME;

import java.io.IOException;
import java.io.StringWriter;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.StandardHugeGraph;
import org.apache.hugegraph.auth.AuthManager;
import org.apache.hugegraph.auth.HugeAuthenticator;
import org.apache.hugegraph.auth.HugeAuthenticator.User;
import org.apache.hugegraph.auth.HugeFactoryAuthProxy;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.auth.HugeUser;
import org.apache.hugegraph.auth.StandardAuthenticator;
import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.cache.Cache;
import org.apache.hugegraph.backend.cache.CacheManager;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.store.AbstractBackendStoreProvider;
import org.apache.hugegraph.backend.store.BackendStoreInfo;
import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.config.TypedOption;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.exception.ExistedException;
import org.apache.hugegraph.exception.NotSupportException;
import org.apache.hugegraph.io.HugeGraphSONModule;
import org.apache.hugegraph.k8s.K8sDriver;
import org.apache.hugegraph.k8s.K8sDriverProxy;
import org.apache.hugegraph.k8s.K8sManager;
import org.apache.hugegraph.k8s.K8sRegister;
import org.apache.hugegraph.kvstore.KvStore;
import org.apache.hugegraph.kvstore.KvStoreImpl;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.masterelection.RoleElectionOptions;
import org.apache.hugegraph.masterelection.RoleElectionStateMachine;
import org.apache.hugegraph.masterelection.StandardRoleListener;
import org.apache.hugegraph.meta.MetaDriver;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.meta.PdMetaDriver;
import org.apache.hugegraph.meta.lock.LockResult;
import org.apache.hugegraph.metrics.MetricsUtil;
import org.apache.hugegraph.metrics.ServerReporter;
import org.apache.hugegraph.pd.client.DiscoveryClientImpl;
import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfos;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.apache.hugegraph.pd.grpc.discovery.RegisterInfo;
import org.apache.hugegraph.rpc.RpcClientProvider;
import org.apache.hugegraph.rpc.RpcConsumerConfig;
import org.apache.hugegraph.rpc.RpcProviderConfig;
import org.apache.hugegraph.rpc.RpcServer;
import org.apache.hugegraph.serializer.JsonSerializer;
import org.apache.hugegraph.serializer.Serializer;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.space.SchemaTemplate;
import org.apache.hugegraph.space.Service;
import org.apache.hugegraph.space.register.RegisterConfig;
import org.apache.hugegraph.space.register.dto.ServiceDTO;
import org.apache.hugegraph.space.register.registerImpl.PdRegister;
import org.apache.hugegraph.task.TaskManager;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.traversal.optimize.HugeScriptTraversal;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.hugegraph.type.define.GraphMode;
import org.apache.hugegraph.type.define.GraphReadMode;
import org.apache.hugegraph.type.define.NodeRole;
import org.apache.hugegraph.util.ConfigUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Events;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.StringEncoding;
import org.apache.hugegraph.util.collection.CollectionFactory;
import org.apache.hugegraph.version.CoreVersion;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;

import com.alipay.sofa.rpc.config.ServerConfig;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;

import io.fabric8.kubernetes.api.model.Namespace;
import jakarta.ws.rs.core.SecurityContext;

public final class GraphManager {

    public static final String NAME_REGEX = "^[a-z][a-z0-9_]{0,47}$";
    // nickname should be compatible with all patterns of name
    public static final String NICKNAME_REGEX = "^[a-zA-Z\u4e00-\u9fa5]" +
                                                "[a-zA-Z0-9\u4e00-\u9fa5~!@#$" +
                                                "%^&*()_+|<>,.?/:;" +
                                                "'`\"\\[\\]{}\\\\]{0,47}$";
    public static final int NICKNAME_MAX_LENGTH = 48;
    public static final String DELIMITER = "-";
    public static final String NAMESPACE_CREATE = "namespace_create";
    private static final Logger LOG = Log.logger(GraphManager.class);
    private KvStore kvStore;

    private final String cluster;
    private final String graphsDir;
    private final Boolean startIgnoreSingleGraphError;
    private final Boolean graphLoadFromLocalConfig;
    private final Boolean k8sApiEnabled;
    private final Map<String, GraphSpace> graphSpaces;
    private final Map<String, Service> services;
    //FIXME: add one class like graphKey as key,which contains graphSpace and graphName
    private final Map<String, Graph> graphs;
    private final Set<String> localGraphs;
    private final Set<String> removingGraphs;
    private final Set<String> creatingGraphs;
    private final HugeAuthenticator authenticator;
    private final AuthManager authManager;
    private final MetaManager metaManager = MetaManager.instance();
    private final K8sManager k8sManager = K8sManager.instance();
    private final String serviceGraphSpace;
    private final String serviceID;
    private final String pdPeers;

    private final RpcServer rpcServer;
    private final RpcClientProvider rpcClient;
    private final GlobalMasterInfo globalNodeRoleInfo;
    private final HugeConfig conf;
    private final EventHub eventHub;
    private final String url;
    private final Set<String> serverUrlsToPd;
    private final Boolean serverDeployInK8s;
    private final HugeConfig config;
    private RoleElectionStateMachine roleStateMachine;
    private K8sDriver.CA ca;
    private final boolean PDExist;

    private String pdK8sServiceId;

    private DiscoveryClientImpl pdClient;

    private boolean licenseValid;

    public GraphManager(HugeConfig conf, EventHub hub) {
        LOG.info("Init graph manager");
        E.checkArgumentNotNull(conf, "The config can't be null");
        String server = conf.get(ServerOptions.SERVER_ID);
        String role = conf.get(ServerOptions.SERVER_ROLE);

        this.config = conf;
        this.url = conf.get(ServerOptions.REST_SERVER_URL);
        this.serverUrlsToPd = new HashSet<>(Arrays.asList(
                conf.get(ServerOptions.SERVER_URLS_TO_PD).split(",")));
        this.serverDeployInK8s =
                conf.get(ServerOptions.SERVER_DEPLOY_IN_K8S);
        this.startIgnoreSingleGraphError = conf.get(
                ServerOptions.SERVER_START_IGNORE_SINGLE_GRAPH_ERROR);
        E.checkArgument(server != null && !server.isEmpty(),
                        "The server name can't be null or empty");
        E.checkArgument(role != null && !role.isEmpty(),
                        "The server role can't be null or empty");
        this.graphsDir = conf.get(ServerOptions.GRAPHS);
        this.cluster = conf.get(ServerOptions.CLUSTER);
        this.graphSpaces = new ConcurrentHashMap<>();
        this.services = new ConcurrentHashMap<>();
        // key is graphSpaceName + "-" + graphName
        this.graphs = new ConcurrentHashMap<>();
        this.removingGraphs = ConcurrentHashMap.newKeySet();
        this.creatingGraphs = ConcurrentHashMap.newKeySet();
        this.authenticator = HugeAuthenticator.loadAuthenticator(conf);
        this.serviceGraphSpace = conf.get(ServerOptions.SERVICE_GRAPH_SPACE);
        this.serviceID = conf.get(ServerOptions.SERVICE_ID);
        this.rpcServer = new RpcServer(conf);
        this.rpcClient = new RpcClientProvider(conf);
        this.pdPeers = conf.get(ServerOptions.PD_PEERS);

        this.roleStateMachine = null;
        this.globalNodeRoleInfo = new GlobalMasterInfo();

        this.eventHub = hub;
        this.conf = conf;
        this.k8sApiEnabled = conf.get(ServerOptions.K8S_API_ENABLE);
        this.licenseValid = true;

        this.listenChanges();
        this.initNodeRole();
        if (this.authenticator != null) {
            this.authManager = this.authenticator.authManager();
        } else {
            this.authManager = null;
        }

        // load graphs
        this.graphLoadFromLocalConfig =
                conf.get(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG);
        if (this.graphLoadFromLocalConfig) {
            // Load graphs configured in local conf/graphs directory
            Map<String, String> graphConfigs =
                    ConfigUtil.scanGraphsDir(this.graphsDir);
            this.localGraphs = graphConfigs.keySet();
            this.loadGraphsFromLocal(graphConfigs);
        } else {
            this.localGraphs = ImmutableSet.of();
        }

        PDExist = conf.get(ServerOptions.USE_PD);
        if (PDExist) {
            try {
                loadMetaFromPD();
            } catch (Exception e) {
                LOG.error("Unable to load meta for PD server and usePD = true in server options",
                          e);
                throw new IllegalStateException(e);
            }
        }
    }

    private static String spaceGraphName(String graphSpace, String graph) {
        return String.join(DELIMITER, graphSpace, graph);
    }

    private static String serviceId(String graphSpace, Service.ServiceType type,
                                    String serviceName) {
        return String.join(DELIMITER, graphSpace, type.name(), serviceName)
                     .replace("_", "-").toLowerCase();
    }

    private boolean usePD() {
        return this.PDExist;
    }

    private static void registerCacheMetrics(Map<String, Cache<?, ?>> caches) {
        Set<String> names = MetricManager.INSTANCE.getRegistry().getNames();
        for (Map.Entry<String, Cache<?, ?>> entry : caches.entrySet()) {
            String key = entry.getKey();
            Cache<?, ?> cache = entry.getValue();

            String hits = String.format("%s.%s", key, "hits");
            String miss = String.format("%s.%s", key, "miss");
            String exp = String.format("%s.%s", key, "expire");
            String size = String.format("%s.%s", key, "size");
            String cap = String.format("%s.%s", key, "capacity");

            // Avoid registering multiple times
            if (names.stream().anyMatch(name -> name.endsWith(hits))) {
                continue;
            }

            MetricsUtil.registerGauge(Cache.class, hits, cache::hits);
            MetricsUtil.registerGauge(Cache.class, miss, cache::miss);
            MetricsUtil.registerGauge(Cache.class, exp, cache::expire);
            MetricsUtil.registerGauge(Cache.class, size, cache::size);
            MetricsUtil.registerGauge(Cache.class, cap, cache::capacity);
        }
    }

    private static void sleep1s() {
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    private static String serviceName(String graphSpace, String service) {
        return String.join(DELIMITER, graphSpace, service);
    }

    private static void checkName(String name, String type) {
        E.checkArgument(name.matches(NAME_REGEX),
                        "Invalid id or name '%s' for %s, valid name is up to " +
                        "48 alpha-numeric characters and underscores and only" +
                        "letters are supported as first letter. " +
                        "Note: letter is lower case", name, type);
    }

    private static void checkGraphSpaceName(String name) {
        if (DEFAULT_GRAPH_SPACE_SERVICE_NAME.equals(name)) {
            return;
        }
        checkName(name, "graph space");
    }

    private static void checkGraphName(String name) {
        checkName(name, "graph");
    }

    public static void checkNickname(String nickname) {
        E.checkArgument(nickname.matches(NICKNAME_REGEX),
                        "Invalid nickname '%s' for %s, valid name is up " +
                        "to %s letters, Chinese or special " +
                        "characters, and can only start with a " +
                        "letter or Chinese", nickname, "graph",
                        NICKNAME_MAX_LENGTH);
    }

    private void loadMetaFromPD() {
        try {
            PDConfig pdConfig = PDConfig.of(this.pdPeers);
            pdConfig.setAuthority(PdMetaDriver.PDAuthConfig.service(),
                                  PdMetaDriver.PDAuthConfig.token());
            this.pdClient = DiscoveryClientImpl
                    .newBuilder()
                    .setCenterAddress(this.pdPeers)
                    .setPdConfig(pdConfig)
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
        }

        this.initMetaManager(conf);
        this.initK8sManagerIfNeeded(conf);
        this.initAdminUserIfNeeded(conf.get(ServerOptions.ADMIN_PA));

        this.createDefaultGraphSpaceIfNeeded(conf);

        this.loadGraphSpaces();

        this.kvStore = this.kvStoreInit();
        this.loadServices();

        this.loadGraphsFromMeta(this.graphConfigs());
        this.listenMetaChanges();
    }

    public void initAdminUserIfNeeded(String password) {
        HugeUser user = new HugeUser("admin");
        user.nickname("超级管理员");
        user.password(StringEncoding.hashPassword(password));
        user.creator(HugeAuthenticator.USER_SYSTEM);
        user.phone("18888886666");
        user.email("admin@hugegraph.com");
        user.description("None");
        user.update(new Date());
        user.create(new Date());
        user.avatar("/image.png");
        try {
            this.metaManager.createUser(user);
            this.metaManager.initDefaultGraphSpace();
        } catch (Exception e) {
            LOG.info(e.getMessage());
        }
    }

    public static void prepareSchema(HugeGraph graph, String gremlin) {
        Map<String, Object> bindings = ImmutableMap.of(
                "graph", graph,
                "schema", graph.schema());
        HugeScriptTraversal<?, ?> traversal = new HugeScriptTraversal<>(
                graph.traversal(),
                "gremlin-groovy", gremlin,
                bindings, ImmutableMap.of());
        while (traversal.hasNext()) {
            traversal.next();
        }
        try {
            traversal.close();
        } catch (Exception e) {
            throw new HugeException("Failed to init schema", e);
        }
    }

    private KvStore kvStoreInit() {
        HugeGraph sysGraph = createSysGraphIfNeed();
        return new KvStoreImpl(sysGraph);
    }

    private HugeGraph createSysGraphIfNeed() {
        Map<String, Object> sysGraphConfig =
                this.metaManager.getSysGraphConfig();
        boolean init = false;
        Date timeStamp = new Date();
        // Create system graph in default graph space
        String gs = "DEFAULT";
        if (sysGraphConfig == null) {
            init = true;
            sysGraphConfig = new HashMap<>();
            sysGraphConfig.put(ServerOptions.PD_PEERS.name(), this.pdPeers);
            sysGraphConfig.put(CoreOptions.GRAPH_SPACE.name(), gs);

            sysGraphConfig.put("gremlin.graph", "org.apache.hugegraph.HugeFactory");
            sysGraphConfig.put("backend", "hstore");
            sysGraphConfig.put("serializer", "binary");
            sysGraphConfig.put("store", SYS_GRAPH);
            sysGraphConfig.putIfAbsent("nickname", SYS_GRAPH);
            sysGraphConfig.putIfAbsent("creator", "admin");
            sysGraphConfig.putIfAbsent("create_time", timeStamp);
            sysGraphConfig.putIfAbsent("update_time", timeStamp);
            this.metaManager.addSysGraphConfig(sysGraphConfig);
        }

        Configuration propConfig =
                this.buildConfig(attachLocalCacheConfig(sysGraphConfig));
        HugeConfig config = new HugeConfig(propConfig);
        HugeGraph graph = this.createGraph(gs, config, this.authManager, init);

        graph.graphSpace(gs);
        graph.nickname(SYS_GRAPH);
        graph.creator("admin");
        graph.createTime(timeStamp);
        graph.updateTime(timeStamp);
        return graph;
    }

    public void init() {
        this.listenChanges();

        this.loadGraphsFromLocal(ConfigUtil.scanGraphsDir(this.graphsDir));

        // Start RPC-Server for raft-rpc/auth-rpc/cache-notify-rpc...
        this.startRpcServer();

        // Raft will load snapshot firstly then launch election and replay log
        this.waitGraphsReady();

        this.checkBackendVersionOrExit(this.conf);
        this.serverStarted(this.conf);

        this.addMetrics(this.conf);
    }

    public void reload() {
        // Remove graphs from GraphManager
        for (String graph : this.graphs.keySet()) {
            String[] parts = graph.split(DELIMITER);
            this.dropGraph(parts[0], parts[1], false);
        }
        int count = 0;
        while (!this.graphs.isEmpty() && count++ < 10) {
            sleep1s();
        }
        if (!this.graphs.isEmpty()) {
            throw new HugeException("Failed to reload grahps, try later");
        }
        if (this.graphLoadFromLocalConfig) {
            // Load graphs configured in local conf/graphs directory
            this.loadGraphsFromLocal(ConfigUtil.scanGraphsDir(this.graphsDir));
        }
        // Load graphs configured in etcd
        this.loadGraphsFromMeta(this.graphConfigs());
    }

    public void destroy() {
        this.unlistenChanges();
    }

    private void initMetaManager(HugeConfig conf) {
        String endpoints = conf.get(ServerOptions.PD_PEERS);
        boolean useCa = conf.get(ServerOptions.META_USE_CA);
        String ca = null;
        String clientCa = null;
        String clientKey = null;
        if (useCa) {
            ca = conf.get(ServerOptions.META_CA);
            clientCa = conf.get(ServerOptions.META_CLIENT_CA);
            clientKey = conf.get(ServerOptions.META_CLIENT_KEY);
            this.ca = new K8sDriver.CA(ca, clientCa, clientKey);
        }
        this.metaManager.connect(this.cluster, MetaManager.MetaDriverType.PD,
                                 ca, clientCa, clientKey, endpoints);
    }

    private void initK8sManagerIfNeeded(HugeConfig conf) {
        boolean useK8s = conf.get(ServerOptions.SERVER_USE_K8S);
        if (useK8s) {
            String oltpImage = conf.get(ServerOptions.SERVER_K8S_OLTP_IMAGE);
            String olapImage = conf.get(ServerOptions.SERVER_K8S_OLAP_IMAGE);
            String storageImage =
                    conf.get(ServerOptions.SERVER_K8S_STORAGE_IMAGE);
            this.k8sManager.connect(oltpImage, olapImage, storageImage, this.ca);
        }
    }

    private void loadGraphSpaces() {
        Map<String, GraphSpace> graphSpaceConfigs =
                this.metaManager.graphSpaceConfigs();
        this.graphSpaces.putAll(graphSpaceConfigs);
        for (Map.Entry<String, GraphSpace> entry : graphSpaceConfigs.entrySet()) {
            if (this.serviceGraphSpace.equals(entry.getKey())) {
                overwriteAlgorithmImageUrl(entry.getValue().internalAlgorithmImageUrl());
            }
        }
    }

    private void loadServices() {
        for (String graphSpace : this.graphSpaces.keySet()) {
            Map<String, Service> services = this.metaManager
                    .serviceConfigs(graphSpace);
            for (Map.Entry<String, Service> entry : services.entrySet()) {
                this.services.put(serviceName(graphSpace, entry.getKey()),
                                  entry.getValue());
            }
        }
        Service service = new Service(this.serviceID, User.ADMIN.getName(),
                                      Service.ServiceType.OLTP,
                                      Service.DeploymentType.MANUAL);
        service.description(service.name());

        if (this.serverDeployInK8s) {
            // Support SaaS mode only in k8s to start server, register correct server service
            // URLs to pd
            service.urls(this.serverUrlsToPd);
        } else {
            service.url(this.url);
        }

        service.serviceId(serviceId(this.serviceGraphSpace,
                                    Service.ServiceType.OLTP,
                                    this.serviceID));

        String serviceName = serviceName(this.serviceGraphSpace, this.serviceID);
        Boolean newAdded = false;
        if (!this.services.containsKey(serviceName)) {
            newAdded = true;
            // add to local cache
            this.services.put(serviceName, service);
        }
        Service self = this.services.get(serviceName);
        if (!self.sameService(service)) {
            /*
             * update service if it has been changed(e.g. for manual service,
             * url may change)
             */
            newAdded = true;
            self = service;
        }
        if (null != self) {
            // register self to pd, should prior to etcd due to pdServiceId info
            this.registerServiceToPd(this.serviceGraphSpace, self);
            if (self.k8s()) {
                try {
                    this.registerK8StoPd(self);
                } catch (Exception e) {
                    LOG.error("Register K8s info to PD failed: {}", e);
                }
            }
            if (newAdded) {
                // Register to etcd since even-handler has not been registered now
                this.metaManager.addServiceConfig(this.serviceGraphSpace, self);
                this.metaManager.notifyServiceAdd(this.serviceGraphSpace,
                                                  this.serviceID);
            }
        }
    }

    /**
     * Force overwrite internalAlgorithmImageUrl
     */
    public void overwriteAlgorithmImageUrl(String imageUrl) {
        if (StringUtils.isNotBlank(imageUrl) && this.k8sApiEnabled) {

            ServerOptions.K8S_INTERNAL_ALGORITHM_IMAGE_URL = new ConfigOption<>(
                    "k8s.internal_algorithm_image_url",
                    "K8s internal algorithm image url",
                    null,
                    imageUrl
            );

            String enableInternalAlgorithm = K8sDriverProxy.getEnableInternalAlgorithm();
            String internalAlgorithm = K8sDriverProxy.getInternalAlgorithm();
            Map<String, String> algorithms = K8sDriverProxy.getAlgorithms();
            try {
                K8sDriverProxy.setConfig(
                        enableInternalAlgorithm,
                        imageUrl,
                        internalAlgorithm,
                        algorithms
                );
            } catch (IOException e) {
                LOG.error("Overwrite internal_algorithm_image_url failed! {}", e);
            }
        }
    }

    private void createDefaultGraphSpaceIfNeeded(HugeConfig config) {
        Map<String, GraphSpace> graphSpaceConfigs =
                this.metaManager.graphSpaceConfigs();
        GraphSpace graphSpace;
        if (graphSpaceConfigs.containsKey(DEFAULT_GRAPH_SPACE_SERVICE_NAME)) {
            return;
        }
        String oltpNs = config.get(
                ServerOptions.SERVER_DEFAULT_OLTP_K8S_NAMESPACE);
        String olapNs = config.get(
                ServerOptions.SERVER_DEFAULT_OLAP_K8S_NAMESPACE);
        graphSpace = this.createGraphSpace(DEFAULT_GRAPH_SPACE_SERVICE_NAME,
                                           GraphSpace.DEFAULT_NICKNAME,
                                           DEFAULT_GRAPH_SPACE_DESCRIPTION,
                                           Integer.MAX_VALUE, Integer.MAX_VALUE,
                                           Integer.MAX_VALUE, Integer.MAX_VALUE,
                                           Integer.MAX_VALUE, oltpNs, olapNs,
                                           false, User.ADMIN.getName(),
                                           ImmutableMap.of());
        boolean useK8s = config.get(ServerOptions.SERVER_USE_K8S);
        if (!useK8s) {
            return;
        }
        String oltp = config.get(ServerOptions.SERVER_DEFAULT_OLTP_K8S_NAMESPACE);
        // oltp namespace
        Namespace oltpNamespace = this.k8sManager.namespace(oltp);
        if (oltpNamespace == null) {
            throw new HugeException(
                    "The config option: %s, value: %s does not exist",
                    ServerOptions.SERVER_DEFAULT_OLTP_K8S_NAMESPACE.name(),
                    oltp);
        }
        graphSpace.oltpNamespace(oltp);
        // olap namespace
        String olap = config.get(ServerOptions.SERVER_DEFAULT_OLAP_K8S_NAMESPACE);
        Namespace olapNamespace = this.k8sManager.namespace(olap);
        if (olapNamespace == null) {
            throw new HugeException(
                    "The config option: %s, value: %s does not exist",
                    ServerOptions.SERVER_DEFAULT_OLAP_K8S_NAMESPACE.name(),
                    olap);
        }
        graphSpace.olapNamespace(olap);
        // storage is same as oltp
        graphSpace.storageNamespace(oltp);
        this.updateGraphSpace(graphSpace);
    }

    private GraphSpace createGraphSpace(String name, String nickname,
                                        String description,
                                        int cpuLimit, int memoryLimit,
                                        int storageLimit,
                                        int maxGraphNumber,
                                        int maxRoleNumber,
                                        String oltpNamespace,
                                        String olapNamespace,
                                        boolean auth, String creator,
                                        Map<String, Object> configs) {
        GraphSpace space = new GraphSpace(name, nickname, description,
                                          cpuLimit,
                                          memoryLimit, storageLimit,
                                          maxGraphNumber, maxRoleNumber,
                                          auth, creator, configs);
        space.oltpNamespace(oltpNamespace);
        space.olapNamespace(olapNamespace);
        return this.createGraphSpace(space);
    }

    /*
     * 1.create DEFAULT space when init service
     * 2.Direct request server, create space with name and nickname
     * */
    public GraphSpace createGraphSpace(GraphSpace space) {
        String name = space.name();
        checkGraphSpaceName(name);
        String nickname = space.nickname();
        if (StringUtils.isNotEmpty(nickname)) {
            checkNickname(nickname);
        } else {
            nickname = name;
        }

        E.checkArgument(!isExistedSpaceNickname(name, nickname),
                        "Space nickname '%s' existed",
                        nickname);
        space.name(name);
        space.nickname(nickname);
        this.limitStorage(space, space.storageLimit);

        boolean useK8s = config.get(ServerOptions.SERVER_USE_K8S);

        if (useK8s) {
            E.checkArgument(!space.oltpNamespace().isEmpty() &&
                            !space.olapNamespace().isEmpty(),
                            "Oltp and olap namespace of space for " +
                            "k8s-enabled server must be set",
                            nickname);

            boolean notDefault = !DEFAULT_GRAPH_SPACE_SERVICE_NAME.equals(name);
            int cpuLimit = space.cpuLimit();
            int memoryLimit = space.memoryLimit();

            int computeCpuLimit = space.computeCpuLimit() == 0 ?
                                  space.cpuLimit() : space.computeCpuLimit();
            int computeMemoryLimit = space.computeMemoryLimit() == 0 ?
                                     space.memoryLimit() : space.computeMemoryLimit();
            boolean sameNamespace = space.oltpNamespace().equals(space.olapNamespace());
            attachK8sNamespace(space.oltpNamespace(),
                               space.operatorImagePath(), sameNamespace);
            if (notDefault) {
                if (sameNamespace) {
                    this.makeResourceQuota(space.oltpNamespace(),
                                           cpuLimit + computeCpuLimit,
                                           memoryLimit + computeMemoryLimit);
                } else {
                    this.makeResourceQuota(space.oltpNamespace(), cpuLimit,
                                           memoryLimit);
                }
            }
            if (!sameNamespace) {
                attachK8sNamespace(space.olapNamespace(),
                                   space.operatorImagePath(), true);
                if (notDefault) {
                    this.makeResourceQuota(space.olapNamespace(),
                                           computeCpuLimit, computeMemoryLimit);
                }
            }
        }

        this.metaManager.addGraphSpaceConfig(name, space);
        this.metaManager.appendGraphSpaceList(name);
        this.metaManager.notifyGraphSpaceAdd(name);

        this.graphSpaces.put(name, space);
        return space;
    }

    private GraphSpace updateGraphSpace(GraphSpace space) {
        String name = space.name();
        this.metaManager.addGraphSpaceConfig(name, space);
        this.metaManager.notifyGraphSpaceUpdate(name);
        this.graphSpaces.put(name, space);
        return space;
    }

    /**
     * Create or get new namespaces
     *
     * @param namespace
     * @return isNewCreated
     */
    private boolean attachK8sNamespace(String namespace, String olapOperatorImage, Boolean isOlap) {
        boolean isNewCreated = false;
        try {
            if (!Strings.isNullOrEmpty(namespace)) {
                Namespace current = k8sManager.namespace(namespace);
                if (null == current) {
                    LockResult lock = this.metaManager.lock(this.cluster,
                                                            NAMESPACE_CREATE,
                                                            namespace);
                    try {
                        current = k8sManager.namespace(namespace);
                        if (null != current) {
                            return false;
                        }
                        current = k8sManager.createNamespace(namespace,
                                                             ImmutableMap.of());
                        if (null == current) {
                            throw new HugeException(
                                    "Cannot attach k8s namespace {}",
                                    namespace);
                        }
                        isNewCreated = true;
                        // start operator pod
                        // read from computer-system or default ?
                        // read from "hugegraph-computer-system"
                        // String containerName = "hugegraph-operator";
                        // String imageName = "";
                        if (isOlap) {
                            LOG.info("Try to create operator pod for k8s " +
                                     "namespace {} with operator image {}",
                                     namespace, olapOperatorImage);
                            k8sManager.createOperatorPod(namespace,
                                                         olapOperatorImage);
                        }
                    } finally {
                        this.metaManager.unlock(lock, this.cluster,
                                                NAMESPACE_CREATE, namespace);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Attach k8s namespace meet error {}", e);
        }
        return isNewCreated;
    }

    private void makeResourceQuota(String namespace, int cpuLimit,
                                   int memoryLimit) {
        k8sManager.loadResourceQuota(namespace, cpuLimit, memoryLimit);
    }

    private void limitStorage(GraphSpace space, int storageLimit) {
        PDConfig pdConfig = PDConfig.of(this.pdPeers).setEnablePDNotify(true);
        pdConfig.setAuthority(PdMetaDriver.PDAuthConfig.service(),
                              PdMetaDriver.PDAuthConfig.token());
        PDClient pdClient = PDClient.create(pdConfig);
        try {
            pdClient.setGraphSpace(space.name(), storageLimit);
        } catch (Exception e) {
            LOG.error("Exception occur when set storage limit!", e);
        }
    }

    public void getSpaceStorage(String graphSpace) {
        GraphSpace gs = this.graphSpace(graphSpace);
        if (gs == null) {
            throw new HugeException("Cannot find graph space {}", graphSpace);
        }
        MetaDriver metaDriver = this.metaManager.metaDriver();
        assert metaDriver instanceof PdMetaDriver;
        PDClient pdClient = ((PdMetaDriver) metaDriver).pdClient();
        try {
            Metapb.GraphSpace spaceMeta = pdClient.getGraphSpace(graphSpace).get(0);
            Long usedGb = (spaceMeta.getUsedSize() / (1024 * 1024));
            gs.setStorageUsed(usedGb.intValue());
        } catch (PDException e) {
            LOG.error("Get graph space '{}' storage information meet error {}",
                      graphSpace, e);
        }
    }

    public void clearGraphSpace(String name) {
        // Clear all roles
        this.metaManager.clearGraphAuth(name);

        // Clear all schemaTemplate
        this.metaManager.clearSchemaTemplate(name);

        // Clear all graphs
        for (String key : this.graphs.keySet()) {
            if (key.startsWith(name)) {
                String[] parts = key.split(DELIMITER);
                this.dropGraph(parts[0], parts[1], true);
            }
        }

        // Clear all services
        for (String key : this.services.keySet()) {
            if (key.startsWith(name)) {
                String[] parts = key.split(DELIMITER);
                this.dropService(parts[0], parts[1]);
            }
        }
    }

    public void dropGraphSpace(String name) {
        if (this.serviceGraphSpace.equals(name)) {
            throw new HugeException("cannot delete service graph space %s",
                                    this.serviceGraphSpace);
        }
        this.clearGraphSpace(name);
        this.metaManager.removeGraphSpaceConfig(name);
        this.metaManager.clearGraphSpaceList(name);
        this.metaManager.notifyGraphSpaceRemove(name);
        this.graphSpaces.remove(name);
    }

    private void registerServiceToPd(String graphSpace, Service service) {
        try {
            PdRegister register = PdRegister.getInstance();
            RegisterConfig config = new RegisterConfig()
                    .setAppName(this.cluster)
                    .setGrpcAddress(this.pdPeers)
                    .setUrls(service.urls())
                    .setConsumer((Consumer<RegisterInfo>) registerInfo -> {
                        if (registerInfo.hasHeader()) {
                            Pdpb.ResponseHeader header = registerInfo.getHeader();
                            if (header.hasError()) {
                                Pdpb.ErrorType errorType = header.getError().getType();
                                if (errorType == Pdpb.ErrorType.LICENSE_ERROR
                                    || errorType == Pdpb.ErrorType.LICENSE_VERIFY_ERROR) {
                                    if (licenseValid) {
                                        LOG.warn("License check failure. {}",
                                                 header.getError().getMessage());
                                        licenseValid = false;
                                    }
                                    return;
                                } else {
                                    LOG.warn("RegisterServiceToPd Error. {}",
                                             header.getError().getMessage());
                                }
                            }
                        }
                        if (!licenseValid) {
                            LOG.warn("License is valid.");
                            licenseValid = true;
                        }
                    })
                    .setLabelMap(ImmutableMap.of(
                            PdRegisterLabel.REGISTER_TYPE.name(),
                            PdRegisterType.NODE_PORT.name(),
                            PdRegisterLabel.GRAPHSPACE.name(), graphSpace,
                            PdRegisterLabel.SERVICE_NAME.name(), service.name(),
                            PdRegisterLabel.SERVICE_ID.name(), service.serviceId(),
                            PdRegisterLabel.cores.name(),
                            String.valueOf(Runtime.getRuntime().availableProcessors())
                    )).setVersion(CoreVersion.VERSION.toString());

            String pdServiceId = register.registerService(config);
            service.pdServiceId(pdServiceId);
            LOG.info("Success to register service to pd");

        } catch (Exception e) {
            LOG.error("Failed to register service to pd", e);
        }
    }

    public void registerK8StoPd(Service service) throws Exception {
        try {
            PdRegister pdRegister = PdRegister.getInstance();
            K8sRegister k8sRegister = K8sRegister.instance();

            k8sRegister.initHttpClient();
            String rawConfig = k8sRegister.loadConfigStr();

            Gson gson = new Gson();
            ServiceDTO serviceDTO = gson.fromJson(rawConfig, ServiceDTO.class);
            RegisterConfig config = new RegisterConfig();

            String nodeName = System.getenv("MY_NODE_NAME");
            if (Strings.isNullOrEmpty(nodeName)) {
                nodeName = serviceDTO.getSpec().getClusterIP();
            }

            config
                    .setNodePort(serviceDTO.getSpec().getPorts()
                                           .get(0).getNodePort().toString())
                    .setNodeName(nodeName)
                    .setAppName(this.cluster)
                    .setGrpcAddress(this.pdPeers)
                    .setVersion(serviceDTO.getMetadata().getResourceVersion())
                    .setLabelMap(ImmutableMap.of(
                            PdRegisterLabel.REGISTER_TYPE.name(), PdRegisterType.NODE_PORT.name(),
                            PdRegisterLabel.GRAPHSPACE.name(), this.serviceGraphSpace,
                            PdRegisterLabel.SERVICE_NAME.name(), service.name(),
                            PdRegisterLabel.SERVICE_ID.name(), service.serviceId()
                    ));

            String ddsHost = this.metaManager.getDDSHost();
            if (!Strings.isNullOrEmpty(ddsHost)) {
                config.setDdsHost(ddsHost);
                //config.setDdsSlave(BrokerConfig.getInstance().isSlave());
            }
            this.pdK8sServiceId = pdRegister.registerService(config);
        } catch (Exception e) {
            LOG.error("Register service k8s external info to pd failed!", e);
            throw e;
        }
    }

    public boolean isAuth() {
        return this.graphSpace(this.serviceGraphSpace).auth();
    }

    private synchronized Map<String, Map<String, Object>> graphConfigs() {
        Map<String, Map<String, Object>> configs =
                CollectionFactory.newMap(CollectionType.EC);
        for (String graphSpace : this.graphSpaces.keySet()) {
            configs.putAll(this.metaManager.graphConfigs(graphSpace));
        }
        return configs;
    }

    private Date parseDate(Object o) {
        if (null == o) {
            return null;
        }
        String timeStr = String.valueOf(o);
        try {
            return HugeGraphSONModule.DATE_FORMAT.parse(timeStr);
        } catch (ParseException exc) {
            return null;
        }
    }

    public void loadGraphsFromLocal(Map<String, String> graphConfs) {
        for (Map.Entry<String, String> conf : graphConfs.entrySet()) {
            String name = conf.getKey();
            String graphConfPath = conf.getValue();
            HugeFactory.checkGraphName(name, "rest-server.properties");
            try {
                this.loadGraph(name, graphConfPath);
            } catch (Throwable e) {
                LOG.error("Graph '{}' can't be loaded: '{}'",
                          name, graphConfPath, e);
            }
        }
    }

    public HugeGraph cloneGraph(String graphspace, String name, String newName, Map<String,
            Object> configs) {
        /*
         * 0. check and modify params
         * 1. create graph instance
         * 2. init backend store
         * 3. inject graph and traversal source into gremlin server context
         * 4. inject graph into rest server context
         */
        String spaceGraphName = spaceGraphName(graphspace, name);
        HugeGraph sourceGraph = this.graph(spaceGraphName);
        E.checkArgumentNotNull(sourceGraph,
                               "The clone source graph '%s' doesn't exist in graphspace '%s'",
                               name, graphspace);
        E.checkArgument(StringUtils.isNotEmpty(newName),
                        "The new graph name can't be null or empty");

        String newGraphKey = spaceGraphName(graphspace, newName);
        E.checkArgument(!this.graphs.containsKey(newGraphKey),
                        "The graph '%s' has existed in graphspace '%s'", newName, graphspace);

        // Get source graph configuration
        HugeConfig cloneConfig = sourceGraph.cloneConfig(newGraphKey);

        // Convert HugeConfig to Map for processing
        Map<String, Object> newConfigs = new HashMap<>();

        // Copy all properties from cloneConfig to newConfigs
        cloneConfig.getKeys().forEachRemaining(key -> {
            newConfigs.put(key, cloneConfig.getProperty(key));
        });

        // Override with new configurations if provided
        if (configs != null && !configs.isEmpty()) {
            newConfigs.putAll(configs);
        }

        // Update store name to the new graph name
        newConfigs.put("store", newName);

        // Get creator from the configuration, fallback to "admin" if not found
        String creator = (String) newConfigs.get("creator");

        //todo: auth
        if (creator == null) {
            creator = "admin"; // default creator
        }

        Date timeStamp = new Date();
        newConfigs.put("create_time", timeStamp);
        newConfigs.put("update_time", timeStamp);

        return this.createGraph(graphspace, newName, creator, newConfigs, true);
    }

    private void loadGraph(Map<String, Map<String, Object>> graphConfigs) {
        // Load graph
        for (Map.Entry<String, Map<String, Object>> conf : graphConfigs.entrySet()) {
            String[] parts = conf.getKey().split(DELIMITER);
            // When server registered graph space is not DEFAULT, only load graphs under its
            // registered graph space
            if (this.filterLoadGraphByServiceGraphSpace(conf.getKey())) {
                continue;
            }
            Map<String, Object> config = conf.getValue();

            String creator = String.valueOf(config.get("creator"));
            Date createTime = parseDate(config.get("create_time"));
            Date updateTime = parseDate(config.get("update_time"));

            HugeFactory.checkGraphName(parts[1], "meta server");
            try {
                HugeGraph graph = this.createGraph(parts[0], parts[1],
                                                   creator, config, false);
                graph.createTime(createTime);
                graph.updateTime(updateTime);
            } catch (HugeException e) {
                if (!this.startIgnoreSingleGraphError) {
                    throw e;
                }
                LOG.error(String.format("Failed to load graph '%s' from " +
                                        "meta server", parts[1]), e);
            }
        }
    }

    private void loadGraphsFromMeta(
            Map<String, Map<String, Object>> graphConfigs) {

        Map<String, Map<String, Object>> realGraphConfigs =
                new HashMap<String, Map<String, Object>>();
        Map<String, Map<String, Object>> aliasGraphConfigs =
                new HashMap<String, Map<String, Object>>();

        for (Map.Entry<String, Map<String, Object>> conf : graphConfigs.entrySet()) {
            // When server registered graph space is not DEFAULT, only load graphs under its
            // registered graph space
            if (this.filterLoadGraphByServiceGraphSpace(conf.getKey())) {
                continue;
            }

            Map<String, Object> config = conf.getValue();
            String aliasName = (String) config.get(CoreOptions.ALIAS_NAME.name());
            if (StringUtils.isNotEmpty(aliasName)) {
                aliasGraphConfigs.put(conf.getKey(), config);
            } else {
                realGraphConfigs.put(conf.getKey(), config);
            }
        }

        // Load actual graph
        this.loadGraph(realGraphConfigs);

    }

    private boolean filterLoadGraphByServiceGraphSpace(String key) {
        String[] parts = key.split(DELIMITER);
        // server 注册的图空间不为 DEFAULT 时，只加载其注册的图空间下的图
        if (!"DEFAULT".equals(this.serviceGraphSpace) &&
            !this.serviceGraphSpace.equals(parts[0])) {
            LOG.warn(String.format("Load graph [%s] was discarded, due to the graph " +
                                   "space [%s] registered by the current server does " +
                                   "not match [%s].", key,
                                   this.serviceGraphSpace, parts[0]));
            return true;
        }
        return false;
    }

    private void checkOptions(HugeConfig config) {
        // The store cannot be the same as the existing graph
        this.checkOptionUnique(config, CoreOptions.STORE);
        /*
         * TODO: should check data path for rocksdb since can't use the same
         * data path for different graphs, but it's not easy to check here.
         */
    }

    private void checkOptionUnique(HugeConfig config,
                                   TypedOption<?, ?> option) {
        Object incomingValue = config.get(option);
        for (String graphName : this.graphs.keySet()) {
            HugeGraph graph = this.graph(graphName);
            assert graph != null;
            Object existedValue = graph.option(option);
            E.checkArgument(!incomingValue.equals(existedValue),
                            "The value '%s' of option '%s' conflicts with " +
                            "existed graph", incomingValue, option.name());
        }
    }

    public HugeGraph createGraphLocal(String name, String configText) {
        E.checkArgument(this.conf.get(ServerOptions.ENABLE_DYNAMIC_CREATE_DROP),
                        "Not allowed to create graph '%s' dynamically, " +
                        "please set `enable_dynamic_create_drop` to true.",
                        name);
        E.checkArgument(StringUtils.isNotEmpty(name),
                        "The graph name can't be null or empty");
        E.checkArgument(!this.graphs().contains(name),
                        "The graph name '%s' has existed", name);

        PropertiesConfiguration propConfig = ConfigUtil.buildConfig(configText);
        HugeConfig config = new HugeConfig(propConfig);
        this.checkOptions(config);

        return this.createGraphLocal(config, name);
    }

    private HugeGraph createGraphLocal(HugeConfig config, String name) {
        HugeGraph graph = null;
        try {
            // Create graph instance
            graph = (HugeGraph) GraphFactory.open(config);

            // Init graph and start it
            graph.create(this.graphsDir, this.globalNodeRoleInfo);
        } catch (Throwable e) {
            LOG.error("Failed to create graph '{}' due to: {}",
                      name, e.getMessage(), e);
            if (graph != null) {
                this.dropGraphLocal(graph);
            }
            throw e;
        }

        // Let gremlin server and rest server add graph to context
        this.notifyAndWaitEvent(Events.GRAPH_CREATE, graph);

        return graph;
    }

    private void dropGraphLocal(HugeGraph graph) {
        // Clear data and config files
        graph.drop();

        /*
         * Will fill graph instance into HugeFactory.graphs after
         * GraphFactory.open() succeed, remove it when the graph drops
         */
        HugeFactory.remove(graph);
    }

    public HugeGraph createGraph(String graphSpace, String name, String creator,
                                 Map<String, Object> configs, boolean init) {
        if (!usePD()) {
            // Extract nickname from configs
            String nickname;
            if (configs.get("nickname") != null) {
                nickname = configs.get("nickname").toString();
                checkNickname(nickname);
            } else {
                nickname = name;
            }

            Date timeStamp = new Date();

            // Convert Map to Properties format string
            PropertiesConfiguration propConfig = new PropertiesConfiguration();
            for (Map.Entry<String, Object> entry : configs.entrySet()) {
                propConfig.setProperty(entry.getKey(), entry.getValue());
            }
            StringWriter writer = new StringWriter();
            try {
                propConfig.write(writer);
            } catch (Exception e) {
                throw new HugeException("Failed to convert config map to properties", e);
            }

            HugeGraph graph = createGraphLocal(name, writer.toString());

            // Set metadata fields for non-PD mode
            graph.nickname(nickname);
            graph.creator(creator);
            graph.createTime(timeStamp);
            graph.updateTime(timeStamp);

            return graph;
        }

        // When the registered graph space is not DEFAULT, only the graphs within that registered
        // graph space are loaded.
        if (!"DEFAULT".equals(this.serviceGraphSpace) &&
            !this.serviceGraphSpace.equals(graphSpace)) {
            throw new HugeException(String.format(
                    "The graph space registered by the current server is " +
                    "[%s], and graph creation of the graph space [%s] is not " +
                    "accepted", this.serviceGraphSpace, graphSpace));
        }

        String key = String.join(DELIMITER, graphSpace, name);
        if (this.graphs.containsKey(key)) {
            throw new ExistedException("graph", key);
        }
        boolean grpcThread = Thread.currentThread().getName().contains("grpc");
        if (grpcThread) {
            HugeGraphAuthProxy.setAdmin();
        }
        E.checkArgumentNotNull(name, "The graph name can't be null");
        checkGraphName(name);
        String nickname;
        if (configs.get("nickname") != null) {
            nickname = configs.get("nickname").toString();
            checkNickname(nickname);
        } else {
            nickname = name;
        }

        // init = false means load graph from meta
        E.checkArgument(!init || !isExistedGraphNickname(graphSpace, nickname),
                        "Graph nickname '%s' for %s has existed",
                        nickname, graphSpace);

        GraphSpace gs = this.graphSpace(graphSpace);
        E.checkArgumentNotNull(gs, "Invalid graph space: '%s'", graphSpace);
        if (!grpcThread && init) {
            Set<String> allGraphs = this.graphs(graphSpace);
            gs.graphNumberUsed(allGraphs.size());
            if (gs.tryOfferGraph()) {
                LOG.info("The graph_number_used successfully increased to {} " +
                         "of graph space: {} for graph: {}",
                         gs.graphNumberUsed(), gs.name(), name);
            } else {
                throw new HugeException("Failed create graph due to reach " +
                                        "graph limit for graph space '%s'",
                                        graphSpace);
            }
        }

        configs.put(ServerOptions.PD_PEERS.name(), this.pdPeers);
        configs.put(CoreOptions.GRAPH_SPACE.name(), graphSpace);
        boolean auth = this.metaManager.graphSpace(graphSpace).auth();
        if (DEFAULT_GRAPH_SPACE_SERVICE_NAME.equals(graphSpace) || !auth) {
            configs.put("gremlin.graph", "org.apache.hugegraph.HugeFactory");
        } else {
            configs.put("gremlin.graph", "org.apache.hugegraph.auth.HugeFactoryAuthProxy");
        }

        configs.put("graphSpace", graphSpace);

        Date timeStamp = new Date();

        // Note: nickname was already extracted and removed from configs earlier
        configs.putIfAbsent("creator", creator);
        configs.putIfAbsent("create_time", timeStamp);
        configs.putIfAbsent("update_time", timeStamp);

        Configuration propConfig = this.buildConfig(attachLocalCacheConfig(configs));
        String storeName = propConfig.getString(CoreOptions.STORE.name());
        E.checkArgument(name.equals(storeName),
                        "The store name '%s' not match url name '%s'",
                        storeName, name);

        HugeConfig config = new HugeConfig(propConfig);
        this.checkOptions(graphSpace, config);
        HugeGraph graph = this.createGraph(graphSpace, config,
                                           this.authManager, init);
        graph.graphSpace(graphSpace);
        graph.kvStore(this.kvStore);

        graph.nickname(nickname);
        graph.creator(creator);
        graph.createTime(timeStamp);
        graph.updateTime(timeStamp);

        String graphName = spaceGraphName(graphSpace, name);
        if (init) {
            this.creatingGraphs.add(graphName);
            this.metaManager.addGraphConfig(graphSpace, name, configs);
            this.metaManager.notifyGraphAdd(graphSpace, name);
        }
        this.graphs.put(graphName, graph);
        if (!grpcThread) {
            this.metaManager.updateGraphSpaceConfig(graphSpace, gs);
        }

        // Let gremlin server and rest server context add graph
        this.eventHub.notify(Events.GRAPH_CREATE, graph);

        if (init) {
            String schema = propConfig.getString(
                    CoreOptions.SCHEMA_INIT_TEMPLATE.name());
            if (schema == null || schema.isEmpty()) {
                return graph;
            }
            String schemas = this.schemaTemplate(graphSpace, schema).schema();
            prepareSchema(graph, schemas);
        }
        if (grpcThread) {
            HugeGraphAuthProxy.resetContext();
        }
        return graph;
    }

    public Set<String> graphs() {
        return Collections.unmodifiableSet(this.graphs.keySet());
    }

    public HugeGraph graph(String spaceGraphName) {
        Graph graph = this.graphs.get(spaceGraphName);
        if (graph == null) {
            return null;
        } else if (graph instanceof HugeGraph) {
            return (HugeGraph) graph;
        }
        throw new NotSupportException("graph instance of %s", graph.getClass());
    }

    public Serializer serializer(Graph g) {
        return JsonSerializer.instance();
    }

    public Serializer serializer(Graph g, Map<String, Object> apiMeasure) {
        return JsonSerializer.instance(apiMeasure);
    }

    public void rollbackAll() {
        for (Graph graph : this.graphs.values()) {
            if (graph.features().graph().supportsTransactions() &&
                graph.tx().isOpen()) {
                graph.tx().rollback();
            }
        }
    }

    public void rollback(final Set<String> graphSourceNamesToCloseTxOn) {
        closeTx(graphSourceNamesToCloseTxOn, Transaction.Status.ROLLBACK);
    }

    public void commitAll() {
        for (Graph graph : this.graphs.values()) {
            if (graph.features().graph().supportsTransactions() &&
                graph.tx().isOpen()) {
                graph.tx().commit();
            }
        }
    }

    public void commit(final Set<String> graphSourceNamesToCloseTxOn) {
        closeTx(graphSourceNamesToCloseTxOn, Transaction.Status.COMMIT);
    }

    public boolean requireAuthentication() {
        if (this.authenticator == null) {
            return false;
        }
        return this.authenticator.requireAuthentication();
    }

    public HugeAuthenticator.User authenticate(Map<String, String> credentials)
            throws AuthenticationException {
        return this.authenticator().authenticate(credentials);
    }

    public void unauthorized(SecurityContext context) {
        this.authenticator().unauthorize(context);
    }

    public AuthManager authManager() {
        return this.authenticator().authManager();
    }

    public GlobalMasterInfo globalNodeRoleInfo() {
        return this.globalNodeRoleInfo;
    }

    public void close() {
        for (Graph graph : this.graphs.values()) {
            try {
                graph.close();
            } catch (Throwable e) {
                LOG.warn("Failed to close graph '{}'", graph, e);
            }
        }
        this.destroyRpcServer();
        this.unlistenChanges();
        if (this.roleStateMachine != null) {
            this.roleStateMachine.shutdown();
        }
    }

    private void startRpcServer() {
        if (!this.rpcServer.enabled()) {
            LOG.info("RpcServer is not enabled, skip starting rpc service");
            return;
        }

        RpcProviderConfig serverConfig = this.rpcServer.config();

        // Start auth rpc service if authenticator enabled
        if (this.authenticator != null) {
            serverConfig.addService(AuthManager.class,
                                    this.authenticator.authManager());
        }

        // Start graph rpc service if RPC_REMOTE_URL enabled
        if (this.rpcClient.enabled()) {
            RpcConsumerConfig clientConfig = this.rpcClient.config();

            for (Graph graph : this.graphs.values()) {
                HugeGraph hugegraph = (HugeGraph) graph;
                hugegraph.registerRpcServices(serverConfig, clientConfig);
            }
        }

        try {
            this.rpcServer.exportAll();
        } catch (Throwable e) {
            this.rpcServer.destroy();
            throw e;
        }
    }

    private com.alipay.remoting.rpc.RpcServer remotingRpcServer() {
        ServerConfig serverConfig = Whitebox.getInternalState(this.rpcServer,
                                                              "serverConfig");
        serverConfig.buildIfAbsent();

        // Start remote rpc server if none rpc services registered
        // Note it goes here only when raft mode enabled
        if (!serverConfig.getServer().isStarted()) {
            serverConfig.getServer().start();
        }
        return Whitebox.getInternalState(serverConfig.getServer(),
                                         "remotingServer");
    }

    private void destroyRpcServer() {
        try {
            this.rpcClient.destroy();
        } finally {
            this.rpcServer.destroy();
        }
    }

    private HugeAuthenticator authenticator() {
        E.checkState(this.authenticator != null,
                     "Unconfigured authenticator, please config " +
                     "auth.authenticator option in rest-server.properties");
        return this.authenticator;
    }

    private void closeTx(final Set<String> graphSourceNamesToCloseTxOn,
                         final Transaction.Status tx) {
        final Set<Graph> graphsToCloseTxOn = new HashSet<>();

        graphSourceNamesToCloseTxOn.forEach(name -> {
            if (this.graphs.containsKey(name)) {
                graphsToCloseTxOn.add(this.graphs.get(name));
            }
        });

        graphsToCloseTxOn.forEach(graph -> {
            if (graph.features().graph().supportsTransactions() &&
                graph.tx().isOpen()) {
                if (tx == Transaction.Status.COMMIT) {
                    graph.tx().commit();
                } else {
                    graph.tx().rollback();
                }
            }
        });
    }

    private String defaultSpaceGraphName(String graphName) {
        return spaceGraphName("DEFAULT", graphName);
    }

    private void loadGraph(String name, String graphConfPath) {
        HugeConfig config = new HugeConfig(graphConfPath);

        // Transfer `raft.group_peers` from server config to graph config
        String raftGroupPeers = this.conf.get(ServerOptions.RAFT_GROUP_PEERS);
        config.addProperty(ServerOptions.RAFT_GROUP_PEERS.name(),
                           raftGroupPeers);
        this.transferRoleWorkerConfig(config);

        Graph graph = GraphFactory.open(config);
        this.graphs.put(defaultSpaceGraphName(name), graph);

        HugeConfig graphConfig = (HugeConfig) graph.configuration();
        assert graphConfPath.equals(Objects.requireNonNull(graphConfig.file()).getPath());

        LOG.info("Graph '{}' was successfully configured via '{}'",
                 name, graphConfPath);

        if (this.requireAuthentication() &&
            !(graph instanceof HugeGraphAuthProxy)) {
            LOG.warn("You may need to support access control for '{}' with {}",
                     graphConfPath, HugeFactoryAuthProxy.GRAPH_FACTORY);
        }
    }

    private void transferRoleWorkerConfig(HugeConfig config) {
        config.setProperty(RoleElectionOptions.NODE_EXTERNAL_URL.name(),
                           this.conf.get(ServerOptions.REST_SERVER_URL));
        config.setProperty(RoleElectionOptions.BASE_TIMEOUT_MILLISECOND.name(),
                           this.conf.get(RoleElectionOptions.BASE_TIMEOUT_MILLISECOND));
        config.setProperty(RoleElectionOptions.EXCEEDS_FAIL_COUNT.name(),
                           this.conf.get(RoleElectionOptions.EXCEEDS_FAIL_COUNT));
        config.setProperty(RoleElectionOptions.RANDOM_TIMEOUT_MILLISECOND.name(),
                           this.conf.get(RoleElectionOptions.RANDOM_TIMEOUT_MILLISECOND));
        config.setProperty(RoleElectionOptions.HEARTBEAT_INTERVAL_SECOND.name(),
                           this.conf.get(RoleElectionOptions.HEARTBEAT_INTERVAL_SECOND));
        config.setProperty(RoleElectionOptions.MASTER_DEAD_TIMES.name(),
                           this.conf.get(RoleElectionOptions.MASTER_DEAD_TIMES));
    }

    private void waitGraphsReady() {
        if (!this.rpcServer.enabled()) {
            LOG.info("RpcServer is not enabled, skip wait graphs ready");
            return;
        }
        com.alipay.remoting.rpc.RpcServer remotingRpcServer = this.remotingRpcServer();
        for (String graphName : this.graphs.keySet()) {
            HugeGraph graph = this.graph(graphName);
            graph.waitReady(remotingRpcServer);
        }
    }

    private void checkBackendVersionOrExit(HugeConfig config) {
        LOG.info("Check backend version");
        for (String graph : this.graphs()) {
            // TODO: close tx from main thread
            HugeGraph hugegraph = this.graph(graph);
            assert hugegraph != null;
            if (!hugegraph.backendStoreFeatures().supportsPersistence()) {
                hugegraph.initBackend();
                if (this.requireAuthentication()) {
                    String token = config.get(ServerOptions.AUTH_ADMIN_TOKEN);
                    try {
                        this.authenticator().initAdminUser(token);
                    } catch (Exception e) {
                        throw new BackendException(
                                "The backend store of '%s' can't " +
                                "initialize admin user", hugegraph.spaceGraphName());
                    }
                }
            }
            BackendStoreInfo info = hugegraph.backendStoreInfo();
            if (!info.exists()) {
                throw new BackendException(
                        "The backend store of '%s' has not been initialized",
                        hugegraph.spaceGraphName());
            }
            if (!info.checkVersion()) {
                throw new BackendException(
                        "The backend store version is inconsistent");
            }
        }
    }

    private void initNodeRole() {
        String id = config.get(ServerOptions.SERVER_ID);
        String role = config.get(ServerOptions.SERVER_ROLE);
        E.checkArgument(StringUtils.isNotEmpty(id),
                        "The server name can't be null or empty");
        E.checkArgument(StringUtils.isNotEmpty(role),
                        "The server role can't be null or empty");

        NodeRole nodeRole = NodeRole.valueOf(role.toUpperCase());
        boolean supportRoleElection = !nodeRole.computer() &&
                                      this.supportRoleElection() &&
                                      config.get(ServerOptions.ENABLE_SERVER_ROLE_ELECTION);
        if (supportRoleElection) {
            // Init any server as Worker role, then do role election
            nodeRole = NodeRole.WORKER;
        }

        this.globalNodeRoleInfo.initNodeId(IdGenerator.of(id));
        this.globalNodeRoleInfo.initNodeRole(nodeRole);
    }

    private void serverStarted(HugeConfig conf) {
        for (String graph : this.graphs()) {
            HugeGraph hugegraph = this.graph(graph);
            assert hugegraph != null;
            hugegraph.serverStarted(this.globalNodeRoleInfo);
        }
        if (!this.globalNodeRoleInfo.nodeRole().computer() && this.supportRoleElection() &&
            config.get(ServerOptions.ENABLE_SERVER_ROLE_ELECTION)) {
            this.initRoleStateMachine();
        }
    }

    public SchemaTemplate schemaTemplate(String graphSpace,
                                         String schemaTemplate) {

        return this.metaManager.schemaTemplate(graphSpace, schemaTemplate);
    }

    private void initRoleStateMachine() {
        E.checkArgument(this.roleStateMachine == null,
                        "Repeated initialization of role state worker");
        this.globalNodeRoleInfo.supportElection(true);
        this.roleStateMachine = this.authenticator().graph().roleElectionStateMachine();
        StandardRoleListener listener = new StandardRoleListener(TaskManager.instance(),
                                                                 this.globalNodeRoleInfo);
        this.roleStateMachine.start(listener);
    }

    private boolean supportRoleElection() {
        try {
            if (!(this.authenticator() instanceof StandardAuthenticator)) {
                LOG.info("{} authenticator does not support role election currently",
                         this.authenticator().getClass().getSimpleName());
                return false;
            }
            return true;
        } catch (IllegalStateException e) {
            LOG.info("{}, does not support role election currently", e.getMessage());
            return false;
        }
    }

    private void addMetrics(HugeConfig config) {
        final MetricManager metric = MetricManager.INSTANCE;
        // Force to add a server reporter
        ServerReporter reporter = ServerReporter.instance(metric.getRegistry());
        reporter.start(60L, TimeUnit.SECONDS);

        // Add metrics for MAX_WRITE_THREADS
        int maxWriteThreads = config.get(ServerOptions.MAX_WRITE_THREADS);
        MetricsUtil.registerGauge(RestServer.class, "max-write-threads", () -> {
            return maxWriteThreads;
        });

        // Add metrics for caches
        @SuppressWarnings({"rawtypes", "unchecked"})
        Map<String, Cache<?, ?>> caches = (Map) CacheManager.instance()
                                                            .caches();
        registerCacheMetrics(caches);
        final AtomicInteger lastCachesSize = new AtomicInteger(caches.size());
        MetricsUtil.registerGauge(Cache.class, "instances", () -> {
            int count = caches.size();
            if (count != lastCachesSize.get()) {
                // Update if caches changed (effect in the next report period)
                registerCacheMetrics(caches);
                lastCachesSize.set(count);
            }
            return count;
        });

        // Add metrics for task
        MetricsUtil.registerGauge(TaskManager.class, "workers", () -> {
            return TaskManager.instance().workerPoolSize();
        });
        MetricsUtil.registerGauge(TaskManager.class, "pending-tasks", () -> {
            return TaskManager.instance().pendingTasks();
        });
    }

    private void listenChanges() {
        this.eventHub.listen(Events.GRAPH_CREATE, event -> {
            LOG.debug("RestServer accepts event '{}'", event.name());
            event.checkArgs(HugeGraph.class);
            HugeGraph graph = (HugeGraph) event.args()[0];
            this.graphs.put(graph.spaceGraphName(), graph);
            return null;
        });
        this.eventHub.listen(Events.GRAPH_DROP, event -> {
            LOG.debug("RestServer accepts event '{}'", event.name());
            event.checkArgs(HugeGraph.class);
            HugeGraph graph = (HugeGraph) event.args()[0];
            this.graphs.remove(graph.spaceGraphName());
            return null;
        });
    }

    private void unlistenChanges() {
        this.eventHub.unlisten(Events.GRAPH_CREATE);
        this.eventHub.unlisten(Events.GRAPH_DROP);
    }

    private void listenMetaChanges() {
        this.metaManager.listenGraphAdd(ConsumerWrapper.wrap(this::graphAddHandler));
        this.metaManager.listenGraphRemove(ConsumerWrapper.wrap(this::graphRemoveHandler));
        this.metaManager.listenGraphUpdate(ConsumerWrapper.wrap(this::graphUpdateHandler));
        this.metaManager.listenGraphClear(ConsumerWrapper.wrap(this::graphClearHandler));
    }

    private void notifyAndWaitEvent(String event, HugeGraph graph) {
        Future<?> future = this.eventHub.notify(event, graph);
        try {
            future.get();
        } catch (Throwable e) {
            LOG.warn("Error when waiting for event execution: {}", event, e);
        }
    }

    public void dropService(String graphSpace, String name) {
        GraphSpace gs = this.graphSpace(graphSpace);
        Service service = this.metaManager.service(graphSpace, name);
        if (null == service) {
            return;
        }
        if (service.k8s()) {
            this.k8sManager.deleteService(gs, service);
        }
        LockResult lock = this.metaManager.lock(this.cluster, graphSpace, name);
        this.metaManager.removeServiceConfig(graphSpace, name);
        this.metaManager.notifyServiceRemove(graphSpace, name);
        this.services.remove(serviceName(graphSpace, name));
        this.metaManager.unlock(lock, this.cluster, graphSpace, name);

        lock = this.metaManager.lock(this.cluster, graphSpace);
        gs.recycleResourceFor(service);
        this.metaManager.updateGraphSpaceConfig(graphSpace, gs);
        this.metaManager.notifyGraphSpaceUpdate(graphSpace);
        this.metaManager.unlock(lock, this.cluster, graphSpace);

        String pdServiceId = service.pdServiceId();
        LOG.debug("Going to unregister service {} from Pd", pdServiceId);
        if (StringUtils.isNotEmpty(pdServiceId)) {
            PdRegister register = PdRegister.getInstance();
            register.unregister(service.pdServiceId());
            LOG.debug("Service {} has been withdrew from Pd", pdServiceId);
        }
    }

    private HugeGraph createGraph(String graphSpace, HugeConfig config,
                                  AuthManager authManager, boolean init) {
        // open succeed will fill graph instance into HugeFactory graphs(map)
        HugeGraph graph;
        try {
            graph = (HugeGraph) GraphFactory.open(config);
        } catch (Throwable e) {
            LOG.error("Exception occur when open graph", e);
            throw e;
        }
        graph.graphSpace(graphSpace);
        graph.nickname(config.getString("nickname"));
        if (this.requireAuthentication()) {
            /*
             * The main purpose is to call method
             * verifyPermission(HugePermission.WRITE, ResourceType.STATUS)
             * that is private
             */
            graph.mode(GraphMode.NONE);
        }
        if (init) {
            try {
                graph.initBackend();
                graph.serverStarted(globalNodeRoleInfo);
            } catch (BackendException e) {
                try {
                    graph.close();
                } catch (Exception e1) {
                    if (graph instanceof StandardHugeGraph) {
                        ((StandardHugeGraph) graph).clearSchedulerAndLock();
                    }
                }
                HugeFactory.remove(graph);
                throw e;
            }
        }
        return graph;
    }

    /**
     * @param configs Configuration for interface graph creation or configuration obtained from pd
     *                Cache configuration priority: PD or User settings > Local settings >
     *                Default settings
     *                - If configs contain vertex/edge cache related configuration items, do not
     *                edit
     *                - If configs do not contain vertex/edge cache related configuration items,
     *                but current local configuration file contains cache related configuration
     *                items, use configuration items from configuration file
     */
    private Map<String, Object> attachLocalCacheConfig(Map<String, Object> configs) {
        Map<String, Object> attachedConfigs = new HashMap<>(configs);
        if (StringUtils.isNotEmpty((String) configs.get(CoreOptions.ALIAS_NAME.name()))) {
            return attachedConfigs;
        }
        Object value = this.config.get(CoreOptions.VERTEX_CACHE_EXPIRE);
        if (Objects.nonNull(value)) {
            attachedConfigs.putIfAbsent(CoreOptions.VERTEX_CACHE_EXPIRE.name(),
                                        String.valueOf(value));
        }
        value = this.config.get(CoreOptions.EDGE_CACHE_EXPIRE);
        if (Objects.nonNull(value)) {
            attachedConfigs.putIfAbsent(CoreOptions.EDGE_CACHE_EXPIRE.name(),
                                        String.valueOf(value));
        }
        value = this.config.get(CoreOptions.EDGE_CACHE_CAPACITY);
        if (Objects.nonNull(value)) {
            attachedConfigs.putIfAbsent(CoreOptions.EDGE_CACHE_CAPACITY.name(),
                                        String.valueOf(value));
        }
        value = this.config.get(CoreOptions.VERTEX_CACHE_CAPACITY);
        if (Objects.nonNull(value)) {
            attachedConfigs.putIfAbsent(CoreOptions.VERTEX_CACHE_CAPACITY.name(),
                                        String.valueOf(value));
        }
        value = this.config.get(CoreOptions.QUERY_TRUST_INDEX);
        if (Objects.nonNull(value)) {
            attachedConfigs.putIfAbsent(CoreOptions.QUERY_TRUST_INDEX.name(),
                                        value);
        }
        return attachedConfigs;
    }

    public Set<String> graphSpaces() {
        // Get all graph space names
        return Collections.unmodifiableSet(this.graphSpaces.keySet());
    }

    public Service service(String graphSpace, String name) {
        String key = String.join(DELIMITER, graphSpace, name);
        Service service = this.services.get(key);
        if (service == null) {
            service = this.metaManager.service(graphSpace, name);
        }
        if (service.manual()) {
            return service;
        }
        GraphSpace gs = this.graphSpace(graphSpace);
        int running = this.k8sManager.podsRunning(gs, service);
        if (service.running() != running) {
            service.running(running);
            this.metaManager.updateServiceConfig(graphSpace, service);
        }
        if (service.running() != 0) {
            service.status(Service.Status.RUNNING);
            this.metaManager.updateServiceConfig(graphSpace, service);
        }
        return service;
    }

    public Set<String> getServiceUrls(String graphSpace, String service,
                                      PdRegisterType registerType) {
        Map<String, String> configs = new HashMap<>();
        if (StringUtils.isNotEmpty(graphSpace)) {
            configs.put(PdRegisterLabel.REGISTER_TYPE.name(), graphSpace);
        }
        if (StringUtils.isNotEmpty(service)) {
            configs.put(PdRegisterLabel.SERVICE_NAME.name(), service);
        }
        configs.put(PdRegisterLabel.REGISTER_TYPE.name(), registerType.name());
        Query query = Query.newBuilder().setAppName(cluster)
                           .putAllLabels(configs)
                           .build();
        NodeInfos nodeInfos = this.pdClient.getNodeInfos(query);
        for (NodeInfo nodeInfo : nodeInfos.getInfoList()) {
            LOG.info("node app name {}, node address: {}",
                     nodeInfo.getAppName(), nodeInfo.getAddress());
        }
        return nodeInfos.getInfoList().stream()
                        .map(nodeInfo -> nodeInfo.getAddress())
                        .collect(Collectors.toSet());
    }

    public HugeGraph graph(String graphSpace, String name) {
        String key = String.join(DELIMITER, graphSpace, name);
        Graph graph = this.graphs.get(key);
        if (graph == null && usePD()) {
            Map<String, Map<String, Object>> configs =
                    this.metaManager.graphConfigs(graphSpace);
            // If current server registered graph space is not DEFAULT, only load graph creation
            // under registered graph space
            if (!configs.containsKey(key) ||
                (!"DEFAULT".equals(this.serviceGraphSpace) &&
                 !graphSpace.equals(this.serviceGraphSpace))) {
                return null;
            }
            Map<String, Object> config = configs.get(key);
            String creator = String.valueOf(config.get("creator"));
            Date createTime = parseDate(config.get("create_time"));
            Date updateTime = parseDate(config.get("update_time"));
            HugeGraph graph1 = this.createGraph(graphSpace, name,
                                                creator, config, false);
            graph1.createTime(createTime);
            graph1.updateTime(updateTime);
            this.graphs.put(key, graph1);
            return graph1;
        } else if (graph instanceof HugeGraph) {
            return (HugeGraph) graph;
        }
        throw new NotSupportException("graph instance of %s", graph.getClass());
    }

    public void dropGraphLocal(String name) {
        HugeGraph graph = this.graph(DEFAULT_GRAPH_SPACE_SERVICE_NAME + "-" + name);
        E.checkArgument(this.conf.get(ServerOptions.ENABLE_DYNAMIC_CREATE_DROP),
                        "Not allowed to drop graph '%s' dynamically, " +
                        "please set `enable_dynamic_create_drop` to true.",
                        name);
        E.checkArgumentNotNull(graph, "The graph '%s' doesn't exist", name);
        E.checkArgument(this.graphs.size() > 1,
                        "The graph '%s' is the only one, not allowed to delete",
                        name);

        this.dropGraphLocal(graph);

        // Let gremlin server and rest server context remove graph
        this.notifyAndWaitEvent(Events.GRAPH_DROP, graph);
    }

    public void dropGraph(String graphSpace, String name, boolean clear) {
        if (!usePD()) {
            dropGraphLocal(name);
            return;
        }

        boolean grpcThread = Thread.currentThread().getName().contains("grpc");
        HugeGraph g = this.graph(graphSpace, name);
        E.checkArgumentNotNull(g, "The graph '%s' doesn't exist", name);
        if (this.localGraphs.contains(name)) {
            throw new HugeException("Can't delete graph '%s' loaded from " +
                                    "local config. Please delete config file " +
                                    "and restart HugeGraphServer if really " +
                                    "want to delete it.", name);
        }

        String graphName = spaceGraphName(graphSpace, name);
        if (clear) {
            this.removingGraphs.add(graphName);
            try {
                this.metaManager.removeGraphConfig(graphSpace, name);
                this.metaManager.notifyGraphRemove(graphSpace, name);
            } catch (Exception e) {
                throw new HugeException(
                        "Failed to remove graph config of '%s'", name, e);
            }

            /**
             * close task scheduler before clear data,
             * because taskinfo stored in backend in
             * {@link org.apache.hugegraph.task.DistributedTaskScheduler}
             */
            try {
                g.taskScheduler().close();
            } catch (Throwable t) {
                LOG.warn(String.format(
                                 "Error when close TaskScheduler of %s",
                                 graphName),
                         t);
            }

            g.clearBackend();
            try {
                g.close();
            } catch (Exception e) {
                LOG.warn("Failed to close graph", e);
            }
        }
        GraphSpace gs = this.graphSpace(graphSpace);
        if (!grpcThread) {
            gs.recycleGraph();
            LOG.info("The graph_number_used successfully decreased to {} " +
                     "of graph space: {} for graph: {}",
                     gs.graphNumberUsed(), gs.name(), name);
            this.metaManager.updateGraphSpaceConfig(graphSpace, gs);
        }
        // Let gremlin server and rest server context remove graph
        LOG.info("Notify remove graph {} by GRAPH_DROP event", name);
        Graph graph = this.graphs.remove(graphName);
        if (graph != null) {
            try {
                graph.close();
            } catch (Exception e) {
                LOG.warn("Failed to close graph", e);
            }
            try {
                // Delete alias graph in HugeFactory
                HugeFactory.remove((HugeGraph) graph);
            } catch (Exception e) {
                LOG.warn("Failed to remove hugeFactory graph", e);
            }
        }
        this.eventHub.notify(Events.GRAPH_DROP, g);
    }

    private void checkOptions(String graphSpace, HugeConfig config) {
        // The store cannot be the same as the existing graph
        this.checkOptionsUnique(graphSpace, config, CoreOptions.STORE);
        // NOTE: rocksdb can't use same data path for different graph,
        // but it's not easy to check here
        String backend = config.get(CoreOptions.BACKEND);
        if (backend.equalsIgnoreCase("rocksdb")) {
            // TODO: should check data path...
        }
    }

    private void checkOptionsUnique(String graphSpace,
                                    HugeConfig config,
                                    TypedOption<?, ?> option) {
        Object incomingValue = config.get(option);
        for (Map.Entry<String, Graph> entry : this.graphs.entrySet()) {
            String[] parts = entry.getKey().split(DELIMITER);
            if (!Objects.equals(graphSpace, parts[0]) ||
                !Objects.equals(incomingValue, parts[1])) {
                continue;
            }
            Object existedValue = ((HugeGraph) entry.getValue()).option(option);
            E.checkArgument(!incomingValue.equals(existedValue),
                            "The option '%s' conflict with existed",
                            option.name());
        }
    }

    public Set<String> graphs(String graphSpace) {
        Set<String> graphs = new HashSet<>();

        if (!usePD()) {
            for (String key : this.graphs.keySet()) {
                String[] parts = key.split(DELIMITER);
                if (parts[0].equals(graphSpace)) {
                    graphs.add(parts[1]);
                }
            }
            return graphs;
        }

        for (String key : this.metaManager.graphConfigs(graphSpace).keySet()) {
            graphs.add(key.split(DELIMITER)[1]);
        }
        return graphs;
    }

    public GraphSpace graphSpace(String name) {
        if (!usePD()) {
            return new GraphSpace("DEFAULT");
        }
        GraphSpace space = this.graphSpaces.get(name);
        if (space == null) {
            space = this.metaManager.graphSpace(name);
        }
        return space;
    }

    public Serializer serializer() {
        return JsonSerializer.instance();
    }

    public boolean isExistedSpaceNickname(String space, String nickname) {
        if (StringUtils.isEmpty(nickname)) {
            return false;
        }
        Set<String> graphSpaces = this.graphSpaces();
        for (String graphSpace : graphSpaces) {
            GraphSpace gs = this.graphSpace(graphSpace);
            // when update space, return true if nickname exists in other space
            if (nickname.equals(gs.nickname()) && !graphSpace.equals(space)) {
                return true;
            }
        }
        return false;
    }

    public boolean isExistedGraphNickname(String graphSpace, String nickname) {
        if (StringUtils.isEmpty(nickname)) {
            return false;
        }
        for (Map<String, Object> graphConfig :
                this.metaManager.graphConfigs(graphSpace).values()) {
            if (nickname.equals(graphConfig.get("nickname").toString())) {
                return true;
            }
        }
        return false;
    }

    private MapConfiguration buildConfig(Map<String, Object> configs) {
        return new MapConfiguration(configs);
    }

    public void graphReadMode(String graphSpace, String graphName,
                              GraphReadMode readMode) {

        if (!usePD()) {
            HugeGraph g = this.graph(spaceGraphName(graphSpace, graphName));
            g.readMode(readMode);
            return;
        }

        try {
            Map<String, Object> configs =
                    this.metaManager.getGraphConfig(graphSpace, graphName);
            configs.put(CoreOptions.GRAPH_READ_MODE.name(), readMode);
            this.metaManager.updateGraphConfig(graphSpace, graphName, configs);
            this.metaManager.notifyGraphUpdate(graphSpace, graphName);
        } catch (Exception e) {
            LOG.warn("The graph not exist or local graph");
        }
    }

    public Map<String, Object> graphConfig(String graphSpace,
                                           String graphName) {
        return this.metaManager.getGraphConfig(graphSpace, graphName);
    }

    public String pdPeers() {
        return this.pdPeers;
    }

    public String cluster() {
        return this.cluster;
    }

    private enum PdRegisterType {
        NODE_PORT,
        DDS
    }

    private enum PdRegisterLabel {
        REGISTER_TYPE,
        GRAPHSPACE,
        SERVICE_NAME,
        SERVICE_ID,
        cores
    }

    public static class ConsumerWrapper<T> implements Consumer<T> {

        private final Consumer<T> consumer;

        private ConsumerWrapper(Consumer<T> consumer) {
            this.consumer = consumer;
        }

        public static ConsumerWrapper wrap(Consumer consumer) {
            return new ConsumerWrapper(consumer);
        }

        @Override
        public void accept(T t) {
            boolean grpcThread = false;
            try {
                grpcThread = Thread.currentThread().getName().contains("grpc");
                if (grpcThread) {
                    HugeGraphAuthProxy.setAdmin();
                }
                consumer.accept(t);
            } catch (Throwable e) {
                LOG.error("Listener exception occurred.", e);
            } finally {
                if (grpcThread) {
                    HugeGraphAuthProxy.resetContext();
                }
            }
        }
    }

    private <T> void graphAddHandler(T response) {
        List<String> names = this.metaManager
                .extractGraphsFromResponse(response);
        for (String graphName : names) {
            String[] parts = graphName.split(DELIMITER);
            if (parts.length < 2) {
                LOG.error("The graph name format is incorrect: {}", graphName);
                continue;
            }
            // If the current server is not registered to the DEFAULT schema,
            // it will only receive graph creation events under the registered schemas.
            if (!"DEFAULT".equals(this.serviceGraphSpace) &&
                !parts[0].equals(this.serviceGraphSpace)) {
                LOG.warn(String.format("Listen event: graph [%s] add was discarded because " +
                                       "it did not belong to the graph space [%s] " +
                                       "registered by the current server",
                                       graphName, this.serviceGraphSpace));
                // TODO: further confirmation is required
                // should be completely ignored or require additional processing
                continue;
            }
            LOG.info("Accept graph add signal from etcd for {}", graphName);
            if (this.graphs.containsKey(graphName) ||
                this.creatingGraphs.contains(graphName)) {
                this.creatingGraphs.remove(graphName);
                continue;
            }

            LOG.info("Not exist in cache, Starting construct graph {}",
                     graphName);
            Map<String, Object> config =
                    this.metaManager.getGraphConfig(parts[0], parts[1]);
            if (config == null) {
                LOG.error("The graph config not exist: {}", graphName);
                continue;
            }
            Object objc = config.get("creator");
            String creator = null == objc ?
                             GraphSpace.DEFAULT_CREATOR_NAME :
                             String.valueOf(objc);

            // Create graph without init
            try {
                HugeGraph graph;
                // TODO: add alias graph
                graph = this.createGraph(parts[0], parts[1], creator, config, false);
                LOG.info("Add graph space:{} graph:{}", parts[0], parts[1]);
                // TODO: use a more secure method to determine administrator privileges
                boolean grpcThread = Thread.currentThread().getName().contains("grpc");
                if (grpcThread) {
                    HugeGraphAuthProxy.setAdmin();
                }
                graph.started(true);
                if (graph.tx().isOpen()) {
                    graph.tx().close();
                }
            } catch (HugeException e) {
                if (!this.startIgnoreSingleGraphError) {
                    throw e;
                }
                LOG.error(String.format(
                        "Failed to create graph '%s'", graphName), e);
            }
        }
    }

    private <T> void graphRemoveHandler(T response) {
        List<String> graphNames = this.metaManager
                .extractGraphsFromResponse(response);
        for (String graphName : graphNames) {
            if (!this.graphs.containsKey(graphName) ||
                this.removingGraphs.contains(graphName)) {
                this.removingGraphs.remove(graphName);
                continue;
            }

            // Remove graph without clear
            String[] parts = graphName.split(DELIMITER);
            if (parts.length < 2) {
                LOG.error("The graph name format is incorrect: {}", graphName);
                continue;
            }
            try {
                this.dropGraph(parts[0], parts[1], false);
            } catch (HugeException e) {
                LOG.error(String.format(
                        "Failed to drop graph '%s'", graphName), e);
            }
        }
    }

    private <T> void graphUpdateHandler(T response) {
        List<String> graphNames = this.metaManager
                .extractGraphsFromResponse(response);
        for (String graphName : graphNames) {
            if (this.graphs.containsKey(graphName)) {
                Graph graph = this.graphs.get(graphName);
                if (graph instanceof HugeGraph) {
                    HugeGraph hugeGraph = (HugeGraph) graph;
                    String[] values =
                            graphName.split(MetaManager.META_PATH_JOIN);
                    if (values.length < 2) {
                        LOG.error("The graph name format is incorrect: {}", graphName);
                        continue;
                    }
                    String graphSpace = values[0];
                    String graphNameInSpace = values[1];
                    Map<String, Object> configs =
                            this.metaManager.getGraphConfig(graphSpace,
                                                            graphNameInSpace);
                    String readMode = configs.getOrDefault(
                            CoreOptions.GRAPH_READ_MODE.name(),
                            CoreOptions.GRAPH_READ_MODE.defaultValue()).toString();
                    hugeGraph.readMode(GraphReadMode.valueOf(readMode));
                    // TODO: Implement alias graph update logic after
                    // alias graph feature is fully designed and implemented
                    // this.updateAliasGraph(hugeGraph, configs);
                    LOG.info("Update graph space:{} graph:{}", values[0], values[1]);
                }
            }
        }
    }

    private <T> void graphClearHandler(T response) {
        List<String> graphNames = this.metaManager
                .extractGraphsFromResponse(response);
        for (String graphName : graphNames) {
            if (this.graphs.containsKey(graphName)) {
                Graph graph = this.graphs.get(graphName);
                if (graph instanceof HugeGraph) {
                    HugeGraph hugeGraph = (HugeGraph) graph;
                    ((AbstractBackendStoreProvider) hugeGraph.storeProvider())
                            .notifyAndWaitEvent(Events.STORE_CLEAR);
                }
            }
        }
    }

}
