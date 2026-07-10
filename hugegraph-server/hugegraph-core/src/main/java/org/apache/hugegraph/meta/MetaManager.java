/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.meta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.auth.HugeAccess;
import org.apache.hugegraph.auth.HugeBelong;
import org.apache.hugegraph.auth.HugeGroup;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.auth.HugeProject;
import org.apache.hugegraph.auth.HugeRole;
import org.apache.hugegraph.auth.HugeTarget;
import org.apache.hugegraph.auth.HugeUser;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.meta.lock.LockResult;
import org.apache.hugegraph.meta.managers.AuthMetaManager;
import org.apache.hugegraph.meta.managers.ConfigMetaManager;
import org.apache.hugegraph.meta.managers.GraphMetaManager;
import org.apache.hugegraph.meta.managers.KafkaMetaManager;
import org.apache.hugegraph.meta.managers.LockMetaManager;
import org.apache.hugegraph.meta.managers.SchemaMetaManager;
import org.apache.hugegraph.meta.managers.SchemaTemplateMetaManager;
import org.apache.hugegraph.meta.managers.ServiceMetaManager;
import org.apache.hugegraph.meta.managers.SpaceMetaManager;
import org.apache.hugegraph.meta.managers.TaskMetaManager;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.space.SchemaTemplate;
import org.apache.hugegraph.space.Service;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableMap;

public class MetaManager {

    private static final Logger LOG = Log.logger(MetaManager.class);

    public static final String META_PATH_DELIMITER = "/";
    public static final String META_PATH_JOIN = "-";

    public static final String META_PATH_HUGEGRAPH = "HUGEGRAPH";
    public static final String META_PATH_GRAPHSPACE = "GRAPHSPACE";
    public static final String META_PATH_GRAPHSPACE_LIST = "GRAPHSPACE_LIST";
    public static final String META_PATH_SYS_GRAPH_CONF = "SYS_GRAPH_CONF";
    public static final String META_PATH_DEFAULT_GS = "DEFAULT";
    public static final String META_PATH_SERVICE = "SERVICE";
    public static final String META_PATH_SERVICE_CONF = "SERVICE_CONF";
    public static final String META_PATH_GRAPH_CONF = "GRAPH_CONF";
    public static final String META_PATH_CONF = "CONF";
    public static final String META_PATH_GRAPH = "GRAPH";
    public static final String META_PATH_SCHEMA = "SCHEMA";
    public static final String META_PATH_PROPERTY_KEY = "PROPERTY_KEY";
    public static final String META_PATH_VERTEX_LABEL = "VERTEX_LABEL";
    public static final String META_PATH_EDGE_LABEL = "EDGE_LABEL";
    public static final String META_PATH_INDEX_LABEL = "INDEX_LABEL";
    public static final String META_PATH_NAME = "NAME";
    public static final String META_PATH_ID = "ID";
    public static final String META_PATH_AUTH = "AUTH";
    public static final String META_PATH_USER = "USER";
    public static final String META_PATH_GROUP = "GROUP";
    public static final String META_PATH_ROLE = "ROLE";
    public static final String META_PATH_TARGET = "TARGET";
    public static final String META_PATH_BELONG = "BELONG";
    public static final String META_PATH_ACCESS = "ACCESS";
    public static final String META_PATH_PROJECT = "PROJECT";
    public static final String META_PATH_K8S_BINDINGS = "BINDING";
    public static final String META_PATH_REST_PROPERTIES = "REST_PROPERTIES";
    public static final String META_PATH_GREMLIN_YAML = "GREMLIN_YAML";
    public static final String META_PATH_SCHEMA_TEMPLATE = "SCHEMA_TEMPLATE";
    public static final String META_PATH_TASK = "TASK";
    public static final String META_PATH_TASK_LOCK = "TASK_LOCK";
    public static final String META_PATH_AUTH_EVENT = "AUTH_EVENT";
    public static final String META_PATH_EVENT = "EVENT";
    public static final String META_PATH_ADD = "ADD";
    public static final String META_PATH_REMOVE = "REMOVE";
    public static final String META_PATH_UPDATE = "UPDATE";
    public static final String META_PATH_CLEAR = "CLEAR";
    public static final String META_PATH_DDS = "DDS_HOST";
    public static final String META_PATH_METRICS = "METRICS";
    public static final String META_PATH_KAFKA = "KAFKA";
    public static final String META_PATH_HOST = "BROKER_HOST";
    public static final String META_PATH_PORT = "BROKER_PORT";
    public static final String META_PATH_PARTITION_COUNT = "PARTITION_COUNT";
    public static final String META_PATH_DATA_SYNC_ROLE = "DATA_SYNC_ROLE";
    public static final String META_PATH_SLAVE_SERVER_HOST = "SLAVE_SERVER_HOST";
    public static final String META_PATH_SLAVE_SERVER_PORT = "SLAVE_SERVER_PORT";
    public static final String META_PATH_SYNC_BROKER = "SYNC_BROKER";
    public static final String META_PATH_SYNC_STORAGE = "SYNC_STORAGE";
    public static final String META_PATH_KAFKA_FILTER = "KAFKA-FILTER";
    public static final String META_PATH_WHITE_IP_LIST = "WHITE_IP_LIST";
    public static final String META_PATH_WHITE_IP_STATUS = "WHITE_IP_STATUS";
    public static final long LOCK_DEFAULT_LEASE = 30L;
    public static final long LOCK_DEFAULT_TIMEOUT = 10L;
    public static final int RANDOM_USER_ID = 100;
    private static final String SCHEMA_CACHE_CLEAR_GRAPH_KEY = "graph";
    private static final String SCHEMA_CACHE_CLEAR_SOURCE_KEY = "source";
    private static final String META_PATH_URLS = "URLS";
    private static final String META_PATH_PD_PEERS = "HSTORE_PD_PEERS";
    private static final MetaManager INSTANCE = new MetaManager();
    private MetaDriver metaDriver;
    private String cluster;
    private AuthMetaManager authMetaManager;
    private GraphMetaManager graphMetaManager;
    private SchemaMetaManager schemaMetaManager;
    private ServiceMetaManager serviceMetaManager;
    private SpaceMetaManager spaceMetaManager;
    private TaskMetaManager taskMetaManager;
    private ConfigMetaManager configMetaManager;
    private KafkaMetaManager kafkaMetaManager;
    private SchemaTemplateMetaManager schemaTemplateManager;
    private LockMetaManager lockMetaManager;

    private MetaManager() {
    }

    public static MetaManager instance() {
        return INSTANCE;
    }

    public synchronized boolean isReady() {
        return null != this.metaDriver;
    }

    public String cluster() {
        return this.cluster;
    }

    public synchronized void connect(String cluster, MetaDriverType type,
                                     String trustFile, String clientCertFile,
                                     String clientKeyFile, Object... args) {
        E.checkArgument(cluster != null && !cluster.isEmpty(),
                        "The cluster can't be null or empty");
        if (this.metaDriver == null) {
            this.cluster = cluster;

            switch (type) {
                case ETCD:
                    this.metaDriver = trustFile == null || trustFile.isEmpty() ?
                                      new EtcdMetaDriver(args) :
                                      new EtcdMetaDriver(trustFile,
                                                         clientCertFile,
                                                         clientKeyFile, args);
                    break;
                case PD:
                    assert args.length > 0;
                    // FIXME: assume pd.peers is urls separated by commas in a string
                    //        like `127.0.0.1:8686,127.0.0.1:8687,127.0.0.1:8688`
                    this.metaDriver = new PdMetaDriver((String) args[0]);
                    break;
                default:
                    throw new AssertionError(String.format(
                            "Invalid meta driver type: %s", type));
            }
        }
        this.initManagers(this.cluster);
    }

    private void initManagers(String cluster) {
        this.authMetaManager = new AuthMetaManager(this.metaDriver, cluster);
        this.graphMetaManager = new GraphMetaManager(this.metaDriver, cluster);
        this.schemaMetaManager = new SchemaMetaManager(this.metaDriver, cluster, null);
        this.serviceMetaManager = new ServiceMetaManager(this.metaDriver, cluster);
        this.spaceMetaManager = new SpaceMetaManager(this.metaDriver, cluster);
        this.taskMetaManager = new TaskMetaManager(this.metaDriver, cluster);
        this.configMetaManager = new ConfigMetaManager(this.metaDriver, cluster);
        this.kafkaMetaManager = new KafkaMetaManager(this.metaDriver, cluster);
        this.schemaTemplateManager = new SchemaTemplateMetaManager(this.metaDriver, cluster);
        this.lockMetaManager = new LockMetaManager(this.metaDriver, cluster);
    }

    public <T> void listenGraphSpaceAdd(Consumer<T> consumer) {
        this.spaceMetaManager.listenGraphSpaceAdd(consumer);
    }

    public <T> void listenGraphSpaceRemove(Consumer<T> consumer) {
        this.spaceMetaManager.listenGraphSpaceRemove(consumer);
    }

    public <T> void listenGraphSpaceUpdate(Consumer<T> consumer) {
        this.spaceMetaManager.listenGraphSpaceUpdate(consumer);
    }

    public void notifyGraphSpaceAdd(String graphSpace) {
        this.spaceMetaManager.notifyGraphSpaceAdd(graphSpace);
    }

    public void notifyGraphSpaceRemove(String graphSpace) {
        this.spaceMetaManager.notifyGraphSpaceRemove(graphSpace);
    }

    public void notifyGraphSpaceUpdate(String graphSpace) {
        this.spaceMetaManager.notifyGraphSpaceUpdate(graphSpace);
    }

    public <T> void listenServiceAdd(Consumer<T> consumer) {
        this.serviceMetaManager.listenServiceAdd(consumer);
    }

    public <T> void listenServiceRemove(Consumer<T> consumer) {
        this.serviceMetaManager.listenServiceRemove(consumer);
    }

    public <T> void listenServiceUpdate(Consumer<T> consumer) {
        this.serviceMetaManager.listenServiceUpdate(consumer);
    }

    public <T> void listenGraphAdd(Consumer<T> consumer) {
        this.graphMetaManager.listenGraphAdd(consumer);
    }

    public <T> void listenGraphUpdate(Consumer<T> consumer) {
        this.graphMetaManager.listenGraphUpdate(consumer);
    }

    public <T> void listenGraphRemove(Consumer<T> consumer) {
        this.graphMetaManager.listenGraphRemove(consumer);
    }

    public <T> void listenGraphClear(Consumer<T> consumer) {
        this.graphMetaManager.listenGraphClear(consumer);
    }

    public <T> void listenSchemaCacheClear(Consumer<T> consumer) {
        this.graphMetaManager.listenSchemaCacheClear(consumer);
    }

    public <T> void listenGraphCacheClear(Consumer<T> consumer) {
        this.graphMetaManager.listenGraphCacheClear(consumer);
    }

    /**
     * Listen for vertex label changes, graph vertex cache clear
     *
     * @param consumer
     * @param <T>
     */
    public <T> void listenGraphVertexCacheClear(Consumer<T> consumer) {
        this.graphMetaManager.listenGraphVertexCacheClear(consumer);
    }

    /**
     * Listen to edge label changes, graph edge cache clear
     *
     * @param consumer
     * @param <T>
     */
    public <T> void listenGraphEdgeCacheClear(Consumer<T> consumer) {
        this.graphMetaManager.listenGraphEdgeCacheClear(consumer);
    }

    public <T> void listenRestPropertiesUpdate(String graphSpace,
                                               String serviceId,
                                               Consumer<T> consumer) {
        this.configMetaManager.listenRestPropertiesUpdate(graphSpace,
                                                          serviceId,
                                                          consumer);
    }

    public <T> void listenGremlinYamlUpdate(String graphSpace,
                                            String serviceId,
                                            Consumer<T> consumer) {
        this.configMetaManager.listenGremlinYamlUpdate(graphSpace,
                                                       serviceId,
                                                       consumer);
    }

    public <T> void listenAuthEvent(Consumer<T> consumer) {
        this.authMetaManager.listenAuthEvent(consumer);
    }

    private void putAuthEvent(AuthEvent event) {
        this.authMetaManager.putAuthEvent(event);
    }

    public <T> void listenKafkaConfig(Consumer<T> consumer) {
        this.kafkaMetaManager.listenKafkaConfig(consumer);
    }

    public String kafkaGetRaw(String key) {
        return this.kafkaMetaManager.getRaw(key);
    }

    public void kafkaPutOrDeleteRaw(String key, String val) {
        this.kafkaMetaManager.putOrDeleteRaw(key, val);
    }

    public Map<String, GraphSpace> graphSpaceConfigs() {
        return this.spaceMetaManager.graphSpaceConfigs();
    }

    public Map<String, Service> serviceConfigs(String graphSpace) {
        return this.serviceMetaManager.serviceConfigs(graphSpace);
    }

    public Map<String, Map<String, Object>> graphConfigs(String graphSpace) {
        return this.graphMetaManager.graphConfigs(graphSpace);
    }

    public Set<String> schemaTemplates(String graphSpace) {
        return this.schemaTemplateManager.schemaTemplates(graphSpace);
    }

    @SuppressWarnings("unchecked")
    public SchemaTemplate schemaTemplate(String graphSpace,
                                         String schemaTemplate) {
        return this.schemaTemplateManager.schemaTemplate(graphSpace,
                                                         schemaTemplate);
    }

    public void addSchemaTemplate(String graphSpace, SchemaTemplate template) {
        this.schemaTemplateManager.addSchemaTemplate(graphSpace, template);
    }

    public void updateSchemaTemplate(String graphSpace,
                                     SchemaTemplate template) {
        this.schemaTemplateManager.updateSchemaTemplate(graphSpace, template);
    }

    public void removeSchemaTemplate(String graphSpace, String name) {
        this.schemaTemplateManager.removeSchemaTemplate(graphSpace, name);
    }

    public void clearSchemaTemplate(String graphSpace) {
        this.schemaTemplateManager.clearSchemaTemplate(graphSpace);
    }

    public String extractGraphSpaceFromKey(String key) {
        String[] parts = key.split(META_PATH_DELIMITER);
        if (parts.length < 4) {
            return null;
        }
        if (parts[3].equals(META_PATH_CONF)) {
            return parts.length < 5 ? null : parts[4];
        }
        return parts[3];
    }

    public List<String> extractGraphFromKey(String key) {
        String[] parts = key.split(META_PATH_DELIMITER);
        if (parts.length < 6) {
            return Collections.EMPTY_LIST;
        }
        return Arrays.asList(parts[3], parts[5]);
    }

    public <T> List<String> extractGraphSpacesFromResponse(T response) {
        return this.metaDriver.extractValuesFromResponse(response);
    }

    public <T> List<String> extractServicesFromResponse(T response) {
        return this.metaDriver.extractValuesFromResponse(response);
    }

    public <T> List<String> extractGraphsFromResponse(T response) {
        return this.metaDriver.extractValuesFromResponse(response);
    }

    public <T> List<SchemaCacheClearEvent> extractSchemaCacheClearEventsFromResponse(
            T response) {
        List<String> values = this.metaDriver.extractValuesFromResponse(response);
        if (values == null) {
            return null;
        }

        List<SchemaCacheClearEvent> events = new ArrayList<>(values.size());
        for (String value : values) {
            SchemaCacheClearEvent event = SchemaCacheClearEvent.fromValue(value);
            if (event != null) {
                events.add(event);
            }
        }
        return events;
    }

    public <T> Map<String, String> extractKVFromResponse(T response) {
        return this.metaDriver.extractKVFromResponse(response);
    }

    public GraphSpace getGraphSpaceConfig(String graphSpace) {
        return this.spaceMetaManager.getGraphSpaceConfig(graphSpace);
    }

    public String getServiceRawConfig(String graphSpace, String service) {
        return this.serviceMetaManager.getServiceRawConfig(graphSpace, service);
    }

    public Service parseServiceRawConfig(String serviceRawConf) {
        return this.serviceMetaManager.parseServiceRawConfig(serviceRawConf);
    }

    public Service getServiceConfig(String graphSpace, String service) {
        return this.serviceMetaManager.getServiceConfig(graphSpace, service);
    }

    public Map<String, Object> getGraphConfig(String graphSpace, String graph) {
        return this.graphMetaManager.getGraphConfig(graphSpace, graph);
    }

    public void addGraphConfig(String graphSpace, String graph,
                               Map<String, Object> configs) {
        this.graphMetaManager.addGraphConfig(graphSpace, graph, configs);
    }

    public void updateGraphConfig(String graphSpace, String graph,
                                  Map<String, Object> configs) {
        this.graphMetaManager.updateGraphConfig(graphSpace, graph, configs);
    }

    public void addSysGraphConfig(Map<String, Object> configs) {
        this.graphMetaManager.addSysGraphConfig(configs);
    }

    public Map<String, Object> getSysGraphConfig() {
        return this.graphMetaManager.getSysGraphConfig();
    }

    public void removeSysGraphConfig() {
        this.graphMetaManager.removeSysGraphConfig();
    }

    public GraphSpace graphSpace(String name) {
        return this.spaceMetaManager.graphSpace(name);
    }

    public void addGraphSpaceConfig(String name, GraphSpace space) {
        this.spaceMetaManager.addGraphSpaceConfig(name, space);
    }

    public void removeGraphSpaceConfig(String name) {
        this.spaceMetaManager.removeGraphSpaceConfig(name);
    }

    public void updateGraphSpaceConfig(String name, GraphSpace space) {
        this.spaceMetaManager.updateGraphSpaceConfig(name, space);
    }

    public void appendGraphSpaceList(String name) {
        this.spaceMetaManager.appendGraphSpaceList(name);
    }

    public void clearGraphSpaceList(String name) {
        this.spaceMetaManager.clearGraphSpaceList(name);
    }

    public void notifyServiceAdd(String graphSpace, String name) {
        this.serviceMetaManager.notifyServiceAdd(graphSpace, name);
    }

    public void notifyServiceRemove(String graphSpace, String name) {
        this.serviceMetaManager.notifyServiceRemove(graphSpace, name);
    }

    public void notifyServiceUpdate(String graphSpace, String name) {
        this.serviceMetaManager.notifyServiceUpdate(graphSpace, name);
    }

    public Service service(String graphSpace, String name) {
        return this.serviceMetaManager.service(graphSpace, name);
    }

    public void addServiceConfig(String graphSpace, Service service) {
        this.serviceMetaManager.addServiceConfig(graphSpace, service);
    }

    public void removeServiceConfig(String graphSpace, String service) {
        this.serviceMetaManager.removeServiceConfig(graphSpace, service);
    }

    public void updateServiceConfig(String graphSpace, Service service) {
        this.addServiceConfig(graphSpace, service);
    }

    public void removeGraphConfig(String graphSpace, String graph) {
        this.graphMetaManager.removeGraphConfig(graphSpace, graph);
    }

    public void notifyGraphAdd(String graphSpace, String graph) {
        this.graphMetaManager.notifyGraphAdd(graphSpace, graph);
    }

    public void notifyGraphRemove(String graphSpace, String graph) {
        this.graphMetaManager.notifyGraphRemove(graphSpace, graph);
    }

    public void notifyGraphUpdate(String graphSpace, String graph) {
        this.graphMetaManager.notifyGraphUpdate(graphSpace, graph);
    }

    public void notifyGraphClear(String graphSpace, String graph) {
        this.graphMetaManager.notifyGraphClear(graphSpace, graph);
    }

    public void notifySchemaCacheClear(String graphSpace, String graph) {
        this.notifySchemaCacheClear(graphSpace, graph, null);
    }

    public void notifySchemaCacheClear(String graphSpace, String graph,
                                       String source) {
        this.graphMetaManager.notifySchemaCacheClear(graphSpace, graph, source);
    }

    public void notifyGraphCacheClear(String graphSpace, String graph) {
        this.graphMetaManager.notifyGraphCacheClear(graphSpace, graph);
    }

    /**
     * Notice Requires graph vertex cache clear
     *
     * @param graphSpace
     * @param graph
     */
    public void notifyGraphVertexCacheClear(String graphSpace, String graph) {
        this.graphMetaManager.notifyGraphVertexCacheClear(graphSpace, graph);
    }

    /**
     * Notice Requires graph edge cache clear
     *
     * @param graphSpace
     * @param graph
     */
    public void notifyGraphEdgeCacheClear(String graphSpace, String graph) {
        this.graphMetaManager.notifyGraphEdgeCacheClear(graphSpace, graph);
    }

    public LockResult lock(String... keys) {
        return this.lockMetaManager.lock(keys);
    }

    public LockResult tryLock(String key) {
        return this.lockMetaManager.tryLock(key);
    }

    public void unlock(LockResult lockResult, String... keys) {
        this.lockMetaManager.unlock(lockResult, keys);
    }

    public void unlock(String key, LockResult lockResult) {
        this.lockMetaManager.unlock(key, lockResult);
    }

    public String belongId(String userName, String roleName) {
        return this.authMetaManager.belongId(userName, roleName, HugeBelong.UR);
    }

    public String belongId(String source, String target, String link) {
        return this.authMetaManager.belongId(source, target, link);
    }

    public String accessId(String roleName, String targetName,
                           HugePermission permission) {
        return this.authMetaManager.accessId(roleName, targetName, permission);
    }

    private String graphSpaceBindingsServer(String name, BindingType type) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/CONF/{graphspace}
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           name,
                           META_PATH_K8S_BINDINGS,
                           type.name(),
                           META_PATH_URLS);
    }

    /**
     * Get DDS (eureka) host, format should be "ip:port", with no /
     *
     * @return
     */
    private String ddsHostKey() {
        // HUGEGRAPH/{cluster}/DDS_HOST
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_DDS);
    }

    private String hugeClusterRoleKey() {
        // HUGEGRAPH/{clusterRole}/KAFKA/DATA_SYNC_ROLE
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_KAFKA,
                           META_PATH_DATA_SYNC_ROLE);
    }

    private String kafkaHostKey() {
        // HUGEGRAPH/{cluster}/KAFKA/BROKER_HOST
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_KAFKA,
                           META_PATH_HOST);
    }

    private String kafkaPortKey() {
        // HUGEGRAPH/{cluster}/KAFKA/BROKER_PORT
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_KAFKA,
                           META_PATH_PORT);
    }

    private String kafkaPartitionCountKey() {
        // HUGEGRAPH/{cluster}/KAFKA/PARTITION_COUNT
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_KAFKA,
                           META_PATH_PARTITION_COUNT);
    }

    private String kafkaSlaveHostKey() {
        // HUGEGRAPH/{cluster}/KAFKA/SLAVE_SERVER_HOST
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_KAFKA,
                           META_PATH_SLAVE_SERVER_HOST);
    }

    private String kafkaSlavePortKey() {
        // HUGEGRAPH/{cluster}/KAFKA/SLAVE_SERVER_PORT
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_KAFKA,
                           META_PATH_SLAVE_SERVER_PORT);
    }

    public String kafkaSyncBrokerKey() {
        // HUGEGRAPH/{cluster}/KAFKA/SYNC_BROKER
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_KAFKA,
                           META_PATH_SYNC_BROKER);
    }

    public String kafkaSyncStorageKey() {
        // HUGEGRAPH/{cluster}/KAFKA/SYNC_STORAGE
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_KAFKA,
                           META_PATH_SYNC_STORAGE);
    }

    public String kafkaFilterGraphspaceKey() {
        // HUGEGRAPH/{cluster}/KAFKA-FILTER/GRAPHSPACE
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_KAFKA_FILTER,
                           META_PATH_GRAPHSPACE);
    }

    public String kafkaFilterGraphKey() {
        // HUGEGRAPH/{cluster}/KAFKA-FILTER/FILTER/GRAPH
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_KAFKA_FILTER,
                           META_PATH_GRAPH);
    }

    private String whiteIpListKey() {
        // HUGEGRAPH/{cluster}/WHITE_IP_LIST
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_WHITE_IP_LIST);
    }

    private String whiteIpStatusKey() {
        // HUGEGRAPH/{cluster}/WHITE_IP_STATUS
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_WHITE_IP_STATUS);
    }

    private String hstorePDPeersKey() {
        // HUGEGRAPH/{cluster}/META_PATH_PD_PEERS
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_PD_PEERS);
    }

    public Id addPropertyKey(String graphSpace, String graph,
                             PropertyKey propertyKey) {
        this.schemaMetaManager.addPropertyKey(graphSpace, graph, propertyKey);
        return IdGenerator.ZERO;
    }

    public void updatePropertyKey(String graphSpace, String graph,
                                  PropertyKey pkey) {
        this.schemaMetaManager.updatePropertyKey(graphSpace, graph, pkey);
    }

    public PropertyKey getPropertyKey(String graphSpace, String graph,
                                      Id propertyKey) {
        return this.schemaMetaManager.getPropertyKey(graphSpace, graph,
                                                     propertyKey);
    }

    public PropertyKey getPropertyKey(String graphSpace, String graph,
                                      String propertyKey) {
        return this.schemaMetaManager.getPropertyKey(graphSpace, graph,
                                                     propertyKey);
    }

    public List<PropertyKey> getPropertyKeys(String graphSpace, String graph) {
        return this.schemaMetaManager.getPropertyKeys(graphSpace, graph);
    }

    public Id removePropertyKey(String graphSpace, String graph,
                                Id propertyKey) {
        return this.schemaMetaManager.removePropertyKey(graphSpace, graph,
                                                        propertyKey);
    }

    public void addVertexLabel(String graphSpace, String graph,
                               VertexLabel vertexLabel) {
        this.schemaMetaManager.addVertexLabel(graphSpace, graph, vertexLabel);
    }

    public void updateVertexLabel(String graphSpace, String graph,
                                  VertexLabel vertexLabel) {
        this.schemaMetaManager.updateVertexLabel(graphSpace, graph,
                                                 vertexLabel);
    }

    public VertexLabel getVertexLabel(String graphSpace, String graph,
                                      Id vertexLabel) {
        return this.schemaMetaManager.getVertexLabel(graphSpace, graph,
                                                     vertexLabel);
    }

    public VertexLabel getVertexLabel(String graphSpace, String graph,
                                      String vertexLabel) {
        return this.schemaMetaManager.getVertexLabel(graphSpace, graph,
                                                     vertexLabel);
    }

    public List<VertexLabel> getVertexLabels(String graphSpace, String graph) {
        return this.schemaMetaManager.getVertexLabels(graphSpace, graph);
    }

    public Id removeVertexLabel(String graphSpace, String graph,
                                Id vertexLabel) {
        return this.schemaMetaManager.removeVertexLabel(graphSpace, graph,
                                                        vertexLabel);
    }

    public void addEdgeLabel(String graphSpace, String graph,
                             EdgeLabel edgeLabel) {
        this.schemaMetaManager.addEdgeLabel(graphSpace, graph, edgeLabel);
    }

    public void updateEdgeLabel(String graphSpace, String graph,
                                EdgeLabel edgeLabel) {
        this.schemaMetaManager.updateEdgeLabel(graphSpace, graph, edgeLabel);
    }

    public EdgeLabel getEdgeLabel(String graphSpace, String graph,
                                  Id edgeLabel) {
        return this.schemaMetaManager.getEdgeLabel(graphSpace, graph,
                                                   edgeLabel);
    }

    public EdgeLabel getEdgeLabel(String graphSpace, String graph,
                                  String edgeLabel) {
        return this.schemaMetaManager.getEdgeLabel(graphSpace, graph,
                                                   edgeLabel);
    }

    public List<EdgeLabel> getEdgeLabels(String graphSpace, String graph) {
        return this.schemaMetaManager.getEdgeLabels(graphSpace, graph);
    }

    public Id removeEdgeLabel(String graphSpace, String graph, Id edgeLabel) {
        return this.schemaMetaManager.removeEdgeLabel(graphSpace, graph,
                                                      edgeLabel);
    }

    public void addIndexLabel(String graphSpace, String graph,
                              IndexLabel indexLabel) {
        this.schemaMetaManager.addIndexLabel(graphSpace, graph, indexLabel);
    }

    public void updateIndexLabel(String graphSpace, String graph,
                                 IndexLabel indexLabel) {
        this.schemaMetaManager.updateIndexLabel(graphSpace, graph, indexLabel);
    }

    public IndexLabel getIndexLabel(String graphSpace, String graph,
                                    Id indexLabel) {
        return this.schemaMetaManager.getIndexLabel(graphSpace, graph,
                                                    indexLabel);
    }

    public IndexLabel getIndexLabel(String graphSpace, String graph,
                                    String indexLabel) {
        return this.schemaMetaManager.getIndexLabel(graphSpace, graph,
                                                    indexLabel);
    }

    public List<IndexLabel> getIndexLabels(String graphSpace, String graph) {
        return this.schemaMetaManager.getIndexLabels(graphSpace, graph);
    }

    public Id removeIndexLabel(String graphSpace, String graph, Id indexLabel) {
        return this.schemaMetaManager.removeIndexLabel(graphSpace, graph,
                                                       indexLabel);
    }

    public void createUser(HugeUser user) throws IOException {
        this.authMetaManager.createUser(user);
    }

    public HugeUser updateUser(HugeUser user) throws IOException {
        return this.authMetaManager.updateUser(user);
    }

    public HugeUser deleteUser(Id id) throws IOException,
                                             ClassNotFoundException {
        return this.authMetaManager.deleteUser(id);
    }

    public HugeUser findUser(String name)
            throws IOException, ClassNotFoundException {
        return this.authMetaManager.findUser(name);
    }

    public List<HugeUser> listUsers(List<Id> ids) throws IOException,
                                                         ClassNotFoundException {
        return this.authMetaManager.listUsers(ids);
    }

    public List<HugeUser> listAllUsers(long limit)
            throws IOException,
                   ClassNotFoundException {
        return this.authMetaManager.listAllUsers(limit);
    }

    public Id createGroup(HugeGroup group) throws IOException {
        return this.authMetaManager.createGroup(group);
    }

    public HugeGroup updateGroup(HugeGroup group) throws IOException {
        return this.authMetaManager.updateGroup(group);
    }

    public HugeGroup deleteGroup(Id id) throws IOException,
                                               ClassNotFoundException {
        return this.authMetaManager.deleteGroup(id);
    }

    public HugeGroup findGroup(String name) throws IOException,
                                                   ClassNotFoundException {
        return this.authMetaManager.findGroup(name);
    }

    public List<HugeGroup> listGroups(long limit) throws IOException,
                                                         ClassNotFoundException {
        return this.authMetaManager.listGroups(limit);
    }

    public Id createRole(String graphSpace, HugeRole role)
            throws IOException {
        return this.authMetaManager.createRole(graphSpace, role);
    }

    public HugeRole updateRole(String graphSpace, HugeRole role)
            throws IOException {
        return this.authMetaManager.updateRole(graphSpace, role);
    }

    public HugeRole deleteRole(String graphSpace, Id id)
            throws IOException,
                   ClassNotFoundException {
        return this.authMetaManager.deleteRole(graphSpace, id);
    }

    public HugeRole findRole(String graphSpace, Id id) {
        return this.authMetaManager.findRole(graphSpace, id);
    }

    public HugeRole getRole(String graphSpace, Id id)
            throws IOException,
                   ClassNotFoundException {
        return this.authMetaManager.getRole(graphSpace, id);
    }

    public List<HugeRole> listRoles(String graphSpace, List<Id> ids)
            throws IOException,
                   ClassNotFoundException {
        return this.authMetaManager.listRoles(graphSpace, ids);
    }

    public List<HugeRole> listAllRoles(String graphSpace, long limit)
            throws IOException,
                   ClassNotFoundException {
        return this.authMetaManager.listAllRoles(graphSpace, limit);
    }

    public Id createTarget(String graphSpace, HugeTarget target)
            throws IOException {
        return this.authMetaManager.createTarget(graphSpace, target);
    }

    public HugeTarget updateTarget(String graphSpace, HugeTarget target)
            throws IOException {
        return this.authMetaManager.updateTarget(graphSpace, target);
    }

    public HugeTarget deleteTarget(String graphSpace, Id id)
            throws IOException,
                   ClassNotFoundException {
        return this.authMetaManager.deleteTarget(graphSpace, id);
    }

    public HugeTarget findTarget(String graphSpace, Id id) {
        return this.authMetaManager.findTarget(graphSpace, id);
    }

    public HugeTarget getTarget(String graphSpace, Id id)
            throws IOException,
                   ClassNotFoundException {
        return this.authMetaManager.getTarget(graphSpace, id);
    }

    public List<HugeTarget> listTargets(String graphSpace, List<Id> ids)
            throws IOException,
                   ClassNotFoundException {
        return this.authMetaManager.listTargets(graphSpace, ids);
    }

    public List<HugeTarget> listAllTargets(String graphSpace, long limit)
            throws IOException,
                   ClassNotFoundException {
        return this.authMetaManager.listAllTargets(graphSpace, limit);
    }

    public Id createBelong(String graphSpace, HugeBelong belong)
            throws IOException, ClassNotFoundException {
        return this.authMetaManager.createBelong(graphSpace, belong);
    }

    public HugeBelong updateBelong(String graphSpace, HugeBelong belong)
            throws IOException, ClassNotFoundException {
        return this.authMetaManager.updateBelong(graphSpace, belong);
    }

    public HugeBelong deleteBelong(String graphSpace, Id id)
            throws IOException,
                   ClassNotFoundException {
        return this.authMetaManager.deleteBelong(graphSpace, id);
    }

    public HugeBelong getBelong(String graphSpace, Id id)
            throws IOException,
                   ClassNotFoundException {
        return this.authMetaManager.getBelong(graphSpace, id);
    }

    public boolean existBelong(String graphSpace, Id id) {
        return this.authMetaManager.existBelong(graphSpace, id);
    }

    public List<HugeBelong> listBelong(String graphSpace, List<Id> ids)
            throws IOException,
                   ClassNotFoundException {
        return this.authMetaManager.listBelong(graphSpace, ids);
    }

    public List<HugeBelong> listAllBelong(String graphSpace, long limit)
            throws IOException,
                   ClassNotFoundException {
        return this.authMetaManager.listAllBelong(graphSpace, limit);
    }

    public List<HugeBelong> listBelongBySource(String graphSpace,
                                               Id user, String link, long limit)
            throws IOException,
                   ClassNotFoundException {
        return this.authMetaManager.listBelongBySource(graphSpace, user,
                                                       link, limit);
    }

    public List<HugeBelong> listBelongByTarget(String graphSpace,
                                               Id role, String link, long limit)
            throws IOException,
                   ClassNotFoundException {
        return this.authMetaManager.listBelongByTarget(graphSpace, role,
                                                       link, limit);
    }

    public Id createAccess(String graphSpace, HugeAccess access)
            throws IOException, ClassNotFoundException {
        return this.authMetaManager.createAccess(graphSpace, access);
    }

    public HugeAccess updateAccess(String graphSpace, HugeAccess access)
            throws IOException, ClassNotFoundException {
        return this.authMetaManager.updateAccess(graphSpace, access);
    }

    public HugeAccess deleteAccess(String graphSpace, Id id)
            throws IOException, ClassNotFoundException {
        return this.authMetaManager.deleteAccess(graphSpace, id);
    }

    public HugeAccess findAccess(String graphSpace, Id id) {
        return this.authMetaManager.findAccess(graphSpace, id);
    }

    public HugeAccess getAccess(String graphSpace, Id id)
            throws IOException, ClassNotFoundException {
        return this.authMetaManager.getAccess(graphSpace, id);
    }

    public List<HugeAccess> listAccess(String graphSpace, List<Id> ids)
            throws IOException,
                   ClassNotFoundException {
        return this.authMetaManager.listAccess(graphSpace, ids);
    }

    public List<HugeAccess> listAllAccess(String graphSpace, long limit)
            throws IOException,
                   ClassNotFoundException {
        return this.authMetaManager.listAllAccess(graphSpace, limit);
    }

    public List<HugeAccess> listAccessByRole(String graphSpace,
                                             Id role, long limit)
            throws IOException,
                   ClassNotFoundException {
        return this.authMetaManager.listAccessByRole(graphSpace, role, limit);
    }

    public List<HugeAccess> listAccessByGroup(String graphSpace,
                                              Id group, long limit)
            throws IOException,
                   ClassNotFoundException {
        return this.authMetaManager.listAccessByGroup(graphSpace, group, limit);
    }

    public String targetFromAccess(String accessKey) {
        return this.authMetaManager.targetFromAccess(accessKey);
    }

    public void clearGraphAuth(String graphSpace) {
        this.authMetaManager.clearGraphAuth(graphSpace);
    }

    public List<HugeAccess> listAccessByTarget(String graphSpace,
                                               Id target, long limit)
            throws IOException,
                   ClassNotFoundException {
        return this.authMetaManager.listAccessByTarget(graphSpace, target,
                                                       limit);
    }

    public Id createProject(String graphSpace, HugeProject project)
            throws IOException {
        return this.authMetaManager.createProject(graphSpace, project);
    }

    public HugeProject updateProject(String graphSpace, HugeProject project)
            throws IOException {
        return this.authMetaManager.updateProject(graphSpace, project);
    }

    public HugeProject deleteProject(String graphSpace, Id id)
            throws IOException, ClassNotFoundException {
        return this.authMetaManager.deleteProject(graphSpace, id);
    }

    public HugeProject getProject(String graphSpace, Id id)
            throws IOException, ClassNotFoundException {
        return this.authMetaManager.getProject(graphSpace, id);
    }

    public List<HugeProject> listAllProjects(String graphSpace, long limit)
            throws IOException, ClassNotFoundException {
        return this.authMetaManager.listAllProjects(graphSpace, limit);
    }

    public List<String> listGraphSpace() {
        return this.spaceMetaManager.listGraphSpace();
    }

    public void initDefaultGraphSpace() {
        String defaultGraphSpace = "DEFAULT";
        this.appendGraphSpaceList(defaultGraphSpace);
    }

    public Map<String, Object> restProperties(String graphSpace,
                                              String serviceId) {
        return this.configMetaManager.restProperties(graphSpace, serviceId);
    }

    public Map<String, Object> restProperties(String graphSpace,
                                              String serviceId,
                                              Map<String, Object> properties) {
        return this.configMetaManager.restProperties(graphSpace, serviceId,
                                                     properties);
    }

    public Map<String, Object> deleteRestProperties(String graphSpace,
                                                    String serviceId,
                                                    String key) {
        return this.configMetaManager.deleteRestProperties(graphSpace,
                                                           serviceId, key);
    }

    public Map<String, Object> clearRestProperties(String graphSpace,
                                                   String serviceId) {
        return this.configMetaManager.clearRestProperties(graphSpace,
                                                          serviceId);
    }

    public LockResult tryLockTask(String graphSpace, String graphName,
                                  String taskId) {
        return this.taskMetaManager.tryLockTask(graphSpace, graphName, taskId);
    }

    public boolean isLockedTask(String graphSpace, String graphName,
                                String taskId) {
        return this.taskMetaManager.isLockedTask(graphSpace, graphName, taskId);
    }

    public void unlockTask(String graphSpace, String graphName,
                           String taskId, LockResult lockResult) {
        this.taskMetaManager.unlockTask(graphSpace, graphName, taskId, lockResult);
    }

    public String gremlinYaml(String graphSpace, String serviceId) {
        return this.configMetaManager.gremlinYaml(graphSpace, serviceId);
    }

    public String gremlinYaml(String graphSpace, String serviceId,
                              String yaml) {
        return this.configMetaManager.gremlinYaml(graphSpace, serviceId, yaml);
    }

    public String hstorePDPeers() {
        return this.metaDriver.get(hstorePDPeersKey());
    }

    public <T> void listenAll(Consumer<T> consumer) {
        this.metaDriver.listenPrefix(MetaManager.META_PATH_HUGEGRAPH, consumer);
    }

    public SchemaMetaManager schemaMetaManager() {
        return this.schemaMetaManager;
    }

    public MetaDriver metaDriver() {
        return this.metaDriver;
    }

    public String getDDSHost() {
        String key = this.ddsHostKey();
        String host = this.metaDriver.get(key);
        return host;
    }

    public String getHugeGraphClusterRole() {
        String key = this.hugeClusterRoleKey();
        String role = this.metaDriver.get(key);
        return role;
    }

    public String getKafkaBrokerHost() {
        String key = this.kafkaHostKey();
        return this.metaDriver.get(key);
    }

    public String getKafkaBrokerPort() {
        String key = this.kafkaPortKey();
        return this.metaDriver.get(key);
    }

    public Integer getPartitionCount() {
        String key = this.kafkaPartitionCountKey();
        String result = this.metaDriver.get(key);
        try {
            Integer count = Integer.parseInt(Optional.ofNullable(result)
                                                     .orElse("0"));
            return count < 1 ? 1 : count;
        } catch (Exception e) {
            return 1;
        }
    }

    public String getKafkaSlaveServerHost() {
        String key = this.kafkaSlaveHostKey();
        return this.metaDriver.get(key);
    }

    public Integer getKafkaSlaveServerPort() {
        String key = this.kafkaSlavePortKey();
        String portStr = this.metaDriver.get(key);
        int port = Integer.parseInt(portStr);
        return port;
    }

    public List<String> getKafkaFilteredGraphspace() {
        String key = this.kafkaFilterGraphspaceKey();

        String raw = this.metaDriver.get(key);
        if (StringUtils.isEmpty(raw)) {
            return Collections.EMPTY_LIST;
        }
        String[] parts = raw.split(",");
        return Arrays.asList(parts);
    }

    public List<String> getKafkaFilteredGraph() {
        String key = this.kafkaFilterGraphKey();

        String raw = this.metaDriver.get(key);
        if (StringUtils.isEmpty(raw)) {
            return Collections.EMPTY_LIST;
        }
        String[] parts = raw.split(",");
        return Arrays.asList(parts);
    }

    public void updateKafkaFilteredGraphspace(List<String> graphSpaces) {
        String key = this.kafkaFilterGraphspaceKey();
        String val = String.join(",", graphSpaces);
        this.metaDriver.put(key, val);

    }

    public void updateKafkaFilteredGraph(List<String> graphs) {
        String key = this.kafkaFilterGraphKey();
        String val = String.join(",", graphs);
        this.metaDriver.put(key, val);
    }

    public List<String> getWhiteIpList() {
        String key = this.whiteIpListKey();

        String raw = this.metaDriver.get(key);
        if (StringUtils.isEmpty(raw)) {
            return new ArrayList<>();
        }
        String[] parts = raw.split(",");
        return new ArrayList<>(Arrays.asList(parts));
    }

    public void setWhiteIpList(List<String> whiteIpList) {
        String key = this.whiteIpListKey();

        String val = String.join(",", whiteIpList);
        this.metaDriver.put(key, val);
    }

    public String getCompStatus(String statuskey) {
        String raw = this.metaDriver.get(statuskey);
        if (StringUtils.isEmpty(raw)) {
            return "";
        }
        return raw;
    }

    public boolean getWhiteIpStatus() {
        String key = this.whiteIpStatusKey();
        String raw = this.metaDriver.get(key);
        return ("true".equals(raw));
    }

    public void setWhiteIpStatus(boolean status) {
        String key = this.whiteIpStatusKey();
        this.metaDriver.put(key, ((Boolean) status).toString());
    }

    public static String schemaCacheClearEventValue(String graph,
                                                    String source) {
        if (StringUtils.isEmpty(source)) {
            return graph;
        }
        return JsonUtil.toJson(ImmutableMap.of(SCHEMA_CACHE_CLEAR_GRAPH_KEY,
                                               graph,
                                               SCHEMA_CACHE_CLEAR_SOURCE_KEY,
                                               source));
    }

    public static final class SchemaCacheClearEvent {

        private final String graph;
        private final String source;

        private SchemaCacheClearEvent(String graph, String source) {
            this.graph = graph;
            this.source = source;
        }

        public String graph() {
            return this.graph;
        }

        public String source() {
            return this.source;
        }

        @SuppressWarnings("unchecked")
        static SchemaCacheClearEvent fromValue(String value) {
            if (StringUtils.isEmpty(value)) {
                return null;
            }
            // Compatibility: events published before source-id support stored
            // only the graph name as a plain string. Keep accepting that format
            // so mixed-version clusters can consume old/new schema-cache-clear
            // events during rolling upgrades.
            if (value.charAt(0) != '{') {
                return new SchemaCacheClearEvent(value, null);
            }

            Map<String, Object> payload;
            try {
                payload = JsonUtil.fromJson(value, Map.class);
            } catch (RuntimeException e) {
                LOG.debug("Malformed schema-cache-clear payload, ignoring: {}",
                          value, e);
                return null;
            }

            Object graph = payload.get(SCHEMA_CACHE_CLEAR_GRAPH_KEY);
            if (graph == null) {
                LOG.debug("Schema-cache-clear payload missing '{}' field: {}",
                          SCHEMA_CACHE_CLEAR_GRAPH_KEY, value);
                return null;
            }

            Object source = payload.get(SCHEMA_CACHE_CLEAR_SOURCE_KEY);
            String sourceValue = source == null ? null : source.toString();
            return new SchemaCacheClearEvent(graph.toString(), sourceValue);
        }
    }

    public enum MetaDriverType {
        ETCD,
        PD
    }

    public enum BindingType {
        OLTP,
        OLAP,
        STORAGE
    }

    public static class AuthEvent {

        private String op; // ALLOW: CREATE | DELETE | UPDATE
        private String type; // ALLOW: USER | GROUP | TARGET | ACCESS | BELONG
        private String id;

        public AuthEvent(String op, String type, String id) {
            this.op = op;
            this.type = type;
            this.id = id;
        }

        public AuthEvent(Map<String, Object> properties) {
            this.op = properties.get("op").toString();
            this.type = properties.get("type").toString();
            this.id = properties.get("id").toString();
        }

        public String op() {
            return this.op;
        }

        public void op(String op) {
            this.op = op;
        }

        public String type() {
            return this.type;
        }

        public void type(String type) {
            this.type = type;
        }

        public String id() {
            return this.id;
        }

        public void id(String id) {
            this.id = id;
        }

        public Map<String, Object> asMap() {
            return ImmutableMap.of("op", this.op,
                                   "type", this.type,
                                   "id", this.id);
        }
    }
}
