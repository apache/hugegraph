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

package org.apache.hugegraph.k8s;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.Subject;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.fabric8.kubernetes.client.*;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.dsl.ParameterNamespaceListVisitFromServerGetDeleteRecreateWaitApplicable;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.space.Service;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class K8sDriver {

    protected static final Logger LOG = Log.logger(K8sDriver.class);

    private static final String DELIMITER = "-";
    private static final String COLON = ":";
    private static final String COMMA = ",";

    private static final String CONTAINER = "container";
    private static final String APP = "app";
    private static final String PORT_SUFFIX = "-port";
    private static final String TCP = "TCP";

    private static final String NODE_PORT = "NodePort";
    private static final int HG_PORT = 8080;

    private static final String CPU = "cpu";
    private static final String MEMORY = "memory";
    private static final String CPU_UNIT = "m";
    private static final String MEMORY_UNIT = "G";

    private static final String HEALTH_CHECK_API = "/versions";

    private static final String CA_CONFIG_MAP_NAME = "hg-ca";

    private static final String GRAPH_SPACE = "GRAPH_SPACE";
    private static final String SERVICE_ID = "SERVICE_ID";
    private static final String META_SERVERS = "META_SERVERS";
    private static final String CLUSTER = "CLUSTER";
    private static final String IMAGE_PULL_POLICY_ALWAYS = "Always";

    private static final String MY_NODE_NAME = "MY_NODE_NAME";
    private static final String MY_POD_IP = "MY_POD_IP";
    private static final String SPEC_NODE_NAME = "spec.nodeName";
    private static final String STATUS_POD_IP = "status.podIP";
    private static final String APP_NAME = "APP_NAME";

    private static final String SERVICE_ACCOUNT_NAME = "hugegraph-user";
    private static final String SERVICE_ACCOUNT = "ServiceAccount";
    private static final String BINDING_API_GROUP = "rbac.authorization.k8s.io";
    private static final String CLUSTER_ROLE = "ClusterRole";
    private static final String CLUSTER_ROLE_NAME = "cluster-admin";
    private static final String BINDING_API_VERSION = "rbac.authorization.k8s.io/v1";

    private final KubernetesClient client;

    private String oltpImage;
    private String olapImage;
    private String storageImage;

    private CA ca;

    public K8sDriver() {
        Config config = new ConfigBuilder().build();
        this.client = new DefaultKubernetesClient(config);
    }

    private static Set<String> urlsOfService(
            io.fabric8.kubernetes.api.model.Service service, String routeType) {
        Set<String> urls = new HashSet<>();
        String clusterIP = service.getSpec().getClusterIP();
        for (ServicePort port : service.getSpec().getPorts()) {
            int actualPort = routeType.equals(NODE_PORT) ?
                             port.getNodePort() : port.getPort();
            urls.add(clusterIP + COLON + HG_PORT + COMMA + actualPort);
        }
        return urls;
    }

    private static String metaServers(List<String> metaServers) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < metaServers.size(); i++) {
            builder.append(metaServers.get(i));
            if (i != metaServers.size() - 1) {
                builder.append(COMMA);
            }
        }
        return builder.toString();
    }

    private static String namespace(GraphSpace graphSpace, Service service) {
        String namespace;
        switch (service.type()) {
            case OLTP:
                namespace = graphSpace.oltpNamespace();
                break;
            case OLAP:
                namespace = graphSpace.olapNamespace();
                break;
            case STORAGE:
                namespace = graphSpace.storageNamespace();
                break;
            default:
                throw new AssertionError(String.format(
                        "Invalid service type '%s'", service.type()));
        }
        return namespace;
    }

    private static String validateNamespaceName(String namespace) {
        return namespace.replace("_", "-").toLowerCase();
    }

    private static String deploymentName(GraphSpace graphSpace,
                                         Service service) {
        return deploymentServiceName(graphSpace, service);
    }

    private static String serviceName(GraphSpace graphSpace,
                                      Service service) {
        return deploymentServiceName(graphSpace, service);
    }

    private static String deploymentServiceName(GraphSpace graphSpace,
                                                Service service) {
        String name = String.join(DELIMITER,
                                  graphSpace.name(),
                                  service.type().name(),
                                  service.name());
        return name.replace("_", "-").toLowerCase();
    }

    private static void sleepAWhile(int second) {
        try {
            Thread.sleep(second * 1000L);
        } catch (InterruptedException e) {
            // Ignore
        }
    }

    private static String serviceAccountName(String namespace) {
        return namespace + SERVICE_ACCOUNT_NAME;
    }

    public void ca(CA ca) {
        this.ca = ca;
    }

    public String oltpImage() {
        return this.oltpImage;
    }

    public void oltpImage(String oltpImage) {
        this.oltpImage = oltpImage;
    }

    public String olapImage() {
        return this.olapImage;
    }

    public void olapImage(String olapImage) {
        this.olapImage = olapImage;
    }

    public String storageImage() {
        return this.storageImage;
    }

    public void storageImage(String storageImage) {
        this.storageImage = storageImage;
    }

    public Namespace namespace(String ns) {
        NamespaceList nameSpaceList = this.client.namespaces().list();
        List<Namespace> namespaceList = nameSpaceList.getItems();
        for (Namespace namespace : namespaceList) {
            if (namespace.getMetadata().getName().equals(ns)) {
                return namespace;
            }
        }
        return null;
    }

    public List<Namespace> namespaces() {
        NamespaceList nameSpaceList = this.client.namespaces().list();
        return nameSpaceList.getItems();
    }

    public List<String> namespaceNames() {
        List<String> names = new ArrayList<>();
        NamespaceList nameSpaceList = this.client.namespaces().list();
        for (Namespace namespace : nameSpaceList.getItems()) {
            names.add(namespace.getMetadata().getName());
        }
        return names;
    }

    public Namespace createNamespace(String name, Map<String, String> labels) {
        Namespace namespace = new NamespaceBuilder()
                .withNewMetadata()
                .withName(validateNamespaceName(name))
                .addToLabels(labels)
                .endMetadata()
                .build();
        return this.client.namespaces().createOrReplace(namespace);
    }

    public boolean deleteNamespace(String name) {
        return this.client.namespaces().withName(name).delete();
    }

    public Pod createPod(String namespace, String podName,
                         Map<String, String> labels,
                         String containerName, String image) {
        Pod pod = new PodBuilder()
                .withNewMetadata()
                .withName(podName)
                .addToLabels(labels)
                .endMetadata()
                .withNewSpec()
                .addNewContainer()
                .withName(containerName)
                .withImage(image)
                .endContainer()
                .endSpec()
                .build();
        return this.client.pods().inNamespace(namespace).createOrReplace(pod);
    }

    public List<Pod> pods(String namespace) {
        return this.client.pods().inNamespace(namespace).list().getItems();
    }

    public Pod pod(String namespace, String podName) {
        return this.client.pods()
                          .inNamespace(namespace)
                          .withName(podName)
                          .get();
    }

    public Set<String> createOltpService(GraphSpace graphSpace,
                                         Service service,
                                         List<String> metaServers,
                                         String cluster) {
        this.createConfigMapForCaIfNeeded(graphSpace, service);
        this.createServiceAccountIfNeeded(graphSpace, service);
        this.createDeployment(graphSpace, service, metaServers, cluster);
        return this.createService(graphSpace, service);
    }

    public Set<String> startOltpService(GraphSpace graphSpace,
                                        Service service,
                                        List<String> metaServers,
                                        String cluster) {
        // Get & check config map
        String namespace = namespace(graphSpace, service);
        ConfigMap configMap = this.client.configMaps()
                                         .inNamespace(namespace)
                                         .withName(CA_CONFIG_MAP_NAME)
                                         .get();
        if (null == configMap) {
            throw new HugeException("Cannot start OLTP service since " +
                                    "configMap does not exist!");
        }

        // Get & check service account
        ServiceAccount serviceAccount = this.client.serviceAccounts()
                                                   .inNamespace(namespace)
                                                   .withName(serviceAccountName(namespace))
                                                   .get();

        if (null == serviceAccount) {
            throw new HugeException("Cannot start OLTP service since service " +
                                    "account is not created!");
        }
        // Get & check deployment
        String deploymentName = deploymentName(graphSpace, service);
        Deployment deployment = this.client.apps().deployments()
                                           .inNamespace(namespace)
                                           .withName(deploymentName)
                                           .get();
        if (null == deployment) {
            throw new HugeException("Cannot start OLTP service since deployment is not created!");
        }
        // start service
        this.client.apps()
                   .deployments()
                   .inNamespace(namespace)
                   .withName(deploymentName)
                   .scale(service.count());
        return this.createService(graphSpace, service);

    }

    public void stopOltpService(GraphSpace graphSpace, Service service) {

        String serviceName = serviceName(graphSpace, service);
        String namespace = namespace(graphSpace, service);
        this.client.services().inNamespace(namespace)
                   .withName(serviceName).delete();

        io.fabric8.kubernetes.api.model.Service svc = this.client.services()
                                                                 .inNamespace(namespace)
                                                                 .withName(serviceName).get();
        int count = 0;
        while (svc != null && count++ < 10) {
            svc = this.client.services().inNamespace(namespace)
                             .withName(serviceName).get();
            sleepAWhile(1);
        }
        if (svc != null) {
            throw new HugeException("Failed to stop service: %s", svc);
        }
        String deploymentName = deploymentName(graphSpace, service);
        Deployment deployment = this.client.apps().deployments()
                                           .inNamespace(namespace)
                                           .withName(deploymentName)
                                           .get();
        if (null != deployment) {
            this.client.apps().deployments()
                       .inNamespace(namespace)
                       .withName(deploymentName)
                       .scale(0);
        }

    }

    public void deleteOltpService(GraphSpace graphSpace, Service service) {
        String deploymentName = serviceName(graphSpace, service);
        String namespace = namespace(graphSpace, service);
        LOG.info("Stop deployment {} in namespace {}",
                 deploymentName, namespace);
        this.client.apps().deployments().inNamespace(namespace)
                   .withName(deploymentName).delete();
        Deployment deployment = this.client.apps().deployments()
                                           .inNamespace(namespace).withName(deploymentName).get();
        int count = 0;
        while (deployment != null && count++ < 10) {
            deployment = this.client.apps().deployments().inNamespace(namespace)
                                    .withName(deploymentName).get();
            sleepAWhile(1);
        }
        if (deployment != null) {
            throw new HugeException("Failed to stop deployment: %s",
                                    deployment);
        }

        LOG.info("Stop service {} in namespace {}", service, namespace);
        String serviceName = deploymentName;
        this.client.services().inNamespace(namespace)
                   .withName(serviceName).delete();
        io.fabric8.kubernetes.api.model.Service svc = this.client.services()
                                                                 .inNamespace(namespace)
                                                                 .withName(serviceName).get();
        count = 0;
        while (svc != null && count++ < 10) {
            svc = this.client.services().inNamespace(namespace)
                             .withName(serviceName).get();
            sleepAWhile(1);
        }
        if (svc != null) {
            throw new HugeException("Failed to stop service: %s", svc);
        }
    }

    public void createConfigMapForCaIfNeeded(GraphSpace graphSpace,
                                             Service service) {
        String namespace = namespace(graphSpace, service);
        ConfigMap configMap = this.client.configMaps()
                                         .inNamespace(namespace)
                                         .withName(CA_CONFIG_MAP_NAME)
                                         .get();
        if (configMap != null) {
            return;
        }

        String ca;
        String clientCa;
        String clientKey;
        String config;
        try {
            ca = FileUtils.readFileToString(new File(this.ca.caFile));
            clientCa = FileUtils.readFileToString(
                    new File(this.ca.clientCaFile));
            clientKey = FileUtils.readFileToString(
                    new File(this.ca.clientKeyFile));
            config = FileUtils.readFileToString(new File(this.ca.config()));
        } catch (IOException e) {
            throw new HugeException("Failed to read ca files", e);
        }

        Map<String, String> data = new HashMap<>(4);
        data.put("config", config);
        data.put("ca.pem", ca);
        data.put("kubernetes.pem", clientCa);
        data.put("kubernetes-key8.pem", clientKey);
        ConfigMap cm = new ConfigMapBuilder()
                .withNewMetadata()
                .withName(CA_CONFIG_MAP_NAME)
                .withNamespace(namespace)
                .endMetadata()
                .withData(data)
                .build();
        this.client.configMaps()
                   .inNamespace(namespace)
                   .create(cm);
    }

    private void createServiceAccountIfNeeded(GraphSpace graphSpace,
                                              Service service) {
        String namespace = namespace(graphSpace, service);
        String serviceAccountName = serviceAccountName(namespace);
        ServiceAccount serviceAccount = this.client
                .serviceAccounts()
                .inNamespace(namespace)
                .withName(serviceAccountName)
                .get();

        if (serviceAccount != null) {
            return;
        }

        // Create service account
        serviceAccount = new ServiceAccountBuilder()
                .withNewMetadata()
                .withName(serviceAccountName)
                .withNamespace(namespace)
                .endMetadata().build();
        this.client.serviceAccounts()
                   .inNamespace(namespace)
                   .create(serviceAccount);

        // Bind service account
        Subject subject = new SubjectBuilder()
                .withKind(SERVICE_ACCOUNT)
                .withName(serviceAccountName)
                .withNamespace(namespace)
                .build();
        ClusterRoleBinding clusterRoleBinding = new ClusterRoleBindingBuilder()
                .withApiVersion(BINDING_API_VERSION)
                .withNewMetadata()
                .withName(serviceAccountName)
                .endMetadata()

                .withNewRoleRef()
                .withApiGroup(BINDING_API_GROUP)
                .withKind(CLUSTER_ROLE)
                .withName(CLUSTER_ROLE_NAME)
                .endRoleRef()

                .withSubjects(subject)
                .build();

        this.client.rbac().clusterRoleBindings().create(clusterRoleBinding);
    }

    public Set<String> createService(GraphSpace graphSpace, Service svc) {
        String serviceName = serviceName(graphSpace, svc);
        String namespace = namespace(graphSpace, svc);
        String portName = serviceName + PORT_SUFFIX;
        io.fabric8.kubernetes.api.model.Service service;
        if (NODE_PORT.equals(svc.routeType())) {
            if (svc.port() != 0) {
                service = new ServiceBuilder()
                        .withNewMetadata()
                        .withName(serviceName)
                        .endMetadata()
                        .withNewSpec()
                        .withSelector(Collections.singletonMap(APP, serviceName))
                        .addNewPort()
                        .withName(portName)
                        .withProtocol(TCP)
                        .withPort(HG_PORT)
                        .withTargetPort(new IntOrString(HG_PORT))
                        .withNodePort(svc.port())
                        .endPort()
                        .withType(NODE_PORT)
                        .endSpec()
                        .build();
            } else {
                service = new ServiceBuilder()
                        .withNewMetadata()
                        .withName(serviceName)
                        .endMetadata()
                        .withNewSpec()
                        .withSelector(Collections.singletonMap(APP, serviceName))
                        .addNewPort()
                        .withName(portName)
                        .withProtocol(TCP)
                        .withPort(HG_PORT)
                        .withTargetPort(new IntOrString(HG_PORT))
                        .endPort()
                        .withType(NODE_PORT)
                        .endSpec()
                        .build();
            }
        } else {
            service = new ServiceBuilder()
                    .withNewMetadata()
                    .withName(serviceName)
                    .endMetadata()
                    .withNewSpec()
                    .withSelector(Collections.singletonMap(APP, serviceName))
                    .addNewPort()
                    .withName(portName)
                    .withProtocol(TCP)
                    .withPort(HG_PORT)
                    .withTargetPort(new IntOrString(HG_PORT))
                    .endPort()
                    .withType(svc.routeType())
                    .endSpec()
                    .build();
        }

        LOG.info("Start service {} in namespace {}", service, namespace);
        this.client.services().inNamespace(namespace).create(service);

        service = this.client.services()
                             .inNamespace(namespace)
                             .withName(serviceName)
                             .get();

        return urlsOfService(service, svc.routeType());
    }

    public Deployment createDeployment(GraphSpace graphSpace, Service service,
                                       List<String> metaServers,
                                       String cluster) {
        Deployment deployment = this.constructDeployment(graphSpace, service,
                                                         metaServers, cluster);
        String namespace = namespace(graphSpace, service);
        LOG.info("Start deployment {} in namespace {}", deployment, namespace);
        deployment = this.client.apps().deployments().inNamespace(namespace)
                                .createOrReplace(deployment);

        ListOptions options = new ListOptions();
        options.setLabelSelector(APP + "=" + serviceName(graphSpace, service));
        List<Pod> hugegraphservers = new ArrayList<>();
        int count = 0;
        while (hugegraphservers.isEmpty() && count++ < 10) {
            hugegraphservers = this.client.pods()
                                          .inNamespace(namespace)
                                          .list(options)
                                          .getItems();
            sleepAWhile(1);
        }
        if (hugegraphservers.isEmpty()) {
            throw new HugeException("Failed to start oltp server pod");
        }
        return deployment;
    }

    private Deployment constructDeployment(GraphSpace graphSpace,
                                           Service service,
                                           List<String> metaServers,
                                           String cluster) {
        String namespace = namespace(graphSpace, service);
        String deploymentName = deploymentName(graphSpace, service);
        String containerName = String.join(DELIMITER, deploymentName,
                                           CONTAINER);
        Quantity cpu = Quantity.parse((service.cpuLimit() * 1000) + CPU_UNIT);
        Quantity memory = Quantity.parse(service.memoryLimit() + MEMORY_UNIT);
        ResourceRequirements rr = new ResourceRequirementsBuilder()
                .addToLimits(CPU, cpu)
                .addToLimits(MEMORY, memory)
                .build();

        HTTPGetAction readyProbeAction = new HTTPGetActionBuilder()
                .withPath(HEALTH_CHECK_API)
                .withPort(new IntOrString(HG_PORT))
                .build();

        ConfigMapVolumeSource cmvs = new ConfigMapVolumeSourceBuilder()
                .withName(CA_CONFIG_MAP_NAME)
                .build();

        String metaServersString = metaServers(metaServers);

        EnvVarSource nodeIP = new EnvVarSourceBuilder()
                .withNewFieldRef()
                .withFieldPath(SPEC_NODE_NAME)
                .endFieldRef()
                .build();
        EnvVarSource podIP = new EnvVarSourceBuilder()
                .withNewFieldRef()
                .withFieldPath(STATUS_POD_IP)
                .endFieldRef()
                .build();

        return new DeploymentBuilder()

                .withNewMetadata()
                .withName(deploymentName)
                .addToLabels(APP, deploymentName)
                .endMetadata()

                .withNewSpec()
                .withReplicas(service.count())
                .withNewTemplate()

                .withNewMetadata()
                .addToLabels(APP, deploymentName)
                .endMetadata()

                .withNewSpec()
                .withServiceAccountName(serviceAccountName(namespace))
                .withAutomountServiceAccountToken(true)

                .addNewContainer()
                .withName(containerName)
                .withImage(this.image(service))
                .withImagePullPolicy(IMAGE_PULL_POLICY_ALWAYS)
                .withResources(rr)

                .withNewReadinessProbe()
                .withHttpGet(readyProbeAction)
                .withInitialDelaySeconds(30)
                .withPeriodSeconds(5)
                .endReadinessProbe()

                .addNewPort()
                .withContainerPort(HG_PORT)
                .endPort()

                .addNewVolumeMount()
                .withName(CA_CONFIG_MAP_NAME)
                .withMountPath(CA_CONFIG_MAP_NAME)
                .endVolumeMount()

                .addNewEnv()
                .withName(GRAPH_SPACE)
                .withValue(graphSpace.name())
                .endEnv()
                .addNewEnv()
                .withName(SERVICE_ID)
                .withValue(service.name())
                .endEnv()
                .addNewEnv()
                .withName(META_SERVERS)
                .withValue(metaServersString)
                .endEnv()
                .addNewEnv()
                .withName(CLUSTER)
                .withValue(cluster)
                .endEnv()
                .addNewEnv()
                .withName(MY_NODE_NAME)
                .withValueFrom(nodeIP)
                .endEnv()
                .addNewEnv()
                .withName(MY_POD_IP)
                .withValueFrom(podIP)
                .endEnv()
                .addNewEnv()
                .withName(APP_NAME)
                .withValue(deploymentName)
                .endEnv()

                .endContainer()

                .addNewVolume()
                .withName(CA_CONFIG_MAP_NAME)
                .withConfigMap(cmvs)
                .endVolume()

                .endSpec()
                .endTemplate()
                .withNewSelector()
                .addToMatchLabels(APP, deploymentName)
                .endSelector()
                .endSpec()
                .build();
    }

    private String image(Service service) {
        switch (service.type()) {
            case OLTP:
                return this.oltpImage;
            case OLAP:
                return this.olapImage;
            case STORAGE:
                return this.storageImage;
            default:
                throw new AssertionError(String.format(
                        "Invalid service type '%s'", service.type()));
        }
    }

    public int podsRunning(GraphSpace graphSpace, Service service) {
        String deploymentName = deploymentName(graphSpace, service);
        String namespace = namespace(graphSpace, service);
        Deployment deployment;
        try {
            deployment = this.client.apps().deployments()
                                    .inNamespace(namespace)
                                    .withName(deploymentName)
                                    .get();
            if (null == deployment) {
                return 0;
            }
            DeploymentStatus status = deployment.getStatus();
            if (null == status) {
                return 0;
            }
            Integer replica = status.getAvailableReplicas();
            return Optional.ofNullable(replica).orElse(0);
        } catch (KubernetesClientException exc) {
            LOG.error("Get k8s deployment failed when check podsRunning", exc);
            return 0;
        }
    }

    public void createOrReplaceByYaml(String yaml) throws IOException {
        InputStream is = new ByteArrayInputStream(yaml.getBytes());
        try {
            ParameterNamespaceListVisitFromServerGetDeleteRecreateWaitApplicable<HasMetadata> meta
                    = this.client.load(is);
            meta.createOrReplace();
        } catch (Exception exc) {

        } finally {
            is.close();
        }
    }

    public void createOrReplaceResourceQuota(String namespace, String yaml) {
        InputStream is = new ByteArrayInputStream(yaml.getBytes());
        Resource<ResourceQuota> quota =
                this.client.resourceQuotas().inNamespace(namespace).load(is);
        this.client.resourceQuotas().inNamespace(namespace).createOrReplace(quota.get());
    }

    public static class CA {

        private static final String CONFIG_PATH_SUFFIX = "/.kube/config";
        private static final String USER_HOME = "user.home";

        private final String caFile;
        private final String clientCaFile;
        private final String clientKeyFile;

        public CA(String caFile, String clientCaFile, String clientKeyFile) {
            E.checkArgument(caFile != null && !caFile.isEmpty(),
                            "The ca file can't be null or empty");
            E.checkArgument(clientCaFile != null && !clientCaFile.isEmpty(),
                            "The client ca file can't be null or empty");
            E.checkArgument(clientKeyFile != null && !clientKeyFile.isEmpty(),
                            "The client key file can't be null or empty");
            this.caFile = caFile;
            this.clientCaFile = clientCaFile;
            this.clientKeyFile = clientKeyFile;
        }

        public String caFile() {
            return this.caFile;
        }

        public String clientCaFile() {
            return this.clientCaFile;
        }

        public String clientKeyFile() {
            return this.clientKeyFile;
        }

        public String config() {
            return System.getProperty(USER_HOME) + CONFIG_PATH_SUFFIX;
        }
    }
}
