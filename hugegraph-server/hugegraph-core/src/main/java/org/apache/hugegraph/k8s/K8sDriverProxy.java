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

import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class K8sDriverProxy {

    private static final Logger LOG = Log.logger(K8sDriverProxy.class);

    private static final String CONFIG_PATH_SUFFIX = "/.kube/config";
    private static final String USER_HOME = "user.home";

    private static final String USER_DIR = System.getProperty("user.dir");

    private static boolean K8S_API_ENABLED = false;

    //private static String NAMESPACE = "";
    private static String KUBE_CONFIG_PATH = "";
    private static String ENABLE_INTERNAL_ALGORITHM = "";
    private static String INTERNAL_ALGORITHM_IMAGE_URL = "";
    private static Map<String, String> ALGORITHM_PARAMS = null;
    private static String INTERNAL_ALGORITHM = "[]";

    static {
        OptionSpace.register("computer-driver",
                             "org.apache.hugegraph.computer.driver.config" +
                             ".ComputerOptions");
        OptionSpace.register("computer-k8s-driver",
                             "org.apache.hugegraph.computer.k8s.config" +
                             ".KubeDriverOptions");
        OptionSpace.register("computer-k8s-spec",
                             "org.apache.hugegraph.computer.k8s.config" +
                             ".KubeSpecOptions");
    }

    // protected HugeConfig config;
    protected final Map<String, String> options = new HashMap<>();

    public K8sDriverProxy(String partitionsCount, String algorithm) {
        try {
            if (!K8sDriverProxy.K8S_API_ENABLED) {
                throw new UnsupportedOperationException(
                        "The k8s api not enabled.");
            }
            String paramsClass = ALGORITHM_PARAMS.get(algorithm);
            this.initConfig(partitionsCount, INTERNAL_ALGORITHM, paramsClass);
        } catch (Throwable throwable) {
            LOG.error("Failed to start K8sDriverProxy ", throwable);
        }
    }

    public static void disable() {
        K8S_API_ENABLED = false;
    }

    public static String getEnableInternalAlgorithm() {
        return ENABLE_INTERNAL_ALGORITHM;
    }

    public static String getInternalAlgorithmImageUrl() {
        return INTERNAL_ALGORITHM_IMAGE_URL;
    }

    public static String getInternalAlgorithm() {
        return INTERNAL_ALGORITHM;
    }

    public static Map<String, String> getAlgorithms() {
        return ALGORITHM_PARAMS;
    }

    public static void setConfig(String enableInternalAlgorithm,
                                 String internalAlgorithmImageUrl,
                                 String internalAlgorithm,
                                 Map<String, String> algorithms)
            throws IOException {
        File kubeConfigFile;
        String path = System.getProperty(USER_HOME) + CONFIG_PATH_SUFFIX;
        kubeConfigFile = new File(path);
        if (!kubeConfigFile.exists()) {
            throw new IOException("[K8s API] k8s config fail");
        }

        K8S_API_ENABLED = true;
        KUBE_CONFIG_PATH = kubeConfigFile.getAbsolutePath();
        ENABLE_INTERNAL_ALGORITHM = enableInternalAlgorithm;
        INTERNAL_ALGORITHM_IMAGE_URL = internalAlgorithmImageUrl;
        ALGORITHM_PARAMS = algorithms;
        INTERNAL_ALGORITHM = internalAlgorithm;
    }

    public static boolean isK8sApiEnabled() {
        return K8S_API_ENABLED;
    }

    public static boolean isValidAlgorithm(String algorithm) {
        return ALGORITHM_PARAMS.containsKey(algorithm);
    }

    public static String getAlgorithmClass(String algorithm) {
        return ALGORITHM_PARAMS.get(algorithm);
    }

    protected void initConfig(String partitionsCount,
                              String internalAlgorithm,
                              String paramsClass) {

        // from configuration
        options.put("k8s.kube_config", K8sDriverProxy.KUBE_CONFIG_PATH);
        options.put("k8s.enable_internal_algorithm",
                    K8sDriverProxy.ENABLE_INTERNAL_ALGORITHM);
        options.put("k8s.internal_algorithm_image_url",
                    K8sDriverProxy.INTERNAL_ALGORITHM_IMAGE_URL);

        // from rest api params
        // partitionsCount >= worker_instances
        options.put("job.partitions_count", partitionsCount);
        options.put("k8s.internal_algorithm", internalAlgorithm);
        options.put("algorithm.params_class", paramsClass);
    }

}
