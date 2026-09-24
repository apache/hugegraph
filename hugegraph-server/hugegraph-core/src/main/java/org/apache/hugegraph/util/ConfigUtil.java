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

package org.apache.hugegraph.util;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.YAMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.configuration2.tree.NodeHandler;
import org.apache.commons.configuration2.tree.NodeModel;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.config.HugeConfig;
import org.slf4j.Logger;

public final class ConfigUtil {

    private static final Logger LOG = Log.logger(ConfigUtil.class);

    private static final String NODE_GRAPHS = "graphs";
    private static final String CONF_SUFFIX = ".properties";
    private static final String CHARSET = "UTF-8";

    public static void checkGremlinConfig(String conf) {
        Parameters params = new Parameters();
        try {

            FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
                    new FileBasedConfigurationBuilder(YAMLConfiguration.class)
                            .configure(params.fileBased().setFileName(conf));
            YAMLConfiguration config = (YAMLConfiguration) builder.getConfiguration();

            List<HierarchicalConfiguration<ImmutableNode>> nodes =
                    config.childConfigurationsAt(
                            NODE_GRAPHS);
            if (nodes == null || nodes.isEmpty()) {
                return;
            }
            E.checkArgument(nodes.size() == 1,
                            "Not allowed to specify multiple '%s' " +
                            "nodes in config file '%s'", NODE_GRAPHS, conf);

            ImmutableNode root = null;
            NodeHandler<ImmutableNode> nodeHandler = null;
            for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                NodeModel<ImmutableNode> nodeModel = node.getNodeModel();
                E.checkArgument(nodeModel != null &&
                                (nodeHandler = nodeModel.getNodeHandler()) != null &&
                                (root = nodeHandler.getRootNode()) != null,
                                "Node '%s' must contain root", node);
            }
        } catch (ConfigurationException e) {
            throw new HugeException("Failed to load yaml config file '%s'",
                                    conf);
        }
    }

    public static Map<String, String> scanGraphsDir(String graphsDirPath) {
        LOG.info("Scanning option 'graphs' directory '{}'", graphsDirPath);
        // Validate and normalize the path to prevent path traversal attacks
        File graphsDir = validateAndNormalizePath(graphsDirPath);
        E.checkArgument(graphsDir.exists() && graphsDir.isDirectory(),
                        "Please ensure the path '%s' of option 'graphs' " +
                        "exist and it's a directory", graphsDir);
        File[] confFiles = graphsDir.listFiles((dir, name) -> {
            return name.endsWith(CONF_SUFFIX);
        });
        E.checkNotNull(confFiles, "graph configuration files");
        Map<String, String> graphConfs = InsertionOrderUtil.newMap();
        for (File confFile : confFiles) {
            // NOTE: use file name as graph name
            String name = StringUtils.substringBefore(confFile.getName(),
                                                      ConfigUtil.CONF_SUFFIX);
            HugeFactory.checkGraphName(name, confFile.getPath());
            graphConfs.put(name, confFile.getPath());
        }
        return graphConfs;
    }

    public static String writeToFile(String dir, String graphName,
                                     HugeConfig config) {
        // Validate and normalize the directory path
        File file = validateAndNormalizePath(dir);
        E.checkArgument(file.exists(),
                        "The directory '%s' must exist", dir);
        // Validate graph name to prevent path traversal
        validateGraphName(graphName);
        String fileName = file.getPath() + File.separator + graphName + CONF_SUFFIX;
        try {
            File newFile = FileUtils.getFile(fileName);
            config.save(newFile);
            LOG.info("Write HugeConfig to file: '{}'", fileName);
        } catch (ConfigurationException e) {
            throw new HugeException("Failed to write HugeConfig to file '%s'",
                                    e, fileName);
        }

        return fileName;
    }

    public static void deleteFile(File file) {
        if (file == null || !file.exists()) {
            return;
        }
        try {
            FileUtils.forceDelete(file);
        } catch (IOException e) {
            throw new HugeException("Failed to delete HugeConfig file '%s'",
                                    e, file);
        }
    }

    public static PropertiesConfiguration buildConfig(String configText) {
        E.checkArgument(StringUtils.isNotEmpty(configText),
                        "The config text can't be null or empty");
        PropertiesConfiguration propConfig = new PropertiesConfiguration();
        try {
            Reader in = new StringReader(configText);
            propConfig.read(in);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read config options", e);
        }
        return propConfig;
    }

    /**
     * Validate and normalize file path to prevent path traversal attacks
     */
    private static File validateAndNormalizePath(String pathString) {
        E.checkArgument(StringUtils.isNotEmpty(pathString),
                        "Path cannot be null or empty");

        try {
            Path path = Paths.get(pathString).normalize().toAbsolutePath();
            String normalizedPath = path.toString();

            // Check for path traversal patterns
            E.checkArgument(!normalizedPath.contains(".."),
                            "Path traversal not allowed: %s", pathString);

            return path.toFile();
        } catch (Exception e) {
            throw new HugeException("Invalid path: %s", e, pathString);
        }
    }

    /**
     * Validate graph name to prevent path traversal in file names
     */
    private static void validateGraphName(String graphName) {
        E.checkArgument(StringUtils.isNotEmpty(graphName),
                        "Graph name cannot be null or empty");
        E.checkArgument(!graphName.contains(".."),
                        "Graph name cannot contain '..': %s", graphName);
        E.checkArgument(!graphName.contains("/") && !graphName.contains("\\"),
                        "Graph name cannot contain path separators: %s", graphName);
        E.checkArgument(graphName.matches("^[a-zA-Z0-9_\\-]+$"),
                        "Graph name can only contain letters, numbers, hyphens and underscores: %s",
                        graphName);
    }

    /**
     * Serialize a HugeConfig to a JSON string for frontend consumption.
     * <p>
     * Sensitive configuration keys (containing keywords such as "password",
     * "secret", "token", "credential", "auth.key") are excluded to prevent
     * accidental exposure of credentials through the API response.
     */
    public static String writeConfigToString(HugeConfig config) {
        Map<String, Object> configMap = new HashMap<>();
        Iterator<String> iterator = config.getKeys();
        while (iterator.hasNext()) {
            String key = iterator.next();
            if (isSensitiveKey(key)) {
                continue;
            }
            configMap.put(key, config.getProperty(key));
        }
        return JsonUtil.toJson(configMap);
    }

    /**
     * Returns true if the config key looks like it may contain sensitive data.
     * Uses a case-insensitive keyword blacklist.
     */
    private static boolean isSensitiveKey(String key) {
        if (key == null) {
            return false;
        }
        String lower = key.toLowerCase();
        return lower.contains("password") ||
               lower.contains("secret")   ||
               lower.contains("token")    ||
               lower.contains("credential") ||
               lower.contains("private_key") ||
               lower.contains("auth.key");
    }
}
