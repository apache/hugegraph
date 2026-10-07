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

package org.apache.hugegraph.job.computer;

import java.io.File;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.YAMLConfiguration;
import org.apache.commons.configuration2.io.FileHandler;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.configuration2.tree.NodeHandler;
import org.apache.commons.configuration2.tree.NodeModel;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.job.ComputerJob;
import org.apache.hugegraph.job.Job;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.ParameterUtil;
import org.slf4j.Logger;

public abstract class AbstractComputer implements Computer {

    private static final Logger LOG = Log.logger(AbstractComputer.class);

    private static final String HADOOP_HOME = "HADOOP_HOME";
    private static final String COMMON = "common";
    private static final String ENV = "env";
    private static final String COMPUTER_HOME = "computer_home";
    private static final String MINUS_C = "-C";
    private static final String EQUAL = "=";
    private static final String SPACE = " ";

    // TODO: 2022/11/18 wait computer project adapt
    private static final String MAIN_COMMAND =
            "%s/bin/hadoop jar hugegraph-computer.jar " +
            "com.baidu.hugegraph.Computer " +
            "-D libjars=./hugegraph-computer-core.jar";

    public static final String MAX_STEPS = "max_steps";
    public static final int DEFAULT_MAX_STEPS = 5;
    public static final String PRECISION = "precision";
    public static final double DEFAULT_PRECISION = 0.0001D;
    public static final String TIMES = "times";
    public static final int DEFAULT_TIMES = 10;
    public static final String DIRECTION = "direction";
    public static final String DEGREE = "degree";
    public static final long DEFAULT_DEGREE = 100L;

    protected static final String CATEGORY_RANK = "rank";
    protected static final String CATEGORY_COMM = "community";

    private YAMLConfiguration config;
    private Map<String, Object> commonConfig = new HashMap<>();

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        E.checkArgument(parameters.isEmpty(),
                        "Unnecessary parameters: %s", parameters);
    }

    @Override
    public Object call(Job<Object> job, Map<String, Object> parameters) {
        this.checkAndCollectParameters(parameters);
        // Read configuration
        try {
            this.initializeConfig((ComputerJob) job);
        } catch (Exception e) {
            throw new HugeException(
                    "Failed to initialize computer config file", e);
        }

        // Set current computer job's specified parameters
        Map<String, Object> configs = new HashMap<>();
        configs.putAll(this.commonConfig);
        configs.putAll(this.checkAndCollectParameters(parameters));

        // Construct shell command for computer job
        String[] command = this.constructShellCommands(configs);
        LOG.info("Execute computer job: {}", String.join(SPACE, command));

        // Execute current computer
        try {
            ProcessBuilder builder = new ProcessBuilder(command);
            builder.redirectErrorStream(true);
            builder.directory(new File(executeDir()));

            Process process = builder.start();

            StringBuilder output = new StringBuilder();
            try (LineNumberReader reader = new LineNumberReader(
                    new InputStreamReader(
                            process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                }
            }

            int exitCode = process.waitFor();
            if (exitCode == 0) {
                return 0;
            }

            throw new HugeException("The computer job exit with code %s: %s",
                                    exitCode, output);
        } catch (HugeException e) {
            throw e;
        } catch (Throwable e) {
            throw new HugeException("Failed to execute computer job", e);
        }
    }

    private String executeDir() {
        Map<String, Object> envs = this.readEnvConfig();
        E.checkState(envs.containsKey(COMPUTER_HOME),
                     "Expect '%s' in '%s' section", COMPUTER_HOME, ENV);
        return (String) envs.get(COMPUTER_HOME);
    }

    private void initializeConfig(ComputerJob job) throws Exception {
        // Load computer config file
        String configPath = job.computerConfigPath();
        E.checkArgument(configPath.endsWith(".yaml"),
                        "Expect a yaml config file.");

        this.config = new YAMLConfiguration();
        FileHandler fileHandler = new FileHandler(this.config);
        fileHandler.load(configPath);

        // Read common and computer specified parameters
        this.commonConfig = this.readCommonConfig();
    }

    private Map<String, Object> readCommonConfig() {
        return this.readSubConfig(COMMON);
    }

    private Map<String, Object> readEnvConfig() {
        return this.readSubConfig(ENV);
    }

    private Map<String, Object> readSubConfig(String sub) {
        List<HierarchicalConfiguration<ImmutableNode>> nodes =
                this.config.childConfigurationsAt(sub);

        E.checkArgument(nodes.size() >= 1,
                        "'%s' must be contained in config '%s'", sub);

        ImmutableNode root = null;
        NodeHandler<ImmutableNode> nodeHandler;
        Map<String, Object> results = new HashMap<>(nodes.size());
        for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
            NodeModel<ImmutableNode> nodeModel = node.getNodeModel();
            E.checkArgument(nodeModel != null &&
                            (nodeHandler = nodeModel.getNodeHandler()) != null &&
                            (root = nodeHandler.getRootNode()) != null,
                            "Node '%s' must contain root", node);
            results.put(root.getNodeName(), root.getValue());
        }

        return results;
    }

    private String[] constructShellCommands(Map<String, Object> configs) {
        String hadoopHome = System.getenv(HADOOP_HOME);
        String commandPrefix = String.format(MAIN_COMMAND, hadoopHome);
        List<String> command = new ArrayList<>(Arrays.asList(commandPrefix.split(SPACE)));
        command.add(this.name());
        for (Map.Entry<String, Object> entry : configs.entrySet()) {
            command.add(MINUS_C);
            command.add(entry.getKey() + EQUAL + entry.getValue());
        }
        return command.toArray(new String[0]);
    }

    protected abstract Map<String, Object> checkAndCollectParameters(
            Map<String, Object> parameters);

    protected static int maxSteps(Map<String, Object> parameters) {
        if (!parameters.containsKey(MAX_STEPS)) {
            return DEFAULT_MAX_STEPS;
        }
        int maxSteps = ParameterUtil.parameterInt(parameters, MAX_STEPS);
        E.checkArgument(maxSteps > 0,
                        "The value of %s must be > 0, but got %s",
                        MAX_STEPS, maxSteps);
        return maxSteps;
    }

    protected static double precision(Map<String, Object> parameters) {
        if (!parameters.containsKey(PRECISION)) {
            return DEFAULT_PRECISION;
        }
        double precision = ParameterUtil.parameterDouble(parameters, PRECISION);
        E.checkArgument(precision > 0.0D && precision < 1.0D,
                        "The value of %s must be (0, 1), but got %s",
                        PRECISION, precision);
        return precision;
    }

    protected static int times(Map<String, Object> parameters) {
        if (!parameters.containsKey(TIMES)) {
            return DEFAULT_TIMES;
        }
        int times = ParameterUtil.parameterInt(parameters, TIMES);
        E.checkArgument(times > 0,
                        "The value of %s must be > 0, but got %s",
                        TIMES, times);
        return times;
    }

    protected static Directions direction(Map<String, Object> parameters) {
        if (!parameters.containsKey(DIRECTION)) {
            return Directions.BOTH;
        }
        Object direction = ParameterUtil.parameter(parameters, DIRECTION);
        return parseDirection(direction);
    }

    protected static long degree(Map<String, Object> parameters) {
        if (!parameters.containsKey(DEGREE)) {
            return DEFAULT_DEGREE;
        }
        long degree = ParameterUtil.parameterLong(parameters, DEGREE);
        HugeTraverser.checkDegree(degree);
        return degree;
    }

    protected static Directions parseDirection(Object direction) {
        if (direction.equals(Directions.BOTH.toString())) {
            return Directions.BOTH;
        } else if (direction.equals(Directions.OUT.toString())) {
            return Directions.OUT;
        } else if (direction.equals(Directions.IN.toString())) {
            return Directions.IN;
        } else {
            throw new IllegalArgumentException(String.format(
                    "The value of direction must be in [OUT, IN, BOTH], " +
                    "but got '%s'", direction));
        }
    }
}
