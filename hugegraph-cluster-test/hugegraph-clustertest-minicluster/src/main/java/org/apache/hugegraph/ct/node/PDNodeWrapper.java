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

package org.apache.hugegraph.ct.node;

import static org.apache.hugegraph.ct.base.ClusterConstant.CONF_DIR;
import static org.apache.hugegraph.ct.base.ClusterConstant.JAVA_CMD;
import static org.apache.hugegraph.ct.base.ClusterConstant.LOG4J_FILE;
import static org.apache.hugegraph.ct.base.ClusterConstant.PD_JAR_PREFIX;
import static org.apache.hugegraph.ct.base.ClusterConstant.PD_LIB_PATH;
import static org.apache.hugegraph.ct.base.ClusterConstant.PD_TEMPLATE_PATH;
import static org.apache.hugegraph.ct.base.ClusterConstant.getFileInDir;
import static org.apache.hugegraph.ct.base.ClusterConstant.isJava17OrHigher;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PDNodeWrapper extends AbstractNodeWrapper {

    public PDNodeWrapper() {
        super();
        fileNames = new ArrayList<>(Arrays.asList(LOG4J_FILE));
        this.workPath = PD_LIB_PATH;
        this.startLine = "Hugegraph-pd started.";
        createNodeDir(Paths.get(PD_TEMPLATE_PATH), getNodePath() + CONF_DIR + File.separator);
        createLogDir();
    }

    public PDNodeWrapper(int clusterIndex, int index) {
        super(clusterIndex, index);
        this.fileNames = new ArrayList<>(Arrays.asList(LOG4J_FILE));
        this.workPath = PD_LIB_PATH;
        this.startLine = "Hugegraph-pd started.";
        createNodeDir(Paths.get(PD_TEMPLATE_PATH), getNodePath() + CONF_DIR + File.separator);
        createLogDir();
    }

    /*
    workPath is path of JAR package, configPath is path of config files
     */
    @Override
    public void start() {
        try {
            File stdoutFile = new File(getLogPath());
            List<String> startCmd = new ArrayList<>();
            startCmd.add(JAVA_CMD);
            if (!isJava17OrHigher()) {
                LOG.error("Please make sure that the JDK is installed and the version >= 17");
                return;
            }

            String pdNodeJarPath = getFileInDir(workPath, PD_JAR_PREFIX);
            startCmd.addAll(Arrays.asList(
                    "-Dname=HugeGraphPD" + this.index,
                    "-Xms512m",
                    "-Xmx4g",
                    "-XX:+HeapDumpOnOutOfMemoryError",
                    "-XX:HeapDumpPath=" + configPath + "logs",
                    "-Dlog4j.configurationFile=" + configPath + File.separator +
                    CONF_DIR + File.separator + "log4j2.xml",
                    "-Dspring.config.location=" + configPath + CONF_DIR + File.separator +
                    "application.yml",
                    "-jar", pdNodeJarPath));
            ProcessBuilder processBuilder = runCmd(startCmd, stdoutFile);
            this.instance = processBuilder.start();
        } catch (IOException ex) {
            throw new AssertionError("Start node failed. " + ex);
        }
    }

    @Override
    public String getID() {
        return "PD" + this.index;
    }
}
