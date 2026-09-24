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
import static org.apache.hugegraph.ct.base.ClusterConstant.EMPTY_SAMPLE_GROOVY_FILE;
import static org.apache.hugegraph.ct.base.ClusterConstant.EXAMPLE_GROOVY_FILE;
import static org.apache.hugegraph.ct.base.ClusterConstant.EXT_DIR;
import static org.apache.hugegraph.ct.base.ClusterConstant.GREMLIN_DRIVER_SETTING_FILE;
import static org.apache.hugegraph.ct.base.ClusterConstant.GREMLIN_SERVER_FILE;
import static org.apache.hugegraph.ct.base.ClusterConstant.JAVA_CMD;
import static org.apache.hugegraph.ct.base.ClusterConstant.LIB_DIR;
import static org.apache.hugegraph.ct.base.ClusterConstant.LOG4J_FILE;
import static org.apache.hugegraph.ct.base.ClusterConstant.PLUGINS_DIR;
import static org.apache.hugegraph.ct.base.ClusterConstant.REMOTE_OBJECTS_SETTING_FILE;
import static org.apache.hugegraph.ct.base.ClusterConstant.REMOTE_SETTING_FILE;
import static org.apache.hugegraph.ct.base.ClusterConstant.SERVER_LIB_PATH;
import static org.apache.hugegraph.ct.base.ClusterConstant.SERVER_PACKAGE_PATH;
import static org.apache.hugegraph.ct.base.ClusterConstant.SERVER_TEMPLATE_PATH;
import static org.apache.hugegraph.ct.base.ClusterConstant.isJava11OrHigher;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ServerNodeWrapper extends AbstractNodeWrapper {

    private static List<String> hgJars = loadHgJarsOnce();
    public ServerNodeWrapper(int clusterIndex, int index) {
        super(clusterIndex, index);
        this.fileNames = new ArrayList<>(
                List.of(LOG4J_FILE, GREMLIN_SERVER_FILE, GREMLIN_DRIVER_SETTING_FILE,
                        REMOTE_SETTING_FILE, REMOTE_OBJECTS_SETTING_FILE));
        this.workPath = SERVER_LIB_PATH;
        createNodeDir(Paths.get(SERVER_TEMPLATE_PATH), getNodePath() + CONF_DIR + File.separator);
        this.fileNames = new ArrayList<>(List.of(EMPTY_SAMPLE_GROOVY_FILE, EXAMPLE_GROOVY_FILE));
        this.startLine = "INFO: [HttpServer] Started.";
        createNodeDir(Paths.get(SERVER_PACKAGE_PATH), getNodePath());
        createLogDir();
    }

    private static void addJarsToClasspath(File directory, List<String> classpath) {
        if (directory.exists() && directory.isDirectory()) {
            File[] files = directory.listFiles((dir, name) -> name.endsWith(".jar"));
            if (files != null) {
                for (File file : files) {
                    classpath.add(file.getAbsolutePath());
                }
            }
        }
    }

    private static void addOrderedJarsToClasspath(File directory, List<String> classpath) {
        // Add jar starts with hugegraph in proper order
        String path = directory.getAbsolutePath();
        for (String jar : hgJars) {
            classpath.add(path + File.separator + jar);
        }
        if (directory.exists() && directory.isDirectory()) {
            File[] files =
                    directory.listFiles((dir, name) -> name.endsWith(".jar") && !name.contains(
                            "hugegraph"));
            if (files != null) {
                for (File file : files) {
                    classpath.add(file.getAbsolutePath());
                }
            }
        }
    }

    private static List<String> loadHgJarsOnce(){
        ArrayList<String> jars = new ArrayList<>();
        try (InputStream is = ServerNodeWrapper.class.getResourceAsStream("/jar.txt");
             BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                jars.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Collections.unmodifiableList(jars);
    }

    @Override
    public void start() {
        try {
            File stdoutFile = new File(getLogPath());
            List<String> startCmd = new ArrayList<>();
            startCmd.add(JAVA_CMD);
            if (!isJava11OrHigher()) {
                LOG.error("Please make sure that the JDK is installed and the version >= 11");
                return;
            }

            List<String> classpath = new ArrayList<>();
            addOrderedJarsToClasspath(new File(workPath + LIB_DIR), classpath);
            addJarsToClasspath(new File(workPath + EXT_DIR), classpath);
            addJarsToClasspath(new File(workPath + PLUGINS_DIR), classpath);
            String storeClassPath = String.join(":", classpath);

            startCmd.addAll(Arrays.asList(
                    "-Dname=HugeGraphServer" + this.index,
                    "--add-exports=java.base/jdk.internal.reflect=ALL-UNNAMED",
                    "--add-modules=jdk.unsupported",
                    "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
                    "-cp", storeClassPath,
                    "org.apache.hugegraph.dist.HugeGraphServer",
                    "./conf/gremlin-server.yaml",
                    "./conf/rest-server.properties"));
            ProcessBuilder processBuilder = runCmd(startCmd, stdoutFile);
            this.instance = processBuilder.start();
        } catch (IOException ex) {
            throw new AssertionError("Started server node failed. " + ex);
        }
    }

    @Override
    public String getID() {
        return "Server" + this.index;
    }
}
