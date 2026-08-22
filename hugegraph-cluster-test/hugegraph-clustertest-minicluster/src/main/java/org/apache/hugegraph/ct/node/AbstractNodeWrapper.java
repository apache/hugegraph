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

import static org.apache.hugegraph.ct.base.ClusterConstant.CT_PACKAGE_PATH;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.file.PathUtils;
import org.apache.hugegraph.ct.base.ClusterConstant;
import org.apache.hugegraph.ct.base.EnvUtil;
import org.apache.hugegraph.ct.base.HGTestLogger;
import org.slf4j.Logger;

import lombok.Getter;

public abstract class AbstractNodeWrapper implements BaseNodeWrapper {

    protected final Logger LOG = HGTestLogger.NODE_LOG;

    protected int clusterIndex;
    @Getter
    protected String workPath;
    @Getter
    protected String configPath;
    protected Process instance;
    protected int index;
    protected List<String> fileNames;
    protected String startLine;

    public AbstractNodeWrapper() {
        this.clusterIndex = 1;
        fileNames = new ArrayList<>();
        this.configPath = getNodePath();
    }

    public AbstractNodeWrapper(int clusterIndex, int index) {
        this.clusterIndex = clusterIndex;
        this.index = index;
        fileNames = new ArrayList<>();
        this.configPath = getNodePath();
    }

    /**
     * Node Dir should be created before changing Config
     */
    public void createNodeDir(Path sourcePath, String destDir) {
        try {
            try {
                if (!new File(destDir).exists()) {
                    FileUtils.createParentDirectories(new File(destDir));
                }
            } catch (NoSuchFileException fileException) {
                // Ignored
            }
            // To avoid following symbolic links
            try (Stream<Path> stream = Files.walk(sourcePath)) {
                stream.forEach(source -> {
                    Path relativePath = sourcePath.relativize(source);
                    Path destination = Paths.get(destDir).resolve(relativePath);
                    if (fileNames.contains(relativePath.toString())) {
                        EnvUtil.copyFileToDestination(source, destination);
                    }
                });
            }
        } catch (IOException ioException) {
            LOG.error("Got error copying files to node destination dir", ioException);
            throw new AssertionError();
        }
    }

    public void createLogDir() {
        String logPath = getLogPath();
        try {
            FileUtils.createParentDirectories(new File(logPath));
        } catch (IOException e) {
            LOG.error("Create log dir failed", e);
            throw new AssertionError();
        }
    }

    public void deleteDir() {
        try {
            PathUtils.deleteDirectory(Paths.get(getNodePath()));
        } catch (IOException ex) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("Fail to delete node file", e);
                throw new AssertionError("Delete node dir failed. " + e);
            }
        }
    }

    /**
     * @return (user.dir).id
     */
    @Override
    public String getNodePath() {
        return CT_PACKAGE_PATH + getID() + File.separator;
    }

    @Override
    public String getLogPath() {
        return getNodePath() + ClusterConstant.LOG + File.separator + getID() + "-start.log";
    }

    @Override
    public void updateWorkPath(String workPath) {
        this.workPath = workPath;
    }

    @Override
    public void updateConfigPath(String ConfigPath) {
        this.configPath = ConfigPath;
    }

    @Override
    public boolean isStarted() {
        if (!isAlive()) {
            return false;
        }

        try (Scanner sc = new Scanner(Paths.get(getLogPath()),
                                      StandardCharsets.UTF_8.name())) {
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                if (line.contains(startLine)) return true;
            }
        } catch (IOException ignored) {
        }
        return false;
    }

    public void stop() {
        if (this.instance == null) {
            return;
        }
        if (!this.instance.isAlive()) {
            System.out.printf("[cluster-test] %s stopped unexpectedly: %s%n",
                              getID(), processStatus());
            dumpLog();
            deleteDir();
            return;
        }
        this.instance.destroy();
        try {
            if (!this.instance.waitFor(20, TimeUnit.SECONDS)) {
                this.instance.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Waiting node to shutdown error.", e);
        }
        deleteDir();
    }

    public boolean isAlive() {
        return this.instance != null && this.instance.isAlive();
    }

    public String processStatus() {
        if (this.instance == null) {
            return "not started";
        }
        if (this.instance.isAlive()) {
            return "alive";
        }
        return "exited with code " + this.instance.exitValue();
    }

    public void dumpLog() {
        Path logPath = Paths.get(getLogPath());
        System.out.println("===== " + getID() + " log: " + logPath + " =====");
        if (!Files.exists(logPath)) {
            System.out.println("Log file does not exist");
            return;
        }

        try {
            List<String> lines = Files.readAllLines(logPath, StandardCharsets.UTF_8);
            int start = Math.max(0, lines.size() - 200);
            for (int i = start; i < lines.size(); i++) {
                System.out.println(lines.get(i));
            }
        } catch (IOException e) {
            System.out.println("Failed to read log file: " + e.getMessage());
        }
    }

    protected ProcessBuilder runCmd(List<String> startCmd, File stdoutFile) throws IOException {
        FileUtils.write(stdoutFile,
                        String.join(" ", startCmd) + System.lineSeparator() + System.lineSeparator(),
                        StandardCharsets.UTF_8, true);
        ProcessBuilder processBuilder = new ProcessBuilder(startCmd)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(stdoutFile))
                .redirectError(ProcessBuilder.Redirect.appendTo(stdoutFile));
        processBuilder.directory(new File(configPath));
        return processBuilder;
    }

}
