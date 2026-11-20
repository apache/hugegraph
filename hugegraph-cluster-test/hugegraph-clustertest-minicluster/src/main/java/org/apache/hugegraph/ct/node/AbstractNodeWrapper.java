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
import java.io.InputStream;
import java.io.UncheckedIOException;
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
    // Where in the startup log the running process began to write
    private long startupLogOffset;

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
                // Retry once, files may still be held by a stopping process
                TimeUnit.SECONDS.sleep(1);
                PathUtils.deleteDirectory(Paths.get(getNodePath()));
            } catch (IOException e) {
                LOG.warn("Fail to delete node dir {}", getNodePath(), e);
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

        try (InputStream log = Files.newInputStream(Paths.get(getLogPath()))) {
            // Skip what an earlier run of this node wrote before a restart
            log.skipNBytes(this.startupLogOffset);
            Scanner sc = new Scanner(log, StandardCharsets.UTF_8.name());
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                if (line.contains(startLine)) return true;
            }
        } catch (IOException ignored) {
        }
        return false;
    }

    /**
     * Stop the process and start it again in the same directory, so the node
     * comes back with its data. {@link #stop()} is for the end of a test: it
     * also removes the directory.
     */
    public void restart() {
        if (this.instance != null && this.instance.isAlive()) {
            this.instance.destroy();
            try {
                if (!this.instance.waitFor(20, TimeUnit.SECONDS)) {
                    this.instance.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while stopping " + getID(), e);
            }
        }
        try {
            Path log = Paths.get(getLogPath());
            this.startupLogOffset = Files.isRegularFile(log) ? Files.size(log) : 0L;
        } catch (IOException e) {
            throw new AssertionError("Can't read the startup log of " + getID(), e);
        }
        this.start();
    }

    protected String moduleOptions(Path options) throws IOException {
        if (!Files.isRegularFile(options) || !Files.isReadable(options)) {
            throw new IOException("Missing or unreadable JVM module options: " + options);
        }
        return "@" + options.toAbsolutePath();
    }

    public void stop() {
        if (this.instance == null) {
            return;
        }
        if (!this.instance.isAlive()) {
            System.out.printf("[cluster-test] %s stopped unexpectedly: %s%n",
                              getID(), processStatus());
            if (dumpRuntimeLogs()) {
                deleteDir();
            }
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
        if (dumpRuntimeLogs()) {
            deleteDir();
        }
    }

    private boolean dumpRuntimeLogs() {
        Path startupLog = Paths.get(getLogPath());
        try {
            if (this.instance.isAlive() || !Files.isReadable(startupLog)) {
                throw new IOException("Node still running or startup log unavailable: " + getID());
            }
            // Stream complete text logs into the suite output before deletion.
            // The suite's existing checker validates them without an archive.
            try (Stream<Path> logs = Files.walk(startupLog.getParent())) {
                for (Path log : (Iterable<Path>) logs.filter(Files::isRegularFile)::iterator) {
                    if (!log.getFileName().toString().matches(".*(\\.log(\\.[0-9]+)?|\\.out|\\.err|\\.txt)")) {
                        continue;
                    }
                    System.out.println("[cluster-test] runtime log: " + log);
                    try (Stream<String> lines = Files.lines(log, StandardCharsets.UTF_8)) {
                        lines.forEach(System.out::println);
                    }
                }
            }
            return true;
        } catch (IOException | UncheckedIOException e) {
            // Keep the original files and continue stopping the other nodes.
            // This explicit marker makes the suite's log checker fail.
            System.out.println("Unable to inspect cluster runtime logs: " + e.getMessage());
            return false;
        }
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
