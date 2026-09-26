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

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

public class NodeProcessTest {

    @Test
    public void testNewCommandArchivesOldReadyMarker() throws Exception {
        Path directory = Files.createTempDirectory("hugegraph-node-log-");
        try {
            ProcessNode node = new ProcessNode(null, directory);
            node.startLine = "OLD_READY";
            node.configPath = directory.toString();
            Path log = directory.resolve("start.log");
            Files.writeString(log, "OLD_READY\n");
            node.runCmd(Collections.singletonList("test-command"), log.toFile());
            Assert.assertEquals("test-command", Files.readString(log).trim());
            try (Stream<Path> files = Files.list(directory)) {
                Path archive = files.filter(path -> path.getFileName().toString()
                                                        .startsWith("start.log.previous-"))
                                    .findFirst().orElseThrow(AssertionError::new);
                Assert.assertEquals("OLD_READY", Files.readString(archive).trim());
            }
        } finally {
            try (Stream<Path> files = Files.list(directory)) {
                for (Path file : (Iterable<Path>) files::iterator) {
                    Files.delete(file);
                }
            }
            Files.delete(directory);
        }
    }

    @Test(timeout = 40000)
    public void testForcedStopPreservesFailureDirectory() throws Exception {
        checkStop(true, false, false);
    }

    @Test(timeout = 20000)
    public void testGracefulStopDeletesNormalDirectory() throws Exception {
        checkStop(false, true, false);
    }

    @Test(timeout = 40000)
    public void testInterruptDuringStopStillReapsChild() throws Exception {
        checkStop(true, false, true);
    }

    @Test
    public void testSurvivingForcedStopFailsAndRetainsDirectory() throws Exception {
        Path directory = Files.createTempDirectory("hugegraph-node-survivor-");
        try {
            ProcessNode node = new ProcessNode(new StubbornProcess(), directory);
            IllegalStateException failure = Assert.assertThrows(IllegalStateException.class,
                    () -> node.stop(true));
            Assert.assertTrue(failure.getMessage().contains("survived forced stop"));
            Assert.assertTrue(Files.exists(directory));
        } finally {
            Files.delete(directory);
        }
    }

    private static void checkStop(boolean slowShutdown, boolean deleteData,
                                  boolean interrupt) throws Exception {
        Path directory = Files.createTempDirectory("hugegraph-node-stop-");
        Process process = null;
        Thread interrupter = null;
        try {
            process = new ProcessBuilder(
                    Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                    "-cp", System.getProperty("java.class.path"), NodeProcessTest.class.getName(),
                    directory.toString(), Boolean.toString(slowShutdown))
                    .redirectErrorStream(true).redirectOutput(directory.resolve("child.log").toFile())
                    .start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!Files.exists(directory.resolve("ready"))) {
                Assert.assertTrue("Child exited before ready", process.isAlive());
                Assert.assertTrue("Child readiness timed out", System.nanoTime() < deadline);
                Thread.sleep(10);
            }
            ProcessNode node = new ProcessNode(process, directory);
            if (interrupt) {
                Thread caller = Thread.currentThread();
                interrupter = new Thread(() -> {
                    try {
                        Thread.sleep(100);
                        caller.interrupt();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                interrupter.start();
            }
            node.stop(deleteData);
            Assert.assertEquals(interrupt, Thread.currentThread().isInterrupted());
            Thread.interrupted();
            Assert.assertFalse("Node process survived stop", process.isAlive());
            Assert.assertEquals(!deleteData, Files.exists(directory));
            if (!deleteData) {
                Assert.assertTrue(Files.exists(directory.resolve("child.log")));
            }
        } finally {
            Thread.interrupted();
            if (interrupter != null) {
                interrupter.interrupt();
                interrupter.join(1000);
            }
            if (process != null && process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
            if (Files.exists(directory)) {
                Files.deleteIfExists(directory.resolve("ready"));
                Files.deleteIfExists(directory.resolve("child.log"));
                Files.delete(directory);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (Boolean.parseBoolean(args[1])) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    Thread.sleep(60000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }
        Files.createFile(Paths.get(args[0], "ready"));
        Thread.sleep(60000);
    }

    private static class StubbornProcess extends Process {

        @Override
        public OutputStream getOutputStream() {
            return OutputStream.nullOutputStream();
        }

        @Override
        public InputStream getInputStream() {
            return InputStream.nullInputStream();
        }

        @Override
        public InputStream getErrorStream() {
            return InputStream.nullInputStream();
        }

        @Override
        public int waitFor() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean waitFor(long timeout, TimeUnit unit) {
            return false;
        }

        @Override
        public int exitValue() {
            throw new IllegalThreadStateException("still alive");
        }

        @Override
        public boolean isAlive() {
            return true;
        }

        @Override
        public void destroy() {
        }

        @Override
        public Process destroyForcibly() {
            return this;
        }
    }

    private static class ProcessNode extends AbstractNodeWrapper {

        private final Path directory;

        ProcessNode(Process process, Path directory) {
            this.instance = process;
            this.directory = directory;
        }

        @Override
        public void start() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getID() {
            return "process-test";
        }

        @Override
        public String getNodePath() {
            return this.directory == null ? super.getNodePath() : this.directory.toString();
        }
    }
}
