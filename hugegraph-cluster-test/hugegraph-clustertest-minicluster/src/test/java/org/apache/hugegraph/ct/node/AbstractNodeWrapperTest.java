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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AbstractNodeWrapperTest {

    private static final String START_LINE = "node is ready";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testStartedRequiresLiveProcess() throws Exception {
        TestNodeWrapper wrapper = new TestNodeWrapper(newNodeDirectory());
        Files.write(Paths.get(wrapper.getLogPath()),
                    START_LINE.getBytes(StandardCharsets.UTF_8));

        Assert.assertFalse(wrapper.isStarted());

        Process process = startSleeperProcess();
        wrapper.attach(process);
        try {
            Assert.assertTrue(process.isAlive());
            Assert.assertTrue(wrapper.isStarted());
        } finally {
            process.destroyForcibly();
            process.waitFor(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testStopDumpsLogOfUnexpectedlyExitedProcess() throws Exception {
        TestNodeWrapper wrapper = new TestNodeWrapper(newNodeDirectory());
        String failure = "fatal startup failure";
        Files.write(Paths.get(wrapper.getLogPath()),
                    failure.getBytes(StandardCharsets.UTF_8));
        Process process = startExitedProcess();
        wrapper.attach(process);

        PrintStream originalOut = System.out;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            System.setOut(new PrintStream(output, true,
                                          StandardCharsets.UTF_8.name()));
            wrapper.stop();
        } finally {
            System.setOut(originalOut);
        }

        String diagnostics = output.toString(StandardCharsets.UTF_8.name());
        Assert.assertTrue(diagnostics, diagnostics.contains(failure));
    }

    private File newNodeDirectory() throws Exception {
        File nodeDirectory = temporaryFolder.newFolder();
        File logDirectory = new File(nodeDirectory, "logs");
        Assert.assertTrue(logDirectory.mkdir());
        return nodeDirectory;
    }

    private static Process startSleeperProcess() throws Exception {
        String java = javaExecutable();
        String classpath = testClassesPath();
        return new ProcessBuilder(java, "-cp", classpath,
                                  Sleeper.class.getName()).start();
    }

    private static Process startExitedProcess() throws Exception {
        Process process = new ProcessBuilder(javaExecutable(), "-version").start();
        Assert.assertTrue(process.waitFor(10, TimeUnit.SECONDS));
        return process;
    }

    private static String javaExecutable() {
        String executable = System.getProperty("os.name").startsWith("Windows") ?
                            "java.exe" : "java";
        return Paths.get(System.getProperty("java.home"), "bin", executable)
                    .toString();
    }

    private static String testClassesPath() throws URISyntaxException {
        return Paths.get(AbstractNodeWrapperTest.class.getProtectionDomain()
                                                     .getCodeSource()
                                                     .getLocation()
                                                     .toURI())
                    .toString();
    }

    public static class Sleeper {

        public static void main(String[] args) throws Exception {
            TimeUnit.SECONDS.sleep(30);
        }
    }

    private static class TestNodeWrapper extends AbstractNodeWrapper {

        private File nodeDirectory;

        private TestNodeWrapper(File nodeDirectory) {
            this.nodeDirectory = nodeDirectory;
            this.startLine = START_LINE;
        }

        private void attach(Process process) {
            this.instance = process;
        }

        @Override
        public void start() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getID() {
            return "TestNode";
        }

        @Override
        public String getNodePath() {
            if (this.nodeDirectory == null) {
                return System.getProperty("java.io.tmpdir") + File.separator;
            }
            return this.nodeDirectory.getAbsolutePath() + File.separator;
        }
    }
}
