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

package org.apache.hugegraph.unit.core;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.auth.HugeFactoryAuthProxy;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.job.GremlinJob;
import org.apache.hugegraph.job.JobBuilder;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.security.HugeSecurityManager;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.hugegraph.util.JsonUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class SecurityManagerTest {

    private static HugeGraph graph;
    private static final HugeSecurityManager sm = new HugeSecurityManager();

    @BeforeClass
    public static void init() {
        graph = loadGraph(false);
        runGremlinJob("1 + 1");
        System.setSecurityManager(new HugeSecurityManager());
    }

    @AfterClass
    public static void clear() throws Exception {
        System.setSecurityManager(null);
        graph.clearBackend();
        graph.close();
        // Stop daemon thread
        HugeFactory.shutdown(30L);
    }

    @Ignore("Enable it after 1.5.0")
    public void testProcessImplMethodAccess() throws Exception {
        new HugeFactoryAuthProxy();
        Class<?> clz = Class.forName("java.lang.ProcessImpl");
        Assert.assertThrows(NoSuchMethodException.class, () -> {
            clz.getDeclaredMethod("start", String[].class, Map.class, String.class,
                                  ProcessBuilder.Redirect[].class, boolean.class);
        });
    }

    @Ignore("Enable it after 1.5.0")
    public void testThreadFieldAccess() throws Exception {
        new HugeFactoryAuthProxy();
        Class<?> clz = Class.forName("java.lang.Thread");
        Assert.assertThrows(NoSuchFieldException.class, () -> {
            clz.getDeclaredField("name");
        });
    }

    @Test
    public void testNormal() {
        String result = runGremlinJob("g.V()");
        Assert.assertEquals("[]", result);

        result = runGremlinJob("1 + 2");
        Assert.assertEquals("3", result);
    }

    @Test
    public void testPermission() {
        String result = runGremlinJob("System.setSecurityManager(null)");
        assertError(result, "Not allowed to access denied permission via Gremlin");
    }

    @Test
    public void testClassLoader() {
        String result = runGremlinJob("System.getSecurityManager().checkCreateClassLoader()");
        assertError(result, "Not allowed to create class loader via Gremlin");
    }

    @Test
    public void testThread() {
        // access a thread group
        new Thread();
        String result = runGremlinJob("new Thread()");
        assertError(result, "Not allowed to access thread group via Gremlin");

        // access thread
        Thread.currentThread().checkAccess();
        result = runGremlinJob("Thread.currentThread().stop()");
        assertError(result, "Not allowed to access thread via Gremlin");
    }

    @Test
    public void testExit() {
        String result = runGremlinJob("System.exit(-1)");
        assertError(result, "Not allowed to call System.exit() via Gremlin");
    }

    @Test
    public void testFile() {
        // read file
        try (FileInputStream fis = new FileInputStream("")) {
            // pass
        } catch (IOException ignored) {
            // ignored exception
        }
        String result = runGremlinJob("new FileInputStream(new File(\"\"))");
        assertError(result, "Not allowed to read file via Gremlin");

        // read file
        String pom = System.getProperty("user.dir") + "/a.groovy";
        try (FileInputStream fis = new FileInputStream(pom)) {
            // pass
        } catch (IOException ignored) {
            // ignored exception
        }
        result = runGremlinJob(String.format(
                "new FileInputStream(new File(\"%s\"))", pom));
        assertError(result, "(No such file or directory)");

        // read file fd
        @SuppressWarnings({"unused", "resource"})
        FileInputStream fis = new FileInputStream(FileDescriptor.in);
        result = runGremlinJob("new FileInputStream(FileDescriptor.in)");
        assertError(result, "Not allowed to read fd via Gremlin");

        sm.checkRead("", new Object());
        result = runGremlinJob("System.getSecurityManager()" +
                               ".checkRead(\"\", new Object())");
        assertError(result, "Not allowed to read file via Gremlin");

        // write file
        try (FileOutputStream fos = new FileOutputStream("")) {
            // pass
        } catch (IOException ignored) {
            // ignored IOException
        }
        result = runGremlinJob("new FileOutputStream(new File(\"\"))");
        assertError(result, "Not allowed to write file via Gremlin");

        // write file fd
        @SuppressWarnings({"unused", "resource"})
        FileOutputStream fos = new FileOutputStream(FileDescriptor.out);
        result = runGremlinJob("new FileOutputStream(FileDescriptor.out)");
        assertError(result, "Not allowed to write fd via Gremlin");

        // delete file
        new File("").delete();
        result = runGremlinJob("new File(\"\").delete()");
        assertError(result, "Not allowed to delete file via Gremlin");

        // get absolute path
        new File("").getAbsolutePath();
        result = runGremlinJob("new File(\"\").getAbsolutePath()");
        assertError(result, "Not allowed to access " +
                            "system property(user.dir) via Gremlin");
    }

    @Test
    public void testSocket() throws IOException {
        /*
         * NOTE: if remove this, gremlin job will call System.loadLibrary("net")
         * then throw exception because checkLink failed
         */
        try (ServerSocket serverSocket = new ServerSocket(8200)) {
            // pass
        } catch (IOException ignored) {
            // ignored UnsatisfiedLinkError
        }
        String result = runGremlinJob("new ServerSocket(8200)");
        assertError(result, "Not allowed to listen socket via Gremlin");

        /*
         * Test accept must go through socket.listen(), so will throw exception
         * from checkListen
         */
        sm.checkAccept("localhost", 8200);
        result = runGremlinJob("System.getSecurityManager()" +
                               ".checkAccept(\"localhost\", 8200)");
        assertError(result, "Not allowed to accept socket via Gremlin");

        try (Socket socket = new Socket()) {
            SocketAddress address = new InetSocketAddress("localhost", 8200);
            socket.connect(address);
        } catch (ConnectException ignored) {
            // ignored ConnectException
        }
        result = runGremlinJob("new Socket().connect(" +
                               "new InetSocketAddress(\"localhost\", 8200))");
        assertError(result, "Not allowed to connect socket via Gremlin");

        sm.checkConnect("localhost", 8200, new Object());
        result = runGremlinJob("System.getSecurityManager()" +
                               ".checkConnect(\"localhost\", 8200, " +
                               "new Object())");
        assertError(result, "Not allowed to connect socket via Gremlin");

        sm.checkMulticast(InetAddress.getByAddress(new byte[]{0, 0, 0, 0}));
        result = runGremlinJob("bs = [0, 0, 0, 0] as byte[];" +
                               "System.getSecurityManager()" +
                               ".checkMulticast(InetAddress.getByAddress(bs))");
        assertError(result, "Not allowed to multicast via Gremlin");

        sm.checkMulticast(InetAddress.getByAddress(new byte[]{0, 0, 0, 0}),
                          (byte) 1);
        result = runGremlinJob("bs = [0, 0, 0, 0] as byte[]; ttl = (byte) 1;" +
                               "System.getSecurityManager()" +
                               ".checkMulticast(InetAddress.getByAddress(" +
                               "bs), ttl)");
        assertError(result, "Not allowed to multicast via Gremlin");

        sm.checkSetFactory();
        result = runGremlinJob("System.getSecurityManager().checkSetFactory()");
        assertError(result, "Not allowed to set socket factory via Gremlin");
    }

    @Test
    public void testExec() throws IOException, InterruptedException {
        Process process = Runtime.getRuntime().exec("ls");
        process.waitFor();

        String result = runGremlinJob("process=Runtime.getRuntime().exec(" +
                                      "'ls'); process.waitFor()");
        assertError(result, "Not allowed to execute command via Gremlin");
    }

    @Test
    public void testLink() {
        try {
            System.loadLibrary("hugegraph.jar");
        } catch (UnsatisfiedLinkError ignored) {
            // ignored UnsatisfiedLinkError
        }

        String result = runGremlinJob("Runtime.getRuntime().loadLibrary" +
                                      "(\"test.jar\")");
        assertError(result, "Not allowed to link library via Gremlin");
    }

    @Test
    public void testProperties() {
        System.getProperties();
        String result = runGremlinJob("System.getProperties()");
        assertError(result, "Not allowed to access system properties via Gremlin");

        System.getProperty("java.version");
        result = runGremlinJob("System.getProperty(\"java.version\")");
        assertError(result, "Not allowed to access system property(java.version) via Gremlin");

        result = runGremlinJob("System.getProperty(\"socksProxyHost\")");
        assertError(result, "Not allowed to access system property(socksProxyHost) via Gremlin");

        result = runGremlinJob("System.getProperty(\"file.encoding\")");
        assertError(result, "Not allowed to access system property(file.encoding) via Gremlin");
    }

    @Test
    public void testPrintJobAccess() {
        sm.checkPrintJobAccess();
        String result = runGremlinJob("System.getSecurityManager()" +
                                      ".checkPrintJobAccess()");
        assertError(result, "Not allowed to print job via Gremlin");
    }

    @Test
    public void testPackageDefinition() {
        sm.checkPackageDefinition("org.apache.hugegraph.util");
    }

    @Test
    public void testSecurityAccess() {
        sm.checkSecurityAccess("link");
    }

    private static void assertError(String result, String message) {
        Assert.assertTrue(result, result.endsWith(message) || result.contains(message));
    }

    private static String runGremlinJob(String gremlin) {
        JobBuilder<Object> builder = JobBuilder.of(graph);
        Map<String, Object> input = new HashMap<>();
        input.put("gremlin", gremlin);
        input.put("bindings", ImmutableMap.of());
        input.put("language", "gremlin-groovy");
        input.put("aliases", ImmutableMap.of());
        builder.name("test-gremlin-job")
               .input(JsonUtil.toJson(input))
               .job(new GremlinJob());
        HugeTask<?> task = builder.schedule();
        try {
            task = graph.taskScheduler().waitUntilTaskCompleted(task.id(), 10);
        } catch (TimeoutException e) {
            throw new HugeException("Wait for task timeout: %s", e, task);
        }
        return task.result();
    }

    private static HugeGraph loadGraph(boolean needClear) {
        HugeConfig config = FakeObjects.newConfig();
        HugeGraph graph = HugeFactory.open(config);

        if (needClear) {
            graph.clearBackend();
        }
        graph.initBackend();
        graph.serverStarted(GlobalMasterInfo.master("server1"));

        return graph;
    }
}
