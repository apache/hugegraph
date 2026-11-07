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

package org.apache.hugegraph.dist;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.plugin.HugeGraphPlugin;
import org.junit.Assert;
import org.junit.Test;

public class RegisterUtilPluginTest {

    @Test
    public void testPluginRegisterFailureKeepsCause() throws Exception {
        Path dir = Files.createTempDirectory("hugegraph-plugin-test");
        Path services = dir.resolve("META-INF/services/" +
                                    HugeGraphPlugin.class.getName());
        Files.createDirectories(services.getParent());
        Files.write(services, FailingPlugin.class.getName()
                                             .getBytes(StandardCharsets.UTF_8));

        Thread thread = Thread.currentThread();
        ClassLoader origin = thread.getContextClassLoader();
        try (URLClassLoader loader = new URLClassLoader(
                new URL[]{dir.toUri().toURL()}, origin)) {
            thread.setContextClassLoader(loader);

            HugeException e = Assert.assertThrows(HugeException.class,
                                                  RegisterUtil::registerPlugins);
            Assert.assertEquals("Failed to load plugin 'failing-test-plugin'",
                                e.getMessage());
            Assert.assertSame(FailingPlugin.FAILURE, e.getCause());
        } finally {
            thread.setContextClassLoader(origin);
            FileUtils.deleteQuietly(dir.toFile());
        }
    }

    public static class FailingPlugin implements HugeGraphPlugin {

        private static final RuntimeException FAILURE =
                new IllegalStateException("Failed to register test plugin");

        @Override
        public String name() {
            return "failing-test-plugin";
        }

        @Override
        public void register() {
            throw FAILURE;
        }

        @Override
        public String supportsMinVersion() {
            return "0.1";
        }

        @Override
        public String supportsMaxVersion() {
            return "1000.0";
        }
    }
}
