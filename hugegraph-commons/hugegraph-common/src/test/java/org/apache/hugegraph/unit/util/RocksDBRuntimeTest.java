/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.unit.util;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.ProtectionDomain;

import javax.tools.ToolProvider;

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.RocksDBRuntime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RocksDBRuntimeTest {

    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void testStandardAndUnrelatedMarkerOrigin() throws Exception {
        Path rocks = this.temporary.newFolder("standard").toPath();
        Path marker = this.temporary.newFolder("unrelated").toPath();
        compile(rocks, "RocksDB");
        compile(marker, "SidePluginRepo");
        verifyWithLoader("rocksdb", rocks);
        verifyWithLoader("rocksdb", rocks, marker);
        Assert.assertThrows(IllegalStateException.class, () -> verifyWithLoader("topling", rocks, marker));
    }

    @Test
    public void testToplingRequiresSameOriginAndMatchingConfiguration() throws Exception {
        Path runtime = this.temporary.newFolder("topling").toPath();
        compile(runtime, "RocksDB");
        compile(runtime, "SidePluginRepo");
        verifyWithLoader("topling", runtime);
        Assert.assertThrows(IllegalStateException.class, () -> verifyWithLoader("rocksdb", runtime));
        Assert.assertThrows(IllegalArgumentException.class, () -> verifyWithLoader("invalid", runtime));
    }

    @Test
    public void testMissingContextRuntimeFallsBackToDefiningLoader() throws Exception {
        for (String provider : new String[]{"rocksdb", "topling"}) {
            Path runtime = this.temporary.newFolder("fallback-" + provider).toPath();
            compile(runtime, "RocksDB");
            if ("topling".equals(provider)) {
                compile(runtime, "SidePluginRepo");
            }
            verifyWithDefiningLoader(provider, null, runtime);
            try (URLClassLoader isolated = new URLClassLoader(new URL[0], null)) {
                verifyWithDefiningLoader(provider, isolated, runtime);
                String mismatch = "topling".equals(provider) ? "rocksdb" : "topling";
                Assert.assertThrows(IllegalStateException.class,
                                    () -> verifyWithDefiningLoader(mismatch, isolated, runtime));
            }
        }
    }

    @Test
    public void testContextLinkageFailureIsNotHiddenByFallback() throws Exception {
        Path runtime = this.temporary.newFolder("fallback-linkage").toPath();
        compile(runtime, "RocksDB");
        LinkageError failure = new NoClassDefFoundError("broken context runtime dependency");
        ClassLoader broken = new ClassLoader(null) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if ("org.rocksdb.RocksDB".equals(name)) {
                    throw failure;
                }
                return super.loadClass(name, resolve);
            }
        };
        Assert.assertSame(failure, Assert.assertThrows(LinkageError.class,
                                                       () -> verifyWithDefiningLoader("rocksdb", broken, runtime)));
    }

    @Test
    public void testContextProviderMismatchDoesNotSelectDefiningRuntime() throws Exception {
        Path standard = this.temporary.newFolder("preferred-context").toPath();
        Path topling = this.temporary.newFolder("unused-defining").toPath();
        compile(standard, "RocksDB");
        compile(topling, "RocksDB");
        compile(topling, "SidePluginRepo");
        try (URLClassLoader context = new URLClassLoader(new URL[]{standard.toUri().toURL()}, null)) {
            verifyWithDefiningLoader("rocksdb", context, topling);
            Assert.assertThrows(IllegalStateException.class,
                                () -> verifyWithDefiningLoader("topling", context, topling));
        }
    }

    @Test
    public void testMissingCodeSourceFailsWithControlledError() throws Exception {
        Path runtime = this.temporary.newFolder("no-origin").toPath();
        compile(runtime, "RocksDB");
        byte[] bytes = Files.readAllBytes(runtime.resolve("org/rocksdb/RocksDB.class"));
        ClassLoader loader = new ClassLoader(null) {
            @Override
            protected Class<?> findClass(String name) throws ClassNotFoundException {
                if ("org.rocksdb.RocksDB".equals(name)) {
                    return this.defineClass(name, bytes, 0, bytes.length, new ProtectionDomain(null, null));
                }
                throw new ClassNotFoundException(name);
            }
        };
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(loader);
            for (String provider : new String[]{"rocksdb", "topling"}) {
                IllegalStateException failure = Assert.assertThrows(IllegalStateException.class,
                                                                    () -> RocksDBRuntime.verify(provider));
                Assert.assertTrue(failure.getMessage().contains("Cannot verify RocksDB runtime origin"));
            }
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    private static void verifyWithDefiningLoader(String provider, ClassLoader context, Path runtime) throws Exception {
        String resource = "org/apache/hugegraph/util/RocksDBRuntime.class";
        Path target = runtime.resolve(resource);
        Files.createDirectories(target.getParent());
        try (InputStream bytes = RocksDBRuntime.class.getClassLoader().getResourceAsStream(resource)) {
            Files.copy(bytes, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader defining = new URLClassLoader(new URL[]{runtime.toUri().toURL()}, null)) {
            Thread.currentThread().setContextClassLoader(context);
            Class<?> verifier = Class.forName(RocksDBRuntime.class.getName(), true, defining);
            try {
                verifier.getMethod("verify", String.class).invoke(null, provider);
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause instanceof Error) {
                    throw (Error) cause;
                }
                throw (Exception) cause;
            }
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    private static void compile(Path root, String name) throws Exception {
        Path source = root.resolve("org/rocksdb/" + name + ".java");
        Files.createDirectories(source.getParent());
        // Loading these classes must not initialize them or native libraries.
        String code = "package org.rocksdb; public class " + name +
                      " { static { if (true) throw new AssertionError(\"initialized\"); } }";
        Files.write(source, code.getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(0, ToolProvider.getSystemJavaCompiler().run(null, null, null, source.toString()));
    }

    private static void verifyWithLoader(String provider, Path... roots) throws Exception {
        URL[] urls = new URL[roots.length];
        for (int i = 0; i < roots.length; i++) {
            urls[i] = roots[i].toUri().toURL();
        }
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader loader = new URLClassLoader(urls, null)) {
            Thread.currentThread().setContextClassLoader(loader);
            RocksDBRuntime.verify(provider);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }
}
