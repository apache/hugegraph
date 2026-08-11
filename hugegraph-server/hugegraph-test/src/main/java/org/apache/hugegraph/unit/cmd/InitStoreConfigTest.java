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

package org.apache.hugegraph.unit.cmd;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.auth.StandardAuthenticator;
import org.apache.hugegraph.cmd.InitStore;
import org.apache.hugegraph.config.ConfigException;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.util.ConfigUtil;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * {@code init_store.enabled} controls whether init-store performs local
 * backend and admin initialization. It defaults to true, so standalone and
 * tarball installs keep the full init path; distributed (PD/HStore)
 * deployments set it to false.
 */
public class InitStoreConfigTest {

    private static final String MISSING_GRAPHS_DIR = "no-such-graphs-dir";
    // Registered by RegisterUtil.registerBackends(), not by registerServer()
    private static final String ROCKSDB_OPTION = "rocksdb.data_path";

    private Path workDir;
    private int confSeq;

    @BeforeClass
    public static void registerOptions() {
        RegisterUtil.registerServer();
    }

    @Before
    public void setup() throws IOException {
        this.workDir = Files.createTempDirectory("init-store-config-test");
    }

    @After
    public void teardown() throws IOException {
        try (Stream<Path> paths = Files.walk(this.workDir)) {
            for (Path path : paths.sorted(Comparator.reverseOrder())
                                  .toArray(Path[]::new)) {
                Files.deleteIfExists(path);
            }
        }
    }

    @Test
    public void testInitStoreEnabledByDefault() {
        HugeConfig config = new HugeConfig(new PropertiesConfiguration());
        Assert.assertTrue(config.get(ServerOptions.INIT_STORE_ENABLED));
    }

    /**
     * Pins the spellings docker-entrypoint.sh may pass through. The shell side
     * is not executed here, so keep the two lists in step by hand.
     */
    @Test
    public void testBooleanSpellingsAcceptedByHugeConfig() {
        for (String value : new String[]{"true", "TRUE", "t", "on", "y", "yes"}) {
            Assert.assertTrue(value, initStoreEnabled(value));
        }
        for (String value : new String[]{"false", "FALSE", "f", "off", "n", "no"}) {
            Assert.assertFalse(value, initStoreEnabled(value));
        }
    }

    /**
     * Conversion runs at load time via commons-configuration 1.x
     * PropertyConverter, which delegates to commons-lang 2.x BooleanUtils —
     * so "0" and "1" are refused here even though commons-lang3 accepts them.
     */
    @Test
    public void testUnparseableBooleanFailsToLoad() {
        for (String value : new String[]{"0", "1", "disabled"}) {
            Assert.assertThrows(ConfigException.class, () -> {
                initStoreEnabled(value);
            });
        }
    }

    private static boolean initStoreEnabled(String value) {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(ServerOptions.INIT_STORE_ENABLED.name(), value);
        return new HugeConfig(properties).get(ServerOptions.INIT_STORE_ENABLED);
    }

    /**
     * A key defined twice loads as a list and fails the scalar type check, so
     * a caller mapping an env var onto it must replace, not append.
     */
    @Test
    public void testDuplicateDefinitionFailsToLoad() throws IOException {
        Path conf = this.workDir.resolve("duplicate.properties");
        String key = ServerOptions.INIT_STORE_ENABLED.name();
        Files.write(conf, Arrays.asList(key + "=false", key + "=true"),
                    StandardCharsets.UTF_8);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new HugeConfig(conf.toString());
        }, e -> Assert.assertContains("[false, true]", e.getMessage()));
    }

    /**
     * Establishes that graph scanning fails for these configs, which is what
     * keeps {@link #testDisabledInitStoreExitsBeforeGraphInit()} honest.
     */
    @Test
    public void testMissingGraphsDirFailsScan() {
        String graphsDir = this.workDir.resolve(MISSING_GRAPHS_DIR).toString();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            ConfigUtil.scanGraphsDir(graphsDir);
        });
    }

    @Test
    public void testDisabledInitStoreExitsBeforeGraphInit() throws Exception {
        String restConf = this.writeDisabledRestServerConf();
        // Completes instead of failing on the missing graphs directory, which
        // proves the gate is applied before any graph or admin initialization
        InitStore.main(new String[]{restConf});
    }

    /**
     * Disabled mode must not reach registerBackends() or registerPlugins(),
     * since plugin registration propagates every plugin's failures. Backend
     * options reach OptionSpace only through registerBackends(), so that state
     * shows whether it ran; OptionSpace is process-wide, hence unchanged rather
     * than empty. The enabled run shares this method so it provably follows the
     * disabled assertions, which registering backends first would make vacuous.
     * It leaves backend providers registered for the rest of the suite's JVM,
     * and BackendProviderFactory.register() rejects duplicates, so this has to
     * stay the only enabled run in the class.
     * <p>
     * The completion marker is asserted here for the same reason. A disabled
     * run must not write it: the entrypoint treats it as "already initialized",
     * so recording one for a config mounted with the property set to false
     * would skip the real initialization for good on a later re-enable. Only
     * a successful enabled run may write it, which needs a live backend and so
     * is not covered here.
     */
    @Test
    public void testGateDecidesWhetherRegistrationRuns() throws Exception {
        boolean backendsRegistered = OptionSpace.containKey(ROCKSDB_OPTION);
        Path marker = this.workDir.resolve("docker/init_complete");
        System.setProperty(InitStore.INIT_COMPLETE_MARKER, marker.toString());

        try {
            InitStore.main(new String[]{this.writeDisabledRestServerConf()});

            Assert.assertEquals(backendsRegistered,
                                OptionSpace.containKey(ROCKSDB_OPTION));
            // Server options stay registered, the gate is read from them
            Assert.assertTrue(OptionSpace.containKey(
                              ServerOptions.INIT_STORE_ENABLED.name()));
            Assert.assertFalse("a disabled run initialized nothing",
                               Files.exists(marker));

            // Absent gate means enabled, so the same config now has to reach
            // the graph scan and fail on the directory the disabled run never
            // looked at — a re-enable is not short-circuited by the marker
            Assert.assertThrows(IllegalArgumentException.class, () -> {
                InitStore.main(new String[]{this.writeEnabledRestServerConf()});
            });
            Assert.assertTrue(OptionSpace.containKey(ROCKSDB_OPTION));
            Assert.assertFalse("a failed run initialized nothing",
                               Files.exists(marker));
        } finally {
            System.clearProperty(InitStore.INIT_COMPLETE_MARKER);
        }
    }

    /**
     * The upgrade scenario: earlier releases wrote the completion marker
     * from the entrypoint, so one can exist for a configuration that was
     * never validated — including a later switch to the disabled gate. The
     * entrypoint half of the regression (skipping init-store entirely on a
     * present marker) is shell and out of a unit test's reach; what is
     * pinned here is the ordering invariant the fix relies on instead: the
     * fail-closed check runs before the marker is consulted, so it fires
     * with the marker present exactly as {@code
     * testDisabledInitStoreFailsWhenAdminCannotBeCreated} shows without it.
     */
    @Test
    public void testExistingMarkerDoesNotBypassDisabledPathCheck()
                                                        throws IOException {
        System.setProperty(InitStore.INIT_COMPLETE_MARKER,
                           this.writeExistingMarker().toString());
        try {
            String restConf = this.writeDisabledRestServerConf(
                    ServerOptions.AUTHENTICATOR.name() +
                    "=" + StandardAuthenticator.class.getName());

            Assert.assertThrows(IllegalStateException.class, () -> {
                InitStore.main(new String[]{restConf});
            }, e -> Assert.assertContains("Refusing to skip init-store",
                                          e.getMessage()));
        } finally {
            System.clearProperty(InitStore.INIT_COMPLETE_MARKER);
        }
    }

    /**
     * The enabled path with a present marker is the plain Docker restart: it
     * must return before the graph scan — completing on a config whose graphs
     * directory is missing proves that — and before backend registration,
     * which {@link #testGateDecidesWhetherRegistrationRuns} relies on being
     * run at most once per JVM.
     */
    @Test
    public void testExistingMarkerSkipsReinitializationWhenEnabled()
                                                        throws Exception {
        System.setProperty(InitStore.INIT_COMPLETE_MARKER,
                           this.writeExistingMarker().toString());
        try {
            InitStore.main(new String[]{this.writeEnabledRestServerConf()});
        } finally {
            System.clearProperty(InitStore.INIT_COMPLETE_MARKER);
        }
    }

    @Test
    public void testNonRegularMarkerDoesNotSkipInitialization()
                                                        throws IOException {
        Path marker = this.workDir.resolve("docker/init_complete");
        Files.createDirectories(marker);
        this.assertInvalidMarker(marker);

        Files.delete(marker);
        Path target = this.workDir.resolve("marker-target");
        Files.createFile(target);
        try {
            Files.createSymbolicLink(marker, target);
        } catch (UnsupportedOperationException e) {
            Assume.assumeTrue("Symbolic links are not supported", false);
        }
        this.assertInvalidMarker(marker);
    }

    @Test
    public void testRecordInitCompleteRejectsNonRegularMarker()
                                                        throws IOException {
        Path marker = this.workDir.resolve("docker/init_complete");
        Files.createDirectories(marker);
        System.setProperty(InitStore.INIT_COMPLETE_MARKER,
                           marker.toString());
        try {
            Assert.assertThrows(IllegalStateException.class, () -> {
                Whitebox.invokeStatic(InitStore.class,
                                       "recordInitComplete");
            }, e -> Assert.assertContains("regular file", e.getMessage()));
        } finally {
            System.clearProperty(InitStore.INIT_COMPLETE_MARKER);
        }
    }

    private void assertInvalidMarker(Path marker) throws IOException {
        System.setProperty(InitStore.INIT_COMPLETE_MARKER,
                           marker.toString());
        try {
            Assert.assertThrows(IllegalStateException.class, () -> {
                InitStore.main(new String[]{this.writeEnabledRestServerConf()});
            }, e -> Assert.assertContains("regular file", e.getMessage()));
        } finally {
            System.clearProperty(InitStore.INIT_COMPLETE_MARKER);
        }
    }

    private Path writeExistingMarker() throws IOException {
        Path marker = this.workDir.resolve("docker/init_complete");
        Files.createDirectories(marker.getParent());
        Files.createFile(marker);
        return marker;
    }

    /**
     * The CLI must not report success for a configuration that would start an
     * auth-enabled server with no admin account, since tarball and init-job
     * callers only see the exit status.
     */
    @Test
    public void testDisabledInitStoreFailsWhenAdminCannotBeCreated()
                                                        throws IOException {
        String restConf = this.writeDisabledRestServerConf(
                ServerOptions.AUTHENTICATOR.name() +
                "=" + StandardAuthenticator.class.getName());

        Assert.assertThrows(IllegalStateException.class, () -> {
            InitStore.main(new String[]{restConf});
        }, e -> Assert.assertContains("Refusing to skip init-store",
                                      e.getMessage()));
    }

    @Test
    public void testDisabledInitStoreFailsWithNonHstoreAuthGraph()
                                                        throws IOException {
        Path graphsDir = this.writeAuthGraphConfig("memory");
        String restConf = this.writeDisabledRestServerConfForGraphs(
                graphsDir,
                ServerOptions.AUTHENTICATOR.name() +
                "=" + StandardAuthenticator.class.getName(),
                ServerOptions.AUTH_GRAPH_STORE.name() + "=hugegraph",
                ServerOptions.USE_PD.name() + "=true");

        Assert.assertThrows(IllegalStateException.class, () -> {
            InitStore.main(new String[]{restConf});
        }, e -> Assert.assertContains("uses backend 'memory', not 'hstore'",
                                      e.getMessage()));
    }

    @Test
    public void testDisabledInitStoreAllowsPdBackedHstoreAuthGraph()
                                                        throws Exception {
        Path graphsDir = this.writeAuthGraphConfig("HSTORE");
        String restConf = this.writeDisabledRestServerConfForGraphs(
                graphsDir,
                ServerOptions.AUTHENTICATOR.name() +
                "=" + StandardAuthenticator.class.getName(),
                ServerOptions.AUTH_GRAPH_STORE.name() + "=hugegraph",
                ServerOptions.USE_PD.name() + "=true",
                ServerOptions.ADMIN_PA.name() + "=secret");

        InitStore.main(new String[]{restConf});
    }

    @Test
    public void testHstoreBackendComparisonIsCaseInsensitive() {
        Assert.assertTrue(Whitebox.invokeStatic(
                          InitStore.class, "isHstoreBackend", "hstore"));
        Assert.assertTrue(Whitebox.invokeStatic(
                          InitStore.class, "isHstoreBackend", "HSTORE"));
        Assert.assertFalse(Whitebox.invokeStatic(
                           InitStore.class, "isHstoreBackend", "memory"));
    }

    /**
     * The server creates the admin from auth.admin_pa without prompting, and
     * Docker PASSWORD never reaches this path, so an absent or empty value
     * would publish the well-known 'pa' default as a working credential.
     */
    @Test
    public void testDisabledInitStoreRejectsDefaultAdminPassword()
                                                        throws IOException {
        Path graphsDir = this.writeAuthGraphConfig("hstore");
        for (String adminPa : new String[]{null, ""}) {
            List<String> extra = new ArrayList<>(Arrays.asList(
                    ServerOptions.AUTHENTICATOR.name() +
                    "=" + StandardAuthenticator.class.getName(),
                    ServerOptions.AUTH_GRAPH_STORE.name() + "=hugegraph",
                    ServerOptions.USE_PD.name() + "=true"));
            if (adminPa != null) {
                extra.add(ServerOptions.ADMIN_PA.name() + "=" + adminPa);
            }
            String restConf = this.writeDisabledRestServerConfForGraphs(
                    graphsDir, extra.toArray(new String[0]));

            Assert.assertThrows(IllegalStateException.class, () -> {
                InitStore.main(new String[]{restConf});
            }, e -> Assert.assertContains("public default", e.getMessage()));
        }
    }

    /**
     * Remote auth delegates to another service, so there is no local admin to
     * create and the check above must not fire.
     */
    @Test
    public void testDisabledInitStoreAllowsRemoteAuth() throws Exception {
        String restConf = this.writeDisabledRestServerConf(
                ServerOptions.AUTHENTICATOR.name() +
                "=" + StandardAuthenticator.class.getName(),
                ServerOptions.AUTH_REMOTE_URL.name() + "=127.0.0.1:8899");

        InitStore.main(new String[]{restConf});
    }

    /**
     * auth.authenticator takes any implementation class, and only the built-in
     * one bootstraps HugeGraph's admin account. A custom authenticator keeps
     * its identities elsewhere, so the check must not reject it; a subclass of
     * the built-in one still relies on the same bootstrap.
     */
    @Test
    public void testDisabledInitStoreAllowsCustomAuthenticator()
                                                        throws Exception {
        String restConf = this.writeDisabledRestServerConf(
                ServerOptions.AUTHENTICATOR.name() +
                "=org.example.auth.LdapAuthenticator");

        InitStore.main(new String[]{restConf});

        String subclassConf = this.writeDisabledRestServerConf(
                ServerOptions.AUTHENTICATOR.name() + "=" +
                DerivedAuthenticator.class.getName());

        Assert.assertThrows(IllegalStateException.class, () -> {
            InitStore.main(new String[]{subclassConf});
        }, e -> Assert.assertContains("Refusing to skip init-store",
                                      e.getMessage()));
    }

    private String writeDisabledRestServerConf(String... extraLines)
                                               throws IOException {
        return this.writeDisabledRestServerConfForGraphs(
                this.workDir.resolve(MISSING_GRAPHS_DIR), extraLines);
    }

    /**
     * The same configuration with the gate left out entirely, so it takes the
     * option's default rather than an explicit value.
     */
    private String writeEnabledRestServerConf() throws IOException {
        Path restConf = this.workDir.resolve(
                "rest-server-" + this.confSeq++ + ".properties");
        Files.write(restConf, Arrays.asList(ServerOptions.GRAPHS.name() + "=" +
                                            this.workDir.resolve(MISSING_GRAPHS_DIR)),
                    StandardCharsets.UTF_8);
        return restConf.toString();
    }

    private String writeDisabledRestServerConfForGraphs(Path graphsDir,
                                                        String... extraLines)
                                                        throws IOException {
        // A distinct file per call, so two configs written by one test do not
        // collide inside its temporary directory
        Path restConf = this.workDir.resolve(
                "rest-server-" + this.confSeq++ + ".properties");
        List<String> lines = new ArrayList<>();
        lines.add(ServerOptions.GRAPHS.name() + "=" + graphsDir);
        lines.add(ServerOptions.INIT_STORE_ENABLED.name() + "=false");
        lines.addAll(Arrays.asList(extraLines));
        Files.write(restConf, lines, StandardCharsets.UTF_8);
        return restConf.toString();
    }

    private Path writeAuthGraphConfig(String backend) throws IOException {
        Path graphsDir = this.workDir.resolve("graphs-" + this.confSeq++);
        Files.createDirectories(graphsDir);
        Files.write(graphsDir.resolve("hugegraph.properties"),
                    Arrays.asList(CoreOptions.BACKEND.name() + "=" + backend),
                    StandardCharsets.UTF_8);
        return graphsDir;
    }

    /**
     * Stands in for a deployment that subclasses the built-in authenticator,
     * which still depends on the admin account it creates.
     */
    public static class DerivedAuthenticator extends StandardAuthenticator {
    }
}
