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

package org.apache.hugegraph.store.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.store.upgrade.LegacyGraphIdChecker;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

public class LegacyGraphIdStartupLockTest {

    private static HugeConfig config;

    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @BeforeClass
    public static void init() {
        RocksDB.loadLibrary();
        config = LegacyGraphIdChecker.configuration(Map.of(
                "rocksdb.optimize_mode", false,
                "rocksdb.write_buffer_size", 1048576L));
    }

    @Test
    public void testStartupCheckRejectsRunningStoreNativeLock() throws Exception {
        Path root = this.temporary.newFolder().toPath();
        Path metadata = root.resolve("hgstore-metadata");
        Files.createDirectories(metadata);
        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB database = RocksDB.open(options, metadata.toString())) {
            database.put(new byte[]{1}, new byte[]{1});
        }

        Path signals = this.temporary.newFolder().toPath();
        Path ready = signals.resolve("ready");
        Path output = signals.resolve("writer.log");
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("surefire.test.class.path",
                                              System.getProperty("java.class.path"));
        Process writer = new ProcessBuilder(
                java, "-cp", classpath,
                LegacyGraphIdCheckerTest.NativeLockHolder.class.getName(),
                metadata.toString(), ready.toString())
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!Files.exists(ready) && writer.isAlive() &&
                   System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }
            String log = Files.exists(output) ? Files.readString(output) : "";
            assertTrue(log, Files.exists(ready));
            LegacyGraphIdChecker.StartupPreflightException failure = assertThrows(
                    LegacyGraphIdChecker.StartupPreflightException.class,
                    () -> LegacyGraphIdChecker.checkStore(root.toString(), config));
            assertTrue(failure.getMessage(), failure.getMessage().contains("locked"));
        } finally {
            writer.getOutputStream().close();
            if (!writer.waitFor(10, TimeUnit.SECONDS)) {
                writer.destroyForcibly();
                writer.waitFor(10, TimeUnit.SECONDS);
            }
        }
        assertEquals(Files.readString(output), 0, writer.exitValue());
        LegacyGraphIdChecker.checkStore(root.toString(), config);
    }
}
