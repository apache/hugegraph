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

package org.apache.hugegraph.store.node;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.store.upgrade.LegacyGraphIdChecker;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

public class GraphIdCheckCommandTest {

    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void testLocalYamlAndExplicitDataPathWithoutServices() throws Exception {
        Path config = this.temporary.newFolder("conf").toPath();
        Path root = this.temporary.newFolder("store").toPath();
        Files.writeString(config.resolve("application.yml"), """
                app:
                  data-path: ${MISSING_DATA_PATH}
                rocksdb:
                  num_levels: 8
                  compression_per_level: '[none,none,none,none,none,none,none,none]'
                """);
        assertEquals(0, GraphIdCheckCommand.run("--check", "--config", config.toString(),
                                              "--data-path", root.toString()));
        assertFalse(Files.exists(root.resolve("hgstore-metadata")));
        assertEquals(2, GraphIdCheckCommand.run("--check", "--config", config.toString()));
    }

    @Test
    public void testEmptyRootReportsZeroInspection() throws Exception {
        Path config = this.temporary.newFolder("empty-conf").toPath();
        Path root = this.temporary.newFolder("empty-data").toPath();
        Files.writeString(config.resolve("application.yml"), "app:\n  data-path: '" + root + "'\n");
        Process process = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"), GraphIdCheckCommand.class.getName(),
                "--check", "--config", config.toString())
                .redirectErrorStream(true).start();
        try {
            assertTrue("Checker process timed out", process.waitFor(10, TimeUnit.SECONDS));
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(output, 0, process.exitValue());
            assertTrue(output, output.contains("Inspected 0 databases, 0 latest snapshots"));
            assertTrue(output, output.contains("No business databases or snapshots were inspected"));
        } finally {
            process.destroyForcibly();
        }
    }

    @Test
    public void testMetadataOnlyReportsZeroBusinessInspection() throws Exception {
        RocksDB.loadLibrary();
        Path config = this.temporary.newFolder("metadata-conf").toPath();
        Path root = this.temporary.newFolder("metadata-data").toPath();
        Path metadata = root.resolve("hgstore-metadata");
        Files.createDirectory(metadata);
        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB ignored = RocksDB.open(options, metadata.toString())) {
            // An initialized metadata DB need not contain any Store partitions.
        }
        Files.writeString(config.resolve("application.yml"), "app:\n  data-path: '" + root + "'\n");
        Process process = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"), GraphIdCheckCommand.class.getName(),
                "--check", "--config", config.toString())
                .redirectErrorStream(true).start();
        try {
            assertTrue("Checker process timed out", process.waitFor(10, TimeUnit.SECONDS));
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(output, 0, process.exitValue());
            assertTrue(output, output.contains("metadata present: true"));
            assertTrue(output, output.contains("No business databases or snapshots were inspected"));
            assertFalse(output, output.contains("No legacy GraphID prefix"));
        } finally {
            process.destroyForcibly();
        }
    }

    @Test
    public void testLegacyDataReturnsFailureWithoutChangingKey() throws Exception {
        RocksDB.loadLibrary();
        Path config = this.temporary.newFolder("conf").toPath();
        Path root = this.temporary.newFolder("store").toPath();
        Path database = root.resolve("db/00000");
        Files.createDirectories(database);
        Files.writeString(config.resolve("application.yml"), "app:\n  data-path: '" + root + "'\n");
        byte[] key = {(byte) 0xFF, (byte) 0xFE, 1, 0, 0};
        try (Options options = new Options().setCreateIfMissing(true);
             ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
             RocksDB db = RocksDB.open(options, database.toString());
             ColumnFamilyHandle table = db.createColumnFamily(new ColumnFamilyDescriptor(
                     new byte[]{'g', '+', 'v'}, cfOptions))) {
            db.put(table, key, new byte[]{1});
        }
        assertEquals(2, GraphIdCheckCommand.run("--check", "--config", config.toString()));
        assertEquals(2, GraphIdCheckCommand.run("--check", "--config", config.toString()));
        RuntimeException refusal = assertThrows(LegacyGraphIdChecker.StartupPreflightException.class,
                () -> LegacyGraphIdChecker.checkStore(root.toString(),
                                                     LegacyGraphIdChecker.configuration(Map.of())));
        assertTrue(StoreNodeApplication.isGraphIdPreflightFailure(refusal));
        assertTrue(StoreNodeApplication.isGraphIdPreflightFailure(new RuntimeException(refusal)));
        assertFalse(Files.exists(root.resolve("hgstore-metadata")));
    }

    @Test
    public void testUnrelatedStartupErrorsKeepTheirExistingLifecycle() {
        assertFalse(StoreNodeApplication.isGraphIdPreflightFailure(
                new RuntimeException("PD unreachable")));
        assertFalse(StoreNodeApplication.isGraphIdPreflightFailure(
                new RuntimeException("GraphID upgrade preflight refused Store startup")));
        RuntimeException first = new RuntimeException();
        RuntimeException second = new RuntimeException(first);
        first.initCause(second);
        assertFalse(StoreNodeApplication.isGraphIdPreflightFailure(first));
    }

    @Test
    public void testMissingDataPathDoesNotGuessAnEmptyDirectory() throws Exception {
        Path directory = this.temporary.newFolder("distribution").toPath();
        Path config = Files.createDirectory(directory.resolve("conf"));
        Files.createDirectory(directory.resolve("storage"));
        Files.writeString(config.resolve("application.yml"), "rocksdb:\n  num_levels: 7\n");
        Process process = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"), GraphIdCheckCommand.class.getName(),
                "--check", "--config", config.toString())
                .directory(directory.toFile()).redirectErrorStream(true).start();
        try {
            assertTrue("Checker process timed out", process.waitFor(10, TimeUnit.SECONDS));
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(output, 2, process.exitValue());
            assertTrue(output, output.contains("Missing app.data-path"));
        } finally {
            process.destroyForcibly();
        }
    }

    @Test
    public void testInvalidArgumentsDoNotRunStore() {
        assertEquals(0, GraphIdCheckCommand.run("--help"));
        assertEquals(2, GraphIdCheckCommand.run());
        assertEquals(2, GraphIdCheckCommand.run("--execute"));
        assertEquals(2, GraphIdCheckCommand.run("--check", "--check"));
        assertEquals(2, GraphIdCheckCommand.run("--check", "--data-path"));
    }
}
