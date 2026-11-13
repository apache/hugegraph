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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.rocksdb.access.RocksDBOptions;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.store.constant.HugeServerTables;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.Checkpoint;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.RocksDB;

public class GraphIdSnapshotLevelsTest {

    private static final String TABLE = HugeServerTables.VERTEX_TABLE;
    private static final byte[] KEY = {(byte) 0xFF, (byte) 0xFD, 1, 0, 0};
    private static final byte[] VALUE = {31, 32, 33};

    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @BeforeClass
    public static void init() {
        RocksDB.loadLibrary();
        OptionSpace.register("rocksdb", RocksDBOptions.class.getName());
    }

    @Test
    public void testConfiguredLevelSevenSnapshotActivatesAndReopens() throws Exception {
        HugeConfig config = new HugeConfig(Map.of(
                "rocksdb.optimize_mode", false,
                "rocksdb.write_buffer_size", 1048576L,
                "rocksdb.num_levels", 8,
                "rocksdb.compression_per_level", "[none,none,none,none,none,none,none,none]"));
        Path producer = this.temporary.newFolder("producer").toPath();
        Path snapshot = producer.resolve("snapshot");
        try (RocksDBSession source = new RocksDBSession(config, producer.toString(), "seed", 0L)) {
            source.checkTable(TABLE);
            var table = source.getTables().get(TABLE);
            source.getDB().put(table, KEY, VALUE);
            source.flush(true);
            try (CompactRangeOptions compact = new CompactRangeOptions()
                    .setChangeLevel(true).setTargetLevel(7)) {
                source.getDB().compactRange(table, null, null, compact);
            }
            assertFalse("The regression requires a real SST in configured level 7",
                        source.getDB().getColumnFamilyMetaData(table).levels().get(7).files().isEmpty());
            try (Checkpoint checkpoint = Checkpoint.create(source.getDB())) {
                checkpoint.createCheckpoint(snapshot.toString());
            }
        }
        Map<String, String> incoming = hashes(snapshot);
        Path destination = this.temporary.newFolder("destination").toPath();
        try (RocksDBSession active = new RocksDBSession(config, destination.toString(), "00000", 0L)) {
            active.loadSnapshot(snapshot.toString(), 5L);
        }
        assertTrue(Files.isRegularFile(destination.resolve("00000_5/CURRENT")));
        assertFalse(Files.exists(destination.resolve("00000_5_temp_")));
        try (RocksDBSession restored = new RocksDBSession(config, destination.toString(), "00000", 5L)) {
            assertEquals(destination.resolve("00000_5").toString(), restored.getDbPath());
            assertArrayEquals(VALUE, restored.sessionOp().get(TABLE, KEY));
        }
        assertEquals(incoming, hashes(snapshot));
    }

    private static Map<String, String> hashes(Path root) throws Exception {
        Map<String, String> result = new TreeMap<>();
        try (var paths = Files.walk(root)) {
            for (Path path : paths.filter(Files::isRegularFile).toList()) {
                result.put(root.relativize(path).toString(), HexFormat.of().formatHex(
                        MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path))));
            }
        }
        return result;
    }
}
