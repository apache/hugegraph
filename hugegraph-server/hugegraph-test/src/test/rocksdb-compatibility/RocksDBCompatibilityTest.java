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

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

/** RocksDB SST/WAL compatibility test. Each phase must run in a separate JVM. */
public final class RocksDBCompatibilityTest {

    private static int assertions;

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            throw new IllegalArgumentException("Usage: <seed|upgrade|verify> <db> <version> <jar>");
        }
        String phase = args[0];
        if (!Arrays.asList("seed", "upgrade", "verify").contains(phase)) {
            throw new IllegalArgumentException("Unknown phase: " + phase);
        }
        Path jar = Paths.get(RocksDB.class.getProtectionDomain().getCodeSource()
                                     .getLocation().toURI()).toRealPath();
        require(jar.equals(Paths.get(args[3]).toRealPath()), "CodeSource matches explicit JAR");
        System.out.println("CodeSource=" + jar + " sha256=" + sha256(jar));
        try {
            Class.forName("org.rocksdb.SidePluginRepo", false, RocksDB.class.getClassLoader());
            throw new IllegalStateException("This fixture requires standard RocksDB, not a TP JAR");
        } catch (ClassNotFoundException expected) {
            assertions++;
        }
        RocksDB.loadLibrary();
        String version = RocksDB.rocksdbVersion().toString();
        System.out.println("nativeVersion=" + version);
        require(version.equals(args[2]), "native version matches requested version");
        Path path = Paths.get(args[1]);
        boolean seed = "seed".equals(phase);
        require(seed ? !Files.exists(path) : Files.isDirectory(path), "database path precondition");
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        try (DBOptions options = new DBOptions().setCreateIfMissing(seed)
                                               .setCreateMissingColumnFamilies(seed);
             ColumnFamilyOptions cfOptions = new ColumnFamilyOptions().setDisableAutoCompactions(true);
             WriteOptions write = new WriteOptions().setSync(true);
             FlushOptions flush = new FlushOptions().setWaitForFlush(true);
             RocksDB db = RocksDB.open(options, path.toString(), Arrays.asList(
                     new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions),
                     new ColumnFamilyDescriptor(bytes("compatibility-fixture"), cfOptions)), handles)) {
            try {
                for (ColumnFamilyHandle handle : handles) {
                    if (seed) {
                        put(db, handle, write, "sst-keep", "old");
                        put(db, handle, write, "sst-update", "old");
                        put(db, handle, write, "sst-delete", "old");
                        db.flush(flush, handle);
                        put(db, handle, write, "wal-keep", "old");
                        put(db, handle, write, "wal-delete", "old");
                        require(db.getLongProperty(handle, "rocksdb.num-entries-active-mem-table") == 2,
                                "two unflushed entries in active memtable");
                    } else if ("upgrade".equals(phase)) {
                        check(db, handle, false);
                        put(db, handle, write, "sst-update", "new");
                        db.delete(handle, write, bytes("sst-delete"));
                        db.delete(handle, write, bytes("wal-delete"));
                        put(db, handle, write, "new-key", "new");
                        check(db, handle, true);
                        db.flush(flush, handle);
                    } else {
                        check(db, handle, true);
                    }
                }
                if (seed) {
                    db.flushWal(true);
                    require(hasFile(path, ".sst"), "flushed SST exists");
                    require(hasFile(path, ".log"), "nonempty WAL exists");
                    // Deliberately skip close: preserve WAL-only writes for recovery by the new version.
                    System.out.printf("PASS phase=seed columnFamilies=2 records=10 assertions=%d " +
                                      "exit=halt(0) shutdownFlush=false%n", assertions);
                    System.out.flush();
                    Runtime.getRuntime().halt(0);
                }
            } finally {
                for (ColumnFamilyHandle handle : handles) {
                    handle.close();
                }
            }
        }
        System.out.printf("PASS phase=%s columnFamilies=2 records=8 assertions=%d%n", phase, assertions);
    }

    private static void check(RocksDB db, ColumnFamilyHandle cf, boolean upgraded) throws Exception {
        expect(db, cf, "sst-keep", "old");
        expect(db, cf, "sst-update", upgraded ? "new" : "old");
        expect(db, cf, "sst-delete", upgraded ? null : "old");
        expect(db, cf, "wal-keep", "old");
        expect(db, cf, "wal-delete", upgraded ? null : "old");
        expect(db, cf, "new-key", upgraded ? "new" : null);
        int count = 0;
        try (RocksIterator iterator = db.newIterator(cf)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                count++;
            }
            iterator.status();
        }
        require(count == (upgraded ? 4 : 5), "exact iterator record count");
    }

    private static void expect(RocksDB db, ColumnFamilyHandle cf, String key, String value) throws Exception {
        require(Arrays.equals(value == null ? null : bytes(value), db.get(cf, bytes(key))), "value for " + key);
    }

    private static void put(RocksDB db, ColumnFamilyHandle cf, WriteOptions write,
                            String key, String value) throws Exception {
        db.put(cf, write, bytes(key), bytes(value));
    }

    private static boolean hasFile(Path directory, String suffix) throws Exception {
        try (Stream<Path> files = Files.list(directory)) {
            return files.anyMatch(file -> file.toString().endsWith(suffix) && file.toFile().length() > 0);
        }
    }

    private static String sha256(Path path) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream input = Files.newInputStream(path)) {
            byte[] buffer = new byte[8192];
            int length;
            while ((length = input.read(buffer)) != -1) {
                digest.update(buffer, 0, length);
            }
        }
        StringBuilder hex = new StringBuilder();
        for (byte value : digest.digest()) {
            hex.append(String.format("%02x", value & 0xff));
        }
        return hex.toString();
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
        assertions++;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private RocksDBCompatibilityTest() {
    }
}
