/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hugegraph.store.upgrade;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongConsumer;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import com.google.protobuf.Int64Value;

/**
 * Experimental cold-DB conversion kernel. Never activates a Store or changes
 * its Raft state. A verified result is NOT yet a cluster-upgrade certificate.
 */
public final class GraphIdRewriter {

    private static final String GRAPH_KEY = "HUGEGRAPH/GRAPH_ID/";
    private static final String SLOT_KEY = "HUGEGRAPH/CID_SLOT/@GRAPH_ID@/";
    private static final String INTERRUPTED =
            "Conversion interrupted; discard unpublished output";
    private static final long BATCH_BYTES = 8L * 1024 * 1024;

    private GraphIdRewriter() {
    }

    public record Result(long converted, long records, String sourceDigest,
                         String targetDigest, Path database) {
    }

    public static GraphIdMigrationPlan plan(Path source, ColumnFamilyOptions options,
                                            List<GraphIdMigrationPlan.Rule> rules) throws Exception {
        try (SourceLock lock = new SourceLock(source);
             Database database = new Database(lock.path, options, true)) {
            return plan(database, rules);
        }
    }

    private static GraphIdMigrationPlan plan(Database source,
                                             List<GraphIdMigrationPlan.Rule> rules) throws Exception {
        GraphIdMigrationPlan.validateRules(rules);
        Set<Integer> occupied = new HashSet<>();
        Map<String, Long> mappings = new LinkedHashMap<>();
        Set<Long> assigned = new HashSet<>();
        for (var entry : source.families.entrySet()) {
            try (RocksIterator iterator = source.db.newIterator(entry.getValue())) {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    checkInterrupted();
                    byte[] key = iterator.key();
                    if (key.length >= 2) {
                        occupied.add(((key[0] & 255) << 8) | (key[1] & 255));
                    }
                    if (!entry.getKey().equals("default")) {
                        continue;
                    }
                    if (startsWith(key, utf8(GRAPH_KEY)) || startsWith(key, utf8(SLOT_KEY))) {
                        long id = Int64Value.parseFrom(iterator.value()).getValue();
                        if (id < 0 || id > GraphIdMigrationPlan.LEGACY_ID) {
                            throw new IOException("Invalid GraphID metadata");
                        }
                        occupied.add((int) id);
                        if (startsWith(key, utf8(SLOT_KEY)) &&
                            !Arrays.equals(key, slotKey((int) id))) {
                            throw new IOException("Allocator slot key/value mismatch");
                        }
                        if (startsWith(key, utf8(GRAPH_KEY))) {
                            String graph = new String(key, utf8(GRAPH_KEY).length,
                                                      key.length - utf8(GRAPH_KEY).length,
                                                      StandardCharsets.UTF_8);
                            if (!Arrays.equals(key, utf8(GRAPH_KEY + graph))) {
                                throw new IOException("Invalid UTF-8 graph metadata");
                            }
                            mappings.put(graph, id);
                            if (id != GraphIdMigrationPlan.LEGACY_ID && !assigned.add(id)) {
                                throw new IOException("Duplicate allocated GraphID");
                            }
                        }
                    }
                }
                iterator.status();
            }
        }
        Map<String, Integer> targets = GraphIdMigrationPlan.allocate(rules, occupied);
        for (var entry : mappings.entrySet()) {
            if (targets.containsKey(entry.getKey()) && entry.getValue() != GraphIdMigrationPlan.LEGACY_ID) {
                throw new IOException("Mixed/already allocated graph requires separate recovery: " + entry.getKey());
            }
            if (entry.getValue() == GraphIdMigrationPlan.LEGACY_ID && !targets.containsKey(entry.getKey())) {
                throw new IOException("Legacy mapping lacks an ownership plan: " + entry.getKey());
            }
        }
        GraphIdMigrationPlan plan = new GraphIdMigrationPlan(digest(source), rules, targets);
        for (var entry : source.families.entrySet()) {
            try (RocksIterator iterator = source.db.newIterator(entry.getValue())) {
                for (iterator.seek(new byte[]{-1, -2}); iterator.isValid() &&
                     GraphIdMigrationPlan.isLegacy(iterator.key()); iterator.next()) {
                    checkInterrupted();
                    plan.rewrite(entry.getKey(), iterator.key());
                }
                iterator.status();
            }
        }
        return plan;
    }

    public static Result convert(Path source, Path output, ColumnFamilyOptions options,
                                  GraphIdMigrationPlan plan) throws Exception {
        return convert(source, output, options, plan, ignored -> { });
    }

    static Result convert(Path source, Path output, ColumnFamilyOptions options,
                           GraphIdMigrationPlan plan, LongConsumer progress) throws Exception {
        try (SourceLock lock = new SourceLock(source);
             Database original = new Database(lock.path, options, true)) {
            GraphIdMigrationPlan actual = plan(original, plan.rules());
            if (!actual.sourceDigest().equals(plan.sourceDigest()) ||
                !actual.targets().equals(plan.targets())) {
                throw new IOException("Stale source or target allocation: regenerate the plan");
            }
            Path destination = output.toAbsolutePath().normalize();
            Path parent = destination.getParent().toRealPath();
            destination = parent.resolve(destination.getFileName());
            if (destination.startsWith(lock.path) || Files.exists(destination, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("Output must be new and outside the source database");
            }
            Files.createDirectory(destination);
            Files.writeString(destination.resolve("INCOMPLETE"), "Never activate this output\n",
                              StandardOpenOption.CREATE_NEW);
            Path data = Files.createDirectory(destination.resolve("database"));
            copyColdDatabase(lock.path, data);
            long converted = 0;
            try (Database target = new Database(data, options, false);
                 WriteOptions writes = new WriteOptions().setSync(true).setDisableWAL(false);
                 WriteBatch batch = new WriteBatch()) {
                long bytes = 0;
                for (var entry : original.families.entrySet()) {
                    try (RocksIterator iterator = original.db.newIterator(entry.getValue())) {
                        for (iterator.seek(new byte[]{-1, -2}); iterator.isValid() &&
                             GraphIdMigrationPlan.isLegacy(iterator.key()); iterator.next()) {
                            checkInterrupted();
                            byte[] key = iterator.key();
                            byte[] value = iterator.value();
                            ColumnFamilyHandle family = target.families.get(entry.getKey());
                            batch.put(family, plan.rewrite(entry.getKey(), key), value);
                            batch.delete(family, key);
                            converted++;
                            bytes += 2L * key.length + value.length;
                            if (bytes >= BATCH_BYTES || converted % 256 == 0) {
                                target.db.write(writes, batch);
                                batch.clear();
                                bytes = 0;
                                progress.accept(converted);
                                checkInterrupted();
                            }
                        }
                        iterator.status();
                    }
                }
                checkInterrupted();
                target.db.write(writes, batch);
                batch.clear();
                for (var entry : plan.targets().entrySet()) {
                    byte[] value = Int64Value.of(entry.getValue()).toByteArray();
                    batch.put(utf8(GRAPH_KEY + entry.getKey()), value);
                    batch.put(slotKey(entry.getValue()), value);
                }
                target.db.write(writes, batch);
                try (FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
                    target.db.flush(flush, new ArrayList<>(target.families.values()));
                }
            }
            // Reopen: verify durable bytes, not only the writer's memtables.
            try (Database target = new Database(data, options, true)) {
                long count = verify(original, target, plan);
                checkInterrupted();
                target.db.verifyChecksum();
                checkInterrupted();
                String resultDigest = digest(target);
                if (!digest(original).equals(plan.sourceDigest())) {
                    throw new IOException("Source changed during conversion");
                }
                checkInterrupted();
                Files.writeString(destination.resolve("DB_VERIFIED_NOT_CLUSTER_READY"),
                                  "source=" + plan.sourceDigest() + "\ntarget=" + resultDigest +
                                  "\nconverted=" + converted + "\nrecords=" + count + "\n",
                                  StandardOpenOption.CREATE_NEW);
                Files.delete(destination.resolve("INCOMPLETE"));
                return new Result(converted, count, plan.sourceDigest(), resultDigest, data);
            }
        }
    }

    private static long verify(Database source, Database target, GraphIdMigrationPlan plan) throws Exception {
        long expectedCount = 0;
        for (var entry : source.families.entrySet()) {
            try (RocksIterator iterator = source.db.newIterator(entry.getValue())) {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    checkInterrupted();
                    byte[] key = iterator.key();
                    byte[] value = iterator.value();
                    if (entry.getKey().equals("default") && startsWith(key, utf8(GRAPH_KEY))) {
                        String graph = new String(key, utf8(GRAPH_KEY).length,
                                                  key.length - utf8(GRAPH_KEY).length,
                                                  StandardCharsets.UTF_8);
                        Integer targetId = plan.targets().get(graph);
                        if (targetId != null) {
                            value = Int64Value.of(targetId).toByteArray();
                        }
                    }
                    byte[] actual = target.db.get(target.families.get(entry.getKey()),
                                                  plan.rewrite(entry.getKey(), key));
                    if (!Arrays.equals(value, actual)) {
                        throw new IOException("Reopened output differs in " + entry.getKey());
                    }
                    expectedCount++;
                }
                iterator.status();
            }
        }
        for (var mapping : plan.targets().entrySet()) {
            checkInterrupted();
            byte[] key = utf8(GRAPH_KEY + mapping.getKey());
            byte[] value = Int64Value.of(mapping.getValue()).toByteArray();
            if (source.db.get(key) == null) {
                expectedCount++;
            }
            if (!Arrays.equals(value, target.db.get(key)) ||
                !Arrays.equals(value, target.db.get(slotKey(mapping.getValue())))) {
                throw new IOException("GraphID mapping/allocator slot mismatch");
            }
            expectedCount++; // Allocation explicitly excludes every existing slot.
        }
        long actualCount = 0;
        for (ColumnFamilyHandle family : target.families.values()) {
            try (RocksIterator iterator = target.db.newIterator(family)) {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    checkInterrupted();
                    if (GraphIdMigrationPlan.isLegacy(iterator.key())) {
                        throw new IOException("Legacy prefix remains in output");
                    }
                    actualCount++;
                }
                iterator.status();
            }
        }
        if (actualCount != expectedCount) {
            throw new IOException("Output contains missing or unexpected records");
        }
        return actualCount;
    }

    private static void copyColdDatabase(Path source, Path target) throws IOException {
        try (var files = Files.walk(source)) {
            for (Path path : files.toList()) {
                if (path.equals(source)) {
                    continue;
                }
                if (Files.isSymbolicLink(path)) {
                    throw new IOException("Symlinks in database are not supported: " + path);
                }
                Path relative = source.relativize(path);
                Path copy = target.resolve(relative);
                if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
                    Files.createDirectory(copy);
                } else if (Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                    String fileName = path.getFileName().toString();
                    // Closing any descriptor for a POSIX-locked file can release this
                    // process' lock. Never reopen the source LOCK while SourceLock is held.
                    if (relative.equals(Path.of("LOCK"))) {
                        Files.createFile(copy);
                        continue;
                    }
                    // Only immutable SST files may share inodes. Never hard-link manifests/WAL.
                    if (fileName.endsWith(".sst")) {
                        try {
                            Files.createLink(copy, path);
                            continue;
                        } catch (IOException | UnsupportedOperationException unavailable) {
                            // A different filesystem requires copying; never weaken isolation.
                        }
                    }
                    Files.copy(path, copy);
                } else {
                    throw new IOException("Unsupported database entry: " + path);
                }
            }
        }
    }

    private static String digest(Database database) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        for (var entry : database.families.entrySet()) {
            checkInterrupted();
            digest.update((byte) 1);
            update(digest, utf8(entry.getKey()));
            try (RocksIterator iterator = database.db.newIterator(entry.getValue())) {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    checkInterrupted();
                    digest.update((byte) 2);
                    update(digest, iterator.key());
                    update(digest, iterator.value());
                }
                iterator.status();
            }
            digest.update((byte) 0);
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static void checkInterrupted() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException(INTERRUPTED);
        }
    }

    private static void update(MessageDigest digest, byte[] bytes) {
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    // Existing GraphIdManager.genCIDSlotKey uses Long.SIZE, not Long.BYTES.
    // Preserve that persisted layout, including its zero padding.
    static byte[] slotKey(int id) {
        byte[] prefix = utf8(SLOT_KEY);
        return ByteBuffer.allocate(prefix.length + Long.SIZE).put(prefix).putLong(id).array();
    }

    private static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static boolean startsWith(byte[] key, byte[] prefix) {
        return key.length >= prefix.length && Arrays.equals(key, 0, prefix.length, prefix, 0, prefix.length);
    }

    private static final class SourceLock implements AutoCloseable {
        private final Path path;
        private final FileChannel channel;
        private final FileLock lock;

        private SourceLock(Path source) throws IOException {
            this.path = source.toRealPath();
            if (!Files.isRegularFile(this.path.resolve("CURRENT"), LinkOption.NOFOLLOW_LINKS) ||
                !Files.isRegularFile(this.path.resolve("LOCK"), LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("Expected a complete, stopped database with CURRENT and LOCK");
            }
            this.channel = FileChannel.open(this.path.resolve("LOCK"), StandardOpenOption.READ);
            FileLock acquired;
            try {
                acquired = this.channel.tryLock(0L, Long.MAX_VALUE, true);
                if (acquired == null) {
                    throw new IOException("Source database is running or locked");
                }
            } catch (IOException | RuntimeException error) {
                this.channel.close();
                throw error;
            }
            this.lock = acquired;
        }

        @Override
        public void close() throws IOException {
            try {
                this.lock.release();
            } finally {
                this.channel.close();
            }
        }
    }

    private static final class Database implements AutoCloseable {
        private final DBOptions options = new DBOptions();
        private final ColumnFamilyOptions familyOptions;
        private final List<ColumnFamilyHandle> handles = new ArrayList<>();
        private final Map<String, ColumnFamilyHandle> families = new LinkedHashMap<>();
        private final Logger logger = new Logger(this.options) {
            @Override
            protected void log(InfoLogLevel level, String message) {
                // Read-only source inspection must not create or rotate LOG files.
            }
        };
        private RocksDB db;

        private Database(Path path, ColumnFamilyOptions template, boolean readOnly) throws Exception {
            this.familyOptions = new ColumnFamilyOptions(template).setDisableAutoCompactions(true);
            try {
                this.options.setCreateIfMissing(false).setCreateMissingColumnFamilies(false).setLogger(this.logger);
                List<byte[]> names;
                try (Options listing = new Options().setLogger(this.logger)) {
                    names = RocksDB.listColumnFamilies(listing, path.toString());
                }
                names.sort(Arrays::compareUnsigned);
                List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
                for (byte[] name : names) {
                    if (!Arrays.equals(name, utf8(new String(name, StandardCharsets.UTF_8)))) {
                        throw new IOException("Non-UTF-8 column family name");
                    }
                    descriptors.add(new ColumnFamilyDescriptor(name, this.familyOptions));
                }
                this.db = readOnly ? RocksDB.openReadOnly(this.options, path.toString(), descriptors, this.handles) :
                          RocksDB.open(this.options, path.toString(), descriptors, this.handles);
                for (int i = 0; i < names.size(); i++) {
                    this.families.put(new String(names.get(i), StandardCharsets.UTF_8), this.handles.get(i));
                }
            } catch (Exception | Error error) {
                this.close();
                throw error;
            }
        }

        @Override
        public void close() {
            this.handles.forEach(ColumnFamilyHandle::close);
            if (this.db != null) {
                this.db.close();
            }
            this.familyOptions.close();
            this.logger.close();
            this.options.close();
        }
    }
}
