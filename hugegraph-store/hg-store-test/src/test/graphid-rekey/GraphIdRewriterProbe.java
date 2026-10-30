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

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import com.google.protobuf.Int64Value;

/** Native synthetic fixtures. These are deliberately NOT called 1.7 cluster upgrade tests. */
public final class GraphIdRewriterProbe {
    private static final String BUSINESS = "DEFAULT/business/g";
    private static final String SYSTEM = "DEFAULT/~sys_graph/g";
    private static final List<String> TABLES = List.of("g+v", "g+ie", "g+oe", "g+index", "custom");
    private static int assertions;

    @FunctionalInterface
    private interface Checked {
        void run() throws Exception;
    }

    private static void check(boolean value, String message) {
        assertions++;
        if (!value) {
            throw new AssertionError(message);
        }
    }

    private static void refused(Checked operation, String message) throws Exception {
        assertions++;
        try {
            operation.run();
        } catch (Exception expected) {
            check(expected.toString().contains(message), expected.toString());
            return;
        }
        throw new AssertionError("Expected refusal: " + message);
    }

    private static byte[] key(int id, int row) {
        return ByteBuffer.allocate(9).putShort((short) id).put((byte) (1 + row % 2))
                         .putInt(row).putShort((short) (row % 65536)).array();
    }

    private static byte[] value(int row) {
        // Include opaque expiry-like bytes to catch accidental value re-serialization.
        return ByteBuffer.allocate(16).putLong(1800000000000L + row).putLong(row).array();
    }

    private static List<GraphIdMigrationPlan.Rule> rules() {
        List<GraphIdMigrationPlan.Rule> rules = new ArrayList<>();
        for (String table : TABLES) {
            rules.add(new GraphIdMigrationPlan.Rule(table, "01", BUSINESS));
            rules.add(new GraphIdMigrationPlan.Rule(table, "02", SYSTEM));
        }
        return rules;
    }

    private static void seed(Path path, int rows) throws Exception {
        Files.createDirectory(path);
        try (Options options = new Options().setCreateIfMissing(true).setNumLevels(8);
             ColumnFamilyOptions cf = new ColumnFamilyOptions().setNumLevels(8);
             RocksDB db = RocksDB.open(options, path.toString())) {
            db.put("HUGEGRAPH/GRAPH_ID/DEFAULT/existing/g".getBytes(StandardCharsets.UTF_8),
                   Int64Value.of(7).toByteArray());
            db.put(GraphIdRewriter.slotKey(7), Int64Value.of(7).toByteArray());
            db.put("store-private-metadata".getBytes(StandardCharsets.UTF_8), new byte[]{5, 6});
            for (String table : TABLES) {
                try (ColumnFamilyHandle handle = db.createColumnFamily(
                        new ColumnFamilyDescriptor(table.getBytes(StandardCharsets.UTF_8), cf))) {
                    for (int row = 0; row < rows; row++) {
                        db.put(handle, key(65534, row), value(row));
                    }
                    db.put(handle, key(7, rows + 1), value(rows + 1));
                    try (FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
                        db.flush(flush, handle);
                    }
                }
            }
            try (FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
                db.flush(flush);
            }
        }
    }

    private static void seedExactBatch(Path path, int rows) throws Exception {
        Files.createDirectory(path);
        try (Options options = new Options().setCreateIfMissing(true).setNumLevels(8);
             ColumnFamilyOptions cf = new ColumnFamilyOptions().setNumLevels(8);
             RocksDB db = RocksDB.open(options, path.toString());
             ColumnFamilyHandle handle = db.createColumnFamily(
                     new ColumnFamilyDescriptor("g+v".getBytes(StandardCharsets.UTF_8), cf))) {
            for (int row = 0; row < rows; row++) {
                db.put(handle, key(65534, row * 2), value(row));
            }
            try (FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
                db.flush(flush, handle);
                db.flush(flush);
            }
        }
    }

    private static boolean canAcquireSourceLock(Path source, Path log) {
        try {
            Process contender = new ProcessBuilder(
                    Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-cp", System.getProperty("java.class.path"),
                    GraphIdRewriterProbe.class.getName(), "try-lock", source.toString())
                    .redirectErrorStream(true).redirectOutput(log.toFile()).start();
            if (!contender.waitFor(10, TimeUnit.SECONDS)) {
                contender.destroyForcibly();
                contender.waitFor(10, TimeUnit.SECONDS);
                throw new IllegalStateException("Timed out waiting for lock contender");
            }
            int exit = contender.exitValue();
            if (exit == 0) {
                return true;
            }
            if (exit == 2) {
                return false;
            }
            throw new IllegalStateException("Lock contender failed with exit " + exit);
        } catch (Exception error) {
            throw new IllegalStateException("Failed to run lock contender", error);
        }
    }

    private static Map<String, String> physical(Path path) throws Exception {
        Map<String, String> result = new TreeMap<>();
        try (var files = Files.walk(path)) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                result.put(path.relativize(file).toString(), Files.getLastModifiedTime(file) + ":" +
                           HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                                                                .digest(Files.readAllBytes(file))));
            }
        }
        return result;
    }

    private static final class OpenDb implements AutoCloseable {
        private final Options listing = new Options();
        private final DBOptions options = new DBOptions();
        private final ColumnFamilyOptions cf = new ColumnFamilyOptions().setNumLevels(8);
        private final List<ColumnFamilyHandle> handles = new ArrayList<>();
        private final Map<String, ColumnFamilyHandle> families = new TreeMap<>();
        private final RocksDB db;

        private OpenDb(Path path, boolean readOnly) throws Exception {
            List<byte[]> names = RocksDB.listColumnFamilies(this.listing, path.toString());
            List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
            names.forEach(name -> descriptors.add(new ColumnFamilyDescriptor(name, this.cf)));
            this.db = readOnly ? RocksDB.openReadOnly(this.options, path.toString(), descriptors, this.handles) :
                      RocksDB.open(this.options, path.toString(), descriptors, this.handles);
            for (int i = 0; i < names.size(); i++) {
                this.families.put(new String(names.get(i), StandardCharsets.UTF_8), this.handles.get(i));
            }
        }

        @Override
        public void close() {
            this.handles.forEach(ColumnFamilyHandle::close);
            this.db.close();
            this.cf.close();
            this.options.close();
            this.listing.close();
        }
    }

    public static void main(String[] args) throws Exception {
        RocksDB.loadLibrary();
        if (args.length > 0 && args[0].equals("try-lock")) {
            try (FileChannel channel = FileChannel.open(Path.of(args[1]).resolve("LOCK"),
                                                        StandardOpenOption.WRITE);
                 FileLock lock = channel.tryLock()) {
                if (lock == null) {
                    System.exit(2);
                }
                return;
            } catch (Exception error) {
                System.err.println(error);
                System.exit(3);
            }
        }
        if (args.length > 0 && args[0].equals("hold")) {
            try (OpenDb db = new OpenDb(Path.of(args[1]), false)) {
                Files.writeString(Path.of(args[2]), "locked");
                System.in.read();
            }
            return;
        }
        Path root = Path.of(args[0]).toAbsolutePath();
        Files.createDirectories(root);
        int rows = 2000;
        Path source = root.resolve("source");
        seed(source, rows);
        Map<String, String> original = physical(source);
        try (ColumnFamilyOptions cf = new ColumnFamilyOptions().setNumLevels(8).setMergeOperatorName("uint64add")) {
            GraphIdMigrationPlan plan = GraphIdRewriter.plan(source, cf, rules());
            check(original.equals(physical(source)), "Planning changed source files/mtimes");
            check(plan.targets().get(BUSINESS) == 0 && plan.targets().get(SYSTEM) == 1, "Allocation");
            long started = System.nanoTime();
            GraphIdRewriter.Result result = GraphIdRewriter.convert(source, root.resolve("converted"), cf, plan);
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            check(result.converted() == (long) rows * TABLES.size(), "Converted count");
            check(original.equals(physical(source)), "Conversion changed source files/mtimes");
            try (OpenDb target = new OpenDb(result.database(), true)) {
                for (String table : TABLES) {
                    for (int row = 0; row < rows; row++) {
                        check(Arrays.equals(value(row), target.db.get(target.families.get(table),
                                                                      key(row % 2, row))), "Value/key mismatch");
                        check(target.db.get(target.families.get(table), key(65534, row)) == null,
                              "Legacy key remains");
                    }
                    check(Arrays.equals(value(rows + 1), target.db.get(target.families.get(table),
                                                                        key(7, rows + 1))), "Unrelated data changed");
                }
                check(Int64Value.parseFrom(target.db.get(GraphIdRewriter.slotKey(0))).getValue() == 0,
                      "Missing allocator slot 0");
            }
            GraphIdRewriter.Result retry = GraphIdRewriter.convert(source, root.resolve("retry"), cf, plan);
            check(result.targetDigest().equals(retry.targetDigest()), "Non-deterministic retry");
            refused(() -> GraphIdRewriter.convert(source, root.resolve("converted"), cf, plan), "Output must be new");
            refused(() -> GraphIdRewriter.convert(source, source.resolve("nested"), cf, plan), "Output must be new");
            List<GraphIdMigrationPlan.Rule> missing = new ArrayList<>(rules());
            missing.remove(0);
            refused(() -> GraphIdRewriter.plan(source, cf, missing), "No proven owner");
            refused(() -> GraphIdRewriter.convert(source, root.resolve("interrupted"), cf, plan,
                                                  n -> { throw new IllegalStateException("injected after durable batch"); }),
                    "injected after durable batch");
            check(Files.exists(root.resolve("interrupted/INCOMPLETE")), "Missing incomplete marker");
            check(!Files.exists(root.resolve("interrupted/DB_VERIFIED_NOT_CLUSTER_READY")), "False certificate");
            check(original.equals(physical(source)), "Interrupted conversion changed original");
            refused(() -> GraphIdRewriter.convert(source, root.resolve("lock-protected"), cf, plan, n -> {
                check(!canAcquireSourceLock(source, root.resolve("lock-contender.log")),
                      "Copying the cold DB released the source lock");
                throw new IllegalStateException("stop after lock retention check");
            }), "stop after lock retention check");
            check(Files.exists(root.resolve("lock-protected/INCOMPLETE")),
                  "Lock test output lost incomplete marker");

            Path exactSource = root.resolve("exact-batch-source");
            seedExactBatch(exactSource, 256);
            List<GraphIdMigrationPlan.Rule> exactRules = List.of(
                    new GraphIdMigrationPlan.Rule("g+v", "01", BUSINESS));
            GraphIdMigrationPlan exactPlan = GraphIdRewriter.plan(exactSource, cf, exactRules);
            try {
                GraphIdRewriter.convert(exactSource, root.resolve("exact-batch-interrupted"), cf,
                                        exactPlan, n -> Thread.currentThread().interrupt());
                throw new AssertionError("Interrupted final batch returned success");
            } catch (InterruptedException expected) {
                check(expected.getMessage().contains("Conversion interrupted"), expected.toString());
            } finally {
                Thread.interrupted();
            }
            check(Files.exists(root.resolve("exact-batch-interrupted/INCOMPLETE")),
                  "Final-batch interruption lost incomplete marker");
            check(!Files.exists(root.resolve(
                          "exact-batch-interrupted/DB_VERIFIED_NOT_CLUSTER_READY")),
                  "Final-batch interruption published a false certificate");

            GraphIdRewriter.convert(source, root.resolve("after-interruption"), cf, plan);
            check(original.equals(physical(source)), "Retry changed original");
            try (OpenDb changed = new OpenDb(source, false)) {
                changed.db.put("new-metadata".getBytes(StandardCharsets.UTF_8), new byte[]{9});
            }
            refused(() -> GraphIdRewriter.convert(source, root.resolve("stale"), cf, plan), "Stale source");
            check(!Files.exists(root.resolve("stale")), "Stale plan created output");
            try (OpenDb changed = new OpenDb(source, false)) {
                changed.db.put(("HUGEGRAPH/GRAPH_ID/" + BUSINESS).getBytes(StandardCharsets.UTF_8),
                               Int64Value.of(12).toByteArray());
            }
            refused(() -> GraphIdRewriter.plan(source, cf, rules()), "Mixed/already allocated");
            // This is the actual mapping-only counterexample, not an in-memory simulation.
            try (OpenDb alias = new OpenDb(source, true)) {
                check(alias.db.get(alias.families.get("g+v"), key(12, 0)) == null, "Alias unexpectedly moved data");
                check(alias.db.get(alias.families.get("g+v"), key(65534, 0)) != null, "Legacy source vanished");
            }
            Path locked = root.resolve("locked");
            seed(locked, 2);
            Path ready = root.resolve("lock-ready");
            Process holder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-cp", System.getProperty("java.class.path"), GraphIdRewriterProbe.class.getName(),
                    "hold", locked.toString(), ready.toString()).redirectErrorStream(true)
                    .redirectOutput(root.resolve("holder.log").toFile()).start();
            try {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (!Files.exists(ready) && holder.isAlive() && System.nanoTime() < deadline) {
                    Thread.sleep(10);
                }
                check(Files.exists(ready), "Native lock holder did not start");
                refused(() -> GraphIdRewriter.plan(locked, cf, rules()), "locked");
            } finally {
                holder.getOutputStream().close();
                if (!holder.waitFor(10, TimeUnit.SECONDS)) {
                    holder.destroyForcibly();
                    holder.waitFor(10, TimeUnit.SECONDS);
                }
            }
            GraphIdRewriter.plan(locked, cf, rules());
            System.out.printf("PASS: %d native assertions; rekeyed=%d; elapsed_ms=%d; JVM=%s%n",
                              assertions, result.converted(), millis, System.getProperty("java.version"));
            System.out.println("NOT TESTED: official 1.7 fixture, ownership inference, Raft, real snapshots, cluster activation.");
        }
    }
}
