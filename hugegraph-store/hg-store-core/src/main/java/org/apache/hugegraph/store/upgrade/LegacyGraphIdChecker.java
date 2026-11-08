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

package org.apache.hugegraph.store.upgrade;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.rocksdb.access.RocksDBOptions;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.store.meta.GraphIdManager;
import org.apache.hugegraph.store.meta.MetadataKeyHelper;
import org.apache.hugegraph.store.util.HgStoreException;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import com.google.protobuf.Int64Value;

/** Read-only inspection before Store registration or an incoming snapshot is opened for writing. */
public final class LegacyGraphIdChecker {

    private static final byte[] LEGACY = {(byte) 0xFF, (byte) 0xFE};
    private static final Pattern DB_NAME = Pattern.compile("([0-9]{5,})(?:_[0-9]+){0,2}");
    private static final Pattern SNAPSHOT_NAME = Pattern.compile("snapshot_([0-9]+)");

    private LegacyGraphIdChecker() {
    }

    public static HugeConfig configuration(Map<String, Object> options) {
        OptionSpace.register("rocksdb", RocksDBOptions.class.getName());
        return new HugeConfig(options);
    }

    public static void checkStore(String dataPaths, HugeConfig config) {
        try {
            inspect(dataPaths, config, false);
        } catch (IOException | RocksDBException | RuntimeException e) {
            throw new StartupPreflightException("GraphID upgrade preflight refused Store startup: " +
                    e.getMessage() + ". Preserve the complete 1.7 backup. Run " +
                    "bin/hugegraph-store-check.sh --check while the Store is stopped; " +
                    "see docs/graphid-upgrade.md", e);
        }
    }

    /** Identifies the startup refusal across Spring's initialization wrappers. */
    public static final class StartupPreflightException extends HgStoreException {

        private static final long serialVersionUID = 1L;

        private StartupPreflightException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** A running Store may have unflushed writes with WAL disabled: never certify its disk view. */
    public static InspectionResult checkOffline(String dataPaths, HugeConfig config)
            throws IOException, RocksDBException {
        return inspect(dataPaths, config, true);
    }

    public record InspectionResult(int databases, int snapshots, boolean metadata) {
    }

    public static void checkDatabase(Path path, HugeConfig config) throws IOException, RocksDBException {
        try (Database database = new Database(path, config)) {
            for (int i = 0; i < database.names.size(); i++) {
                try (RocksIterator iterator = database.db.newIterator(database.handles.get(i))) {
                    iterator.seek(LEGACY);
                    if (iterator.isValid() && startsWith(iterator.key(), LEGACY)) {
                        throw new IllegalStateException("Legacy GraphID 0xFFFE in " + path +
                                ", column family " + new String(database.names.get(i), StandardCharsets.UTF_8));
                    }
                    iterator.status();
                }
            }
            // Also reject a historical manual FFFE mapping whose business rows have been removed.
            Map<String, Long> ids = new TreeMap<>();
            byte[] prefix = MetadataKeyHelper.getGraphIDKey("");
            try (RocksIterator iterator = database.db.newIterator()) {
                for (iterator.seek(prefix); iterator.isValid() && startsWith(iterator.key(), prefix);
                     iterator.next()) {
                    byte[] key = iterator.key();
                    byte[] name = Arrays.copyOfRange(key, prefix.length, key.length);
                    String graph = new String(name, StandardCharsets.UTF_8);
                    // Replacement characters can collapse distinct damaged keys and hide an invalid ID.
                    if (!Arrays.equals(name, graph.getBytes(StandardCharsets.UTF_8))) {
                        throw new IllegalStateException("Invalid UTF-8 GraphID metadata in " + path);
                    }
                    ids.put(graph, Int64Value.parseFrom(iterator.value()).getValue());
                }
                iterator.status();
            }
            try {
                GraphIdManager.checkGraphIds(ids, -1);
            } catch (HgStoreException e) {
                throw new IllegalStateException("Invalid GraphID metadata in " + path + ": " + e.getMessage(), e);
            }
        }
    }

    private static InspectionResult inspect(String dataPaths, HugeConfig config, boolean offline)
            throws IOException, RocksDBException {
        List<Path> roots = new ArrayList<>();
        for (String value : dataPaths.split(",")) {
            if (value.isBlank()) {
                throw new IOException("Empty Store data path");
            }
            Path root = Path.of(value).toAbsolutePath().normalize();
            if (offline && !Files.isDirectory(root)) {
                throw new IOException("Expected an existing stopped Store data directory: " + root);
            }
            roots.add(root);
        }
        if (roots.isEmpty()) {
            throw new IOException("Empty Store data path");
        }
        try (DatabaseLocks locks = new DatabaseLocks(offline)) {
            Path metadata = roots.get(0).resolve("hgstore-metadata");
            boolean metadataExists = Files.exists(metadata, LinkOption.NOFOLLOW_LINKS);
            if (metadataExists) {
                requireCurrent(metadata);
                locks.add(metadata);
            }
            Set<Path> databases = new LinkedHashSet<>();
            Set<Path> snapshots = new LinkedHashSet<>();
            Set<Integer> discovered = new HashSet<>();
            for (Path root : roots) {
                discover(root, null, databases, snapshots, discovered);
            }
            if (metadataExists) {
                try (Database database = new Database(metadata, config);
                     RocksIterator iterator = database.db.newIterator()) {
                    byte[] prefix = MetadataKeyHelper.getPartitionStorePrefix();
                    for (iterator.seek(prefix); iterator.isValid() && startsWith(iterator.key(), prefix);
                         iterator.next()) {
                        Metapb.PartitionStore location = Metapb.PartitionStore.parseFrom(iterator.value());
                        // Normal moved roots are remapped by PartitionManager.resetPartitionPath().
                        if (!discovered.contains(location.getPartitionId())) {
                            discover(Path.of(location.getStoreLocation()), location.getPartitionId(),
                                     databases, snapshots, new HashSet<>());
                        }
                    }
                    iterator.status();
                }
            }
            for (Path database : databases) {
                requireCurrent(database);
                locks.add(database);
                checkDatabase(database, config);
            }
            for (Path snapshot : snapshots) {
                checkDatabase(snapshot, config);
            }
            return new InspectionResult(databases.size(), snapshots.size(), metadataExists);
        }
    }

    private static void discover(Path root, Integer onlyPartition, Set<Path> databases,
                                 Set<Path> snapshots, Set<Integer> discovered) throws IOException {
        for (Path path : children(root.resolve("db"))) {
            Matcher match = DB_NAME.matcher(path.getFileName().toString());
            if (Files.isDirectory(path) && match.matches()) {
                int partition = Integer.parseInt(match.group(1));
                if (onlyPartition == null || onlyPartition == partition) {
                    // Inspect retained versions without invoking findLatestDBPath(), which deletes older DBs.
                    databases.add(path.toRealPath());
                    discovered.add(partition);
                }
            }
        }
        // Snapshots live on the data disk even when Raft logs use a separate disk.
        for (Path partition : children(root.resolve("raft"))) {
            if (!partition.getFileName().toString().matches("[0-9]{5,}") ||
                onlyPartition != null && Integer.parseInt(partition.getFileName().toString()) != onlyPartition) {
                continue;
            }
            long latestIndex = -1L;
            Path latest = null;
            for (Path candidate : children(partition.resolve("snapshot"))) {
                Matcher match = SNAPSHOT_NAME.matcher(candidate.getFileName().toString());
                if (match.matches()) {
                    long index = Long.parseLong(match.group(1));
                    if (index > latestIndex) {
                        latest = candidate;
                        latestIndex = index;
                    }
                }
            }
            if (latest != null) {
                // Local should_not_load does not prevent this checkpoint being sent to a new replica.
                snapshots.add(latest.resolve("data").toAbsolutePath().normalize());
            }
        }
    }

    private static List<Path> children(Path parent) throws IOException {
        if (!Files.exists(parent)) {
            return List.of();
        }
        try (var stream = Files.list(parent)) {
            return stream.sorted().toList();
        }
    }

    private static void requireCurrent(Path path) throws IOException {
        if (!Files.isRegularFile(path.resolve("CURRENT"))) {
            throw new IOException("Incomplete RocksDB directory: " + path +
                    ". CURRENT is missing or not a regular file. Preserve this directory; see " +
                    "docs/graphid-upgrade.md#incomplete-database-directories");
        }
    }

    private static boolean startsWith(byte[] value, byte[] prefix) {
        return value.length >= prefix.length && Arrays.equals(value, 0, prefix.length, prefix, 0, prefix.length);
    }

    private static final class Database implements AutoCloseable {

        private final DBOptions options = new DBOptions();
        private final ColumnFamilyOptions familyOptions = new ColumnFamilyOptions();
        private final List<ColumnFamilyHandle> handles = new ArrayList<>();
        private final Logger logger = new Logger(this.options) {
            @Override
            protected void log(InfoLogLevel level, String message) {
                // Read-only open must not create or rotate LOG files in the inspected database.
            }
        };
        private List<byte[]> names;
        private RocksDB db;

        private Database(Path path, HugeConfig config) throws IOException, RocksDBException {
            try {
                requireCurrent(path);
                RocksDBSession.initOptions(config, null, null, this.familyOptions, this.familyOptions);
                this.options.setCreateIfMissing(false).setCreateMissingColumnFamilies(false).setLogger(this.logger);
                try (Options listing = new Options().setLogger(this.logger)) {
                    this.names = RocksDB.listColumnFamilies(listing, path.toString());
                }
                List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
                for (byte[] name : this.names) {
                    descriptors.add(new ColumnFamilyDescriptor(name, this.familyOptions));
                }
                this.db = RocksDB.openReadOnly(this.options, path.toString(), descriptors, this.handles);
            } catch (IOException | RocksDBException | RuntimeException | Error e) {
                this.close();
                throw e;
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

    private static final class DatabaseLocks implements AutoCloseable {

        private final List<FileChannel> channels = new ArrayList<>();
        private final Set<Path> paths = new HashSet<>();
        private final boolean offline;

        private DatabaseLocks(boolean offline) {
            this.offline = offline;
        }

        private void add(Path database) throws IOException {
            Path lockPath = database.resolve("LOCK");
            if (!this.offline) {
                try {
                    // A checkpoint moved into db/ may not contain a LOCK yet. Create the same
                    // stable inode RocksDB will use, without replacing an existing lock.
                    Files.createFile(lockPath);
                } catch (FileAlreadyExistsException ignored) {
                    // The RocksDB writer or another checker already created it.
                }
            } else if (!Files.exists(lockPath)) {
                throw new IOException("Cannot check offline without a RocksDB LOCK file: " + database +
                                      ". Preserve the data and consult the upgrade guide.");
            }
            Path path = lockPath.toRealPath();
            if (!this.paths.add(path)) {
                return;
            }
            FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
            this.channels.add(channel);
            FileLock lock = channel.tryLock(0L, Long.MAX_VALUE, true);
            if (lock == null) {
                throw new IOException("Store is running or database is locked: " + database);
            }
        }

        @Override
        public void close() throws IOException {
            IOException failure = null;
            for (int i = this.channels.size() - 1; i >= 0; i--) {
                try {
                    this.channels.get(i).close();
                } catch (IOException e) {
                    failure = e;
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }
}
