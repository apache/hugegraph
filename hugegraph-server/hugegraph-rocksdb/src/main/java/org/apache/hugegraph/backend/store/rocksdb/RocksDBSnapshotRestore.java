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

package org.apache.hugegraph.backend.store.rocksdb;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

/**
 * Fail-closed snapshot installation. The marker lives outside the data directory
 * and the original checkpoint remains the recovery source until after reopening.
 * A subsequent open retries the recorded checkpoint before native recovery runs.
 */
final class RocksDBSnapshotRestore {

    private static final Logger LOG = Log.logger(RocksDBSnapshotRestore.class);

    private final File data;
    private final File wal;
    private final File snapshot;
    private final Path marker;
    private final FileOperations files;
    private final Path configuredWal;
    private String generation;
    private String operation;
    private final List<Path> walLinks = new ArrayList<>();
    private final List<Path> walLinkTargets = new ArrayList<>();

    RocksDBSnapshotRestore(String data, String wal, String snapshot,
                           FileOperations files) throws IOException {
        this.data = dataDirectory(data);
        this.wal = wal == null || wal.isEmpty() ? this.data :
                   new File(wal).getCanonicalFile();
        this.snapshot = new File(snapshot).getCanonicalFile();
        this.marker = marker(this.data.toString());
        this.files = files;
        this.configuredWal = wal == null || wal.isEmpty() ? this.data.toPath() :
                             new File(wal).toPath().toAbsolutePath().normalize();
        Path rawWal = wal == null || wal.isEmpty() ? this.data.toPath() :
                      new File(wal).toPath().toAbsolutePath();
        this.validateWalLinks(rawWal);
        Path component = this.configuredWal.getRoot();
        for (Path name : this.configuredWal) {
            component = component.resolve(name);
            if (Files.isSymbolicLink(component)) {
                this.walLinks.add(component);
                this.walLinkTargets.add(Files.readSymbolicLink(component));
            }
        }
        this.validatePaths();
        validateCheckpoint(this.snapshot);
    }

    private static File dataDirectory(String data) throws IOException {
        Path configured = new File(data).toPath().toAbsolutePath();
        // A direct DB alias becomes dangling while install replaces its target,
        // changing canonical marker/lock identity. Stable parent aliases are fine.
        while (configured.getFileName() != null && ".".equals(configured.getFileName().toString())) {
            configured = configured.getParent();
        }
        if (Files.isSymbolicLink(configured)) {
            throw new IOException("Database directory cannot itself be a symbolic link; " +
                                  "use a stable parent directory alias instead: " + configured);
        }
        return new File(data).getCanonicalFile();
    }

    private void validateWalLinks(Path rawWal) throws IOException {
        boolean parentTraversal = false;
        for (Path part : rawWal) {
            parentTraversal |= "..".equals(part.toString());
        }
        Path component = rawWal.getRoot();
        for (Path part : rawWal) {
            component = component.resolve(part);
            if (Files.isSymbolicLink(component)) {
                if (parentTraversal && this.linkInsideData(component)) {
                    throw new IOException("WAL cannot combine a link inside data with '..': " + rawWal);
                }
                this.validateWalLinkTarget(component, 0);
            }
        }
    }

    // Inspect target paths without normalizing away filesystem link/.. semantics.
    // Only reject hidden links that data replacement would delete; external
    // parent aliases and directly recorded simple WAL links remain supported.
    private void validateWalLinkTarget(Path link, int depth) throws IOException {
        if (depth >= 40) {
            throw new IOException("WAL symbolic link target chain is too deep: " + link);
        }
        Path target = Files.readSymbolicLink(link);
        if (!target.isAbsolute()) {
            target = link.getParent().resolve(target);
        }
        Path component = target.getRoot();
        for (Path part : target) {
            component = component.resolve(part);
            if (Files.isSymbolicLink(component)) {
                if (this.linkInsideData(component)) {
                    throw new IOException("WAL target chain contains an unrecorded link inside data: " + component);
                }
                this.validateWalLinkTarget(component, depth + 1);
            }
        }
    }

    private boolean linkInsideData(Path link) throws IOException {
        Path parent = link.getParent().toFile().getCanonicalFile().toPath();
        if (parent.startsWith(this.data.toPath())) {
            return true;
        }
        // Parent bind aliases have different canonical strings but the same inode.
        if (Files.exists(this.data.toPath())) {
            for (Path ancestor = parent; ancestor != null; ancestor = ancestor.getParent()) {
                if (Files.isSameFile(ancestor, this.data.toPath())) {
                    return true;
                }
            }
        }
        return false;
    }

    private static void validateCheckpoint(File snapshot) throws IOException {
        Path current = new File(snapshot, "CURRENT").toPath();
        if (!Files.isRegularFile(current, LinkOption.NOFOLLOW_LINKS) || Files.size(current) > 128L) {
            throw new IOException("Checkpoint CURRENT is missing or invalid: " + snapshot);
        }
        String manifest = new String(Files.readAllBytes(current), StandardCharsets.UTF_8).trim();
        if (!manifest.matches("MANIFEST-[0-9]+") ||
            !Files.isRegularFile(new File(snapshot, manifest).toPath(), LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Checkpoint MANIFEST is missing or invalid: " + snapshot);
        }
    }

    private static String generation(File snapshot) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            Path root = snapshot.toPath();
            try (Stream<Path> paths = Files.walk(root)) {
                for (Path path : paths.sorted(Comparator.comparing(Path::toString)).collect(Collectors.toList())) {
                    BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class,
                                                                     LinkOption.NOFOLLOW_LINKS);
                    if (!attrs.isDirectory() && !attrs.isRegularFile()) {
                        throw new IOException("Invalid checkpoint entry: " + path);
                    }
                    digest.update((root.relativize(path) + "\n" + attrs.fileKey() + "\n").getBytes(
                            StandardCharsets.UTF_8));
                    if (attrs.isDirectory()) {
                        continue;
                    }
                    digest.update((attrs.size() + "\n").getBytes(StandardCharsets.UTF_8));
                    // SSTs are immutable. Their filesystem identity avoids rereading
                    // all table data on each retry; filesystems without fileKey use content.
                    if (path.toString().endsWith(".sst") && attrs.fileKey() != null) {
                        continue;
                    }
                    try (InputStream input = Files.newInputStream(path)) {
                        byte[] buffer = new byte[8192];
                        int count;
                        while ((count = input.read(buffer)) != -1) {
                            digest.update(buffer, 0, count);
                        }
                    }
                }
            }
            StringBuilder value = new StringBuilder();
            for (byte part : digest.digest()) {
                value.append(String.format("%02x", part & 0xff));
            }
            return value.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }

    private static String metadataDigest(Properties state) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String key : new TreeSet<>(state.stringPropertyNames())) {
                if ("metadata-sha256".equals(key)) {
                    continue;
                }
                digestMetadataString(digest, key);
                digestMetadataString(digest, state.getProperty(key));
            }
            StringBuilder value = new StringBuilder();
            for (byte part : digest.digest()) {
                value.append(String.format("%02x", part & 0xff));
            }
            return value.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }

    private static void digestMetadataString(MessageDigest digest, String value) {
        // Length-prefixed UTF-16 code units preserve every Properties string,
        // including escaped delimiters and unpaired surrogates, without ambiguity.
        int length = value.length();
        for (int shift = 24; shift >= 0; shift -= 8) {
            digest.update((byte) (length >>> shift));
        }
        for (int i = 0; i < length; i++) {
            char part = value.charAt(i);
            digest.update((byte) (part >>> 8));
            digest.update((byte) part);
        }
    }

    private static void validateMetadata(Properties state, Path marker) throws IOException {
        String checksum = state.getProperty("metadata-sha256");
        if (checksum == null || !checksum.matches("[0-9a-f]{64}") ||
            !checksum.equals(metadataDigest(state))) {
            throw new IOException("Recovery metadata checksum missing or changed; preserve marker: " + marker);
        }
    }

    private void validateGeneration(Properties state) throws IOException {
        this.generation = state.getProperty("generation");
        this.operation = state.getProperty("operation");
        if (this.generation == null || this.operation == null ||
            !this.operation.matches("[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}") ||
            !this.data.toString().equals(state.getProperty("data")) ||
            !this.generation.equals(generation(this.snapshot))) {
            throw new IOException("Checkpoint generation changed or recovery identity missing: " + this.marker);
        }
    }

    private Path staging() {
        // A sibling survives every supported nesting of the data and WAL roots.
        return Paths.get(this.data + ".resume-staging-" + this.operation);
    }

    private static boolean overlaps(File first, File second) throws IOException {
        return contains(first, second) || contains(second, first);
    }

    private static boolean contains(File root, File directory) throws IOException {
        Path parent = root.toPath();
        Path child = directory.toPath();
        if (child.startsWith(parent)) {
            return true;
        }
        // Parent bind aliases keep distinct canonical strings. Match existing
        // physical prefixes, then compare their remaining directory suffixes.
        for (Path left = parent; left != null; left = left.getParent()) {
            if (Files.notExists(left)) {
                continue;
            }
            for (Path right = child; right != null; right = right.getParent()) {
                if (!Files.notExists(right) && Files.isSameFile(left, right)) {
                    Path prefix = left.relativize(parent);
                    Path suffix = right.relativize(child);
                    if (prefix.toString().isEmpty() || suffix.startsWith(prefix)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private void validatePaths() throws IOException {
        if (overlaps(this.snapshot, this.data) || contains(this.snapshot, this.wal)) {
            throw new IOException("Checkpoint must not overlap data or WAL: " + this.snapshot);
        }
    }

    private static Path marker(String data) throws IOException {
        return new File(dataDirectory(data) + ".resume-pending").toPath();
    }

    // Caller must hold the native owner lease or the same database recovery lock.
    static boolean hasNoPendingRestore(String data) throws IOException {
        return Files.notExists(marker(data), LinkOption.NOFOLLOW_LINKS);
    }

    static RecoveryLock lock(String data) {
        try {
            Path directory = dataDirectory(data).toPath();
            Path parent = directory.getParent();
            if (parent == null) {
                throw new IOException("Database directory cannot be a filesystem root: " + directory);
            }
            // Sibling guards must share the DB's backing parent. Binding just the
            // DB directory hides those guards at aliases and cannot be replaced
            // during restore (EBUSY). Mount the parent data root instead.
            if (Files.isDirectory(directory) &&
                (System.getProperty("os.name").startsWith("Linux") ? isLinuxMountPoint(directory) :
                 !Files.getFileStore(directory).equals(Files.getFileStore(parent)))) {
                throw new IOException("Database directory cannot itself be a mount point; " +
                                      "mount its parent data root instead: " + directory);
            }
            Path path = new File(directory + ".resume-lock").toPath();
            Files.createDirectories(path.getParent());
            return RecoveryLock.acquire(path, data);
        } catch (IOException | OverlappingFileLockException e) {
            throw new BackendException("Cannot lock database for open/recovery: '%s'", e, data);
        }
    }

    private static boolean isLinuxMountPoint(Path directory) throws IOException {
        // FileStore equality cannot detect a bind mount within the same filesystem.
        // Read this process's namespace; kernel mountinfo escapes whitespace and backslashes.
        for (String mount : Files.readAllLines(Paths.get("/proc/self/mountinfo"),
                                              StandardCharsets.UTF_8)) {
            String[] fields = mount.split(" ", 7);
            if (fields.length < 7) {
                throw new IOException("Invalid Linux mountinfo record");
            }
            String path = fields[4].replace("\\040", " ").replace("\\011", "\t")
                                   .replace("\\012", "\n").replace("\\134", "\\");
            if (directory.equals(Paths.get(path))) {
                return true;
            }
        }
        return false;
    }

    static void unlock(RecoveryLock channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                throw new BackendException("Failed to release database recovery lock", e);
            }
        }
    }

    static void unlock(RecoveryLock channel, Throwable failure) {
        try {
            unlock(channel);
        } catch (RuntimeException | Error closeFailure) {
            if (closeFailure != failure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }

    static void start(String data, String wal, String snapshot) throws IOException {
        Path marker = marker(data);
        if (Files.exists(marker, LinkOption.NOFOLLOW_LINKS)) {
            Properties pending = new Properties();
            try (InputStream input = Files.newInputStream(marker)) {
                pending.load(input);
            }
            validateMetadata(pending, marker);
            Path configuredWal = wal == null || wal.isEmpty() ? new File(data).getCanonicalFile().toPath() :
                                 new File(wal).toPath().toAbsolutePath().normalize();
            if (!new File(snapshot).getCanonicalPath().equals(pending.getProperty("snapshot")) ||
                !configuredWal.toString().equals(pending.getProperty("configured-wal"))) {
                throw new IOException("Another or incomplete checkpoint restore is pending: " + marker);
            }
            // prepareOpen will reconstruct links and revalidate the recorded source.
            return;
        }
        new RocksDBSnapshotRestore(data, wal, snapshot, new FileOperations()).begin();
    }

    void begin() throws IOException {
        this.validatePaths();
        Properties state = new Properties();
        this.generation = generation(this.snapshot);
        this.operation = UUID.randomUUID().toString();
        state.setProperty("generation", this.generation);
        state.setProperty("operation", this.operation);
        state.setProperty("data", this.data.toString());
        state.setProperty("snapshot", this.snapshot.toString());
        state.setProperty("wal", this.wal.toString());
        state.setProperty("configured-wal", this.configuredWal.toString());
        state.setProperty("wal-links", Integer.toString(this.walLinks.size()));
        for (int i = 0; i < this.walLinks.size(); i++) {
            state.setProperty("wal-link-" + i, this.walLinks.get(i).toString());
            state.setProperty("wal-link-target-" + i, this.walLinkTargets.get(i).toString());
        }
        state.setProperty("metadata-sha256", metadataDigest(state));
        // Publish only a complete, forced record without replacing an interrupted restore.
        try {
            this.files.publishMarker(this.marker, state);
        } catch (FileAlreadyExistsException e) {
            Properties pending = new Properties();
            try (InputStream input = Files.newInputStream(this.marker)) {
                pending.load(input);
            }
            validateMetadata(pending, this.marker);
            this.validateGeneration(pending);
            state.setProperty("operation", this.operation);
            state.setProperty("metadata-sha256", metadataDigest(state));
            if (!state.equals(pending)) {
                throw new IOException("Another checkpoint restore is pending: " + this.marker, e);
            }
        }
        try (FileChannel channel = FileChannel.open(this.marker, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
        this.files.forceDirectory(this.marker.getParent());
    }

    static RocksDBSnapshotRestore prepareOpen(String data, String wal) {
        try {
            Path marker = marker(data);
            if (Files.notExists(marker, LinkOption.NOFOLLOW_LINKS)) {
                return null;
            }
            if (!Files.isRegularFile(marker, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("Invalid recovery marker " + marker);
            }
            Properties state = new Properties();
            try (InputStream input = Files.newInputStream(marker)) {
                state.load(input);
            }
            validateMetadata(state, marker);
            String snapshot = state.getProperty("snapshot");
            String savedWal = state.getProperty("wal");
            if (snapshot == null || savedWal == null) {
                throw new IOException("Incomplete recovery marker " + marker);
            }
            Path configuredWal = wal == null || wal.isEmpty() ? new File(data).getCanonicalFile().toPath() :
                                 new File(wal).toPath().toAbsolutePath().normalize();
            if (!configuredWal.toString().equals(state.getProperty("configured-wal"))) {
                throw new IOException("WAL configuration changed: " + marker);
            }
            String operation = state.getProperty("operation");
            String generation = state.getProperty("generation");
            File source = new File(snapshot).getCanonicalFile();
            File target = new File(data).getCanonicalFile();
            if (operation == null || !operation.matches("[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}") ||
                generation == null || !generation.matches("[0-9a-f]{64}") ||
                !target.toString().equals(state.getProperty("data")) ||
                !source.toString().equals(snapshot) || overlaps(source, target) ||
                !new File(savedWal).isAbsolute() ||
                !Paths.get(savedWal).normalize().toString().equals(savedWal) ||
                contains(source, new File(savedWal)) ||
                !generation(source).equals(generation)) {
                throw new IOException("Checkpoint generation changed or recovery identity invalid: " + marker);
            }
            int links;
            try {
                links = Integer.parseInt(state.getProperty("wal-links"));
            } catch (NumberFormatException e) {
                throw new IOException("Invalid WAL links in recovery marker " + marker, e);
            }
            if (links < 0 || links > configuredWal.getNameCount()) {
                throw new IOException("Invalid WAL link count: " + marker);
            }
            List<Path> recordedLinks = new ArrayList<>();
            List<Path> recordedTargets = new ArrayList<>();
            for (int i = 0; i < links; i++) {
                String link = state.getProperty("wal-link-" + i);
                String linkTarget = state.getProperty("wal-link-target-" + i);
                if (link == null || linkTarget == null || !Paths.get(link).isAbsolute() ||
                    !Paths.get(link).normalize().toString().equals(link) ||
                    !configuredWal.startsWith(Paths.get(link))) {
                    throw new IOException("Invalid WAL link in recovery marker " + marker);
                }
                recordedLinks.add(Paths.get(link));
                recordedTargets.add(Paths.get(linkTarget));
            }
            // The complete record is checksum-verified before reconstructing any alias.
            // Real canonical WAL identity is checked again after links are restored.
            for (int i = 0; i < links; i++) {
                restoreLink(recordedLinks.get(i), recordedTargets.get(i));
            }
            RocksDBSnapshotRestore restore =
                    new RocksDBSnapshotRestore(data, wal, snapshot, new FileOperations());
            if (!restore.wal.toString().equals(savedWal)) {
                throw new IOException("WAL configuration changed during recovery: " + marker);
            }
            restore.validateGeneration(state);
            restore.install();
            return restore;
        } catch (IOException e) {
            throw new BackendException("Pending snapshot recovery failed for '%s'; " +
                                       "preserve the .resume-pending marker and checkpoint", e, data);
        }
    }

    void install() throws IOException {
        this.validatePaths();
        if (!this.generation.equals(generation(this.snapshot))) {
            throw new IOException("Checkpoint generation changed: " + this.snapshot);
        }
        // A preceding publication fsync may have failed. Persist its entry before retry deletes live data.
        this.files.forceDirectory(this.marker.getParent());
        // Copy, never move: partial data copies and WAL failures must be retryable.
        this.files.deleteDirectory(this.data);
        this.files.copyDirectory(this.snapshot, this.data);
        verifyTree(this.snapshot.toPath(), this.data.toPath());
        for (int i = 0; i < this.walLinks.size(); i++) {
            restoreLink(this.walLinks.get(i), this.walLinkTargets.get(i));
        }
        this.installWal();
    }

    private static void verifyTree(Path source, Path copy) throws IOException {
        try (Stream<Path> paths = Files.walk(source)) {
            for (Path path : (Iterable<Path>) paths::iterator) {
                Path target = copy.resolve(source.relativize(path));
                if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
                    if (!Files.isDirectory(target, LinkOption.NOFOLLOW_LINKS)) {
                        throw new IOException("Missing checkpoint directory: " + target);
                    }
                } else if (Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) &&
                           Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) {
                    verify(path.toFile(), target.toFile());
                } else {
                    throw new IOException("Invalid checkpoint file: " + path);
                }
            }
        }
    }

    private static void restoreLink(Path link, Path target) throws IOException {
        if (!Files.exists(link, LinkOption.NOFOLLOW_LINKS)) {
            Files.createDirectories(link.getParent());
            Files.createSymbolicLink(link, target);
        } else if (!Files.isSymbolicLink(link) || !Files.readSymbolicLink(link).equals(target)) {
            throw new IOException("WAL link changed during recovery: " + link);
        }
    }

    void complete() {
        try {
            this.validatePaths();
            // Only this operation's staging is reclaimed, after successful native reopen.
            FileUtils.deleteDirectory(this.staging().toFile());
            Files.delete(this.marker);
            // Do not consume the only retry source until marker removal is durable.
            this.files.forceDirectory(this.marker.getParent());
        } catch (IOException e) {
            throw new BackendException("Failed to finish snapshot recovery at '%s'", e, this.marker);
        }
        // Restore historically consumes its source. Only clean it after native
        // reopen succeeded AND the marker no longer directs retries to it.
        try {
            this.validatePaths();
            FileUtils.deleteDirectory(this.snapshot);
        } catch (IOException e) {
            LOG.warn("Snapshot restored but source cleanup failed: {}", this.snapshot, e);
        }
    }

    private void installWal() throws IOException {
        FileUtils.forceMkdir(this.wal);
        if (this.data.equals(this.wal) || Files.isSameFile(this.data.toPath(), this.wal.toPath())) {
            return;
        }
        List<File> source = logs(this.data);
        Path staging = this.staging();
        if (Files.exists(staging, LinkOption.NOFOLLOW_LINKS) &&
            !Files.isDirectory(staging, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Invalid operation staging directory: " + staging);
        }
        Files.createDirectories(staging);
        // The persisted operation UUID names this directory exclusively. Retry
        // republishes the verified checkpoint; retain all retired WAL until reopen.
        for (File log : source) {
            File staged = staging.resolve(log.getName()).toFile();
            this.files.copyFile(log, staged);
            verify(log, staged);
        }
        for (File old : logs(this.wal)) {
            Path aside = staging.resolve(old.getName() + ".aside");
            if (Files.exists(aside, LinkOption.NOFOLLOW_LINKS)) {
                // An earlier attempt already preserved the original WAL.
                Files.delete(old.toPath());
            } else {
                this.files.move(old.toPath(), aside);
            }
        }
        for (File log : source) {
            this.files.move(staging.resolve(log.getName()), new File(this.wal, log.getName()).toPath());
        }
        List<File> installed = logs(this.wal);
        if (installed.size() != source.size()) {
            throw new IOException("Unexpected WAL files during checkpoint restore: " + this.wal);
        }
        for (File log : source) {
            verify(log, new File(this.wal, log.getName()));
        }
        for (File log : source) {
            Files.delete(log.toPath());
        }
    }

    private static void verify(File source, File copy) throws IOException {
        if (!FileUtils.contentEquals(source, copy)) {
            throw new IOException("Checkpoint WAL content mismatch: " + copy);
        }
    }

    private static List<File> logs(File directory) throws IOException {
        File[] children = directory.listFiles();
        if (children == null) {
            throw new IOException("Cannot list WAL directory " + directory);
        }
        List<File> logs = new ArrayList<>();
        for (File child : children) {
            if (child.getName().matches("[0-9]+\\.log")) {
                if (!Files.isRegularFile(child.toPath(), LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("WAL must be a regular file: " + child);
                }
                logs.add(child);
            }
        }
        return logs;
    }

    // Package-private seam for deterministic IO faults, without mocking native DBs.
    static class FileOperations {

        void publishMarker(Path marker, Properties state) throws IOException {
            Path staged = Files.createTempFile(marker.getParent(),
                                               marker.getFileName() + ".staging-", ".tmp");
            try {
                try (OutputStream output = this.markerOutput(staged)) {
                    state.store(output, "Pending RocksDB checkpoint restore; do not remove");
                }
                try (FileChannel channel = FileChannel.open(staged, StandardOpenOption.WRITE)) {
                    channel.force(true);
                }
                // A same-directory hard link publishes atomically and refuses an existing name.
                // If linking is unsupported, fail before installing or touching the live data.
                Files.createLink(marker, staged);
            } catch (IOException | RuntimeException | Error failure) {
                try {
                    Files.deleteIfExists(staged);
                } catch (IOException cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
            Files.delete(staged);
        }

        void forceDirectory(Path directory) throws IOException {
            if (!System.getProperty("os.name").startsWith("Linux")) {
                return;
            }
            try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
                channel.force(true);
            }
        }

        OutputStream markerOutput(Path staged) throws IOException {
            return Files.newOutputStream(staged, StandardOpenOption.WRITE);
        }

        void copyDirectory(File source, File target) throws IOException {
            FileUtils.copyDirectory(source, target);
        }

        void deleteDirectory(File directory) throws IOException {
            FileUtils.deleteDirectory(directory);
        }

        void copyFile(File source, File target) throws IOException {
            FileUtils.copyFile(source, target);
        }

        void move(Path source, Path target) throws IOException {
            Files.move(source, target);
        }
    }
}
