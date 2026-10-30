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

package org.apache.hugegraph.store.raft;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.storage.log.RocksDBSegmentLogStorage;
import com.alipay.sofa.jraft.storage.log.SegmentFile;
import com.alipay.sofa.jraft.util.Utils;

import lombok.extern.slf4j.Slf4j;

/**
 * Normalize incompatible blank allocations before JRaft 1.3.x loads its segment pool.
 */
@Slf4j
public final class RaftSegmentLogStorage extends RocksDBSegmentLogStorage {

    private final Path segmentsPath;
    private final int segmentSize;
    private boolean normalized;
    private boolean normalizationFailed;
    private boolean normalizationFailureClosed;

    public RaftSegmentLogStorage(Builder builder) {
        super(builder.getPath(), builder.getRaftOptions(), builder.getValueSizeThreshold(),
              builder.getMaxSegmentFileSize(), builder.getPreAllocateSegmentCount(),
              builder.getKeepInMemorySegmentCount(), builder.getCheckpointIntervalMs(),
              builder.getWriteExecutor());
        this.segmentsPath = Path.of(builder.getPath(), "segments");
        this.segmentSize = builder.getMaxSegmentFileSize();
    }

    @Override
    public synchronized boolean init(LogStorageOptions options) {
        this.normalizationFailed = false;
        this.normalizationFailureClosed = false;
        boolean initialized = super.init(options);
        if (!initialized && this.normalizationFailed) {
            // The hook ran after RocksDB.open() acquired its directory lock. Release it on
            // normalization failure too; JRaft's segment allocator has not been started yet.
            this.shutdown();
        }
        return initialized;
    }

    @Override
    public synchronized void shutdown() {
        // NodeImpl.afterShutdown() can repeat cleanup after init() has already closed a
        // failed normalization attempt. Keep that path idempotent without changing normal
        // successful-storage shutdown behavior.
        if (!this.normalizationFailureClosed) {
            super.shutdown();
            this.normalizationFailureClosed = this.normalizationFailed;
        }
    }

    @Override
    protected boolean onInitLoaded() {
        if (!this.normalized) {
            try {
                this.normalizeBlankSegments();
                this.normalized = true;
            } catch (IOException e) {
                this.normalizationFailed = true;
                log.error("Cannot normalize blank Raft segments in {} for size {}; files retained",
                          this.segmentsPath, this.segmentSize, e);
                return false;
            }
        }
        this.normalizationFailed = false;
        return super.onInitLoaded();
    }

    @Override
    protected void onShutdown() {
        if (!this.normalizationFailed) {
            super.onShutdown();
        }
    }

    private void normalizeBlankSegments() throws IOException {
        if (!Files.exists(this.segmentsPath, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        if (!Files.isDirectory(this.segmentsPath, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Not a segment directory: " + this.segmentsPath);
        }
        // RocksDB already holds the directory lock. At startup no segment has been mapped
        // and no allocator is running. Do not normalize again from reset(): the immutable
        // size has not changed and its existing allocator can still be creating files.
        try (DirectoryStream<Path> files = Files.newDirectoryStream(this.segmentsPath, "*.s")) {
            for (Path file : files) {
                if (!file.getFileName().toString().matches("[0-9]+\\.s")) {
                    continue;
                }
                if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("Not a regular segment file: " + file);
                }
                long size = Files.size(file);
                if (size == this.segmentSize || !this.isBlank(file)) {
                    continue;
                }
                Path isolated = file.resolveSibling(file.getFileName() + ".blank-" + size + "-" +
                                                    UUID.randomUUID());
                Files.move(file, isolated, StandardCopyOption.ATOMIC_MOVE);
                log.warn("Isolated blank Raft segment {} ({} bytes) as {}; new allocations use {} bytes",
                         file, size, isolated, this.segmentSize);
            }
        }
        // Persist renames before JRaft can reuse an old blank's numeric filename. Also sync
        // on a retry where a previous attempt renamed every blank but failed before this step.
        Utils.fsync(this.segmentsPath.toFile());
    }

    private boolean isBlank(Path file) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(SegmentFile.HEADER_SIZE);
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ,
                                                    LinkOption.NOFOLLOW_LINKS)) {
            while (header.hasRemaining()) {
                if (channel.read(header) < 0) {
                    throw new IOException("Incomplete segment header: " + file);
                }
            }
        }
        header.flip();
        // JRaft 1.3.x SegmentHeader: two magic bytes, firstLogIndex, reserved zero.
        // Never infer emptiness from a corrupt header, apparent size or sparse blocks.
        if (header.get() != 0x20 || header.get() != 0x20) {
            throw new IOException("Unrecognized segment header: " + file);
        }
        long firstLogIndex = header.getLong();
        if (firstLogIndex != -99L) {
            return false;
        }
        if (header.getLong() != 0L) {
            throw new IOException("Unrecognized blank segment header: " + file);
        }
        return true;
    }
}
