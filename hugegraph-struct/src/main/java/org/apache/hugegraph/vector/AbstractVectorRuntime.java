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

package org.apache.hugegraph.vector;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hugegraph.util.JsonUtilCommon;

import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

public abstract class AbstractVectorRuntime<Id> implements VectorIndexRuntime<Id> {

    public String basePath = "";
    protected static final String CURRENT_VERSION_LINK_NAME = "current";
    protected static final String TEMP_LINK_NAME = "current_temp";
    private static final String VERSION_PREFIX = "version_";
    protected static final String INDEX_FILE_NAME = "index.inline";
    protected static final String META_FILE_NAME = "vector_meta.json";

    protected final ConcurrentMap<Id, IndexContext<Id>> vectorMap = new ConcurrentHashMap<>();

    public AbstractVectorRuntime(String basePath) {
        this.basePath = basePath;
    }

    public static class IndexContext<Id> {

        final Id indexLabelId;

        // for jvector
        public final UpdatableRandomAccessVectorValues vectors; // per-index RAVV
        public final GraphIndexBuilder builder;        // owns the mutable OnHeapGraphIndex
        VectorSimilarityFunction similarityFunction;
        int dimension;

        // for recover and update
        IndexContextMetaData metaData;

        public IndexContext(Id indexLabelId,
                            UpdatableRandomAccessVectorValues vectors,
                            GraphIndexBuilder builder,
                            long watermark,
                            int dimension,
                            VectorSimilarityFunction similarityFunction) {
            this.indexLabelId = indexLabelId;
            this.vectors = vectors;
            this.builder = builder;
            this.similarityFunction = similarityFunction;
            this.dimension = dimension;
            this.metaData = new IndexContextMetaData(0, watermark, false);
        }

        GraphIndex graphView() {
            return builder.getGraph();
        }

        public IndexContextMetaData metaData() { return metaData; }

        void setMetaData(IndexContextMetaData metaData) { this.metaData = metaData; }

        public static class IndexContextMetaData {

            private int nextVectorId;
            private final long watermark;
            private long currentMaxSequence;
            private boolean isUpdateFromLog;

            public IndexContextMetaData(int nextVectorId, long watermark, boolean isUpdateFromLog) {
                this.nextVectorId = nextVectorId;
                this.watermark = watermark;
                this.isUpdateFromLog = isUpdateFromLog;
            }

            int getNextVectorId() {
                this.nextVectorId++;
                return nextVectorId;
            }

            void setNextVectorId(int nextVectorId) {
                this.nextVectorId = nextVectorId;
            }

            void setCurrentMaxSequence(long currentMaxSequence) {
                this.currentMaxSequence = currentMaxSequence;
            }

            long getCurrentMaxSequence() {
                this.currentMaxSequence++;
                return this.currentMaxSequence;
            }

            public long getWatermark() { return watermark; }

            public boolean isUpdateFromLog() {
                return isUpdateFromLog;
            }

            public void setUpdateFromLog(boolean isUpdateFromLog) {
                this.isUpdateFromLog = isUpdateFromLog;
            }
        }
    }

    @Override
    public void init() {
    //   read the properties

    }

    @Override
    public void stop() throws IOException {
        // flush all the context to disk
        for (Map.Entry<Id, IndexContext<Id>> e : vectorMap.entrySet()) {
            Id indexLabelId = e.getKey();
            flush(indexLabelId);
        }
    }

    @Override
    public void flush(Id indexlabelId) throws IOException {
        IndexContext<Id> context = obtainContext(indexlabelId);
        String pathString = basePath + "/" + (indexlabelId) + "/";
        Path indexBaseDir = Paths.get(pathString);

        // create temp path
        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        Path newVersionDir = indexBaseDir.resolve(VERSION_PREFIX + "_" + timestamp);
        Files.createDirectories(newVersionDir);

        try {
            Path indexPath = newVersionDir.resolve(INDEX_FILE_NAME);
            // Create the file first (required by OnDiskGraphIndex.write)
            Files.createFile(indexPath);
            context.builder.cleanup();
            OnDiskGraphIndex.write(context.builder.getGraph(), context.vectors, indexPath);

            Path metaPath = newVersionDir.resolve(META_FILE_NAME);
            Files.write(metaPath, JsonUtilCommon.toJson(context.metaData()).getBytes());

            // Sync to filesystem
            forceSyncDirectory(newVersionDir);

            // Create a temp symlink pointing to the new version
            Path tempSymlink = indexBaseDir.resolve(TEMP_LINK_NAME);
            Files.deleteIfExists(tempSymlink);
            Files.createSymbolicLink(tempSymlink, newVersionDir);

            // Atomically switch the 'current' symlink
            Path currentLink = indexBaseDir.resolve(CURRENT_VERSION_LINK_NAME);
            if (Files.isSymbolicLink(currentLink)) {
                Files.move(tempSymlink, currentLink,
                           StandardCopyOption.ATOMIC_MOVE,
                           StandardCopyOption.REPLACE_EXISTING);
            } else {
                // First save: no existing symlink, simply rename
                Files.move(tempSymlink, currentLink, StandardCopyOption.ATOMIC_MOVE);
            }
        } catch (IOException e) {
            System.err.println("Atomic save failed: " + e.getMessage());
            throw e;
        }

        vectorMap.clear();
    }

    private void forceSyncDirectory(Path directory) throws IOException {
        // need test to prove if it is work

        // First, fsync all files within the directory
        if (!Files.isDirectory(directory)) {
            throw new IllegalArgumentException("Path is not a directory: " + directory);
        }

        try (Stream<Path> stream = Files.list(directory)) {
            for (Path file : stream.collect(Collectors.toList())) {
                if (!Files.isRegularFile(file)) { continue; }
                try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
                    channel.force(true);
                }
            }
        }
        // Then, fsync the directory itself to persist its metadata (the file entries)
        // Use FileChannel.open instead of RandomAccessFile to avoid "(Is a directory)" on Linux
        try (FileChannel ch = FileChannel.open(directory, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException e) {
            // Best-effort: some systems don't support fsync on directories
        }
    }

    @Override
    public long getCurrentWaterMark(Id indexlabelId) {
        if (!this.vectorMap.containsKey(indexlabelId)) {
            return -1;
        }
        return vectorMap.get(indexlabelId).metaData().getWatermark();
    }

    @Override
    public int getNextVectorId(Id indexlabelId) {
        if (!this.vectorMap.containsKey(indexlabelId)) {
            return -1;
        }
        return vectorMap.get(indexlabelId).metaData().getNextVectorId();
    }

    @Override
    public long getNextSequence(Id indexLabelId) {
        if (!this.vectorMap.containsKey(indexLabelId)) {
            return -1;
        }
        return vectorMap.get(indexLabelId).metaData().getCurrentMaxSequence();
    }

    @Override
    public void updateMetaData(Id indexLabelId, int vectorId, long sequence) {
        //if (!this.vectorMap.containsKey(indexLabelId)) {
        //    // warning to log?
        //    return;
        //}
        IndexContext<Id> context = obtainContext(indexLabelId);
        context.metaData.setNextVectorId(vectorId);
        context.metaData.setCurrentMaxSequence(sequence);
        context.metaData.setUpdateFromLog(true);
    }

    public IndexContext<Id> obtainContext(Id indexlabelId) {
        // TODO:add the function that update the ord and sequence in the context
        IndexContext<Id> context = getContext(indexlabelId);
        if (context != null) {
            return context;
        }
        // If the IndexLabelId invalid when create New Context will throw error
        return createNewContext(indexlabelId);
    }

    protected abstract IndexContext<Id> createNewContext(Id indexlabelId);

    protected IndexContext<Id> getContext(Id indexlabelId) {
        if (this.vectorMap.containsKey(indexlabelId)) {
            return vectorMap.get(indexlabelId);
        }
        return null;
    }

    boolean checkPathValid(Id indexlabelId) {
        String pathString = basePath + "/" + (indexlabelId) + "/";
        Path indexBaseDir = Paths.get(pathString);
        if (!Files.isDirectory(indexBaseDir)) {
            System.err.println(
                    "Validation failed: Base directory does not exist or is not a directory: " +
                    indexBaseDir);
            return false;
        }
        // 2. check the current link path
        Path currentLinkPath = indexBaseDir.resolve(CURRENT_VERSION_LINK_NAME);
        if (!Files.isSymbolicLink(currentLinkPath)) {
            System.err.println(
                    "Validation failed: 'current' is not a symbolic link or does not exist in: " +
                    indexBaseDir);
            return false;
        }

        try {
            // 3. read the real Path dir
            Path realVersionDir = Files.readSymbolicLink(currentLinkPath);
            // transform to the absolute path
            if (!realVersionDir.isAbsolute()) {
                realVersionDir = indexBaseDir.resolve(realVersionDir).toAbsolutePath();
            }
            // 4. check the dir existed
            if (!Files.isDirectory(realVersionDir)) {
                System.err.println(
                        "Validation failed: 'current' points to a non-existent directory: " +
                        realVersionDir);
                return false;
            }
            // 5. check 2 files of index existed
            Path indexFilePath = realVersionDir.resolve(INDEX_FILE_NAME);
            Path metaFilePath = realVersionDir.resolve(META_FILE_NAME);

            boolean indexFileExists = Files.isRegularFile(indexFilePath);
            boolean metaFileExists = Files.isRegularFile(metaFilePath);
            if (!indexFileExists) {
                System.err.println(
                        "Validation failed: Index file not found in version directory: " +
                        indexFilePath);
            }
            if (!metaFileExists) {
                System.err.println(
                        "Validation failed: Metadata file not found in version directory: " +
                        metaFilePath);
            }

            return indexFileExists && metaFileExists;

        } catch (IOException e) {
            System.err.println(
                    "An I/O error occurred during validation for index " + indexlabelId + ": " +
                    e.getMessage());
            return false;
        }
    }

    Path getOnDiskIndexDirPath(Id indexlabelId) throws IOException {
        String pathString = basePath + "/" + (indexlabelId) + "/";
        Path indexBaseDir = Paths.get(pathString);
        Path currentLinkPath = indexBaseDir.resolve(CURRENT_VERSION_LINK_NAME);
        return Files.readSymbolicLink(currentLinkPath);
    }

    protected abstract String idToString(Id id);
}
