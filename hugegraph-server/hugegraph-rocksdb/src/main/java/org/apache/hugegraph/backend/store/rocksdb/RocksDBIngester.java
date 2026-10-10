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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.util.Log;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;

public class RocksDBIngester {

    public static final String SST = ".sst";

    private static final Logger LOG = Log.logger(RocksDBIngester.class);

    private final RocksDB rocksdb;
    private final IngestExternalFileOptions options;

    public RocksDBIngester(RocksDB rocksdb) {
        this.rocksdb = rocksdb;

        this.options = new IngestExternalFileOptions();
        this.options.setMoveFiles(true);
    }

    public List<String> ingest(Path path, ColumnFamilyHandle cf) throws RocksDBException {
        SuffixFileVisitor visitor = new SuffixFileVisitor(SST);
        try {
            Files.walkFileTree(path, visitor);
        } catch (IOException e) {
            throw new BackendException("Failed to walk path '%s'", e, path);
        }

        List<Path> files = visitor.files();
        List<String> ssts = new ArrayList<>(files.size());
        for (Path file : files) {
            File sst = file.toFile();
            if (sst.exists() && sst.length() > 0L) {
                ssts.add(sst.getPath());
            }
        }
        this.ingest(cf, ssts);

        return ssts;
    }

    public void ingest(ColumnFamilyHandle cf, List<String> ssts) throws RocksDBException {
        LOG.info("Ingest sst files to CF '{}': {}", RocksDBStdSessions.decode(cf.getName()), ssts);
        if (!ssts.isEmpty()) {
            this.rocksdb.ingestExternalFile(cf, ssts, this.options);
        }
    }

    public static class SuffixFileVisitor extends SimpleFileVisitor<Path> {

        private final List<Path> files = new ArrayList<>();
        private final String suffix;

        public SuffixFileVisitor(String suffix) {
            this.suffix = suffix;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
            if (file.toString().endsWith(this.suffix)) {
                this.files.add(file);
            }
            return FileVisitResult.CONTINUE;
        }

        public List<Path> files() {
            return this.files;
        }
    }
}
