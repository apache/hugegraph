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
package org.apache.hugegraph.store.node.metrics;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

class ProcfsReader {

    /* default */ static final long CACHE_DURATION_MS = 100;
    private static final Map<String, ProcfsReader> instances = new HashMap<>();
    private static final Object instancesLock = new Object();
    private static final Map<Path, List<String>> data = new HashMap<>();
    private static final Object dataLock = new Object();
    private static final Path BASE = Paths.get("/proc", "self");
    private final Path entryPath;
    private final boolean osSupport;
    /* default */ long lastReadTime = -1;

    private ProcfsReader(String entry) {
        this(BASE, entry, false);
    }

    /* default */ ProcfsReader(Path base, String entry) {
        this(base, entry, true);
    }

    private ProcfsReader(Path base, String entry, boolean forceOSSupport) {
        Objects.requireNonNull(base);
        Objects.requireNonNull(entry);

        this.entryPath = base.resolve(entry);

        this.osSupport = forceOSSupport
                         || System.getProperty("os.name").toLowerCase(Locale.ENGLISH)
                                  .startsWith("linux");
    }

    /* default */
    static ProcfsReader getInstance(String entry) {
        Objects.requireNonNull(entry);

        synchronized (instancesLock) {
            ProcfsReader reader = instances.get(entry);
            if (reader == null) {
                reader = new ProcfsReader(entry);
                instances.put(entry, reader);
            }
            return reader;
        }
    }

    /* default */ Path getEntryPath() {
        return entryPath;
    }

    /* default */ ReadResult read() throws IOException {
        return read(currentTime());
    }

    /* default */ ReadResult read(long currentTimeMillis) throws IOException {
        synchronized (dataLock) {
            final Path key = getEntryPath().getFileName();

            final ReadResult readResult;
            if (lastReadTime == -1 || lastReadTime + CACHE_DURATION_MS < currentTimeMillis) {
                final List<String> lines = readPath(entryPath);
                cacheResult(key, lines);
                lastReadTime = currentTime();
                readResult = new ReadResult(lines, lastReadTime);
            } else {
                readResult = new ReadResult(data.get(key), lastReadTime);
            }
            return readResult;
        }
    }

    /* default */ List<String> readPath(Path path) throws IOException {
        Objects.requireNonNull(path);

        if (!osSupport) {
            return Collections.emptyList();
        }
        return Files.readAllLines(path);
    }

    /* default */ void cacheResult(Path key, List<String> lines) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(lines);

        data.put(key, lines);
    }

    /* default */ long currentTime() {
        return System.currentTimeMillis();
    }

    /* default */ static class ReadResult {

        private final List<String> lines;

        private final long readTime;

        /* default */ ReadResult(List<String> lines, long readTime) {
            this.lines = Objects.requireNonNull(lines);
            this.readTime = readTime;
        }

        public long getReadTime() {
            return readTime;
        }

        public List<String> getLines() {
            return lines;
        }

    }

}
