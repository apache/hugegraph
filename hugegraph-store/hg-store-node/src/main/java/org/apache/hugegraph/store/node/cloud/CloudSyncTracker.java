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

package org.apache.hugegraph.store.node.cloud;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.rocksdb.access.RocksDBFactory.LiveSstFile;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

/**
 * Tracks which RocksDB SST files are confirmed present in cloud storage.
 *
 * <p>SST file names are monotonic per-DB file numbers ({@code 000123.sst}), so the natural key is
 * the integer file number rather than the string path. Sync state is kept as one
 * {@link Roaring64NavigableMap} per DB — each set bit means "SST file number N for this DB is
 * confirmed present in cloud". Roaring bitmaps stay compact even as low numbers are cleared after
 * old files are deleted, and file numbers are unique across column families within a DB, so a
 * single bitmap per DB is sufficient (which fits RocksDB delete events not carrying a CF name)
 *
 * <p>This tracker is the linchpin of the delete-guard invariant: a superseded cloud object is
 * deleted only once every live SST file of that DB is confirmed here.
 *
 * <p>{@link Roaring64NavigableMap} is not thread-safe, so all access to a per-DB bitmap is
 * synchronized on the bitmap instance.
 */
public final class CloudSyncTracker {

    private final Map<String, Roaring64NavigableMap> confirmedByDb = new ConcurrentHashMap<>();

    /**
     * Parses the SST file number from a file path such as {@code /data/db/000123.sst}.
     *
     * @return the file number, or {@code -1} if the path is not a parseable {@code *.sst} file
     */
    public static long parseSstFileNumber(String filePath) {
        if (filePath == null || !filePath.endsWith(".sst")) {
            return -1L;
        }
        int slash = Math.max(filePath.lastIndexOf('/'), filePath.lastIndexOf(File.separatorChar));
        String name = filePath.substring(slash + 1, filePath.length() - ".sst".length());
        if (name.isEmpty()) {
            return -1L;
        }
        // RocksDB SST base names are pure digits (e.g. 000123). Reject anything else.
        try {
            return Long.parseLong(name);
        } catch (NumberFormatException e) {
            return -1L;
        }
    }

    private Roaring64NavigableMap bitmap(String dbName) {
        return confirmedByDb.computeIfAbsent(dbName, k -> new Roaring64NavigableMap());
    }

    /** Marks the SST file as confirmed present in cloud. No-op for non-SST paths. */
    public void markConfirmed(String dbName, String filePath) {
        long number = parseSstFileNumber(filePath);
        if (number < 0) {
            return;
        }
        Roaring64NavigableMap bm = bitmap(dbName);
        synchronized (bm) {
            bm.addLong(number);
        }
    }

    /** Returns whether the SST file is confirmed present in cloud. */
    public boolean isConfirmed(String dbName, String filePath) {
        long number = parseSstFileNumber(filePath);
        if (number < 0) {
            return false;
        }
        Roaring64NavigableMap bm = confirmedByDb.get(dbName);
        if (bm == null) {
            return false;
        }
        synchronized (bm) {
            return bm.contains(number);
        }
    }

    /** Clears the confirmed bit for a file (called after the cloud object is deleted). */
    public void clearConfirmed(String dbName, String filePath) {
        long number = parseSstFileNumber(filePath);
        if (number < 0) {
            return;
        }
        Roaring64NavigableMap bm = confirmedByDb.get(dbName);
        if (bm == null) {
            return;
        }
        synchronized (bm) {
            bm.removeLong(number);
        }
    }

    /**
     * Returns {@code true} if <em>every</em> file in {@code liveFiles} is confirmed present
     * in cloud.
     *
     * <p>This is the fast-path check used by the delete guard: it acquires the per-DB lock
     * <em>once</em> for the entire set and short-circuits on the first miss, so a DB whose
     * live set is fully confirmed costs one lock acquisition regardless of set size — compared
     * to N acquisitions with N individual {@link #isConfirmed} calls.
     *
     * @param liveFiles the current live SST file set obtained from RocksDB's manifest
     * @return {@code true} iff every SST path in {@code liveFiles} has its confirmed bit set
     */
    public boolean allConfirmed(String dbName, List<LiveSstFile> liveFiles) {
        if (liveFiles.isEmpty()) {
            return true;
        }
        Roaring64NavigableMap bm = confirmedByDb.get(dbName);
        if (bm == null) {
            return false;
        }
        synchronized (bm) {
            for (LiveSstFile live : liveFiles) {
                long num = parseSstFileNumber(live.getAbsolutePath());
                if (num >= 0 && !bm.contains(num)) {
                    return false;  // short-circuit on first unconfirmed file
                }
            }
        }
        return true;
    }

    /** Number of confirmed SST files for a DB (testing / monitoring). */
    public long confirmedCount(String dbName) {
        Roaring64NavigableMap bm = confirmedByDb.get(dbName);
        if (bm == null) {
            return 0L;
        }
        synchronized (bm) {
            return bm.getLongCardinality();
        }
    }
}
