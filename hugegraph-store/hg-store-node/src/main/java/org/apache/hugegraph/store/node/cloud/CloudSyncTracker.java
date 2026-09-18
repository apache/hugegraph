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
import java.util.concurrent.atomic.AtomicLong;

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
 * <p><b>Epoch-based stale-callback protection:</b> each DB has a monotone epoch counter that is
 * incremented by {@link #clearDb}. Callers that want their confirmation to survive a concurrent
 * recreation must first call {@link #currentEpoch} before the upload, then pass the captured
 * epoch to {@link #markConfirmedIfEpoch}. A late callback carrying an old epoch is silently
 * dropped, preventing file-number reuse after DB recreation from producing stale confirmations.
 *
 * <p>{@link Roaring64NavigableMap} is not thread-safe, so all access to a per-DB bitmap is
 * synchronized on the bitmap instance.
 */
public final class CloudSyncTracker {

    /** Holds the bitmap and the epoch it was created under. */
    private static final class DbState {
        final long epoch;
        final Roaring64NavigableMap bitmap;

        DbState(long epoch) {
            this.epoch = epoch;
            this.bitmap = new Roaring64NavigableMap();
        }
    }

    private final Map<String, DbState> stateByDb = new ConcurrentHashMap<>();
    /** Monotone epoch counter per DB. Starts at 1; incremented by clearDb(). */
    private final Map<String, AtomicLong> epochByDb = new ConcurrentHashMap<>();

    // -----------------------------------------------------------------------
    // Epoch API
    // -----------------------------------------------------------------------

    /**
     * Returns the current epoch for the named DB. Call this before starting an upload; pass the
     * returned value to {@link #markConfirmedIfEpoch} on success to guard against stale callbacks
     * after a concurrent {@link #clearDb}.
     */
    public long currentEpoch(String dbName) {
        return epochByDb.computeIfAbsent(dbName, k -> new AtomicLong(1L)).get();
    }

    // -----------------------------------------------------------------------
    // Static utility
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // Confirm / check API
    // -----------------------------------------------------------------------

    /**
     * Marks the SST file as confirmed present in cloud. No-op for non-SST paths.
     *
     * <p>Unlike {@link #markConfirmedIfEpoch}, this method does not check the epoch. Use it only
     * when the caller holds an external guarantee that the DB has not been recreated (e.g. during
     * startup hydration seeding where the DB was just opened and no concurrent clear can occur).
     */
    public void markConfirmed(String dbName, String filePath) {
        long number = parseSstFileNumber(filePath);
        if (number < 0) {
            return;
        }
        long epoch = currentEpoch(dbName);
        markBit(dbName, number, epoch);
    }

    /**
     * Marks the SST file as confirmed only if the DB epoch has not advanced since {@code epoch}
     * was captured (i.e. no intervening {@link #clearDb} occurred). Silently no-ops if the epoch
     * is stale. Use this from async upload callbacks to prevent stale confirmations after DB
     * recreation with reused file numbers.
     *
     * @param dbName   logical DB name
     * @param filePath absolute path of the confirmed SST file
     * @param epoch    epoch captured by {@link #currentEpoch} before the upload started
     * @return {@code true} if the bit was set; {@code false} if the epoch was stale (callback dropped)
     */
    public boolean markConfirmedIfEpoch(String dbName, String filePath, long epoch) {
        long number = parseSstFileNumber(filePath);
        if (number < 0) {
            return false;
        }
        return markBit(dbName, number, epoch);
    }

    /** Core: set the bit for {@code number} if the current epoch matches {@code requiredEpoch}. */
    private boolean markBit(String dbName, long number, long requiredEpoch) {
        // Use compute() to hold the CHM bin lock across both the epoch check and the bitmap write,
        // preventing clearDb() from removing the entry between the two steps.
        boolean[] set = {false};
        stateByDb.compute(dbName, (k, state) -> {
            if (state == null) {
                // DB was never registered or was cleared; create a fresh state under the given epoch.
                long currentEp = epochByDb.computeIfAbsent(dbName, ign -> new AtomicLong(1L)).get();
                if (currentEp != requiredEpoch) {
                    return null;  // epoch mismatch — do not create a new state
                }
                state = new DbState(currentEp);
            } else if (state.epoch != requiredEpoch) {
                return state;  // stale callback — leave state unchanged
            }
            synchronized (state.bitmap) {
                state.bitmap.addLong(number);
            }
            set[0] = true;
            return state;
        });
        return set[0];
    }

    /** Returns whether the SST file is confirmed present in cloud. */
    public boolean isConfirmed(String dbName, String filePath) {
        long number = parseSstFileNumber(filePath);
        if (number < 0) {
            return false;
        }
        boolean[] result = {false};
        stateByDb.computeIfPresent(dbName, (k, state) -> {
            synchronized (state.bitmap) {
                result[0] = state.bitmap.contains(number);
            }
            return state;
        });
        return result[0];
    }

    /** Clears the confirmed bit for a file (called after the cloud object is deleted). */
    public void clearConfirmed(String dbName, String filePath) {
        long number = parseSstFileNumber(filePath);
        if (number < 0) {
            return;
        }
        stateByDb.computeIfPresent(dbName, (k, state) -> {
            synchronized (state.bitmap) {
                state.bitmap.removeLong(number);
            }
            return state;
        });
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
        boolean[] allPresent = {false};
        stateByDb.computeIfPresent(dbName, (k, state) -> {
            synchronized (state.bitmap) {
                for (LiveSstFile live : liveFiles) {
                    long num = parseSstFileNumber(live.getAbsolutePath());
                    if (num >= 0 && !state.bitmap.contains(num)) {
                        return state;  // allPresent stays false — short-circuit
                    }
                }
                allPresent[0] = true;
            }
            return state;
        });
        return allPresent[0];
    }

    /**
     * Removes all confirmed-sync state for the named database and advances the epoch.
     *
     * <p>After this call any in-flight upload callbacks that call {@link #markConfirmedIfEpoch}
     * with the old epoch will be silently dropped, preventing stale confirmations from surviving
     * into the next DB generation — even if RocksDB reuses the same file numbers.
     */
    public void clearDb(String dbName) {
        // Advance the epoch first so any concurrent markConfirmedIfEpoch calls with the old epoch
        // fail the epoch check.  Then remove the state so the next markConfirmed starts fresh.
        epochByDb.computeIfAbsent(dbName, k -> new AtomicLong(1L)).incrementAndGet();
        stateByDb.remove(dbName);
    }

    /** Number of confirmed SST files for a DB (testing / monitoring). */
    public long confirmedCount(String dbName) {
        DbState state = stateByDb.get(dbName);
        if (state == null) {
            return 0L;
        }
        synchronized (state.bitmap) {
            return state.bitmap.getLongCardinality();
        }
    }
}
