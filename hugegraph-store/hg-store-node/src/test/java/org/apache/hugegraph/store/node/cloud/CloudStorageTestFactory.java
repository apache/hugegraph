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

import java.util.List;
import java.util.function.BiConsumer;

/**
 * Shared construction helpers for {@link CloudStorageEventListener} and
 * {@link CloudUploadRetryQueue} tests. Both classes expose exactly one production constructor
 * (fully-parameterised, so each is completely configured the moment it is built); this factory
 * mirrors the parameter combinations tests actually vary so call sites don't have to spell out
 * every default themselves.
 */
final class CloudStorageTestFactory {

    private CloudStorageTestFactory() {
    }

    static CloudStorageEventListener newListener(List<String> dataRoots) {
        return newListener(dataRoots, true, CloudStorageEventListener.DEFAULT_READ_MISS_GUARD_WINDOW_MS,
                            null);
    }

    static CloudStorageEventListener newListener(List<String> dataRoots,
                                                  boolean startupHydrationEnabled) {
        return newListener(dataRoots, startupHydrationEnabled,
                            CloudStorageEventListener.DEFAULT_READ_MISS_GUARD_WINDOW_MS, null);
    }

    static CloudStorageEventListener newListener(List<String> dataRoots,
                                                  boolean startupHydrationEnabled,
                                                  long readMissGuardWindowMs) {
        return newListener(dataRoots, startupHydrationEnabled, readMissGuardWindowMs, null);
    }

    static CloudStorageEventListener newListener(List<String> dataRoots,
                                                  boolean startupHydrationEnabled,
                                                  long readMissGuardWindowMs,
                                                  CloudUploadRetryQueue retryQueue) {
        return newListener(dataRoots, startupHydrationEnabled, readMissGuardWindowMs, retryQueue,
                            new CloudSyncTracker(), null);
    }

    static CloudStorageEventListener newListener(List<String> dataRoots,
                                                  boolean startupHydrationEnabled,
                                                  long readMissGuardWindowMs,
                                                  CloudUploadRetryQueue retryQueue,
                                                  CloudSyncTracker syncTracker) {
        return newListener(dataRoots, startupHydrationEnabled, readMissGuardWindowMs, retryQueue,
                            syncTracker, null);
    }

    static CloudStorageEventListener newListener(List<String> dataRoots,
                                                  boolean startupHydrationEnabled,
                                                  long readMissGuardWindowMs,
                                                  CloudUploadRetryQueue retryQueue,
                                                  CloudSyncTracker syncTracker,
                                                  String storeScopePrefix) {
        return newListener(dataRoots, startupHydrationEnabled, readMissGuardWindowMs, retryQueue,
                            syncTracker, storeScopePrefix,
                            CloudStorageEventListener.DEFAULT_METADATA_SYNC_DEBOUNCE_MS,
                            CloudStorageEventListener.DEFAULT_METADATA_SYNC_MAX_UNPUBLISHED);
    }

    static CloudStorageEventListener newListener(List<String> dataRoots,
                                                  boolean startupHydrationEnabled,
                                                  long readMissGuardWindowMs,
                                                  CloudUploadRetryQueue retryQueue,
                                                  CloudSyncTracker syncTracker,
                                                  String storeScopePrefix,
                                                  long metadataSyncDebounceMs,
                                                  int metadataSyncMaxUnpublished) {
        return new CloudStorageEventListener(dataRoots, startupHydrationEnabled,
                                              readMissGuardWindowMs, retryQueue, syncTracker,
                                              storeScopePrefix, metadataSyncDebounceMs,
                                              metadataSyncMaxUnpublished);
    }

    static CloudUploadRetryQueue newRetryQueue(int maxAttempts, long initialDelayMs,
                                               long maxDelayMs, String dataRoot) {
        return newRetryQueue(maxAttempts, initialDelayMs, maxDelayMs, dataRoot,
                              (CloudUploadRetryQueue.UploadConfirmedCallback) null);
    }

    /**
     * Legacy convenience overload accepting a simple {@link BiConsumer}. The epoch parameter is
     * not forwarded; use the {@link CloudUploadRetryQueue.UploadConfirmedCallback} overload for
     * epoch-safe confirmation.
     */
    static CloudUploadRetryQueue newRetryQueue(int maxAttempts, long initialDelayMs,
                                               long maxDelayMs, String dataRoot,
                                               BiConsumer<String, String> onUploadConfirmed) {
        return newRetryQueue(maxAttempts, initialDelayMs, maxDelayMs, dataRoot,
                              onUploadConfirmed == null ? null
                                      : (db, path, epoch) -> onUploadConfirmed.accept(db, path));
    }

    static CloudUploadRetryQueue newRetryQueue(int maxAttempts, long initialDelayMs,
                                               long maxDelayMs, String dataRoot,
                                               CloudUploadRetryQueue.UploadConfirmedCallback
                                                       onUploadConfirmed) {
        return newRetryQueue(maxAttempts, initialDelayMs, maxDelayMs, dataRoot, onUploadConfirmed,
                              CloudUploadRetryQueue.DEFAULT_MAX_DLQ_SIZE);
    }

    static CloudUploadRetryQueue newRetryQueue(int maxAttempts, long initialDelayMs,
                                               long maxDelayMs, String dataRoot,
                                               CloudUploadRetryQueue.UploadConfirmedCallback
                                                       onUploadConfirmed,
                                               int maxDlqSize) {
        return new CloudUploadRetryQueue(maxAttempts, initialDelayMs, maxDelayMs, dataRoot,
                                          onUploadConfirmed, maxDlqSize);
    }
}
