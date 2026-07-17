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

import lombok.Getter;

/**
 * Immutable dead-letter queue entry for a failed SST cloud upload.
 *
 * <p>When the upload is from a hard-linked staging file (queue-overflow case),
 * {@code filePath} is the staging path used for the actual upload, and
 * {@code sourceSstPath} is the original {@code *.sst} path used for confirmation
 * and cleanup. In the normal case both fields are equal.
 *
 * <p>{@code uploadEpoch} is the {@link CloudSyncTracker} epoch that was current when the
 * upload task was originally submitted. It is passed to
 * {@link CloudSyncTracker#markConfirmedIfEpoch} on retry success so that a late callback after
 * DB recreation with reused file numbers is silently discarded.
 */
@Getter
public final class FailedUploadTask {

    private final String dbName;
    private final String cfName;
    /** Path used for the actual file upload (may be a staging hard-link). */
    private final String filePath;
    /**
     * Original {@code *.sst} path used for {@code onUploadConfirmed} callback and
     * staging-file cleanup. Equal to {@code filePath} when no staging file is involved.
     */
    private final String sourceSstPath;
    private final String remoteKey;
    private final long failedAt;
    private final int attemptCount;
    private final String lastError;
    /**
     * CloudSyncTracker epoch at submission time. Used by the retry callback to call
     * {@link CloudSyncTracker#markConfirmedIfEpoch} instead of {@code markConfirmed},
     * so a delayed callback after DB recreation is silently dropped.
     */
    private final long uploadEpoch;

    public FailedUploadTask(String dbName, String cfName, String filePath,
                            String sourceSstPath, String remoteKey, long failedAt,
                            int attemptCount, String lastError, long uploadEpoch) {
        this.dbName = dbName;
        this.cfName = cfName;
        this.filePath = filePath;
        this.sourceSstPath = sourceSstPath != null ? sourceSstPath : filePath;
        this.remoteKey = remoteKey;
        this.failedAt = failedAt;
        this.attemptCount = attemptCount;
        this.lastError = lastError != null ? lastError : "";
        this.uploadEpoch = uploadEpoch;
    }

}
