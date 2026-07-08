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
 */
@Getter
public final class FailedUploadTask {

    private final String dbName;
    private final String cfName;
    private final String filePath;
    private final String remoteKey;
    private final long failedAt;
    private final int attemptCount;
    private final String lastError;

    public FailedUploadTask(String dbName, String cfName, String filePath,
                            String remoteKey, int attemptCount, String lastError) {
        this(dbName, cfName, filePath, remoteKey, System.currentTimeMillis(),
             attemptCount, lastError);
    }

    public FailedUploadTask(String dbName, String cfName, String filePath,
                            String remoteKey, long failedAt,
                            int attemptCount, String lastError) {
        this.dbName = dbName;
        this.cfName = cfName;
        this.filePath = filePath;
        this.remoteKey = remoteKey;
        this.failedAt = failedAt;
        this.attemptCount = attemptCount;
        this.lastError = lastError != null ? lastError : "";
    }

}
