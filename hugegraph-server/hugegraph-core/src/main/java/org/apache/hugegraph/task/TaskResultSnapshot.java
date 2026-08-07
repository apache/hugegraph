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

package org.apache.hugegraph.task;

import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.LZ4Util;

public final class TaskResultSnapshot {

    private final Id taskId;
    private final TaskStatus status;
    private final byte[] compressedResult;
    private volatile String fingerprint;

    public TaskResultSnapshot(Id taskId, TaskStatus status,
                              byte[] compressedResult) {
        E.checkNotNull(taskId, "task id");
        E.checkNotNull(status, "task status");
        this.taskId = taskId;
        this.status = status;
        this.compressedResult = compressedResult;
        this.fingerprint = null;
    }

    public Id taskId() {
        return this.taskId;
    }

    public TaskStatus status() {
        return this.status;
    }

    public boolean hasResult() {
        return this.compressedResult != null;
    }

    public int compressedSize() {
        return this.hasResult() ? this.compressedResult.length : 0;
    }

    public InputStream openResultStream() {
        E.checkState(this.hasResult(),
                     "Task '%s' has no persisted result", this.taskId);
        return LZ4Util.decompressStream(this.compressedResult);
    }

    public String fingerprint() {
        E.checkState(this.hasResult(),
                     "Task '%s' has no persisted result", this.taskId);
        String value = this.fingerprint;
        if (value != null) {
            return value;
        }
        synchronized (this) {
            if (this.fingerprint == null) {
                this.fingerprint = fingerprint(this.compressedResult);
            }
            return this.fingerprint;
        }
    }

    private static String fingerprint(byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(bytes);
            return Base64.getUrlEncoder().withoutPadding()
                         .encodeToString(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new HugeException("Failed to load SHA-256", e);
        }
    }
}
