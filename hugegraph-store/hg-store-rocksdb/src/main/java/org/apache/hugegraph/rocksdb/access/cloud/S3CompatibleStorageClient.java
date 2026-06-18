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

package org.apache.hugegraph.rocksdb.access.cloud;

import software.amazon.awssdk.services.s3.S3Client;

/**
 * S3CompatibleStorageClient implements CloudStorageClient for S3-compatible storage.
 * Wraps AWS SDK S3Client and delegates operations to S3Util.
 * Supports AWS S3, MinIO, and other S3-compatible storage services.
 */
public class S3CompatibleStorageClient implements CloudStorageClient {

    private final S3Client s3Client;

    public S3CompatibleStorageClient(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    @Override
    public String provider() {
        return "s3";
    }

    @Override
    public void uploadDirectory(String container, String path, String localDirectory) {
        S3Util.uploadDirectory(this.s3Client, container, path, localDirectory);
    }

    @Override
    public void uploadIncremental(String container, String path, String localDirectory) {
        S3Util.uploadIncremental(this.s3Client, container, path, localDirectory);
    }

    @Override
    public void downloadDirectory(String container, String path, String localDirectory) {
        S3Util.downloadDirectory(this.s3Client, container, path, localDirectory);
    }

    @Override
    public void close() throws Exception {
        this.s3Client.close();
    }
}

