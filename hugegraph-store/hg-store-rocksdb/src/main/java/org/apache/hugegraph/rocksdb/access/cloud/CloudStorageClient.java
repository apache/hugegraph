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

/**
 * CloudStorageClient defines a common interface for cloud object storage operations.
 * Implementations can target AWS S3, MinIO, Azure Blob Storage, Google Cloud Storage,
 * or any other cloud storage provider.
 * This interface allows different cloud vendors to be plugged in via JARs without
 * modifying the core RocksDB cloud session logic.
 */
public interface CloudStorageClient extends AutoCloseable {

    /**
     * Get the name of the cloud storage provider.
     * E.g., "s3", "azure", "gcs"
     *
     * @return provider name
     */
    String provider();

    /**
     * Upload a directory to cloud storage, replacing all existing content.
     * This performs a full upload of all files in the local directory.
     *
     * @param container the bucket/container name in cloud storage
     * @param path the path/prefix in cloud storage where files will be stored
     * @param localDirectory the local directory path to upload
     * @throws Exception if upload fails
     */
    void uploadDirectory(String container, String path, String localDirectory)
            throws Exception;

    /**
     * Upload a directory incrementally, uploading only changed or new files.
     * This is more efficient than full upload for subsequent syncs.
     *
     * @param container the bucket/container name in cloud storage
     * @param path the path/prefix in cloud storage where files will be stored
     * @param localDirectory the local directory path to upload
     * @throws Exception if upload fails
     */
    void uploadIncremental(String container, String path, String localDirectory)
            throws Exception;

    /**
     * Download a directory from cloud storage to local filesystem.
     *
     * @param container the bucket/container name in cloud storage
     * @param path the path/prefix in cloud storage to download from
     * @param localDirectory the local directory path where files will be downloaded
     * @throws Exception if download fails
     */
    void downloadDirectory(String container, String path, String localDirectory)
            throws Exception;

    /**
     * Close the client and release any resources (connections, clients, etc).
     *
     * @throws Exception if close fails
     */
    @Override
    void close() throws Exception;
}

