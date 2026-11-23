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

package org.apache.hugegraph.store.cloud;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * SPI interface for pluggable cloud storage backends.
 *
 * <p>Implementations are discovered via {@link java.util.ServiceLoader}:
 * each provider JAR must contain
 * {@code META-INF/services/org.apache.hugegraph.store.cloud.CloudStorageProvider}
 * listing the fully-qualified implementation class.
 *
 * <p>The provider whose {@link #providerName()} matches {@link CloudStorageConfig#getProvider()}
 * is selected once at store startup, initialized via {@link #init(CloudStorageConfig)}, and passed
 * to its consumers; it is never reconfigured at runtime.
 */
public interface CloudStorageProvider extends Closeable {

    /**
     * Returns the unique, lower-case name of this provider (e.g. {@code "s3"}, {@code "gcs"}).
     * Must match the value set in {@link CloudStorageConfig#getProvider()}.
     */
    String providerName();

    /**
     * Initializes the provider with the supplied configuration.
     * Called once before any upload/download/delete operations.
     *
     * @param config cloud storage configuration
     */
    void init(CloudStorageConfig config);

    /**
     * Uploads a local file to cloud storage.
     *
     * @param localPath  absolute path of the local source file
     * @param remoteKey  destination key / path inside the bucket (without prefix)
     * @throws IOException on I/O or network failure
     */
    void uploadFile(String localPath, String remoteKey) throws IOException;

    /**
     * Deletes an object from cloud storage.
     *
     * @param remoteKey key / path of the object to delete (without prefix)
     * @throws IOException on I/O or network failure
     */
    void deleteFile(String remoteKey) throws IOException;

    /**
     * Checks whether an object exists in cloud storage.
     *
     * @param remoteKey key / path to check (without prefix)
     * @return {@code true} if the object exists
     * @throws IOException on I/O or network failure
     */
    boolean fileExists(String remoteKey) throws IOException;

    /**
     * Lists object keys that start with a remote key prefix. The prefix is matched as a plain
     * string, as S3 {@code ListObjectsV2} does: callers pass a trailing {@code /} to list one
     * directory, and omit it to list sibling directories that share a name prefix (used to find a
     * DB's version-suffixed directories, e.g. {@code 00001} and {@code 00001_5}).
     *
     * @param remoteDirPrefix key prefix inside bucket (without provider pathPrefix)
     * @return object keys relative to provider root (without provider pathPrefix)
     * @throws IOException on I/O or network failure
     */
    List<String> listFiles(String remoteDirPrefix) throws IOException;

    /**
     * Downloads an object from cloud storage to a local file.
     *
     * @param remoteKey destination key / path inside the bucket (without prefix)
     * @param localPath absolute path of the local destination file
     * @throws IOException on I/O or network failure
     */
    void downloadFile(String remoteKey, String localPath) throws IOException;

    /**
     * Deletes all objects under a prefix. Implementations should use bulk-delete semantics where
     * the underlying storage backend supports them (e.g. S3 DeleteObjects API).
     *
     * @param remoteDirPrefix directory/prefix inside bucket (without provider pathPrefix)
     * @return number of objects deleted
     * @throws IOException on I/O or network failure, including a partial delete
     */
    int deletePrefix(String remoteDirPrefix) throws IOException;

    /**
     * Releases resources held by the provider (e.g. HTTP clients, connections).
     */
    @Override
    void close() throws IOException;
}

