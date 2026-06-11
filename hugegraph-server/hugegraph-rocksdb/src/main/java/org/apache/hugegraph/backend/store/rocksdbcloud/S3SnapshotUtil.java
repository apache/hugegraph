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

package org.apache.hugegraph.backend.store.rocksdbcloud;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Utility for uploading/downloading a local directory tree to/from an S3 prefix.
 *
 * <p>Supports two modes:
 * <ul>
 *   <li><b>Full upload</b> — uploads every file in the local directory unconditionally.</li>
 *   <li><b>Incremental upload</b> — only uploads files that are new or have changed
 *       (different size) since the last sync. This is the default for periodic sync,
 *       drastically reducing S3 PUT costs and sync duration for large RocksDB stores.</li>
 * </ul>
 */
public final class S3SnapshotUtil {

    private static final Logger LOG = Log.logger(S3SnapshotUtil.class);

    private S3SnapshotUtil() {
    }

    // -------------------------------------------------------------------------
    // Full upload (existing behaviour — used for close/snapshot)
    // -------------------------------------------------------------------------

    /**
     * Recursively upload {@code localDir} under {@code s3Prefix} in {@code bucket}.
     * Every file is uploaded unconditionally.
     */
    public static void uploadDirectory(S3Client s3, String bucket,
                                       String s3Prefix, String localDir) {
        Path rootPath = Paths.get(localDir);
        try {
            List<Path> files = new ArrayList<>();
            try (var stream = Files.walk(rootPath)) {
                stream.filter(Files::isRegularFile).forEach(files::add);
            }

            for (Path file : files) {
                String relativePath = rootPath.relativize(file).toString();
                String s3Key = s3Prefix + relativePath.replace(File.separatorChar, '/');
                LOG.debug("Uploading '{}' to s3://{}/{}", file, bucket, s3Key);
                s3.putObject(PutObjectRequest.builder()
                                             .bucket(bucket)
                                             .key(s3Key)
                                             .build(),
                             RequestBody.fromFile(file.toFile()));
            }
            LOG.info("Uploaded {} files to s3://{}/{}", files.size(), bucket, s3Prefix);
        } catch (IOException e) {
            throw new BackendException("Failed to upload snapshot directory '%s' to S3: %s",
                                       e, localDir, e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // Incremental upload (only new/changed files — for periodic sync)
    // -------------------------------------------------------------------------

    /**
     * Incrementally sync {@code localDir} to S3, uploading only SST / manifest
     * files that are <em>new or have a different size</em> compared to what is
     * already in S3.  Files that already exist in S3 with the same size are
     * skipped (RocksDB SST files are immutable once written).
     *
     * <p>WAL files (*.log) and LOCK files are always skipped — they are
     * process-local and not needed for crash recovery from S3.
     *
     * @return number of files actually uploaded (0 if nothing changed)
     */
    public static int uploadIncremental(S3Client s3, String bucket,
                                        String s3Prefix, String localDir) {
        Path rootPath = Paths.get(localDir);
        if (!rootPath.toFile().exists()) {
            LOG.debug("Local data dir '{}' does not exist yet; skipping incremental sync",
                      localDir);
            return 0;
        }

        // 1. Build a map of s3Key → size for objects already in S3
        Map<String, Long> s3Inventory = listS3Objects(s3, bucket, s3Prefix);

        // 2. Walk local dir and upload only new/changed files
        int uploaded = 0;
        int skipped = 0;
        try {
            List<Path> localFiles = new ArrayList<>();
            try (var stream = Files.walk(rootPath)) {
                stream.filter(Files::isRegularFile).forEach(localFiles::add);
            }

            for (Path file : localFiles) {
                String name = file.getFileName().toString();

                // Skip WAL logs, LOCK, and temp files — not needed in S3
                if (name.endsWith(".log") || name.equals("LOCK") ||
                    name.startsWith("tmp") || name.endsWith(".tmp")) {
                    continue;
                }

                String relativePath = rootPath.relativize(file).toString();
                String s3Key = s3Prefix + relativePath.replace(File.separatorChar, '/');
                long localSize = Files.size(file);

                Long s3Size = s3Inventory.get(s3Key);
                if (s3Size != null && s3Size == localSize) {
                    // File already exists in S3 with the same size — skip
                    // (RocksDB SST files are immutable; same name+size = same content)
                    skipped++;
                    continue;
                }

                LOG.debug("Incremental upload: '{}' → s3://{}/{} (localSize={}, s3Size={})",
                          file, bucket, s3Key, localSize, s3Size);
                s3.putObject(PutObjectRequest.builder()
                                             .bucket(bucket)
                                             .key(s3Key)
                                             .build(),
                             RequestBody.fromFile(file.toFile()));
                uploaded++;
            }
        } catch (IOException e) {
            throw new BackendException(
                    "Incremental sync failed for local dir '%s': %s", e, localDir, e.getMessage());
        }

        LOG.info("Incremental sync: {} uploaded, {} unchanged (s3://{}/{})",
                 uploaded, skipped, bucket, s3Prefix);
        return uploaded;
    }

    // -------------------------------------------------------------------------
    // S3 inventory helper
    // -------------------------------------------------------------------------

    /**
     * List all objects under {@code prefix} in {@code bucket} and return a map
     * of {@code s3Key → size}. Handles pagination transparently.
     */
    public static Map<String, Long> listS3Objects(S3Client s3, String bucket, String prefix) {
        Map<String, Long> inventory = new HashMap<>();
        String continuationToken = null;
        do {
            ListObjectsV2Request.Builder reqBuilder = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(prefix);
            if (continuationToken != null) {
                reqBuilder.continuationToken(continuationToken);
            }
            ListObjectsV2Response response = s3.listObjectsV2(reqBuilder.build());
            for (S3Object obj : response.contents()) {
                inventory.put(obj.key(), obj.size());
            }
            continuationToken = response.isTruncated() ? response.nextContinuationToken() : null;
        } while (continuationToken != null);
        return inventory;
    }

    // -------------------------------------------------------------------------
    // Full download (unchanged)
    // -------------------------------------------------------------------------

    /**
     * Recursively download all objects under {@code s3Prefix} in {@code bucket}
     * into {@code localDir}.
     */
    public static void downloadDirectory(S3Client s3, String bucket,
                                         String s3Prefix, String localDir) {
        Path rootPath = Paths.get(localDir);
        try {
            String continuationToken = null;
            int count = 0;
            do {
                ListObjectsV2Request.Builder reqBuilder = ListObjectsV2Request.builder()
                        .bucket(bucket)
                        .prefix(s3Prefix);
                if (continuationToken != null) {
                    reqBuilder.continuationToken(continuationToken);
                }
                ListObjectsV2Response response = s3.listObjectsV2(reqBuilder.build());
                for (S3Object obj : response.contents()) {
                    String key = obj.key();
                    String relativePath = key.substring(s3Prefix.length())
                                            .replace('/', File.separatorChar);
                    Path localFile = rootPath.resolve(relativePath);
                    Files.createDirectories(localFile.getParent());
                    LOG.debug("Downloading s3://{}/{} to '{}'", bucket, key, localFile);
                    s3.getObject(GetObjectRequest.builder()
                                                 .bucket(bucket)
                                                 .key(key)
                                                 .build(),
                                ResponseTransformer.toFile(localFile));
                    count++;
                }
                continuationToken = response.isTruncated() ?
                                    response.nextContinuationToken() :
                                    null;
            } while (continuationToken != null);

            LOG.info("Downloaded {} files from s3://{}/{} to '{}'",
                     count, bucket, s3Prefix, localDir);
        } catch (IOException e) {
            throw new BackendException(
                    "Failed to download snapshot directory from S3 prefix '%s': %s",
                    e, s3Prefix, e.getMessage());
        }
    }
}
