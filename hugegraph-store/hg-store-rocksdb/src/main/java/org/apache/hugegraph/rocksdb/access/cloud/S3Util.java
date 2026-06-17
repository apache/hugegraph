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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.rocksdb.access.DBStoreException;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

@Slf4j
public final class S3Util {

    private S3Util() {
    }

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
                s3.putObject(PutObjectRequest.builder()
                                             .bucket(bucket)
                                             .key(s3Key)
                                             .build(),
                             RequestBody.fromFile(file.toFile()));
            }
            log.info("Uploaded {} files to s3://{}/{}", files.size(), bucket, s3Prefix);
        } catch (IOException e) {
            throw new DBStoreException("Failed to upload '%s' to S3: %s",
                                       localDir, e.getMessage());
        }
    }

    public static void uploadIncremental(S3Client s3, String bucket,
                                         String s3Prefix, String localDir) {
        Path rootPath = Paths.get(localDir);
        if (!rootPath.toFile().exists()) {
            return;
        }

        Map<String, Long> s3Inventory = listS3Objects(s3, bucket, s3Prefix);

        int uploaded = 0;
        int skipped = 0;
        try {
            List<Path> localFiles = new ArrayList<>();
            try (var stream = Files.walk(rootPath)) {
                stream.filter(Files::isRegularFile).forEach(localFiles::add);
            }

            for (Path file : localFiles) {
                String name = file.getFileName().toString();
                if (name.endsWith(".log") || name.equals("LOCK") ||
                    name.startsWith("tmp") || name.endsWith(".tmp")) {
                    continue;
                }

                String relativePath = rootPath.relativize(file).toString();
                String s3Key = s3Prefix + relativePath.replace(File.separatorChar, '/');
                long localSize = Files.size(file);

                Long s3Size = s3Inventory.get(s3Key);
                if (s3Size != null && s3Size == localSize) {
                    skipped++;
                    continue;
                }

                s3.putObject(PutObjectRequest.builder()
                                             .bucket(bucket)
                                             .key(s3Key)
                                             .build(),
                             RequestBody.fromFile(file.toFile()));
                uploaded++;
            }
        } catch (IOException e) {
            throw new DBStoreException("Incremental sync failed for '%s': %s",
                                       localDir, e.getMessage());
        }

        log.info("Incremental sync: {} uploaded, {} unchanged (s3://{}/{})",
                 uploaded, skipped, bucket, s3Prefix);
    }

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
            continuationToken = response.isTruncated() ?
                                response.nextContinuationToken() :
                                null;
        } while (continuationToken != null);
        return inventory;
    }

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

            log.info("Downloaded {} files from s3://{}/{} to '{}'",
                     count, bucket, s3Prefix, localDir);
        } catch (IOException e) {
            throw new DBStoreException("Failed to download S3 prefix '%s': %s",
                                       s3Prefix, e.getMessage());
        }
    }
}

