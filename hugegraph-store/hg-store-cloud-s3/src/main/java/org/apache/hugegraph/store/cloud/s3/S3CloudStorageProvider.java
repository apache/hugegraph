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

package org.apache.hugegraph.store.cloud.s3;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.store.cloud.CloudStorageConfig;
import org.apache.hugegraph.store.cloud.CloudStorageProvider;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Amazon S3 (and S3-compatible) implementation of {@link CloudStorageProvider}.
 *
 * <h3>Activation</h3>
 * Place {@code hg-store-cloud-s3-*.jar} on the classpath and configure:
 * <pre>
 * cloud:
 *   storage:
 *     enabled: true
 *     provider: s3
 *     bucket:  my-bucket
 *     region:  us-east-1
 * </pre>
 *
 * <h3>Credentials</h3>
 * <ul>
 *   <li>If {@code access-key} / {@code secret-key} are set, they are used directly.</li>
 *   <li>Otherwise the standard AWS Default Credentials chain is followed
 *       (env vars, instance profile, ~/.aws/credentials, etc.).</li>
 * </ul>
 *
 * <h3>S3-compatible endpoints (MinIO, Ceph, etc.)</h3>
 * Set {@code cloud.storage.endpoint} to the custom HTTP/HTTPS endpoint URL.
 */
@Slf4j
public class S3CloudStorageProvider implements CloudStorageProvider {

    /** Provider name as referenced in {@link CloudStorageConfig#getProvider()}. */
    public static final String PROVIDER_NAME = "s3";

    private S3Client s3Client;
    private String bucket;
    private String pathPrefix;

    // -----------------------------------------------------------------------
    // CloudStorageProvider
    // -----------------------------------------------------------------------

    @Override
    public String providerName() {
        return PROVIDER_NAME;
    }

    @Override
    public void init(CloudStorageConfig config) {
        this.bucket = config.getBucket();
        this.pathPrefix = config.getPathPrefix();

        S3ClientBuilder builder = S3Client.builder();

        // Credentials
        String ak = config.getAccessKey();
        String sk = config.getSecretKey();
        if (ak != null && !ak.isEmpty() && sk != null && !sk.isEmpty()) {
            builder.credentialsProvider(
                    StaticCredentialsProvider.create(AwsBasicCredentials.create(ak, sk)));
        } else {
            builder.credentialsProvider(DefaultCredentialsProvider.create());
        }

        // Region
        String region = config.getRegion();
        if (region != null && !region.isEmpty()) {
            builder.region(Region.of(region));
        }

        // Custom endpoint (MinIO, Ceph, LocalStack …)
        String endpoint = config.getEndpoint();
        if (endpoint != null && !endpoint.isEmpty()) {
            builder.endpointOverride(URI.create(endpoint));
            // Path-style required for most non-AWS S3 services
            builder.serviceConfiguration(
                    software.amazon.awssdk.services.s3.S3Configuration.builder()
                                                                       .pathStyleAccessEnabled(true)
                                                                       .build());
        }

        this.s3Client = builder.build();
        log.info("S3CloudStorageProvider initialized: bucket='{}', region='{}', endpoint='{}'",
                 bucket, region, endpoint);
    }

    @Override
    public void uploadFile(String localPath, String remoteKey) throws IOException {
        String fullKey = buildKey(remoteKey);
        try {
            s3Client.putObject(
                    PutObjectRequest.builder().bucket(bucket).key(fullKey).build(),
                    Paths.get(localPath));
            log.debug("S3 upload: {} -> s3://{}/{}", localPath, bucket, fullKey);
        } catch (SdkClientException e) {
            throw new IOException(
                    "S3 upload failed for local='" + localPath + "' key='" + fullKey + "'", e);
        }
    }

    @Override
    public void deleteFile(String remoteKey) throws IOException {
        String fullKey = buildKey(remoteKey);
        try {
            s3Client.deleteObject(
                    DeleteObjectRequest.builder().bucket(bucket).key(fullKey).build());
            log.debug("S3 delete: s3://{}/{}", bucket, fullKey);
        } catch (SdkClientException e) {
            throw new IOException("S3 delete failed for key='" + fullKey + "'", e);
        }
    }

    @Override
    public boolean fileExists(String remoteKey) throws IOException {
        String fullKey = buildKey(remoteKey);
        try {
            s3Client.headObject(HeadObjectRequest.builder().bucket(bucket).key(fullKey).build());
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (SdkClientException e) {
            throw new IOException("S3 headObject failed for key='" + fullKey + "'", e);
        }
    }

    @Override
    public List<String> listFiles(String remoteDirPrefix) throws IOException {
        String fullPrefix = buildKey(remoteDirPrefix == null ? "" : remoteDirPrefix);
        List<String> keys = new ArrayList<>();
        try {
            String token = null;
            do {
                ListObjectsV2Request.Builder req =
                        ListObjectsV2Request.builder().bucket(bucket).prefix(fullPrefix);
                if (token != null) {
                    req.continuationToken(token);
                }
                ListObjectsV2Response resp = s3Client.listObjectsV2(req.build());
                for (S3Object obj : resp.contents()) {
                    String key = obj.key();
                    if (key == null || key.endsWith("/")) {
                        continue;
                    }
                    keys.add(stripPathPrefix(key));
                }
                token = resp.nextContinuationToken();
            } while (token != null && !token.isEmpty());
            return keys;
        } catch (SdkClientException e) {
            throw new IOException("S3 listObjects failed for prefix='" + fullPrefix + "'", e);
        }
    }

    @Override
    public void downloadFile(String remoteKey, String localPath) throws IOException {
        String fullKey = buildKey(remoteKey);
        try {
            s3Client.getObject(
                    GetObjectRequest.builder().bucket(bucket).key(fullKey).build(),
                    Paths.get(localPath));
            log.debug("S3 download: s3://{}/{} -> {}", bucket, fullKey, localPath);
        } catch (SdkClientException e) {
            throw new IOException(
                    "S3 download failed for key='" + fullKey + "' local='" + localPath + "'", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (s3Client != null) {
            s3Client.close();
            s3Client = null;
            log.info("S3CloudStorageProvider closed");
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /**
     * Prepends {@link #pathPrefix} to the supplied key, using "/" as separator.
     * If the prefix is null or empty, the key is returned unchanged.
     */
    private String buildKey(String key) {
        if (pathPrefix == null || pathPrefix.isEmpty()) {
            return key;
        }
        // Normalise leading slashes
        String normalKey = key.startsWith("/") ? key.substring(1) : key;
        return pathPrefix.endsWith("/")
               ? pathPrefix + normalKey
               : pathPrefix + "/" + normalKey;
    }

    private String stripPathPrefix(String fullKey) {
        if (fullKey == null) {
            return "";
        }
        if (pathPrefix == null || pathPrefix.isEmpty()) {
            return fullKey.startsWith("/") ? fullKey.substring(1) : fullKey;
        }
        String normalizedPrefix = pathPrefix.endsWith("/") ? pathPrefix : pathPrefix + "/";
        if (fullKey.startsWith(normalizedPrefix)) {
            return fullKey.substring(normalizedPrefix.length());
        }
        return fullKey;
    }
}
