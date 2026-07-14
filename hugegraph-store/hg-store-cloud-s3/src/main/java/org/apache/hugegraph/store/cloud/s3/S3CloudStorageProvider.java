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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hugegraph.store.cloud.CloudStorageConfig;
import org.apache.hugegraph.store.cloud.CloudStorageNonRetryableException;
import org.apache.hugegraph.store.cloud.CloudStorageProvider;

import lombok.extern.slf4j.Slf4j;

import org.jetbrains.annotations.NotNull;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

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
 *     s3:
 *       bucket: my-bucket
 *       region: us-east-1
 * </pre>
 *
 * <h3>Credentials</h3>
 * <ul>
 *   <li>If {@code cloud.storage.s3.access-key} / {@code secret-key} are set,
 *       they are used directly.</li>
 *   <li>Otherwise the standard AWS Default Credentials chain is followed
 *       (env vars, instance profile, ~/.aws/credentials, etc.).</li>
 * </ul>
 *
 * <h3>S3-compatible endpoints (MinIO, Ceph, etc.)</h3>
 * Set {@code cloud.storage.s3.endpoint} to the custom HTTP/HTTPS endpoint URL.
 *
 * <h3>Large-file (multipart) uploads</h3>
 * S3 limits a single PUT to 5 GB. Files larger than
 * {@link #MULTIPART_THRESHOLD_BYTES} ({@value #MULTIPART_THRESHOLD_BYTES} MB)
 * are automatically split into {@link #PART_SIZE_BYTES} ({@value #PART_SIZE_MB} MB)
 * chunks and uploaded using the S3 Multipart Upload API.
 * Each chunk is logged individually so progress is visible for very large files.
 *
 * <h3>Multipart part retry tuning</h3>
 * Tune part-level retry behavior via typed S3 keys:
 * <pre>
 * cloud:
 *   storage:
 *     s3:
 *       multipart-part-retry-max-attempts: 5
 *       multipart-part-retry-base-backoff-ms: 1500
 *       multipart-exhausted-direct-dlq: false
 * </pre>
 * These options apply only to multipart chunks, not to whole-file retry/DLQ policy
 * in {@code CloudUploadRetryQueue}.
 *
 * <h3>Timing metrics</h3>
 * Every upload logs the file size, elapsed time, and throughput at INFO level:
 * <pre>
 *   S3 upload complete: db/000042.sst | size=64.0 MB | elapsed=830 ms | throughput=77.11 MB/s
 * </pre>
 */
@Slf4j
public class S3CloudStorageProvider implements CloudStorageProvider {

    /** Provider name as referenced in {@link CloudStorageConfig#getProvider()}. */
    public static final String PROVIDER_NAME = "s3";

    /**
     * Files larger than this are uploaded via multipart.
     * S3's hard per-PUT limit is 5 GB; we start multipart well below that.
     */
    static final long MULTIPART_THRESHOLD_BYTES = 512L * 1024 * 1024;   // 512 MB

    /**
     * Size of each multipart chunk.
     * S3 minimum part size is 5 MB (except for the last part).
     */
    static final long PART_SIZE_BYTES = 512L * 1024 * 1024;             // 512 MB
    static final int  PART_SIZE_MB    = 512;

    private S3Client s3Client;
    private String bucket;
    private String pathPrefix;
    private int partUploadMaxRetries = S3CloudStorageConfig.DEFAULT_MULTIPART_PART_RETRY_MAX_ATTEMPTS;
    private long partUploadRetryBaseBackoffMs =
            S3CloudStorageConfig.DEFAULT_MULTIPART_PART_RETRY_BASE_BACKOFF_MS;
    private boolean multipartExhaustedDirectDlq = false;

    // -----------------------------------------------------------------------
    // CloudStorageProvider
    // -----------------------------------------------------------------------

    @Override
    public String providerName() {
        return PROVIDER_NAME;
    }

    @Override
    public void init(CloudStorageConfig config) {
        Map<String, String> props = config.getProviderProperties();
        if (props == null || props.isEmpty()) {
            throw new IllegalArgumentException("S3 provider selected but providerProperties are empty");
        }

        this.bucket = props.get(S3CloudStorageConfig.KEY_BUCKET);
        if (this.bucket == null || this.bucket.isBlank()) {
            throw new IllegalArgumentException("S3 bucket is required: cloud.storage.s3.bucket");
        }
        this.pathPrefix = config.getPathPrefix();
        this.initRetryConfig(props);

        S3ClientBuilder builder = S3Client.builder();

        // Credentials
        String ak = props.get(S3CloudStorageConfig.KEY_ACCESS_KEY);
        String sk = props.get(S3CloudStorageConfig.KEY_SECRET_KEY);
        if (ak != null && !ak.isEmpty() && sk != null && !sk.isEmpty()) {
            builder.credentialsProvider(
                    StaticCredentialsProvider.create(AwsBasicCredentials.create(ak, sk)));
        } else {
            builder.credentialsProvider(DefaultCredentialsProvider.builder().build());
        }

        // Region
        String region = props.get(S3CloudStorageConfig.KEY_REGION);
        if (region != null && !region.isEmpty()) {
            builder.region(Region.of(region));
        }

        // Custom endpoint (MinIO, Ceph, LocalStack …)
        String endpoint = props.get(S3CloudStorageConfig.KEY_ENDPOINT);
        if (endpoint != null && !endpoint.isEmpty()) {
            builder.endpointOverride(URI.create(endpoint));
            // Path-style required for most non-AWS S3 services
            builder.serviceConfiguration(
                    software.amazon.awssdk.services.s3.S3Configuration.builder()
                                                                       .pathStyleAccessEnabled(true)
                                                                       .build());
        }

        this.s3Client = builder.build();
        log.info("S3CloudStorageProvider initialized: bucket='{}', region='{}', endpoint='{}', "
                 + "partRetryMaxAttempts={}, partRetryBaseBackoffMs={}, "
                 + "multipartExhaustedDirectDlq={}",
                 bucket, region, endpoint,
                 this.partUploadMaxRetries,
                 this.partUploadRetryBaseBackoffMs,
                 this.multipartExhaustedDirectDlq);
    }

    private void initRetryConfig(Map<String, String> props) {
        int retryMaxAttempts = parseIntOrDefault(
                props.get(S3CloudStorageConfig.KEY_MULTIPART_RETRY_MAX_ATTEMPTS)
        );
        if (retryMaxAttempts <= 0) {
            log.warn("Invalid cloud.storage.s3.multipart-part-retry-max-attempts={} "
                     + "(must be > 0), using default {}",
                     retryMaxAttempts,
                     S3CloudStorageConfig.DEFAULT_MULTIPART_PART_RETRY_MAX_ATTEMPTS);
            this.partUploadMaxRetries = S3CloudStorageConfig.DEFAULT_MULTIPART_PART_RETRY_MAX_ATTEMPTS;
        } else {
            this.partUploadMaxRetries = retryMaxAttempts;
        }

        long retryBaseBackoffMs = parseLongOrDefault(
                props.get(S3CloudStorageConfig.KEY_MULTIPART_RETRY_BASE_BACKOFF_MS)
        );
        if (retryBaseBackoffMs <= 0L) {
            log.warn("Invalid cloud.storage.s3.multipart-part-retry-base-backoff-ms={} "
                     + "(must be > 0), using default {}",
                     retryBaseBackoffMs,
                     S3CloudStorageConfig.DEFAULT_MULTIPART_PART_RETRY_BASE_BACKOFF_MS);
            this.partUploadRetryBaseBackoffMs =
                    S3CloudStorageConfig.DEFAULT_MULTIPART_PART_RETRY_BASE_BACKOFF_MS;
        } else {
            this.partUploadRetryBaseBackoffMs = retryBaseBackoffMs;
        }

        this.multipartExhaustedDirectDlq = parseBooleanOrDefault(
                props.get(S3CloudStorageConfig.KEY_MULTIPART_EXHAUSTED_DIRECT_DLQ));
    }

    private static int parseIntOrDefault(String value) {
        if (value == null || value.isBlank()) {
            return S3CloudStorageConfig.DEFAULT_MULTIPART_PART_RETRY_MAX_ATTEMPTS;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            log.warn("Invalid {}={}, using default {}",
                     "cloud.storage.s3.multipart-part-retry-max-attempts", value,
                     S3CloudStorageConfig.DEFAULT_MULTIPART_PART_RETRY_MAX_ATTEMPTS);
            return S3CloudStorageConfig.DEFAULT_MULTIPART_PART_RETRY_MAX_ATTEMPTS;
        }
    }

    private static long parseLongOrDefault(String value) {
        if (value == null || value.isBlank()) {
            return S3CloudStorageConfig.DEFAULT_MULTIPART_PART_RETRY_BASE_BACKOFF_MS;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            log.warn("Invalid {}={}, using default {}",
                     "cloud.storage.s3.multipart-part-retry-base-backoff-ms", value,
                     S3CloudStorageConfig.DEFAULT_MULTIPART_PART_RETRY_BASE_BACKOFF_MS);
            return S3CloudStorageConfig.DEFAULT_MULTIPART_PART_RETRY_BASE_BACKOFF_MS;
        }
    }

    private static boolean parseBooleanOrDefault(String value) {
        if (value == null || value.isBlank()) {
            return false;
        }
        return Boolean.parseBoolean(value.trim());
    }

    /**
     * Uploads a local file to S3.
     *
     * <p>Files &le; {@link #MULTIPART_THRESHOLD_BYTES} use a single PUT request.
     * Larger files are split into {@link #PART_SIZE_BYTES} chunks and uploaded via
     * the S3 Multipart Upload API, which is required for files larger than 5 GB.
     *
     * <p>Timing and throughput are always logged at INFO level after the upload
     * completes (or each part for multipart uploads).
     */
    @Override
    public void uploadFile(String localPath, String remoteKey) throws IOException {
        java.nio.file.Path path = Paths.get(localPath);
        long fileSize;
        try {
            fileSize = Files.size(path);
        } catch (IOException e) {
            throw new IOException("Cannot stat local file: " + localPath, e);
        }

        String fullKey = buildKey(remoteKey);
        long startNs = System.nanoTime();

        if (fileSize > MULTIPART_THRESHOLD_BYTES) {
            uploadMultipart(path, fileSize, fullKey);
        } else {
            uploadSinglePart(path, fullKey, localPath);
        }

        long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;
        double throughputMBps = elapsedMs > 0
                                ? (fileSize / 1_048_576.0) / (elapsedMs / 1000.0)
                                : 0.0;
        log.info("S3 upload complete: {} | size={} | elapsed={} ms | throughput={} MB/s",
                 remoteKey,
                 humanSize(fileSize),
                 elapsedMs,
                 String.format(Locale.US, "%.2f", throughputMBps));
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
        long startNs = System.nanoTime();
        Path destinationPath = Paths.get(localPath);
        try {

            s3Client.getObject(
                    GetObjectRequest.builder().bucket(bucket).key(fullKey).build(),
                    destinationPath);
        } catch (SdkClientException e) {
            throw new IOException(
                    "S3 download failed for key='" + fullKey + "' local='" + localPath + "'", e);
        }
        long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;
        long fileSize = 0;
        try {
            fileSize = Files.size(destinationPath);
        } catch (IOException ignored) {
            // best-effort; don't fail download reporting
        }
        double throughputMBps = elapsedMs > 0
                                ? (fileSize / 1_048_576.0) / (elapsedMs / 1000.0)
                                : 0.0;
        log.info("S3 download complete: {} | size={} | elapsed={} ms | throughput={} MB/s",
                 remoteKey,
                 humanSize(fileSize),
                 elapsedMs,
                 String.format(Locale.US, "%.2f", throughputMBps));
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
    // Internal – upload strategies
    // -----------------------------------------------------------------------

    /**
     * Single-PUT upload for files ≤ {@link #MULTIPART_THRESHOLD_BYTES}, with bounded
     * exponential-backoff retry on transient failures (using the same tuning as multipart parts).
     * Without this, a single transient network blip on the common small-SST path would surface
     * immediately and, with whole-file retries disabled, go straight to the DLQ.
     */
    private void uploadSinglePart(java.nio.file.Path path, String fullKey,
                                  String localPath) throws IOException {
        SdkClientException last = null;
        for (int attempt = 1; attempt <= this.partUploadMaxRetries; attempt++) {
            try {
                s3Client.putObject(
                        PutObjectRequest.builder().bucket(bucket).key(fullKey).build(),
                        path);
                return;
            } catch (SdkClientException e) {
                last = e;
                if (attempt >= this.partUploadMaxRetries) {
                    break;
                }
                long backoffMs = this.partUploadRetryBaseBackoffMs * (1L << (attempt - 1));
                log.warn("S3 single-PUT retry: attempt={}/{} key={} reason={} nextBackoffMs={}",
                         attempt, this.partUploadMaxRetries, fullKey, e.getMessage(), backoffMs);
                sleepQuietly(backoffMs);
            }
        }
        throw new IOException(
                "S3 upload failed for local='" + localPath + "' key='" + fullKey + "' after "
                + this.partUploadMaxRetries + " attempt(s)", last);
    }

    /**
     * Multipart upload for files > {@link #MULTIPART_THRESHOLD_BYTES}.
     *
     * <p>Each part is logged individually so that progress of multi-hour uploads
     * is visible in the server log:
     * <pre>
     *   S3 multipart part 1/41 uploaded: size=512.0 MB | elapsed=6 230 ms | throughput=82.18 MB/s
     *   S3 multipart part 2/41 uploaded: size=512.0 MB | elapsed=6 050 ms | throughput=84.63 MB/s
     *   ...
     *   S3 multipart upload completed: key=hugegraph/hgstore-data/000099.sst | parts=41
     * </pre>
     *
     * <p>If any part fails the multipart upload is aborted (to avoid incomplete-upload storage
     * charges) and an {@link IOException} is thrown.
     */
    private void uploadMultipart(java.nio.file.Path path, long fileSize,
                                 String fullKey) throws IOException {
        int totalParts = (int) Math.ceil((double) fileSize / PART_SIZE_BYTES);
        log.info("S3 multipart upload started: key={} | size={} | parts={} | partSize={} MB",
                 fullKey, humanSize(fileSize), totalParts, PART_SIZE_MB);

        // Step 1 – initiate
        CreateMultipartUploadResponse initResp;
        try {
            initResp = s3Client.createMultipartUpload(
                    CreateMultipartUploadRequest.builder().bucket(bucket).key(fullKey).build());
        } catch (SdkClientException e) {
            throw new IOException("S3 createMultipartUpload failed for key='" + fullKey + "'", e);
        }
        String uploadId = initResp.uploadId();

        List<CompletedPart> completedParts = new ArrayList<>(totalParts);
        try {
            // Step 2 – upload each part
            for (int partNum = 1; partNum <= totalParts; partNum++) {
                long offset = (long) (partNum - 1) * PART_SIZE_BYTES;
                long partLen = Math.min(PART_SIZE_BYTES, fileSize - offset);

                long partStartNs = System.nanoTime();
                String eTag = uploadOnePartWithRetry(path, fullKey, uploadId,
                                                     partNum, totalParts,
                                                     offset, partLen);
                long partElapsedMs = (System.nanoTime() - partStartNs) / 1_000_000;
                double partThroughput = partElapsedMs > 0
                                        ? (partLen / 1_048_576.0) / (partElapsedMs / 1000.0)
                                        : 0.0;
                log.info("S3 multipart part {}/{} uploaded: size={} | elapsed={} ms | "
                         + "throughput={} MB/s",
                         partNum, totalParts,
                         humanSize(partLen),
                         partElapsedMs,
                         String.format(Locale.US, "%.2f", partThroughput));

                completedParts.add(CompletedPart.builder()
                                                .partNumber(partNum)
                                                .eTag(eTag)
                                                .build());
            }

            // Step 3 – complete
            s3Client.completeMultipartUpload(
                    CompleteMultipartUploadRequest.builder()
                                                 .bucket(bucket).key(fullKey)
                                                 .uploadId(uploadId)
                                                 .multipartUpload(
                                                         CompletedMultipartUpload.builder()
                                                                                 .parts(completedParts)
                                                                                 .build())
                                                 .build());
            log.info("S3 multipart upload completed: key={} | parts={}", fullKey, totalParts);

        } catch (Exception e) {
            // Abort to avoid partial-upload storage charges
            try {
                s3Client.abortMultipartUpload(
                        AbortMultipartUploadRequest.builder()
                                                  .bucket(bucket).key(fullKey)
                                                  .uploadId(uploadId).build());
                log.warn("S3 multipart upload aborted: key={} uploadId={}", fullKey, uploadId);
            } catch (Exception abortEx) {
                log.warn("S3 multipart abort failed: key={} uploadId={} reason={}",
                         fullKey, uploadId, abortEx.getMessage());
            }
            throw new IOException(
                    "S3 multipart upload failed for key='" + fullKey + "'", e);
        }
    }

    /**
     * Uploads a single part of a multipart upload and returns its ETag.
     * Opens a fresh {@link FileInputStream} per part to avoid channel-position
     * races in concurrent scenarios, then skips to {@code offset}.
     */
    private String uploadOnePart(java.nio.file.Path path, String fullKey,
                                 String uploadId, int partNumber,
                                 long offset, long partLen) throws IOException {
        try (FileInputStream fis = new FileInputStream(path.toFile())) {
            // Skip to the start of this part
            long remaining = offset;
            while (remaining > 0) {
                long skipped = fis.skip(remaining);
                if (skipped <= 0) {
                    throw new IOException(
                            "Unexpected EOF while seeking to offset " + offset + " in " + path);
                }
                remaining -= skipped;
            }

            InputStream partStream = new LimitedInputStream(fis, partLen);
            UploadPartResponse resp = s3Client.uploadPart(
                    UploadPartRequest.builder()
                                    .bucket(bucket).key(fullKey)
                                    .uploadId(uploadId).partNumber(partNumber)
                                    .contentLength(partLen)
                                    .build(),
                    RequestBody.fromInputStream(partStream, partLen));
            return resp.eTag();
        } catch (SdkClientException e) {
            throw new IOException(
                    "S3 uploadPart failed: key=" + fullKey + " part=" + partNumber, e);
        }
    }

    /**
     * Uploads one multipart chunk with local retries. This avoids restarting the whole
     * SST upload when only one or two parts fail due to transient network/S3 errors.
     */
    private String uploadOnePartWithRetry(java.nio.file.Path path,
                                          String fullKey,
                                          String uploadId,
                                          int partNumber,
                                          int totalParts,
                                          long offset,
                                          long partLen) throws IOException {
        IOException last = null;
        for (int attempt = 1; attempt <= this.partUploadMaxRetries; attempt++) {
            try {
                return uploadOnePart(path, fullKey, uploadId, partNumber, offset, partLen);
            } catch (IOException e) {
                last = e;
                if (attempt >= this.partUploadMaxRetries) {
                    break;
                }
                long backoffMs = this.partUploadRetryBaseBackoffMs * (1L << (attempt - 1));
                log.warn("S3 multipart part retry: part={}/{} attempt={}/{} key={} "
                         + "reason={} nextBackoffMs={}",
                         partNumber, totalParts,
                         attempt, this.partUploadMaxRetries,
                         fullKey,
                         e.getMessage(),
                         backoffMs);
                sleepQuietly(backoffMs);
            }
        }
        String message = String.format(
                "S3 multipart part failed after %d attempt(s): key=%s part=%d/%d",
                this.partUploadMaxRetries, fullKey, partNumber, totalParts);
        if (this.multipartExhaustedDirectDlq) {
            throw new CloudStorageNonRetryableException(message, last);
        }
        throw new IOException(message, last);
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    // -----------------------------------------------------------------------
    // Internal – key helpers
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

    // -----------------------------------------------------------------------
    // Internal – formatting
    // -----------------------------------------------------------------------

    static String humanSize(long bytes) {
        if (bytes < 1024L)                   return bytes + " B";
        if (bytes < 1024L * 1024)            return String.format(Locale.US, "%.1f KB", bytes / 1024.0);
        if (bytes < 1024L * 1024 * 1024)     return String.format(Locale.US, "%.1f MB",
                                                                   bytes / (1024.0 * 1024));
        return String.format(Locale.US, "%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }

    // -----------------------------------------------------------------------
    // Inner types
    // -----------------------------------------------------------------------

    /**
     * An {@link InputStream} wrapper that limits reading to exactly {@code limit} bytes.
     * Used to feed each multipart chunk to the S3 SDK without loading it into memory.
     */
    private static final class LimitedInputStream extends InputStream {

        private final InputStream wrapped;
        private long remaining;

        LimitedInputStream(InputStream wrapped, long limit) {
            this.wrapped   = wrapped;
            this.remaining = limit;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int b = wrapped.read();
            if (b >= 0) {
                remaining--;
            }
            return b;
        }

        @Override
        public int read(@NotNull byte[] buf, int off, int len) throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int toRead = (int) Math.min(len, remaining);
            int n = wrapped.read(buf, off, toRead);
            if (n > 0) {
                remaining -= n;
            }
            return n;
        }

        @Override
        public void close() throws IOException {
            wrapped.close();
        }
    }
}
