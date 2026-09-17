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

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

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
 * Files larger than {@link #MULTIPART_THRESHOLD_BYTES} ({@value #MULTIPART_THRESHOLD_BYTES} MB)
 * are uploaded via {@link S3TransferManager}, which splits them into parts and uploads them
 * concurrently using the S3 Multipart Upload API — required for files larger than 5 GB, S3's
 * hard per-PUT limit. Smaller files use a direct single PUT. Retry (including per-part retry
 * for multipart uploads) is handled entirely by the AWS SDK's own client-level retry strategy
 * (see {@link #buildRetryOverride()}) rather than by application code.
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
     * Files larger than this are uploaded via {@link S3TransferManager} (multipart).
     * S3's hard per-PUT limit is 5 GB; we start multipart well below that.
     */
    static final long MULTIPART_THRESHOLD_BYTES = 512L * 1024 * 1024;   // 512 MB

    private S3Client s3Client;
    private S3AsyncClient s3AsyncClient;
    private S3TransferManager transferManager;
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
        Map<String, String> props = config.getProviderProperties();
        if (props == null || props.isEmpty()) {
            throw new IllegalArgumentException("S3 provider selected but providerProperties are empty");
        }

        this.bucket = props.get(S3CloudStorageConfig.KEY_BUCKET);
        if (this.bucket == null || this.bucket.isBlank()) {
            throw new IllegalArgumentException("S3 bucket is required: cloud.storage.s3.bucket");
        }
        this.pathPrefix = config.getPathPrefix();

        String ak = props.get(S3CloudStorageConfig.KEY_ACCESS_KEY);
        String sk = props.get(S3CloudStorageConfig.KEY_SECRET_KEY);
        String region = props.get(S3CloudStorageConfig.KEY_REGION);
        String endpoint = props.get(S3CloudStorageConfig.KEY_ENDPOINT);

        S3ClientBuilder builder = S3Client.builder();
        builder.overrideConfiguration(buildRetryOverride());
        applyCredentials(builder::credentialsProvider, ak, sk);
        if (region != null && !region.isEmpty()) {
            builder.region(Region.of(region));
        }
        if (endpoint != null && !endpoint.isEmpty()) {
            builder.endpointOverride(URI.create(endpoint));
            // Path-style required for most non-AWS S3 services
            builder.serviceConfiguration(
                    software.amazon.awssdk.services.s3.S3Configuration.builder()
                                                                       .pathStyleAccessEnabled(true)
                                                                       .build());
        }

        S3CrtAsyncClientBuilder asyncBuilder = S3AsyncClient.crtBuilder();
        applyCredentials(asyncBuilder::credentialsProvider, ak, sk);
        if (region != null && !region.isEmpty()) {
            asyncBuilder.region(Region.of(region));
        }
        if (endpoint != null && !endpoint.isEmpty()) {
            asyncBuilder.endpointOverride(URI.create(endpoint));
            asyncBuilder.forcePathStyle(true);
        }

        // Close any clients from a previous init() so a re-initialization (e.g. Spring context
        // restart, which re-runs the same singleton provider instance) does not leak the old
        // clients' connection pools and SDK threads.
        closeQuietly();

        this.s3Client = builder.build();
        this.s3AsyncClient = asyncBuilder.build();
        this.transferManager = S3TransferManager.builder().s3Client(this.s3AsyncClient).build();
        log.info("S3CloudStorageProvider initialized: bucket='{}', region='{}', endpoint='{}'",
                 bucket, region, endpoint);
    }

    @Override
    public void uploadFile(String localPath, String remoteKey) throws IOException {
        Path path = Paths.get(localPath);
        long fileSize;
        try {
            fileSize = Files.size(path);
        } catch (IOException e) {
            throw new IOException("Cannot stat local file: " + localPath, e);
        }

        String fullKey = buildKey(remoteKey);
        long startNs = System.nanoTime();

        if (fileSize > MULTIPART_THRESHOLD_BYTES) {
            uploadViaTransferManager(path, fullKey);
        } else {
            uploadSinglePart(path, fullKey);
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
        } catch (SdkException e) {
            throw classifySdkException("deleteObject", fullKey, e);
        }
    }

    /**
     * Deletes all objects under a prefix using S3's DeleteObjects (batch delete) API.
     *
     * <p>Much more efficient than individual deletes, especially for prefixes with many objects.
     * Handles pagination internally if the prefix contains more than 1000 objects.
     *
     * @param remoteDirPrefix directory/prefix inside bucket (without provider pathPrefix)
     * @return number of objects deleted
     * @throws IOException on I/O or network failure
     */
    @Override
    public int deletePrefix(String remoteDirPrefix) throws IOException {
        String fullPrefix = buildKey(remoteDirPrefix == null ? "" : remoteDirPrefix);
        int totalDeleted = 0;

        try {
            String token = null;
            do {
                ListObjectsV2Request.Builder listReq =
                        ListObjectsV2Request.builder().bucket(bucket).prefix(fullPrefix);
                if (token != null) {
                    listReq.continuationToken(token);
                }
                ListObjectsV2Response listResp = s3Client.listObjectsV2(listReq.build());

                List<ObjectIdentifier> toDelete = new ArrayList<>();
                for (S3Object obj : listResp.contents()) {
                    String key = obj.key();
                    if (key != null && !key.endsWith("/")) {
                        toDelete.add(ObjectIdentifier.builder().key(key).build());
                    }
                }

                if (!toDelete.isEmpty()) {
                    try {
                        DeleteObjectsResponse deleteResp = s3Client.deleteObjects(
                                DeleteObjectsRequest.builder()
                                                   .bucket(bucket)
                                                   .delete(software.amazon.awssdk.services.s3.model.Delete.builder()
                                                                                                          .objects(toDelete)
                                                                                                          .build())
                                                   .build());
                        totalDeleted += deleteResp.deleted().size();
                        log.debug("S3 batch delete: deleted {} objects from prefix {}",
                                  deleteResp.deleted().size(), remoteDirPrefix);

                        // S3 returns HTTP 200 even when individual keys fail; always inspect.
                        if (!deleteResp.errors().isEmpty()) {
                            log.warn("S3 batch delete partial failure: {}/{} key(s) failed in "
                                     + "prefix '{}' — retrying individually",
                                     deleteResp.errors().size(), toDelete.size(), remoteDirPrefix);
                            List<String> stillFailed = new ArrayList<>();
                            for (software.amazon.awssdk.services.s3.model.S3Error err
                                    : deleteResp.errors()) {
                                log.warn("  S3 DeleteObjects error: key={} code={} message={}",
                                         err.key(), err.code(), err.message());
                                try {
                                    s3Client.deleteObject(DeleteObjectRequest.builder()
                                                                             .bucket(bucket)
                                                                             .key(err.key())
                                                                             .build());
                                    totalDeleted++;
                                    log.debug("S3 individual retry delete succeeded: key={}",
                                              err.key());
                                } catch (SdkException ex) {
                                    stillFailed.add(err.key());
                                    log.warn("S3 individual retry delete failed: key={}: {}",
                                             err.key(), ex.getMessage());
                                }
                            }
                            if (!stillFailed.isEmpty()) {
                                throw new IOException(
                                        "S3 DeleteObjects: " + stillFailed.size()
                                        + " key(s) could not be deleted from prefix '"
                                        + remoteDirPrefix + "': " + stillFailed);
                            }
                        }
                    } catch (SdkException e) {
                        log.warn("S3 batch delete failed for prefix='{}': {}",
                                fullPrefix, e.getMessage());
                        // Fall back to individual deletes for any remaining objects.
                        // Failures are collected and re-thrown so callers (e.g. purgeRemotePrefix)
                        // can correctly preserve the tombstone guard when the purge is incomplete.
                        List<String> stillFailed = new ArrayList<>();
                        for (ObjectIdentifier obj : toDelete) {
                            try {
                                s3Client.deleteObject(DeleteObjectRequest.builder()
                                                                        .bucket(bucket)
                                                                        .key(obj.key())
                                                                        .build());
                                totalDeleted++;
                            } catch (SdkException ex) {
                                stillFailed.add(obj.key());
                                log.debug("S3 fallback delete failed for key='{}': {}",
                                         obj.key(), ex.getMessage());
                            }
                        }
                        if (!stillFailed.isEmpty()) {
                            throw new IOException(
                                    "S3 fallback delete: " + stillFailed.size()
                                    + " key(s) could not be deleted from prefix '"
                                    + remoteDirPrefix + "': " + stillFailed);
                        }
                    }
                }

                token = listResp.nextContinuationToken();
                // Some S3-compatible gateways return isTruncated=true without a usable
                // continuation token. Unlike the best-effort multipart sweep (which can stop
                // early), a prefix purge that silently stops here would report success while
                // objects remain — and the caller (onDBDeleted / truncate purge) would then remove
                // its tombstone guard, leaving stale objects that can be re-hydrated as live data.
                // Fail loudly so the purge is marked incomplete and the guard is preserved.
                if (Boolean.TRUE.equals(listResp.isTruncated())
                        && (token == null || token.isEmpty())) {
                    throw new IOException(
                            "S3 deletePrefix: listing for prefix '" + remoteDirPrefix
                            + "' is truncated but returned no continuation token; the purge is "
                            + "incomplete and cannot be confirmed. Deleted " + totalDeleted
                            + " object(s) so far.");
                }
            } while (token != null && !token.isEmpty());

            if (totalDeleted > 0) {
                log.info("S3 prefix delete completed: prefix={}, deleted={}", remoteDirPrefix, totalDeleted);
            }
            return totalDeleted;

        } catch (SdkException e) {
            throw classifySdkException("deletePrefix", fullPrefix, e);
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
        } catch (AwsServiceException e) {
            // A missing bucket is a misconfiguration, not a missing key. Treat it as a hard error
            // so callers (e.g. tombstone check in preHydrateDbFiles) surface the problem rather
            // than silently skipping hydration and starting with an empty database.
            String errorCode = e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : "";
            if ("NoSuchBucket".equals(errorCode) || "InvalidBucketName".equals(errorCode)) {
                throw new IOException(
                        "S3 bucket not found or misconfigured (key=" + fullKey + "): " + errorCode,
                        e);
            }
            // Some S3-compatible providers return generic service exceptions for 404.
            if (e.statusCode() == 404) {
                return false;
            }
            throw classifySdkException("headObject", fullKey, e);
        } catch (SdkException e) {
            throw classifySdkException("headObject", fullKey, e);
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
                // A truncated listing with no continuation token would silently return a PARTIAL
                // key set. Startup hydration relies on a complete listing (e.g. to find CURRENT);
                // a partial set could let a DB open on incomplete local state. Fail loudly so the
                // caller blocks rather than proceeding on a partial listing.
                if (Boolean.TRUE.equals(resp.isTruncated())
                        && (token == null || token.isEmpty())) {
                    throw new IOException(
                            "S3 listFiles: listing for prefix '" + fullPrefix + "' is truncated "
                            + "but returned no continuation token; refusing to return a partial "
                            + "listing (" + keys.size() + " key(s) seen so far).");
                }
            } while (token != null && !token.isEmpty());
            return keys;
        } catch (SdkException e) {
            throw classifySdkException("listObjectsV2", fullPrefix, e);
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
        } catch (SdkException e) {
            throw classifySdkException("getObject", fullKey, e);
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
        closeQuietly();
        log.info("S3CloudStorageProvider closed");
    }

    // -----------------------------------------------------------------------
    // Internal – client construction
    // -----------------------------------------------------------------------

    /**
     * Retry policy applied to both the sync and async S3 clients. {@code STANDARD} mode retries
     * transient failures (including individual multipart part uploads) with exponential backoff
     * and jitter — the AWS SDK owns retry entirely; no application-level retry loop is layered
     * on top.
     */
    private static ClientOverrideConfiguration buildRetryOverride() {
        return ClientOverrideConfiguration.builder()
                                          .retryStrategy(RetryMode.STANDARD)
                                          .build();
    }

    private static void applyCredentials(
            java.util.function.Consumer<software.amazon.awssdk.auth.credentials.AwsCredentialsProvider> sink,
            String accessKey, String secretKey) {
        if (accessKey != null && !accessKey.isEmpty() && secretKey != null && !secretKey.isEmpty()) {
            sink.accept(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey)));
        } else {
            sink.accept(DefaultCredentialsProvider.builder().build());
        }
    }

    private void closeQuietly() {
        if (this.transferManager != null) {
            try {
                this.transferManager.close();
            } catch (Exception e) {
                log.warn("Failed to close previous S3 transfer manager: {}", e.getMessage());
            }
            this.transferManager = null;
        }
        if (this.s3AsyncClient != null) {
            try {
                this.s3AsyncClient.close();
            } catch (Exception e) {
                log.warn("Failed to close previous S3 async client: {}", e.getMessage());
            }
            this.s3AsyncClient = null;
        }
        if (this.s3Client != null) {
            try {
                this.s3Client.close();
            } catch (Exception e) {
                log.warn("Failed to close previous S3 client: {}", e.getMessage());
            }
            this.s3Client = null;
        }
    }

    // -----------------------------------------------------------------------
    // Internal – upload strategies
    // -----------------------------------------------------------------------

    /**
     * Single-PUT upload for files ≤ {@link #MULTIPART_THRESHOLD_BYTES}. Transient failures are
     * retried by the SDK's own retry strategy (see {@link #buildRetryOverride()}); this method
     * makes one call and lets exceptions propagate.
     */
    private void uploadSinglePart(Path path, String fullKey) throws IOException {
        try {
            s3Client.putObject(
                    PutObjectRequest.builder().bucket(bucket).key(fullKey).build(),
                    path);
        } catch (SdkException e) {
            throw classifySdkException("putObject", fullKey, e);
        }
    }

    /**
     * Multipart upload for files > {@link #MULTIPART_THRESHOLD_BYTES}, delegated entirely to
     * {@link S3TransferManager}. The Transfer Manager handles splitting into parts, uploading
     * them concurrently, completing (or aborting, on failure) the multipart upload, and retrying
     * transient per-part failures via the underlying async client's SDK-level retry strategy.
     */
    private void uploadViaTransferManager(Path path, String fullKey) throws IOException {
        UploadFileRequest request = UploadFileRequest.builder()
                .putObjectRequest(b -> b.bucket(bucket).key(fullKey))
                .source(path)
                .build();
        FileUpload fileUpload = transferManager.uploadFile(request);
        try {
            CompletedFileUpload completed = fileUpload.completionFuture().join();
            log.info("S3 multipart upload completed: key={} eTag={}",
                     fullKey, completed.response().eTag());
        } catch (java.util.concurrent.CompletionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof SdkException) {
                throw classifySdkException("uploadFile(multipart)", fullKey, (SdkException) cause);
            }
            throw new IOException("S3 multipart upload failed for key='" + fullKey + "'", cause);
        }
    }

    /**
     * Classifies SDK exceptions into retryable/non-retryable provider exceptions.
     *
     * <p>Retryable:
     * <ul>
     *   <li>client-side transport failures ({@link SdkException});</li>
     *   <li>service throttling / transient statuses (408/425/429/500/502/503/504);</li>
     *   <li>AWS error codes that the SDK classifies as retryable regardless of HTTP status
     *       (e.g. {@code RequestTimeout} at HTTP 400, {@code PriorRequestNotComplete}).</li>
     * </ul>
     * Non-retryable:
     * <ul>
     *   <li>permanent service-side statuses (e.g. auth/permission/not-found/validation).</li>
     * </ul>
     */
    private IOException classifySdkException(String operation, String key, SdkException e) {
        if (e instanceof AwsServiceException) {
            AwsServiceException ase = (AwsServiceException) e;
            int status = ase.statusCode();
            AwsErrorDetails errorDetails = ase.awsErrorDetails();
            String code = errorDetails != null ? errorDetails.errorCode() : "";
            String requestId = ase.requestId();
            String message = String.format(Locale.US,
                                           "S3 %s failed: key=%s status=%d code=%s requestId=%s",
                                           operation, key, status, code, requestId);
            if (isRetryableServiceFailure(ase)) {
                return new IOException(message, ase);
            }
            return new CloudStorageNonRetryableException(message, ase);
        }

        // Client-side network/IO/timeout failures are retryable by default.
        return new IOException("S3 " + operation + " failed for key='" + key + "'", e);
    }

    /**
     * AWS error codes that are retryable regardless of HTTP status code.
     * For example, {@code RequestTimeout} arrives as HTTP 400 but is transient and should
     * be retried. This list is sourced from the AWS SDK retry-condition documentation.
     */
    private static final java.util.Set<String> RETRYABLE_ERROR_CODES =
            new java.util.HashSet<>(java.util.Arrays.asList(
                    "RequestTimeout",
                    "RequestTimeoutException",
                    "PriorRequestNotComplete",
                    "InternalError",
                    "ServiceUnavailable",
                    "SlowDown",
                    "ProvisionedThroughputExceededException"
            ));

    private static boolean isRetryableServiceFailure(AwsServiceException e) {
        if (e.isThrottlingException()) {
            return true;
        }
        AwsErrorDetails errorDetails = e.awsErrorDetails();
        String code = errorDetails != null ? errorDetails.errorCode() : "";
        if (code != null && RETRYABLE_ERROR_CODES.contains(code)) {
            return true;
        }
        int status = e.statusCode();
        return status == 408 || status == 425 || status == 429 ||
               status == 500 || status == 502 || status == 503 || status == 504;
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
}
