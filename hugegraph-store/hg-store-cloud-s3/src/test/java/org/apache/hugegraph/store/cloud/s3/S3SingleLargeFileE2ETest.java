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

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.UUID;

import org.apache.hugegraph.store.cloud.CloudStorageConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;

/**
 * End-to-end large-file S3 test focused on one realistic scenario:
 * generate one 20 GB SST-like file, upload (multipart), check remote existence,
 * download it back, and validate size.
 *
 * <p><b>Setup:</b> The test requires a running S3-compatible service (e.g., MinIO).
 * It automatically skips if the endpoint is unreachable or required properties are missing.
 *
 * <p><b>Zero-config local run:</b> If no VM options are set, the test defaults to a local
 * MinIO instance at {@code http://localhost:9000} with credentials {@code minioadmin/minioadmin}.
 * Simply start MinIO and run the test directly from your IDE:
 * <pre>
 * docker run -p 9000:9000 \
 *   -e MINIO_ROOT_USER=minioadmin \
 *   -e MINIO_ROOT_PASSWORD=minioadmin \
 *   minio/minio server /data
 * </pre>
 *
 * <p><b>Custom run with explicit properties:</b>
 * <pre>
 * # Start MinIO in Docker:
 * docker run -p 9000:9000 -e MINIO_ROOT_USER=minioadmin \
 *   -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
 *
 * # Run test:
 * mvn test -pl hugegraph-store/hg-store-cloud-s3 \
 *   -Dtest=S3SingleLargeFileE2ETest \
 *   -Ds3.perf.endpoint=<a href="http://localhost:9000">...</a> \
 *   -Ds3.perf.bucket=hugegraph-perf \
 *   -Ds3.perf.accessKey=minioadmin \
 *   -Ds3.perf.secretKey=minioadmin \
 *   -Ds3.perf.region=us-east-1 \
 *   -Ds3.perf.singleFileSizeGb=20
 * </pre>
 *
 * <p><b>Properties:</b>
 * <ul>
 *   <li>{@code s3.perf.bucket} (required) — S3 bucket name
 *   <li>{@code s3.perf.endpoint} (required if MinIO) — Service endpoint URL
 *   <li>{@code s3.perf.accessKey} — Access key (default: empty, uses default credentials)
 *   <li>{@code s3.perf.secretKey} — Secret key (default: empty, uses default credentials)
 *   <li>{@code s3.perf.region} — AWS region (default: us-east-1)
 *   <li>{@code s3.perf.pathPrefix} — Path prefix in bucket (default: hugegraph-perf)
 *   <li>{@code s3.perf.singleFileSizeGb} — File size in GB for test (default: 20)
 *   <li>{@code s3.perf.skipCleanup} — Skip remote file cleanup after test (default: false)
 * </ul>
 *
 * <p>The test auto-skips if:
 * <ul>
 *   <li>The endpoint (default: {@code http://localhost:9000}) is not reachable
 * </ul>
 */
@SuppressWarnings("SameParameterValue")
public class S3SingleLargeFileE2ETest {

    private static final String PROP_BUCKET = "s3.perf.bucket";
    private static final String PROP_REGION = "s3.perf.region";
    private static final String PROP_ENDPOINT = "s3.perf.endpoint";
    private static final String PROP_ACCESS_KEY = "s3.perf.accessKey";
    private static final String PROP_SECRET_KEY = "s3.perf.secretKey";
    private static final String PROP_PATH_PREFIX = "s3.perf.pathPrefix";
    private static final String PROP_SINGLE_FILE_GB = "s3.perf.singleFileSizeGb";
    private static final String PROP_SKIP_CLEANUP = "s3.perf.skipCleanup";

    // Defaults for zero-config local MinIO runs (e.g. running the test directly from an IDE).
    // These values match the standard MinIO Docker quickstart:
    //   docker run -p 9000:9000 -e MINIO_ROOT_USER=minioadmin \
    //              -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
    private static final String DEFAULT_ENDPOINT   = "http://localhost:9000";
    private static final String DEFAULT_BUCKET     = "hugegraph-perf";
    private static final String DEFAULT_REGION     = "us-east-1";
    private static final String DEFAULT_ACCESS_KEY = "minioadmin";
    private static final String DEFAULT_SECRET_KEY = "minioadmin";

    private S3CloudStorageProvider provider;
    private Path tmpDir;
    private String remoteKey;
    private boolean skipCleanup;

    /** S3 client used exclusively for bucket-lifecycle management in tests. */
    private S3Client adminS3Client;

    @Before
    public void setUp() throws IOException {
        String endpoint = System.getProperty(PROP_ENDPOINT, DEFAULT_ENDPOINT);
        String bucket   = System.getProperty(PROP_BUCKET,   DEFAULT_BUCKET);
        String region   = System.getProperty(PROP_REGION,   DEFAULT_REGION);
        String ak       = System.getProperty(PROP_ACCESS_KEY, DEFAULT_ACCESS_KEY);
        String sk       = System.getProperty(PROP_SECRET_KEY, DEFAULT_SECRET_KEY);

        // Verify the endpoint is reachable — skip gracefully if MinIO / S3 is not running
        Assume.assumeTrue(
            "Skipping S3SingleLargeFileE2ETest: S3 endpoint not reachable at " + endpoint
            + ". Start MinIO with: docker run -p 9000:9000 "
            + "-e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin "
            + "minio/minio server /data",
            isEndpointReachable(endpoint));

        // Build an admin S3 client for bucket management
        this.adminS3Client = buildAdminS3Client(endpoint, region, ak, sk);

        // Auto-create the bucket if it does not exist yet
        ensureBucketExists(this.adminS3Client, bucket, region, endpoint);

        CloudStorageConfig s3Cfg = new CloudStorageConfig();
        s3Cfg.setEnabled(true);
        s3Cfg.setProvider("s3");
        s3Cfg.setPathPrefix(System.getProperty(PROP_PATH_PREFIX, "hugegraph-perf"));
        s3Cfg.getProviderProperties().put("bucket",     bucket);
        s3Cfg.getProviderProperties().put("region",     region);
        s3Cfg.getProviderProperties().put("endpoint",   endpoint);
        s3Cfg.getProviderProperties().put("access-key", ak);
        s3Cfg.getProviderProperties().put("secret-key", sk);

        this.skipCleanup = boolProp(PROP_SKIP_CLEANUP, false);
        this.provider = new S3CloudStorageProvider();
        this.provider.init(s3Cfg);
        this.tmpDir = Files.createTempDirectory("hg-s3-e2e-");
    }

    @After
    public void tearDown() throws Exception {
        if (this.provider != null) {
            if (!this.skipCleanup && this.remoteKey != null) {
                try {
                    this.provider.deleteFile(this.remoteKey);
                } catch (Exception ignored) {
                    // Best-effort cleanup only.
                }
            }
            this.provider.close();
        }
        if (this.adminS3Client != null) {
            this.adminS3Client.close();
        }
        if (this.tmpDir != null) {
            deleteDirectory(this.tmpDir.toFile());
        }
    }

    @Test
    public void uploadDownloadLargeFileEndToEnd() throws Exception {
        long fileSizeGb = longProp(PROP_SINGLE_FILE_GB, 20L);
        long fileSizeBytes = fileSizeGb * 1024L * 1024L * 1024L;

        Assert.assertTrue("file size must trigger multipart upload",
                          fileSizeBytes > S3CloudStorageProvider.MULTIPART_THRESHOLD_BYTES);

        Path localSrc = this.tmpDir.resolve("src-" + UUID.randomUUID() + ".sst");
        Path localDst = this.tmpDir.resolve("dst-" + UUID.randomUUID() + ".sst");
        this.remoteKey = "perf-single-file/hugegraph-large-" + UUID.randomUUID() + ".sst";

        System.out.printf(Locale.US,
                          "%n[E2E] generating file: size=%d GB (%s)%n",
                          fileSizeGb, humanSize(fileSizeBytes));
        generateLargeFile(localSrc, fileSizeBytes);

        long uploadStart = System.nanoTime();
        this.provider.uploadFile(localSrc.toString(), this.remoteKey);
        long uploadMs = (System.nanoTime() - uploadStart) / 1_000_000;

        Assert.assertTrue("uploaded object not found in remote storage",
                          this.provider.fileExists(this.remoteKey));

        long downloadStart = System.nanoTime();
        this.provider.downloadFile(this.remoteKey, localDst.toString());
        long downloadMs = (System.nanoTime() - downloadStart) / 1_000_000;

        long srcSize = Files.size(localSrc);
        long dstSize = Files.size(localDst);
        Assert.assertEquals("downloaded file size mismatch", srcSize, dstSize);

        double uploadMBps = srcSize > 0 && uploadMs > 0
                            ? (srcSize / 1_048_576.0) / (uploadMs / 1000.0)
                            : 0.0;
        double downloadMBps = dstSize > 0 && downloadMs > 0
                              ? (dstSize / 1_048_576.0) / (downloadMs / 1000.0)
                              : 0.0;

        System.out.printf(Locale.US,
                          "[E2E] upload=%s (%.2f MB/s), download=%s (%.2f MB/s)%n",
                          humanDuration(uploadMs), uploadMBps,
                          humanDuration(downloadMs), downloadMBps);
    }

    private static void generateLargeFile(Path dest, long sizeBytes) throws IOException {
        final int blockSize = 64 * 1024 * 1024;
        byte[] block = buildSstBlock(blockSize);
        long written = 0L;

        try (BufferedOutputStream out = new BufferedOutputStream(
                new FileOutputStream(dest.toFile()), blockSize)) {
            while (written < sizeBytes) {
                long toWrite = Math.min(blockSize, sizeBytes - written);
                block[0] = (byte) (written >>> 20);
                out.write(block, 0, (int) toWrite);
                written += toWrite;
            }
        }
    }

    private static byte[] buildSstBlock(int size) {
        byte[] block = new byte[size];
        block[0] = (byte) 0x57;
        block[1] = (byte) 0xfb;
        block[2] = (byte) 0x80;
        block[3] = (byte) 0x8b;
        for (int i = 4; i < size; i++) {
            block[i] = (byte) ((i * 1103515245 + 12345) & 0xFF);
        }
        return block;
    }

    private static String humanSize(long bytes) {
        if (bytes < 1024L) {
            return bytes + " B";
        }
        if (bytes < 1024L * 1024L) {
            return String.format(Locale.US, "%.1f KB", bytes / 1024.0);
        }
        if (bytes < 1024L * 1024L * 1024L) {
            return String.format(Locale.US, "%.1f MB", bytes / (1024.0 * 1024.0));
        }
        return String.format(Locale.US, "%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }

    private static String humanDuration(long millis) {
        if (millis < 1000L) {
            return millis + " ms";
        }
        if (millis < 60_000L) {
            return String.format(Locale.US, "%.1f s", millis / 1000.0);
        }
        long m = millis / 60_000L;
        long s = (millis % 60_000L) / 1000L;
        return String.format(Locale.US, "%dm %02ds", m, s);
    }

    private static long longProp(String key, long def) {
        String v = System.getProperty(key);
        if (v == null || v.isBlank()) {
            return def;
        }
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static boolean boolProp(String key, boolean def) {
        String v = System.getProperty(key);
        if (v == null || v.isBlank()) {
            return def;
        }
        return Boolean.parseBoolean(v.trim());
    }

    /**
     * Builds an S3 admin client for bucket management during tests.
     * Mirrors the same credential / endpoint / region logic used by {@link S3CloudStorageProvider}.
     */
    private static S3Client buildAdminS3Client(String endpoint, String region,
                                               String ak, String sk) {
        S3ClientBuilder builder = S3Client.builder();

        if (ak != null && !ak.isEmpty() && sk != null && !sk.isEmpty()) {
            builder.credentialsProvider(
                    StaticCredentialsProvider.create(AwsBasicCredentials.create(ak, sk)));
        } else {
            builder.credentialsProvider(DefaultCredentialsProvider.builder().build());
        }

        if (region != null && !region.isEmpty()) {
            builder.region(Region.of(region));
        }

        if (endpoint != null && !endpoint.isEmpty()) {
            builder.endpointOverride(URI.create(endpoint));
            builder.serviceConfiguration(
                    software.amazon.awssdk.services.s3.S3Configuration.builder()
                                                                       .pathStyleAccessEnabled(true)
                                                                       .build());
        }

        return builder.build();
    }

    /**
     * Creates the bucket if it does not already exist.
     * For AWS S3, {@code CreateBucketRequest} needs a {@code LocationConstraint} for
     * regions other than {@code us-east-1}; for MinIO (path-style) no constraint is needed.
     *
     * @param s3     admin S3 client
     * @param bucket bucket name to ensure
     * @param region AWS region (used for location constraint on real AWS)
     * @param endpoint custom endpoint (empty for real AWS)
     */
    private static void ensureBucketExists(S3Client s3, String bucket,
                                           String region, String endpoint) {
        try {
            s3.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
            System.out.printf("[E2E] bucket already exists: %s%n", bucket);
        } catch (software.amazon.awssdk.services.s3.model.S3Exception e) {
            // Bucket does not exist — create it
            System.out.printf("[E2E] bucket '%s' not found, creating...%n", bucket);

            CreateBucketRequest.Builder createReq = CreateBucketRequest.builder().bucket(bucket);

            // Real AWS: us-east-1 must NOT have a LocationConstraint; every other region must.
            boolean isRealAws = endpoint == null || endpoint.isEmpty();
            if (isRealAws && region != null && !region.isEmpty()
                    && !region.equals("us-east-1")) {
                createReq.createBucketConfiguration(
                        software.amazon.awssdk.services.s3.model.CreateBucketConfiguration.builder()
                                .locationConstraint(region)
                                .build());
            }

            s3.createBucket(createReq.build());
            System.out.printf("[E2E] bucket created: %s%n", bucket);
        }
    }

    /**
     * Checks if the S3 endpoint (e.g., <a href="http://localhost:9000">...</a>) is reachable.
     * Parses the URL to extract host and port, then attempts a socket connection.
     *
     * @param endpoint the endpoint URL (e.g., <a href="http://localhost:9000">...</a>)
     * @return true if reachable, false otherwise
     */
    private static boolean isEndpointReachable(String endpoint) {
        try {
            // Parse endpoint URL to extract host and port
            // Expected format: http://host:port or https://host:port
            java.net.URL url = new java.net.URL(endpoint);
            String host = url.getHost();
            int port = url.getPort();
            if (port == -1) {
                port = url.getDefaultPort(); // 80 for http, 443 for https
            }

            if (host == null || host.isBlank() || port <= 0) {
                return false;
            }

            // Attempt socket connection with 2-second timeout
            try (Socket socket = new Socket()) {
                socket.connect(new java.net.InetSocketAddress(host, port), 2000);
                return true;
            }
        } catch (Exception e) {
            return false;
        }
    }

     @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void deleteDirectory(java.io.File dir) {
        if (!dir.exists()) {
            return;
        }
        java.io.File[] children = dir.listFiles();
        if (children != null) {
            for (java.io.File child : children) {
                if (child.isDirectory()) {
                    deleteDirectory(child);
                } else {
                    child.delete();
                }
            }
        }
        dir.delete();
    }
}

