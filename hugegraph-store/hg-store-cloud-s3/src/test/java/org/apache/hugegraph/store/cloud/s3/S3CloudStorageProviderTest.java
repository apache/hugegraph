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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.store.cloud.CloudStorageConfig;
import org.apache.hugegraph.store.cloud.CloudStorageNonRetryableException;
import org.junit.Test;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.DeletedObject;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Unit-level tests for {@link S3CloudStorageProvider} and {@link S3CloudStorageConfig}, all driven
 * without a live S3/MinIO via a dynamic-{@link Proxy} {@code S3Client}:
 * <ul>
 *   <li>SDK-exception classification (retryable vs non-retryable) and multipart
 *       abort-on-non-retryable-failure;</li>
 *   <li>client lifecycle across re-init;</li>
 *   <li>operations: single-PUT upload (success/retry/exhaustion), delete, prefix purge (multi-page,
 *       partial-batch-failure, batch-exception fallback, truncated-without-token safety), listing,
 *       {@code fileExists} status handling, download, and human-size formatting;</li>
 *   <li>the {@link S3CloudStorageConfig} bean (defaults, key constants, accessors/equality).</li>
 * </ul>
 * The full single-large-file (multipart) upload/download round trip lives in
 * {@link S3SingleLargeFileE2ETest}.
 */
@SuppressWarnings("resource")
public class S3CloudStorageProviderTest {

    // =========================================================================
    // SDK exception classification
    // =========================================================================

    @Test
    public void testClassifySdkExceptionRetryableServiceStatusReturnsIOException() throws Exception {
        S3CloudStorageProvider provider = new S3CloudStorageProvider();
        AwsServiceException retryable503 = s3ServiceException(503, "SlowDown", "req-503");

        IOException classified = invokeClassify(provider, retryable503);

        assertFalse("503 should be treated as retryable IOException",
                    classified instanceof CloudStorageNonRetryableException);
    }

    @Test
    public void testClassifySdkExceptionNonRetryableServiceStatusReturnsNonRetryable() throws Exception {
        S3CloudStorageProvider provider = new S3CloudStorageProvider();
        AwsServiceException forbidden403 = s3ServiceException(403, "AccessDenied", "req-403");

        IOException classified = invokeClassify(provider, forbidden403);

        assertTrue("403 should be treated as non-retryable",
                   classified instanceof CloudStorageNonRetryableException);
    }

    @Test
    public void testClassifySdkExceptionRetryable429StatusIsRetryable() throws Exception {
        S3CloudStorageProvider provider = new S3CloudStorageProvider();
        AwsServiceException throttled = s3ServiceException(429, "TooManyRequests", "req-throttle");

        IOException classified = invokeClassify(provider, throttled);

        assertFalse("429 should be treated as retryable",
                    classified instanceof CloudStorageNonRetryableException);
    }

    @Test
    public void testClassifySdkExceptionClientSideFailureIsRetryableIOException() throws Exception {
        S3CloudStorageProvider provider = new S3CloudStorageProvider();
        SdkClientException clientFailure =
                SdkClientException.builder().message("connection reset").build();

        IOException classified = invokeClassify(provider, clientFailure);

        assertFalse("Client-side failures should stay retryable",
                    classified instanceof CloudStorageNonRetryableException);
    }

    // =========================================================================
    // Multipart upload: non-retryable part failure aborts then rethrows
    // =========================================================================

    @Test
    public void testUploadMultipartNonRetryablePartFailureRethrowsNonRetryableAfterAbort()
            throws Exception {
        S3CloudStorageProvider provider = new S3CloudStorageProvider();
        AtomicBoolean aborted = new AtomicBoolean(false);

        // Dynamic proxy keeps this test lightweight without extra mocking deps.
        S3Client s3 = (S3Client) Proxy.newProxyInstance(
                S3Client.class.getClassLoader(),
                new Class<?>[]{S3Client.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    switch (name) {
                        case "createMultipartUpload":
                            return CreateMultipartUploadResponse.builder()
                                                                .uploadId("u-1")
                                                                .build();
                        case "uploadPart":
                            throw s3ServiceException(403, "AccessDenied", "req-upload-part");
                        case "abortMultipartUpload":
                            aborted.set(true);
                            return AbortMultipartUploadResponse.builder().build();
                        case "close":
                            return null;
                    }
                    throw new UnsupportedOperationException("Unexpected S3Client method: " + name);
                });

        setField(provider, "s3Client", s3);
        setField(provider, "bucket", "test-bucket");
        setField(provider, "partUploadMaxRetries", 2);

        Path tmp = Files.createTempFile("hg-s3-classify", ".bin");
        Files.write(tmp, new byte[]{1});
        try {
            assertThrows("Non-retryable part failure must propagate as CloudStorageNonRetryableException",
                         CloudStorageNonRetryableException.class,
                         () -> invokeUploadMultipart(provider, tmp));
            assertTrue("Multipart failure must abort upload before rethrowing", aborted.get());
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    // =========================================================================
    // Client lifecycle: init() closes the previous client on re-init (no leak)
    // =========================================================================

    /**
     * {@code S3CloudStorageProvider} is a singleton discovered once via SPI; a Spring context
     * restart re-runs {@code init()} on the same instance. If {@code init()} overwrote the client
     * without closing the previous one, the old SDK connection pool and threads would leak.
     */
    @Test
    public void testInitClosesPreviousClientOnReinit() throws Exception {
        S3CloudStorageProvider provider = new S3CloudStorageProvider();

        // Inject a stub S3Client that records whether close() was invoked.
        AtomicBoolean staleClosed = new AtomicBoolean(false);
        S3Client stale = (S3Client) Proxy.newProxyInstance(
                S3Client.class.getClassLoader(),
                new Class<?>[]{S3Client.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "close":
                            staleClosed.set(true);
                            return null;
                        case "serviceName":
                            return "s3";
                        default:
                            //noinspection SuspiciousInvocationHandlerImplementation
                            return null;
                    }
                });
        setField(provider, "s3Client", stale);

        // Re-initialize. Empty path-prefix skips the (network) stale-multipart sweep; a region is
        // set so the real client builds without needing a resolvable default region.
        CloudStorageConfig cfg = new CloudStorageConfig();
        cfg.setEnabled(true);
        cfg.setProvider("s3");
        cfg.setPathPrefix("");
        cfg.getProviderProperties().put("bucket", "test-bucket");
        cfg.getProviderProperties().put("region", "us-east-1");

        try {
            provider.init(cfg);
            assertTrue("init() must close the previous S3 client to avoid leaking its "
                       + "connection pool/threads on re-init", staleClosed.get());
        } finally {
            provider.close();
        }
    }

    // =========================================================================
    // Pagination safety: truncated listing without a continuation token must fail loudly
    // =========================================================================

    /**
     * Some S3-compatible gateways return {@code isTruncated=true} but omit the continuation token.
     * A prefix purge that stops there would report success while objects remain, causing the caller
     * to drop its tombstone guard and later re-hydrate stale data. deletePrefix() must instead throw.
     */
    @Test
    public void testDeletePrefixTruncatedWithoutTokenFailsLoudly() throws Exception {
        S3CloudStorageProvider provider = new S3CloudStorageProvider();
        S3Client s3 = (S3Client) Proxy.newProxyInstance(
                S3Client.class.getClassLoader(),
                new Class<?>[]{S3Client.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "listObjectsV2":
                            // Truncated, but NO continuation token: the malformed-gateway case.
                            return ListObjectsV2Response.builder().isTruncated(true).build();
                        case "close":
                            return null;
                        default:
                            throw new UnsupportedOperationException(
                                    "Unexpected S3Client method: " + method.getName());
                    }
                });
        setField(provider, "s3Client", s3);
        setField(provider, "bucket", "test-bucket");

        IOException ex = assertThrows("Incomplete purge must fail loudly, not report success",
                                      IOException.class, () -> provider.deletePrefix("db/prefix/"));
        assertTrue("Failure must indicate a truncated/incomplete purge: " + ex.getMessage(),
                   ex.getMessage().toLowerCase().contains("truncated"));
    }

    /**
     * Startup hydration relies on a complete listing (e.g. to find CURRENT). A truncated listing
     * with no continuation token would return a partial key set; listFiles() must throw instead.
     */
    @Test
    public void testListFilesTruncatedWithoutTokenFailsLoudly() throws Exception {
        S3CloudStorageProvider provider = new S3CloudStorageProvider();
        S3Client s3 = (S3Client) Proxy.newProxyInstance(
                S3Client.class.getClassLoader(),
                new Class<?>[]{S3Client.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "listObjectsV2":
                            return ListObjectsV2Response.builder()
                                    .isTruncated(true)
                                    .contents(S3Object.builder()
                                                      .key("db/prefix/000001.sst").build())
                                    .build();
                        case "close":
                            return null;
                        default:
                            throw new UnsupportedOperationException(
                                    "Unexpected S3Client method: " + method.getName());
                    }
                });
        setField(provider, "s3Client", s3);
        setField(provider, "bucket", "test-bucket");

        IOException ex = assertThrows("Partial listing must fail loudly", IOException.class,
                                      () -> provider.listFiles("db/prefix/"));
        assertTrue("Failure must indicate a truncated/partial listing: " + ex.getMessage(),
                   ex.getMessage().toLowerCase().contains("truncated"));
    }

    /**
     * Regression guard: the new truncation check must NOT break the normal (complete) listing path —
     * an {@code isTruncated=false} response with no token is a valid terminal page.
     */
    @Test
    public void testDeletePrefixNotTruncatedCompletesWithoutError() throws Exception {
        S3CloudStorageProvider provider = new S3CloudStorageProvider();
        S3Client s3 = (S3Client) Proxy.newProxyInstance(
                S3Client.class.getClassLoader(),
                new Class<?>[]{S3Client.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "listObjectsV2":
                            return ListObjectsV2Response.builder().isTruncated(false).build();
                        case "close":
                            return null;
                        default:
                            throw new UnsupportedOperationException(
                                    "Unexpected S3Client method: " + method.getName());
                    }
                });
        setField(provider, "s3Client", s3);
        setField(provider, "bucket", "test-bucket");

        assertEquals("A complete (non-truncated) empty listing must succeed with 0 deletions",
                     0, provider.deletePrefix("db/prefix/"));
    }

    // =========================================================================
    // S3CloudStorageConfig bean: defaults, key constants, accessors/equality
    // =========================================================================

    @Test
    public void testConfigDefaults() {
        S3CloudStorageConfig cfg = new S3CloudStorageConfig();
        assertEquals(S3CloudStorageConfig.DEFAULT_MULTIPART_PART_RETRY_MAX_ATTEMPTS,
                     cfg.getMultipartPartRetryMaxAttempts());
        assertEquals(S3CloudStorageConfig.DEFAULT_MULTIPART_PART_RETRY_BASE_BACKOFF_MS,
                     cfg.getMultipartPartRetryBaseBackoffMs());
        assertFalse(cfg.isMultipartExhaustedDirectDlq());
        assertEquals(S3CloudStorageConfig.DEFAULT_MULTIPART_STALE_ABORT_ON_INIT,
                     cfg.isMultipartStaleAbortOnInit());
    }

    @Test
    public void testConfigKeyConstants() {
        assertEquals("bucket", S3CloudStorageConfig.KEY_BUCKET);
        assertEquals("region", S3CloudStorageConfig.KEY_REGION);
        assertEquals("endpoint", S3CloudStorageConfig.KEY_ENDPOINT);
        assertEquals("access-key", S3CloudStorageConfig.KEY_ACCESS_KEY);
        assertEquals("secret-key", S3CloudStorageConfig.KEY_SECRET_KEY);
        assertEquals("multipart-part-retry-max-attempts",
                     S3CloudStorageConfig.KEY_MULTIPART_RETRY_MAX_ATTEMPTS);
        assertEquals("multipart-part-retry-base-backoff-ms",
                     S3CloudStorageConfig.KEY_MULTIPART_RETRY_BASE_BACKOFF_MS);
        assertEquals("multipart-exhausted-direct-dlq",
                     S3CloudStorageConfig.KEY_MULTIPART_EXHAUSTED_DIRECT_DLQ);
        assertEquals("multipart-stale-abort-on-init",
                     S3CloudStorageConfig.KEY_MULTIPART_STALE_ABORT_ON_INIT);
    }

    @Test
    public void testConfigSettersGettersAndEquality() {
        S3CloudStorageConfig a = newPopulatedConfig();
        assertEquals("b", a.getBucket());
        assertEquals("us-east-1", a.getRegion());
        assertEquals("http://localhost:9000", a.getEndpoint());
        assertEquals("ak", a.getAccessKey());
        assertEquals("sk", a.getSecretKey());
        assertEquals(7, a.getMultipartPartRetryMaxAttempts());
        assertEquals(2500L, a.getMultipartPartRetryBaseBackoffMs());
        assertTrue(a.isMultipartExhaustedDirectDlq());
        assertFalse(a.isMultipartStaleAbortOnInit());

        assertNotEquals(new S3CloudStorageConfig(), a);
        S3CloudStorageConfig b = newPopulatedConfig();
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertTrue(a.toString().contains("bucket"));
    }

    // =========================================================================
    // Operations via a dynamic-proxy S3Client (no live S3): upload / delete /
    // deletePrefix / listFiles / fileExists / download / humanSize
    // =========================================================================

    @Test
    public void testUploadFileSinglePartSucceeds() throws Exception {
        AtomicInteger puts = new AtomicInteger();
        S3CloudStorageProvider p = proxyProvider(handler(m -> {
            if (m.equals("putObject")) {
                puts.incrementAndGet();
                return PutObjectResponse.builder().build();
            }
            return null;
        }));
        Path f = tmpFile();
        try {
            p.uploadFile(f.toString(), "db/000001.sst");
            assertEquals("one successful PUT expected", 1, puts.get());
        } finally {
            Files.deleteIfExists(f);
        }
    }

    @Test
    public void testUploadFileSinglePartRetriesThenSucceeds() throws Exception {
        AtomicInteger puts = new AtomicInteger();
        S3CloudStorageProvider p = proxyProvider(handler(m -> {
            if (m.equals("putObject")) {
                if (puts.incrementAndGet() == 1) {
                    throw s3ServiceException(503, "SlowDown", "r1"); // transient → retry
                }
                return PutObjectResponse.builder().build();
            }
            return null;
        }));
        setField(p, "partUploadRetryBaseBackoffMs", 1L); // keep the backoff sleep tiny
        Path f = tmpFile();
        try {
            p.uploadFile(f.toString(), "db/2.sst");
            assertEquals("PUT should be retried once then succeed", 2, puts.get());
        } finally {
            Files.deleteIfExists(f);
        }
    }

    @Test
    public void testUploadFileSinglePartExhaustsRetriesThrowsIoException() throws Exception {
        S3CloudStorageProvider p = proxyProvider(handler(m -> {
            if (m.equals("putObject")) {
                throw s3ServiceException(503, "SlowDown", "r"); // always transient
            }
            return null;
        }));
        setField(p, "partUploadMaxRetries", 2);
        setField(p, "partUploadRetryBaseBackoffMs", 1L);
        Path f = tmpFile();
        try {
            IOException ex = assertThrows(IOException.class,
                                          () -> p.uploadFile(f.toString(), "db/3.sst"));
            assertTrue(ex.getMessage().contains("after 2 attempt"));
        } finally {
            Files.deleteIfExists(f);
        }
    }

    @Test
    public void testDeleteFileSucceeds() throws Exception {
        AtomicInteger deletes = new AtomicInteger();
        S3CloudStorageProvider p = proxyProvider(handler(m -> {
            if (m.equals("deleteObject")) {
                deletes.incrementAndGet();
                return DeleteObjectResponse.builder().build();
            }
            return null;
        }));
        p.deleteFile("db/old.sst");
        assertEquals(1, deletes.get());
    }

    @Test
    public void testDeleteFileNonRetryableIsClassified() throws Exception {
        S3CloudStorageProvider p = proxyProvider(handler(m -> {
            if (m.equals("deleteObject")) {
                throw s3ServiceException(403, "AccessDenied", "r");
            }
            return null;
        }));
        assertThrows(IOException.class, () -> p.deleteFile("db/old.sst"));
    }

    @Test
    public void testDeletePrefixDeletesAcrossPages() throws Exception {
        AtomicInteger lists = new AtomicInteger();
        S3CloudStorageProvider p = proxyProvider(handler((m, args) -> {
            switch (m) {
                case "listObjectsV2":
                    if (lists.incrementAndGet() == 1) {
                        return ListObjectsV2Response.builder()
                                .contents(obj("db/p/1.sst"), obj("db/p/2.sst"), obj("db/p/"))
                                .isTruncated(true).nextContinuationToken("t1").build();
                    }
                    return ListObjectsV2Response.builder()
                            .contents(obj("db/p/3.sst"))
                            .isTruncated(false).build();
                case "deleteObjects":
                    software.amazon.awssdk.services.s3.model.DeleteObjectsRequest req =
                            (software.amazon.awssdk.services.s3.model.DeleteObjectsRequest) args[0];
                    List<DeletedObject> deleted = req.delete().objects().stream()
                            .map(o -> DeletedObject.builder().key(o.key()).build())
                            .collect(java.util.stream.Collectors.toList());
                    return DeleteObjectsResponse.builder().deleted(deleted).build();
                default:
                    return null;
            }
        }));
        // Directory marker "db/p/" must be skipped; 3 real objects deleted across 2 pages.
        assertEquals(3, p.deletePrefix("db/p/"));
    }

    @Test
    public void testDeletePrefixPartialBatchFailureRetriedIndividually() throws Exception {
        S3CloudStorageProvider p = proxyProvider(handler((m, args) -> {
            switch (m) {
                case "listObjectsV2":
                    return ListObjectsV2Response.builder()
                            .contents(obj("db/p/1.sst"), obj("db/p/2.sst"))
                            .isTruncated(false).build();
                case "deleteObjects":
                    // One key deleted, one reported as an error (retried individually below).
                    return DeleteObjectsResponse.builder()
                            .deleted(DeletedObject.builder().key("db/p/1.sst").build())
                            .errors(S3Error.builder().key("db/p/2.sst")
                                            .code("InternalError").message("x").build())
                            .build();
                case "deleteObject":
                    return DeleteObjectResponse.builder().build();
                default:
                    return null;
            }
        }));
        assertEquals("batch-deleted + individually-retried", 2, p.deletePrefix("db/p/"));
    }

    @Test
    public void testDeletePrefixBatchExceptionFallsBackToIndividualDeletes() throws Exception {
        S3CloudStorageProvider p = proxyProvider(handler((m, args) -> {
            switch (m) {
                case "listObjectsV2":
                    return ListObjectsV2Response.builder()
                            .contents(obj("db/p/1.sst"), obj("db/p/2.sst"))
                            .isTruncated(false).build();
                case "deleteObjects":
                    throw s3ServiceException(500, "InternalError", "r"); // batch API fails entirely
                case "deleteObject":
                    return DeleteObjectResponse.builder().build(); // fallback individual deletes ok
                default:
                    return null;
            }
        }));
        assertEquals("fallback individual deletes", 2, p.deletePrefix("db/p/"));
    }

    @Test
    public void testListFilesReturnsKeysAcrossPagesSkippingDirMarkers() throws Exception {
        AtomicInteger lists = new AtomicInteger();
        S3CloudStorageProvider p = proxyProvider(handler(m -> {
            if (m.equals("listObjectsV2")) {
                if (lists.incrementAndGet() == 1) {
                    return ListObjectsV2Response.builder()
                            .contents(obj("db/p/CURRENT"), obj("db/p/"))
                            .isTruncated(true).nextContinuationToken("t1").build();
                }
                return ListObjectsV2Response.builder()
                        .contents(obj("db/p/000001.sst"))
                        .isTruncated(false).build();
            }
            return null;
        }));
        List<String> keys = p.listFiles("db/p/");
        assertEquals(List.of("db/p/CURRENT", "db/p/000001.sst"), keys);
    }

    @Test
    public void testFileExistsTrueWhenHeadSucceeds() throws Exception {
        S3CloudStorageProvider p = proxyProvider(handler(m ->
                m.equals("headObject") ? HeadObjectResponse.builder().build() : null));
        assertTrue(p.fileExists("db/x.sst"));
    }

    @Test
    public void testFileExistsFalseOnNoSuchKey() throws Exception {
        S3CloudStorageProvider p = proxyProvider(handler(m -> {
            if (m.equals("headObject")) {
                throw NoSuchKeyException.builder().message("missing").build();
            }
            return null;
        }));
        assertFalse(p.fileExists("db/x.sst"));
    }

    @Test
    public void testFileExistsFalseOnGeneric404() throws Exception {
        S3CloudStorageProvider p = proxyProvider(handler(m -> {
            if (m.equals("headObject")) {
                throw s3ServiceException(404, "NotFound", "r"); // S3-compatible generic 404
            }
            return null;
        }));
        assertFalse(p.fileExists("db/x.sst"));
    }

    @Test
    public void testFileExistsThrowsOnMissingBucket() throws Exception {
        S3CloudStorageProvider p = proxyProvider(handler(m -> {
            if (m.equals("headObject")) {
                throw s3ServiceException(404, "NoSuchBucket", "r"); // misconfig, not a missing key
            }
            return null;
        }));
        IOException ex = assertThrows(IOException.class, () -> p.fileExists("db/x.sst"));
        assertTrue(ex.getMessage().contains("bucket"));
    }

    @Test
    public void testDownloadFileInvokesGetObject() throws Exception {
        AtomicInteger gets = new AtomicInteger();
        S3CloudStorageProvider p = proxyProvider(handler(m -> {
            if (m.equals("getObject")) {
                gets.incrementAndGet();
                return GetObjectResponse.builder().build();
            }
            return null;
        }));
        Path dest = Files.createTempDirectory("hg-s3-dl").resolve("out.sst");
        p.downloadFile("db/x.sst", dest.toString());
        assertEquals(1, gets.get());
    }

    @Test
    public void testHumanSizeUnits() {
        assertEquals("512 B", S3CloudStorageProvider.humanSize(512));
        assertTrue(S3CloudStorageProvider.humanSize(2048).endsWith(" KB"));
        assertTrue(S3CloudStorageProvider.humanSize(5L * 1024 * 1024).endsWith(" MB"));
        assertTrue(S3CloudStorageProvider.humanSize(3L * 1024 * 1024 * 1024).endsWith(" GB"));
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static S3CloudStorageConfig newPopulatedConfig() {
        S3CloudStorageConfig c = new S3CloudStorageConfig();
        c.setBucket("b");
        c.setRegion("us-east-1");
        c.setEndpoint("http://localhost:9000");
        c.setAccessKey("ak");
        c.setSecretKey("sk");
        c.setMultipartPartRetryMaxAttempts(7);
        c.setMultipartPartRetryBaseBackoffMs(2500L);
        c.setMultipartExhaustedDirectDlq(true);
        c.setMultipartStaleAbortOnInit(false);
        return c;
    }

    /** Builds a provider with the given proxy S3Client and a fixed bucket, no path-prefix. */
    private static S3CloudStorageProvider proxyProvider(InvocationHandler handler) throws Exception {
        S3CloudStorageProvider p = new S3CloudStorageProvider();
        S3Client s3 = (S3Client) Proxy.newProxyInstance(
                S3Client.class.getClassLoader(), new Class<?>[]{S3Client.class}, handler);
        setField(p, "s3Client", s3);
        setField(p, "bucket", "test-bucket");
        return p;
    }

    /** Handler dispatching on method name only. */
    private interface ByName {
        Object apply(String method) throws Throwable;
    }

    /** Handler dispatching on method name and raw args. */
    private interface ByNameArgs {
        Object apply(String method, Object[] args) throws Throwable;
    }

    private static InvocationHandler handler(ByName fn) {
        return (proxy, method, args) -> {
            if (method.getName().equals("close")) {
                return null;
            }
            Object r = fn.apply(method.getName());
            if (r != null) {
                return r;
            }
            throw new UnsupportedOperationException("Unexpected S3Client call: " + method.getName());
        };
    }

    private static InvocationHandler handler(ByNameArgs fn) {
        return (proxy, method, args) -> {
            if (method.getName().equals("close")) {
                return null;
            }
            Object r = fn.apply(method.getName(), args);
            if (r != null) {
                return r;
            }
            throw new UnsupportedOperationException("Unexpected S3Client call: " + method.getName());
        };
    }

    private static S3Object obj(String key) {
        return S3Object.builder().key(key).build();
    }

    private static Path tmpFile() throws IOException {
        Path f = Files.createTempFile("hg-s3-op", ".sst");
        Files.write(f, new byte[16]);
        return f;
    }

    private static IOException invokeClassify(S3CloudStorageProvider provider, SdkException e)
            throws Exception {
        Method classify = S3CloudStorageProvider.class.getDeclaredMethod(
                "classifySdkException", String.class, String.class, SdkException.class);
        classify.setAccessible(true);
        return (IOException) classify.invoke(provider, "uploadPart", "k.sst", e);
    }

    private static void invokeUploadMultipart(S3CloudStorageProvider provider,
                                              Path path) throws Exception {
        Method method = S3CloudStorageProvider.class.getDeclaredMethod(
                "uploadMultipart", java.nio.file.Path.class, long.class, String.class);
        method.setAccessible(true);
        try {
            method.invoke(provider, path, 1L, "k.sst");
        } catch (InvocationTargetException ite) {
            Throwable cause = ite.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw ite;
        }
    }

    private static void setField(S3CloudStorageProvider provider,
                                 String fieldName,
                                 Object value) throws Exception {
        Field field = S3CloudStorageProvider.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(provider, value);
    }

    private static AwsServiceException s3ServiceException(int statusCode,
                                                          String errorCode,
                                                          String requestId) {
        AwsErrorDetails details = AwsErrorDetails.builder()
                                                .serviceName("S3")
                                                .errorCode(errorCode)
                                                .errorMessage("simulated")
                                                .build();
        return S3Exception.builder()
                          .statusCode(statusCode)
                          .requestId(requestId)
                          .awsErrorDetails(details)
                          .message("simulated")
                          .build();
    }
}
