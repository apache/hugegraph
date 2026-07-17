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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

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
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Unit-level tests for {@link S3CloudStorageProvider}: SDK-exception classification (retryable vs
 * non-retryable), multipart abort-on-non-retryable-failure, and client lifecycle across re-init.
 * The full single-large-file upload/download round trip lives in
 * {@link S3SingleLargeFileE2ETest}.
 */
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
    // Helpers
    // =========================================================================

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
