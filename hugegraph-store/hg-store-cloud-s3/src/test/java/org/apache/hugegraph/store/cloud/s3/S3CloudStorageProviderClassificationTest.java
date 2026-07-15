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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.store.cloud.CloudStorageNonRetryableException;
import org.junit.Test;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3CloudStorageProviderClassificationTest {

    @Test
    public void classifySdkException_retryableServiceStatus_returnsIOException() throws Exception {
        S3CloudStorageProvider provider = new S3CloudStorageProvider();
        AwsServiceException retryable503 = s3ServiceException(503, "SlowDown", "req-503");

        IOException classified = invokeClassify(provider, retryable503);

        assertFalse("503 should be treated as retryable IOException",
                    classified instanceof CloudStorageNonRetryableException);
    }

    @Test
    public void classifySdkException_nonRetryableServiceStatus_returnsNonRetryable() throws Exception {
        S3CloudStorageProvider provider = new S3CloudStorageProvider();
        AwsServiceException forbidden403 = s3ServiceException(403, "AccessDenied", "req-403");

        IOException classified = invokeClassify(provider, forbidden403);

        assertTrue("403 should be treated as non-retryable",
                   classified instanceof CloudStorageNonRetryableException);
    }

    @Test
    public void classifySdkException_retryable429Status_isRetryable() throws Exception {
        S3CloudStorageProvider provider = new S3CloudStorageProvider();
        AwsServiceException throttled = s3ServiceException(429, "TooManyRequests", "req-throttle");

        IOException classified = invokeClassify(provider, throttled);

        assertFalse("429 should be treated as retryable",
                    classified instanceof CloudStorageNonRetryableException);
    }

    @Test
    public void classifySdkException_clientSideFailure_isRetryableIOException() throws Exception {
        S3CloudStorageProvider provider = new S3CloudStorageProvider();
        SdkClientException clientFailure =
                SdkClientException.builder().message("connection reset").build();

        IOException classified = invokeClassify(provider, clientFailure);

        assertFalse("Client-side failures should stay retryable",
                    classified instanceof CloudStorageNonRetryableException);
    }

    @Test
    public void uploadMultipart_nonRetryablePartFailure_rethrowsNonRetryableAfterAbort()
            throws Exception {
        S3CloudStorageProvider provider = new S3CloudStorageProvider();
        AtomicBoolean aborted = new AtomicBoolean(false);

        // Dynamic proxy keeps this test lightweight without extra mocking deps.
        S3Client s3 = (S3Client) java.lang.reflect.Proxy.newProxyInstance(
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
            try {
                invokeUploadMultipart(provider, tmp);
            } catch (CloudStorageNonRetryableException e) {
                // expected path
            }
            assertTrue("Multipart failure must abort upload before rethrowing", aborted.get());
        } finally {
            Files.deleteIfExists(tmp);
        }
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

