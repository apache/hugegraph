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

import lombok.Data;

/**
 * Amazon S3 / S3-compatible provider configuration, bound from
 * {@code cloud.storage.s3.*} in application.yml.
 *
 * <p>This is a plain Java POJO (no Spring annotations) so it can be used
 * both as a Spring {@code @ConfigurationProperties} target (via standard setters)
 * and directly in tests without a Spring context.
 *
 * <pre>
 * cloud:
 *   storage:
 *     s3:
 *       bucket: hugegraph-store
 *       region: us-east-1
 *       endpoint:                             # optional custom endpoint (MinIO, Ceph…)
 *       access-key: AKIAIOSFODNN7EXAMPLE      # omit to use AWS default credentials chain
 *       secret-key: wJalrXUtnFEMI/…           # omit to use AWS default credentials chain
 *       multipart-part-retry-max-attempts: 3
 *       multipart-part-retry-base-backoff-ms: 1000
 *       multipart-exhausted-direct-dlq: false
 * </pre>
 */
@Data
public class S3CloudStorageConfig {

    public static final int DEFAULT_MULTIPART_PART_RETRY_MAX_ATTEMPTS = 3;
    public static final long DEFAULT_MULTIPART_PART_RETRY_BASE_BACKOFF_MS = 1000L;

    public static final String KEY_BUCKET = "bucket";
    public static final String KEY_REGION = "region";
    public static final String KEY_ENDPOINT = "endpoint";
    public static final String KEY_ACCESS_KEY = "access-key";
    public static final String KEY_SECRET_KEY = "secret-key";
    public static final String KEY_MULTIPART_RETRY_MAX_ATTEMPTS =
            "multipart-part-retry-max-attempts";
    public static final String KEY_MULTIPART_RETRY_BASE_BACKOFF_MS =
            "multipart-part-retry-base-backoff-ms";
    public static final String KEY_MULTIPART_EXHAUSTED_DIRECT_DLQ =
            "multipart-exhausted-direct-dlq";

    /**
     * S3 bucket name.
     */
    private String bucket;

    /**
     * AWS region (e.g. "us-east-1").
     */
    private String region;

    /**
     * Optional custom endpoint URL for S3-compatible stores (e.g. MinIO, Ceph).
     * Leave empty to use the default AWS endpoint.
     */
    private String endpoint;

    /**
     * AWS access key ID. Omit to use the default AWS credentials chain
     * (env vars, instance profile, ~/.aws/credentials, etc.).
     */
    private String accessKey;

    /**
     * AWS secret access key. Omit to use the default AWS credentials chain.
     */
    private String secretKey;

    /**
     * Maximum retry attempts for a single multipart chunk upload (part-level retry).
     * Default: {@value DEFAULT_MULTIPART_PART_RETRY_MAX_ATTEMPTS}.
     */
    private int multipartPartRetryMaxAttempts =
            DEFAULT_MULTIPART_PART_RETRY_MAX_ATTEMPTS;

    /**
     * Base backoff in milliseconds for multipart chunk retry (1x/2x/4x…).
     * Default: {@value DEFAULT_MULTIPART_PART_RETRY_BASE_BACKOFF_MS} ms.
     */
    private long multipartPartRetryBaseBackoffMs =
            DEFAULT_MULTIPART_PART_RETRY_BASE_BACKOFF_MS;

    /**
     * If true, multipart chunk retry exhaustion is treated as non-retryable so outer
     * SST retry can move directly to DLQ without further whole-file attempts.
     */
    private boolean multipartExhaustedDirectDlq = false;

}

