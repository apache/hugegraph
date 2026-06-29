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

package org.apache.hugegraph.store.cloud;

import java.util.HashMap;
import java.util.Map;

import lombok.Data;

/**
 * Configuration for pluggable cloud storage providers.
 *
 * <p>Mapped from application.yml under the {@code cloud.storage} prefix. Example:
 * <pre>
 * cloud:
 *   storage:
 *     enabled: true
 *     provider: s3
 *     bucket: hugegraph-store
 *     region: us-east-1
 *     access-key: AKIAIOSFODNN7EXAMPLE
 *     secret-key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
 *     path-prefix: hugegraph/data
 * </pre>
 */
@Data
public class CloudStorageConfig {

    /**
     * Whether cloud storage is enabled. Defaults to false.
     */
    private boolean enabled = false;

    /**
     * Name of the provider to activate. Must match {@link CloudStorageProvider#providerName()}.
     * The provider JAR must be on the classpath. Defaults to "s3".
     */
    private String provider = "s3";

    /**
     * Cloud storage bucket / container name.
     */
    private String bucket;

    /**
     * Cloud region (e.g. "us-east-1").
     */
    private String region;

    /**
     * Optional custom endpoint URL for S3-compatible stores (e.g. MinIO, Ceph).
     * Leave empty to use the default AWS endpoint.
     */
    private String endpoint;

    /**
     * Access key / access ID credential.
     */
    private String accessKey;

    /**
     * Secret key / secret credential.
     */
    private String secretKey;

    /**
     * Key prefix prepended to every object stored in the bucket.
     * Defaults to "hugegraph".
     */
    private String pathPrefix = "hugegraph";

    /**
     * Whether startup pre-hydration (cloud -> local before DB open) is enabled.
     */
    private boolean startupHydrationEnabled = true;

    /**
     * Guard window in milliseconds for repeated read-miss hydration attempts on the
     * same db/table pair. Values <= 0 disable the guard.
     */
    private long readMissGuardWindowMs = 3000L;

    /**
     * Provider-specific extra properties forwarded verbatim to the provider on init.
     */
    private Map<String, String> extraProperties = new HashMap<>();
}
