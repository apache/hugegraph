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
 * <p>Mapped from application.yml under the {@code cloud.storage} prefix.
  <pre>
 * cloud:
 *   storage:
 *     enabled: true
 *     provider: s3
 *     path-prefix: hugegraph
 *     s3:
 *       bucket: hugegraph-store
 *       region: us-east-1
 *       access-key: AKIAIOSFODNN7EXAMPLE
 *       secret-key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
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
     * Maximum number of whole-file upload retries after a first failure. Default is {@code 5}.
     * Under the primary-durability model an SST that never reaches cloud is at risk on the
     * ephemeral local disk, so whole-file retries are enabled by default. After all attempts are
     * exhausted the task is moved to the DLQ for operational visibility / replay.
     *
     * <p>Set to {@code 0} to disable whole-file retries (failures go straight to the DLQ) when the
     * provider already implements a sufficient internal retry strategy.
     */
    private int uploadRetryMaxAttempts = 3;

    /**
     * Delay before the first whole-file retry attempt, in milliseconds.
     * Subsequent retries use exponential back-off up to {@link #uploadRetryMaxDelayMs}.
     * Only used when {@link #uploadRetryMaxAttempts} > 0. Default: 1 000 ms.
     */
    private long uploadRetryInitialDelayMs = 1_000L;

    /**
     * Upper bound for the exponential back-off delay for whole-file retries, in milliseconds.
     * Only used when {@link #uploadRetryMaxAttempts} > 0. Default: 60 000 ms.
     */
    private long uploadRetryMaxDelayMs = 60_000L;

    /**
     * Backpressure high-watermark on the pending-upload backlog (retry-queue in-flight + DLQ).
     * When {@code > 0} and the backlog exceeds this value, RocksDB's flush/compaction thread is
     * briefly slowed in {@code onTableFileCreated} so ingestion cannot outrun the cloud mirror,
     * bounding the amount of local-only (at-risk) data. Default: {@code 64}. {@code 0} disables it.
     */
    private int uploadBackpressureHighWatermark = 64;


    /**
     * WAL durability mode for metadata mirroring:
     * <ul>
     *   <li>{@code flush} (default): a flush is forced before each metadata capture so the durable
     *       state is fully in SST files; at most the un-flushed in-memory tail written since the
     *       last sync is lost on an uncontrolled crash.</li>
     *   <li>{@code wal}: the active WAL {@code *.log} segments are mirrored alongside the metadata
     *       and replayed on restore (lower RPO, at the cost of more frequent small uploads).</li>
     * </ul>
     */
    private String walMode = "flush";


    /**
     * Provider-specific properties, mapped from {@code cloud.storage.<provider>.*} in the configuration.
     * These are passed verbatim to the provider implementation and may include credentials,
     */
    private Map<String, String> providerProperties = new HashMap<>();
}
