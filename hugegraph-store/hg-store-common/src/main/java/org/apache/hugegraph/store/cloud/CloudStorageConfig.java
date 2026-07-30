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
     * Maximum number of whole-file upload retries after a first failure. Default is {@code 3}
     * (whole-file retries enabled). Under the primary-durability model an SST that never reaches
     * cloud is at risk on the ephemeral local disk, so whole-file retries are on by default. After
     * all attempts are exhausted the task is moved to the DLQ for operational visibility / replay.
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
     * Backpressure high-watermark on the pending-upload backlog. The backlog is the sum of: the
     * async upload executor's queued + active uploads, the retry queue's in-flight (scheduled or
     * executing) retries, and a bounded DLQ <em>enqueue rate</em> — the number of uploads that
     * exhausted their retries and became local-only within a trailing ~1s window, capped at this
     * watermark. The DLQ enqueue rate (not the static DLQ depth) is used so backpressure engages
     * while durability is actively degrading during a sustained outage, yet releases once failures
     * stop rather than pinning the write path on historical DLQ debt awaiting an explicit replay.
     * When {@code > 0} and the backlog exceeds this value, RocksDB's flush/compaction thread is
     * briefly slowed in {@code onTableFileCreated} so ingestion cannot outrun the cloud mirror,
     * bounding the amount of local-only (at-risk) data. Default: {@code 64}. {@code 0} disables it.
     */
    private int uploadBackpressureHighWatermark = 64;


    /**
     * Maximum number of entries retained in the failed-upload dead-letter queue (in memory and,
     * amortized, on disk). Bounds memory/disk growth during a prolonged provider outage; when
     * exceeded the oldest entries are evicted (evicted files stay recoverable via the delete guard
     * and startup SST backfill). Must be {@code > 0}. Default: {@code 100000}.
     */
    private int dlqMaxSize = 100_000;

    /**
     * Debounce window in milliseconds for the per-SST metadata sync triggered by
     * {@code onTableFileCreated}. Within a window per DB, at most one metadata publish runs (plus a
     * trailing publish), coalescing the checkpoint + S3 list/prune cost under write-heavy load.
     * Values {@code <= 0} disable debouncing (publish on every SST). Default: {@code 1000} ms.
     * Event-driven publishes (delete guard, compaction, DB open) are never debounced.
     */
    private long metadataSyncDebounceMs = 1_000L;

    /**
     * Backlog bound for the metadata-sync debounce: once this many SST uploads accumulate without a
     * metadata publish for a DB, a publish is forced regardless of {@link #metadataSyncDebounceMs},
     * bounding the cloud recovery point by count (not just time) during heavy-ingestion bursts.
     * Values {@code <= 0} disable the count bound (time-only debounce). Default: {@code 32}.
     */
    private int metadataSyncMaxUnpublished = 32;

    /**
     * Stable per-node identity used to derive the cloud key scope ({@code store-<nodeId>}).
     *
     * <p>When set, this is the authoritative scope key and is <b>stable across restarts, IP/hostname
     * changes, and local disk loss</b> — so a node can always find its prior remote objects during
     * recovery. Leave blank to fall back to a scope persisted in the data directory (stable across
     * network-identity drift but lost with the disk), which is in turn seeded from the runtime
     * network address on first start. Operators who need guaranteed post-disk-loss recovery should
     * set an explicit, deployment-stable value here (e.g. a Kubernetes StatefulSet pod ordinal or a
     * provisioned node UUID).
     */
    private String nodeId = "";


    /**
     * Provider-specific properties, mapped from {@code cloud.storage.<provider>.*} in the
     * configuration. These are passed verbatim to the provider implementation and may include
     * credentials, endpoint overrides, regions, or SDK tuning keys expected by that provider.
     */
    private Map<String, String> providerProperties = new HashMap<>();
}
