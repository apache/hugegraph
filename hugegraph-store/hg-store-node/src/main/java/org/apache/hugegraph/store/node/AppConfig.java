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

package org.apache.hugegraph.store.node;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.hugegraph.rocksdb.access.RocksDBFactory;
import org.apache.hugegraph.store.node.cloud.CloudStorageEventListener;
import org.apache.hugegraph.store.node.cloud.CloudStorageMetrics;
import org.apache.hugegraph.store.node.cloud.CloudSyncTracker;
import org.apache.hugegraph.store.node.cloud.CloudUploadRetryQueue;
import org.apache.hugegraph.store.cloud.CloudStorageConfig;
import org.apache.hugegraph.store.cloud.CloudStorageProviderFactory;
import org.apache.hugegraph.store.options.JobOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
@Component
public class AppConfig {

    private static int cpus = Runtime.getRuntime().availableProcessors();

    @Value("${pdserver.address}")
    private String pdServerAddress;

    @Value("${grpc.host}")
    private String host;

    @Value("${grpc.port}")
    private int grpcPort;

    @Value("${grpc.server.wait-time: 3600}")
    private int serverWaitTime;

    @Value("${server.port}")
    private int restPort;

    // Built-in pd mode, for standalone deployment
    @Value("${app.data-path: store}")
    private String dataPath;

    @Value("${app.placeholder-size: 10}")
    private Integer placeholderSize;

    @Value("${app.raft-path:}")
    private String raftPath;

    // Built-in pd mode, for standalone deployment
    @Value("${app.fake-pd: false}")
    private boolean fakePd;
    @Autowired
    private Raft raft;
    @Autowired
    private ArthasConfig arthasConfig;
    @Autowired
    private FakePdConfig fakePdConfig;
    @Autowired
    private LabelConfig labelConfig;
    @Autowired
    private RocksdbConfig rocksdbConfig;
    @Autowired
    private ThreadPoolGrpc threadPoolGrpc;
    @Autowired
    private ThreadPoolScan threadPoolScan;

    @Autowired
    private JobConfig jobConfig;

    @Autowired
    private QueryPushDownConfig queryPushDownConfig;

    @Autowired
    private CloudStorageSpringConfig cloudStorageSpringConfig;

    @Autowired(required = false)
    private MeterRegistry meterRegistry;

    /** Retry queue created during {@link #initCloudStorage()}; closed on {@link #onDestroy()}. */
    private volatile CloudUploadRetryQueue cloudUploadRetryQueue;

    /** Listener registered with RocksDBFactory during {@link #initCloudStorage()}; deregistered on
     *  {@link #onDestroy()} so a context restart does not leave a stale listener in the static
     *  RocksDBFactory listener list. */
    private volatile CloudStorageEventListener cloudStorageListener;

    public String getRaftPath() {
        if (raftPath == null || raftPath.length() == 0) {
            return dataPath;
        }
        return raftPath;
    }

    @PostConstruct
    public void init() {
        Runtime rt = Runtime.getRuntime();
        if (threadPoolScan.core == 0) {
            threadPoolScan.core = rt.availableProcessors() * 4;
        }

        Map<String, String> rocksdb = rocksdbConfig.rocksdb;
        if (!rocksdb.containsKey("total_memory_size")
            || "0".equals(rocksdb.get("total_memory_size"))) {
            rocksdb.put("total_memory_size", Long.toString(rt.maxMemory()));
        }
        long totalMemory = Long.parseLong(rocksdbConfig.rocksdb.get("total_memory_size"));
        if (raft.getDisruptorBufferSize() == 0) {
            int size = (int) (totalMemory / 1000 / 1000 / 1000);
            size = (int) Math.pow(2, Math.round(Math.log(size) / Math.log(2))) * 32;
            raft.setDisruptorBufferSize(size); // Increase one buffer every 32M
        }

        if (!rocksdb.containsKey("write_buffer_size") ||
            "0".equals(rocksdb.get("write_buffer_size"))) {
            rocksdb.put("write_buffer_size", Long.toString(totalMemory / 1000));
        }

        // ---- Cloud storage initialization ----
        initCloudStorage();
    }

    /**
     * Initialises the cloud storage provider (if enabled) and registers
     * {@link CloudStorageEventListener} with {@link RocksDBFactory} so that
     * SST file creation/deletion events are forwarded to cloud storage.
     *
     * <p>The resolved absolute data-path is passed to the listener so that
     * S3 object keys are relative to the store's storage root rather than
     * being full container-specific absolute paths.
     */
    private void initCloudStorage() {
        CloudStorageConfig cfg = cloudStorageSpringConfig.toCloudStorageConfig();
        if (!cfg.isEnabled()) {
            log.info("Cloud storage disabled (cloud.storage.enabled=false)");
            return;
        }
        try {
            CloudStorageProviderFactory.initialize(cfg);
            // Parse comma-separated dataPath into individual roots. Filter blank tokens so a
            // trailing/duplicate comma (e.g. "store," or "a,,b") does not resolve "" to the JVM
            // working directory and inject it as a bogus data root.
            List<String> resolvedDataRoots = Arrays.stream(dataPath.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .map(path -> Paths.get(path).toAbsolutePath().normalize().toString())
                    .collect(Collectors.toList());
            if (resolvedDataRoots.isEmpty()) {
                throw new IllegalStateException(
                        "Cloud storage enabled but app.data-path resolved to no valid roots: '"
                        + dataPath + "'");
            }

            // Shared sync tracker: the listener's delete guard and the retry queue's success
            // callback both update it, so a superseded cloud object is deleted only once every
            // live SST file of that DB is confirmed present in cloud.
            CloudSyncTracker syncTracker = new CloudSyncTracker();

            // Use the first root for DLQ location (backward compatible; can be any root)
            String primaryDataRoot = resolvedDataRoots.get(0);

            CloudUploadRetryQueue retryQueue = new CloudUploadRetryQueue(
                    cfg.getUploadRetryMaxAttempts(),
                    cfg.getUploadRetryInitialDelayMs(),
                    cfg.getUploadRetryMaxDelayMs(),
                    primaryDataRoot,
                    syncTracker::markConfirmedIfEpoch,
                    cfg.getDlqMaxSize());
            this.cloudUploadRetryQueue = retryQueue;

            String storeScopePrefix = resolveStableStoreScopePrefix(cfg.getNodeId(),
                                                                    primaryDataRoot);

            CloudStorageEventListener.Tuning tuning = CloudStorageEventListener.Tuning.builder()
                    .metadataSyncDebounceMs(cfg.getMetadataSyncDebounceMs())
                    .metadataSyncMaxUnpublished(cfg.getMetadataSyncMaxUnpublished())
                    .build();

            CloudStorageEventListener listener = new CloudStorageEventListener(
                    resolvedDataRoots,
                    cfg.isStartupHydrationEnabled(),
                    cfg.getReadMissGuardWindowMs(),
                    retryQueue,
                    syncTracker,
                    cfg.getUploadBackpressureHighWatermark(),
                    storeScopePrefix,
                    tuning);

            // After a retry / DLQ-replay upload becomes durable, publish CURRENT/MANIFEST so the
            // mirrored recovery point advances even on an idle DB (the tracker-confirm callback
            // alone does not trigger a metadata sync). Wired here since it needs the listener.
            retryQueue.setMetadataSyncTrigger(listener::onRetryUploadDurable);

            // Initialize metrics if MeterRegistry is available
            if (meterRegistry != null) {
                CloudStorageMetrics.init(meterRegistry, syncTracker);
                // Wire the retry-queue-size gauge to the live queue so it reflects the real upload
                // backlog (the gauge otherwise reports a constant 0).
                CloudStorageMetrics.bindRetryQueueSizeSupplier(retryQueue::getInFlightCount);
                // Surface DLQ on-disk persistence health (1=healthy, 0=degraded) so a swallowed
                // persist failure is alertable instead of masquerading as healthy durability.
                CloudStorageMetrics.bindDlqPersistenceHealthySupplier(
                        () -> retryQueue.isDlqPersistenceHealthy() ? 1 : 0);
                // Surface pending-delete marker persistence health (1=healthy, 0=degraded) so a
                // DB delete held for lack of a durable anti-resurrection guard is alertable.
                CloudStorageMetrics.bindDeleteMarkerHealthySupplier(
                        () -> listener.isDeleteMarkerHealthy() ? 1 : 0);
            }

            RocksDBFactory.getInstance().addRocksdbChangedListener(listener);
            this.cloudStorageListener = listener;
            log.info("Cloud storage provider '{}' registered with RocksDBFactory "
                    + "(dataRoots={}, storeScopePrefix='{}', startupHydration={}, "
                    + "readMissHydration=true, "
                    + "readMissGuardWindowMs={}, uploadRetryMaxAttempts={}, "
                    + "uploadRetryInitialDelayMs={}, uploadRetryMaxDelayMs={}, "
                    + "dlqMaxSize={}, metadataSyncDebounceMs={}, metadataSyncMaxUnpublished={})",
                    cfg.getProvider(), resolvedDataRoots, storeScopePrefix,
                    cfg.isStartupHydrationEnabled(),
                    cfg.getReadMissGuardWindowMs(),
                    cfg.getUploadRetryMaxAttempts(),
                    cfg.getUploadRetryInitialDelayMs(),
                    cfg.getUploadRetryMaxDelayMs(),
                    cfg.getDlqMaxSize(),
                    cfg.getMetadataSyncDebounceMs(),
                    cfg.getMetadataSyncMaxUnpublished());
        } catch (Exception e) {
            log.error("Failed to initialize cloud storage provider '{}': {}",
                      cfg.getProvider(), e.getMessage(), e);
            // Release whatever was already started so a failed @PostConstruct does not leak the
            // provider's client/threads — Spring does not invoke @PreDestroy when @PostConstruct
            // throws.
            if (this.cloudUploadRetryQueue != null) {
                try {
                    this.cloudUploadRetryQueue.close();
                } catch (Exception ignore) {
                    // best-effort cleanup on the failure path
                }
                this.cloudUploadRetryQueue = null;
            }
            try {
                CloudStorageProviderFactory.shutdown();
            } catch (Exception ignore) {
                // best-effort cleanup on the failure path
            }
            // Fail startup: an explicitly-enabled provider that cannot initialize leaves
            // the node silently uploading nothing, making durability failures invisible.
            throw new IllegalStateException(
                    "Cloud storage initialization failed for provider '"
                    + cfg.getProvider() + "': " + e.getMessage(), e);
        }
    }

    /** File in the primary data root that persists the resolved cloud key scope across restarts. */
    static final String CLOUD_SCOPE_MARKER_FILE = ".cloud-store-scope";

    /**
     * Resolves the cloud key scope prefix with stability across restarts, giving precedence to the
     * most durable source available:
     *
     * <ol>
     *   <li><b>Configured {@code cloud.storage.node-id}</b> — authoritative and survives IP drift
     *       AND local disk loss (it lives in deployment config), so recovery always finds prior
     *       objects. This is the recommended setting for production.</li>
     *   <li><b>Persisted marker</b> in the primary data root — written on first start, it keeps the
     *       scope stable across network-identity (IP/hostname) drift, though it is lost with the
     *       disk.</li>
     *   <li><b>Runtime network address</b> — legacy behavior, used to seed the marker on first
     *       start. If a node's address later changes it would read a different scope, so we warn
     *       that an explicit node-id is advisable for durable recovery.</li>
     * </ol>
     *
     * <p>Seeding the marker from the network address on first start keeps upgrades migration-safe:
     * an existing deployment whose objects are already keyed by {@code store-<host_port>} resolves
     * to the same scope and keeps finding its data.
     */
    String resolveStableStoreScopePrefix(String configuredNodeId, String primaryDataRoot) {
        if (configuredNodeId != null && !configuredNodeId.trim().isEmpty()) {
            String prefix = "store-" + sanitizeCloudKeySegment(configuredNodeId.trim());
            // Persist so a later removal of the config value still resolves to the same scope.
            persistCloudScopeMarker(primaryDataRoot, prefix);
            log.info("Cloud key scope from configured cloud.storage.node-id: '{}'", prefix);
            return prefix;
        }

        String persisted = readCloudScopeMarker(primaryDataRoot);
        if (persisted != null && !persisted.isEmpty()) {
            log.info("Cloud key scope loaded from persisted marker in data root: '{}'", persisted);
            return persisted;
        }

        // First start (or the marker was lost with the disk): seed from the network identity.
        String prefix = buildIdentityScopePrefix();
        persistCloudScopeMarker(primaryDataRoot, prefix);
        log.warn("Cloud key scope seeded from runtime network address: '{}'. This scope is only "
                 + "stable while the node's address does not change. For guaranteed recovery after "
                 + "an IP/hostname change or local disk loss, set a stable cloud.storage.node-id.",
                 prefix);
        return prefix;
    }

    /** Reads the persisted cloud scope prefix from the data root, or {@code null} if unavailable. */
    private static String readCloudScopeMarker(String primaryDataRoot) {
        if (primaryDataRoot == null || primaryDataRoot.isEmpty()) {
            return null;
        }
        java.nio.file.Path marker = Paths.get(primaryDataRoot, CLOUD_SCOPE_MARKER_FILE);
        try {
            if (!java.nio.file.Files.exists(marker)) {
                return null;
            }
            String content = java.nio.file.Files.readString(marker).trim();
            return content.isEmpty() ? null : content;
        } catch (IOException e) {
            log.warn("Failed to read cloud scope marker {}: {}", marker, e.getMessage());
            return null;
        }
    }

    /** Best-effort persist of the resolved cloud scope prefix into the data root. */
    private static void persistCloudScopeMarker(String primaryDataRoot, String prefix) {
        if (primaryDataRoot == null || primaryDataRoot.isEmpty()) {
            return;
        }
        java.nio.file.Path marker = Paths.get(primaryDataRoot, CLOUD_SCOPE_MARKER_FILE);
        try {
            java.nio.file.Files.createDirectories(marker.getParent());
            String existing = readCloudScopeMarker(primaryDataRoot);
            if (prefix.equals(existing)) {
                return; // already persisted
            }
            java.nio.file.Files.writeString(marker, prefix,
                    java.nio.file.StandardOpenOption.CREATE,
                    java.nio.file.StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            log.warn("Failed to persist cloud scope marker {}: {} — scope stability across restarts "
                     + "is not guaranteed until this succeeds", marker, e.getMessage());
        }
    }

    /**
     * Builds a deterministic per-store cloud key prefix from the runtime network identity so
     * distributed store nodes can share a bucket/path-prefix without key collisions. Used only to
     * seed {@link #resolveStableStoreScopePrefix} on first start.
     */
    private String buildIdentityScopePrefix() {
        String identity = raft != null ? raft.getAddress() : null;
        if (identity == null || identity.trim().isEmpty()) {
            identity = getStoreServerAddress();
        }
        return "store-" + sanitizeCloudKeySegment(identity);
    }

    /** Converts an address-like identifier into a cloud-key-safe path segment. */
    private static String sanitizeCloudKeySegment(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return "unknown";
        }
        StringBuilder sb = new StringBuilder(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char ch = raw.charAt(i);
            if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
                (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '.') {
                sb.append(ch);
            } else {
                sb.append('_');
            }
        }
        return sb.toString();
    }

    /**
     * Gracefully tears down cloud storage on application stop. Order: deregister the listener, then
     * stop the shared upload executor so no new SST dispatches occur, then close the retry queue
     * (which drains its own independent scheduler — retries do NOT run in the shared executor), and
     * finally close the provider so in-flight uploads/retries could still use it until the end.
     */
    @PreDestroy
    public void onDestroy() {
        // Deregister the listener first so RocksDB stops dispatching cloud events to it before we
        // tear down the executor/provider it depends on — and so a context restart in the same JVM
        // does not leave a stale listener registered in the static RocksDBFactory listener list
        // (which would double-dispatch events to a defunct instance).
        if (this.cloudStorageListener != null) {
            RocksDBFactory.getInstance().removeRocksdbChangedListener(this.cloudStorageListener);
            this.cloudStorageListener = null;
        }

        // Shut down the shared upload executor so no new SST upload tasks are dispatched
        // after this point. awaitTermination gives in-flight uploads a chance to complete
        // before the JVM exits; any that don't finish will be absent from cloud with no DLQ
        // entry if the process is killed, but at least we tried.
        CloudStorageEventListener.shutdownSharedUploadExecutor(10, java.util.concurrent.TimeUnit.SECONDS);

        if (cloudUploadRetryQueue != null) {
            log.info("Shutting down CloudUploadRetryQueue (dlqSize={}) …",
                     cloudUploadRetryQueue.getDlqSize());
            cloudUploadRetryQueue.close();
        }

        // Close the active provider LAST — after uploads and retries have drained — so its client
        // (e.g. the S3 SDK connection pool and threads) is released rather than leaked on context
        // shutdown/restart. Doing it last ensures in-flight uploads/retries above could still use it.
        CloudStorageProviderFactory.shutdown();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("AppConfig \n")
               .append("rocksdb:\n");
        rocksdbConfig.rocksdb.forEach((k, v) -> builder.append("\t" + k + ":")
                                                       .append(v)
                                                       .append("\n"));
        builder.append("raft:\n");
        builder.append("\tdisruptorBufferSize: " + raft.disruptorBufferSize);
        return builder.toString();
    }

    public String getStoreServerAddress() {
        return String.format("%s:%d", host, grpcPort);
    }

    public Map<String, Object> getRocksdbConfig() {
        Map<String, Object> config = new HashMap<>();
        rocksdbConfig.rocksdb.forEach((k, v) -> {
            config.put("rocksdb." + k, v);
        });
        return config;
    }

    @Data
    @Configuration
    public class ThreadPoolGrpc {

        @Value("${thread.pool.grpc.core:600}")
        private int core;
        @Value("${thread.pool.grpc.max:1000}")
        private int max;
        @Value("${thread.pool.grpc.queue:" + Integer.MAX_VALUE + "}")
        private int queue;
    }

    @Data
    @Configuration
    public class ThreadPoolScan {

        @Value("${thread.pool.scan.core: 128}")
        private int core;
        @Value("${thread.pool.scan.max: 1000}")
        private int max;
        @Value("${thread.pool.scan.queue: 0}")
        private int queue;
    }

    @Data
    @Configuration
    public class Raft {

        @Value("${raft.address}")
        private String address;

        @Value("${raft.rpc-timeout:10000}")
        private int rpcTimeOut;
        @Value("${raft.metrics:true}")
        private boolean metrics;
        @Value("${raft.snapshotLogIndexMargin:0}")
        private int snapshotLogIndexMargin;
        @Value("${raft.snapshotInterval:300}")
        private int snapshotInterval;
        @Value("${raft.disruptorBufferSize:0}")
        private int disruptorBufferSize;
        @Value("${raft.max-log-file-size:50000000000}")
        private long maxLogFileSize;
        @Value("${ave-logEntry-size-ratio:0.95}")
        private double aveLogEntrySizeRation;
        @Value("${raft.useRocksDBSegmentLogStorage:true}")
        private boolean useRocksDBSegmentLogStorage;
        @Value("${raft.maxSegmentFileSize:67108864}")
        private int maxSegmentFileSize;
        @Value("${raft.maxReplicatorInflightMsgs:256}")
        private int maxReplicatorInflightMsgs;
        @Value("${raft.maxEntriesSize:256}")
        private int maxEntriesSize;
        @Value("${raft.maxBodySize:524288}")
        private int maxBodySize;

    }

    @Data
    @Configuration
    public class ArthasConfig {

        @Value("${arthas.telnetPort:8566}")
        private String telnetPort;

        @Value("${arthas.httpPort:8565}")
        private String httpPort;

        @Value("${arthas.ip:0.0.0.0}")
        private String arthasip;

        @Value("${arthas.disabledCommands:jad}")
        private String disCmd;
    }

    @Data
    @Configuration
    public class FakePdConfig {

        @Value("${fake-pd.store-list:''}")
        private String storeList;
        @Value("${fake-pd.peers-list:''}")
        private String peersList;   // fakePd mode, raft cluster initial configuration
        @Value("${fake-pd.partition-count:3}")
        private int partitionCount;
        @Value("${fake-pd.shard-count:3}")
        private int shardCount;
    }

    @Data
    @Configuration
    public class JobConfig {

        @Value("${job.interruptableThreadPool.core:128}")
        private int core;

        @Value("${job.interruptableThreadPool.max:256}")
        private int max;

        @Value("${job.interruptableThreadPool.queue:" + Integer.MAX_VALUE + "}")
        private int queueSize;

        @Value("${job.cleaner.batch.size:10000}")
        private int batchSize;

        @Value("${job.start-time:0}")
        private int startTime;

        @Value("${job.uninterruptibleThreadPool.core:0}")
        private int uninterruptibleCore;

        @Value("${job.uninterruptibleThreadPool.max:256}")
        private int uninterruptibleMax;

        @Value("${job.uninterruptibleThreadPool.queue:" + Integer.MAX_VALUE + "}")
        private int uninterruptibleQueueSize;
    }

    @Data
    @Configuration
    public class QueryPushDownConfig {

        /**
         * query v2 thread pool size
         */
        @Value("${query.push-down.threads:1500}")
        private int threadPoolSize;

        /**
         * the batch size that each request gets
         */
        @Value("${query.push-down.fetch_batch:20000}")
        private int fetchBatchSize;

        /**
         * the timeout of request fetch (ms)
         */
        @Value("${query.push-down.fetch_timeout:300000}")
        private long fetchTimeOut;

        /**
         * the limit of memory operations, like sort etc.
         */
        @Value("${query.push-down.memory_limit_count:50000}")
        private int memoryLimitCount;

        /**
         * limit size of index sst file size (kB)
         */
        @Value("${query.push-down.index_size_limit_count:50000}")
        private int indexSizeLimitCount;
    }

    @Data
    @Configuration
    @ConfigurationProperties(prefix = "app")
    public class LabelConfig {

        private final Map<String, String> label = new HashMap<>();
    }

    @Data
    @Configuration
    @ConfigurationProperties(prefix = "")
    public class RocksdbConfig {

        private Map<String, String> rocksdb = new HashMap<>();
    }

    /**
     * Spring {@link ConfigurationProperties} wrapper for the common cloud storage properties.
     *
     * <p>Provider-specific keys (e.g. {@code cloud.storage.s3.bucket}) are read
     * directly from the Spring {@link Environment} at conversion time, so this class
     * has zero knowledge of any specific cloud provider.
     *
     * <pre>
     * cloud:
     *   storage:
     *     enabled: true
     *     provider: s3          # selects which sub-namespace to forward to the provider
     *     path-prefix: hugegraph
     *     s3:                   # all keys here are forwarded as-is to the S3 provider
     *       bucket: my-bucket
     *       region: us-east-1
     *       access-key: ${AWS_ACCESS_KEY_ID}
     *       secret-key: ${AWS_SECRET_ACCESS_KEY}
     * </pre>
     */
    @Data
    @Configuration
    @ConfigurationProperties(prefix = "cloud.storage")
    public class CloudStorageSpringConfig {

        private boolean enabled = false;
        private String provider = "s3";
        private String pathPrefix = "hugegraph";
        private boolean startupHydrationEnabled = true;
        private long readMissGuardWindowMs = 3000L;
        // Whole-file upload retries after a first failure. Default 3 under the primary-durability
        private int uploadRetryMaxAttempts = 3;
        private long uploadRetryInitialDelayMs = 1_000L;
        private long uploadRetryMaxDelayMs = 60_000L;
        // Backpressure high-watermark on the pending-upload backlog; 0 (default) disables it.
        // Opt-in: when > 0 the throttle parks RocksDB's flush/compaction thread (up to 30s/event),
        // which under a sustained cloud outage can stall memtable flushes / stop writes.
        private int uploadBackpressureHighWatermark = 0;
        // Max DLQ entries before oldest are evicted (bounds memory/disk under a prolonged outage).
        private int dlqMaxSize = 100_000;
        // Debounce window (ms) for the per-SST metadata sync; <= 0 disables debouncing.
        private long metadataSyncDebounceMs = 1_000L;
        // Force a metadata publish once this many SST uploads accumulate unmirrored; <= 0 disables.
        private int metadataSyncMaxUnpublished = 32;
        // Stable per-node identity for the cloud key scope. Blank => persisted-in-data-dir scope
        // (seeded from network address). Set for guaranteed recovery after IP drift / disk loss.
        private String nodeId = "";

        /**
         * Injected by Spring; used to read {@code cloud.storage.<provider>.*} properties
         * without coupling this class to any specific provider.
         */
        @Autowired
        @EqualsAndHashCode.Exclude
        @ToString.Exclude
        private Environment environment;

        /** Converts this Spring-bound config into a plain {@link CloudStorageConfig} POJO. */
        public CloudStorageConfig toCloudStorageConfig() {
            CloudStorageConfig cfg = new CloudStorageConfig();
            cfg.setEnabled(enabled);
            cfg.setProvider(provider);
            cfg.setPathPrefix(pathPrefix);
            cfg.setStartupHydrationEnabled(startupHydrationEnabled);
            cfg.setReadMissGuardWindowMs(readMissGuardWindowMs);
            cfg.setUploadRetryMaxAttempts(uploadRetryMaxAttempts);
            cfg.setUploadRetryInitialDelayMs(uploadRetryInitialDelayMs);
            cfg.setUploadRetryMaxDelayMs(uploadRetryMaxDelayMs);
            cfg.setUploadBackpressureHighWatermark(uploadBackpressureHighWatermark);
            cfg.setDlqMaxSize(dlqMaxSize);
            cfg.setMetadataSyncDebounceMs(metadataSyncDebounceMs);
            cfg.setMetadataSyncMaxUnpublished(metadataSyncMaxUnpublished);
            cfg.setNodeId(nodeId);
            cfg.setProviderProperties(readProviderProperties());
            return cfg;
        }

        /**
         * Reads all {@code cloud.storage.<provider>.*} keys from the Spring Environment
         * and returns them as a flat map with the provider sub-prefix stripped.
         *
         * <p>For example, with {@code provider=s3}, the YAML key
         * {@code cloud.storage.s3.bucket} becomes {@code bucket} in the returned map.
         */
        private Map<String, String> readProviderProperties() {
            Map<String, String> props = new LinkedHashMap<>();
            if (!(environment instanceof AbstractEnvironment)) {
                return props;
            }
            String prefix = "cloud.storage." + provider + ".";
            ((AbstractEnvironment) environment).getPropertySources().stream()
                    .filter(ps -> ps instanceof EnumerablePropertySource)
                    .map(ps -> (EnumerablePropertySource<?>) ps)
                    .flatMap(ps -> Arrays.stream(ps.getPropertyNames()))
                    .filter(key -> key.startsWith(prefix))
                    .distinct()
                    .forEach(key -> {
                        String shortKey = key.substring(prefix.length());
                        String value = environment.getProperty(key);
                        if (value != null) {
                            props.put(shortKey, value);
                        }
                    });
            return props;
        }
    }

    public JobOptions getJobOptions() {
        JobOptions jobOptions = new JobOptions();
        jobOptions.setCore(jobConfig.getCore() == 0 ? cpus : jobConfig.getCore());
        jobOptions.setMax(jobConfig.getMax() == 0 ? cpus * 4 : jobConfig.getMax());
        jobOptions.setQueueSize(jobConfig.getQueueSize());
        jobOptions.setBatchSize(jobConfig.getBatchSize());
        int uninterruptibleCore = jobOptions.getUninterruptibleCore();
        jobOptions.setUninterruptibleCore(uninterruptibleCore == 0 ? cpus : uninterruptibleCore);
        int uninterruptibleMax = jobOptions.getUninterruptibleMax();
        jobOptions.setUninterruptibleMax(uninterruptibleMax == 0 ? cpus * 4 : uninterruptibleMax);
        jobOptions.setUninterruptibleQueueSize(jobConfig.getUninterruptibleQueueSize());
        return jobOptions;
    }
}
