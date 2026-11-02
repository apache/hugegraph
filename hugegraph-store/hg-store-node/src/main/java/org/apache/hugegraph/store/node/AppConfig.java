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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.hugegraph.rocksdb.access.RocksDBFactory;
import org.apache.hugegraph.store.node.cloud.CloudStorageEventListener;
import org.apache.hugegraph.store.node.cloud.CloudStorageMetrics;
import org.apache.hugegraph.store.node.cloud.CloudSyncTracker;
import org.apache.hugegraph.store.node.cloud.CloudUploadRetryQueue;
import org.apache.hugegraph.store.cloud.CloudStorageConfig;
import org.apache.hugegraph.store.cloud.CloudStorageProvider;
import org.apache.hugegraph.store.options.JobOptions;
import org.apache.hugegraph.store.options.RocksdbMemoryBudget;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
@Component
public class AppConfig {

    private static int cpus = Runtime.getRuntime().availableProcessors();

    @Value("${pdserver.address}")
    private String pdServerAddress;

    @Value("${pd.cluster:hg}")
    private String pdCluster = "hg";

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
    private Environment environment;

    @Autowired(required = false)
    private MeterRegistry meterRegistry;

    /** Provider resolved and initialized during {@link #initCloudStorage()}; closed last on
     *  {@link #onDestroy()}. */
    private volatile CloudStorageProvider cloudStorageProvider;

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
        long totalMemory = RocksdbMemoryBudget.resolve(rocksdb.get(RocksdbMemoryBudget.KEY));
        rocksdb.put(RocksdbMemoryBudget.KEY, Long.toString(totalMemory));
        if (raft.getDisruptorBufferSize() == 0) {
            raft.setDisruptorBufferSize(disruptorBufferSize(totalMemory));
        }

        if (!rocksdb.containsKey("write_buffer_size") ||
            "0".equals(rocksdb.get("write_buffer_size"))) {
            rocksdb.put("write_buffer_size", Long.toString(writeBufferSize(totalMemory)));
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
        CloudStorageConfig cfg = bindCloudStorageConfig(environment);
        if (!cfg.isEnabled()) {
            log.info("Cloud storage disabled (cloud.storage.enabled=false)");
            return;
        }
        try {
            CloudStorageProvider provider = ServiceLoader.load(CloudStorageProvider.class).stream()
                    .map(ServiceLoader.Provider::get)
                    .filter(p -> p.providerName().equals(cfg.getProvider()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(
                            "No cloud storage provider '" + cfg.getProvider()
                            + "' on the classpath"));
            this.cloudStorageProvider = provider;
            provider.init(cfg);
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

            // The first root holds the persisted cloud key-scope marker.
            String primaryDataRoot = resolvedDataRoots.get(0);

            CloudUploadRetryQueue retryQueue = new CloudUploadRetryQueue(
                    provider,
                    cfg.getUploadRetryMaxAttempts(),
                    cfg.getUploadRetryInitialDelayMs(),
                    cfg.getUploadRetryMaxDelayMs(),
                    syncTracker::markConfirmedIfEpoch);
            this.cloudUploadRetryQueue = retryQueue;

            String storeScopePrefix = resolveStableStoreScopePrefix(cfg.getNodeId(),
                                                                    primaryDataRoot);

            CloudStorageEventListener listener = new CloudStorageEventListener(
                    provider,
                    resolvedDataRoots,
                    cfg.isStartupHydrationEnabled(),
                    cfg.getReadMissGuardWindowMs(),
                    retryQueue,
                    syncTracker,
                    storeScopePrefix,
                    cfg.getMetadataSyncDebounceMs());

            // After a retry upload becomes durable, publish CURRENT/MANIFEST so the
            // mirrored recovery point advances even on an idle DB (the tracker-confirm callback
            // alone does not trigger a metadata sync). Wired here since it needs the listener.
            retryQueue.setMetadataSyncTrigger(listener::onRetryUploadDurable);

            // Initialize metrics if MeterRegistry is available
            if (meterRegistry != null) {
                CloudStorageMetrics.init(meterRegistry, syncTracker);
                // Wire the retry-queue-size gauge to the live queue so it reflects the real upload
                // backlog (the gauge otherwise reports a constant 0).
                CloudStorageMetrics.bindRetryQueueSizeSupplier(retryQueue::getInFlightCount);
                CloudStorageMetrics.bindUploadsGivenUpSupplier(retryQueue::getGivenUpCount);
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
                    + "metadataSyncDebounceMs={})",
                    cfg.getProvider(), resolvedDataRoots, storeScopePrefix,
                    cfg.isStartupHydrationEnabled(),
                    cfg.getReadMissGuardWindowMs(),
                    cfg.getUploadRetryMaxAttempts(),
                    cfg.getUploadRetryInitialDelayMs(),
                    cfg.getUploadRetryMaxDelayMs(),
                    cfg.getMetadataSyncDebounceMs());
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
            closeCloudStorageProvider();
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
     * close it (draining its upload executor) so no new SST dispatches occur, then close the retry
     * queue (which drains its own independent scheduler — retries do NOT run in the upload
     * executor), and finally close the provider so in-flight uploads/retries could still use it.
     */
    @PreDestroy
    public void onDestroy() {
        // Deregister the listener first so RocksDB stops dispatching cloud events to it before we
        // tear down the executor/provider it depends on — and so a context restart in the same JVM
        // does not leave a stale listener registered in the static RocksDBFactory listener list
        // (which would double-dispatch events to a defunct instance).
        CloudStorageEventListener listener = this.cloudStorageListener;
        if (listener != null) {
            RocksDBFactory.getInstance().removeRocksdbChangedListener(listener);
            this.cloudStorageListener = null;
            // Shut down the listener's upload executor so no new SST upload tasks are dispatched
            // after this point. close() gives in-flight uploads a chance to complete before the
            // JVM exits; any that don't finish are re-uploaded by startup SST backfill.
            listener.close();
        }

        if (cloudUploadRetryQueue != null) {
            log.info("Shutting down CloudUploadRetryQueue (givenUp={}) …",
                     cloudUploadRetryQueue.getGivenUpCount());
            cloudUploadRetryQueue.close();
        }

        // Close the active provider LAST — after uploads and retries have drained — so its client
        // (e.g. the S3 SDK connection pool and threads) is released rather than leaked on context
        // shutdown/restart. Doing it last ensures in-flight uploads/retries above could still use it.
        closeCloudStorageProvider();
    }

    private void closeCloudStorageProvider() {
        CloudStorageProvider provider = this.cloudStorageProvider;
        if (provider == null) {
            return;
        }
        this.cloudStorageProvider = null;
        try {
            provider.close();
            log.info("Cloud storage provider '{}' closed", provider.providerName());
        } catch (IOException e) {
            log.warn("Error closing cloud storage provider '{}'", provider.providerName(), e);
        }
    }

    /**
     * The raft disruptor buffer size when raft.disruptorBufferSize is 0: one buffer every
     * 32M, from the RocksDB budget in GB, at least 1, rounded to a power of two.
     */
    static int disruptorBufferSize(long totalMemory) {
        // A budget under 1 GB would otherwise give log(0) and a zero-sized buffer
        int size = (int) Math.max(1, totalMemory / 1000 / 1000 / 1000);
        return (int) Math.pow(2, Math.round(Math.log(size) / Math.log(2))) * 32;
    }

    /**
     * The write_buffer_size when the key is unset or 0: a thousandth of the RocksDB budget,
     * but never below the 1 MiB rocksdb.write_buffer_size accepts, which a budget under
     * 1 GiB would otherwise fall under.
     */
    static long writeBufferSize(long totalMemory) {
        return Math.max(totalMemory / 1000, 1024L * 1024L);
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
        @Value("${raft.maxSegmentFileSize:${jraft.log_storage.segment.max.size.bytes:1073741824}}")
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

        @Value("${arthas.ip:127.0.0.1}")
        private String arthasip;

        @Value("${arthas.disabledCommands:}")
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
     * Binds {@code cloud.storage.*} from the Spring {@link Environment} directly onto
     * {@link CloudStorageConfig}, so its field defaults are the only defaults. Relaxed binding
     * applies, so env vars such as {@code CLOUD_STORAGE_ENABLED} resolve as usual.
     *
     * <p>Provider-specific keys are forwarded from the sub-namespace named by
     * {@code cloud.storage.provider}, keeping this class unaware of any specific cloud provider:
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
     *       access-key: ${AWS_ACCESS_KEY_ID:}
     *       secret-key: ${AWS_SECRET_ACCESS_KEY:}
     * </pre>
     *
     * <p>Provider-specific properties are bound only when {@code cloud.storage.enabled=true}: a
     * disabled node must start even if a provider placeholder (e.g. the example above without the
     * trailing {@code :} default) cannot be resolved. Give placeholders an empty default
     * ({@code ${VAR:}}) so an unset variable also does not fail an enabled node at bind time.
     */
    static CloudStorageConfig bindCloudStorageConfig(Environment environment) {
        Binder binder = Binder.get(environment);
        CloudStorageConfig cfg = binder.bind("cloud.storage", CloudStorageConfig.class)
                                       .orElseGet(CloudStorageConfig::new);
        if (cfg.isEnabled()) {
            cfg.setProviderProperties(readProviderProperties(binder, cfg.getProvider()));
        }
        return cfg;
    }

    /**
     * Reads all {@code cloud.storage.<provider>.*} keys and returns them as a flat map with the
     * provider sub-prefix stripped.
     *
     * <p>For example, with {@code provider=s3}, the YAML key {@code cloud.storage.s3.bucket}
     * becomes {@code bucket} in the returned map, and relaxed binding lets the env var
     * {@code CLOUD_STORAGE_S3_ACCESS_KEY} resolve the same way. Multi-word keys bound from a
     * {@code SCREAMING_SNAKE_CASE} source come back dot-separated (e.g. {@code secret.key}), so
     * they are normalized to the kebab-case form ({@code secret-key}) that provider configs such
     * as {@code S3CloudStorageConfig.KEY_*} expect.
     *
     * <p>The same key may arrive in both forms (application.yml {@code secret-key} plus the env
     * var bound as {@code secret.key}); the env-var form wins, matching Spring's precedence of
     * environment variables over application.yml.
     */
    private static Map<String, String> readProviderProperties(Binder binder, String provider) {
        Map<String, String> props = binder
                .bind("cloud.storage." + provider, Bindable.mapOf(String.class, String.class))
                .orElseGet(Collections::emptyMap);
        Map<String, String> normalised = new HashMap<>();
        for (Map.Entry<String, String> e : props.entrySet()) {
            String key = e.getKey().replace('.', '-');
            boolean fromEnvVar = !key.equals(e.getKey());
            if (fromEnvVar || !normalised.containsKey(key)) {
                normalised.put(key, e.getValue());
            }
        }
        return normalised;
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
