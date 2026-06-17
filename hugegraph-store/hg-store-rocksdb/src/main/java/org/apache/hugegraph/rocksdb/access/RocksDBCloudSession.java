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

package org.apache.hugegraph.rocksdb.access;

import java.net.URI;
import java.util.Objects;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.rocksdb.access.cloud.S3Util;
import org.apache.hugegraph.store.term.HgPair;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;

@Slf4j
public class RocksDBCloudSession extends RocksDBSession {

    private static final String KEY_BUCKET = "rocksdb.cloud.s3_bucket";
    private static final String KEY_BUCKET_LEGACY = "rocksdb.cloud_s3_bucket";

    private static final String KEY_ENDPOINT = "rocksdb.cloud.s3_endpoint";
    private static final String KEY_ENDPOINT_LEGACY = "rocksdb.cloud_s3_endpoint";

    private static final String KEY_REGION = "rocksdb.cloud.s3_region";
    private static final String KEY_REGION_LEGACY = "rocksdb.cloud_s3_region";

    private static final String KEY_ACCESS_KEY = "rocksdb.cloud.s3_access_key";
    private static final String KEY_ACCESS_KEY_LEGACY = "rocksdb.cloud_s3_access_key";

    private static final String KEY_SECRET_KEY = "rocksdb.cloud.s3_secret_key";
    private static final String KEY_SECRET_KEY_LEGACY = "rocksdb.cloud_s3_secret_key";

    private static final String KEY_PATH_STYLE = "rocksdb.cloud.s3_path_style";
    private static final String KEY_PATH_STYLE_LEGACY = "rocksdb.cloud_s3_path_style";

    private static final String KEY_PREFIX = "rocksdb.cloud.s3_object_prefix";
    private static final String KEY_PREFIX_LEGACY = "rocksdb.cloud_s3_object_prefix";

    private static final String KEY_SYNC_INTERVAL = "rocksdb.cloud.sync_interval_seconds";
    private static final String KEY_SYNC_INTERVAL_LEGACY =
            "rocksdb.cloud_sync_interval_seconds";

    private static final String KEY_SYNC_INCREMENTAL = "rocksdb.cloud.sync_incremental";
    private static final String KEY_SYNC_INCREMENTAL_LEGACY =
            "rocksdb.cloud_sync_incremental";

     private static final String KEY_S3_FIRST_MODE = "rocksdb.cloud.s3_first_mode";
     private static final String KEY_S3_FIRST_MODE_LEGACY = "rocksdb.cloud_s3_first_mode";

     private static final String KEY_SYNC_RETRY_MAX = "rocksdb.cloud.sync_retry_max";
     private static final String KEY_SYNC_RETRY_MAX_LEGACY = "rocksdb.cloud_sync_retry_max";

     private static final String KEY_SYNC_RETRY_BACKOFF_MS = "rocksdb.cloud.sync_retry_backoff_ms";
     private static final String KEY_SYNC_RETRY_BACKOFF_MS_LEGACY = "rocksdb.cloud_sync_retry_backoff_ms";

     private static final String KEY_SYNC_RETRY_MAX_BACKOFF_MS = "rocksdb.cloud.sync_retry_max_backoff_ms";
     private static final String KEY_SYNC_RETRY_MAX_BACKOFF_MS_LEGACY = "rocksdb.cloud_sync_retry_max_backoff_ms";

     private static final ScheduledExecutorService SYNC_SCHEDULER =
            Executors.newScheduledThreadPool(1, r -> {
                Thread t = new Thread(r, "store-rocksdb-cloud-sync");
                t.setDaemon(true);
                return t;
            });

     private final S3Client s3Client;
     private final String bucket;
     private final String objectPrefix;
     private final int syncIntervalSeconds;
     private final boolean syncIncremental;
     private final boolean s3FirstMode;
     private final int syncRetryMax;
     private final int syncRetryBackoffMs;
     private final int syncRetryMaxBackoffMs;

     private final AtomicBoolean syncInProgress = new AtomicBoolean(false);
     private final AtomicBoolean hydrationInProgress = new AtomicBoolean(false);

    private ScheduledFuture<?> periodicSyncFuture;

    public RocksDBCloudSession(HugeConfig hugeConfig, String dbDataPath,
                                String graphName, long version) {
        super(hugeConfig, dbDataPath, graphName, version);

        boolean cloudEnabled = getBoolean(hugeConfig, "rocksdb.cloud.enabled",
                                          "rocksdb.cloud_enabled", true);
         if (!cloudEnabled) {
             log.warn("RocksDBCloudSession is initialized while cloud sync is disabled for graph {}",
                      graphName);
         }

        this.s3Client = buildS3Client(hugeConfig);

        this.bucket = getString(hugeConfig, KEY_BUCKET, KEY_BUCKET_LEGACY,
                                "hugegraph-rocksdb");
        String basePrefix = getString(hugeConfig, KEY_PREFIX, KEY_PREFIX_LEGACY,
                                      "store");
        this.objectPrefix = normalizedPrefix(basePrefix, graphName);

         this.syncIntervalSeconds = getInt(hugeConfig, KEY_SYNC_INTERVAL,
                                           KEY_SYNC_INTERVAL_LEGACY, 60);
         this.syncIncremental = getBoolean(hugeConfig, KEY_SYNC_INCREMENTAL,
                                           KEY_SYNC_INCREMENTAL_LEGACY, true);
         this.s3FirstMode = getBoolean(hugeConfig, KEY_S3_FIRST_MODE,
                                       KEY_S3_FIRST_MODE_LEGACY, false);
         this.syncRetryMax = getInt(hugeConfig, KEY_SYNC_RETRY_MAX,
                                    KEY_SYNC_RETRY_MAX_LEGACY, 100);
         this.syncRetryBackoffMs = getInt(hugeConfig, KEY_SYNC_RETRY_BACKOFF_MS,
                                          KEY_SYNC_RETRY_BACKOFF_MS_LEGACY, 10);
         this.syncRetryMaxBackoffMs = getInt(hugeConfig, KEY_SYNC_RETRY_MAX_BACKOFF_MS,
                                             KEY_SYNC_RETRY_MAX_BACKOFF_MS_LEGACY, 1000);

         startPeriodicSync();
         log.info("RocksDB cloud enabled for graph {}: s3://{}/{}, interval={}s, " +
                  "incremental={}, s3_first_mode={}, retry_max={}, " +
                  "retry_backoff_ms={}, retry_max_backoff_ms={}",
                  graphName, this.bucket, this.objectPrefix,
                  this.syncIntervalSeconds, this.syncIncremental, this.s3FirstMode,
                  this.syncRetryMax, this.syncRetryBackoffMs, this.syncRetryMaxBackoffMs);
    }

    @Override
    public SessionOperator sessionOp() {
        return new CloudSessionOperator(this);
    }

     void syncNow(boolean fullSync, boolean forceFlush) {
         // Acquire syncInProgress lock with retries and exponential backoff.
         // If forceFlush=true (commit-time), block/retry until acquired.
         // If forceFlush=false (periodic), skip if already locked.
         for (int attempt = 0; attempt < this.syncRetryMax; attempt++) {
             if (this.syncInProgress.compareAndSet(false, true)) {
                 break;  // Successfully acquired lock
             }
             // Lock not acquired
             if (!forceFlush) {
                 // Best-effort periodic reconcile skips if another sync in progress
                 return;
             }
             // Commit-time fence (forceFlush=true) must block and retry
             if (attempt < this.syncRetryMax - 1) {
                 long backoffMs = Math.min(
                     this.syncRetryBackoffMs * (1L << Math.min(attempt, 5)),
                     this.syncRetryMaxBackoffMs
                 );
                 try {
                     Thread.sleep(backoffMs);
                 } catch (InterruptedException e) {
                     Thread.currentThread().interrupt();
                     throw new DBStoreException(
                         "Interrupted while waiting for commit-time cloud sync at attempt " + attempt
                     );
                 }
             }
         }

         // If we exit the loop without acquiring lock and still locked, fail
         if (!this.syncInProgress.get()) {
             throw new DBStoreException(
                 "Failed to acquire syncInProgress lock after " + this.syncRetryMax + " attempts"
             );
         }

         try {
             if (forceFlush) {
                 flush(true);
             }
             String s3Prefix = this.objectPrefix + "data/";
             String localPath = getDbPath();
             if (fullSync || !this.syncIncremental) {
                 S3Util.uploadDirectory(this.s3Client, this.bucket, s3Prefix, localPath);
             } else {
                 S3Util.uploadIncremental(this.s3Client, this.bucket, s3Prefix, localPath);
             }
         } finally {
             this.syncInProgress.set(false);
         }
     }

    void rehydrateForRead() {
        if (!this.hydrationInProgress.compareAndSet(false, true)) {
            return;
        }
        try {
            String s3Prefix = this.objectPrefix + "data/";
            String localPath = getDbPath();
            log.warn("Attempt read-path hydration for graph {} from s3://{}/{}",
                     getGraphName(), this.bucket, s3Prefix);
            S3Util.downloadDirectory(this.s3Client, this.bucket, s3Prefix, localPath);
            reload(0L);
            log.warn("Read-path hydration finished for graph {}", getGraphName());
        } finally {
            this.hydrationInProgress.set(false);
        }
    }

    private static boolean nonRecoverableReadError(Throwable t) {
         if (t == null) {
            return true;
         }
         String msg = String.valueOf(t.getMessage()).toLowerCase(Locale.ROOT);
         return !(msg.contains("no such file") ||
                 msg.contains("not found") ||
                 msg.contains("sst") ||
                 msg.contains("corrupt") ||
                 msg.contains("checksum") ||
                 msg.contains("io error"));
     }

    @Override
    void shutdown() {
        stopPeriodicSync();
        try {
            syncNow(true, true);
        } catch (Throwable t) {
            log.warn("Failed to sync db {} to S3 on close: {}",
                     getGraphName(), t.getMessage());
        }
        super.shutdown();
    }

    private void startPeriodicSync() {
        if (this.syncIntervalSeconds <= 0) {
            return;
        }
        this.periodicSyncFuture = SYNC_SCHEDULER.scheduleAtFixedRate(() -> {
            try {
                // Reconcile to cloud from already-generated SST files only.
                syncNow(false, false);
            } catch (Throwable t) {
                log.warn("Periodic cloud sync failed for {}: {}",
                         getGraphName(), t.getMessage());
            }
        }, this.syncIntervalSeconds, this.syncIntervalSeconds, TimeUnit.SECONDS);
    }

    private void stopPeriodicSync() {
        if (this.periodicSyncFuture != null && !this.periodicSyncFuture.isCancelled()) {
            this.periodicSyncFuture.cancel(false);
        }
    }

    private static S3Client buildS3Client(HugeConfig config) {
        String endpoint = getString(config, KEY_ENDPOINT, KEY_ENDPOINT_LEGACY, "");
        String region = getString(config, KEY_REGION, KEY_REGION_LEGACY, "us-east-1");
        String accessKey = getString(config, KEY_ACCESS_KEY, KEY_ACCESS_KEY_LEGACY, "");
        String secretKey = getString(config, KEY_SECRET_KEY, KEY_SECRET_KEY_LEGACY, "");
        boolean pathStyle = getBoolean(config, KEY_PATH_STYLE, KEY_PATH_STYLE_LEGACY, false);

        AwsCredentialsProvider credentialsProvider;
        if (!accessKey.isEmpty() && !secretKey.isEmpty()) {
            credentialsProvider = StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey));
        } else {
            credentialsProvider = DefaultCredentialsProvider.create();
        }

        S3ClientBuilder builder = S3Client.builder()
                                          .region(Region.of(region))
                                          .credentialsProvider(credentialsProvider);
        if (!endpoint.isEmpty()) {
            builder.endpointOverride(URI.create(endpoint));
        }
        if (pathStyle) {
            builder.serviceConfiguration(
                    S3Configuration.builder().pathStyleAccessEnabled(true).build());
        }
        return builder.build();
    }

    private static String normalizedPrefix(String basePrefix, String graphName) {
        String trimmed = Objects.requireNonNullElse(basePrefix, "").trim();
        if (trimmed.isEmpty()) {
            return graphName + "/";
        }
        String withoutLeading = trimmed.startsWith("/") ?
                                trimmed.substring(1) :
                                trimmed;
        String normalized = withoutLeading.endsWith("/") ?
                            withoutLeading :
                            withoutLeading + "/";
        return normalized + graphName + "/";
    }

    private static String getString(HugeConfig conf, String key,
                                    String legacyKey, String defaultValue) {
        String value = null;
        if (conf.containsKey(key)) {
            value = String.valueOf(conf.getProperty(key));
        } else if (conf.containsKey(legacyKey)) {
            value = String.valueOf(conf.getProperty(legacyKey));
        }
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        return value.trim();
    }

    private static boolean getBoolean(HugeConfig conf, String key,
                                      String legacyKey, boolean defaultValue) {
        return Boolean.parseBoolean(getString(conf, key, legacyKey, String.valueOf(defaultValue)));
    }

    private static int getInt(HugeConfig conf, String key,
                              String legacyKey, int defaultValue) {
        return Integer.parseInt(
                getString(conf, key, legacyKey, String.valueOf(defaultValue)).trim());
    }

     private static final class CloudSessionOperator extends SessionOperatorImpl {

        private final RocksDBCloudSession cloudSession;

        private CloudSessionOperator(RocksDBCloudSession session) {
            super(session);
            this.cloudSession = session;
        }

        @FunctionalInterface
        private interface Op<T> {
            T run() throws DBStoreException;
        }

        private <T> T withReadHydrationRetry(Op<T> primary, Op<T> retry) throws DBStoreException {
            try {
                return primary.run();
            } catch (DBStoreException e) {
                if (nonRecoverableReadError(e)) {
                     throw e;
                 }
                 log.warn("Read failed, attempting S3 hydration for {}: {}",
                          this.cloudSession.getGraphName(), e.getMessage());
                 this.cloudSession.rehydrateForRead();
                 return retry.run();
             }
         }

        @Override
        public Integer commit() throws DBStoreException {
            Integer count = super.commit();
            if (count != null && count > 0) {
                if (this.cloudSession.s3FirstMode) {
                    // In S3-first mode, sync before acknowledging commit to caller.
                    this.cloudSession.syncNow(false, true);
                }
            }
            return count;
        }

        @Override
        public byte[] get(String table, byte[] key) throws DBStoreException {
            return withReadHydrationRetry(
                    () -> super.get(table, key),
                    () -> new SessionOperatorImpl(this.cloudSession).get(table, key)
            );
        }

        @Override
        public ScanIterator scan(String tableName) {
            try {
                return super.scan(tableName);
            } catch (RuntimeException e) {
                if (nonRecoverableReadError(e)) {
                     throw e;
                 }
                 this.cloudSession.rehydrateForRead();
                 return new SessionOperatorImpl(this.cloudSession).scan(tableName);
             }
         }

        @Override
        public ScanIterator scan(String tableName, byte[] prefix) {
            try {
                return super.scan(tableName, prefix);
            } catch (RuntimeException e) {
                if (nonRecoverableReadError(e)) {
                     throw e;
                 }
                 this.cloudSession.rehydrateForRead();
                 return new SessionOperatorImpl(this.cloudSession).scan(tableName, prefix);
             }
         }

        @Override
        public ScanIterator scan(String tableName, byte[] prefix, int scanType) {
            try {
                return super.scan(tableName, prefix, scanType);
            } catch (RuntimeException e) {
                if (nonRecoverableReadError(e)) {
                     throw e;
                 }
                 this.cloudSession.rehydrateForRead();
                 return new SessionOperatorImpl(this.cloudSession).scan(tableName, prefix, scanType);
             }
         }

        @Override
        public ScanIterator scan(String tableName, byte[] keyFrom, byte[] keyTo, int scanType) {
            try {
                return super.scan(tableName, keyFrom, keyTo, scanType);
            } catch (RuntimeException e) {
                if (nonRecoverableReadError(e)) {
                     throw e;
                 }
                 this.cloudSession.rehydrateForRead();
                 return new SessionOperatorImpl(this.cloudSession).scan(tableName, keyFrom, keyTo,
                                                                        scanType);
             }
         }

        @Override
        public ScanIterator scanRaw(byte[] keyFrom, byte[] keyTo, long startSeqNum) {
            try {
                return super.scanRaw(keyFrom, keyTo, startSeqNum);
            } catch (RuntimeException e) {
                if (nonRecoverableReadError(e)) {
                     throw e;
                 }
                 this.cloudSession.rehydrateForRead();
                 return new SessionOperatorImpl(this.cloudSession).scanRaw(keyFrom, keyTo,
                                                                            startSeqNum);
             }
         }

        @Override
        public HgPair<byte[], byte[]> keyRange(String table) {
            try {
                return super.keyRange(table);
            } catch (RuntimeException e) {
                if (nonRecoverableReadError(e)) {
                     throw e;
                 }
                 this.cloudSession.rehydrateForRead();
                 return new SessionOperatorImpl(this.cloudSession).keyRange(table);
             }
         }

        @Override
        public long estimatedKeyCount(String tableName) throws DBStoreException {
            return withReadHydrationRetry(
                    () -> super.estimatedKeyCount(tableName),
                    () -> new SessionOperatorImpl(this.cloudSession).estimatedKeyCount(tableName)
            );
        }
    }
}
