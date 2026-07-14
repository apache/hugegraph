# Pluggable Cloud Storage Architecture (HStore)

This document explains:

- The existing HStore workflow (without cloud storage)
- The new pluggable cloud storage workflow (cloud-backed SST lifecycle)
- Write path and read path behavior
- Failure handling and operational recovery scenarios

## Overview

HStore keeps partition data in local RocksDB on each Store node. The pluggable cloud-storage
feature extends this by mirroring SST file lifecycle events (create/delete) to an external
object store and hydrating missing files back when needed (startup and read-miss).

- Existing path: local RocksDB is the primary online read/write path.
- New extension: cloud object storage acts as a pluggable SST mirror and recovery source.
- Provider model: runtime-selected SPI provider via `CloudStorageProviderFactory`.

## Architecture Diagram

```text
+---------------------------+
| Client Layer              |
| Gremlin/REST/Cypher       |
+-------------|-------------+
              v
+---------------------------+   +----------------+
| HugeGraph Server          |-->| PD Cluster     |			
+-------------|-------------+   +----------------+
              v
+----------------------------------------------+
| Store Cluster (Raft replication)             |
|                                              |
| +-----------------------------------------+  |
| | WAL + MemTable -> Local RocksDB SST     |  |
| +--------------------+--------------------+  |
|                      |                       |
|                      v                       |
| +-----------------------------------------+  |
| | Pluggable Cloud Storage Workflow (NEW)  |  |
| | CloudStorageEventListener (NEW)         |  |
| | -> CloudStorageProviderFactory (NEW)    |  |
| | -> CloudStorageProvider (NEW)           |  |
| +-----------------------------------------+  |
+----------------------|-----------------------+
                       v                                                  
        +-------------------------------+                                   
        | Cloud Storage (NEW)           |                                   
        | +-------------+ +----+ +----+ |                             
        | |S3 compatible| |ADLS| |GCP | |                              
        | +-------------+ +----+ +----+ |  
        +-------------------------------+                                

```

- `Client Layer`: External clients issuing Gremlin/REST/Cypher read-write requests.
- `HugeGraph Server`: API/query layer that routes graph requests to PD and Store.
- `PD Cluster`: Placement/metadata control plane (partition mapping, leader scheduling).
- `Store Cluster`: Raft-based data plane where writes are replicated and persisted.
- `WAL + MemTable -> Local RocksDB SST`: Local durability and compaction pipeline in each Store node.
- `Pluggable Cloud Storage Workflow (NEW)`: SST lifecycle hook path for upload/delete/download/list.
- `CloudStorageEventListener (NEW)`: Captures RocksDB file events and triggers cloud operations.
- `CloudStorageProviderFactory (NEW)`: SPI loader that selects and initializes the active provider.
- `CloudStorageProvider (NEW)`: Provider implementation (`s3`, etc.) used for object operations.
- `Cloud Storage (NEW)`: Remote object backend options (S3 compatible, ADSL, GCP).


## New Configuration Options

The pluggable cloud-storage behavior is controlled from `application.yml` under
`cloud.storage`.

| Configuration                                        | Default     | Description                                                                                                                                                                                                                                                                                                                                                                                    |
|------------------------------------------------------|-------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `cloud.storage.enabled`                              | `false`     | Enables/disables cloud storage integration.                                                                                                                                                                                                                                                                                                                                                    |
| `cloud.storage.provider`                             | `s3`        | Active provider name. Must match `CloudStorageProvider#providerName()`.                                                                                                                                                                                                                                                                                                                        |
| `cloud.storage.path-prefix`                          | `hugegraph` | Prefix prepended to all remote object keys.                                                                                                                                                                                                                                                                                                                                                    |
| `cloud.storage.startup-hydration-enabled`            | `true`      | Downloads missing remote files on DB opening before normal serving.                                                                                                                                                                                                                                                                                                                            |
| `cloud.storage.read-miss-guard-window-ms`            | `3000`      | Guard window to throttle repeated read-miss hydration attempts per db/table. Values `<= 0` disable throttling.                                                                                                                                                                                                                                                                                 |
| `cloud.storage.upload-retry-max-attempts`            | `3`         | Whole-file retry attempts after a first upload failure. Default `3` retries are enabled to protect against transient network errors. <br/>Set to `0` to disable whole-file retries (failures go directly to DLQ) when the provider already has sufficient internal retry logic. `CloudStorageNonRetryableException` always bypasses retries and goes directly to DLQ regardless of this value. |
| `cloud.storage.upload-retry-initial-delay-ms`        | `1000`      | Delay before first whole-file retry; subsequent retries use exponential backoff. Only used when `upload-retry-max-attempts > 0`.                                                                                                                                                                                                                                                               |
| `cloud.storage.upload-retry-max-delay-ms`            | `60000`     | Upper bound for exponential backoff delay between whole-file retry attempts. Only used when `upload-retry-max-attempts > 0`.                                                                                                                                                                                                                                                                   |
| `cloud.storage.upload-backpressure-high-watermark`   | `64`        | When `> 0`, slows the RocksDB flush/compaction thread in `onTableFileCreated` while the pending-upload backlog (retry-queue in-flight + DLQ size) exceeds this value, bounding the amount of local-only at-risk data. Blocked at most 30 s per event. `0` disables backpressure.                                                                                                               |
| `cloud.storage.wal-mode`                             | `flush`     | WAL durability mode for metadata mirroring. `flush` forces a MemTable flush before each capture; at most the un-flushed tail since the last sync is lost on an uncontrolled crash. `wal` also mirrors active `*.log` WAL segments alongside metadata and replays them on restore (lower RPO, at the cost of more frequent small uploads).                                                      |

Notes:

- Keep `path-prefix` stable across restarts to preserve object-key continuity.
- If `provider` is not found on classpath, initialization fails fast in `CloudStorageProviderFactory`.
- Upload retry uses a two-layer model: S3 part-level retries (inside one `uploadFile()` call) are handled by the provider; whole-file retries are handled by `CloudUploadRetryQueue` when `upload-retry-max-attempts > 0` (default `3`).
- `CloudStorageNonRetryableException` thrown by a provider bypasses immediate retries. The file remains unconfirmed and is automatically retried on the next compaction via the delete guard.
- Failed uploads are tracked via metrics (`cloud_storage_upload_failures_total`) and structured logs. Automatic retry occurs on subsequent compactions; no manual recovery needed.
- Backpressure blocks the RocksDB compaction thread for at most 30 s (`BACKPRESSURE_MAX_WAIT_MS`) even if the backlog remains above the watermark after that window.
- Metadata sync publishes a consistent `CURRENT`/`MANIFEST`/`OPTIONS` (plus WAL tail when `wal-mode: wal`) snapshot so a full-disk-loss node can reopen from cloud.
- Metadata sync is always enabled when cloud storage is enabled.
- Metadata sync is event-triggered by storage events; there is no background interval scheduler.
- Provider-specific properties (e.g., bucket, region, credentials) are configured under `cloud.storage.<provider>.*` namespace.
- **Operational observability**: Monitor metrics and logs to track sync progress (see "Observability: Metrics & Logs" section above).

### S3 provider-specific options (`cloud.storage.s3.*`)

For provider `s3`, configure these properties under `cloud.storage.s3`:

| Configuration                                           | Default  | Description                                                                                        |
|---------------------------------------------------------|----------|----------------------------------------------------------------------------------------------------|
| `cloud.storage.s3.bucket`                               | _(none)_ | Target S3 bucket name. Required when S3 provider is enabled.                                       |
| `cloud.storage.s3.region`                               | _(none)_ | AWS region (for example `us-east-1`).                                                              |
| `cloud.storage.s3.endpoint`                             | _(none)_ | Optional custom endpoint for S3-compatible stores (MinIO/Ceph).                                    |
| `cloud.storage.s3.access-key`                           | _(none)_ | Access key / AWS Access ID credential. Omit to use AWS default credentials chain.                  |
| `cloud.storage.s3.secret-key`                           | _(none)_ | Secret key credential. Omit to use AWS default credentials chain.                                  |
| `cloud.storage.s3.multipart-part-retry-max-attempts`    | `3`     | Max retries for each multipart upload part before the whole file upload fails.                      |
| `cloud.storage.s3.multipart-part-retry-base-backoff-ms` | `1000`  | Base backoff for part retries (exponential: 1x/2x/4x...).                                           |
| `cloud.storage.s3.multipart-exhausted-direct-dlq`       | `false` | If `true`, part-retry exhaustion is marked non-retryable so outer SST retry can go directly to DLQ. |

### Sample `application.yml` (`cloud.storage`)

If you want to configure Store directly through `application.yml` (instead of env injection),
use a snippet like this:

```yaml
cloud:
  storage:
    enabled: true
    provider: s3
    path-prefix: hugegraph

    # Hydration controls
    startup-hydration-enabled: true
    read-miss-guard-window-ms: 3000

    # Upload retry & dead-letter queue (whole-file level; common for all providers)
    upload-retry-max-attempts: 3          # 0 to disable whole-file retries (DLQ-only)
    upload-retry-initial-delay-ms: 1000
    upload-retry-max-delay-ms: 60000

    # Backpressure: slow RocksDB flush/compaction thread while pending-upload backlog > watermark
    # Blocks at most 30 s per event; 0 disables
    upload-backpressure-high-watermark: 64

    # Metadata durability (CURRENT/MANIFEST/OPTIONS[/WAL])
    # Metadata sync is always enabled when cloud storage is enabled
    wal-mode: flush                        # 'flush' or 'wal' (lower RPO, more uploads)

    # S3 provider-specific configuration (cloud.storage.s3.*)
    s3:
      bucket: hugegraph-store0
      region: us-east-1
      endpoint: http://minio:9000
      access-key: minioadmin
      secret-key: minioadmin
      
      # Multipart upload retry configuration (S3 part-level, inside one uploadFile() call)
      multipart-part-retry-max-attempts: 3
      multipart-part-retry-base-backoff-ms: 1000
      multipart-exhausted-direct-dlq: false
```

Notes:

- `upload-retry-max-attempts` defaults to `3` (immediate retries enabled after first failure). Set to `0` only if the provider has sufficient built-in retry logic. Automatic retry also occurs on subsequent compactions via the delete guard, so failed uploads are never silently lost.
- In this docker stack, each Store node uses its own bucket (`hugegraph-store0/1/2`).
- For local MinIO, endpoint is typically `http://minio:9000` inside the docker network.
- Provider-specific properties are configured under `cloud.storage.<provider>.*` namespace.
- **Monitor these metrics**: `cloud_storage_unconfirmed_files_total` (should trend to 0), `cloud_storage_upload_failures_total` (should be low), `cloud_storage_sync_latency_ms` (should be sub-second for typical files).


## 3) Write Path

Two sub-flows exist: **SST upload** (on flush/compaction) and **SST delete** (on compaction cleanup).

### 3a) SST Upload Flow

```text
SST UPLOAD (onTableFileCreated)

Client
  |
  | 1) Write request
  v
HugeGraph Server
  |
  | 2) Route to partition leader
  v
Store Node (RocksDB)
  |
  | 3) WAL append + MemTable update
  | 4) Flush/compaction creates *.sst
  v
CloudStorageEventListener
  |
  | 5) onTableFileCreated(db, cf, file)
  | 6) toRelativeKey(file) -> <store-scope-prefix>/<relative-path>
  | 7) uploadFile(localPath, remoteKey)
  v
CloudStorageProvider
  |
  | 8) PUT object (full key: <path-prefix>/<store-scope-prefix>/<relative-path>)
  v
Object Storage
  |
  | 9) ACK
  v
CloudStorageEventListener
  |
  | 10) syncTracker.markConfirmed(dbName, fileNumber)  <- bitmap updated
  | 11) applyBackpressure(dbName)  <- optional throttle if backlog > watermark
```

### 3b) SST Delete Flow

```text
SST DELETE (onTableFileDeleted) — runs on the RocksDB compaction thread

RocksDB compaction completes: SST1 + SST2 -> MERGED_SST
  |
  | 1) onTableFileDeleted(db, cf, filePath=SST1 or SST2)
  v
CloudStorageEventListener
  |
  | 2) ensureLiveSetUploaded(provider, dbName)
  |      -> syncTracker.allConfirmed(dbName, liveFiles)   <- single lock, short-circuit on first miss
  |      If NOT all confirmed:
  |        -> for each unconfirmed live file:
  |             upload now (idempotent PUT, no existence probe)
  |             syncTracker.markConfirmed on success
  |      If any live file still not durable: return false -> skip delete (hold old SST)
  |
  | 3) [live set confirmed durable]
  |    syncMetadataSnapshotInline(dbName)  <- publish MANIFEST/CURRENT BEFORE delete
  |      -> RocksDBFactory.captureMetadataSnapshot()
  |      -> uploadMetadataSnapshot(...)
  |         * verifies all manifest-referenced SSTs are in cloud
  |         * uploads OPTIONS / MANIFEST / CURRENT atomically
  |      If snapshot fails: return false -> skip delete (hold old SST)
  |
  | 4) [MANIFEST published] provider.deleteFile(remoteKey)  <- safe to remove old SST
  v
Object Storage: old SST deleted
```

### Write-path notes

- On DB creation (`onDBCreated`), existing local SST files are scanned and uploaded if missing in cloud.
  A MemTable flush is triggered via `onDBCreated` (event-driven, not a background async task) so
  WAL-recovered/in-memory data materializes into SST files and gets mirrored to cloud.
- Remote object key is always scoped per store node:
  **`<path-prefix>/<store-scope-prefix>/<relative-path>`** e.g. `hugegraph/store-127.0.0.1_8501/0/000001.sst`.
  `store-scope-prefix` is derived from `raft.address` at startup (see `AppConfig.buildCloudStoreScopePrefix()`).
  This guarantees key uniqueness across nodes even when multiple Store nodes share the same bucket.
- After every successful upload, `syncTracker.markConfirmed(dbName, fileNumber)` sets the corresponding
  bit in a per-`dbName` in-memory bitmap (`CloudSyncTracker`). This bitmap is the fast path used by the
  delete guard (`allConfirmed`) to avoid per-file cloud API calls on the hot compaction thread.
- The delete guard (`ensureLiveSetUploaded`) checks the **entire current live SST set**, not just the file
  being deleted. Reason: the old SST being confirmed says nothing about whether its compaction outputs
  (replacement files) are durable. Only when every live file is confirmed does the delete proceed.
- MANIFEST/CURRENT is published to cloud **before** the old SST is deleted, so that a recovery attempt
  always has a consistent metadata + data state to restore from.

## 4) Read Path

Two hydration modes exist:

1. **Startup pre-hydration (`onDBOpening`)**: download missing remote files before serving,
   then seed the sync-tracker bitmap from the remote listing so the first post-restart compaction
   can use the fast-path bitmap check instead of issuing per-file cloud API calls.
2. **Read-miss hydration (`onReadMiss`)**: if local read misses, fetch missing SST from cloud, ingest, and retry.

```text
READ PATH (ANSI)

Read request
  |
  | 1) get(key)
  v
RocksDB
  |
  | 2) miss -> onReadMiss(session, table, key)
  v
CloudStorageEventListener
  |
  | 3) listFiles(<path-prefix>/<store-scope-prefix>/<db-prefix>)
  v
CloudStorageProvider
  |
  | 4) LIST objects
  v
Object Storage
  |
  | 5) return key list (scoped to this store node's prefix)
  v
CloudStorageProvider
  |
  | 6) for each missing *.sst:
  |    downloadFile(remoteKey, localPath)
  |    GET object -> receive object bytes
  v
CloudStorageEventListener
  |
  | 7) ingestSstFile(downloaded)
  v
RocksDB
  |
  | 8) retry get(key)
  v
Read request result
```

### Startup pre-hydration detail (`onDBOpening`)

```text
onDBOpening(dbName)
  |
  | 1) listFiles(<path-prefix>/<store-scope-prefix>/<dbName>/)
  |    -> remoteFiles (scoped to this store node)
  |
  | 2) for each remoteFile not present locally:
  |      downloadFile(remoteKey, localPath)
  |
  | 3) [after download loop]
  |    for each *.sst in remoteFiles:
  |      syncTracker.markConfirmed(dbName, fileNumber)   <- bitmap seeded from remote listing
  |                                                         zero extra cloud API calls
```

### Read-path notes

- All `listFiles` calls use the **`<path-prefix>/<store-scope-prefix>/`** namespace so each store node
  only sees and downloads its own SST files, even when multiple nodes share the same bucket.
- A guard window (`read-miss-guard-window-ms`) throttles repeated hydration attempts for the same db/table pair.
- Only missing local SST files are downloaded.
- Non-SST objects are ignored for read-miss ingestion.
- After startup pre-hydration, `syncTracker` is fully seeded from the remote listing. Subsequent
  compaction delete guards use the in-memory bitmap (`allConfirmed`) and do **not** issue
  `provider.fileExists()` round-trips on the hot compaction thread.

## 5) Failure Handling

### Upload failures (write-critical)

#### Two-layer retry model

Upload failures use a two-layer retry strategy:

| Layer                      | Where                                        | What it retries                            | Config keys                                                                                             |
|----------------------------|----------------------------------------------|--------------------------------------------|---------------------------------------------------------------------------------------------------------|
| **Part-level** (S3 only)   | Inside `S3CloudStorageProvider.uploadFile()` | Individual 512 MB multipart chunks         | `cloud.storage.s3.multipart-part-retry-max-attempts`, `multipart-part-retry-base-backoff-ms`            |
| **File-level** (common)    | `CloudUploadRetryQueue` (async)              | Whole SST file after `uploadFile()` throws | `cloud.storage.upload-retry-max-attempts`, `upload-retry-initial-delay-ms`, `upload-retry-max-delay-ms` |

The default `upload-retry-max-attempts=3` enables whole-file retries for transient failures. The S3 provider also handles part-level retries internally. Both layers can be tuned independently.

#### Failure flow (default `upload-retry-max-attempts=3`)

- `onTableFileCreated` calls `provider.uploadFile()`.
- S3 retries individual parts internally (via `multipart-part-retry-*`).
- If `uploadFile()` still throws a regular `IOException`, `CloudUploadRetryQueue.submit()` enqueues the task for up to `upload-retry-max-attempts` whole-file retries with exponential backoff.
- If `uploadFile()` throws `CloudStorageNonRetryableException`, the task bypasses retries and skips the upload queue.
- After all retry attempts are exhausted (or if non-retryable), the file remains unconfirmed in the bitmap. The file stays on disk and is **automatically retried on the next compaction** via the delete guard (`ensureLiveSetUploaded`).

#### Observability: Metrics & Logs

Instead of a persistent DLQ, the system provides real-time observability through metrics and structured logs:

**Metrics (exposed to Prometheus/monitoring):**

- `cloud_storage_unconfirmed_files_total` (gauge, labeled by `db_name`) — Count of SST files not yet confirmed in cloud bitmap. High value → uploads are failing or slow.
- `cloud_storage_upload_failures_total` (counter, labeled by `db_name`, `cf_name`, `error_type`) — Total upload failures (transient + permanent). Tracks upload problem frequency.
- `cloud_storage_retry_queue_size` (gauge) — Files waiting in the upload retry queue. Indicates backlog of pending uploads.
- `cloud_storage_sync_latency_ms` (histogram, labeled by `db_name`) — Time from SST file creation (onTableFileCreated) to bitmap confirmation (markConfirmed). Measures sync speed.
- `cloud_storage_delete_guard_reupload_count` (counter, labeled by `db_name`) — Number of files re-uploaded by the delete guard when live set was not fully durable. Indicates frequent upload failures.

**Structured Logs:**

Log entries provide visibility into sync progress and failures:

- **Startup (bitmap seeding)**: `"Seeded sync-tracker bitmap with N confirmed files from remote listing: dbName=..."`
  - Indicates: bitmap is warm after restart, delete guard can use fast path.

- **Delete guard checking**: `"Checking live set durability: dbName=..., liveFileCount=..., unconfirmedCount=..."`
  - Indicates: delete guard evaluated M files, found K unconfirmed.

- **Delete guard re-upload**: `"Re-uploading M unconfirmed live files (not yet confirmed in cloud): dbName=..., files=[...], reason=live_set_not_durable"`
  - Indicates: files failed to sync on first attempt, being re-attempted now.

- **Delete guard re-upload success**: `"Successfully confirmed L previously unconfirmed files; delete proceeding: dbName=..., oldSstFile=..."`
  - Indicates: re-upload succeeded, old SST can now be safely deleted.

- **Delete guard skipped**: `"Delete skipped (live set not fully durable in cloud): dbName=..., oldSstFile=..., unconfirmedFiles=[...], nextRetryAt=..."`
  - Indicates: at least one live file is still not durable. Delete is held. Retry will occur on next compaction.

- **Upload failure (first attempt)**: `"Upload failed (will retry): dbName=..., cfName=..., filePath=..., attempt=1/3, error=..."`
  - Indicates: transient failure, will be retried.

- **Upload failure (exhausted retries)**: `"Upload failed permanently after 3 retries: dbName=..., cfName=..., filePath=..., error=..., nextRetryOnCompaction=..."`
  - Indicates: all retries exhausted. File stays on disk; will retry automatically on next compaction.

**Example: Finding unsynced files**

Query metrics in your monitoring system:

```promql
# Find databases with unconfirmed files
cloud_storage_unconfirmed_files_total > 0

# Alert if unconfirmed count grows over time
rate(cloud_storage_unconfirmed_files_total[5m]) > 0
```

Or search logs:

```bash
# Find all delete-guard re-uploads in the past hour
kubectl logs -l app=hugegraph-store --since=1h | grep "Re-uploading.*unconfirmed"

# Find all permanently-failed uploads
kubectl logs -l app=hugegraph-store | grep "Upload failed permanently"

# Track a specific file
kubectl logs -l app=hugegraph-store | grep "filePath=/path/to/000123.sst"
```

**Automatic retry without DLQ:**

- Files that fail to upload remain unconfirmed in the bitmap.
- On the next compaction that touches the same DB, the delete guard calls `ensureLiveSetUploaded`, which:
  1. Checks bitmap for all current live files
  2. For any unconfirmed file, attempts upload again (idempotent PUT)
  3. On success, updates bitmap; on failure, logs and holds delete
- This cycle repeats automatically; no manual DLQ replay needed.
- Upload success rate and retry frequency are tracked via metrics and logs.

**Tuning:**

To control retry behavior:

```yaml
cloud.storage.upload-retry-max-attempts: 3         # Immediate retries on first failure
cloud.storage.upload-retry-initial-delay-ms: 1000  # Start backoff at 1s
cloud.storage.upload-retry-max-delay-ms: 60000     # Cap backoff at 60s
```

If the provider handles all retry logic internally, disable whole-file retries:

```yaml
cloud.storage.upload-retry-max-attempts: 0   # No immediate retries; rely on delete guard auto-retry
```

### Delete failures (non-fatal)

- `onTableFileDeleted` delete failure is logged and processing continues.
- Impact is stale/orphaned cloud objects, not immediate read/write unavailability.

### Hydration failures

- Pre-hydration/list/download failures throw `IllegalStateException` and stop the flow for that DB open attempt.
- Read-miss hydration failures are logged and return `false`; caller falls back to original miss behavior.

### Provider lifecycle / config failures

- Unknown provider name or missing plugin JAR fails initialization in `CloudStorageProviderFactory`.
- Provider switching/re-init is handled with close-and-reinitialize semantics.
- If no active provider is found at runtime (provider is `null`), all event callbacks (`onTableFileCreated`, `onTableFileDeleted`, `onReadMiss`) return immediately without error, so RocksDB continues normally without cloud mirroring.

### Non-retryable upload failures (`CloudStorageNonRetryableException`)

- Providers can signal a permanently failed upload by throwing `CloudStorageNonRetryableException` from `uploadFile()`.
- This exception bypasses the whole-file retry loop and marks the file as unconfirmed (no bitmap entry).
- The file remains on disk and is **automatically retried on the next compaction** via the delete guard's re-upload logic.
- The S3 provider uses this when `multipart-exhausted-direct-dlq: true` and all multipart part retries are exhausted, preventing pointless full-file re-attempts for a part that consistently fails.
- Metrics (`cloud_storage_upload_failures_total`) and logs track the failure for operational visibility.

### Backpressure timeout

- When `upload-backpressure-high-watermark > 0` and the pending-upload backlog (retry-queue in-flight + DLQ size) exceeds the watermark, `onTableFileCreated` parks the RocksDB flush/compaction thread in 50 ms increments.
- After `30 000 ms` (`BACKPRESSURE_MAX_WAIT_MS`) the backpressure wait exits unconditionally and the new SST upload is attempted regardless, to prevent a permanent RocksDB stall.
- A warning log is emitted when backpressure starts, and an info log when it is released.

### Delete guard failure (live set not fully durable)

- Before deleting a superseded SST from cloud, `onTableFileDeleted` verifies the entire current live SST set is confirmed present in cloud (`ensureLiveSetUploaded`).
- If any live SST cannot be confirmed durable (e.g. a prior upload failed), the delete is skipped with a warning log.
- The unconfirmed file remains on disk. On the next compaction, the delete guard will re-attempt upload for all unconfirmed files.
- **Observability**: Metrics `cloud_storage_unconfirmed_files_total` and `cloud_storage_delete_guard_reupload_count` track the frequency. Logs show which files are unconfirmed and why the delete was held.
- **Impact**: Orphaned (but safe) old object may remain in cloud temporarily. Data integrity is preserved because the replacement files are not yet confirmed durable.

### Metadata snapshot capture failure

- `syncMetadataSnapshotInline` calls `RocksDBFactory.captureMetadataSnapshot()`. If this returns `null` (e.g. the DB is not open or the checkpoint could not be acquired), the metadata sync is skipped and returns `false`.
- When called from `onTableFileDeleted`, a `null` snapshot causes the delete to be held with a warning log, preserving the old SST in cloud until a valid snapshot can be published.

### Metadata publish failure (SST durability check fails)

- `uploadMetadataSnapshot` first ensures all manifest-referenced SSTs are present in cloud. If any SST cannot be made durable, the method aborts before uploading `MANIFEST` or `CURRENT`, returns `false`, and logs a warning.
- The previous durable generation (prior `CURRENT`/`MANIFEST`) is left intact in cloud, so a recovery attempt still has a valid consistent state to restore from.
- An `IOException` during upload of `OPTIONS`/`MANIFEST`/`CURRENT` also aborts the publish with a warning.

### S3 multipart upload failures

- **Initiation failure**: if `CreateMultipartUpload` fails, `uploadFile()` throws `IOException` immediately (no part retries). The whole-file retry queue then handles it if `upload-retry-max-attempts > 0`.
- **Part retry exhaustion**: individual part upload failures are retried up to `multipart-part-retry-max-attempts` times with exponential backoff (`multipart-part-retry-base-backoff-ms`). When all part retries are exhausted, the multipart upload is aborted.
- **Abort failure**: if the `AbortMultipartUpload` call itself fails, the failure is logged as a warning. The incomplete multipart upload may remain in the bucket (incurring storage cost) until an S3 lifecycle rule or manual cleanup removes it.
- **Direct-DLQ on part exhaustion**: when `multipart-exhausted-direct-dlq: true`, part-retry exhaustion throws `CloudStorageNonRetryableException`, bypassing whole-file retries.


### Local file compacted away during retry

- When a local SST file is compacted into a new merged SST (e.g., SST1 → MERGED_SST), the old SST1 may still be unconfirmed in the bitmap if its upload failed.
- On the next compaction involving MERGED_SST, the delete guard checks if MERGED_SST is confirmed. If not, it re-attempts upload.
- If SST1 was compacted away, its local file no longer exists, so no retry is attempted for SST1 itself.
- However, the data from SST1 is now present in MERGED_SST in cloud (or will be on next attempt). Once MERGED_SST is confirmed, SST1 can be safely deleted from cloud.
- Metrics track `cloud_storage_delete_guard_reupload_count` to monitor how often the delete guard needs to re-upload files.

## 6) Recovery Scenarios

Failure-mode and RPO-oriented recovery summary:

| Failure scenario                              | Data loss?                                 | RPO                                                    | Recovery mechanism                                                                                                                                                                 | Mitigation                                                                                                      |
|-----------------------------------------------|--------------------------------------------|--------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------|
| Single Store crash                            | No                                         | 0s                                                     | 2/3 Raft quorum survives. Leader re-election continues service. In-flight (not SST-flushed) data is recovered from Raft logs; flushed data remains available via local/cloud SSTs. | Keep 3+ replicas across zones, alert on replica loss, and use durable PV-backed store nodes.                    |
| 2 of 3 Stores crash                           | No (service stalls until quorum restored)  | 0s                                                     | Surviving replica Raft log bootstraps recovering nodes after restart. In-flight data is recovered from Raft log replay.                                                            | Enforce anti-affinity and failure-domain isolation to prevent correlated failures.                              |
| Catastrophic loss: all Store disks destroyed  | Yes                                        | Seconds to minutes (depends on SST sync mode/interval) | Raft logs and local SSTs are lost. Nodes recover from last completed cloud SST sync; data after that sync is unrecoverable.                                                        | Use durable disks, scheduled volume snapshots/backups, and synchronous SST upload mode for tighter RPO.         |

Notes:

- In-flight data (not yet flushed to SST) is recovered from Raft logs when quorum-replicated.
- Cloud storage recovery primarily protects flushed SST state and disaster cases involving local disk loss.
- Synchronous SST upload reduces catastrophic-loss RPO compared with periodic upload mode.

## 7) Operational Notes

### Monitoring & Alerting

Cloud storage health is exposed through metrics and logs. Set up monitoring for:

**Alerts (suggest these thresholds):**

```yaml
# Alert if any database has unconfirmed files for > 5 minutes (sync is stuck)
- alert: CloudStorageUnsyncedFilesHigh
  expr: cloud_storage_unconfirmed_files_total > 0 and increase(cloud_storage_unconfirmed_files_total[5m]) > 0
  annotations:
    summary: "Store node {{ $labels.instance }} has {{ $value }} unconfirmed files in {{ $labels.db_name }}"
    action: "Check upload logs for permission errors or network issues"

# Alert if upload failures are increasing (transient or persistent failures)
- alert: CloudStorageUploadFailuresIncreasing
  expr: rate(cloud_storage_upload_failures_total[5m]) > 0
  annotations:
    summary: "Store node {{ $labels.instance }} is experiencing {{ $value }} upload failures/sec"
    action: "Review logs for details; check S3/cloud provider status"

# Alert if retry queue is growing (backlog of pending uploads)
- alert: CloudStorageRetryQueueBacklog
  expr: cloud_storage_retry_queue_size > 10
  annotations:
    summary: "Store node {{ $labels.instance }} has {{ $value }} files in retry queue"
    action: "Monitor until queue drains; backpressure may be slowing compaction"

# Alert if sync latency is high (uploads taking too long)
- alert: CloudStorageSyncLatencyHigh
  expr: histogram_quantile(0.95, cloud_storage_sync_latency_ms) > 30000
  annotations:
    summary: "Store node {{ $labels.instance }} 95th percentile sync latency is {{ $value }}ms"
    action: "Check network link to cloud provider; consider tuning upload concurrency"
```

**Logs to monitor:**

```bash
# Watch for re-uploads (delete guard re-attempting failed uploads)
kubectl logs -f -l app=hugegraph-store | grep "Re-uploading.*unconfirmed"

# Watch for permanently-failed uploads (still retried, but worth investigation)
kubectl logs -f -l app=hugegraph-store | grep "Upload failed permanently"

# Watch for delete-skipped events (delete guard holding old SSTs)
kubectl logs -f -l app=hugegraph-store | grep "Delete skipped.*not fully durable"

# Trace a specific unconfirmed file
kubectl logs -f -l app=hugegraph-store | grep "filePath=/path/to/000123.sst"
```

### Troubleshooting

**Symptom: `cloud_storage_unconfirmed_files_total` stays > 0**

- **Cause**: Uploads are failing and not recovering.
- **Action**:
  1. Check logs for "Upload failed permanently" messages — what's the error?
  2. Verify cloud provider credentials and connectivity: `curl https://s3-endpoint/bucket-name`
  3. Check if bucket exists and Store node has put/get/delete permissions.
  4. Verify `cloud.storage.path-prefix` matches actual S3 prefix where Store expects to write.
  5. If a specific file is stuck, manually trigger sync via admin API or wait for next compaction.

**Symptom: Delete operations are slow or stalling**

- **Cause**: Delete guard is re-uploading many files; backpressure may be active.
- **Action**:
  1. Check logs for "Re-uploading M unconfirmed files" — how many?
  2. If backpressure is active, monitor `cloud_storage_retry_queue_size`. Once it drains, delete resumes.
  3. Increase `upload-backpressure-high-watermark` if you want to allow more local-only data during cloud outages.

**Symptom: High `cloud_storage_sync_latency_ms`**

- **Cause**: Uploads are slow. Could be network, object size, or cloud provider latency.
- **Action**:
  1. Check average file size: `ls -lh /data/raft/*/db-*/` (larger files take longer)
  2. Measure network throughput: `iperf3` or cloud provider bandwidth test.
  3. Check if S3 multipart upload is enabled and tuned (reduces latency for large files).

### General Best Practices

- Prefer stable `path-prefix` and bucket naming; changing them affects object lookup continuity.
- Keep plugin JAR versions aligned with Store version to avoid SPI/API mismatch.
- Track logs around upload, hydration, and provider init to detect divergence early.
- For DR drills, test both node-level restart and full-cluster restart with cloud hydration enabled.
- Set up alerts on the key metrics (`unconfirmed_files_total`, `upload_failures_total`, `sync_latency_ms`) to catch issues early.

## Plugin Development: Adding Custom Cloud Storage Providers

This section guides developers on building and deploying new cloud storage provider plugins (e.g., Azure Blob Storage, ADLS, GCP).

### Plugin Architecture Overview

HugeGraph uses a **Service Provider Interface (SPI)** pattern to discover and load cloud storage providers at runtime:

1. **CloudStorageProvider interface**: Located in `hugegraph-store/hg-store-common`, defines the contract all providers must implement.
2. **ServiceLoader discovery**: Store node uses `java.util.ServiceLoader` to find all `CloudStorageProvider` implementations on the classpath.
3. **SPI selection**: At Store startup, `CloudStorageProviderFactory` loads the provider specified in `cloud.storage.provider` config.
4. **Plugin packaging/runtime classpath**: Provider implementations are packaged as separate JAR modules and must be available on Store runtime classpath.

### CloudStorageProvider Interface

All custom providers must implement:

```java
public interface CloudStorageProvider {
    
    /**
     * Unique name for this provider (e.g., "s3", "azure", "adls").
     * Must match the `cloud.storage.provider` config value to be activated.
     */
    String providerName();
    
    /**
     * Initialize the provider with configuration.
     * Called once at Store startup. Throw exception if init fails.
     */
    void init(CloudStorageConfig config) throws IOException;
    
    /**
     * Upload a local file to cloud storage with the given remote key.
     */
    void uploadFile(String localPath, String remoteKey) throws IOException;
    
    /**
     * Download a remote file from cloud storage to the local path.
     */
    void downloadFile(String remoteKey, String localPath) throws IOException;
    
    /**
     * Delete a remote file from cloud storage.
     */
    void deleteFile(String remoteKey) throws IOException;
    
    /**
     * Check if a remote file exists.
     */
    boolean fileExists(String remoteKey) throws IOException;
    
    /**
     * List all remote files under the given directory prefix.
     * Returns list of keys (with prefix stripped).
     */
    List<String> listFiles(String remoteDirPrefix) throws IOException;
    
    /**
     * Close and release all provider resources.
     */
    void close() throws IOException;
}
```

**Location:** `hugegraph-store/hg-store-common/src/main/java/org/apache/hugegraph/store/cloud/CloudStorageProvider.java`

### Module Structure for a New Provider

```
hugegraph-store/
├── hg-store-cloud-newprovider/                             # New provider module
│   ├── pom.xml                                              # Maven module definition
│   ├── src/main/java/org/apache/hugegraph/store/cloud/newprovider/
│   │   └── NewProviderCloudStorageProvider.java             # Implementation
│   ├── src/main/resources/META-INF/services/
│   │   └── org.apache.hugegraph.store.cloud.CloudStorageProvider
│   └── src/test/java/...                                    # Unit tests
```

Recommendation:

- For providers maintained in this repository, prefer adding them as submodules under `hugegraph-store/` (same model as `hg-store-cloud-s3`).
- External providers are also supported if their jars are placed on runtime classpath.

### Step 1: Create Module POM

File: `hugegraph-store/hg-store-cloud-newprovider/pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    
    <parent>
        <groupId>org.apache.hugegraph</groupId>
        <artifactId>hugegraph-store</artifactId>
        <version>${revision}</version>
        <relativePath>../pom.xml</relativePath>
    </parent>
    
    <artifactId>hg-store-cloud-newprovider</artifactId>
    <name>HugeGraph Store Cloud NewProvider</name>
    <description> NewProvider Storage plugin for HugeGraph Store cloud storage integration</description>
    
    <dependencies>
        <!-- HugeGraph Store cloud SPI -->
        <dependency>
            <groupId>org.apache.hugegraph</groupId>
            <artifactId>hg-store-common</artifactId>
            <version>${revision}</version>
        </dependency>
        
        <!-- NewProvider SDK -->
        <dependency>
            <groupId>NewProvider</groupId>
            <artifactId>NewProvider</artifactId>
            <version>1.0</version>
        </dependency>
        <!-- Optional: additional provider-specific dependencies -->
    </dependencies>
</project>
```

### Step 2: Implement the Provider

File: `hugegraph-store/hg-store-cloud-newprovider/src/main/java/org/apache/hugegraph/store/cloud/newprovider/NewProviderCloudStorageProvider.java`

```java
package org.apache.hugegraph.store.cloud.newprovider;

import org.apache.hugegraph.store.cloud.CloudStorageConfig;
import org.apache.hugegraph.store.cloud.CloudStorageProvider;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("RedundantThrows")
@Slf4j
public class NewProviderCloudStorageProvider implements CloudStorageProvider {
    
    public static final String PROVIDER_NAME = "newprovider";
    
    private Object providerClient;
    private String pathPrefix;
    
    @Override
    public String providerName() {
        return PROVIDER_NAME;
    }
    
    @Override
    public void init(CloudStorageConfig config) throws IOException {
        //Steps to initialize the NewProvider client using config parameters
    }
    
    @Override
    public void uploadFile(String localPath, String remoteKey) throws IOException {
        //Steps to upload file to NewProvider cloud storage
    }
    
    @Override
    public void downloadFile(String remoteKey, String localPath) throws IOException {
        //Steps to download file from NewProvider cloud storage
    }
    
    @Override
    public void deleteFile(String remoteKey) throws IOException {
        //Steps to delete file from NewProvider cloud storage
    }
    
    @Override
    public boolean fileExists(String remoteKey) throws IOException {
        //Steps to check if file exists in NewProvider cloud storage
        return false;
    }
    
    @Override
    public List<String> listFiles(String remoteDirPrefix) throws IOException {
        //Steps to list files in NewProvider cloud storage
        return new ArrayList<>();
    }
    
    @Override
    public void close() throws IOException {
        //Steps to close and cleanup NewProvider client resources
    }
}
```

### Step 3: Register via SPI

File: `hugegraph-store/hg-store-cloud-newprovider/src/main/resources/META-INF/services/org.apache.hugegraph.store.cloud.CloudStorageProvider`

```
org.apache.hugegraph.store.cloud.newprovider.NewProviderCloudStorageProvider
```

This file tells Java's `ServiceLoader` to discover this provider at runtime.

### Step 4: Build and Test

```bash
# Build the provider module
cd hugegraph-store
mvn clean package -pl hg-store-cloud-newprovider -am -DskipTests

# Find the generated JAR
find . -name "hg-store-cloud-newprovider-*.jar"
# Output: hugegraph-store/hg-store-cloud-newprovider/target/hg-store-cloud-newprovider-1.0.jar
```

### Step 5: Package for Runtime Classpath

Use one of these runtime models:

**Option A (recommended for in-repo providers): add as Store submodule dependency**

1. Add module `hg-store-cloud-newprovider` under `hugegraph-store/`.
2. Add dependency from `hg-store-node` to `hg-store-cloud-newprovider`.
3. Rebuild distribution so provider + transitive dependencies are packaged with Store runtime.

**Option B (external provider): provide jars on runtime classpath**

- Supply `hg-store-cloud-newprovider-*.jar` **and** all required dependency jars, or
- supply a single shaded/uber provider jar that already contains transitive dependencies.

Notes:

- Simply copying a provider jar without its runtime dependencies can cause `ClassNotFoundException` during SPI loading.
- The exact classpath injection method depends on your launch model (custom startup script, container image, or JVM args).

### Step 6: Configure and Activate

In Store `application.yml`:

```yaml
cloud:
  storage:
    enabled: true
    provider: newprovider  # Must match NewProviderCloudStorageProvider.providerName()
    path-prefix: hugegraph
    
    # Provider-specific configuration (cloud.storage.newprovider.*)
    newprovider:
      bucket: my-container
      region: myaccount  # e.g., Azure account name
      endpoint: https://myaccount.blob.core.windows.net
      access-key: ${NEWPROVIDER_KEY}
      secret-key: ${NEWPROVIDER_SECRET}
```

Or via Docker env (for S3 example):

```bash
HG_CLOUD_STORAGE_ENABLED=true \
HG_CLOUD_STORAGE_PROVIDER=s3 \
HG_CLOUD_STORAGE_PATH_PREFIX=hugegraph \
HG_CLOUD_STORAGE_S3_BUCKET=hugegraph-store0 \
HG_CLOUD_STORAGE_S3_REGION=us-east-1 \
HG_CLOUD_STORAGE_S3_ENDPOINT=http://minio:9000 \
HG_CLOUD_STORAGE_S3_ACCESS_KEY=minioadmin \
HG_CLOUD_STORAGE_S3_SECRET_KEY=minioadmin \
docker run hugegraph-store:latest
```

Or via Docker env (for custom newprovider):

```bash
HG_CLOUD_STORAGE_ENABLED=true \
HG_CLOUD_STORAGE_PROVIDER=newprovider \
HG_CLOUD_STORAGE_PATH_PREFIX=hugegraph \
HG_CLOUD_STORAGE_NEWPROVIDER_BUCKET=my-container \
HG_CLOUD_STORAGE_NEWPROVIDER_REGION=myaccount \
HG_CLOUD_STORAGE_NEWPROVIDER_ENDPOINT=https://myaccount.blob.core.windows.net \
HG_CLOUD_STORAGE_NEWPROVIDER_ACCESS_KEY=${NEWPROVIDER_KEY} \
HG_CLOUD_STORAGE_NEWPROVIDER_SECRET_KEY=${NEWPROVIDER_SECRET} \
docker run hugegraph-store:latest
```

Notes on property naming conventions:

- **YAML properties**: Use kebab-case with provider-specific namespacing (e.g., `cloud.storage.s3.bucket`, `cloud.storage.newprovider.access-key`)
- **Environment variables**: Use uppercase with underscores and `HG_CLOUD_STORAGE_*` prefix. Provider-specific options use `HG_CLOUD_STORAGE_<PROVIDER>_*` pattern
- Kebab-case YAML properties are converted to uppercase with underscores (e.g., `multipart-part-retry-max-attempts` → `MULTIPART_PART_RETRY_MAX_ATTEMPTS`)

### Testing Your Provider

Add unit tests in `hg-store-cloud-newprovider/src/test/`:

```java
@Test
public void testInit() {
    // Test provider initialization with mock config
}

@Test
public void testUploadDownload() {
    // Test upload and download of a sample file
}

@Test
public void testDeleteAndList() {
    // Test delete and list operations
}

@Test
public void testFileExists() {
    // Test file existence check
}

@Test
public void testClose() {
    // Test provider close and resource cleanup
}
```

### Troubleshooting Provider Discovery

If your provider isn't loaded:

1. **Check SPI registration file exists:**
   ```bash
   jar tf hg-store-cloud-newprovider-1.0.jar | grep "META-INF/services"
   ```
   Should show: `META-INF/services/org.apache.hugegraph.store.cloud.CloudStorageProvider`

2. **Check provider name matches config:**
   ```bash
   jar xf hg-store-cloud-newprovider-1.0.jar META-INF/services/org.apache.hugegraph.store.cloud.CloudStorageProvider
   cat META-INF/services/org.apache.hugegraph.store.cloud.CloudStorageProvider
   ```

3. **Enable debug logging:**
   ```yaml
   logging:
     level:
       org.apache.hugegraph.store.cloud: DEBUG
       org.apache.hugegraph.store.node.cloud: DEBUG
   ```

4. **Verify provider jars are on classpath:**
   ```bash
   # Check Store startup logs for provider discovery / class-loading errors
   docker logs hugegraph-store | grep -i "ServiceLoader\|provider"
   ```

### Key Implementation Tips

1. **Thread safety**: Ensure provider is thread-safe for concurrent upload/download/delete operations.
2. **Connection pooling**: Reuse client connections; initialize once in `init()`, close in `close()`.
3. **Path normalization**: Always use `pathPrefix` correctly (see S3 example for reference).
4. **Error handling**: Throw `IOException` for operational issues. Implement your own internal retry logic (see `S3CloudStorageProvider` for reference). The common `CloudUploadRetryQueue` provides a DLQ safety net but does not retry by default (`upload-retry-max-attempts=0`).
5. **Logging**: Use SLF4J (via `@Slf4j`) for consistent log levels with Store.
6. **Configuration validation**: Validate all required fields in `init()` and fail fast.

### Contributing Your Provider

To upstream your new provider (e.g., `hg-store-cloud-newprovider`):

1. Create PR to `hugegraph-store/` with new provider module
2. Add tests in `hg-store-test/` that validate provider behavior
3. Update `docker/cloud-storage/` examples with new provider setup steps
4. Update `install-dist/` license/notice if adding third-party dependencies
