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
- Provider model: SPI provider selected once at startup via `ServiceLoader` and injected into its consumers.

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
| | Raft log + MemTable -> Local RocksDB SST|  |
| +--------------------+--------------------+  |
|                      |                       |
|                      v                       |
| +-----------------------------------------+  |
| | Pluggable Cloud Storage Workflow (NEW)  |  |
| | CloudStorageEventListener (NEW)         |  |
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
- `Raft log + MemTable -> Local RocksDB SST`: Local write pipeline in each Store node. The Raft log is the durability source for recent writes (the RocksDB WAL is disabled under Raft); the MemTable is flushed/compacted into SSTs.
- `Pluggable Cloud Storage Workflow (NEW)`: SST lifecycle hook path for upload/delete/download/list.
- `CloudStorageEventListener (NEW)`: Captures RocksDB file events and triggers cloud operations.
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
| `cloud.storage.upload-retry-max-attempts`            | `3`         | Whole-file retry attempts after a first upload failure. Default `3` retries are enabled to protect against transient network errors. <br/>Set to `0` to disable whole-file retries (failures are given up on immediately) when the provider already has sufficient internal retry logic. `CloudStorageNonRetryableException` always bypasses retries and is given up on immediately regardless of this value. |
| `cloud.storage.upload-retry-initial-delay-ms`        | `1000`      | Delay before first whole-file retry; subsequent retries use exponential backoff. Only used when `upload-retry-max-attempts > 0`.                                                                                                                                                                                                                                                               |
| `cloud.storage.upload-retry-max-delay-ms`            | `60000`     | Upper bound for exponential backoff delay between whole-file retry attempts. Only used when `upload-retry-max-attempts > 0`.                                                                                                                                                                                                                                                                   |
| `cloud.storage.metadata-sync-debounce-ms`            | `1000`      | Interval at which DBs with SST uploads since their last metadata publish are drained: each such DB gets at most one coalesced publish per interval, and a failed publish is retried on the next interval. Shutdown flushes pending DBs once. `<= 0` publishes on every SST. Event-driven publishes (delete guard, compaction, DB open) are **never** coalesced. |
| `cloud.storage.node-id`                              | _(blank)_   | Stable per-node identity used to derive the cloud key scope (`store-<node-id>`). When set, it is authoritative and stable across restarts, IP/hostname changes, **and** local disk loss, so recovery always finds prior remote objects — recommended for production (e.g. a StatefulSet pod ordinal or provisioned node UUID). Blank falls back to a scope persisted in the data root (`.cloud-store-scope`), which is stable across network-identity drift but lost with the disk, and is in turn seeded from the runtime network address on first start. See "Cloud key scope resolution" below.            |

Notes:

- Keep `path-prefix` stable across restarts to preserve object-key continuity.
- If `provider` is not found on classpath, initialization fails fast in `AppConfig`.
- Upload retry uses a two-layer model: S3 part-level retries (inside one `uploadFile()` call) are handled by the provider; whole-file retries are handled by `CloudUploadRetryQueue` when `upload-retry-max-attempts > 0` (default `3`).
- `CloudStorageNonRetryableException` thrown by a provider bypasses immediate retries. The file remains unconfirmed and is automatically retried on the next compaction via the delete guard.
- Failed uploads are tracked via metrics (`cloud_storage_upload_failures_total`) and structured logs. Automatic retry occurs through the whole-file retry queue, the deferred SST-delete retry, and startup SST backfill; no manual recovery needed.
- Provider-specific properties (`cloud.storage.<provider>.*`) are bound only when `cloud.storage.enabled=true`; with cloud storage disabled they are never read, so a disabled node needs no bucket or credential settings. Credential placeholders in `application.yml` use empty defaults (for example `${AWS_ACCESS_KEY_ID:}`), so an unset variable resolves to blank and falls back to the AWS default credentials chain instead of failing placeholder resolution.
- Metadata sync publishes a consistent `CURRENT`/`MANIFEST`/`OPTIONS` snapshot so a full-disk-loss node can reopen from cloud. Capture forces a MemTable flush so the un-flushed tail is persisted into an SST and mirrored (the RocksDB WAL is disabled under Raft — the Raft log is the tail's durability source — so a flush is the only path that pushes recent writes to cloud).
- Metadata sync is always enabled when cloud storage is enabled.
- Metadata sync is triggered by storage events. Each SST upload marks its DB dirty, and a fixed-delay drain (`metadata-sync-debounce-ms`) publishes each dirty DB once per interval; event-driven publishes (delete guard, DB open) are never coalesced.
- Compaction completion (`onCompacted`) only marks the DB metadata-dirty; the publish runs on the single `cloud-metadata-sync` thread. `onTableFileDeleted` only queues the superseded SST; the live-set check, the metadata publish and the remote delete run on the same thread, in publish-before-delete order, with one metadata publish per DB for all deletes queued at that point. A failed publish backs off exponentially **per DB** (base `max(1s, metadata-sync-debounce-ms)`, doubling per consecutive failure, capped at 5 minutes) instead of redoing a checkpoint + upload every interval; a success clears the backoff. The backoff covers both the drain and held superseded-SST deletes, so the 500 ms delete retry does not re-publish a DB whose publishes are failing.
- **Publish before close.** When a DB is released (a partition moving away, or `releaseAllGraphDB()` at shutdown), RocksDB listeners get `onDBClosing` while the session is still open, and a DB with an unpublished metadata change is published then. At shutdown the store engine releases every DB before the cloud listener is closed, so a publish deferred to `close()` could no longer capture the DB. A DB being deleted is not published.
- The whole-file retry queue dedupes by `(db, remoteKey)`: a publish that re-checks an already-queued upload does not enqueue a second retry for the same object.
- Uploads that exhaust whole-file retries are given up on: logged at ERROR, counted in `cloud_storage_uploads_given_up_total`, and re-uploaded by startup SST backfill.
- Set a stable `cloud.storage.node-id` for production so the cloud key scope survives IP/hostname changes and local disk loss (see "Cloud key scope resolution").
- Provider-specific properties (e.g., bucket, region, credentials) are configured under `cloud.storage.<provider>.*` namespace.
- **Operational observability**: Monitor metrics and logs to track sync progress (see "Observability: Metrics & Logs" section above).

### Metadata capture, the sync window, and RPO

> **The RocksDB WAL is disabled in HStore.** The state-machine write path calls
> `dbSession.setDisableWAL(true)` (`BusinessHandlerImpl`, comment: *"raft mode, disable rocksdb
> log"*) because the **Raft log** — replicated across the partition's peers — is the durability
> source for recent writes, not the RocksDB WAL. RocksDB `*.log` segments therefore stay empty, and
> cloud storage never mirrors a WAL; the discussion below is about how recent writes reach cloud and
> how current the cloud copy is.

#### How recent writes reach cloud (force-flush-and-mirror)

Each metadata capture uses RocksDB `Checkpoint.createCheckpoint`, which flushes the MemTable,
persisting the un-flushed tail into a new L0 SST that is then mirrored. Because the RocksDB WAL is
disabled, this forced flush is the **only** mechanism that proactively pushes recent
(still-in-MemTable) writes into cloud. When the MemTable is empty the flush is a no-op.

- *Cloud RPO for the recent tail:* bounded by the metadata-sync cadence
  (`metadata-sync-debounce-ms`).
- *Cost:* a small extra L0 SST per non-empty capture (write amplification + cloud object churn +
  compaction pressure), whose frequency is bounded by the drain interval. The Raft log is the
  tail's durability source for any single/partial-node loss regardless of cloud cadence.

#### Is metadata-sync coalescing a data-loss window?

No uploaded SST bytes are ever lost to coalescing. In `SstUploadTask.run()` the SST object is
uploaded to cloud **first** (the code notes *"the SST is already durable in cloud"*); only the
subsequent `CURRENT`/`MANIFEST` **pointer publish** is coalesced. So the window is a **bounded
metadata / recovery-point lag, not a byte-loss hole**:

- The newest uploaded SST(s) may exist in cloud but not yet be *referenced* by the mirrored MANIFEST
  for about one `metadata-sync-debounce-ms` interval (default 1 s), longer only while publishes
  fail. The dirty DB stays marked until a publish succeeds, so the final state is always published
  even if writes then stop; `onDBCreated`, the delete guard and DB release (`onDBClosing`)
  publish without coalescing.
- This lag becomes observable data loss **only** under a total, simultaneous loss of *all* Raft
  peers' local state followed by a cloud-only restore that lands inside that ~1 s window —
  then the newest unreferenced SSTs are orphans not in the restored set.
- For any lesser failure (single / partial node loss) the **Raft log** holds the committed tail and
  is the primary recovery source, so the cloud metadata lag is irrelevant. Restore is also
  fail-closed consistent (the "Cloud restore inconsistent" guard never restores a `CURRENT` that
  references a missing MANIFEST — worst case an older but fully consistent point).

To tighten the cloud recovery point — at the cost of more frequent metadata publishes and forced
flushes — lower `metadata-sync-debounce-ms` (`0` = publish on every SST).

### S3 provider-specific options (`cloud.storage.s3.*`)

For provider `s3`, configure these properties under `cloud.storage.s3`:

| Configuration                                           | Default   | Description                                                                                          |
|---------------------------------------------------------|-----------|------------------------------------------------------------------------------------------------------|
| `cloud.storage.s3.bucket`                               | _(none)_  | Target S3 bucket name. Required when S3 provider is enabled.                                         |
| `cloud.storage.s3.region`                               | _(none)_  | AWS region (for example `us-east-1`). Blank with `endpoint` set defaults to `us-east-1` (signing region only). Blank with no `endpoint` falls back to the AWS region chain (`AWS_REGION`, `aws.region`, profile, instance metadata); if none resolves, startup fails with an error naming `cloud.storage.s3.region`. |
| `cloud.storage.s3.endpoint`                             | _(none)_  | Optional custom endpoint for S3-compatible stores (MinIO/Ceph).                                      |
| `cloud.storage.s3.access-key`                           | _(none)_  | Access key / AWS Access ID credential. Omit or leave blank to use AWS default credentials chain.     |
| `cloud.storage.s3.secret-key`                           | _(none)_  | Secret key credential. Omit or leave blank to use AWS default credentials chain.                     |

Request and response checksums use the AWS SDK `WHEN_REQUIRED` mode, so checksums are computed or
validated only for operations that mandate them, which keeps S3-compatible stores that reject the
SDK's default trailer checksums working.

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

    # Upload retry (whole-file level; common for all providers)
    upload-retry-max-attempts: 3          # 0 to disable whole-file retries (give up immediately)
    upload-retry-initial-delay-ms: 1000
    upload-retry-max-delay-ms: 60000

    # Metadata durability (CURRENT/MANIFEST/OPTIONS)
    # Metadata sync is always enabled when cloud storage is enabled
    metadata-sync-debounce-ms: 1000        # coalesced per-SST metadata sync interval; <= 0 = publish on every SST

    # Stable per-node identity for the cloud key scope (store-<node-id>). Blank => persist one in the
    # data root (seeded from the network address). Set a deployment-stable value (e.g. StatefulSet
    # ordinal / provisioned UUID) for guaranteed recovery after address change or local disk loss.
    node-id:

    # S3 provider-specific configuration (cloud.storage.s3.*)
    s3:
      bucket: hugegraph-store0
      region: us-east-1
      endpoint: http://minio:9000
      access-key: minioadmin
      secret-key: minioadmin
```

Notes:

- `upload-retry-max-attempts` defaults to `3` (immediate retries enabled after first failure). Set to `0` only if the provider has sufficient built-in retry logic. Automatic retry also occurs on subsequent compactions via the delete guard, so failed uploads are never silently lost.
- In this docker stack, each Store node uses its own bucket (`hugegraph-store0/1/2`).
- For local MinIO, endpoint is typically `http://minio:9000` inside the docker network.
- Provider-specific properties are configured under `cloud.storage.<provider>.*` namespace.
- **Monitor these metrics**: `cloud_storage_confirmed_files_total` (per-DB gauge; should track the live SST count), `cloud_storage_upload_failures_total` (should stay flat), `cloud_storage_sync_latency_ms_seconds` (the Prometheus name of the sync-latency timer; should be sub-second for typical files).


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
  | 3) Raft log append + MemTable update (RocksDB WAL disabled)
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
```

### 3b) SST Delete Flow

```text
SST DELETE (onTableFileDeleted) — queued on the RocksDB callback thread, processed on cloud-metadata-sync

RocksDB compaction completes: SST1 + SST2 -> MERGED_SST
  |
  | 1) onTableFileDeleted(db, cf, filePath=SST1 or SST2)
  |      -> queue the delete, schedule processDeferredSstDeletes; no cloud I/O on this thread
  v
CloudStorageEventListener (cloud-metadata-sync thread), once per DB per pass:
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
  | 4) [MANIFEST published] provider.deleteFile(remoteKey)  <- for every queued SST of the DB
  v
Object Storage: old SST(s) deleted

Any "hold" above keeps the delete queued; it is retried on the
cloud-metadata-sync thread every 500 ms. Deletes still queued when the
listener closes are not persisted: the superseded objects stay in cloud
as unreferenced orphans (no data loss).
```

### Write-path notes

- On DB creation (`onDBCreated`), existing local SST files are scanned and uploaded if missing in cloud.
  A MemTable flush is triggered via `onDBCreated` (event-driven, not a background async task) so
  in-memory data (recovered by replaying the Raft log on open) materializes into SST files and gets
  mirrored to cloud.
- Remote object key is always scoped per store node:
  **`<path-prefix>/<store-scope-prefix>/<relative-path>`** e.g. `hugegraph/store-127.0.0.1_8501/0/000001.sst`.
  `store-scope-prefix` is `store-<node-id>`, resolved at startup by
  `AppConfig.resolveStableStoreScopePrefix()` (see "Cloud key scope resolution" below).
  This guarantees key uniqueness across nodes even when multiple Store nodes share the same bucket.
- After every successful upload, `syncTracker.markConfirmed(dbName, fileNumber)` sets the corresponding
  bit in a per-`dbName` in-memory bitmap (`CloudSyncTracker`). This bitmap is the fast path used by the
  delete guard (`allConfirmed`) to avoid per-file cloud API calls on every delete pass.
- The delete guard (`ensureLiveSetUploaded`) checks the **entire current live SST set**, not just the file
  being deleted. Reason: the old SST being confirmed says nothing about whether its compaction outputs
  (replacement files) are durable. Only when every live file is confirmed does the delete proceed.
- MANIFEST/CURRENT is published to cloud **before** the old SST is deleted, so that a recovery attempt
  always has a consistent metadata + data state to restore from.

- The native RocksDB event listener is attached to `DBOptions` only when at least one registered `RocksdbChangedListener` reports `managesRemoteState()` (the cloud listener does). Nodes without cloud storage keep the pre-cloud behaviour. Only four native callbacks are enabled: `ON_COMPACTION_BEGIN`, `ON_COMPACTION_COMPLETED`, `ON_TABLE_FILE_CREATED` and `ON_TABLE_FILE_DELETED`, so other event kinds never cross the JNI boundary.
- Destroy timing changed for every node, with or without cloud storage. A destroyed DB that still has live references now waits up to 30 minutes before its references are force-released and the directory is deleted. Earlier releases did this one to two minutes after the destroy. See [Operations Guide](operations-guide.md#issue-2-partition-imbalance) for details.

### Cloud key scope resolution

The `store-scope-prefix` that isolates each node's objects is resolved by
`AppConfig.resolveStableStoreScopePrefix()` in precedence order, most-durable first:

1. **Configured `cloud.storage.node-id`** — authoritative. Lives in deployment config, so it is stable
   across restarts, IP/hostname changes, **and** local disk loss. Recovery always finds prior objects.
   Recommended for production (e.g. StatefulSet pod ordinal, provisioned node UUID). When set, it is
   also written to the persisted marker so a later removal of the config value still resolves the same scope.
2. **Persisted marker** `<primary-data-root>/.cloud-store-scope` — written on first start. Keeps the
   scope stable across network-identity (IP/hostname) drift, but is **lost with the disk**.
3. **Runtime network address** — legacy fallback used to *seed* the marker on first start (keeps upgrades
   migration-safe: an existing deployment already keyed by `store-<host_port>` resolves to the same scope).
   A `WARN` is logged advising an explicit `node-id` for durable recovery, since a later address change
   would otherwise read a different scope.

## 4) Read Path

Two hydration modes exist:

1. **Startup pre-hydration (`onDBOpening`)**: download missing remote files before serving,
   then seed the sync-tracker bitmap from the remote listing so the first post-restart compaction
   can use the fast-path bitmap check instead of issuing per-file cloud API calls.
2. **Read-miss hydration (`onReadMiss`)**: if a local read fails because a live SST is missing on disk,
   download the missing SST back to its original path and retry the read once. A read that returns no
   value is a genuine key-not-found (RocksDB raises an error when a file it needs is missing), so it
   does not trigger hydration.

```text
READ PATH (ANSI)

Read request
  |
  | 1) get(key)
  v
RocksDB
  |
  | 2) read error (missing live SST) -> onReadMiss(session, table, key)
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
  | 0) pending-delete marker present for this prefix?
  |      -> up to 3 bounded synchronous cleanup attempts (tryCompletePendingDelete)
  |      -> still failing: throw IllegalStateException("Cloud open blocked ..."),
  |         async purge retry re-armed. Runs even with startup-hydration-enabled=false.
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

- **Open blocked by a pending delete.** If a pending-delete marker exists for the DB's prefix, `onDBOpening` makes three bounded synchronous cleanup attempts. If the purge still cannot complete, the open fails with `IllegalStateException("Cloud open blocked ...")` and the asynchronous retry is re-armed; opening is never allowed to proceed over a prefix that still holds the deleted generation. Cleanup is complete only once both the prefix purge and the deletion of the sibling tombstone succeed; until then the marker stays and the open stays blocked, because a tombstone left next to a re-created DB would make a later open purge that DB's live objects. This check runs even when `startup-hydration-enabled=false`.
- **Leftover tombstone at open.** If a tombstone exists without a local marker (for example the marker was lost with the disk), `onDBOpening` purges the prefix and deletes the tombstone before the DB can hydrate from or upload into it, and blocks the open if either step fails. This also runs when `startup-hydration-enabled=false`; in that mode a failed tombstone check is logged and the open proceeds, since nothing is read from cloud. Every successful purge also drops the purged DB's sync-tracker confirmations, so the next metadata publish re-uploads the SSTs it references.
- **Version-suffixed directories.** A partition can move to a version-suffixed directory (`<name>_<v1>` or `<name>_<v1>_<v2>`), and its objects then live under that prefix. When the directory chosen for a DB holds no local DB (no `CURRENT`), the listener lists the remote siblings that share the base name (one `listFiles(<parent>/<name>)` call, a plain key prefix) and the DB is opened at the newest restorable one, in the same version order `RocksDBSession` uses locally. A sibling is restorable when it has a remote `CURRENT`, no `_DELETED` tombstone and no local pending-delete marker. The redirect only applies with `startup-hydration-enabled=true`, only to a sibling of the chosen directory that is not awaiting deletion, and never when a local DB exists. If the listing fails, the chosen directory is kept and its hydration applies the usual failure rules.
- **Sweeps on open.** Leftover `<db>_cloudmeta_*` metadata checkpoint directories (from a crash during a snapshot capture) and `.hyd-tmp-*` partial hydration download files are deleted when the DB is opened.
- All `listFiles` calls use the **`<path-prefix>/<store-scope-prefix>/`** namespace so each store node
  only sees and downloads its own SST files, even when multiple nodes share the same bucket.
- A guard window (`read-miss-guard-window-ms`) throttles repeated hydration attempts for the same db/table pair.
- Only missing local SST files are downloaded.
- Non-SST objects are ignored for read-miss ingestion.
- After startup pre-hydration, `syncTracker` is fully seeded from the remote listing. Subsequent
  compaction delete guards use the in-memory bitmap (`allConfirmed`) and do **not** issue
  `provider.fileExists()` round-trips per delete.

### Scope: managed delete vs accidental local loss

- Current behavior intentionally treats a missing local DB directory as a **recovery** case and hydrates from cloud when remote objects exist.
- Intentional delete/reset is recognized through the managed deletion flow (`RocksDBFactory.destroyGraphDB()` -> `onDBDeleted`).
- **Anti-resurrection guard (implemented).** On managed delete, `onDBDeleted`:
  1. **First** persists a local *pending-delete marker* — this is the durable guard. If the marker cannot be durably written, the delete is **held** (`deleteMarkerHealthy` flips to `0`) rather than risking a re-hydration after a crash.
  2. Writes a sibling *tombstone* object (`<scope>/<path-prefix>/<db>_DELETED`) alongside the data prefix, then purges the remote DB prefix (SSTs, metadata, tombstone).
  - If the provider is unavailable, the purge/tombstone is deferred but the local pending-delete marker still blocks re-hydration (`onDBOpening`/`preHydrateDbFiles` detect the deleted generation and skip hydration), and an async retry completes the tombstone + purge once a provider returns.
- **Epoch-guarded purge retries.** The asynchronous pending-delete retry is tied to a per-prefix epoch and the truncate-purge retry to a per-DB write epoch, both captured when the retry is scheduled and re-checked under the per-prefix lock. If the prefix is opened or created (delete retry) or the DB writes again (truncate retry) in the meantime, the retry is abandoned rather than purging objects that now belong to the new DB; an abandoned truncate retry also removes its on-disk truncate marker so a restart does not re-drive it.
- Store tracks the last successfully published RocksDB **generation** per DB, so a re-create during a provider outage still cannot re-hydrate the deleted generation.
- If a local DB directory is removed **outside** the managed flow, no delete callback is emitted; hydration may still restore data from cloud for the same DB path — this remains an accepted scope tradeoff prioritizing accidental local-loss recovery.

## 5) Failure Handling

### Upload failures (write-critical)

#### Two-layer retry model

Upload failures use a two-layer retry strategy:

| Layer                      | Where                                        | What it retries                            | Config keys                                                                                             |
|----------------------------|----------------------------------------------|--------------------------------------------|---------------------------------------------------------------------------------------------------------|
| **Request-level** (S3 only) | AWS SDK client inside `S3CloudStorageProvider.uploadFile()` | Individual PUTs and multipart parts | None (SDK `STANDARD` retry mode, see `buildRetryOverride()`)                                            |
| **File-level** (common)    | `CloudUploadRetryQueue` (async)              | Whole SST file after `uploadFile()` throws | `cloud.storage.upload-retry-max-attempts`, `upload-retry-initial-delay-ms`, `upload-retry-max-delay-ms` |

The default `upload-retry-max-attempts=3` enables whole-file retries for transient failures. The S3 provider also relies on the AWS SDK's own retries for individual requests and multipart parts; only the file-level layer is configurable.

#### Failure flow (default `upload-retry-max-attempts=3`)

- `onTableFileCreated` calls `provider.uploadFile()`.
- The AWS SDK retries individual requests and multipart parts internally.
- If `uploadFile()` still throws a regular `IOException`, `CloudUploadRetryQueue.submit()` enqueues the task for up to `upload-retry-max-attempts` whole-file retries with exponential backoff.
- If `uploadFile()` throws `CloudStorageNonRetryableException`, the task bypasses retries and skips the upload queue.
- After all retry attempts are exhausted (or if non-retryable), the file remains unconfirmed in the bitmap. The file stays on disk and is retried by the delete guard (`ensureLiveSetUploaded`), which runs in the deferred SST-delete pass and on every metadata publish.

#### Observability: Metrics & Logs

The system provides real-time observability through metrics and structured logs:

**Metrics (exposed to Prometheus/monitoring):**

Names below are the Prometheus exposition names (Micrometer Prometheus naming: counters end in `_total`, timers export in seconds).

- `cloud_storage_confirmed_files_total` (gauge, label `db_name`) — Number of SST files confirmed present in cloud for that DB (the bitmap size). Despite the `_total` suffix it is a gauge and can go down after compaction. A value well below the DB's live SST count means uploads are failing or slow.
- `cloud_storage_upload_failures_total` (counter, no labels) — Total upload failures (transient + permanent) across all DBs. Tracks upload problem frequency; use logs to identify the DB or file.
- `cloud_storage_retry_queue_size` (gauge) — Files waiting in the upload retry queue. Bound to the live queue (`retryQueue.getInFlightCount()`), so it reflects the real backlog rather than a constant `0`.
- `cloud_storage_uploads_given_up_total` (counter) — Uploads the retry queue gave up on (retries exhausted, `CloudStorageNonRetryableException`, or shutdown). Each file stays unconfirmed in the sync tracker and is re-uploaded by the next metadata sync, the delete guard, or startup SST backfill. Alert on a sustained increase.
- `cloud_storage_sync_latency_ms_seconds` (summary, no labels; series `{quantile="0.5|0.95|0.99"}`, `_count`, `_sum`, `_max`) — Time from SST file creation (`onTableFileCreated`) to bitmap confirmation (`markConfirmed`). The timer is registered as `cloud_storage_sync_latency_ms`; the Prometheus registry appends the `_seconds` base unit, so values are in seconds.
- `cloud_storage_delete_guard_reupload_count` (gauge, label `db_name`) — Cumulative number of files re-uploaded by the delete guard for that DB when the live set was not fully durable. Use `increase(...)` over a window; a rising value indicates frequent upload failures.
- `cloud_storage_delete_marker_healthy` (gauge, `1`=healthy / `0`=degraded) — Health of pending-delete marker persistence. `0` means a marker could not be durably written, so a DB delete whose remote cleanup fails may not be guarded against re-hydration after a crash. This is a hard error state: **delete progression is held while degraded.** Alert on `0`.

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
# Delete-guard re-uploads per database over the last hour (live set was not durable)
increase(cloud_storage_delete_guard_reupload_count[1h]) > 0

# Uploads abandoned by the retry queue (re-uploaded later by backfill / delete guard)
increase(cloud_storage_uploads_given_up_total[1h]) > 0
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

**Giving up on an upload:**

- Uploads that exhaust all whole-file retries (or are thrown as `CloudStorageNonRetryableException`) are given up on: logged at ERROR, counted in `cloud_storage_uploads_given_up_total`, and their staging hard-link is released. There is no dead-letter queue; nothing retries a given-up upload.
- Given-up files are **not** lost: they stay recoverable via the delete guard and startup SST backfill. After a restart, `onDBCreated` re-runs the startup SST backfill, which uploads every live `*.sst` missing from the bucket, and orphaned staging hard-links under `<data-root>/.cloud-upload-staging/` are deleted when the listener starts.

**Automatic retry (delete guard, no manual replay):**

- Files that fail to upload remain unconfirmed in the bitmap.
- On the next deferred delete pass for the same DB (triggered by a compaction deleting an SST, retried every 500 ms while held), the delete guard calls `ensureLiveSetUploaded`, which:
  1. Checks bitmap for all current live files
  2. For any unconfirmed file, attempts upload again (idempotent PUT)
  3. On success, updates bitmap; on failure, logs and holds delete
- This cycle repeats automatically; no manual replay needed.
- After a retry upload becomes durable, `onRetryUploadDurable` publishes `CURRENT`/`MANIFEST` so the mirrored recovery point advances even on an idle DB.
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

- A failed superseded-SST delete is logged and stays queued for retry; processing continues.
- Impact is stale/orphaned cloud objects, not immediate read/write unavailability.

### Hydration failures

- Pre-hydration/list/download failures throw `IllegalStateException` and stop the flow for that DB open attempt.
- Read-miss hydration failures are logged and return `false`; the original read error propagates.

### Provider lifecycle / config failures

- Unknown provider name or missing plugin JAR fails initialization in `AppConfig`.
- **Resolved once, never reconfigured.** `AppConfig` selects the provider via `ServiceLoader`, calls `init()` once, and passes it to `CloudUploadRetryQueue` and `CloudStorageEventListener` as a final, non-null field. When `cloud.storage.enabled=false`, neither is constructed and no provider is loaded.
- **Failed `@PostConstruct` cleanup.** If cloud-storage wiring throws during startup, `AppConfig` closes the retry queue and the provider so a failed init does not leak the provider's client/threads (Spring does not invoke `@PreDestroy` when `@PostConstruct` throws), then fails startup.
- **`listFiles` and `deletePrefix` are required.** `CloudStorageProvider` has no default implementations for them: every provider must list keys and must throw an `IOException` from `deletePrefix()` if any object under the prefix could not be deleted, so callers such as the DB-purge path can detect an incomplete purge and keep the tombstone + local marker.

### Non-retryable upload failures (`CloudStorageNonRetryableException`)

- Providers can signal a permanently failed upload by throwing `CloudStorageNonRetryableException` from `uploadFile()`.
- This exception bypasses the whole-file retry loop and marks the file as unconfirmed (no bitmap entry).
- The file remains on disk and is retried by the delete guard's re-upload logic.
- The S3 provider throws it for permanent service-side errors (e.g. auth, permission, not-found, validation), so a request that cannot succeed is not retried as a whole file.
- Metrics (`cloud_storage_upload_failures_total`) and logs track the failure for operational visibility.

### Delete guard failure (live set not fully durable)

- Before deleting a superseded SST from cloud, the queued-delete pass (`processDeferredSstDeletes`) verifies the entire current live SST set is confirmed present in cloud (`ensureLiveSetUploaded`).
- If any live SST cannot be confirmed durable (e.g. a prior upload failed), the delete is held with a warning log and stays queued.
- The unconfirmed file remains on disk. The deferred delete is retried every 500 ms on the `cloud-metadata-sync` thread, and each attempt re-attempts upload for all unconfirmed files.
- **Observability**: Metrics `cloud_storage_delete_guard_reupload_count` (per DB) and `cloud_storage_confirmed_files_total` track the frequency and sync state. Logs show which files are unconfirmed and why the delete was held.
- **Impact**: Orphaned (but safe) old object may remain in cloud temporarily. Data integrity is preserved because the replacement files are not yet confirmed durable.

### Metadata snapshot capture failure

- `syncMetadataSnapshotInline` calls `RocksDBFactory.captureMetadataSnapshot()`. If this returns `null` (e.g. the DB is not open or the checkpoint could not be acquired), the metadata sync is skipped and returns `false`.
- When called from the deferred SST-delete pass, a `null` snapshot causes the delete to be held with a warning log, preserving the old SST in cloud until a valid snapshot can be published.

### Metadata publish failure (SST durability check fails)

- `uploadMetadataSnapshot` first ensures all manifest-referenced SSTs are present in cloud. If any SST cannot be made durable, the method aborts before uploading `MANIFEST` or `CURRENT`, returns `false`, and logs a warning.
- The previous durable generation (prior `CURRENT`/`MANIFEST`) is left intact in cloud, so a recovery attempt still has a valid consistent state to restore from.
- An `IOException` during upload of `OPTIONS`/`MANIFEST`/`CURRENT` also aborts the publish with a warning.

### S3 multipart upload failures

- Files larger than 512 MB are uploaded through `S3TransferManager`, which splits them into parts, uploads them concurrently and completes the multipart upload, or aborts it on failure. Part retries come from the AWS SDK's retry strategy; there is no provider-specific part-retry setting.
- If the upload still fails, `uploadFile()` throws an `IOException` (or `CloudStorageNonRetryableException` for permanent service errors), and the whole-file retry queue handles it as for any other upload.
- If the abort itself fails, the incomplete multipart upload can remain in the bucket (incurring storage cost). The provider does not sweep for these; configure an S3 lifecycle rule (`AbortIncompleteMultipartUpload`) on the bucket to remove them.


### Local file compacted away during retry

- When a local SST file is compacted into a new merged SST (e.g., SST1 → MERGED_SST), the old SST1 may still be unconfirmed in the bitmap if its upload failed.
- On the next deferred delete pass involving MERGED_SST, the delete guard checks if MERGED_SST is confirmed. If not, it re-attempts upload.
- If SST1 was compacted away, its local file no longer exists, so no retry is attempted for SST1 itself.
- However, the data from SST1 is now present in MERGED_SST in cloud (or will be on next attempt). Once MERGED_SST is confirmed, SST1 can be safely deleted from cloud.
- Metrics track `cloud_storage_delete_guard_reupload_count` to monitor how often the delete guard needs to re-upload files.

## 6) Recovery Scenarios

Failure-mode and RPO-oriented recovery summary:

| Failure scenario                              | Data loss?                                                                                                                                                                                    | RPO                                                                                                          | Recovery mechanism                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | Mitigation                                                                                                                                                                                                   |
|-----------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Single Store crash                            | No                                                                                                                                                                                            | 0s                                                                                                           | 2/3 Raft quorum survives. Leader re-election continues service. In-flight (not SST-flushed) data is recovered from Raft logs; flushed data remains available via local/cloud SSTs.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | Keep 3+ replicas across zones, alert on replica loss, and use durable PV-backed store nodes.                                                                                                                 |
| 2 of 3 Stores crash                           | No (service stalls until quorum restored)                                                                                                                                                     | 0s                                                                                                           | Surviving replica Raft log bootstraps recovering nodes after restart. In-flight data is recovered from Raft log replay.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | Enforce anti-affinity and failure-domain isolation to prevent correlated failures.                                                                                                                           |
| Catastrophic loss: all Store disks destroyed  | Yes — writes committed after the last cloud metadata publish (`CURRENT`/`MANIFEST`) are unrecoverable because both the Raft log (the tail's durability source) and the local SSTs are gone.   | Seconds to minutes (bounded by flush/compaction cadence + async upload latency + the metadata-sync window)   | Nodes recover from the last cloud-mirrored `CURRENT`/`MANIFEST` + SST set. **Scope re-seed risk:** `.cloud-store-scope` is also lost; the node falls back to the runtime address to derive its cloud key scope. If the address changed (new VM, rescheduled pod), the node silently scopes to a wrong prefix and starts empty. Set `cloud.storage.node-id` to a stable value so the scope is config-driven and survives address changes (see "Cloud key scope resolution"). | Set `cloud.storage.node-id` (StatefulSet ordinal or node UUID). Use durable disks, scheduled backups, and a lower `metadata-sync-debounce-ms` for a smaller recovery point.|

Notes:

- In-flight data (not yet flushed to SST) is recovered from Raft logs when quorum-replicated.
- Cloud storage recovery primarily protects flushed SST state and disaster cases involving local disk loss.
- SST upload is always asynchronous and event-triggered: each `onTableFileCreated` (flush/compaction)
  eagerly uploads the new SST on the shared upload executor, with failures routed to the retry queue.
  There is no periodic/interval uploader and no synchronous-upload mode. The cloud recovery point
  is bounded by how soon the SSTs upload and the subsequent coalesced `CURRENT`/`MANIFEST` publish
  lands (`metadata-sync-debounce-ms`).
- Recovery correctness is protected by the metadata-before-delete invariant: before deleting superseded SSTs, Store confirms manifest-referenced SST durability and publishes updated `MANIFEST`/`CURRENT`, preventing stale or missing-manifest references after compaction retries.
- Policy note: for how managed delete vs accidental local-loss is distinguished during hydration, see `## 4) Read Path` -> `Scope: managed delete vs accidental local loss`.

## 7) Operational Notes

### Monitoring & Alerting

Cloud storage health is exposed through metrics and logs. Set up monitoring for:

**Alerts (suggest these thresholds):**

```yaml
# Alert if the delete guard keeps re-uploading files for a database (uploads are not sticking)
- alert: CloudStorageDeleteGuardReuploads
  expr: increase(cloud_storage_delete_guard_reupload_count[15m]) > 0
  for: 15m
  annotations:
    summary: "Store node {{ $labels.instance }} delete guard is re-uploading files in {{ $labels.db_name }}"
    action: "Check upload logs for permission errors or network issues"

# Alert if the retry queue is giving up on uploads
- alert: CloudStorageUploadsGivenUp
  expr: increase(cloud_storage_uploads_given_up_total[15m]) > 0
  annotations:
    summary: "Store node {{ $labels.instance }} gave up on {{ $value }} uploads"
    action: "Given-up files are re-uploaded by backfill / delete guard; check provider status and credentials"

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
    action: "Monitor until queue drains"

# Alert if sync latency is high (uploads taking too long)
- alert: CloudStorageSyncLatencyHigh
  expr: cloud_storage_sync_latency_ms_seconds{quantile="0.95"} > 30
  annotations:
    summary: "Store node {{ $labels.instance }} 95th percentile sync latency is {{ $value }}s"
    action: "Check network link to cloud provider; consider tuning upload concurrency"

# Alert if a pending-delete marker could not be persisted (DB delete progression is held)
- alert: CloudStorageDeleteMarkerDegraded
  expr: cloud_storage_delete_marker_healthy == 0
  annotations:
    summary: "Store node {{ $labels.instance }} pending-delete marker persistence is degraded"
    action: "DB delete is held to preserve the anti-resurrection guard; check data-root disk/permissions"
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

**Symptom: `cloud_storage_delete_guard_reupload_count` keeps increasing, or `cloud_storage_confirmed_files_total` stays below the DB's live SST count**

- **Cause**: Uploads are failing and not recovering.
- **Action**:
  1. Check logs for "Upload failed permanently" messages — what's the error?
  2. Verify cloud provider credentials and connectivity: `curl https://s3-endpoint/bucket-name`
  3. Check if bucket exists and Store node has put/get/delete permissions.
  4. Verify `cloud.storage.path-prefix` matches actual S3 prefix where Store expects to write.
  5. If a specific file is stuck, wait for the next deferred delete pass / metadata publish (both re-attempt unconfirmed live files) or restart the node to run startup SST backfill.

**Symptom: Delete operations are slow or stalling**

- **Cause**: Delete guard is re-uploading many files.
- **Action**:
  1. Check logs for "Re-uploading M unconfirmed files" — how many?
  2. Monitor `cloud_storage_retry_queue_size`. Once it drains, delete resumes.

**Symptom: High `cloud_storage_sync_latency_ms_seconds`**

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
- Set up alerts on the key metrics (`delete_guard_reupload_count`, `uploads_given_up_total`, `upload_failures_total`, `sync_latency_ms_seconds`) to catch issues early.

## Plugin Development: Adding Custom Cloud Storage Providers

This section guides developers on building and deploying new cloud storage provider plugins (e.g., Azure Blob Storage, ADLS, GCP).

### Plugin Architecture Overview

HugeGraph uses a **Service Provider Interface (SPI)** pattern to discover and load cloud storage providers at runtime:

1. **CloudStorageProvider interface**: Located in `hugegraph-store/hg-store-common`, defines the contract all providers must implement.
2. **ServiceLoader discovery**: Store node uses `java.util.ServiceLoader` to find all `CloudStorageProvider` implementations on the classpath.
3. **SPI selection**: At Store startup, `AppConfig` loads the provider specified in `cloud.storage.provider` config via `ServiceLoader`.
4. **Plugin packaging/runtime classpath**: Provider implementations are packaged as separate JAR modules and must be discoverable by Store at startup — either bundled into the distribution (Option A) or placed on the JVM launch classpath (Option B). There is no `plugins/` directory scan. See [Step 5](#step-5-package-for-runtime-classpath) below.

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
     * List all remote keys that start with the given key prefix (a plain string match, as in
     * S3 ListObjectsV2: pass a trailing "/" to list one directory). Returns keys relative to
     * the provider root (provider path prefix stripped).
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
- The stock distribution does not scan a `plugins/` directory (the test harness image under `docker/cloud-storage` is the exception: it launches Spring `PropertiesLauncher` with `-Dloader.path=/hugegraph-store/plugins`, which is a harness-only arrangement). `AppConfig` calls
  `ServiceLoader.load(CloudStorageProvider.class)` once at startup, which only sees jars inside the
  `hg-store-node` boot jar (`BOOT-INF/lib`, i.e. Option A) or on the JVM launch classpath.
- The shipped `bin/start-hugegraph-store.sh` builds the launch classpath from only the rocksdbjni
  jar and the `hg-store-node` boot jar (`-cp "<rocksdbjni>:<hg-store-node>" JarLauncher`), so there
  is no supported drop-in location for an external provider; Option A is the supported path.
- Appending a provider jar to that `-cp` is not enough by itself: classes on `-cp` are loaded by
  the system classloader, which cannot see `CloudStorageProvider` (it lives in `hg-store-common`
  inside `BOOT-INF/lib`), so loading the provider fails with `NoClassDefFoundError`. The
  `hg-store-common` jar would have to be on `-cp` as well.
- The provider is resolved once, so adding or replacing a provider jar requires a restart.
- To ship S3 in the stock distribution, build `hg-store-node` with the opt-in `-Ps3-cloud-storage` profile, which bundles `hg-store-cloud-s3` and the AWS SDK into the node fat jar (Option A). See "Enabling S3 on the stock distribution" in the [Store README](../README.md).
- For the harness plugin directory, the build stages only the SDK runtime closure in `hg-store-cloud-s3/target/dependency`: `hg-store-common` is `provided` scope there, because the node boot jar already supplies it and a second copy on `loader.path` would split `CloudStorageProvider` across classloaders.

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
- Kebab-case YAML properties are converted to uppercase with underscores (e.g., `access-key` → `ACCESS_KEY`)

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
4. **Error handling**: Throw `IOException` for operational (retryable) issues, or `CloudStorageNonRetryableException` for permanent failures that should skip whole-file retries. Implement your own internal retry logic where useful (see `S3CloudStorageProvider` for reference). The common `CloudUploadRetryQueue` performs whole-file retries by default (`upload-retry-max-attempts=3`) and gives up on (logs and counts) uploads that exhaust them.
5. **Logging**: Use SLF4J (via `@Slf4j`) for consistent log levels with Store.
6. **Configuration validation**: Validate all required fields in `init()` and fail fast.

### Contributing Your Provider

To upstream your new provider (e.g., `hg-store-cloud-newprovider`):

1. Create PR to `hugegraph-store/` with new provider module
2. Add tests in `hg-store-test/` that validate provider behavior
3. Update `docker/cloud-storage/` examples with new provider setup steps
4. Update `install-dist/` license/notice if adding third-party dependencies
