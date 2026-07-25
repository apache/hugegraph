# HStore + Cloud Distributed Architecture - Complete Reference

## Overview

This document explains the **fully distributed HugeGraph architecture** where the server runs `backend=hstore`
with optional cloud sync (`hstore.cloud_enabled=true`). Each store node uses RocksDB with cloud storage sync enabled,
with its own cloud storage bucket for cloud durability (S3 is the default implementation).

### Architectural Positioning: HStore vs Cloud Integration

The cloud-storage work is an **additive integration**, not a replacement for existing HStore implementation:

| Component | Existing HStore | New Cloud Integration |
|---|---|---|
| **Role** | Distributed graph serving (server + PD + store + Raft) | Optional durability/rehydration extension for store RocksDB data |
| **Enabled by** | `backend=hstore` | `hstore.cloud_enabled=true` + store cloud env/config |
| **Runtime dependency** | Required for distributed deployment | Optional (can be fully disabled) |
| **Failure when disabled** | HStore still works as before | No cloud sync/rehydration path only |

In short: **HStore remains the core distributed data path**; cloud storage is a **separate optional capability** attached at the store layer.

## System Architecture

### Three-Layer Design (HStore Core + Optional Cloud Integration)

The following view separates the **existing HStore core path** from the **new cloud-storage integration path**.

```
┌──────────────────────────────────────────────────────────────────┐
│ Layer 1: API Gateway (HugeGraph Server)                          │
│ ─────────────────────────────────────────────────────────────────│
│ • Backend: hstore (stateless)                                    │
│ • Role: REST endpoint, query routing, authentication             │
│ • Data Storage: NONE (all data in stores)                        │
│ • Failure Impact: NONE - write/read latency + lose REST access   │
│ • Deployment: Can scale horizontally (all stateless)             │
└──────────────────────────────────────────────────────────────────┘
                         ↓ gRPC calls
┌──────────────────────────────────────────────────────────────────┐
│ Layer 2: Cluster Coordinator (Placement Driver - PD)             │
│ ─────────────────────────────────────────────────────────────────│
│ • Role: Manages store node membership, data partitioning         │
│ • Consensus: Single Raft instance coordinates 3 stores           │
│ • Failure Impact: Existing read/write ops can continue, but      │
│   membership/partition-management actions are blocked            │
│ • Backup: Should be HA in production (3 PD nodes)                │
└──────────────────────────────────────────────────────────────────┘
                         ↓ gRPC calls
┌─────────────────────────────────────────────────────────────────────────────┐
│ Layer 3: Graph Storage (Store Cluster)                                      │
│ ────────────────────────────────────────────────────────────────────────────│
│ Each store node contains BOTH:                                              │
│   (1) RocksDB (embedded, existing HStore core)                              │
│   (2) Cloud Module [NEW] (optional, additive integration)                   │
│                                                                             │
│ ┌──────────────────────┐ ┌──────────────────────┐ ┌──────────────────────┐  │
│ │ Store0               │ │ Store1               │ │ Store2               │  │
│ ├──────────────────────┤ ├──────────────────────┤ ├──────────────────────┤  │
│ │ RocksDB (embedded)   │ │ RocksDB (embedded)   │ │ RocksDB (embedded)   │  │
│ │  ├─ vertices         │ │  ├─ vertices         │ │  ├─ vertices         │  │
│ │  ├─ edges            │ │  ├─ edges            │ │  ├─ edges            │  │
│ │  └─ metadata         │ │  └─ metadata         │ │  └─ metadata         │  │
│ │----------------------│ │----------------------│ │----------------------│  │
│ │  Cloud Module [NEW]  │ │ Cloud Module [NEW]   │ │ Cloud Module [NEW]   │  │
│ │  ├─ sync SST upload  │ │  ├─ sync SST upload  │ │  ├─ sync SST upload  │  │
│ │  │  (mode=true)      │ │  │  (mode=true)      │ │  │  (mode=true)      │  │
│ │  └─ periodic recon.  │ │  └─ periodic recon.  │ │  └─ periodic recon.  │  │
│ │     (mode=false)     │ │     (mode=false)     │ │     (mode=false)     │  │
│ └──────────────────────┘ └──────────────────────┘ └──────────────────────┘  │
│                               ↓ object sync/download                        │
│ ┌────────────────────────────────────────────────────────────────────────┐  │
│ │ Cloud Storage                                                          │  │
│ ├────────────────────────────────────────────────────────────────────────┤  │
│ │    Bucket: store0-*        Bucket: store1-*         Bucket: store2-*   │  │
│ └────────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│ Core consistency: 3-way Raft replication (all writes replicate)             │
│ Failure mode: single store failure => reduced capacity,                     │
│                continued operations (2-node quorum OK for 3-node cluster)   │
└─────────────────────────────────────────────────────────────────────────────┘
```

Mode legend (single flag): `rocksdb.cloud.synchronous_sst_upload_mode=true` => synchronous cloud upload;
`rocksdb.cloud.synchronous_sst_upload_mode=false` => periodic background reconcile path.

## Data Flow Examples

### Write Operation Flow

```
User POST /graphs/hugegraph/graph/vertices
                ↓
        HugeGraph Server
        backend=hstore (routing only)
                ↓
        PD lookup: which partition?
                ↓
        Route to Store0/1/2 (leader)
                ↓
        RocksDB write path:
          - WAL append + MemTable (memstore) update
          - local commit
                ↓
        Raft: replicate to other stores
        (Store0 → Store1 + Store2)
                ↓
        Upload mode (`rocksdb.cloud.synchronous_sst_upload_mode=true`):
          - RocksDB flush thresholds materialize MemTable data to SST files
          - If `rocksdb.cloud.synchronous_sst_upload_mode=true`, cloud upload runs synchronously
          - If `rocksdb.cloud.synchronous_sst_upload_mode=false`, synchronous upload is disabled
        Periodic fallback (`rocksdb.cloud.synchronous_sst_upload_mode=false`):
          - ACK returned after local/Raft commit
          - Periodic background reconcile runs `syncNow(..., forceFlush=false)`
          - No forced flush in periodic mode; upload uses files already materialized by normal RocksDB flush/compaction
         
Store0: upload to cloud storage bucket for store0-rocksdb/...
Store1: upload to cloud storage bucket for store1-rocksdb/...
Store2: upload to cloud storage bucket for store2-rocksdb/...
```

### Read Operation Flow

```
User GET /graphs/hugegraph/graph/vertices
                ↓
        HugeGraph Server
        backend=hstore (routing only)
                ↓
        PD lookup: which partition?
                ↓
        Route to any Store (read can go to any replica)
                ↓
        RocksDB local read path
          ├─ Data available locally: serve from RocksDB
          └─ Local data missing/corrupted: recovery is required
             (runtime performs one on-demand rehydration from cloud storage,
              reloads local DB, then retries the read once)
                ↓
        Return to client (or error if recovery needed)
```

## Key Configuration Points

### Server Configuration
**File:** `hugegraph.properties`
```properties
backend=hstore                    # Distributed routing to store cluster
pd.peers=pd:8686                  # PD coordinator address
serializer=binary                 # RPC serialization format

# Optional: Enable cloud storage sync directly from server config
hstore.cloud_enabled=true
hstore.cloud_bucket=hugegraph-data      # base name; stores append -0, -1, -2
hstore.cloud_region=us-east-1
hstore.cloud_endpoint=http://minio:9000
hstore.cloud_path_style=true            # required for some S3-compatible providers
hstore.cloud_sync_mode=sync             # sync (zero-loss) or async
hstore.cloud_sync_interval_seconds=60
```

### Per-Store Configuration (via environment variables)

Each store node reads cloud storage settings from environment variables.
The following example matches the current store container wiring.

**Store0 Example:**
```bash
HG_STORE_ROCKSDB_CLOUD_ENABLED=true
HG_STORE_ROCKSDB_CLOUD_BUCKET=hugegraph-data-0   # per-store isolated bucket
HG_STORE_ROCKSDB_CLOUD_ENDPOINT=http://minio:9000
HG_STORE_ROCKSDB_CLOUD_REGION=us-east-1
HG_STORE_ROCKSDB_CLOUD_ACCESS_KEY=minioadmin
HG_STORE_ROCKSDB_CLOUD_SECRET_KEY=minioadmin
HG_STORE_ROCKSDB_CLOUD_PATH_STYLE=true
HG_STORE_ROCKSDB_CLOUD_SYNC_INTERVAL_SECONDS=30
HG_STORE_ROCKSDB_CLOUD_SYNC_INCREMENTAL=true
HG_STORE_ROCKSDB_CLOUD_SYNCHRONOUS_SST_UPLOAD_MODE=true    # single control flag: true=sync upload, false=periodic fallback
```

**Store1 & Store2:** Same as Store0 but bucket names `hugegraph-data-1` / `hugegraph-data-2`

### Production Considerations

| Aspect | Development | Production |
|--------|------------|-----------|
| **Server replicas** | 1 (stateless) | 2-3 (stateless, behind LB) |
| **PD nodes** | 1 (single point of failure) | 3 (Raft HA) |
| **Store nodes** | 3 | 9+ (sharding by region) |
| **Cloud storage buckets** | Shared cloud storage | Separate per-store (or per-region) |
| **Cloud storage credentials** | Shared (dev) | Per-store/per-node (prod) |
| **Synchronous SST upload mode** | true (default) | true (recommended) |
| **Sync interval** | 30s (optional) | 60-300s (optional, reconciliation) |

## Cloud Storage Bucket Isolation Benefits

### Per-Store Bucket Strategy

Each store has **its own isolated cloud storage bucket** for several reasons:

```
┌─────────────────────────────────────────────────────────────┐
│ Benefits of Separate Buckets                                │
├─────────────────────────────────────────────────────────────┤
│ 1. Independent quota/billing per store                      │
│    - Store0 quota ≠ Store1 quota (can auto-scale)           │
│                                                             │
│ 2. Fine-grained access control (IAM per bucket)             │
│    - Store0 only accesses store0 bucket                     │
│    - Prevents cross-store data leaks                        │
│                                                             │
│ 3. Disaster recovery isolation                              │
│    - Bucket deletion of store0 doesn't affect store1        │
│    - Can restore individual stores independently            │
│                                                             │
│ 4. Regional/DC distribution                                 │
│    - Store0 → cloud storage in us-east-1                    │
│    - Store1 → cloud storage in eu-west-1                    │
│    - Store2 → cloud storage in ap-southeast-1               │
│                                                             │
│ 5. Performance isolation                                    │
│    - Store0 cloud sync doesn't compete with Store1          │
│    - Independent cloud storage API rate limiting            │
└─────────────────────────────────────────────────────────────┘
```

## Failure Modes and Recovery

> Default upload timing is synchronous (`rocksdb.cloud.synchronous_sst_upload_mode=true`,
> env: `HG_STORE_ROCKSDB_CLOUD_SYNCHRONOUS_SST_UPLOAD_MODE=true`).
>
> If `rocksdb.cloud.synchronous_sst_upload_mode=false`, synchronous upload is disabled and
> periodic background reconciliation is used.

### Data Loss Analysis by Configuration Mode

**Data Loss Window Identification:**

In sync-upload mode (`rocksdb.cloud.synchronous_sst_upload_mode=true`), the system operates as follows:

```
Commit Acknowledged → WAL + MemTable (Raft replicated)
        ↓ (when thresholds met)
RocksDB materializes MemTable → SST files on local disk
        ↓ (WatchService detects .sst creation)
queueSstSync() schedules → syncNow(false, false)
        ↓ (synchronous cloud upload if `rocksdb.cloud.synchronous_sst_upload_mode=true`)
Cloud storage upload STARTS
        ↓ (at some point in time)
Cloud storage upload COMPLETES
```

**Critical Data Loss Window:** 
- **From**: SST file creation (or flush threshold crossed)
- **To**: Cloud upload completion
- **Duration**: Depends on:
  - RocksDB flush interval (threshold-triggered: variable, typically seconds)
  - Cloud storage upload latency (typically 100ms - 5s for SST files)
  - Network/cloud API health

**Scenarios Where Data Loss Occurs:**

| Scenario | Data Loss? | Why | Probability |
|----------|-----------|-----|-------------|
| **Single store crash before cloud sync** | NO | Raft has data; replicas are quorum (2/3) | Low |
| **Single store crash during cloud upload** | NO | Upload continues on cloud; Raft quorum OK | Low |
| **2 of 3 stores crash (quorum lost) before cloud sync** | YES | Only 1 replica has data; lost if that replica also crashes | Very Low |
| **All 3 stores crash during cloud upload (disk intact)** | NO | Raft log on disk; replay on boot; cloud has partial files | Medium |
| **All 3 stores lose local disks during cloud upload** | YES | Raft log lost; cloud upload incomplete | Medium |
| **All 3 stores lose local disks BEFORE cloud sync starts** | YES | Data only in Raft log (lost); cloud has older version | Medium |

**Detailed Failure Scenario: Catastrophic Disk Loss**

```
Timeline:
T0: Write committed
    └─ In: WAL (local) + MemTable + Raft log (3 replicas)
    └─ Not yet: Cloud storage

T1: Threshold triggered, MemTable → SST files (local disk)
    └─ In: SST files (local) + Raft log (3 replicas)
    └─ Not yet: Cloud storage

T2: WatchService detects .sst creation
T3: rocksdb.cloud.synchronous_sst_upload_mode=true
    └─ queueSstSync() performs synchronous cloud upload

T4: All 3 stores' local disks fail SIMULTANEOUSLY
    └─ SST files lost (not yet uploaded)
    └─ WAL lost
    └─ Raft log lost
    └─ Cloud storage has OLDER snapshot (last completed sync, minutes ago)

T5: Stores boot from cloud
    └─ Restore from cloud storage
    └─ Recovery window: all writes since last completed cloud sync
    └─ DATA LOSS: Yes
```

**Key Differences from Old cloud_first_mode=true:**

| Aspect | Old cloud_first_mode=true | Current mode (`rocksdb.cloud.synchronous_sst_upload_mode=true`) | Fallback mode (`rocksdb.cloud.synchronous_sst_upload_mode=false`) |
|--------|---------------------------|---------------------------------------------------|------------------------------------------------------|
| **Flush trigger** | Every commit (forced) | RocksDB thresholds (natural) | RocksDB thresholds (natural) |
| **Cloud sync trigger** | Every commit (synchronous fence) | SST file creation event | Periodic reconcile timer |
| **Cloud upload timing** | Synchronous (commit waits) | Synchronous (config=true) | Background periodic (config=false) |
| **Data loss window** | Brief (commit-time to sync complete) | Near-zero cloud durability gap | Wider (depends on interval) |
| **Performance** | Slowest | Middle (flush-path latency trade-off) | Fastest writes |

**Recommended Mitigation Strategies:**

1. **Use Raft replication across 3+ stores**: Ensures quorum survives single-node failures
   ```
   3 stores: 1 can fail, 2 survive (quorum OK)
   5 stores: 2 can fail, 3 survive (quorum OK)
   ```

2. **Monitor cloud sync latency and errors**:
   ```bash
   # Log entries to watch for:
   # WARN "Synchronous SST cloud upload failed for ..."
   # WARN "Failed to acquire syncInProgress lock after..."
   ```

3. **Use persistent local storage** (not ephemeral):
   - Store nodes must have durable local disks (SSD, EBS, etc.)
   - Ephemeral storage + catastrophic failure = guaranteed data loss

4. **Enable periodic reconciliation** even with SST sync:
   ```bash
   HG_STORE_ROCKSDB_CLOUD_SYNCHRONOUS_SST_UPLOAD_MODE=false  # Periodic fallback mode
   HG_STORE_ROCKSDB_CLOUD_SYNC_INTERVAL_SECONDS=60           # Periodic sync every 60s
   ```

5. **Minimize data loss window**:
   - Tune RocksDB flush thresholds to create SSTs more frequently:
     ```
     rocksdb.write_buffer_size=64MB        # smaller = faster flush (more SSTs)
     rocksdb.max_write_buffer_number=3     # trigger flush earlier
     ```
   - Accept slightly higher cloud API costs for lower RPO (Recovery Point Objective)

### Recovery Point Objective (RPO) & Recovery Time Objective (RTO)

**RPO = Maximum acceptable data loss**
**RTO = Maximum acceptable downtime**

#### Scenario 1: Single Store Failure (Most Common)
| Metric | Value | Notes |
|--------|-------|-------|
| **RPO** | 0 seconds | No data loss; Raft has all writes; other replicas survive |
| **RTO** | 30-60 seconds | Raft elects new leader; routes continue |
| **Cloud sync** | Not needed (Raft covers) | But sync still runs for disaster recovery preparation |

#### Scenario 2: Two Stores Fail (Quorum Lost, Rare)
| Metric | Value | Notes |
|--------|-------|-------|
| **RPO** | 0 seconds (if last survivor has latest write) | Depends on which stores survive |
| **RTO** | 5-10 minutes | Failed stores restart; Raft resync from survivor |
| **Cloud sync** | Not directly used | Survivor boots other stores from cloud |

#### Scenario 3: All Stores Fail with Persistent Local Disk (Rare)
| Metric | Value | Notes |
|--------|-------|-------|
| **RPO** | Last completed cloud sync | Typically 30-60 seconds old (depends on sync frequency) |
| **RTO** | 10-30 minutes | Boot from cloud + Raft recovery |
| **Cloud sync** | Critical for recovery | Cloud is single source of truth after disk failure |

#### Scenario 4: All Stores Fail with Ephemeral Local Disk (Catastrophic, Not Recommended)
| Metric | Value | Notes |
|--------|-------|-------|
| **RPO** | Last completed cloud sync | Same as Scenario 3 |
| **RTO** | 30-60 minutes | Cloud download + re-index + Raft recovery slower |
| **Cloud sync** | Only option | No local recovery possible |

**How to Improve RPO in SST-Driven Mode:**

| Configuration | RPO Improvement | Trade-offs |
|---|---|---|
| `write_buffer_size=64MB` (default 256MB) | Better; SSTs created 4x faster | More SST files; more cloud sync calls |
| `SYNC_INTERVAL_SECONDS=30` (default 60) | Better; periodic fallback more frequent | More cloud API calls |
| `SYNC_INTERVAL_SECONDS=10` | Best; catch any gaps | Highest cloud API cost |
| Persistent local disk + good network | Best possible | Already configured for production |

**Target RPO for Production:** 
- **Best case**: 0-5 seconds (single store failure with Raft)
- **Disaster case**: 30-60 seconds (all stores fail; recover from cloud)

### Scenario: Store0 RocksDB Corrupted (Recoverable)

```
1. Store0 detects corruption in local RocksDB (e.g., checksum failure)
   └─ Raft quorum: Store1 + Store2 = still OK (2 of 3)

2. Write requests: routed to Store1/2 (Store0 excluded)

3. Recovery options:
    a) FAST: Store0 syncs from cloud storage bucket (store0-rocksdb)
       └─ Restores all SST files (from last completed sync)
       └─ Raft replay resync fills any gaps
       └─ ETA: minutes (depends on dataset size + cloud latency)
       └─ Data loss: NO (if Raft had the write; Raft is single source of truth)

    b) SLOW: Delete Store0, replace with new node
       └─ PD adds new store3
       └─ Raft rebalances: 3 stores again
       └─ ETA: hours (data transfer from other stores)
       └─ Data loss: NO (Raft rebalancing transfers all data)

4. Graph operations: Continue throughout (no downtime)
```

### Scenario: All 3 Stores Lose Local Disk (Catastrophic, Data Loss Possible)

```
1. All 3 stores' local disks fail simultaneously (or in quick succession)
   └─ Raft log is gone (normally on-disk)
   └─ Local SST files are gone
   └─ Cloud storage has last COMPLETED sync (may be seconds/minutes old)

2. Recovery phase:
   └─ Stores boot and discover local disks corrupted
   └─ No Raft consensus possible (need at least 1 survivor)
   └─ Fallback: restore from cloud storage
   └─ Raft log replayed from cloud: identifies writes since last sync
   └─ Data loss window: writes between last completed cloud sync and disk failure

3. Mitigation (to reduce RPO):
   └─ Reduce RocksDB MemTable flush thresholds → more frequent SST files
   └─ Monitor `HG_STORE_ROCKSDB_CLOUD_SYNC_INTERVAL_SECONDS` (periodic fallback)
   └─ Ensure network/cloud storage is healthy (monitor sync latency & errors)
   └─ Set `HG_STORE_ROCKSDB_CLOUD_SYNCHRONOUS_SST_UPLOAD_MODE=true` for strict durability
   └─ Use dedicated, persistent local storage (not ephemeral)
```


## File Locations & References

- **Documentation**: 
   - Main guide: `docker/cloud-storage/RocksDB-Cloud.md`
   - Architecture (this file): `docker/cloud-storage/ARCHITECTURE.md`
   - **Data Loss Analysis** (detailed failure scenarios): `docker/cloud-storage/DATA-LOSS-ANALYSIS.md` ⭐

- **Test Script**: `docker/cloud-storage/test-rocksdb-cloud-distributed.sh`

- **Server Config Options**: `hugegraph-server/hugegraph-hstore/src/main/java/.../HstoreOptions.java`

- **Config Propagation Utility**: `hugegraph-server/hugegraph-hstore/src/main/java/.../HstoreCloudConfigUtil.java`

- **Store Cloud Options**: `hugegraph-store/hg-store-rocksdb/src/main/java/.../cloud/RocksDBStoreCloudOptions.java`

## Glossary

| Term | Meaning |
|------|---------|
| **hstore** | HStore backend: stateless server routing layer that talks to store cluster via PD |
| **hstore.cloud_enabled** | Server-side flag to activate cloud storage sync; config propagated to store nodes |
| **rocksdb-cloud (store-level)** | RocksDB running on each store node with cloud storage sync enabled (via env vars) |
| **rocksdb-cloud (backend)** | ~~Deprecated~~ server-side `backend=rocksdb-cloud` — removed; use `hstore` instead |
| **PD** | Placement Driver: cluster coordinator, manages partition assignment |
| **Raft** | Consensus algorithm: ensures data consistency across replicas |
| **SST** | Sorted String Table: RocksDB internal file format for storage |
| **Cloud Sync** | Store-to-cloud-storage upload path controlled by `rocksdb.cloud.synchronous_sst_upload_mode`: synchronous upload when `true`, periodic reconciliation when `false` |
| **Bucket** | Cloud storage container: isolated namespace for objects |
| **Quorum** | Minimum subset of nodes needed for consensus (2 of 3 = OK) |

## Next Steps

1. **Run the automated test**: Follow `docker/cloud-storage/RocksDB-Cloud.md`
2. **Inspect configuration**: Review generated `hugegraph.properties` and `docker-compose.yml`
3. **Test manually**: Use `KEEP_UP=true` and query API while containers run
4. **Read full docs**: `docker/cloud-storage/RocksDB-Cloud.md` has step-by-step manual guide
5. **Production deployment**: Consider HA for PD and multiple servers behind load balancer


## Pluggable Cloud Storage Architecture

HugeGraph supports a **pluggable cloud storage provider** architecture that enables support for multiple cloud storage vendors without modifying core code.

This architecture is intentionally separated from core HStore semantics:

- **HStore responsibilities stay unchanged:** request routing, partition placement, Raft replication, and local RocksDB reads/writes.
- **Cloud module responsibilities are orthogonal:** SST/object upload, object download, and post-failure rehydration.
- **Operational choice:** keep classic HStore behavior (cloud off) or enable cloud durability (cloud on) without switching backend type.

### Core Components

```
┌─────────────────────────────────────────────────┐
│ RocksDBCloudSession                             │
│ (Cloud sync orchestration - vendor-neutral)     │
└──────────────┬──────────────────────────────────┘
               │
               ↓ (uses)
┌──────────────────────────────────────────────────┐
│ CloudStorageClient Interface                     │
│ - provider(): String                             │
│ - uploadDirectory()                              │
│ - uploadIncremental()                            │
│ - downloadDirectory()                            │
│ - close()                                        │
└──────────────┬───────────────────────────────────┘
               │
               ↓ (discovered via ServiceLoader)
┌──────────────────────────────────────────────────────────────┐
│ CloudStorageRegistry                                         │
│ (Manages available providers via ServiceLoader)              │
├──────────────────────────────────────────────────────────────┤
│ Registered Providers:                                        │
│ ├─ S3CompatibleStorageProvider (built-in)                    │
│ │   └─ Supports: AWS S3, LocalStack, Wasabi, etc.            │
│ │        (any S3-compatible storage)                         │
│ ├─ AzureStorageProvider (plugin JAR)                         │
│ ├─ GcsStorageProvider (plugin JAR)                           │
│ └─ Custom providers (user-implemented plugins)               │
└──────────────────────────────────────────────────────────────┘
```

### Provider Selection

Providers are selected at runtime via configuration (choose one):

- **S3-compatible storage (default):**
  ```properties
  rocksdb.cloud.provider=s3
  ```

- **Azure Blob Storage (when plugin JAR added):**
  ```properties
  rocksdb.cloud.provider=azure
  ```

- **Google Cloud Storage (when plugin JAR added):**
  ```properties
  rocksdb.cloud.provider=gcs
  ```

### Adding New Cloud Providers

New cloud storage providers can be added as **external plugins** without modifying HugeGraph source code.

**Process:**
1. Implement `CloudStorageProvider` factory interface
2. Implement `CloudStorageClient` interface with vendor SDK
3. Register via `META-INF/services/org.apache.hugegraph.rocksdb.access.cloud.CloudStorageProvider`
4. Package as JAR and add to HugeGraph classpath
5. Configure via `rocksdb.cloud.provider=<your-provider>`
6. Restart HugeGraph

**Reference Implementation:**
- Sample plugin: `examples/cloud-storage-plugin/SampleCloudStorage/`
- Developer guide: `examples/cloud-storage-plugin/PLUGIN_DEVELOPMENT_GUIDE.md`

### Built-in Providers

#### S3-Compatible Provider (Built-in, Default)
- **Provider ID:** `s3`
- **Description:** Default cloud storage provider that supports S3-compatible APIs
- **Supports:**
   - AWS S3
   - LocalStack
   - Wasabi
   - DigitalOcean Spaces
   - And any other S3-compatible object storage (including MinIO)

```properties
rocksdb.cloud.provider=s3
rocksdb.cloud_region=us-east-1
rocksdb.cloud_endpoint=https://s3-compatible-endpoint.example.com:9000
rocksdb.cloud_access_key=access_key
rocksdb.cloud_secret_key=secret_key
rocksdb.cloud_path_style=true  # required for some S3-compatible providers
```

### Plugin Architecture Benefits

| Benefit | Description |
|---------|------------|
| **No Code Changes** | Add new provider via plugin JAR without recompiling HugeGraph |
| **Vendor Isolation** | Each provider in separate JAR with independent dependencies |
| **Lazy Discovery** | Providers loaded on first use via Java ServiceLoader |
| **Multi-Cloud Support** | Multiple providers can coexist; config determines which is used |
| **Future-Proof** | Adding Azure, GCS, or other providers requires no core changes |

---
