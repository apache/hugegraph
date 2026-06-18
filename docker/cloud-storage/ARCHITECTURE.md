# HStore + Cloud Distributed Architecture - Complete Reference

## Overview

This document explains the **fully distributed HugeGraph architecture** where the server runs `backend=hstore`
with optional cloud sync (`hstore.cloud_enabled=true`). Each store node uses RocksDB with cloud storage sync enabled,
with its own cloud storage bucket for cloud durability (S3 is the default implementation).

## System Architecture

### Three-Layer Design

```
┌──────────────────────────────────────────────────────────────────┐
│ Layer 1: API Gateway (HugeGraph Server)                         │
│ ─────────────────────────────────────────────────────────────── │
│ • Backend: hstore (stateless)                                   │
│ • Role: REST endpoint, query routing, authentication            │
│ • Data Storage: NONE (all data in stores)                       │
│ • Failure Impact: NONE - write/read latency + lose REST access  │
│ • Deployment: Can scale horizontally (all stateless)            │
└──────────────────────────────────────────────────────────────────┘
                         ↓ gRPC calls
┌──────────────────────────────────────────────────────────────────┐
│ Layer 2: Cluster Coordinator (Placement Driver - PD)            │
│ ─────────────────────────────────────────────────────────────── │
│ • Role: Manages store node membership, data partitioning        │
│ • Consensus: Single Raft instance coordinates 3 stores          │
│ • Failure Impact: Existing read/write ops can continue, but      │
│   membership/partition-management actions are blocked            │
│ • Backup: Should be HA in production (3 PD nodes)                │
└──────────────────────────────────────────────────────────────────┘
                         ↓ gRPC calls
┌─────────────────────────────────────────────────────────────────────────┐
│ Layer 3: Graph Storage (Store Cluster)                                 │
│ ───────────────────────────────────────────────────────────────────── │
│ Each Store Node:                                                      │
│ ┌─────────────────────┐  ┌──────────────────┐  ┌─────────────────┐ │
│ │ Store0              │  │ Store1           │  │ Store2          │ │
│ ├─────────────────────┤  ├──────────────────┤  ├─────────────────┤ │
│ │ RocksDB (embedded)  │  │ RocksDB          │  │ RocksDB         │ │
│ │  ├─ vertices       │  │  ├─ vertices    │  │  ├─ vertices    │ │
│ │  ├─ edges         │  │  ├─ edges       │  │  ├─ edges       │ │
│ │  └─ metadata      │  │  └─ metadata    │  │  └─ metadata    │ │
│ │                     │  │                  │  │                 │ │
| │ Cloud Module        │  │ Cloud Module     │  │ Cloud Module    │ │
│ │  └─ commit-time    │  │  └─ commit-time │  │  └─ commit-time│ │
│ │     upload         │  │     upload      │  │     upload     │ │
│ │     (cloud-first)  │  │     (cloud-first) │  │  (cloud-first) │ │
│ │  └─ periodic       │  │  └─ periodic    │  │  └─ periodic   │ │
│ │     reconcile      │  │     reconcile   │  │     reconcile  │ │
│ │     (async mode)   │  │     (async mode)│  │     (async mode)│ │
│ ├─────────────────────┤  ├──────────────────┤  ├─────────────────┤ │
│ │ Cloud Bucket:       │  │ Cloud Bucket:    │  │ Cloud Bucket:   │ │
│ │ store0-rocksdb      │  │ store1-rocksdb   │  │ store2-rocksdb  │ │
│ │                     │  │                  │  │                 │ │
│ │ Credentials:        │  │ Credentials:     │  │ Credentials:    │ │
│ │ (via env var)       │  │ (via env var)    │  │ (via env var)   │ │
│ └─────────────────────┘  └──────────────────┘  └─────────────────┘ │
│                                                                       │
│ Consensus: 3-way Raft replication (all writes replicate)           │
│ Failure Mode: Single store failure = reduced capacity, continued   │
│              operations (2-node quorum OK for 3-node cluster)      │
└─────────────────────────────────────────────────────────────────────┘
```

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
        RocksDB write + local commit
                ↓
        Raft: replicate to other stores
        (Store0 → Store1 + Store2)
                ↓
        Default (`cloud_first_mode=true`):
          - Synchronous cloud storage upload (incremental/full per config)
          - ACK returned only after cloud storage sync succeeds
        Optional fallback (`cloud_first_mode=false`):
          - ACK returned after local/Raft commit
          - Periodic background sync/reconciliation uploads to cloud storage
         
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
hstore.cloud_provider=s3          # Cloud storage provider (default: s3)
hstore.cloud_bucket=hugegraph-data      # base name; stores append -0, -1, -2
hstore.cloud_endpoint=http://minio:9000
hstore.cloud_access_key=minioadmin
hstore.cloud_secret_key=minioadmin
hstore.cloud_path_style=true            # required for some S3-compatible providers
hstore.cloud_sync_mode=sync             # sync (zero-loss) or async
```

### Per-Store Configuration (via environment variables)

Each store node reads cloud storage settings from environment variables.
Use `HstoreCloudConfigUtil.getStoreNodeEnvVars(config, storeIndex)` to generate them
from the server-side `hstore.cloud_*` configuration.

**Store0 Example:**
```bash
HG_STORE_ROCKSDB_CLOUD_ENABLED=true
HG_STORE_ROCKSDB_CLOUD_PROVIDER=s3           # Cloud storage provider (default: s3)
HG_STORE_ROCKSDB_CLOUD_BUCKET=hugegraph-data-0   # per-store isolated bucket
HG_STORE_ROCKSDB_CLOUD_ENDPOINT=http://minio:9000
HG_STORE_ROCKSDB_CLOUD_ACCESS_KEY=minioadmin
HG_STORE_ROCKSDB_CLOUD_SECRET_KEY=minioadmin
HG_STORE_ROCKSDB_CLOUD_PATH_STYLE=true
HG_STORE_ROCKSDB_CLOUD_CLOUD_FIRST_MODE=true           # maps from hstore.cloud_sync_mode=sync
HG_STORE_ROCKSDB_CLOUD_SYNC_INTERVAL_SECONDS=30
HG_STORE_ROCKSDB_CLOUD_SYNC_INCREMENTAL=true
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
| **Cloud-first mode** | true (default) | true (recommended) |
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
│                                                              │
│ 2. Fine-grained access control (IAM per bucket)            │
│    - Store0 only accesses store0 bucket                     │
│    - Prevents cross-store data leaks                        │
│                                                              │
│ 3. Disaster recovery isolation                              │
│    - Bucket deletion of store0 doesn't affect store1        │
│    - Can restore individual stores independently            │
│                                                              │
│ 4. Regional/DC distribution                                 │
│    - Store0 → cloud storage in us-east-1                   │
│    - Store1 → cloud storage in eu-west-1                   │
│    - Store2 → cloud storage in ap-southeast-1              │
│                                                              │
│ 5. Performance isolation                                    │
│    - Store0 cloud sync doesn't compete with Store1          │
│    - Independent cloud storage API rate limiting           │
└─────────────────────────────────────────────────────────────┘
```

## Failure Modes and Recovery

> Default behavior: Cloud-first mode is enabled (`rocksdb.cloud_cloud_first_mode=true`,
> env: `HG_STORE_ROCKSDB_CLOUD_CLOUD_FIRST_MODE=true`). Each committed write batch
> performs synchronous cloud storage upload before acknowledging commit.
>
> Optional fallback mode: set `rocksdb.cloud_cloud_first_mode=false` to use
> periodic background cloud storage sync only.

### Scenario: Store0 RocksDB Corrupted

```
1. Store0 detects corruption in local RocksDB
   └─ Raft quorum: Store1 + Store2 = still OK (2 of 3)

2. Write requests: routed to Store1/2 (Store0 excluded)

3. Recovery options:
    a) FAST: Store0 syncs from cloud storage bucket (store0-rocksdb)
       └─ Restores all SST files
       └─ Raft resync fills gaps
       └─ TBD: minutes

    b) SLOW: Delete Store0, replace with new node
       └─ PD adds new store3
       └─ Raft rebalances: 3 stores again
       └─ Can be hours (data transfer)

4. Graph operations: Continue throughout (no downtime)
```

### Scenario: All 3 Stores Lose Local Disk

```
1. If local disks fail before latest upload completes:
   └─ Cloud storage may lag the latest acknowledged writes
   └─ Potential recent-write loss window depends on sync settings

2. AFTER (depends on sync recency):
   └─ Stores boot from cloud storage buckets
   └─ Raft identifies missing commits
   └─ Data consistency restored
   └─ May lose last N seconds of writes (depends on sync grace period)

3. Mitigation:
   └─ Best durability: set HG_STORE_ROCKSDB_CLOUD_CLOUD_FIRST_MODE=true
   └─ Monitor sync errors and cloud storage latency/availability
```


## File Locations & References

- **Documentation**: 
   - Main guide: `docker/cloud-storage/RocksDB-Cloud.md`
   - Architecture (this file): `docker/cloud-storage/ARCHITECTURE.md`

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
| **Cloud Sync** | Store-to-cloud-storage upload path: synchronous on commit when `cloud_first_mode=true`, periodic reconciliation when `cloud_first_mode=false` |
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

### Core Components

```
┌─────────────────────────────────────────────────┐
│ RocksDBCloudSession                             │
│ (Cloud sync orchestration - vendor-neutral)     │
└──────────────┬──────────────────────────────────┘
               │
               ↓ (uses)
┌─────────────────────────────────────────────────┐
│ CloudStorageClient Interface                     │
│ - provider(): String                             │
│ - uploadDirectory()                              │
│ - uploadIncremental()                            │
│ - downloadDirectory()                            │
│ - close()                                        │
└──────────────┬──────────────────────────────────┘
               │
               ↓ (discovered via ServiceLoader)
┌──────────────────────────────────────────────────────────────┐
│ CloudStorageRegistry                                         │
│ (Manages available providers via ServiceLoader)              │
├──────────────────────────────────────────────────────────────┤
│ Registered Providers:                                        │
│ ├─ S3CompatibleStorageProvider (built-in)                   │
│ │   └─ Supports: AWS S3, LocalStack, Wasabi, etc. (any S3-compatible storage)   │
│ ├─ AzureStorageProvider (plugin JAR)                        │
│ ├─ GcsStorageProvider (plugin JAR)                          │
│ └─ Custom providers (user-implemented plugins)              │
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
