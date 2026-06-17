# HStore + Cloud Distributed Architecture - Complete Reference

## Overview

This document explains the **fully distributed HugeGraph architecture** where the server runs `backend=hstore`
with optional cloud sync (`hstore.cloud_enabled=true`). Each store node uses RocksDB with S3 sync enabled,
with its own S3 bucket for cloud durability.

> **Note:** The old `backend=rocksdb-cloud` (single-node, server-side) has been removed.
> Use `backend=hstore` with `hstore.cloud_*` options instead — it provides the same cloud
> durability with full distributed Raft replication on top.

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
│ │ Cloud Module        │  │ Cloud Module     │  │ Cloud Module    │ │
│ │  └─ commit-time    │  │  └─ commit-time │  │  └─ commit-time│ │
│ │     upload         │  │     upload      │  │     upload     │ │
│ │     (s3_first)     │  │     (s3_first)  │  │     (s3_first) │ │
│ │  └─ periodic       │  │  └─ periodic    │  │  └─ periodic   │ │
│ │     reconcile      │  │     reconcile   │  │     reconcile  │ │
│ │     (async mode)   │  │     (async mode)│  │     (async mode)│ │
│ ├─────────────────────┤  ├──────────────────┤  ├─────────────────┤ │
│ │ S3 Bucket:          │  │ S3 Bucket:       │  │ S3 Bucket:      │ │
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
        Default (`s3_first_mode=true`):
          - Synchronous S3 upload (incremental/full per config)
          - ACK returned only after S3 sync succeeds
        Optional fallback (`s3_first_mode=false`):
          - ACK returned after local/Raft commit
          - Periodic background sync/reconciliation uploads to S3
        
Store0: upload to store0-rocksdb/...
Store1: upload to store1-rocksdb/...
Store2: upload to store2-rocksdb/...
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
             (runtime attempts live auto-hydration from S3,
              reloads local DB, then retries read once)
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

# Optional: Enable cloud sync directly from server config
hstore.cloud_enabled=true
hstore.cloud_s3_bucket=hugegraph-data   # base name; stores append -0, -1, -2
hstore.cloud_s3_endpoint=http://minio:9000
hstore.cloud_s3_access_key=minioadmin
hstore.cloud_s3_secret_key=minioadmin
hstore.cloud_s3_path_style=true         # required for MinIO
hstore.cloud_sync_mode=sync             # sync (zero-loss) or async
```

### Per-Store Configuration (via environment variables)

Each store node reads cloud settings from environment variables.
Use `HstoreCloudConfigUtil.getStoreNodeEnvVars(config, storeIndex)` to generate them
from the server-side `hstore.cloud_*` configuration.

**Store0 Example:**
```bash
HG_STORE_ROCKSDB_CLOUD_ENABLED=true
HG_STORE_ROCKSDB_CLOUD_S3_BUCKET=hugegraph-data-0   # per-store isolated bucket
HG_STORE_ROCKSDB_CLOUD_S3_ENDPOINT=http://minio:9000
HG_STORE_ROCKSDB_CLOUD_S3_ACCESS_KEY=minioadmin
HG_STORE_ROCKSDB_CLOUD_S3_SECRET_KEY=minioadmin
HG_STORE_ROCKSDB_CLOUD_S3_PATH_STYLE=true
HG_STORE_ROCKSDB_CLOUD_S3_FIRST_MODE=true           # maps from hstore.cloud_sync_mode=sync
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
| **S3 buckets** | Shared MinIO | Separate per-store (or per-region) |
| **S3 credentials** | Shared (dev) | Per-store/per-node (prod) |
| **S3-first mode** | true (default) | true (recommended) |
| **Sync interval** | 30s (optional) | 60-300s (optional, reconciliation) |

## Bucket Isolation Benefits

### Per-Store Bucket Strategy

Each store has **its own isolated S3 bucket** for several reasons:

```
┌─────────────────────────────────────────────────────────────┐
│ Benefits of Separate Buckets                                │
├─────────────────────────────────────────────────────────────┤
│ 1. Independent quota/billing per store                      │
│    - Store0 quota ≠ Store1 quota (can auto-scale)           │
│                                                              │
│ 2. Fine-grained access control (IAM per bucket)            │
│    - Store0 only accesses store0-rocksdb                    │
│    - Prevents cross-store data leaks                        │
│                                                              │
│ 3. Disaster recovery isolation                              │
│    - Bucket deletion of store0 doesn't affect store1        │
│    - Can restore individual stores independently            │
│                                                              │
│ 4. Regional/DC distribution                                 │
│    - Store0 → S3 in us-east-1                              │
│    - Store1 → S3 in eu-west-1                              │
│    - Store2 → S3 in ap-southeast-1                         │
│                                                              │
│ 5. Performance isolation                                    │
│    - Store0 cloud sync doesn't compete with Store1          │
│    - Independent cloud API rate limiting                    │
└─────────────────────────────────────────────────────────────┘
```

## Failure Modes and Recovery

> Default behavior: S3-first mode is enabled (`rocksdb.cloud_s3_first_mode=true`,
> env: `HG_STORE_ROCKSDB_CLOUD_S3_FIRST_MODE=true`). Each committed write batch
> performs synchronous S3 upload before acknowledging commit.
>
> Optional fallback mode: set `rocksdb.cloud_s3_first_mode=false` to use
> periodic background cloud sync only.

### Scenario: Store0 RocksDB Corrupted

```
1. Store0 detects corruption in local RocksDB
   └─ Raft quorum: Store1 + Store2 = still OK (2 of 3)

2. Write requests: routed to Store1/2 (Store0 excluded)

3. Recovery options:
   a) FAST: Store0 syncs from S3 bucket (store0-rocksdb)
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
   └─ S3 may lag the latest acknowledged writes
   └─ Potential recent-write loss window depends on sync settings

2. AFTER (depends on sync recency):
   └─ Stores boot from S3 buckets
   └─ Raft identifies missing commits
   └─ Data consistency restored
   └─ May lose last N seconds of writes (depends on sync grace period)

3. Mitigation:
   └─ Best durability: set HG_STORE_ROCKSDB_CLOUD_S3_FIRST_MODE=true
   └─ Monitor sync errors and S3 latency/availability
```


## File Locations & References

- **Documentation**: 
  - Main guide: `docker/HStore-On-S3/RocksDB-Cloud.md`
  - Architecture (this file): `docker/HStore-On-S3/ARCHITECTURE.md`

- **Test Script**: `docker/HStore-On-S3/test-rocksdb-cloud-distributed.sh`

- **Server Config Options**: `hugegraph-server/hugegraph-hstore/src/main/java/.../HstoreOptions.java`

- **Config Propagation Utility**: `hugegraph-server/hugegraph-hstore/src/main/java/.../HstoreCloudConfigUtil.java`

- **Store Cloud Options**: `hugegraph-store/hg-store-rocksdb/src/main/java/.../cloud/RocksDBStoreCloudOptions.java`

## Glossary

| Term | Meaning |
|------|---------|
| **hstore** | HStore backend: stateless server routing layer that talks to store cluster via PD |
| **hstore.cloud_enabled** | Server-side flag to activate cloud sync; config propagated to store nodes |
| **rocksdb-cloud (store-level)** | RocksDB running on each store node with S3 sync enabled (via env vars) |
| **rocksdb-cloud (backend)** | ~~Deprecated~~ server-side `backend=rocksdb-cloud` — removed; use `hstore` instead |
| **PD** | Placement Driver: cluster coordinator, manages partition assignment |
| **Raft** | Consensus algorithm: ensures data consistency across replicas |
| **SST** | Sorted String Table: RocksDB internal file format for storage |
| **Cloud Sync** | Store-to-S3 upload path: synchronous on commit when `s3_first_mode=true`, periodic reconciliation when `s3_first_mode=false` |
| **Bucket** | S3 storage container: isolated namespace for objects |
| **Quorum** | Minimum subset of nodes needed for consensus (2 of 3 = OK) |

## Next Steps

1. **Run the automated test**: Follow `docker/HStore-On-S3/RocksDB-Cloud.md`
2. **Inspect configuration**: Review generated `hugegraph.properties` and `docker-compose.yml`
3. **Test manually**: Use `KEEP_UP=true` and query API while containers run
4. **Read full docs**: `docker/HStore-On-S3/RocksDB-Cloud.md` has step-by-step manual guide
5. **Production deployment**: Consider HA for PD and multiple servers behind load balancer

