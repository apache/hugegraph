# Distributed Architecture

This document provides a deep dive into HugeGraph Store's distributed architecture, including the three-tier design, Raft consensus mechanisms, partition management, and coordination with HugeGraph PD.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Three-Tier Architecture](#three-tier-architecture)
- [Raft Consensus Mechanism](#raft-consensus-mechanism)
- [Partition Management](#partition-management)
- [PD Coordination](#pd-coordination)
- [Data Flow and Control Flow](#data-flow-and-control-flow)

---

## Architecture Overview

HugeGraph Store is designed as a **distributed, partition-based storage system** that provides:

1. **Strong Consistency**: Raft consensus ensures linearizable reads and writes
2. **High Availability**: Multi-replica design with automatic failover
3. **Horizontal Scalability**: Dynamic partition allocation across Store nodes
4. **Efficient Query Processing**: Query pushdown and parallel execution across partitions

### Design Philosophy

- **Partition as Unit of Distribution**: Each partition is independently managed and replicated
- **Raft per Partition**: Each partition has its own Raft group, enabling fine-grained replication control
- **Centralized Metadata**: PD serves as the single source of truth for cluster topology and partition assignment
- **Separation of Concerns**: Data plane (Store) and control plane (PD) are decoupled

---

## Three-Tier Architecture

HugeGraph Store follows a layered architecture with clear separation of responsibilities:

```
┌─────────────────────────────────────────────────────────────┐
│                     Client Layer                             │
│  (hugegraph-server with hg-store-client library)            │
│  - Graph API requests                                        │
│  - Query execution planning                                  │
│  - Partition routing via PD                                  │
└───────────────────────┬─────────────────────────────────────┘
                        │ gRPC (Query, Batch, Session)
                        ↓
┌─────────────────────────────────────────────────────────────┐
│                  Store Node Layer                            │
│  (hg-store-node: multiple Store instances)                  │
│                                                              │
│  ┌─────────────────┐  ┌─────────────────┐                  │
│  │  gRPC Services  │  │  PD Integration │                  │
│  │  - Session      │  │  - Registration │                  │
│  │  - Query        │  │  - Heartbeat    │                  │
│  │  - State        │  │  - Partition    │                  │
│  └────────┬────────┘  └────────┬────────┘                  │
│           │                     │                           │
│           ↓                     ↓                           │
│  ┌──────────────────────────────────────┐                  │
│  │      HgStoreEngine (singleton)       │                  │
│  │  - Manages all partition engines     │                  │
│  │  - Coordinates with PD               │                  │
│  │  - Handles partition lifecycle       │                  │
│  └─────────────────┬────────────────────┘                  │
│                    │                                        │
│       ┌────────────┼────────────┐                          │
│       │            │            │                          │
│       ↓            ↓            ↓                          │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐                   │
│  │Partition│  │Partition│  │Partition│  (N partitions)   │
│  │Engine 1 │  │Engine 2 │  │Engine N │                   │
│  │         │  │         │  │         │                   │
│  │ Raft    │  │ Raft    │  │ Raft    │                   │
│  │ Group 1 │  │ Group 2 │  │ Group N │                   │
│  └────┬────┘  └────┬────┘  └────┬────┘                   │
│       │            │            │                          │
└───────┼────────────┼────────────┼──────────────────────────┘
        │            │            │
        ↓            ↓            ↓
┌─────────────────────────────────────────────────────────────┐
│              Storage Engine Layer                            │
│  (hg-store-core + hg-store-rocksdb)                         │
│                                                              │
│  ┌────────────────────────────────────────────────────┐    │
│  │     PartitionEngine (per partition)                │    │
│  │  ┌──────────────────────────────────────────────┐  │    │
│  │  │  Raft State Machine                          │  │    │
│  │  │  - Apply log entries                         │  │    │
│  │  │  - Snapshot creation/loading                 │  │    │
│  │  │  - Business logic delegation                 │  │    │
│  │  └────────────────┬─────────────────────────────┘  │    │
│  │                   │                                 │    │
│  │                   ↓                                 │    │
│  │  ┌──────────────────────────────────────────────┐  │    │
│  │  │  BusinessHandler                             │  │    │
│  │  │  - Put/Get/Delete/Scan operations            │  │    │
│  │  │  - Query processing (filters, aggregations)  │  │    │
│  │  │  - Transaction management                    │  │    │
│  │  └────────────────┬─────────────────────────────┘  │    │
│  │                   │                                 │    │
│  └───────────────────┼─────────────────────────────────┘    │
│                      │                                      │
│                      ↓                                      │
│  ┌─────────────────────────────────────────────────────┐   │
│  │         RocksDB Session & Store                     │   │
│  │  - Column families for different data types         │   │
│  │  - LSM tree storage                                 │   │
│  │  - Compaction and caching                           │   │
│  │  - Persistent storage on disk                       │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Layer Responsibilities

#### 1. Client Layer (`hg-store-client`)

**Location**: `hugegraph-server/hugegraph-hstore` (backend implementation) + `hugegraph-store/hg-store-client`

**Responsibilities**:
- **Service Discovery**: Connects to PD to discover Store nodes
- **Partition Routing**: Determines which Store node holds a specific partition
- **Request Distribution**: Routes requests to appropriate Store nodes based on partition mapping
- **Connection Management**: Maintains gRPC connection pool to Store nodes
- **Failover Handling**: Retries failed requests and handles Store node failures

**Key Classes**:
- `HgStoreClient`: Main client interface
- `HgStoreSession`: Session-based operations (put, get, delete, scan)
- `HgStoreNodeManager`: Manages connections to Store nodes

#### 2. Store Node Layer (`hg-store-node`)

**Location**: `hugegraph-store/hg-store-node`

**Responsibilities**:
- **gRPC Service Endpoints**: Exposes gRPC services for client requests
- **Partition Engine Management**: Creates and manages `PartitionEngine` instances for assigned partitions
- **PD Integration**: Registers with PD, sends heartbeats, receives partition assignment commands
- **Request Routing**: Forwards requests to the appropriate `PartitionEngine` based on partition ID
- **Cluster Coordination**: Participates in Raft consensus and partition leadership

**Key Components**:

**gRPC Services** (7 proto files in `hg-store-grpc/src/main/proto/`):
1. `HgStoreSession` (`store_session.proto`): Session management, batch operations
2. `QueryService` (`query.proto`): Query pushdown operations
3. `GraphStore` (`graphpb.proto`): Graph-specific operations (vertex, edge scanning)
4. `HgStoreState` (`store_state.proto`): Node state and cluster state queries
5. `HgStoreStreamMeta` (`store_stream_meta.proto`): Streaming metadata operations
6. `Healthy` (`healthy.proto`): Health check endpoints
7. Common types (`store_common.proto`): Shared data structures

**HgStoreEngine** (`hg-store-core/src/main/java/.../HgStoreEngine.java`):
- Singleton per Store node
- Manages lifecycle of all `PartitionEngine` instances
- Coordinates with PD via `DefaultPdProvider`
- Handles partition creation, deletion, and state transitions
- Sends heartbeats to PD (`HeartbeatService`)

**PartitionEngine** (`hg-store-core/src/main/java/.../PartitionEngine.java`):
- One instance per partition replica on this Store node
- Wraps a Raft node (`RaftEngine`) for consensus
- Delegates business logic to `BusinessHandler`
- Manages partition state (Normal, Offline, etc.)

#### 3. Storage Engine Layer (`hg-store-core` + `hg-store-rocksdb`)

**Location**: `hugegraph-store/hg-store-core` and `hugegraph-store/hg-store-rocksdb`

**Responsibilities**:
- **Raft State Machine**: Implements Raft state machine for log application
- **Business Logic**: Executes graph operations (put, get, delete, scan)
- **Query Processing**: Handles query pushdown (filters, aggregations, index scans)
- **Persistent Storage**: Manages RocksDB instances for data persistence
- **Snapshot Management**: Creates and loads Raft snapshots

**Key Components**:

**HgStoreStateMachine** (`hg-store-core/.../raft/HgStoreStateMachine.java`):
- Implements JRaft's `StateMachine` interface
- Applies committed Raft log entries to RocksDB
- Handles snapshot creation (`onSnapshotSave`) and loading (`onSnapshotLoad`)
- Delegates to `BusinessHandler` for actual data operations

**BusinessHandler** (`hg-store-core/.../business/BusinessHandler.java`):
- Implements all data operations: put, get, delete, scan, batch
- Processes queries with filters and aggregations
- Manages transactions and batch operations
- Interacts with `RocksDBSession` for storage access

**RocksDBSession** (`hg-store-rocksdb/.../RocksDBSession.java`):
- Abstraction over RocksDB operations
- Supports multiple column families (default, write, data)
- Provides optimized scan iterators (`ScanIterator`)
- Handles RocksDB configuration and lifecycle

---

## Raft Consensus Mechanism

HugeGraph Store uses **Sofa-JRaft** (Ant Financial's Raft implementation) to achieve strong consistency and high availability.

### Raft per Partition Design

Unlike some distributed systems that use a single Raft group for the entire cluster, HugeGraph Store uses MultiRaft:

```
Store Cluster (3 nodes: S1, S2, S3)
Partition 1: Raft Group 1
  - Leader: S1
  - Followers: S2, S3

Partition 2: Raft Group 2
  - Leader: S2
  - Followers: S1, S3

Partition 3: Raft Group 3
  - Leader: S3
  - Followers: S1, S2
```

**Advantages**:
1. **Fine-grained Replication**: Each partition can have different replica counts
2. **Load Distribution**: Leaders are distributed across Store nodes
3. **Independent Failures**: Partition failures don't affect the entire cluster
4. **Scalability**: Adding partitions doesn't increase Raft group size

**Trade-offs**:
- More Raft groups mean more background work (heartbeats, elections)
- Increased memory overhead (each Raft group has its own log and state)

### Raft Components in Store

#### 1. Raft Node (`RaftEngine`)

**Location**: `hg-store-core/.../raft/RaftEngine.java` (wraps JRaft's `Node`)

**Responsibilities**:
- **Leadership**: Participates in leader election
- **Log Replication**: Replicates write operations to followers
- **Snapshot Management**: Triggers snapshot creation based on interval

**Key Configuration** (`application.yml`):
```yaml
raft:
  address: 127.0.0.1:8510       # Raft RPC address
  snapshotInterval: 1800         # Snapshot every 30 minutes
  disruptorBufferSize: 1024      # Raft log buffer size
  max-log-file-size: 600000000000  # Max log file size
```

#### 2. State Machine (`HgStoreStateMachine`)

**Location**: `hg-store-core/.../raft/HgStoreStateMachine.java`

Implements JRaft's `StateMachine` interface with these key methods:

**`onApply(Iterator iter)`**:
- Called when Raft log entries are committed
- Deserializes `RaftOperation` from log entry
- Delegates to `BusinessHandler` for execution
- Returns result via `RaftClosure`

**`onSnapshotSave(SnapshotWriter writer, Closure done)`**:
- Creates a consistent snapshot of partition data
- Uses `HgSnapshotHandler` to save RocksDB data
- Triggered periodically (default: every 30 minutes)
- Includes metadata like partition ID, shard group, etc.

**`onSnapshotLoad(SnapshotReader reader)`**:
- Loads snapshot data during partition initialization or recovery
- Restores RocksDB state from snapshot files
- Called when a follower needs to catch up or a new replica is added

**`onLeaderStart(long term)`**:
- Invoked when this node becomes the Raft leader for a partition
- Updates partition leader information in PD
- Enables write operations for this partition

**`onLeaderStop(Status status)`**:
- Invoked when this node loses leadership
- Rejects write operations (only followers accept reads)

#### 3. Raft Operations (`RaftOperation`)

**Location**: `hg-store-core/.../raft/RaftOperation.java`

Encapsulates all operations that need Raft consensus:

**Operation Types**:
- `PUT`: Single key-value write
- `DELETE`: Key deletion
- `BATCH`: Batch write operations
- `PARTITION_META`: Partition metadata updates
- `SNAPSHOT`: Snapshot-related operations

**Flow for Write Operations**:
1. Client sends write request to Store node
2. Store node creates `RaftOperation` with operation data
3. If this node is the Raft leader:
   - Proposes operation to Raft group via `node.apply(task)`
   - Waits for Raft commit (majority of replicas acknowledge)
   - State machine applies operation to RocksDB
   - Returns result to client
4. If this node is a follower:
   - Rejects write (clients must retry with leader)

#### 4. Snapshot Handling (`HgSnapshotHandler`)

**Location**: `hg-store-core/.../snapshot/HgSnapshotHandler.java`

**Snapshot Creation**:
1. Triggered by `snapshotInterval` (default: 1800 seconds)
2. Creates RocksDB checkpoint (consistent point-in-time snapshot)
3. Saves checkpoint files to snapshot directory
4. Includes metadata: partition ID, shard group, last applied index

**Snapshot Loading**:
1. Invoked when a new replica joins or a follower falls too far behind
2. Leader sends snapshot files to follower
3. Follower loads snapshot into RocksDB
4. Follower catches up with remaining log entries

**Snapshot Directory Structure**:
```
raft/
└── partition-<partition-id>/
    ├── log/               # Raft log files
    ├── snapshot/          # Snapshots
    │   ├── snapshot_<index>_<term>/
    │   │   ├── data/      # RocksDB data files
    │   │   └── meta       # Snapshot metadata
    │   └── ...
    └── meta               # Raft metadata
```

### Raft Performance Tuning

#### Log Management

**`raft.max-log-file-size`**: Maximum size of a single log file
- **Default**: 600GB (effectively unlimited)
- **Recommendation**: Set to 1-10GB for faster log rotation
- **Impact**: Smaller files enable faster snapshot compaction

**Log Retention**:
- Logs older than the last snapshot are automatically deleted
- Controlled by `snapshotInterval` and JRaft's log compaction

#### Snapshot Interval

**`raft.snapshotInterval`**: How often to create snapshots (seconds)
- **Default**: 1800 (30 minutes)
- **For write-heavy workloads**: Reduce to 600-900 seconds (10-15 minutes)
- **For read-heavy workloads**: Increase to 3600+ seconds (1+ hour)
- **Trade-off**: Frequent snapshots reduce log size but increase I/O

#### Disruptor Buffer

**`raft.disruptorBufferSize`**: Raft log buffer size
- **Default**: 1024
- **For high write throughput**: Increase to 4096 or 8192
- **Impact**: Larger buffer reduces contention but increases memory usage

### Raft Failure Scenarios

#### Leader Failure

1. **Detection**: Followers detect leader failure via missed heartbeats (default: 5 seconds)
2. **Election**: Followers start leader election (timeout: randomized 1-2 seconds)
3. **New Leader**: Follower with most up-to-date log becomes new leader
4. **Client Impact**: Write requests fail during election (~2-10 seconds)
5. **Recovery**: Clients retry writes with new leader

**PD Notification**: New leader reports leadership to PD via `updatePartitionLeader()` call

#### Follower Failure

1. **Detection**: Leader detects follower failure via heartbeat timeout
2. **Replication**: Leader continues replicating to remaining healthy followers
3. **Quorum**: As long as majority is healthy, writes succeed (e.g., 2/3 nodes)
4. **Recovery**: When follower recovers, it catches up via log replay or snapshot

#### Network Partition (Split-Brain)

**Scenario**: Network partition splits cluster into two groups

**Example**: 3-node cluster (S1, S2, S3) splits into {S1} and {S2, S3}

**Behavior**:
- **Majority partition** {S2, S3}: Can elect leader and accept writes
- **Minority partition** {S1}: Cannot form quorum, rejects writes
- **Read behavior**: Followers can still serve reads (may be stale)

**Recovery**: When network heals, S1 rejoins, discards any uncommitted writes, and syncs from the leader

**Prevention**: Use Raft's pre-vote mechanism (enabled by default in JRaft) to prevent unnecessary elections

---

## Partition Management

Partitions are the fundamental unit of data distribution in HugeGraph Store. Understanding partition management is critical for operating Store clusters.

### Partition Basics

**Partition**: A logical unit of data with a unique partition ID

**Shard**: A replica of a partition (e.g., Partition 1 might have Shard 1.1, 1.2, 1.3 on three Store nodes)

**Shard Group**: The set of all shards (replicas) for a partition, forming a Raft group

**Partition Metadata** (`hg-pd-grpc/src/main/proto/metapb.proto`):
```protobuf
message Partition {
  uint32 id = 1;                  // Unique partition ID
  uint64 version = 2;             // Version for partition updates
  uint32 start_key = 3;           // Start of key range (hash value)
  uint32 end_key = 4;             // End of key range (hash value)
  repeated Shard shards = 5;      // List of replicas
  PartitionState state = 6;       // Normal, Offline, etc.
}

message Shard {
  uint64 store_id = 1;            // Store node ID
  ShardRole role = 2;             // Leader or Follower
  ShardState state = 3;           // Normal, Offline, etc.
}
```

### Partition Assignment Flow

#### 1. Store Registration

When a Store node starts:

1. **Connect to PD**: Store connects to PD cluster using `pdserver.address`
2. **Register**: Store sends registration request with:
   - Store ID (or requests new ID)
   - gRPC address (`grpc.host:grpc.port`)
   - Raft address (`raft.address`)
   - Data path and capacity
3. **PD Response**: PD assigns a unique Store ID and returns current partition assignments

**Code**: `hg-store-core/.../pd/DefaultPdProvider.java` handles PD communication

#### 2. Initial Partition Assignment

**Trigger**: First Store nodes join the cluster

**Process**:
1. PD detects sufficient Store nodes (configured via `pd.initial-store-count`)
2. PD creates initial partitions (count configured in PD)
3. PD assigns shards (replicas) to Store nodes using placement rules:
   - **Load Balancing**: Distribute shards evenly across Store nodes
   - **Fault Isolation**: Avoid placing replicas on the same physical host (if configured)
   - **Shard Group**: Ensure each partition has the configured number of replicas (default: 3)

**Example**: 3 Store nodes (S1, S2, S3), 6 partitions, 3 replicas each
```
Partition 1: S1 (leader), S2, S3
Partition 2: S2 (leader), S1, S3
Partition 3: S3 (leader), S1, S2
Partition 4: S1 (leader), S2, S3
Partition 5: S2 (leader), S1, S3
Partition 6: S3 (leader), S1, S2
```

#### 3. Dynamic Partition Creation

**Code**: `hg-store-core/.../PartitionEngine.java` and `HgStoreEngine.java`

When PD instructs a Store to create a partition:

1. **Receive Instruction**: Store receives `PartitionInstructionListener` command from PD
2. **Create PartitionEngine**: `HgStoreEngine` creates a new `PartitionEngine` instance
3. **Initialize Raft**: `PartitionEngine` initializes Raft node with peer list (shard group)
4. **Start Raft**: Raft group starts, performs leader election
5. **Report Status**: Store reports partition creation success to PD

**Partition State Transitions**:
- `None` → `Normal`: Partition successfully created and operational
- `Normal` → `Offline`: Partition marked for deletion or migration
- `Offline` → `Tombstone`: Partition data deleted (pending cleanup)

### Partition Key Routing

**Hash-based Partitioning** (default):

1. **Key Hashing**: Client hashes the graph key to a 32-bit hash value
2. **Partition Lookup**: Determines which partition owns the hash range
3. **Store Routing**: Queries PD for the partition's shard group
4. **Leader Selection**: Sends request to the Raft leader for that partition

**Example**:
```
Key: "vertex:person:1001"
Hash: MurmurHash3("vertex:person:1001") = 0x12345678
Partition Range: 0x10000000 - 0x1FFFFFFF → Partition 3
Partition 3 Shards: S1 (leader), S2, S3
Request sent to: S1 (leader of Partition 3)
```

**Code**:
- Client-side routing: `hg-store-client/.../HgStoreNodeManager.java`
- Partition range lookup: Queries PD's partition metadata

### Partition Rebalancing

**Trigger**: PD's patrol task detects imbalance (runs every `pd.patrol-interval` seconds)

**Imbalance Scenarios**:
1. **Uneven Partition Distribution**: One Store has significantly more partitions than others
2. **Load Imbalance**: One Store has higher read/write traffic
3. **Capacity Imbalance**: One Store is running out of disk space

**Rebalancing Process**:
1. **PD Decision**: PD calculates optimal partition distribution
2. **Migration Plan**: PD creates partition migration tasks (move Partition X from Store A to Store B)
3. **Execute Migration**:
   - Add new replica on target Store (joins Raft group as learner)
   - New replica syncs data via Raft (log replay + snapshot)
   - Promote new replica to follower
   - Remove old replica from source Store
4. **Update Metadata**: PD updates partition shard group in metadata
5. **Client Updates**: Clients refresh partition routing information

**Configuration** (in PD `application.yml`):
```yaml
pd:
  patrol-interval: 1800         # Rebalancing check interval (seconds)

store:
  max-down-time: 172800         # Mark Store offline after 48 hours

partition:
  store-max-shard-count: 12     # Max partitions per Store
```

### Partition Split (Future Enhancement)

**Note**: Partition splitting is planned but not yet implemented in the current version.

**Planned Behavior**:
- **Trigger**: Partition size exceeds threshold (e.g., 10GB) or hotspot detected
- **Process**: Split partition into two smaller partitions with adjusted key ranges
- **Use Case**: Handle data growth and hot partitions

---

## PD Coordination

PD (Placement Driver) serves as the **control plane** for HugeGraph Store, managing cluster metadata, partition assignment, and health monitoring.

### PD Integration Points

#### 1. Store Registration

**When**: Store node startup

**Process**:
1. Store connects to PD cluster (tries each PD peer until success)
2. Store sends registration request:
   ```
   StoreId: 0 (or previously assigned ID)
   Address: 192.168.1.20:8500
   RaftAddress: 192.168.1.20:8510
   DataPath: /data/hugegraph-store
   Capacity: 1TB
   ```
3. PD assigns Store ID and returns initial partition assignments

**Code**: `hg-store-core/.../pd/DefaultPdProvider.java` → `register()` method

#### 2. Heartbeat Mechanism

**Frequency**: Every 30 seconds (configurable in PD)

**Heartbeat Content**:
- **Store Heartbeat**: Store-level metrics (CPU, memory, disk usage, partition count)
- **Partition Heartbeat**: Per-partition metrics (leader status, Raft term, shard states)

**Purpose**:
- **Liveness Detection**: PD marks Store offline if heartbeat times out
- **Metric Collection**: PD collects metrics for monitoring and scheduling
- **Partition Status**: PD tracks partition leaders and replica health

**Code**: `hg-store-core/.../HeartbeatService.java`

**Heartbeat Timeout** (in PD):
- **Grace Period**: Store is marked "Down" after 60 seconds of missed heartbeats
- **Permanent Offline**: Store is marked "Offline" after `store.max-down-time` (default: 48 hours)

#### 3. Partition Instruction Listener

**Purpose**: Receive partition management commands from PD

**Instruction Types**:
- `CREATE_PARTITION`: Create a new partition replica on this Store
- `DELETE_PARTITION`: Delete a partition replica from this Store
- `UPDATE_PARTITION`: Update partition metadata (e.g., add/remove shard)
- `TRANSFER_LEADER`: Transfer Raft leadership to another shard

**Code**: `hg-store-core/.../pd/PartitionInstructionListener.java`

**Flow**:
1. PD sends instruction via gRPC stream or heartbeat response
2. Store validates instruction (e.g., sufficient disk space)
3. Store executes instruction (e.g., creates `PartitionEngine`)
4. Store reports execution result back to PD

#### 4. Partition Leader Updates

**Trigger**: Raft leader election completes

**Process**:
1. Raft state machine detects leadership change (`onLeaderStart()` or `onLeaderStop()`)
2. Store sends leader update to PD: `updatePartitionLeader(partitionId, newLeader, term)`
3. PD updates partition metadata with new leader information
4. Clients query PD for updated partition routing

**Importance**: Ensures clients always route writes to the current Raft leader

### PD Metadata Queried by Store

**Graph Metadata**: List of graphs managed by the cluster
**Partition Metadata**: Partition ID, key ranges, shard list
**Store Metadata**: Store ID, address, capacity, state
**Shard Group Metadata**: Replica list for each partition

### Fault Tolerance with PD

**PD Cluster Failure**:
- **Store Impact**: Store continues serving existing partitions (data plane unaffected)
- **Limitation**: Cannot create/delete partitions or perform rebalancing
- **Recovery**: When PD recovers, Stores re-register and sync metadata

**Recommendation**: Always run PD in a 3-node or 5-node cluster for high availability

---

## Data Flow and Control Flow

### Write Request Flow

**Scenario**: Client writes a vertex to HugeGraph Server

```
1. [Client] → [hugegraph-server]
   GraphAPI.addVertex(vertex)

2. [hugegraph-server] → [hg-store-client]
   HstoreStore.put(key, value)

3. [hg-store-client] → [PD]
   Query: Which partition owns hash(key)?
   Response: Partition 3, Leader = Store 1 (192.168.1.20:8500)

4. [hg-store-client] → [Store 1 gRPC]
   Put Request (key, value)

5. [Store 1] → [PartitionEngine 3]
   Identify partition by key hash

6. [PartitionEngine 3] → [Raft Leader]
   Propose RaftOperation(PUT, key, value)

7. [Raft Leader] → [Raft Followers (Store 2, Store 3)]
   Replicate log entry

8. [Raft Followers] → [Raft Leader]
   Acknowledge (2/3 quorum achieved)

9. [Raft Leader] → [State Machine]
   Apply committed log entry

10. [State Machine] → [BusinessHandler]
    Execute put(key, value)

11. [BusinessHandler] → [RocksDB]
    rocksDB.put(key, value)

12. [Store 1] → [hg-store-client]
    Put Response (success)

13. [hg-store-client] → [hugegraph-server]
    Success

14. [hugegraph-server] → [Client]
    HTTP 201 Created
```

**Latency Breakdown** (typical production cluster):
- Client → Server: 1-2ms
- Server → Store (gRPC): 1-2ms
- Raft consensus (2-replica ack): 3-5ms
- State machine apply: 0.5-1ms
- RocksDB write: 1-2ms
- **Total**: ~7-12ms (p99)

### Read Request Flow (Consistent Read)

**Scenario**: Client queries vertices by label

```
1. [Client] → [hugegraph-server]
   GraphAPI.queryVertices(label="person")

2. [hugegraph-server] → [hg-store-client]
   HstoreSession.scan(labelKey, filters)

3. [hg-store-client] → [PD]
   Query: Which partitions store vertices?
   Response: All partitions (multi-partition scan)

4. [hg-store-client] → [Multiple Stores in parallel]
   Scan Request (labelKey, filters) to each partition

5. [Each Store] → [PartitionEngine]
   Forward scan to appropriate partition

6. [PartitionEngine] → [Raft Leader]
   Optional: Read index check (ensure linearizable read)

7. [PartitionEngine] → [BusinessHandler]
   scan(labelKey, filters)

8. [BusinessHandler] → [RocksDB]
   rocksDB.scan(startKey, endKey, filter)

9. [RocksDB] → [BusinessHandler]
   Iterator over matching keys

10. [BusinessHandler] → [Query Processor]
    Apply filters and aggregations (if pushdown)

11. [Stores] → [hg-store-client]
    Partial results from each partition

12. [hg-store-client] → [MultiPartitionIterator]
    Merge and deduplicate results

13. [hugegraph-server] → [Client]
    Final result set
```

**Optimization: Query Pushdown**:
- Filters applied at Store nodes (reduce network transfer)
- Aggregations (COUNT, SUM) computed at Store nodes
- Only final results returned to client

### Control Flow: Partition Creation

**Scenario**: PD decides to create a new partition on Store 1

```
1. [PD Patrol Task]
   Detect: Need more partitions for load balancing

2. [PD] → [PartitionService]
   createPartition(partitionId=100, shards=[Store1, Store2, Store3])

3. [PD] → [Store 1, Store 2, Store 3]
   Instruction: CREATE_PARTITION (partitionId=100)

4. [Each Store] → [PartitionInstructionListener]
   Receive and validate instruction

5. [Each Store] → [HgStoreEngine]
   createPartitionEngine(partitionId=100, peers=[S1, S2, S3])

6. [HgStoreEngine] → [PartitionEngine]
   new PartitionEngine(partitionId=100)

7. [PartitionEngine] → [RaftEngine]
   Initialize Raft node with peer list

8. [Raft Nodes] → [Raft Leader Election]
   Perform leader election (typically 1-3 seconds)

9. [New Leader] → [PD]
   Report: updatePartitionLeader(100, leaderId)

10. [All Stores] → [PD]
    Report: Partition creation successful

11. [PD] → [Metadata Store]
    Update partition metadata: State = Normal

12. [hg-store-client] → [PD]
    Refresh partition routing cache
```

**Total Time**: ~5-10 seconds for a new partition to become operational

---

## Summary

HugeGraph Store's distributed architecture is designed for:
- **Strong Consistency**: Raft consensus per partition ensures linearizable operations
- **High Availability**: Multi-replica design with automatic failover (<10s)
- **Horizontal Scalability**: Partition-based distribution enables cluster expansion
- **Operational Simplicity**: PD provides centralized control plane for cluster management

**Key Takeaways**:
1. Understand the three-tier architecture and each layer's responsibilities
2. Raft per partition provides fine-grained replication control
3. PD serves as the single source of truth for cluster topology
4. Partition management (assignment, rebalancing, split) is critical for scaling
5. Write latency is dominated by Raft consensus (~3-5ms), read latency by RocksDB access (~1-2ms)

For deployment strategies and cluster sizing, see [Deployment Guide](deployment-guide.md).

For query optimization and pushdown mechanisms, see [Query Engine](query-engine.md).

For operational best practices, see [Operations Guide](operations-guide.md).
