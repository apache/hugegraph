# HugeGraph PD Architecture

This document provides a comprehensive overview of HugeGraph PD's architecture, design principles, and internal components.

## Table of Contents

- [System Overview](#system-overview)
- [Module Architecture](#module-architecture)
- [Core Components](#core-components)
- [Raft Consensus Layer](#raft-consensus-layer)
- [Data Flow](#data-flow)
- [Interaction with Store and Server](#interaction-with-store-and-server)

## System Overview

### What is HugeGraph PD?

HugeGraph PD (Placement Driver) is the control plane for HugeGraph distributed deployments. It acts as a centralized coordinator that manages cluster topology, partition allocation, and node scheduling while maintaining strong consistency through Raft consensus.

### Key Responsibilities

```
┌─────────────────────────────────────────────────────────────────┐
│                        HugeGraph PD Cluster                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              Service Discovery & Registration             │   │
│  │  - Store node registration and health monitoring          │   │
│  │  - Server node discovery and load balancing              │   │
│  └──────────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              Partition Management                         │   │
│  │  - Partition allocation across stores                     │   │
│  │  - Dynamic rebalancing and splitting                      │   │
│  │  - Leader election coordination                           │   │
│  └──────────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              Metadata Storage                             │   │
│  │  - Cluster configuration and state                        │   │
│  │  - Graph metadata and schemas                             │   │
│  │  - Distributed KV operations                              │   │
│  └──────────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              Task Scheduling                              │   │
│  │  - Partition patrol and health checks                     │   │
│  │  - Automated rebalancing triggers                         │   │
│  │  - Metrics collection coordination                        │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Technology Stack

- **Consensus**: Apache JRaft (Raft implementation from Ant Design)
- **Storage**: RocksDB for persistent metadata
- **Communication**: gRPC with Protocol Buffers
- **Framework**: Spring Boot for REST APIs and dependency injection
- **Language**: Java 11+

## Module Architecture

HugeGraph PD consists of 8 Maven modules organized in a layered architecture:

```
┌─────────────────────────────────────────────────────────────┐
│                        Client Layer                          │
├─────────────────────────────────────────────────────────────┤
│  hg-pd-client     │  Java client library for PD access      │
│  hg-pd-cli        │  Command-line tools for administration  │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                        Service Layer                         │
├─────────────────────────────────────────────────────────────┤
│  hg-pd-service    │  gRPC service implementations           │
│                   │  REST API endpoints (Spring Boot)       │
│                   │  Service discovery and pulse monitoring │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                         Core Layer                           │
├─────────────────────────────────────────────────────────────┤
│  hg-pd-core       │  Raft consensus integration (JRaft)    │
│                   │  Metadata stores (RocksDB-backed)       │
│                   │  Partition allocation and balancing     │
│                   │  Store node monitoring and scheduling   │
│                   │  Task coordination and execution        │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                     Foundation Layer                         │
├─────────────────────────────────────────────────────────────┤
│  hg-pd-grpc       │  Protocol Buffers definitions          │
│                   │  Generated gRPC stubs                   │
│  hg-pd-common     │  Shared utilities and interfaces        │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                    Distribution Layer                        │
├─────────────────────────────────────────────────────────────┤
│  hg-pd-dist       │  Assembly configuration                │
│                   │  Startup/shutdown scripts               │
│                   │  Configuration templates                │
│  hg-pd-test       │  Integration and unit tests            │
└─────────────────────────────────────────────────────────────┘
```

### Module Dependencies

```
hg-pd-grpc (proto definitions)
    ↓
hg-pd-common (utilities)
    ↓
hg-pd-core (business logic)
    ↓
hg-pd-service (API layer)
    ↓
hg-pd-dist (packaging)

Branch:
hg-pd-client ← hg-pd-grpc + hg-pd-common
hg-pd-cli ← hg-pd-client
hg-pd-test ← hg-pd-core + hg-pd-service
```

### Module Details

#### hg-pd-grpc

Protocol Buffers definitions and generated gRPC code.

**Key Proto Files**:
- `pdpb.proto`: Main PD service RPCs (GetMembers, RegisterStore, GetPartition)
- `metapb.proto`: Core metadata objects (Partition, Shard, Store, Graph)
- `discovery.proto`: Service discovery protocol
- `kv.proto`: Distributed key-value operations
- `pd_pulse.proto`: Heartbeat and monitoring protocol
- `pd_watch.proto`: Change notification watchers
- `metaTask.proto`: Distributed task coordination

**Location**: `hg-pd-grpc/src/main/proto/`

**Generated Code**: Excluded from source control; regenerated via `mvn compile`

#### hg-pd-common

Shared utilities and common interfaces used across modules.

**Key Components**:
- Configuration POJOs
- Common exceptions and error codes
- Utility classes for validation and conversion

#### hg-pd-core

Core business logic and metadata management. This is the heart of PD.

**Package Structure**:
```
org.apache.hugegraph.pd/
├── meta/                    # Metadata stores (RocksDB-backed)
│   ├── MetadataRocksDBStore # Base persistence layer
│   ├── PartitionMeta        # Partition and shard group management
│   ├── StoreInfoMeta        # Store node information
│   ├── TaskInfoMeta         # Distributed task coordination
│   ├── IdMetaStore          # Auto-increment ID generation
│   ├── ConfigMetaStore      # Configuration management
│   └── DiscoveryMetaStore   # Service discovery metadata
├── raft/                    # Raft integration layer
│   ├── RaftEngine           # Raft group lifecycle management
│   ├── RaftStateMachine     # State machine for metadata operations
│   ├── RaftTaskHandler      # Async task execution via Raft
│   ├── KVOperation          # Raft operation abstraction
│   └── KVStoreClosure       # Raft callback handling
├── PartitionService         # Partition allocation and balancing
├── StoreNodeService         # Store registration and monitoring
├── StoreMonitorDataService  # Metrics collection and time-series
├── TaskScheduleService      # Automated partition patrol
├── KvService                # Distributed KV operations
├── IdService                # ID generation service
├── ConfigService            # Configuration management
└── LogService               # Operational logging
```

#### hg-pd-service

gRPC service implementations and REST API.

**Key Classes**:
- `ServiceGrpc`: Main gRPC service endpoint
- `PDPulseService`: Heartbeat processing
- `DiscoveryService`: Service discovery
- REST APIs: `PartitionAPI`, `StoreAPI` (Spring Boot controllers)

**REST Endpoints** (port 8620 by default):
- `/actuator/health`: Health check
- `/actuator/metrics`: Prometheus-compatible metrics
- `/v1/partitions`: Partition management API
- `/v1/stores`: Store management API

#### hg-pd-client

Java client library for applications to interact with PD.

**Features**:
- gRPC connection pooling
- Automatic leader detection and failover
- Partition routing and caching
- Store discovery and health awareness

**Typical Usage**:
```java
PDConfig config = PDConfig.builder()
    .pdServers("192.168.1.10:8686,192.168.1.11:8686,192.168.1.12:8686")
    .build();

PDClient client = new PDClient(config);

// Register a store
client.registerStore(storeId, storeAddress);

// Get partition information
Partition partition = client.getPartitionByCode(graphName, partitionCode);

// Watch for partition changes
client.watchPartitions(graphName, listener);
```

#### hg-pd-cli

Command-line tools for PD administration.

**Common Operations**:
- Store management (list, offline, online)
- Partition inspection and balancing
- Raft cluster status
- Metadata backup and restore

#### hg-pd-test

Integration and unit tests.

**Test Categories**:
- Core service tests: `PartitionServiceTest`, `StoreNodeServiceTest`
- Raft integration tests: Leader election, snapshot, log replication
- gRPC API tests: Service registration, partition queries
- Metadata persistence tests: RocksDB operations, recovery

**Location**: `hg-pd-test/src/main/java/` (non-standard location)

#### hg-pd-dist

Distribution packaging and deployment artifacts.

**Structure**:
```
src/assembly/
├── descriptor/
│   └── server-assembly.xml     # Maven assembly configuration
└── static/
    ├── bin/
    │   ├── start-hugegraph-pd.sh
    │   ├── stop-hugegraph-pd.sh
    │   └── util.sh
    └── conf/
        ├── application.yml.template
        └── log4j2.xml
```

## Core Components

### Metadata Stores

All metadata is persisted in RocksDB via the `MetadataRocksDBStore` base class, ensuring durability and fast access.

#### PartitionMeta

Manages partition allocation and shard group information.

**Key Responsibilities**:
- Partition-to-store mapping
- Shard group (replica set) management
- Partition leader tracking
- Partition splitting metadata

**Data Structure**:
```
Partition {
    graphName: String
    partitionId: Int
    startKey: Long
    endKey: Long
    shards: List<Shard>
    workState: PartitionState (NORMAL, SPLITTING, OFFLINE)
}

Shard {
    storeId: Long
    role: ShardRole (LEADER, FOLLOWER, LEARNER)
}
```

**Related Service**: `PartitionService` (hg-pd-core:712)

#### StoreInfoMeta

Stores information about Store nodes in the cluster.

**Key Responsibilities**:
- Store registration and activation
- Store state management (ONLINE, OFFLINE, TOMBSTONE)
- Store labels and deployment topology
- Store capacity and load tracking

**Data Structure**:
```
Store {
    storeId: Long
    address: String (gRPC endpoint)
    raftAddress: String
    state: StoreState
    labels: Map<String, String>  # rack, zone, region
    stats: StoreStats (capacity, available, partitionCount)
    lastHeartbeat: Timestamp
}
```

**Related Service**: `StoreNodeService` (hg-pd-core:589)

#### TaskInfoMeta

Coordinates distributed tasks across the PD cluster.

**Task Types**:
- Partition balancing
- Partition splitting
- Store decommissioning
- Data migration

**Related Service**: `TaskScheduleService`

#### IdMetaStore

Provides auto-increment ID generation for:
- Store IDs
- Partition IDs
- Task IDs
- Custom business IDs

**Location**: `hg-pd-core/src/main/java/org/apache/hugegraph/pd/meta/IdMetaStore.java:41`

**Implementation**: Cluster ID-based ID allocation with local batching for performance.

### Service Layer

#### PartitionService

The most complex service, responsible for all partition management.

**Key Methods**:
- `getPartitionByCode(graphName, code)`: Route queries to correct partition
- `splitPartition(partitionId)`: Split partition when size exceeds threshold
- `balancePartitions()`: Rebalance partitions across stores
- `updatePartitionLeader(partitionId, shardId)`: Handle leader changes
- `transferLeader(partitionId, targetStoreId)`: Manual leader transfer

**Balancing Algorithm**:
1. Calculate partition distribution across stores
2. Identify overloaded stores (above threshold)
3. Identify underloaded stores (below threshold)
4. Generate transfer plans (partition → target store)
5. Execute transfers sequentially with validation

**Location**: `hg-pd-core/.../PartitionService.java` (2000+ lines)

#### StoreNodeService

Manages Store node lifecycle and health monitoring.

**Key Methods**:
- `registerStore(store)`: Register new store node
- `handleStoreHeartbeat(storeId, stats)`: Process heartbeat and update state
- `setStoreState(storeId, state)`: Change store state (ONLINE/OFFLINE)
- `getStore(storeId)`: Retrieve store information
- `getStoresByGraphName(graphName)`: Get stores for specific graph

**Heartbeat Processing**:
1. Update store last heartbeat timestamp
2. Update store statistics (disk usage, partition count)
3. Detect store failures (heartbeat timeout)
4. Trigger partition rebalancing if needed

**Location**: `hg-pd-core/.../StoreNodeService.java`

#### TaskScheduleService

Automated background tasks for cluster maintenance.

**Scheduled Tasks**:
- **Partition Patrol**: Periodically scan all partitions for health issues
- **Balance Check**: Detect imbalanced partition distribution
- **Store Monitor**: Check store health and trigger failover
- **Metrics Collection**: Aggregate cluster metrics

**Configuration**:
- `pd.patrol-interval`: Patrol interval in seconds (default: 1800)

#### KvService

Distributed key-value operations backed by Raft consensus.

**Operations**:
- `put(key, value)`: Store key-value pair
- `get(key)`: Retrieve value by key
- `delete(key)`: Remove key-value pair
- `scan(startKey, endKey)`: Range scan

**Use Cases**:
- Configuration storage
- Graph metadata
- Custom application data

## Raft Consensus Layer

### Why Raft?

PD uses Apache JRaft to ensure:
- **Strong Consistency**: All PD nodes see the same metadata
- **High Availability**: Automatic leader election on failures
- **Fault Tolerance**: Cluster survives (N-1)/2 node failures

### Raft Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        PD Node 1 (Leader)                    │
│  ┌──────────────┐  ┌─────────────┐  ┌──────────────────┐   │
│  │ gRPC Service │→ │ RaftEngine  │→ │ RaftStateMachine │   │
│  └──────────────┘  └─────────────┘  └──────────────────┘   │
│                            ↓                   ↓             │
│                    ┌──────────────────────────────┐          │
│                    │     RocksDB (Metadata)       │          │
│                    └──────────────────────────────┘          │
└─────────────────────────────────────────────────────────────┘
                              ↓ (Log Replication)
        ┌─────────────────────┴─────────────────────┐
        ↓                                             ↓
┌───────────────────┐                       ┌───────────────────┐
│  PD Node 2        │                       │  PD Node 3        │
│  (Follower)       │                       │  (Follower)       │
│                   │                       │                   │
│  RaftStateMachine │                       │  RaftStateMachine │
│       ↓           │                       │       ↓           │
│  RocksDB          │                       │  RocksDB          │
└───────────────────┘                       └───────────────────┘
```

### Raft Components

#### RaftEngine

Manages Raft group lifecycle.

**Location**: `hg-pd-core/src/main/java/.../raft/RaftEngine.java`

**Responsibilities**:
- Initialize Raft group on startup
- Handle leader election
- Manage Raft configuration changes (add/remove nodes)
- Snapshot creation and recovery

**Key Methods**:
- `init()`: Initialize Raft node
- `isLeader()`: Check if current node is leader
- `submitTask(operation)`: Submit operation to Raft (leader only)

#### RaftStateMachine

Applies committed Raft log entries to metadata stores.

**Location**: `hg-pd-core/src/main/java/.../raft/RaftStateMachine.java`

**Workflow**:
1. Receive committed log entry from Raft
2. Deserialize operation (PUT, DELETE, etc.)
3. Apply operation to RocksDB
4. Return result to client (if on leader)

**Snapshot Management**:
- Periodic snapshots to reduce log size
- Snapshots stored in `pd_data/raft/snapshot/`
- Followers recover from snapshots + incremental logs

#### KVOperation

Abstraction for Raft operations.

**Types**:
- `PUT`: Write key-value pair
- `DELETE`: Remove key-value pair
- `BATCH`: Atomic batch operations

**Serialization**: Hessian2 for compact binary encoding

### Raft Data Flow

**Write Operation**:
```
1. Client → PD Leader gRPC API
2. Leader → RaftEngine.submitTask(PUT operation)
3. RaftEngine → Replicate log to followers
4. Followers → Acknowledge log entry
5. Leader → Commit log entry (quorum reached)
6. RaftStateMachine → Apply to RocksDB
7. Leader → Return success to client
```

**Read Operation** (default mode):
```
1. Client → Any PD node gRPC API
2. PD Node → Read from local RocksDB
3. PD Node → Return result to client
```

**Linearizable Read** (optional):
```
1. Client → PD Leader gRPC API
2. Leader → ReadIndex query to ensure leadership
3. Leader → Wait for commit index ≥ read index
4. Leader → Read from RocksDB
5. Leader → Return result to client
```

## Data Flow

### Store Registration Flow

```
1. Store Node starts up
2. Store → gRPC RegisterStore(storeInfo) → PD Leader
3. PD Leader → Validate store info
4. PD Leader → Raft proposal (PUT store metadata)
5. Raft → Replicate and commit
6. PD Leader → Assign store ID
7. PD Leader → Return store ID to Store
8. Store → Start heartbeat loop
```

### Partition Query Flow

```
1. Server → gRPC GetPartition(graphName, key) → PD
2. PD → Hash key to partition code
3. PD → Query PartitionMeta (local RocksDB)
4. PD → Return partition info (shards, leader)
5. Server → Cache partition info
6. Server → Route query to Store (partition leader)
```

### Heartbeat Flow

```
1. Store → gRPC StoreHeartbeat(storeId, stats) → PD Leader (every 10s)
2. PD Leader → Update store last heartbeat
3. PD Leader → Update store statistics
4. PD Leader → Check for partition state changes
5. PD Leader → Return instructions (transfer leader, split partition, etc.)
6. Store → Execute instructions
```

### Partition Balancing Flow

```
1. TaskScheduleService → Periodic patrol (every 30 min by default)
2. PartitionService → Calculate partition distribution
3. PartitionService → Identify imbalanced stores
4. PartitionService → Generate balance plan
5. PartitionService → Raft proposal (update partition metadata)
6. PD → Send transfer instructions via heartbeat response
7. Store → Execute partition transfers
8. Store → Report completion via heartbeat
```

## Interaction with Store and Server

### Architecture Context

```
┌────────────────────────────────────────────────────────────────┐
│                      HugeGraph Cluster                          │
│                                                                  │
│  ┌─────────────────┐      ┌─────────────────┐                  │
│  │  HugeGraph      │      │  HugeGraph      │                  │
│  │  Server (3x)    │      │  Server (3x)    │                  │
│  │  - REST API     │      │  - REST API     │                  │
│  │  - Gremlin      │      │  - Cypher       │                  │
│  └────────┬────────┘      └────────┬────────┘                  │
│           │                         │                            │
│           └──────────┬──────────────┘                           │
│                      ↓ (query routing)                          │
│           ┌─────────────────────────┐                           │
│           │   HugeGraph PD Cluster  │                           │
│           │   (3x or 5x nodes)      │                           │
│           │   - Service Discovery   │                           │
│           │   - Partition Routing   │                           │
│           │   - Metadata Management │                           │
│           └─────────┬───────────────┘                           │
│                     ↓ (partition assignment)                    │
│  ┌──────────────────┴───────────────────────────┐              │
│  │                                                │              │
│  ↓                     ↓                          ↓              │
│ ┌───────────────┐  ┌───────────────┐  ┌───────────────┐       │
│ │ HugeGraph     │  │ HugeGraph     │  │ HugeGraph     │       │
│ │ Store Node 1  │  │ Store Node 2  │  │ Store Node 3  │       │
│ │ - RocksDB     │  │ - RocksDB     │  │ - RocksDB     │       │
│ │ - Raft (data) │  │ - Raft (data) │  │ - Raft (data) │       │
│ └───────────────┘  └───────────────┘  └───────────────┘       │
└────────────────────────────────────────────────────────────────┘
```

### PD ↔ Store Communication

**gRPC Services Used**:
- `PDGrpc.registerStore()`: Store registration
- `PDGrpc.getStoreInfo()`: Retrieve store metadata
- `PDGrpc.reportTask()`: Task completion reporting
- `HgPdPulseGrpc.pulse()`: Heartbeat streaming

**Store → PD** (initiated by Store):
- Store registration on startup
- Periodic heartbeat (every 10 seconds)
- Partition state updates
- Task completion reports

**PD → Store** (via heartbeat response):
- Partition transfer instructions
- Partition split instructions
- Leadership transfer commands
- Store state changes (ONLINE/OFFLINE)

### PD ↔ Server Communication

**gRPC Services Used**:
- `PDGrpc.getPartition()`: Partition routing queries
- `PDGrpc.getPartitionsByGraphName()`: Batch partition queries
- `HgPdWatchGrpc.watch()`: Real-time partition change notifications

**Server → PD**:
- Partition routing queries (on cache miss)
- Watch partition changes
- Graph metadata queries

**PD → Server** (via watch stream):
- Partition added/removed events
- Partition leader changes
- Store online/offline events

### Partition Assignment Example

Scenario: A new graph "social_network" is created with 12 partitions and 3 stores.

**Step-by-Step**:
1. Server → `CreateGraph("social_network", partitionCount=12)` → PD
2. PD → Calculate partition distribution: 4 partitions per store
3. PD → Create partition metadata:
   ```
   Partition 0: [Shard(store=1, LEADER), Shard(store=2, FOLLOWER), Shard(store=3, FOLLOWER)]
   Partition 1: [Shard(store=2, LEADER), Shard(store=3, FOLLOWER), Shard(store=1, FOLLOWER)]
   ...
   Partition 11: [Shard(store=3, LEADER), Shard(store=1, FOLLOWER), Shard(store=2, FOLLOWER)]
   ```
4. PD → Raft commit partition metadata
5. PD → Send create partition instructions to stores via heartbeat
6. Stores → Create RocksDB instances for assigned partitions
7. Stores → Form Raft groups for each partition
8. Stores → Report partition ready via heartbeat
9. PD → Return success to Server
10. Server → Cache partition routing table

### Load Balancing Example

Scenario: Store 3 is overloaded (8 partitions), Store 1 is underloaded (2 partitions).

**Rebalancing Process**:
1. TaskScheduleService detects imbalance during patrol
2. PartitionService generates plan: Move 3 partitions from Store 3 to Store 1
3. For each partition to move:
   - PD → Raft commit: Add Store 1 as LEARNER to partition
   - PD → Instruct Store 3 to add Store 1 replica (via heartbeat)
   - Store 3 → Raft add learner and sync data
   - Store 1 → Catch up with leader
   - PD → Raft commit: Promote Store 1 to FOLLOWER
   - PD → Raft commit: Transfer leader to Store 1
   - PD → Raft commit: Remove Store 3 from partition
   - Store 3 → Delete partition RocksDB
4. Repeat for remaining partitions
5. Final state: Store 1 (5 partitions), Store 3 (5 partitions)

## Summary

HugeGraph PD provides a robust, highly available control plane for distributed HugeGraph deployments through:

- **Raft Consensus**: Strong consistency and automatic failover
- **Modular Design**: Clean separation of concerns across 8 modules
- **Scalable Metadata**: RocksDB-backed persistence with efficient indexing
- **Intelligent Scheduling**: Automated partition balancing and failure recovery
- **gRPC Communication**: High-performance inter-service communication

For configuration details, see [Configuration Guide](configuration.md).

For API usage, see [API Reference](api-reference.md).

For development workflows, see [Development Guide](development.md).
