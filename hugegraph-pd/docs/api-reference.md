# HugeGraph PD API Reference

This document provides comprehensive API reference for HugeGraph PD, including gRPC services, Protocol Buffers definitions, and usage examples.

## Table of Contents

- [gRPC Services Overview](#grpc-services-overview)
- [Protocol Buffers Definitions](#protocol-buffers-definitions)
- [Core gRPC APIs](#core-grpc-apis)
- [Java Client Library](#java-client-library)
- [REST API](#rest-api)

## gRPC Services Overview

HugeGraph PD exposes multiple gRPC services for cluster management and coordination:

| Service | Proto File | Description |
|---------|------------|-------------|
| **PDGrpc** | `pdpb.proto` | Main PD service: store registration, partition queries, member management |
| **KvServiceGrpc** | `kv.proto` | Distributed key-value operations for metadata storage |
| **HgPdPulseGrpc** | `pd_pulse.proto` | Heartbeat and health monitoring for Store nodes |
| **HgPdWatchGrpc** | `pd_watch.proto` | Watch for partition and store change notifications |
| **DiscoveryServiceGrpc** | `discovery.proto` | Service discovery and registration |

**Proto Location**: `hugegraph-pd/hg-pd-grpc/src/main/proto/`

**Generated Stubs**: `hugegraph-pd/hg-pd-grpc/src/main/java/org/apache/hugegraph/pd/grpc/`

## Protocol Buffers Definitions

### Proto Files Structure

```
hg-pd-grpc/src/main/proto/
├── pdpb.proto          # Main PD service RPCs
├── metapb.proto        # Core metadata objects (Partition, Shard, Store)
├── meta.proto          # Extended metadata definitions
├── pd_common.proto     # Common types and enums
├── kv.proto            # Key-value service
├── pd_pulse.proto      # Heartbeat protocol
├── pd_watch.proto      # Watch notification protocol
├── discovery.proto     # Service discovery
└── metaTask.proto      # Task coordination
```

### Key Message Types

#### Partition

Represents a data partition in the cluster.

```protobuf
message Partition {
    uint64 id = 1;
    string graph_name = 2;
    uint64 start_key = 3;
    uint64 end_key = 4;
    repeated Shard shards = 5;
    PartitionState state = 6;
    uint64 version = 7;
}

enum PartitionState {
    PState_None = 0;
    PState_Normal = 1;
    PState_Splitting = 2;
    PState_Offline = 3;
}
```

#### Shard

Represents a replica of a partition.

```protobuf
message Shard {
    uint64 store_id = 1;
    ShardRole role = 2;
}

enum ShardRole {
    None = 0;
    Leader = 1;
    Follower = 2;
    Learner = 3;
}
```

#### Store

Represents a Store node in the cluster.

```protobuf
message Store {
    uint64 id = 1;
    string address = 2;              // gRPC address (host:port)
    string raft_address = 3;         // Raft address for data replication
    StoreState state = 4;
    map<string, string> labels = 5;  // Topology labels (rack, zone, region)
    StoreStats stats = 6;
    int64 last_heartbeat = 7;        // Unix timestamp
    uint64 version = 8;
}

enum StoreState {
    Unknown = 0;
    Up = 1;          // Store is online and healthy
    Offline = 2;     // Store is temporarily offline
    Tombstone = 3;   // Store is permanently removed
    Exiting = 4;     // Store is in the process of shutting down
}

message StoreStats {
    uint64 capacity = 1;          // Total disk capacity (bytes)
    uint64 available = 2;         // Available disk space (bytes)
    uint32 partition_count = 3;   // Number of partitions on this store
    uint32 leader_count = 4;      // Number of partitions where this store is leader
}
```

#### Graph

Represents a graph in the cluster.

```protobuf
message Graph {
    string graph_name = 1;
    uint32 partition_count = 2;
    GraphState state = 3;
}

enum GraphState {
    Graph_Normal = 0;
    Graph_Deleting = 1;
}
```

## Core gRPC APIs

### 1. PD Service (PDGrpc)

Main service for cluster management.

#### GetMembers

Retrieve all PD members in the cluster.

**Request**:
```protobuf
message GetMembersRequest {}
```

**Response**:
```protobuf
message GetMembersResponse {
    ResponseHeader header = 1;
    repeated Member members = 2;
    Member leader = 3;
}

message Member {
    string cluster_id = 1;
    string member_id = 2;
    string grpc_url = 3;       // gRPC endpoint
    string raft_url = 4;       // Raft endpoint
    MemberState state = 5;
}
```

**Java Example**:
```java
import org.apache.hugegraph.pd.grpc.PDGrpc;
import org.apache.hugegraph.pd.grpc.Pdpb;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

// Create gRPC channel
ManagedChannel channel = ManagedChannelBuilder
    .forAddress("localhost", 8686)
    .usePlaintext()
    .build();

// Create blocking stub
PDGrpc.PDBlockingStub stub = PDGrpc.newBlockingStub(channel);

// Get PD members
Pdpb.GetMembersRequest request = Pdpb.GetMembersRequest.newBuilder().build();
Pdpb.GetMembersResponse response = stub.getMembers(request);

System.out.println("Leader: " + response.getLeader().getGrpcUrl());
for (Pdpb.Member member : response.getMembersList()) {
    System.out.println("Member: " + member.getGrpcUrl() + " State: " + member.getState());
}

// Clean up
channel.shutdown();
```

#### RegisterStore

Register a new Store node with PD.

**Request**:
```protobuf
message RegisterStoreRequest {
    RequestHeader header = 1;
    Store store = 2;
}
```

**Response**:
```protobuf
message RegisterStoreResponse {
    ResponseHeader header = 1;
    uint64 store_id = 2;  // Assigned store ID
}
```

**Java Example**:
```java
import org.apache.hugegraph.pd.grpc.Metapb;

// Build store information
Metapb.Store store = Metapb.Store.newBuilder()
    .setAddress("192.168.1.100:8500")
    .setRaftAddress("192.168.1.100:8501")
    .setState(Metapb.StoreState.Up)
    .putLabels("zone", "zone-1")
    .putLabels("rack", "rack-a")
    .build();

// Register store
Pdpb.RegisterStoreRequest request = Pdpb.RegisterStoreRequest.newBuilder()
    .setStore(store)
    .build();

Pdpb.RegisterStoreResponse response = stub.registerStore(request);
long storeId = response.getStoreId();
System.out.println("Registered store with ID: " + storeId);
```

#### GetStoreInfo

Retrieve Store node information.

**Request**:
```protobuf
message GetStoreInfoRequest {
    RequestHeader header = 1;
    uint64 store_id = 2;
}
```

**Response**:
```protobuf
message GetStoreInfoResponse {
    ResponseHeader header = 1;
    Store store = 2;
}
```

**Java Example**:
```java
Pdpb.GetStoreInfoRequest request = Pdpb.GetStoreInfoRequest.newBuilder()
    .setStoreId(storeId)
    .build();

Pdpb.GetStoreInfoResponse response = stub.getStoreInfo(request);
Metapb.Store store = response.getStore();

System.out.println("Store " + store.getId() + " at " + store.getAddress());
System.out.println("State: " + store.getState());
System.out.println("Partitions: " + store.getStats().getPartitionCount());
System.out.println("Capacity: " + store.getStats().getCapacity() / (1024*1024*1024) + " GB");
```

#### GetPartition

Retrieve partition information by partition code.

**Request**:
```protobuf
message GetPartitionRequest {
    RequestHeader header = 1;
    string graph_name = 2;
    uint64 partition_key = 3;  // Hash code of the data key
}
```

**Response**:
```protobuf
message GetPartitionResponse {
    ResponseHeader header = 1;
    Partition partition = 2;
    Shard leader = 3;  // Current leader shard
}
```

**Java Example**:
```java
String graphName = "social_network";
long partitionKey = 12345L;  // Hash of vertex/edge key

Pdpb.GetPartitionRequest request = Pdpb.GetPartitionRequest.newBuilder()
    .setGraphName(graphName)
    .setPartitionKey(partitionKey)
    .build();

Pdpb.GetPartitionResponse response = stub.getPartition(request);
Metapb.Partition partition = response.getPartition();
Metapb.Shard leader = response.getLeader();

System.out.println("Partition " + partition.getId() + " range: [" +
                   partition.getStartKey() + ", " + partition.getEndKey() + ")");
System.out.println("Leader store: " + leader.getStoreId());
System.out.println("Replicas: " + partition.getShardsCount());
```

#### GetPartitionByCode

Retrieve partition by exact partition code (optimized for routing).

**Request**:
```protobuf
message GetPartitionByCodeRequest {
    RequestHeader header = 1;
    string graph_name = 2;
    uint64 partition_id = 3;
}
```

**Response**:
```protobuf
message GetPartitionByCodeResponse {
    ResponseHeader header = 1;
    Partition partition = 2;
}
```

**Java Example**:
```java
Pdpb.GetPartitionByCodeRequest request = Pdpb.GetPartitionByCodeRequest.newBuilder()
    .setGraphName("social_network")
    .setPartitionId(5)
    .build();

Pdpb.GetPartitionByCodeResponse response = stub.getPartitionByCode(request);
Metapb.Partition partition = response.getPartition();

// Find leader shard
Metapb.Shard leader = partition.getShardsList().stream()
    .filter(s -> s.getRole() == Metapb.ShardRole.Leader)
    .findFirst()
    .orElse(null);

if (leader != null) {
    System.out.println("Route query to store: " + leader.getStoreId());
}
```

### 2. KV Service (KvServiceGrpc)

Distributed key-value operations for metadata storage.

#### Put

Store a key-value pair.

**Request**:
```protobuf
message PutRequest {
    string key = 1;
    bytes value = 2;
    int64 ttl = 3;  // Time-to-live in seconds (0 = no expiration)
}
```

**Response**:
```protobuf
message PutResponse {
    ResponseHeader header = 1;
}
```

**Java Example**:
```java
import org.apache.hugegraph.pd.grpc.kv.KvServiceGrpc;
import org.apache.hugegraph.pd.grpc.kv.Kv;

KvServiceGrpc.KvServiceBlockingStub kvStub = KvServiceGrpc.newBlockingStub(channel);

// Store configuration
String key = "config/max_retry_count";
String value = "5";

Kv.PutRequest request = Kv.PutRequest.newBuilder()
    .setKey(key)
    .setValue(com.google.protobuf.ByteString.copyFromUtf8(value))
    .setTtl(0)  // No expiration
    .build();

Kv.PutResponse response = kvStub.put(request);
System.out.println("Stored: " + key);
```

#### Get

Retrieve a value by key.

**Request**:
```protobuf
message GetRequest {
    string key = 1;
}
```

**Response**:
```protobuf
message GetResponse {
    ResponseHeader header = 1;
    bytes value = 2;
}
```

**Java Example**:
```java
Kv.GetRequest request = Kv.GetRequest.newBuilder()
    .setKey("config/max_retry_count")
    .build();

Kv.GetResponse response = kvStub.get(request);
String value = response.getValue().toStringUtf8();
System.out.println("Retrieved value: " + value);
```

#### Scan

Range scan for keys matching a prefix.

**Request**:
```protobuf
message ScanRequest {
    string start_key = 1;
    string end_key = 2;
    int32 limit = 3;  // Max number of results
}
```

**Response**:
```protobuf
message ScanResponse {
    ResponseHeader header = 1;
    repeated KvPair kvs = 2;
}

message KvPair {
    string key = 1;
    bytes value = 2;
}
```

**Java Example**:
```java
// Scan all configuration keys
Kv.ScanRequest request = Kv.ScanRequest.newBuilder()
    .setStartKey("config/")
    .setEndKey("config/\uffff")  // End of prefix range
    .setLimit(100)
    .build();

Kv.ScanResponse response = kvStub.scan(request);
for (Kv.KvPair kv : response.getKvsList()) {
    System.out.println(kv.getKey() + " = " + kv.getValue().toStringUtf8());
}
```

### 3. Pulse Service (HgPdPulseGrpc)

Heartbeat and health monitoring for Store nodes.

#### Pulse (Streaming)

Bidirectional streaming for continuous heartbeat.

**Request Stream**:
```protobuf
message PulseRequest {
    PulseType pulse_type = 1;
    oneof notice {
        PulseCreatePartition create_partition = 2;
        PulseTransferLeader transfer_leader = 3;
        PulseMovePartition move_partition = 4;
        PulseDeletePartition delete_partition = 5;
    }
}

enum PulseType {
    PULSE_TYPE_UNKNOWN = 0;
    PULSE_TYPE_STORE_HEARTBEAT = 1;
    PULSE_TYPE_PARTITION_HEARTBEAT = 2;
}
```

**Response Stream**:
```protobuf
message PulseResponse {
    PulseType pulse_type = 1;
    oneof notice {
        PulseCreatePartition create_partition = 2;
        PulseTransferLeader transfer_leader = 3;
        PulseMovePartition move_partition = 4;
        PulseDeletePartition delete_partition = 5;
    }
}
```

**Java Example**:
```java
import org.apache.hugegraph.pd.grpc.pulse.HgPdPulseGrpc;
import org.apache.hugegraph.pd.grpc.pulse.HgPdPulse;
import io.grpc.stub.StreamObserver;

HgPdPulseGrpc.HgPdPulseStub asyncStub = HgPdPulseGrpc.newStub(channel);

// Response handler
StreamObserver<HgPdPulse.PulseResponse> responseObserver = new StreamObserver<>() {
    @Override
    public void onNext(HgPdPulse.PulseResponse response) {
        System.out.println("Received instruction: " + response.getPulseType());
        // Handle instructions from PD (partition transfer, split, etc.)
    }

    @Override
    public void onError(Throwable t) {
        System.err.println("Pulse stream error: " + t.getMessage());
    }

    @Override
    public void onCompleted() {
        System.out.println("Pulse stream completed");
    }
};

// Create bidirectional stream
StreamObserver<HgPdPulse.PulseRequest> requestObserver = asyncStub.pulse(responseObserver);

// Send periodic heartbeat
ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
scheduler.scheduleAtFixedRate(() -> {
    HgPdPulse.PulseRequest heartbeat = HgPdPulse.PulseRequest.newBuilder()
        .setPulseType(HgPdPulse.PulseType.PULSE_TYPE_STORE_HEARTBEAT)
        .build();

    requestObserver.onNext(heartbeat);
}, 0, 10, TimeUnit.SECONDS);
```

### 4. Watch Service (HgPdWatchGrpc)

Watch for partition and store change notifications.

#### WatchPartition

Watch for partition changes in a graph.

**Request**:
```protobuf
message WatchPartitionRequest {
    RequestHeader header = 1;
    string graph_name = 2;
    WatchType watch_type = 3;
}

enum WatchType {
    WATCH_TYPE_PARTITION_CHANGE = 0;
    WATCH_TYPE_STORE_CHANGE = 1;
}
```

**Response Stream**:
```protobuf
message WatchPartitionResponse {
    ResponseHeader header = 1;
    WatchChangeType change_type = 2;
    Partition partition = 3;
}

enum WatchChangeType {
    WATCH_CHANGE_TYPE_ADD = 0;
    WATCH_CHANGE_TYPE_DEL = 1;
    WATCH_CHANGE_TYPE_ALTER = 2;
}
```

**Java Example**:
```java
import org.apache.hugegraph.pd.grpc.watch.HgPdWatchGrpc;
import org.apache.hugegraph.pd.grpc.watch.HgPdWatch;

HgPdWatchGrpc.HgPdWatchStub watchStub = HgPdWatchGrpc.newStub(channel);

// Watch partition changes
HgPdWatch.WatchPartitionRequest request = HgPdWatch.WatchPartitionRequest.newBuilder()
    .setGraphName("social_network")
    .setWatchType(HgPdWatch.WatchType.WATCH_TYPE_PARTITION_CHANGE)
    .build();

StreamObserver<HgPdWatch.WatchPartitionResponse> responseObserver = new StreamObserver<>() {
    @Override
    public void onNext(HgPdWatch.WatchPartitionResponse response) {
        WatchChangeType changeType = response.getChangeType();
        Metapb.Partition partition = response.getPartition();

        switch (changeType) {
            case WATCH_CHANGE_TYPE_ADD:
                System.out.println("Partition added: " + partition.getId());
                break;
            case WATCH_CHANGE_TYPE_DEL:
                System.out.println("Partition deleted: " + partition.getId());
                break;
            case WATCH_CHANGE_TYPE_ALTER:
                System.out.println("Partition changed: " + partition.getId());
                // Refresh local cache
                break;
        }
    }

    @Override
    public void onError(Throwable t) {
        System.err.println("Watch error: " + t.getMessage());
    }

    @Override
    public void onCompleted() {
        System.out.println("Watch completed");
    }
};

watchStub.watchPartition(request, responseObserver);
```

## Java Client Library

HugeGraph PD provides a high-level Java client library (`hg-pd-client`) that simplifies interaction with PD.

### PDClient

Main client class for PD operations.

**Initialization**:
```java
import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;

// Configure PD client
PDConfig config = PDConfig.builder()
    .pdServers("192.168.1.10:8686,192.168.1.11:8686,192.168.1.12:8686")
    .connectTimeout(5000)  // 5 seconds
    .requestTimeout(10000) // 10 seconds
    .enableCache(true)     // Enable partition cache
    .build();

// Create client
PDClient client = new PDClient(config);

// Use client...

// Clean up
client.close();
```

### Partition Operations

```java
import org.apache.hugegraph.pd.common.PartitionEngine;

// Get partition by key
String graphName = "social_network";
long vertexId = 12345L;
long partitionKey = PartitionEngine.calcHashcode(vertexId);

Metapb.Partition partition = client.getPartitionByKey(graphName, partitionKey);
System.out.println("Partition ID: " + partition.getId());

// Get all partitions for a graph
List<Metapb.Partition> partitions = client.getPartitionsByGraphName(graphName);
System.out.println("Total partitions: " + partitions.size());

// Get partition leader
Metapb.Shard leader = client.getPartitionLeader(graphName, partition.getId());
Metapb.Store leaderStore = client.getStore(leader.getStoreId());
System.out.println("Leader at: " + leaderStore.getAddress());
```

### Store Operations

```java
// Get all stores
List<Metapb.Store> stores = client.getStores();
for (Metapb.Store store : stores) {
    System.out.println("Store " + store.getId() + ": " + store.getAddress() +
                       " (" + store.getState() + ")");
}

// Get active stores
List<Metapb.Store> activeStores = client.getActiveStores();
System.out.println("Active stores: " + activeStores.size());

// Get stores by graph
List<Metapb.Store> graphStores = client.getStoresByGraphName(graphName);
```

### Watch Operations

```java
import org.apache.hugegraph.pd.client.PDWatch;

// Create watch listener
PDWatch.Listener<Metapb.Partition> listener = new PDWatch.Listener<>() {
    @Override
    public void onNext(PDWatch.WatchEvent<Metapb.Partition> event) {
        System.out.println("Partition " + event.getTarget().getId() +
                           " " + event.getType());
    }

    @Override
    public void onError(Throwable error) {
        System.err.println("Watch error: " + error.getMessage());
    }
};

// Watch partition changes
PDWatch watch = client.watchPartition(graphName, listener);

// Stop watching
watch.close();
```

### KV Operations

```java
// Put key-value
client.put("config/setting1", "value1".getBytes());

// Get value
byte[] value = client.get("config/setting1");
System.out.println("Value: " + new String(value));

// Delete key
client.delete("config/setting1");

// Scan with prefix
Map<String, byte[]> results = client.scan("config/", "config/\uffff", 100);
for (Map.Entry<String, byte[]> entry : results.entrySet()) {
    System.out.println(entry.getKey() + " = " + new String(entry.getValue()));
}
```

## REST API

PD exposes a REST API for management and monitoring (default port: 8620).

### Health Check

```bash
curl http://localhost:8620/actuator/health
```

**Response**:
```json
{
  "status": "UP",
  "groups": ["liveness", "readiness"]
}
```

### Liveness and Readiness

Two unauthenticated endpoints are meant for probes and startup gates:

| Endpoint | Meaning | Status |
|----------|---------|--------|
| `GET /v1/health` | Liveness: the REST listener is up. Does not consult raft. | always `200` |
| `GET /v1/ready` | Readiness: the raft node is active and sees a leader, so this PD is inside a quorum. | `200` when ready, `503` otherwise |

```bash
curl -i http://localhost:8620/v1/ready
```

**Response** (leader of a healthy cluster):
```json
{
  "ready": true,
  "state": "STATE_LEADER",
  "isLeader": true
}
```

A follower reports `"state": "STATE_FOLLOWER"` with `"isLeader": false`. When
the quorum is lost the PD keeps answering `/v1/health` with `200` but
`/v1/ready` turns into `503` with `"ready": false`. Being unauthenticated, the
body carries no cluster addresses; the leader's address stays on `/v1/members`.

The answer is served from state the raft callbacks maintain rather than from
the raft node, so it stays prompt while an election is running and never waits
on the node lock. `state` is therefore the last change raft announced. A PD
reports `STATE_UNINITIALIZED` with `"ready": false` from process start until
its first raft callback, which is the ordinary startup window before a quorum
first forms, and jraft emits no callback for candidacy or leadership transfer,
so a candidate reports `STATE_FOLLOWER` with `"ready": false`.

Point Kubernetes readiness probes, `depends_on` healthchecks and any
"wait for PD" script at `/v1/ready`; keep liveness probes on `/v1/health`
so a PD that merely lost its leader is not restarted.

Match on the body rather than on the status code alone. A PD that predates this
endpoint does not reliably answer `404` for it: `RestAuthentication` refuses a
request it does not exclude by writing an error envelope, and as of 1.7.0 it
does so without setting a status, so an unknown path answers `200` with
`{"status":-1,"error":"Unauthorized!"}`. A status-only probe therefore reads
such a PD as ready. The body match holds whichever status a refusal carries: a
shell gate should use
`curl -fsS http://<pd-host>:8620/v1/ready | grep -q '"ready":true'`, and a
Kubernetes `httpGet` probe should be paired with a PD image that carries the
endpoint.

### Metrics

```bash
curl http://localhost:8620/actuator/metrics
```

**Response** (Prometheus format):
```
# HELP pd_raft_state Raft state (0=Follower, 1=Candidate, 2=Leader)
# TYPE pd_raft_state gauge
pd_raft_state 2.0

# HELP pd_store_count Number of stores
# TYPE pd_store_count gauge
pd_store_count{state="Up"} 3.0
pd_store_count{state="Offline"} 0.0

# HELP pd_partition_count Number of partitions
# TYPE pd_partition_count gauge
pd_partition_count 36.0
```

#### Raft membership gauges

Exported on `/actuator/prometheus` for alerting on quorum loss:

| Gauge | Value |
|-------|-------|
| `hg_raft_leader` | `1` on the raft leader, `0` elsewhere |
| `hg_raft_has_leader` | `1` while this PD sees a leader (is inside a quorum), `0` otherwise |
| `hg_raft_alive_peers` | On the leader, the number of peers (itself included) heard from within the leader lease timeout (90% of the election timeout by default); `NaN` on other nodes |

A cluster has lost its quorum when `sum(hg_raft_leader) == 0` or when
`hg_raft_has_leader == 0` on every member. Both are briefly true during a
normal election, so alert on them with a `for:` clause longer than the
election timeout rather than on the instantaneous value.

Do not aggregate `hg_raft_alive_peers` across instances: it is `NaN` on every
node but the leader, and one `NaN` sample turns the result of `sum()` or
`avg()` into `NaN` as well. Select the leader's series instead, for example
`hg_raft_alive_peers and on(instance) (hg_raft_leader == 1)`.

### Partition API

#### List Partitions

```bash
curl http://localhost:8620/v1/partitions?graph_name=social_network
```

**Response**:
```json
{
  "partitions": [
    {
      "id": 1,
      "graph_name": "social_network",
      "start_key": 0,
      "end_key": 1000,
      "shards": [
        {"store_id": 1, "role": "Leader"},
        {"store_id": 2, "role": "Follower"},
        {"store_id": 3, "role": "Follower"}
      ],
      "state": "PState_Normal"
    }
  ]
}
```

### Store API

#### List Stores

```bash
curl http://localhost:8620/v1/stores
```

**Response**:
```json
{
  "stores": [
    {
      "id": 1,
      "address": "192.168.1.100:8500",
      "raft_address": "192.168.1.100:8501",
      "state": "Up",
      "labels": {
        "zone": "zone-1",
        "rack": "rack-a"
      },
      "stats": {
        "capacity": 107374182400,
        "available": 53687091200,
        "partition_count": 12,
        "leader_count": 8
      },
      "last_heartbeat": 1620000000
    }
  ]
}
```

## Error Handling

### gRPC Status Codes

PD uses standard gRPC status codes:

| Code | Name | Description |
|------|------|-------------|
| 0 | OK | Success |
| 1 | CANCELLED | Operation cancelled |
| 2 | UNKNOWN | Unknown error |
| 3 | INVALID_ARGUMENT | Invalid request parameters |
| 4 | DEADLINE_EXCEEDED | Timeout |
| 5 | NOT_FOUND | Resource not found (store, partition, etc.) |
| 6 | ALREADY_EXISTS | Resource already exists |
| 7 | PERMISSION_DENIED | Insufficient permissions |
| 8 | RESOURCE_EXHAUSTED | Quota exceeded |
| 14 | UNAVAILABLE | Service unavailable (not leader, Raft not ready) |

### Response Header

All responses include a `ResponseHeader` with error information:

```protobuf
message ResponseHeader {
    uint64 cluster_id = 1;
    Error error = 2;
}

message Error {
    ErrorType type = 1;
    string message = 2;
}

enum ErrorType {
    OK = 0;
    NOT_LEADER = 1;      // Current node is not Raft leader
    STORE_NOT_FOUND = 2;
    PARTITION_NOT_FOUND = 3;
    STORE_TOMBSTONE = 4; // Store is permanently removed
    RAFT_ERROR = 5;
}
```

**Error Handling Example**:
```java
Pdpb.GetStoreInfoResponse response = stub.getStoreInfo(request);

if (response.getHeader().hasError()) {
    Error error = response.getHeader().getError();

    if (error.getType() == ErrorType.NOT_LEADER) {
        // Retry with leader node
        String leaderUrl = getLeaderFromMembers();
        // Reconnect and retry...
    } else {
        System.err.println("Error: " + error.getMessage());
    }
} else {
    Metapb.Store store = response.getStore();
    // Process store...
}
```

## Best Practices

### 1. Connection Management

- **Reuse gRPC channels**: Creating channels is expensive
- **Connection pooling**: Use multiple channels for high throughput
- **Automatic reconnection**: Handle disconnections gracefully

```java
// Good: Reuse channel
ManagedChannel channel = ManagedChannelBuilder
    .forAddress("pd-host", 8686)
    .usePlaintext()
    .keepAliveTime(30, TimeUnit.SECONDS)
    .idleTimeout(60, TimeUnit.SECONDS)
    .build();

// Bad: Create new channel per request
// ManagedChannel channel = ...
// channel.shutdown()  // Don't do this after every request
```

### 2. Leader Detection

- Always check `ResponseHeader.error.type` for `NOT_LEADER`
- Use `GetMembers()` to find current leader
- Cache leader information but refresh on errors

### 3. Partition Caching

- Cache partition routing information locally
- Use `WatchPartition` to invalidate cache on changes
- Set reasonable cache TTL (e.g., 5 minutes)

### 4. Retry Strategy

- Implement exponential backoff for retries
- Retry on transient errors (UNAVAILABLE, DEADLINE_EXCEEDED)
- Don't retry on permanent errors (NOT_FOUND, INVALID_ARGUMENT)

```java
int maxRetries = 3;
int retryDelay = 1000; // milliseconds

for (int i = 0; i < maxRetries; i++) {
    try {
        response = stub.getPartition(request);
        break;  // Success
    } catch (StatusRuntimeException e) {
        if (e.getStatus().getCode() == Status.Code.UNAVAILABLE && i < maxRetries - 1) {
            Thread.sleep(retryDelay * (1 << i));  // Exponential backoff
        } else {
            throw e;
        }
    }
}
```

### 5. Timeout Configuration

- Set appropriate timeouts for all RPCs
- Use shorter timeouts for read operations
- Use longer timeouts for write operations

```java
PDGrpc.PDBlockingStub stub = PDGrpc.newBlockingStub(channel)
    .withDeadlineAfter(5, TimeUnit.SECONDS);
```

## Summary

HugeGraph PD provides comprehensive gRPC APIs for:
- Cluster membership and leadership management
- Store registration and monitoring
- Partition routing and querying
- Distributed key-value operations
- Real-time change notifications

Use the high-level `PDClient` library for simplified integration, or use raw gRPC stubs for fine-grained control.

For architecture details, see [Architecture Documentation](architecture.md).

For configuration, see [Configuration Guide](configuration.md).
