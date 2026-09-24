# Query Engine

This document explains HugeGraph Store's query processing capabilities, including query pushdown, multi-partition queries, and gRPC API reference.

## Table of Contents

- [Query Processing Overview](#query-processing-overview)
- [Query Pushdown](#query-pushdown)
- [Multi-Partition Queries](#multi-partition-queries)
- [gRPC API Reference](#grpc-api-reference)
- [Query Optimization](#query-optimization)

---

## Query Processing Overview

HugeGraph Store implements advanced query processing to minimize network traffic and improve performance.

### Query Execution Flow

```
1. [Server] Receives Graph Query (e.g., g.V().has('age', 30))
   ↓
2. [Server] Translates to Store Query
   - Determine table (vertices, edges, indexes)
   - Extract filters (age == 30)
   - Identify partitions (all or specific)
   ↓
3. [Server] Sends Query to Store Nodes (parallel)
   - Query includes filters and aggregations
   - One request per partition
   ↓
4. [Store] Executes Query Locally
   - Apply filters at RocksDB scan level
   - Compute aggregations if requested
   - Stream results back to Server
   ↓
5. [Server] Merges Results
   - Deduplicate if needed
   - Apply final ordering/limiting
   ↓
6. [Server] Returns to Client
```

### Key Capabilities

- **Filter Pushdown**: Filters applied at Store nodes (not at Server)
- **Aggregation Pushdown**: COUNT, SUM, MIN, MAX, AVG computed at Store
- **Index Pushdown**: Index scans executed at Store
- **Streaming**: Large result sets streamed (no full materialization)
- **Parallel Execution**: Queries across multiple partitions run concurrently

---

## Query Pushdown

### Filter Pushdown

**Supported Filters**:
- Equality: `key == value`
- Range: `key > value`, `key >= value`, `key < value`, `key <= value`
- Prefix: `key.startsWith(prefix)`
- IN: `key in [value1, value2, ...]`

**Example**: Query vertices with age = 30

**Without Pushdown** (inefficient):
```
1. Store scans all vertices
2. Sends all vertices to Server
3. Server filters age == 30
Network: Transfers all vertices
```

**With Pushdown** (efficient):
```
1. Server sends filter: age == 30
2. Store scans and filters locally
3. Store sends only matching vertices
Network: Transfers only matching vertices
```

**Implementation** (proto definition):

File: `hg-store-grpc/src/main/proto/query.proto`

```protobuf
message Condition {
  string key = 1;                // Property key
  ConditionType type = 2;        // EQ, GT, LT, GTE, LTE, IN, PREFIX
  bytes value = 3;               // Value to compare
  repeated bytes values = 4;     // For IN operator
}

enum ConditionType {
  EQ = 0;        // Equals
  GT = 1;        // Greater than
  LT = 2;        // Less than
  GTE = 3;       // Greater than or equal
  LTE = 4;       // Less than or equal
  IN = 5;        // In list
  PREFIX = 6;    // Prefix match
}
```

**Code Example** (using client API):

```java
import org.apache.hugegraph.store.client.HgStoreQuery;
import org.apache.hugegraph.store.client.HgStoreQuery.Condition;

// Build query with filter
HgStoreQuery query = HgStoreQuery.builder()
    .table("VERTEX")
    .filter(Condition.eq("age", 30))
    .build();

// Execute (filter applied at Store)
HgStoreResultSet results = session.query(query);
```

---

### Aggregation Pushdown

**Supported Aggregations**:
- `COUNT`: Count matching rows
- `SUM`: Sum numeric property
- `MIN`: Minimum value
- `MAX`: Maximum value
- `AVG`: Average value

**Example**: Count vertices with label "person"

**Without Pushdown**:
```
1. Store sends all person vertices to Server
2. Server counts vertices
Network: Transfers millions of vertices
```

**With Pushdown**:
```
1. Server sends COUNT query
2. Store counts locally
3. Store sends count (single number)
Network: Transfers 8 bytes
```

**Proto Definition**:

```protobuf
message QueryRequest {
  string table = 1;
  bytes start_key = 2;
  bytes end_key = 3;
  repeated Condition conditions = 4;
  AggregationType aggregation = 5;    // COUNT, SUM, MIN, MAX, AVG
  string aggregation_key = 6;          // Property to aggregate
  int64 limit = 7;
}

enum AggregationType {
  NONE = 0;
  COUNT = 1;
  SUM = 2;
  MIN = 3;
  MAX = 4;
  AVG = 5;
}
```

**Code Example**:

```java
// Count query
HgStoreQuery query = HgStoreQuery.builder()
    .table("VERTEX")
    .prefix("person:")
    .aggregation(HgStoreQuery.Aggregation.COUNT)
    .build();

long count = session.aggregate(query);
System.out.println("Person count: " + count);

// Sum query
HgStoreQuery sumQuery = HgStoreQuery.builder()
    .table("VERTEX")
    .prefix("person:")
    .aggregation(HgStoreQuery.Aggregation.SUM)
    .aggregationKey("age")
    .build();

long totalAge = session.aggregate(sumQuery);
```

---

### Index Pushdown

HugeGraph Server creates indexes as separate tables in Store. Store can directly scan index tables.

**Index Types**:
1. **Secondary Index**: Index on property values
2. **Range Index**: Index for range queries
3. **Search Index**: Full-text search index (if enabled)

**Example**: Query by indexed property

```java
// Server creates index (one-time setup)
// schema.indexLabel("personByName").onV("person").by("name").secondary().create()

// Query using index
// Server translates to index table scan
HgStoreQuery query = HgStoreQuery.builder()
    .table("INDEX_personByName")       // Index table
    .filter(Condition.eq("name", "Alice"))
    .build();

// Store scans index table directly (fast)
HgStoreResultSet results = session.query(query);
```

**Index Table Structure**:
```
Key: <index_value>:<vertex_id>
Value: <vertex_id>

Example:
Key: "Alice:V1001" → Value: "V1001"
Key: "Bob:V1002"   → Value: "V1002"
```

---

## Multi-Partition Queries

### Partition-Aware Routing

Store distributes data across partitions using hash-based partitioning. Queries may target:
1. **Single Partition**: When key or hash is known
2. **Multiple Partitions**: When scanning by label or property

**Example**: Single-partition query

```java
// Get vertex by ID (single partition)
String vertexId = "person:1001";
int hash = MurmurHash3.hash32(vertexId);
int partitionId = hash % totalPartitions;

// Server routes to specific partition
HgStoreQuery query = HgStoreQuery.builder()
    .table("VERTEX")
    .partitionId(partitionId)
    .key(vertexId.getBytes())
    .build();

byte[] value = session.get(query);
```

**Example**: Multi-partition query

```java
// Scan all person vertices (all partitions)
HgStoreQuery query = HgStoreQuery.builder()
    .table("VERTEX")
    .prefix("person:")
    .build();

// Server sends query to ALL partitions in parallel
HgStoreResultSet results = session.queryAll(query);
```

### Multi-Partition Iterator

**Implementation**: `hg-store-core/src/main/java/.../business/MultiPartitionIterator.java`

**Purpose**: Merge results from multiple partitions

**Deduplication Modes**:
1. **NONE**: No deduplication (fastest, may have duplicates)
2. **DEDUP**: Basic deduplication using hash set
3. **LIMIT_DEDUP**: Deduplicate up to limit (for top-K queries)
4. **PRECISE_DEDUP**: Full deduplication with sorted merge

**Code Example**:

```java
import org.apache.hugegraph.store.client.DeduplicationMode;

// Query with deduplication
HgStoreQuery query = HgStoreQuery.builder()
    .table("VERTEX")
    .prefix("person:")
    .limit(100)
    .deduplicationMode(DeduplicationMode.LIMIT_DEDUP)
    .build();

HgStoreResultSet results = session.queryAll(query);
```

**Algorithm** (LIMIT_DEDUP):
```
1. Iterate partition 1, add results to set (up to limit)
2. Iterate partition 2, skip duplicates, add new results
3. Continue until limit reached or all partitions exhausted
4. Return deduplicated results
```

### Parallel Query Execution

**Flow**:
```
Server                           Store 1        Store 2        Store 3
  |                                 |              |              |
  |---Query(partition 1-4)--------->|              |              |
  |---Query(partition 5-8)--------------------->|              |
  |---Query(partition 9-12)------------------------------>|
  |                                 |              |              |
  |<---Results (partitions 1-4)-----|              |              |
  |<---Results (partitions 5-8)----------------|              |
  |<---Results (partitions 9-12)---------------------------|
  |                                 |              |              |
  |-Merge results                   |              |              |
  |                                 |              |              |
```

**Performance**:
- 3 Store nodes: 3x parallelism
- 12 partitions: Up to 12x parallelism (if evenly distributed)
- Network: 3 concurrent gRPC streams

---

## gRPC API Reference

### HgStoreSession Service

File: `hg-store-grpc/src/main/proto/store_session.proto`

```protobuf
service HgStoreSession {
  // Get single key
  rpc Get(GetRequest) returns (GetResponse);

  // Batch get
  rpc BatchGet(BatchGetRequest) returns (stream BatchGetResponse);

  // Put/Delete/Batch operations
  rpc Batch(stream BatchRequest) returns (BatchResponse);

  // Scan range
  rpc ScanTable(ScanTableRequest) returns (stream ScanResponse);

  // Table operations
  rpc Table(TableRequest) returns (TableResponse);

  // Clean/Truncate
  rpc Clean(CleanRequest) returns (CleanResponse);
}
```

**GetRequest**:
```protobuf
message GetRequest {
  Header header = 1;              // Graph name, table name
  bytes key = 2;                  // Key to get
}
```

**ScanTableRequest**:
```protobuf
message ScanTableRequest {
  Header header = 1;
  bytes start_key = 2;            // Start of range
  bytes end_key = 3;              // End of range (exclusive)
  int64 limit = 4;                // Max results
  ScanMethod scan_method = 5;     // ALL, PREFIX, RANGE
  bytes prefix = 6;               // For PREFIX scan
}

enum ScanMethod {
  ALL = 0;       // Scan all keys
  PREFIX = 1;    // Scan keys with prefix
  RANGE = 2;     // Scan key range
}
```

---

### QueryService

File: `hg-store-grpc/src/main/proto/query.proto`

```protobuf
service QueryService {
  // Execute query with filters
  rpc Query(QueryRequest) returns (stream QueryResponse);

  // Execute query (alternative API)
  rpc Query0(QueryRequest) returns (stream QueryResponse);

  // Aggregate query (COUNT, SUM, etc.)
  rpc Count(QueryRequest) returns (CountResponse);
}
```

**QueryRequest**:
```protobuf
message QueryRequest {
  Header header = 1;               // Graph name, partition ID
  string table = 2;                // Table name
  bytes start_key = 3;             // Scan start
  bytes end_key = 4;               // Scan end
  repeated Condition conditions = 5;  // Filters
  AggregationType aggregation = 6;    // COUNT, SUM, MIN, MAX, AVG
  string aggregation_key = 7;         // Property to aggregate
  int64 limit = 8;                    // Max results
  ScanType scan_type = 9;             // TABLE_SCAN, PRIMARY_SCAN, INDEX_SCAN
}

enum ScanType {
  TABLE_SCAN = 0;      // Full table scan
  PRIMARY_SCAN = 1;    // Primary key lookup
  INDEX_SCAN = 2;      // Index scan
}
```

**QueryResponse**:
```protobuf
message QueryResponse {
  repeated KV data = 1;            // Key-value pairs
  bool has_more = 2;               // More results available
  bytes continuation_token = 3;    // For pagination
}
```

**CountResponse**:
```protobuf
message CountResponse {
  int64 count = 1;                 // Aggregation result
}
```

---

### GraphStore Service

File: `hg-store-grpc/src/main/proto/graphpb.proto`

```protobuf
service GraphStore {
  // Scan partition (graph-specific API)
  rpc ScanPartition(ScanPartitionRequest) returns (stream ScanPartitionResponse);
}
```

**ScanPartitionRequest**:
```protobuf
message ScanPartitionRequest {
  Header header = 1;
  int32 partition_id = 2;          // Partition to scan
  ScanType scan_type = 3;          // SCAN_VERTEX, SCAN_EDGE
  bytes start_key = 4;
  bytes end_key = 5;
  int64 limit = 6;
}

enum ScanType {
  SCAN_VERTEX = 0;
  SCAN_EDGE = 1;
  SCAN_ALL = 2;
}
```

**ScanPartitionResponse**:
```protobuf
message ScanPartitionResponse {
  repeated Vertex vertices = 1;    // Vertex results
  repeated Edge edges = 2;         // Edge results
}

message Vertex {
  bytes id = 1;                    // Vertex ID
  string label = 2;                // Vertex label
  repeated Property properties = 3;
}

message Edge {
  bytes id = 1;
  string label = 2;
  bytes source_id = 3;             // Source vertex ID
  bytes target_id = 4;             // Target vertex ID
  repeated Property properties = 5;
}

message Property {
  string key = 1;                  // Property name
  Variant value = 2;               // Property value
}

message Variant {
  oneof value {
    int32 int_value = 1;
    int64 long_value = 2;
    float float_value = 3;
    double double_value = 4;
    string string_value = 5;
    bytes bytes_value = 6;
    bool bool_value = 7;
  }
}
```

---

### HgStoreState Service

File: `hg-store-grpc/src/main/proto/store_state.proto`

```protobuf
service HgStoreState {
  // Subscribe to state changes
  rpc SubState(SubStateRequest) returns (stream StateResponse);

  // Unsubscribe
  rpc UnsubState(UnsubStateRequest) returns (UnsubStateResponse);

  // Get partition scan state
  rpc GetScanState(GetScanStateRequest) returns (GetScanStateResponse);

  // Get Raft peers
  rpc GetPeers(GetPeersRequest) returns (GetPeersResponse);
}
```

**StateResponse**:
```protobuf
message StateResponse {
  NodeState node_state = 1;        // Store node state
  repeated PartitionState partition_states = 2;
}

message NodeState {
  int64 store_id = 1;
  string address = 2;
  NodeStateType state = 3;         // STARTING, ONLINE, PAUSE, STOPPING, ERROR
}

enum NodeStateType {
  STARTING = 0;
  STANDBY = 1;
  ONLINE = 2;
  PAUSE = 3;
  STOPPING = 4;
  HALTED = 5;
  ERROR = 6;
}

message PartitionState {
  int32 partition_id = 1;
  int64 leader_term = 2;
  string leader_address = 3;
  PartitionStateType state = 4;
}
```

---

## Query Optimization

### Best Practices

#### 1. Use Indexes for Selective Queries

**Inefficient** (full scan):
```java
// g.V().has('name', 'Alice')  # Scans all vertices
```

**Efficient** (index scan):
```java
// Create index first
schema.indexLabel("personByName").onV("person").by("name").secondary().create()

// Query uses index
// g.V().has('name', 'Alice')  # Scans index only
```

#### 2. Limit Results Early

**Inefficient**:
```java
// Fetches all, then limits at Server
HgStoreQuery query = HgStoreQuery.builder()
    .table("VERTEX")
    .prefix("person:")
    .build();

HgStoreResultSet results = session.query(query);
// Server limits to 100 after fetching all
```

**Efficient**:
```java
// Limits at Store
HgStoreQuery query = HgStoreQuery.builder()
    .table("VERTEX")
    .prefix("person:")
    .limit(100)  // Store limits before sending
    .build();

HgStoreResultSet results = session.query(query);
```

#### 3. Use Aggregations for Counts

**Inefficient**:
```java
// Fetch all, count at Server
HgStoreResultSet results = session.query(query);
int count = 0;
while (results.hasNext()) {
    results.next();
    count++;
}
```

**Efficient**:
```java
// Count at Store
HgStoreQuery query = HgStoreQuery.builder()
    .table("VERTEX")
    .prefix("person:")
    .aggregation(HgStoreQuery.Aggregation.COUNT)
    .build();

long count = session.aggregate(query);
```

#### 4. Batch Reads

**Inefficient** (N sequential requests):
```java
for (String id : vertexIds) {
    byte[] value = session.get(table, id.getBytes());
}
```

**Efficient** (single batch request):
```java
List<byte[]> keys = vertexIds.stream()
    .map(String::getBytes)
    .collect(Collectors.toList());

Map<byte[], byte[]> results = session.batchGet(table, keys);
```

### Query Performance Metrics

**Typical Latencies** (production cluster, 3 Store nodes):

| Operation | Single Partition | Multi-Partition (12 partitions) |
|-----------|------------------|--------------------------------|
| Get by key | 1-2 ms | N/A |
| Scan 100 rows | 5-10 ms | 15-30 ms |
| Scan 10,000 rows | 50-100 ms | 100-200 ms |
| Count (no filter) | 10-20 ms | 30-60 ms |
| Count (with filter) | 20-50 ms | 50-150 ms |
| Index scan (100 rows) | 3-8 ms | 10-25 ms |

**Factors Affecting Performance**:
- Network latency between Server and Store
- RocksDB read amplification (depends on compaction)
- Number of partitions (more partitions = more parallel work but more overhead)
- Filter selectivity (fewer matches = faster)

---

For operational monitoring of query performance, see [Operations Guide](operations-guide.md).

For RocksDB tuning to improve query speed, see [Best Practices](best-practices.md).
