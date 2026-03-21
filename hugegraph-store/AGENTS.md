# AGENTS.md

This file provides guidance to an AI coding tool when working with code in this repository.

## Project Overview

HugeGraph Store is a distributed storage backend for Apache HugeGraph, using RocksDB as the underlying storage engine with Raft consensus protocol for distributed coordination. It is designed for production-scale deployments requiring high availability and horizontal scalability.

**Technology Stack**:
- Java 11+
- RocksDB: Embedded key-value storage engine
- Raft (JRaft): Distributed consensus protocol
- gRPC: Inter-node communication
- Protocol Buffers: Data serialization

## Architecture

### Module Structure

HugeGraph Store consists of 9 submodules:

```
hugegraph-store/
├── hg-store-common      # Shared utilities, constants, query abstractions
├── hg-store-grpc        # gRPC protocol definitions (proto files) and generated stubs
├── hg-store-client      # Client library for connecting to Store cluster
├── hg-store-rocksdb     # RocksDB abstraction and optimizations
├── hg-store-core        # Core storage logic, partition management
├── hg-store-node        # Store node server implementation with Raft
├── hg-store-dist        # Distribution packaging, scripts, configs
├── hg-store-cli         # Command-line tools for cluster management
└── hg-store-test        # Integration and unit tests
```

### Key Package Structure

```
org/apache/hugegraph/store/
├── grpc/               # Generated gRPC stubs (do not edit manually)
├── client/             # Client API for Store operations
├── node/               # Store node server and Raft integration
├── core/               # Core storage abstractions
│   ├── store/          # Store interface and implementations
│   ├── partition/      # Partition management
│   └── raft/           # Raft consensus integration
├── rocksdb/            # RocksDB wrapper and optimizations
├── query/              # Query processing and aggregation
└── util/               # Common utilities
```

### Distributed Architecture

Store operates as a cluster of nodes:
- **Store Nodes**: 3+ nodes (typically 3 or 5 for Raft quorum)
- **Raft Groups**: Data partitioned into Raft groups for replication
- **PD Coordination**: Requires hugegraph-pd for cluster metadata and partition assignment
- **Client Access**: hugegraph-server connects via hg-store-client

## Build Commands

### Prerequisites
```bash
# HugeGraph Store depends on hugegraph-struct
# Build struct module first from repository root
cd /path/to/hugegraph-org
mvn install -pl hugegraph-struct -am -DskipTests
```

### Full Build
```bash
# From hugegraph-store directory
mvn clean install -DskipTests

# Build with tests
mvn clean install

# Build specific module (e.g., client only)
mvn clean install -pl hg-store-client -am -DskipTests
```

### Testing

**Test profiles** (defined in pom.xml):
- `store-client-test` (default): Client library tests
- `store-core-test` (default): Core storage tests
- `store-common-test` (default): Common utilities tests
- `store-rocksdb-test` (default): RocksDB abstraction tests
- `store-server-test` (default): Store node server tests
- `store-raftcore-test` (default): Raft consensus tests

```bash
# Run all tests (from hugegraph-store/)
mvn test -pl hg-store-test -am

# Run specific test class
mvn test -pl hg-store-test -am -Dtest=YourTestClassName

# Run tests for specific module
mvn test -pl hg-store-core -am
mvn test -pl hg-store-client -am
```

### Code Quality
```bash
# License header check (Apache RAT) - from repository root
mvn apache-rat:check

# EditorConfig validation - from repository root
mvn editorconfig:check
```

## Running Store Cluster

Scripts are located in `hg-store-dist/src/assembly/static/bin/`:

```bash
# Start Store node
bin/start-hugegraph-store.sh

# Stop Store node
bin/stop-hugegraph-store.sh

# Restart Store node
bin/restart-hugegraph-store.sh
```

**Important**: For a functional distributed cluster, you need:
1. HugeGraph PD cluster running (3+ nodes)
2. HugeGraph Store cluster (3+ nodes)
3. Proper configuration pointing Store nodes to PD cluster

See Docker Compose examples in the repository root `../docker/` directory. Single-node quickstart (pre-built images): `../docker/docker-compose.yml`. Single-node dev build (from source): `../docker/docker-compose.dev.yml`. 3-node cluster: `../docker/docker-compose-3pd-3store-3server.yml`. See `../docker/README.md` for the full setup guide.

## Configuration Files

Located in `hg-store-dist/src/assembly/static/conf/`:

- **`application.yml`**: Main Store node configuration
  - RocksDB settings (data paths, cache sizes, compaction)
  - Raft configuration (election timeout, snapshot interval)
  - Network settings (gRPC ports)
  - Store capacity and partition management

- **`application-pd.yml`**: PD client configuration
  - PD cluster endpoints
  - Heartbeat intervals
  - Partition query settings

- **`log4j2.xml`**: Logging configuration

## Important Development Notes

### gRPC Protocol Definitions

Protocol Buffer files are in `hg-store-grpc/src/main/proto/`:
- `store_common.proto` - Common data structures
- `store_session.proto` - Client-server session management
- `store_state.proto` - Cluster state and metadata
- `store_stream_meta.proto` - Streaming operations
- `graphpb.proto` - Graph data structures
- `query.proto` - Query operations
- `healthy.proto` - Health check endpoints

**When modifying `.proto` files**:
1. Edit the `.proto` file in `hg-store-grpc/src/main/proto/`
2. Run `mvn clean compile` to regenerate Java stubs
3. Generated code appears in `target/generated-sources/protobuf/`
4. Generated files are excluded from license checks

### Module Dependencies

Build order matters due to dependencies:
```
hugegraph-struct (external)
    ↓
hg-store-common
    ↓
hg-store-grpc → hg-store-rocksdb
    ↓
hg-store-core
    ↓
hg-store-client, hg-store-node
    ↓
hg-store-cli, hg-store-dist, hg-store-test
```

Always build `hugegraph-struct` first, then Store modules follow Maven reactor order.

### Working with RocksDB

Store uses RocksDB for persistent storage:
- Abstraction layer: `hg-store-rocksdb/src/main/java/org/apache/hugegraph/rocksdb/`
- Column families for different data types
- Custom compaction and compression settings
- Optimized for graph workloads (vertices, edges, indexes)

Configuration in `application.yml`:
- `rocksdb.data-path` - Data directory location
- `rocksdb.block-cache-size` - In-memory cache size
- `rocksdb.write-buffer-size` - Write buffer configuration

### Raft Consensus Integration

Store uses JRaft (Ant Financial's Raft implementation):
- Each partition is a Raft group with 3 replicas (typically)
- Leader election, log replication, snapshot management
- Configuration: `raft.*` settings in `application.yml`

Key Raft operations:
- Snapshot creation and loading
- Log compaction
- Leadership transfer
- Membership changes

### Client Development

When working with `hg-store-client`:
- Client connects to PD to discover Store nodes
- Automatic failover and retry logic
- Connection pooling and load balancing
- Batch operations support

Example usage in hugegraph-server:
- Backend: `hugegraph-server/hugegraph-hstore/`
- Client integration: Uses `hg-store-client` library

### Partition Management

Data is partitioned for distributed storage:
- Partition assignment managed by PD
- Partition splitting and merging (future feature)
- Partition rebalancing on node addition/removal
- Hash-based partition key distribution

## Common Development Tasks

### Adding New gRPC Service

1. Define service in appropriate `.proto` file in `hg-store-grpc/src/main/proto/`
2. Add message definitions for request/response
3. Run `mvn clean compile` to generate stubs
4. Implement service in `hg-store-node/` server
5. Add client methods in `hg-store-client/`
6. Add tests in `hg-store-test/`

### Modifying Storage Engine

1. Core storage interfaces: `hg-store-core/src/main/java/org/apache/hugegraph/store/core/store/`
2. RocksDB implementation: `hg-store-rocksdb/`
3. Update Raft state machine if needed: `hg-store-node/src/main/java/org/apache/hugegraph/store/node/raft/`
4. Consider backward compatibility for stored data format

### Adding Query Operations

1. Query abstractions: `hg-store-common/src/main/java/org/apache/hugegraph/store/query/`
2. Aggregation functions: `hg-store-common/.../query/func/`
3. Update proto definitions if new query types needed
4. Implement in `hg-store-core/` and expose via gRPC

### Cluster Testing

For distributed cluster tests:
- Module: `hugegraph-cluster-test/` (repository root)
- Requires: PD cluster + Store cluster + Server instances
- Docker Compose recommended for local testing
- CI/CD: See `.github/workflows/cluster-test-ci.yml`

## Debugging Tips

- **Logging**: Edit `hg-store-dist/src/assembly/static/conf/log4j2.xml` for detailed logs
- **Raft State**: Check Raft logs and snapshots in data directory
- **RocksDB Stats**: Enable RocksDB statistics in `application.yml`
- **gRPC Tracing**: Enable gRPC logging for request/response debugging
- **PD Connection**: Verify Store can connect to PD endpoints
- **Health Checks**: Use gRPC health check service for node status

## Cross-Repository Integration

Store integrates with other HugeGraph components:

1. **hugegraph-pd**: Cluster metadata and partition management
   - Store registers with PD on startup
   - PD assigns partitions to Store nodes
   - Heartbeat mechanism for health monitoring

2. **hugegraph-server**: Graph engine uses Store as backend
   - Backend implementation: `hugegraph-server/hugegraph-hstore/`
   - Uses `hg-store-client` for Store cluster access
   - Configuration: `backend=hstore` in `hugegraph.properties`

3. **hugegraph-commons**: Shared utilities
   - RPC framework: `hugegraph-commons/hugegraph-rpc/`
   - Common utilities: `hugegraph-commons/hugegraph-common/`

## Version Management

- Version managed via `${revision}` property (currently 1.7.0)
- Flatten Maven plugin for CI-friendly versioning
- Must match version of other HugeGraph components (server, PD)

## Special Notes

### Performance Tuning

Key performance factors:
- RocksDB block cache size (memory)
- Raft batch size and flush interval
- gRPC connection pool size
- Partition count and distribution
- Network latency between nodes

Refer to `application.yml` for tuning parameters.
