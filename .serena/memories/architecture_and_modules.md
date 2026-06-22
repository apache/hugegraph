# Architecture and Module Structure

## Three-Tier Architecture

### 1. Client Layer
- Gremlin/Cypher query interfaces
- REST API endpoints
- Multiple client language bindings

### 2. Server Layer (hugegraph-server)
- **REST API Layer** (hugegraph-api): GraphAPI, SchemaAPI, GremlinAPI, CypherAPI, AuthAPI
- **Graph Engine Layer** (hugegraph-core): Schema management, traversal optimization, task scheduling
- **Backend Interface**: Abstraction over storage backends

### 3. Storage Layer
- Pluggable backend implementations
- Each backend extends `hugegraph-core` abstractions
- Implements `BackendStore` interface

## Multi-Module Structure

The project consists of 7 main modules:

### 1. hugegraph-server (13 submodules)
Core graph engine, REST APIs, and backend implementations:
- `hugegraph-core` - Core graph engine and abstractions
- `hugegraph-api` - REST API implementations (includes OpenCypher in `opencypher/`)
- `hugegraph-dist` - Distribution packaging and scripts
- `hugegraph-test` - Test suites (unit, core, API, TinkerPop)
- `hugegraph-example` - Example code
- Backend implementations:
  - `hugegraph-rocksdb` (default)
  - `hugegraph-hstore` (distributed)
  - `hugegraph-hbase`
  - `hugegraph-mysql`
  - `hugegraph-postgresql`
  - `hugegraph-cassandra`
  - `hugegraph-scylladb`
  - `hugegraph-palo`

### 2. hugegraph-pd (8 submodules)
Placement Driver for distributed deployments (meta server):
- `hg-pd-core` - Core PD logic
- `hg-pd-service` - PD service implementation
- `hg-pd-client` - Client library
- `hg-pd-common` - Shared utilities
- `hg-pd-grpc` - gRPC protocol definitions (auto-generated)
- `hg-pd-cli` - Command line interface
- `hg-pd-dist` - Distribution packaging
- `hg-pd-test` - Test suite

### 3. hugegraph-store (9 submodules)
Distributed storage backend with RocksDB and Raft:
- `hg-store-core` - Core storage logic
- `hg-store-node` - Storage node implementation
- `hg-store-client` - Client library
- `hg-store-common` - Shared utilities
- `hg-store-grpc` - gRPC protocol definitions (auto-generated)
- `hg-store-rocksdb` - RocksDB integration
- `hg-store-cli` - Command line interface
- `hg-store-dist` - Distribution packaging
- `hg-store-test` - Test suite

### 4. hugegraph-commons
Shared utilities across modules:
- Locks and concurrency utilities
- Configuration management
- RPC framework components

### 5. hugegraph-struct
Data structure definitions shared between modules.
**Important**: Must be built before PD and Store modules.

### 6. install-dist
Distribution packaging and release management:
- License and NOTICE files
- Dependency management scripts
- Release documentation

### 7. hugegraph-cluster-test
Cluster integration tests for distributed deployments

## Cross-Module Dependencies

```
hugegraph-commons → (shared by all modules)
hugegraph-struct → hugegraph-pd + hugegraph-store
hugegraph-core → (extended by all backend implementations)
```

## Distributed Architecture (Optional)

For production distributed deployments:
- **hugegraph-pd**: Service discovery, partition management, metadata
- **hugegraph-store**: Distributed storage with Raft (3+ nodes)
- **hugegraph-server**: Multiple server instances (3+)
- Communication: All use gRPC with Protocol Buffers

**Status**: Distributed components (PD + Store) are in BETA
