# Architecture and Module Structure

## Three-Tier Architecture

### 1. Client Layer
- Gremlin/Cypher queries, REST APIs, Swagger UI

### 2. Server Layer (hugegraph-server, 8 submodules)
- **REST API** (hugegraph-api): GraphAPI, SchemaAPI, GremlinAPI, CypherAPI, AuthAPI, GraphSpaceAPI (distributed only), ManagerAPI (distributed only)
- **Graph Engine** (hugegraph-core): Schema (with TTL update), traversal, task scheduling, GraphSpace multi-tenancy
- **Backend Interface**: Pluggable via `BackendStore`

### 3. Storage Layer
- RocksDB (default/embedded), HStore (distributed/production)
- HBase (deprecated; planned for removal in 2.0)

## Module Structure (7 top-level modules)

### hugegraph-server (8 submodules)
`hugegraph-core`, `hugegraph-api` (includes `opencypher/`, `space/`), `hugegraph-dist`, `hugegraph-test`, `hugegraph-example`, plus backends: `hugegraph-rocksdb`, `hugegraph-hstore`, `hugegraph-hbase`

### hugegraph-pd (8 submodules)
Placement Driver: `hg-pd-core`, `hg-pd-service`, `hg-pd-client`, `hg-pd-common`, `hg-pd-grpc`, `hg-pd-cli`, `hg-pd-dist`, `hg-pd-test`

### hugegraph-store (9 submodules)
Distributed storage + Raft: `hg-store-core`, `hg-store-node`, `hg-store-client`, `hg-store-common`, `hg-store-grpc`, `hg-store-rocksdb`, `hg-store-cli`, `hg-store-dist`, `hg-store-test`

### Others
- **hugegraph-commons**: Shared utilities, RPC framework
- **hugegraph-struct**: Data structures (must build before PD/Store)
- **install-dist**: Distribution packaging, license files
- **hugegraph-cluster-test**: Cluster integration tests

## Distributed Deployment (BETA)
PD + Store + Server (3+ nodes each), all gRPC. Docker compose configs in `docker/` directory, using bridge networking (migrated from host mode).
