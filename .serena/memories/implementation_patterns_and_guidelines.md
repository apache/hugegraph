# Implementation Patterns and Guidelines

## Backend Architecture
- Backends implement `BackendStore` interface from `hugegraph-core`
- Each backend = separate Maven module under `hugegraph-server/`
- Configured via `hugegraph.properties` → `backend` property
- **Active backends (focus here)**: RocksDB (default/embedded), HStore (distributed)
- **Legacy backends** (deprecated, excluded from Serena context): MySQL, PostgreSQL, Cassandra, ScyllaDB, HBase, Palo

## GraphSpace Multi-Tenancy
- Core: `hugegraph-core/.../space/` (GraphSpace, SchemaTemplate, Service, register/)
- API: `hugegraph-api/.../api/space/GraphSpaceAPI.java` (includes GS profile endpoints)
- **Standalone mode**: GraphSpaceAPI and ManagerAPI are disabled

## Auth System
- Disabled by default, enable via `bin/enable-auth.sh`
- ConfigAuthenticator was removed, use standard auth
- Multi-level: Users, Groups, Projects, Targets, Access control
- Location: `hugegraph-api/.../api/auth/`

## gRPC Protocol
- PD protos: `hugegraph-pd/hg-pd-grpc/src/main/proto/`
- Store protos: `hugegraph-store/hg-store-grpc/src/main/proto/`
- After `.proto` changes: `mvn clean compile` → `target/generated-sources/protobuf/`

## Query Languages
- **Gremlin**: Native TinkerPop 3.5.1
- **OpenCypher**: `hugegraph-api/opencypher/`
- TinkerPop exceptions are passed through in Gremlin responses

## Schema
- Labels support TTL with runtime update
- Edge label conflicting conditions are handled safely

## Testing
- **Profiles**: `unit-test`, `core-test`, `api-test`, `tinkerpop-structure-test`, `tinkerpop-process-test`
- **Backends in CI**: memory, rocksdb, hbase (matrix)
- **Single test class**: `mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,memory -Dtest=ClassName`
- TinkerPop tests: only on `release-*`/`test-*` branches
- Raft tests: only on `test*`/`raft*` branches

## Docker
- Single-node: `docker/docker-compose.yml` (bridge network, pd+store+server)
- Cluster: `docker/docker-compose-3pd-3store-3server.yml`
- Container logs: stdout-based

## CI Pipelines
- `server-ci.yml`: compile + unit/core/API tests (memory/rocksdb/hbase × Java 11)
- `rerun-ci.yml`: auto-rerun flaky failures (max 2 reruns, 180s delay)
- `auto-pr-review.yml`: auto-comment on new PRs
