# Store guidance

Shared rules: [root AGENTS.md](../AGENTS.md). Paths here are relative to this module.

## Entry points

- Routing, retry and streaming: `hg-store-client/`.
- Partition lifecycle, Raft and snapshots: `hg-store-core/src/main/java/org/apache/hugegraph/store/`.
- RocksDB access: `hg-store-rocksdb/`; RPC/REST serving: `hg-store-node/`.
- Wire definitions: [protos](hg-store-grpc/src/main/proto/).
- Shipped configs/scripts: [distribution sources](hg-store-dist/src/assembly/static/).
- Server adapter: [HStore backend](../hugegraph-server/hugegraph-hstore/).

## Build and tests

Run from the repository root:

```bash
mvn install -pl hugegraph-struct -am -DskipTests
mvn test -pl hugegraph-store/hg-store-test -am
```

- Tests live in `hg-store-test/src/main/java/`, not `src/test/java/`.
- Test profiles in the [parent POM](pom.xml) activate matching Surefire executions.
  Check [test POM](hg-store-test/pom.xml) suite includes when adding tests.
- CoreSuiteTest and BatchGraphIsolationTest need separate JVM forks: Store shutdown leaves
  singleton state closed. Preserve `reuseForks=false` in the core test execution.
- Test prerequisites: [PD/Store CI](../.github/workflows/pd-store-ci.yml).
  Cluster setup: [Docker guide](../docker/README.md).

## Cross-module traps

- PD manages placement; Store owns graph data and partition Raft groups.
  Routing or membership changes cross both components.
- Codec changes must agree with [Struct](../hugegraph-struct/AGENTS.md) and the Server adapter.
