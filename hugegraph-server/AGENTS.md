# Server guidance

Shared rules: [root AGENTS.md](../AGENTS.md). Paths here are relative to this module.

## Entry points

- Engine/schema/traversal: `hugegraph-core/src/main/java/org/apache/hugegraph/`.
- REST, auth and graph lifecycle: `hugegraph-api/src/main/java/org/apache/hugegraph/`.
- Backend contract: `hugegraph-core/src/main/java/org/apache/hugegraph/backend/store/BackendStore.java`.
- HStore adapter: `hugegraph-hstore/`; distributed client lives in [Store](../hugegraph-store/AGENTS.md).
- Shipped configs/scripts: [distribution sources](hugegraph-dist/src/assembly/static/).
  Graph configs are in `conf/graphs/`; REST and Gremlin configs are in `conf/`.

## Tests

Run from the repository root:

```bash
mvn test -pl hugegraph-server/hugegraph-test -am -P unit-test
mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,rocksdb
mvn test -pl hugegraph-server/hugegraph-test -am -P api-test,rocksdb
```

- Suite sources are in `hugegraph-test/src/main/java/`, not `src/test/java/`.
- Combine test and backend profiles. TinkerPop profiles:
  `tinkerpop-structure-test`, `tinkerpop-process-test`.
- Selection and suite registration: [test POM](hugegraph-test/pom.xml).
- Service setup: [Server CI](../.github/workflows/server-ci.yml);
  HStore setup: [PD/Store CI](../.github/workflows/pd-store-ci.yml).

## Cross-module traps

- Struct has separate ID/query/serializer implementations. For format changes, inspect
  [Struct](../hugegraph-struct/AGENTS.md) and both read/write paths.
- Authentication spans REST and Gremlin; inspect both shipped configs when changing bootstrap.
