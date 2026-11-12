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

### Launcher shell fixtures

No JVM, cluster or network needed; run directly with `bash`:

```bash
bash hugegraph-dist/src/test/shell/test-cron-quote.sh       # crontab locking/quoting, shared util.sh
bash hugegraph-dist/src/test/shell/test-canonicalize.sh     # canonicalize_dir/canonicalize_file
bash hugegraph-dist/src/test/shell/test-launcher-argv.sh    # launcher argv/classpath construction
bash hugegraph-dist/src/test/shell/test-override-guards.sh  # -c/-l/-o/-i guard coverage
```

`test-cron-quote.sh` and `test-override-guards.sh` walk up to the repository root and
also exercise the PD and Store copies of `util.sh` in turn, since the three modules
ship separate, duplicated copies of these helpers. All four run in CI for the
`rocksdb` backend job: see "Run path-override guard and quoting unit tests" in
[server-tests.yml](../.github/workflows/server-tests.yml).

## Cross-module traps

- Struct has separate ID/query/serializer implementations. For format changes, inspect
  [Struct](../hugegraph-struct/AGENTS.md) and both read/write paths.
- Authentication spans REST and Gremlin; inspect both shipped configs when changing bootstrap.
