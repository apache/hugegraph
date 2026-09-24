# PD guidance

Shared rules: [root AGENTS.md](../AGENTS.md). Paths here are relative to this module.

## Entry points

- Partition allocation and Store registration: `hg-pd-core/src/main/java/org/apache/hugegraph/pd/`.
- Metadata persistence and Raft: its `meta/` and `raft/` packages.
- RPC/REST handlers: `hg-pd-service/`; discovery, locks and watches: `hg-pd-client/`.
- Wire definitions: [protos](hg-pd-grpc/src/main/proto/).
- Shipped config: [distribution conf](hg-pd-dist/src/assembly/static/conf/).
  Container endpoints/peers: [Docker guide](../docker/README.md).

## Build and tests

Run from the repository root:

```bash
mvn install -pl hugegraph-struct -am -DskipTests
mvn test -pl hugegraph-pd/hg-pd-test -am
```

- Tests live in `hg-pd-test/src/main/java/`, not `src/test/java/`.
- Test profiles in the [parent POM](pom.xml) activate matching Surefire executions;
  suite selection is configured in the [test POM](hg-pd-test/pom.xml).
- Service setup: [PD/Store CI](../.github/workflows/pd-store-ci.yml).

## Cross-module traps

- Trace metadata mutations through `RaftTaskHandler` / `KVOperation` and the state machine.
- Client changes affect Server and Store; inspect watch/lock callers before changing
  transport lifecycle or timeout behavior.
