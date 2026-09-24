# Commons guidance

Shared rules: [root AGENTS.md](../AGENTS.md). Paths here are relative to this module.

## Entry points

- Config, locks, events, iterators and REST: `hugegraph-common/src/main/java/org/apache/hugegraph/`.
- RPC framework: `hugegraph-rpc/`, which depends on `hugegraph-common`.
- Commons targets Java 8 bytecode in [pom.xml](pom.xml), although CI uses JDK 11.
  Do not infer its language/API baseline from Server's JDK requirement.

## Tests

Tests are skipped by default through `skipCommonsTests`. After installing dependencies,
run from the repository root:

```bash
mvn test -pl hugegraph-commons/hugegraph-common -Dtest=UnitTestSuite -DskipCommonsTests=false
mvn test -pl hugegraph-commons/hugegraph-rpc -Dtest=UnitTestSuite -DskipCommonsTests=false
```

- Tests live in each module's `src/test/java/`.
- CI selects `UnitTestSuite`; add new tests to the relevant suite.
- Full setup: [Commons CI](../.github/workflows/commons-ci.yml).

## Compatibility

- Utility behavior is shared across components, including iterator cleanup,
  configuration parsing and lock semantics.
- Version changes: check `CommonVersion.java` and `RpcVersion.java` alongside the POM.
- Details on demand: [Common README](hugegraph-common/README.md) and
  [RPC README](hugegraph-rpc/README.md).
