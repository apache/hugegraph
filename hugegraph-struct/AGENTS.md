# Struct guidance

Shared rules: [root AGENTS.md](../AGENTS.md). Paths here are relative to this module.

## Entry points

Sources: `src/main/java/org/apache/hugegraph/`.

- Schema metadata: `struct/schema/`; graph instances: `structure/`.
- IDs, binary serialization and conditions: `id/`, `serializer/`, `query/`.
- Type codes: `type/`; metadata client lifecycle: `meta/`.
- Dependency versions and compiler level: [pom.xml](pom.xml).

## Build and tests

Run from the repository root:

```bash
mvn install -pl hugegraph-struct -am -DskipTests
mvn test -pl hugegraph-struct -am
```

Tests exist under `src/test/java/`. See [PD/Store CI](../.github/workflows/pd-store-ci.yml)
for the Struct job and downstream build setup.

## Compatibility

- Server core has separate query, ID and serializer implementations;
  matching class names do not guarantee matching behavior.
- Type codes, ID encoding and binary layout affect persisted data and Store decoding.
  Check corresponding Server and Store readers/writers before changing the format.
