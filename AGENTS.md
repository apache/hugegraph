# AGENTS.md

Repository-specific guidance for coding agents. Module files add local ownership, commands and failure modes.

## System map

```text
Server: graph engine and user APIs
  ├─ RocksDB: embedded standalone backend
  └─ HStore adapter → Store client → Store partition data and Raft

PD: placement and metadata coordination
Struct: shared graph types and codecs
Commons: shared utilities and RPC infrastructure
```

## Load on demand

| Work area | Read first |
|---|---|
| Documentation ownership and durable links | [Repository docs index](docs/README.md) |
| Graph engine, API and backends | [Server guidance](hugegraph-server/AGENTS.md) |
| Placement, metadata and PD clients | [PD guidance](hugegraph-pd/AGENTS.md) |
| Distributed storage and Store clients | [Store guidance](hugegraph-store/AGENTS.md) |
| Shared graph types and serialization | [Struct guidance](hugegraph-struct/AGENTS.md) |
| Utilities and RPC | [Commons guidance](hugegraph-commons/AGENTS.md) |
| Docker deployment | [Docker guide](docker/README.md) |
| Kubernetes deployment | [Helm guide](helm/hugegraph/README.md) |
| Contribution requirements | [Contribution guide](docs/CONTRIBUTING.md) |

## Repository constraints

- Keep changes inside the module that owns the behavior; exclude unrelated cleanup from a functional fix.
- A user-visible feature, configuration or deployment change requires matching documentation in the same change. Link a coordinated `apache/hugegraph-doc` change when the website is affected; a follow-up issue alone is not sufficient.
- Keep root and module READMEs as entry points. Put detailed procedures in a focused document and link it from the relevant index.
- Treat `docs/README.md` as the repository documentation map. New durable Markdown files must be linked from it or from a module-local index.
- Treat `.serena/` as a small navigation cache. Do not store duplicated procedures, fixed release numbers, timestamps, review history, pull-request links or commit hashes there.
- Write documentation for the current contract and operational boundary, not as a development diary. Keep prose paragraphs as logical lines; wrap near 160 characters only when necessary for review.

## Build and validation

Run commands from the repository root. Java and Maven requirements are defined by [pom.xml](pom.xml) and summarized in [BUILDING.md](docs/BUILDING.md).

```bash
mvn clean install -DskipTests
mvn clean install -pl hugegraph-server -am -DskipTests
```

For separate distributed-module builds, install `hugegraph-struct` first, then build PD, Store and Server. Commons tests require `-DskipCommonsTests=false`; a successful aggregate build does not imply those tests ran.

Before pushing code, run `mvn editorconfig:format`, `mvn clean compile -Dmaven.javadoc.skip=true` and the tests selected by the affected modules. Documentation-only changes require relative-link review, rendered-heading review and `git diff --check`.

## Non-obvious boundaries

- Server changes commonly require both Server and PD/Store CI because the HStore path crosses module boundaries.
- PD and Store protocol builds generate Java into their gRPC modules; inspect the module POM before editing or deleting generated sources.
- New dependencies require updates to `install-dist/release-docs/` and `install-dist/scripts/dependency/known-dependencies.txt`.
- Read versions, plugin settings and module membership from Maven configuration instead of copying them into agent notes.
