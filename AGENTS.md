# AGENTS.md

Repository-specific guidance; module files add local details.

## Key relationships

```text
Server (hugegraph-server): graph engine + REST/Gremlin API
  ├─ RocksDB: embedded backend
  └─ HStore adapter → Store client → Store: partition data + Raft

PD: placement + metadata; consulted by Server and Store
Struct: shared types/codecs; separate implementations also exist in Server
Commons: shared utilities; independent compiler and test settings
```

## Load on demand

| Work area | Read when working there |
|---|---|
| Graph engine, API, backends | [Server](hugegraph-server/AGENTS.md) |
| Placement, metadata, PD client | [PD](hugegraph-pd/AGENTS.md) |
| Distributed storage and client | [Store](hugegraph-store/AGENTS.md) |
| Shared data types and serialization | [Struct](hugegraph-struct/AGENTS.md) |
| Utilities and RPC | [Commons](hugegraph-commons/AGENTS.md) |
| Container deployment | [Docker guide](docker/README.md) |
| PR requirements | [Contribution guide](docs/CONTRIBUTING.md) |

## Repository constraints

- Use existing module boundaries; keep unrelated refactors out of a fix.
- User-visible feature, configuration or deployment behavior changes must ship with matching docs.
  Update in-repository docs in the same PR; link a paired `apache/hugegraph-doc` PR when website
  docs are affected and coordinate both merges. A follow-up issue alone does not satisfy this rule.
  Internal-only changes can use `Doc - No Need`.
- Keep README as an entry point; link detailed deployment instructions instead of duplicating them.
- Keep AGENTS.md under 100 lines where practical. Shared rules belong here, local exceptions
  in module files. Keep key relationships, common commands and non-obvious pitfalls inline;
  reference existing sources for versions, configuration details and lengthy procedures.

## Build and validation

Commands run from the repository root. Java 11+ and Maven 3.5+;
versions come from [pom.xml](pom.xml), including `${revision}`.

```bash
mvn clean install -DskipTests
mvn clean install -pl hugegraph-server -am -DskipTests
```

For separate distributed-module builds, install `hugegraph-struct` first,
then build PD, Store and Server. Module guidance covers tests and CI prerequisites.
Commons tests require `-DskipCommonsTests=false`; a successful build does not imply its tests ran.

- Java style: 120 columns, 4 spaces, no star imports; see [.editorconfig](.editorconfig).
- Before pushing code: `mvn editorconfig:format`,
  `mvn clean compile -Dmaven.javadoc.skip=true`, and relevant module tests.
  Documentation-only changes need link/path checks and `git diff --check`.
- PD/Store `.proto` builds generate Java into each `hg-*-grpc/src/main/java/` directory;
  see the corresponding gRPC module POM before editing or cleaning generated sources.
- New dependencies require updating [release LICENSE/NOTICE/licenses](install-dist/release-docs/)
  and [dependency inventory](install-dist/scripts/dependency/known-dependencies.txt).
