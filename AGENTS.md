# AGENTS.md

Single source of truth for AI coding agents.
README.md covers human-facing deployment/ecosystem context; only consult it on demand.

## Stack & Modules

Apache HugeGraph — Apache TinkerPop 3 compliant graph database.
Java 11+, Maven 3.5+. Version managed via `${revision}` (currently `1.8.0`).

```
Client (Gremlin / Cypher / REST)
   │
Server = hugegraph-server
   ├─ hugegraph-api     REST, Gremlin/Cypher, auth
   ├─ hugegraph-core    engine, schema, traversal, BackendStore interface
   └─ Backend impls     rocksdb (default, embedded) │ hstore (distributed)
                                                    ▼
                           hugegraph-pd (placement) + hugegraph-store (Raft)
```

Top-level modules: `hugegraph-server` · `hugegraph-pd` · `hugegraph-store` ·
`hugegraph-commons` (shared utils & RPC) · `hugegraph-struct` (data types; dep of PD/Store).

Server submodules worth knowing: `hugegraph-core`, `hugegraph-api`,
`hugegraph-rocksdb`, `hugegraph-hstore`, `hugegraph-test`, `hugegraph-dist`.

## Code Search Anchors

| Area | Path |
|---|---|
| Graph engine | `hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/` |
| REST APIs | `hugegraph-server/hugegraph-api/src/main/java/org/apache/hugegraph/api/` |
| Backend interface | `hugegraph-server/hugegraph-core/.../backend/store/BackendStore.java` |
| Auth | `hugegraph-server/hugegraph-api/.../api/auth/` |
| gRPC protos | `hugegraph-{pd,store}/hg-{pd,store}-grpc/src/main/proto/` |

Config roots (under each dist module's `src/assembly/static/conf/`):
- Server — `hugegraph.properties`, `rest-server.properties`, `gremlin-server.yaml`
- PD / Store — `application.yml`

## Build

```bash
# All modules
mvn clean install -DskipTests

# Single module
mvn clean install -pl hugegraph-server -am -DskipTests
```

Distributed build order (for HStore-enabled dev):

```bash
mvn install -pl hugegraph-struct -am -DskipTests         # 1. shared data types
mvn clean package -pl hugegraph-pd -am -DskipTests       # 2. placement driver
mvn clean package -pl hugegraph-store -am -DskipTests    # 3. distributed storage
mvn clean package -pl hugegraph-server -am -DskipTests   # 4. server
```

Runtime scripts (human-run) live in `hugegraph-server/hugegraph-dist/src/assembly/static/bin/`:
`init-store.sh`, `start-hugegraph.sh`, `stop-hugegraph.sh`.

## Testing

Server tests implicitly prefix `mvn test -pl hugegraph-server/hugegraph-test -am`:

| Profile | Suffix |
|---|---|
| Unit | `-P unit-test` |
| Core | `-P core-test,rocksdb` (swap `rocksdb` for `memory`) |
| API | `-P api-test,rocksdb` |
| TinkerPop structure / process | `-P tinkerpop-{structure,process}-test,memory` |
| Single class | `-P core-test,rocksdb -Dtest=YourTestClass` |

PD / Store tests (need `hugegraph-struct` installed first):

```bash
mvn install -pl hugegraph-struct -am -DskipTests
mvn test -pl hugegraph-pd/hg-pd-test -am
mvn test -pl hugegraph-store/hg-store-test -am
```

Before writing new tests, check existing suites under `hugegraph-server/hugegraph-test/`.

## Style & Pre-commit

- Line 100, 4-space indent, LF, UTF-8, **no star imports**
- Commit format: `feat|fix|refactor(module): msg`
- Run before pushing:
  ```bash
  mvn editorconfig:format                         # enforce code style
  mvn clean compile -Dmaven.javadoc.skip=true     # surface warnings
  ```

## Cross-module notes

- `.proto` edits: `mvn clean compile` regenerates gRPC stubs under
  `target/generated-sources/protobuf/` (output packages `*/grpc/` are excluded from Apache RAT).
- Adding a third-party dep: update `install-dist/release-docs/{LICENSE,NOTICE,licenses/}`
  and `install-dist/scripts/dependency/known-dependencies.txt`.
- `hugegraph-commons` is shared by every module; `hugegraph-struct` must precede PD/Store;
  server backends depend on `hugegraph-core`.

## Additional context files

`.serena/memories/` — notably `suggested_commands.md` and `task_completion_checklist.md`
when a task needs depth beyond this file.
