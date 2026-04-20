# AGENTS.md

Development reference for AI coding agents. See [README.md](README.md) if the task really needs project overview or architecture context.

## Key Architectural Patterns

1. **Pluggable Backend Architecture**: Storage backends implement the `BackendStore` interface — new backends require no core changes. Active backends: RocksDB (default/embedded), HStore (distributed)
2. **gRPC Communication**: All distributed components (PD, Store, Server) communicate via gRPC. Proto definitions in `*/grpc/` directories
3. **Multi-Language Queries**: Native Gremlin support(Main) + OpenCypher translation(Backup)

## Build Commands

Requires Java 11 for compilation and testing.

```bash
# Full build (all modules)
mvn clean install -DskipTests

# Single module (e.g., server only)
mvn clean install -pl hugegraph-server -am -DskipTests
```

## Testing

All test commands target `hugegraph-server/hugegraph-test` with `-am` flag:

| Profile | Command |
|---------|---------|
| Unit tests | `mvn test -P unit-test` |
| Core tests | `mvn test -P core-test,rocksdb` |
| API tests | `mvn test -P api-test,rocksdb` |
| TinkerPop structure | `mvn test -P tinkerpop-structure-test,memory` |
| TinkerPop process | `mvn test -P tinkerpop-process-test,memory` |
| Single test class | `mvn test -P core-test,rocksdb -Dtest=YourTestClass` |

All commands above implicitly start with `mvn test -pl hugegraph-server/hugegraph-test -am`.

#### PD & Store Tests

```bash
# Build dependency first
mvn install -pl hugegraph-struct -am -DskipTests

mvn test -pl hugegraph-pd/hg-pd-test -am
mvn test -pl hugegraph-store/hg-store-test -am
```

## Development Conventions

- Any code change (bug fix or feature) must have sufficient test coverage for the affected logic
- Check existing suites in `hugegraph-server/hugegraph-test` before writing new tests

## Code Quality

Run before every commit:

```bash
mvn editorconfig:format # Apply code style (root .editorconfig)
mvn clean compile -Dmaven.javadoc.skip=true # Compile with warnings
```

Key rules from `.editorconfig`: 100-char line limit, 4-space indent, LF endings, UTF-8.

## Running the Server

Scripts in `hugegraph-server/hugegraph-dist/src/assembly/static/bin/`:

```bash
bin/init-store.sh          # Initialize storage backend
bin/start-hugegraph.sh     # Start server
bin/stop-hugegraph.sh      # Stop server
```

## Configuration Files

| Component | Path | Key Files |
|-----------|------|-----------|
| Server | `hugegraph-server/hugegraph-dist/src/assembly/static/conf/` | `hugegraph.properties`, `rest-server.properties`, `gremlin-server.yaml` |
| PD | `hugegraph-pd/hg-pd-dist/src/assembly/static/conf/` | `application.yml` |
| Store | `hugegraph-store/hg-store-dist/src/assembly/static/conf/` | `application.yml` |

## Development Workflows

### Adding Third-Party Dependencies

Follow ASF compliance: update `install-dist/release-docs/` (LICENSE, NOTICE, licenses/) and `install-dist/scripts/dependency/known-dependencies.txt`.

### gRPC Protocol Changes

When modifying `.proto` files: Run `mvn clean compile` to regenerate gRPC stubs

### Build Order & Cross-Module Dependencies

For distributed development, build in this order:

```bash
mvn install -pl hugegraph-struct -am -DskipTests       # 1. Shared data structures
mvn clean package -pl hugegraph-pd -am -DskipTests      # 2. Placement Driver
mvn clean package -pl hugegraph-store -am -DskipTests    # 3. Distributed storage
mvn clean package -pl hugegraph-server -am -DskipTests   # 4. Server
```

Key dependencies: `hugegraph-commons` is shared by all modules. `hugegraph-struct` must be built before PD and Store. Server backends depend on `hugegraph-core`.

## Reference Documents

| Resource | When to consult |
|----------|-----------------|
| `README.md` | Project overview, deployment topology, contribution guide |
| `.serena/memories/` | Project agent memory; key files: `suggested_commands.md` (commands), `task_completion_checklist.md` (pre-commit checks) |
