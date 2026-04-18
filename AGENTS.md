# AGENTS.md

Development reference for AI coding agents. See [README.md](README.md) if the task really needs project overview or architecture context.

## Key Architectural Patterns

1. **Pluggable Backend Architecture**: Storage backends implement the `BackendStore` interface — new backends require no core changes. Active backends: RocksDB (default/embedded), HStore (distributed)
2. **gRPC Communication**: All distributed components (PD, Store, Server) communicate via gRPC. Proto definitions in `*/grpc/` directories
3. **Multi-Language Queries**: Native Gremlin support + OpenCypher implementation in `hugegraph-api/opencypher/`

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

TinkerPop tests run only on `release-*`/`test-*` branches. Raft tests run only on `test*`/`raft*` branches.

#### PD & Store Tests

```bash
# Build dependency first
mvn install -pl hugegraph-struct -am -DskipTests

mvn test -pl hugegraph-pd/hg-pd-test -am
mvn test -pl hugegraph-store/hg-store-test -am
```

## Code Quality

```bash
mvn editorconfig:format     # Apply code style (EditorConfig)
mvn clean compile -Dmaven.javadoc.skip=true  # Compile with warnings
```

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

When adding dependencies (Apache compliance):
1. Add license files to `install-dist/release-docs/licenses/`
2. Declare dependency in `install-dist/release-docs/LICENSE`
3. Append NOTICE info to `install-dist/release-docs/NOTICE` (if upstream has NOTICE)
4. Update `install-dist/scripts/dependency/known-dependencies.txt` (run `regenerate_known_dependencies.sh`)

### gRPC Protocol Changes

When modifying `.proto` files:
- Run `mvn clean compile` to regenerate gRPC stubs
- Generated files go to `target/generated-sources/protobuf/` (excluded from Apache RAT)

### Build Order & Cross-Module Dependencies

For distributed development, build in this order:

```bash
mvn install -pl hugegraph-struct -am -DskipTests       # 1. Shared data structures
mvn clean package -pl hugegraph-pd -am -DskipTests      # 2. Placement Driver
mvn clean package -pl hugegraph-store -am -DskipTests    # 3. Distributed storage
mvn clean package -pl hugegraph-server -am -DskipTests   # 4. Server
```

Key dependencies: `hugegraph-commons` is shared by all modules. `hugegraph-struct` must be built before PD and Store. Server backends depend on `hugegraph-core`.

