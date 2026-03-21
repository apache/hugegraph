# AGENTS.md

This file provides guidance to an AI coding tool when working with code in this repository.

## Project Overview

Apache HugeGraph is a fast-speed and highly-scalable graph database that supports billions of vertices and edges. It is compliant with Apache TinkerPop 3 and supports both Gremlin and Cypher query languages.

**Technology Stack**:
- Java 11+ (required)
- Apache Maven 3.5+
- Apache TinkerPop 3.5.1
- gRPC for distributed communication
- RocksDB as default storage backend

## Architecture

### Multi-Module Structure

This is a Maven multi-module project with 7 main modules:

1. **hugegraph-server**: Core graph engine, REST APIs, and backend implementations (13 submodules)
2. **hugegraph-pd**: Placement Driver (meta server) for distributed deployments (8 submodules)
3. **hugegraph-store**: Distributed storage backend with RocksDB and Raft (9 submodules)
4. **hugegraph-commons**: Shared utilities (locks, configs, RPC framework)
5. **hugegraph-struct**: Data structure definitions
6. **install-dist**: Distribution packaging
7. **hugegraph-cluster-test**: Cluster integration tests

### Three-Tier Architecture

```bash
Client Layer (Gremlin/Cypher queries, REST APIs)
       ↓
Server Layer (hugegraph-server)
  ├─ REST API Layer (hugegraph-api): GraphAPI, SchemaAPI, GremlinAPI, CypherAPI, AuthAPI
  ├─ Graph Engine Layer (hugegraph-core): Schema management, traversal optimization, task scheduling
  └─ Backend Interface: Abstraction over storage backends
       ↓
Storage Layer (pluggable backends)
  ├─ RocksDB (default, embedded)
  ├─ HStore (distributed, production)
  └─ Legacy: MySQL, PostgreSQL, Cassandra, ScyllaDB, HBase, Palo
```

### Distributed Components (Optional)

For production distributed deployments:
- **hugegraph-pd**: Service discovery, partition management, metadata coordination
- **hugegraph-store**: Distributed storage with Raft consensus (typically 3+ nodes)
- **hugegraph-server**: Multiple server instances (typically 3+)

All inter-service communication uses gRPC with Protocol Buffers.

### Key Architectural Patterns

1. **Pluggable Backend Architecture**: Storage backends implement the `BackendStore` interface, allowing new backends without modifying core code
2. **TinkerPop Compliance**: Full Apache TinkerPop 3 implementation with custom optimization strategies
3. **gRPC Communication**: All distributed components communicate via gRPC (proto definitions in `*/grpc/` directories)
4. **Multi-Language Queries**: Native Gremlin support + OpenCypher implementation in `hugegraph-api/opencypher`

## Build & Development Commands

### Prerequisites Check
```bash
# Verify Java version (11+ required)
java -version

# Verify Maven version (3.5+ required)
mvn -version
```

### Full Build
```bash
# Clean build with all modules
mvn clean install -DskipTests

# Build with tests
mvn clean install

# Build specific module (e.g., server only)
mvn clean install -pl hugegraph-server -am -DskipTests
```

### Testing

#### Server Module Tests
```bash
# Unit tests (memory backend)
mvn test -pl hugegraph-server/hugegraph-test -am -P unit-test

# Core tests with specific backend
mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,memory
mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,rocksdb
mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,hbase

# API tests with backend
mvn test -pl hugegraph-server/hugegraph-test -am -P api-test,rocksdb

# TinkerPop compliance tests (for release branches)
mvn test -pl hugegraph-server/hugegraph-test -am -P tinkerpop-structure-test,memory
mvn test -pl hugegraph-server/hugegraph-test -am -P tinkerpop-process-test,memory
```

#### PD & Store Module Tests
```bash
# Build and test hugegraph-struct first (dependency)
mvn install -pl hugegraph-struct -am -DskipTests

# Test PD module
mvn test -pl hugegraph-pd/hg-pd-test -am

# Test Store module
mvn test -pl hugegraph-store/hg-store-test -am
```

### Code Quality & Validation

```bash
# License header check (Apache RAT)
mvn apache-rat:check

# Code style check (EditorConfig)
mvn editorconfig:check

# Compile with warnings
mvn clean compile -Dmaven.javadoc.skip=true
```

### Running the Server

Scripts are located in `hugegraph-server/hugegraph-dist/src/assembly/static/bin/`:

```bash
# Initialize storage backend
bin/init-store.sh

# Start server
bin/start-hugegraph.sh

# Stop server
bin/stop-hugegraph.sh

# Gremlin console
bin/gremlin-console.sh

# Enable authentication
bin/enable-auth.sh
```

### Creating Distribution Package

```bash
# Build distribution tarball (auto-enabled by default)
mvn clean package -DskipTests

# Skip assembly creation (if needed)
mvn clean package -DskipTests -Dskip-assembly-hugegraph

# Output: install-dist/target/hugegraph-<version>.tar.gz
```

## Important File Locations

### Configuration Files
- Server configs: `hugegraph-server/hugegraph-dist/src/assembly/static/conf/`
  - `hugegraph.properties` - Main server configuration
  - `rest-server.properties` - REST API settings
  - `gremlin-server.yaml` - Gremlin server configuration
- PD configs: `hugegraph-pd/hg-pd-dist/src/assembly/static/conf/`
- Store configs: `hugegraph-store/hg-store-dist/src/assembly/static/conf/`

### Proto Definitions (gRPC)
- PD protos: `hugegraph-pd/hg-pd-grpc/src/main/proto/`
- Store protos: `hugegraph-store/hg-store-grpc/src/main/proto/`

### Core Implementation Paths
- Graph engine: `hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/`
- REST APIs: `hugegraph-server/hugegraph-api/src/main/java/org/apache/hugegraph/api/`
- Backend implementations: `hugegraph-server/hugegraph-{backend}/` (e.g., `hugegraph-rocksdb`)

## Development Workflow

### Code Style
Configure your IDE to use `.editorconfig` for code style and `style/checkstyle.xml` for Checkstyle rules

### Adding Dependencies

When adding third-party dependencies:
1. Add license files to `install-dist/release-docs/licenses/`
2. Declare dependency in `install-dist/release-docs/LICENSE`
3. Append NOTICE info to `install-dist/release-docs/NOTICE` (if upstream has NOTICE)
4. Update `install-dist/scripts/dependency/known-dependencies.txt` (run `regenerate_known_dependencies.sh`)

### Backend Development

When working on storage backends:
- All backends extend `hugegraph-server/hugegraph-core` abstractions
- Implement the `BackendStore` interface
- Each backend is a separate Maven module in `hugegraph-server/`
- Backend selection is configured in `hugegraph.properties` via the `backend` property

### gRPC Protocol Changes

When modifying `.proto` files:
- Generated Java code goes to `*/grpc/` packages (excluded from Apache RAT checks)
- Run `mvn clean compile` to regenerate gRPC stubs
- Generated files are in `target/generated-sources/protobuf/`

### Authentication System

Authentication is optional and disabled by default:
- Enable via `bin/enable-auth.sh` or configuration
- Auth implementation: `hugegraph-server/hugegraph-api/src/main/java/org/apache/hugegraph/api/auth/`
- Multi-level: Users, Groups, Projects, Targets, Access control
- Required for production deployments

## Common Workflows

### Running a Single Test Class
```bash
# Use Maven's -Dtest parameter
mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,memory -Dtest=YourTestClass
```

### Working with Distributed Components

For distributed development:
1. Build struct module first: `mvn install -pl hugegraph-struct -am -DskipTests`
2. Build PD: `mvn clean package -pl hugegraph-pd -am -DskipTests`
3. Build Store: `mvn clean package -pl hugegraph-store -am -DskipTests`
4. Build Server with HStore backend: `mvn clean package -pl hugegraph-server -am -DskipTests`

See Docker Compose examples: `docker/` directory. Single-node quickstart (pre-built images): `docker/docker-compose.yml`. Single-node dev build (from source): `docker/docker-compose.dev.yml`. 3-node cluster: `docker/docker-compose-3pd-3store-3server.yml`. See `docker/README.md` for full setup guide.

### Debugging Tips

- Enable detailed logging in `hugegraph-server/hugegraph-dist/src/assembly/static/conf/log4j2.xml`
- Use `bin/dump-conf.sh` to view effective configuration
- Arthas diagnostics tool is included (version 3.7.1)
- Monitor with `bin/monitor-hugegraph.sh`

## CI/CD Profiles

The project uses multiple GitHub Actions workflows:
- `server-ci.yml`: Server module tests (memory, rocksdb, hbase backends)
- `pd-store-ci.yml`: PD and Store module tests
- `commons-ci.yml`: Commons module tests
- `cluster-test-ci.yml`: Distributed cluster integration tests
- `licence-checker.yml`: Apache RAT license validation

## Special Notes

### Cross-Module Dependencies
- `hugegraph-commons` is a shared dependency for all modules
- `hugegraph-struct` must be built before PD and Store
- Server backends depend on `hugegraph-core`

### Version Management
- Version is managed via `${revision}` property (currently `1.7.0`)
- Flatten Maven plugin used for CI-friendly versioning
