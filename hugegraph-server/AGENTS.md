# AGENTS.md

This file provides guidance to an AI coding tool when working with code in this repository.

## Project Overview

HugeGraph Server is the graph engine layer of Apache HugeGraph, consisting of:
- **REST API Layer** (hugegraph-api): RESTful APIs for graph operations, Gremlin/Cypher queries, schema management, and authentication
- **Graph Engine Layer** (hugegraph-core): TinkerPop 3 implementation, schema management, traversal optimization, task scheduling
- **Backend Interface**: Abstraction layer for pluggable storage backends
- **Storage Backend Implementations**: RocksDB (default), HStore (distributed), HBase (deprecated; planned for removal in 2.0), and Memory (test-only)

Technology: Java 11+, Maven 3.5+, Apache TinkerPop 3.5.1, Jersey 3.0 (REST), gRPC (distributed communication)

## Build Commands

### Full Build
```bash
# Build all modules (from hugegraph-server directory)
mvn clean install -DskipTests

# Build with tests
mvn clean install

# Build specific module
mvn clean install -pl hugegraph-core -am -DskipTests
```

### Code Quality
```bash
# Run checkstyle validation
mvn checkstyle:check

# Checkstyle runs automatically during 'validate' phase
# Configuration: ../style/checkstyle.xml
```

## Testing

### Running Tests

**Test profiles** (`-P` flag):
- `core-test` (default): Core graph engine tests
- `unit-test`: Unit tests only (memory backend)
- `api-test`: REST API tests
- `tinkerpop-structure-test`: TinkerPop structure compliance
- `tinkerpop-process-test`: TinkerPop process compliance

**Backend profiles** (combine with test profiles):
- `memory`: In-memory backend (default for tests)
- `rocksdb`: RocksDB backend
- `hbase`: HBase backend (deprecated; planned for removal in 2.0)
- `hstore`: HStore backend

```bash
# Unit tests (from hugegraph-server/)
mvn test -pl hugegraph-test -am -P unit-test

# Core tests with RocksDB backend
mvn test -pl hugegraph-test -am -P core-test,rocksdb

# API tests with memory backend
mvn test -pl hugegraph-test -am -P api-test,memory

# Run specific test class
mvn test -pl hugegraph-test -am -P core-test,memory -Dtest=YourTestClassName

# TinkerPop compliance tests (for release validation)
mvn test -pl hugegraph-test -am -P tinkerpop-structure-test,memory
mvn test -pl hugegraph-test -am -P tinkerpop-process-test,memory
```

### Test Module Structure
All tests are in `hugegraph-test/` which depends on all other modules. Tests are organized by:
- Unit tests: `src/test/java/.../unit/`
- Core tests: `src/test/java/.../core/`
- API tests: `src/test/java/.../api/`

## Module Structure

Multi-module Maven project with 8 submodules:

```
hugegraph-server/
├── hugegraph-core          # Graph engine, TinkerPop impl, schema, backend interface
├── hugegraph-api           # REST API, Gremlin/Cypher endpoints, authentication
├── hugegraph-rocksdb       # RocksDB backend (default, embedded)
├── hugegraph-hstore        # HStore backend (distributed, production)
├── hugegraph-hbase         # HBase backend (deprecated; planned for removal in 2.0)
├── hugegraph-dist          # Distribution packaging, scripts, configs
├── hugegraph-test          # All test suites
└── hugegraph-example       # Example code
```

### Key Package Structure (hugegraph-core)

```
org/apache/hugegraph/
├── HugeGraph.java              # Main graph interface
├── StandardHugeGraph.java      # Core implementation
├── HugeFactory.java            # Factory for graph instances
├── backend/                    # Backend abstraction and implementations
│   ├── store/BackendStore.java # Storage backend interface
│   ├── serializer/             # Data serialization
│   └── tx/                     # Transaction management
├── schema/                     # Schema management (VertexLabel, EdgeLabel, etc.)
├── structure/                  # Graph elements (Vertex, Edge, Property)
├── traversal/                  # Gremlin traversal optimization
├── task/                       # Async task scheduling and execution
├── auth/                       # Authentication and authorization
├── job/                        # Job management (rebuild, compact, etc.)
└── meta/                       # Metadata management
```

### Key Package Structure (hugegraph-api)

```
org/apache/hugegraph/
├── api/                        # REST API endpoints
│   ├── graph/GraphAPI.java     # Graph CRUD operations
│   ├── schema/                 # Schema APIs
│   ├── gremlin/GremlinAPI.java # Gremlin query endpoint
│   ├── cypher/CypherAPI.java   # Cypher query endpoint (via OpenCypher)
│   ├── auth/                   # Authentication APIs
│   └── job/TaskAPI.java        # Job/Task management APIs
├── server/RestServer.java      # Jersey/Grizzly REST server
├── auth/                       # Auth filters and handlers
└── metrics/                    # Metrics collection
```

## Architecture Patterns

### Pluggable Backend Architecture
All storage backends implement the `BackendStore` interface from hugegraph-core. New backends can be added as separate modules without modifying core code. Backend selection is via `backend` property in `conf/hugegraph.properties`.

### TinkerPop Compliance
Full Apache TinkerPop 3 implementation with:
- Custom traversal strategies in `hugegraph-core/src/main/java/org/apache/hugegraph/traversal/`
- Structure and process API implementations
- Gremlin groovy script support via `gremlin-groovy` integration

### Multi-Language Query Support
- **Gremlin**: Native support via TinkerPop
- **Cypher**: OpenCypher implementation in `hugegraph-api/src/main/java/org/apache/hugegraph/opencypher/`
  - Translates Cypher to Gremlin using `opencypher-gremlin` library

### gRPC for Distributed Components
When working with HStore backend or distributed features:
- Protocol Buffer definitions: `hugegraph-core/src/main/resources/proto/`
- Generated code: `target/generated-sources/protobuf/java/`
- Regenerate after .proto changes: `mvn clean compile`

### Authentication System
Authentication is **optional and disabled by default**:
- Enable: `bin/enable-auth.sh` or via configuration
- Implementation: `hugegraph-api/src/main/java/org/apache/hugegraph/auth/`
- Multi-level access control: Users, Groups, Projects, Targets, Permissions
- Core auth logic: `hugegraph-core/src/main/java/org/apache/hugegraph/auth/`

## Running the Server

After building, server distribution is in `hugegraph-dist/target/`. Scripts are in `hugegraph-dist/src/assembly/static/bin/`:

```bash
# Initialize backend storage (first time only)
bin/init-store.sh

# Start server (REST API on 8080, Gremlin on 8182)
bin/start-hugegraph.sh

# Stop server
bin/stop-hugegraph.sh

# Gremlin console (interactive)
bin/gremlin-console.sh

# Monitor server status
bin/monitor-hugegraph.sh

# Enable authentication
bin/enable-auth.sh
```

## Configuration Files

Located in `hugegraph-dist/src/assembly/static/conf/`:
- **`hugegraph.properties`**: Main server configuration (backend, storage paths, cache)
- **`rest-server.properties`**: REST API settings (host, port, thread pool, SSL)
- **`gremlin-server.yaml`**: Gremlin server configuration (WebSocket, serializers)
- **`log4j2.xml`**: Logging configuration

## Common Development Tasks

### Adding a New REST API Endpoint
1. Create API class in `hugegraph-api/src/main/java/org/apache/hugegraph/api/`
2. Extend `ApiBase` or relevant base class
3. Use JAX-RS annotations (`@Path`, `@GET`, `@POST`, etc.)
4. Add Swagger/OpenAPI annotations for documentation
5. Add tests in `hugegraph-test/src/test/java/.../api/`

### Adding Backend Support
1. Create new module: `hugegraph-{backend-name}/`
2. Add dependency on `hugegraph-core`
3. Implement `BackendStore` interface
4. Implement `BackendStoreProvider` for factory
5. Add backend module to parent `pom.xml` `<modules>` section
6. Add tests combining with test profiles

### Modifying Schema or Graph Elements
- Schema definitions: `hugegraph-core/src/main/java/org/apache/hugegraph/schema/`
- Graph structure: `hugegraph-core/src/main/java/org/apache/hugegraph/structure/`
- Always consider backward compatibility for stored data
- Update serializers if changing storage format

### Working with Transactions
Transaction management is in `hugegraph-core/src/main/java/org/apache/hugegraph/backend/tx/`:
- `GraphTransaction`: Vertex/Edge operations
- `SchemaTransaction`: Schema operations
- `IndexTransaction`: Index operations
- Follow the transaction lifecycle pattern in existing code

## Important Notes

### Code Style
- Checkstyle configuration: `../style/checkstyle.xml`
- Enforced during Maven `validate` phase
- Import code style into IDE (IntelliJ IDEA recommended)

### License Headers
All Java files must have Apache License header. Verified via `maven-checkstyle-plugin`.

### Version Management
- Version managed via `${revision}` property (currently 1.7.0)
- Uses `flatten-maven-plugin` for CI-friendly versioning
- Don't hardcode versions in module POMs

### Cross-Module Dependencies
```
hugegraph-api → hugegraph-core → hugegraph-commons (external)
hugegraph-{backend} → hugegraph-core
hugegraph-test → all modules
```

### Generated Code
- Protobuf Java classes: Generated, not manually edited
- Located in `target/generated-sources/`
- Excluded from checkstyle/license checks

### Backend Selection at Runtime
Backends are loaded via ServiceLoader pattern. The `backend` property in `hugegraph.properties` determines which implementation is used. All backend JARs must be on classpath.

## Debugging Tips

- Detailed logging: Edit `hugegraph-dist/src/assembly/static/conf/log4j2.xml`
- View effective config: `bin/dump-conf.sh`
- Arthas diagnostics: Built-in (version 3.7.1)
- Backend state inspection: `bin/dump-store.sh`
- Raft cluster tools (for distributed): `bin/raft-tools.sh`
