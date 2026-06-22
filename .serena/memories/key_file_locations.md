# Key File and Directory Locations

## Project Root
The project root contains the multi-module Maven structure.

## Configuration Files

### Build Configuration
- `pom.xml` - Root Maven POM (multi-module)
- `.editorconfig` - Code style rules
- `style/checkstyle.xml` - Checkstyle rules
- `.licenserc.yaml` - License checker config

### Documentation
- `README.md` - Project overview
- `BUILDING.md` - Build instructions
- `CONTRIBUTING.md` - Contribution guide
- `AGENTS.md` - AI agent development guide
- `LICENSE` - Apache License 2.0
- `NOTICE` - Copyright notices

## Server Module (hugegraph-server)

### Core Implementation
- `hugegraph-core/src/main/java/org/apache/hugegraph/` - Core engine
  - `backend/` - Backend interface
  - `schema/` - Schema management
  - `traversal/` - Query processing
  - `task/` - Background tasks

### API Layer
- `hugegraph-api/src/main/java/org/apache/hugegraph/api/` - REST APIs
  - `graph/` - GraphAPI
  - `schema/` - SchemaAPI
  - `gremlin/` - GremlinAPI
  - `cypher/` - CypherAPI
  - `auth/` - AuthAPI
  - `opencypher/` - OpenCypher implementation

### Backend Implementations
- `hugegraph-rocksdb/` - RocksDB backend (default)
- `hugegraph-hstore/` - HStore distributed backend
- `hugegraph-hbase/` - HBase backend
- `hugegraph-mysql/` - MySQL backend
- `hugegraph-postgresql/` - PostgreSQL backend
- `hugegraph-cassandra/` - Cassandra backend
- `hugegraph-scylladb/` - ScyllaDB backend
- `hugegraph-palo/` - Palo backend

### Distribution and Scripts
- `hugegraph-dist/src/assembly/static/` - Distribution files
  - `bin/` - Shell scripts (init-store.sh, start-hugegraph.sh, stop-hugegraph.sh, etc.)
  - `conf/` - Configuration files (hugegraph.properties, rest-server.properties, gremlin-server.yaml, log4j2.xml)
  - `lib/` - JAR dependencies
  - `logs/` - Log files

### Testing
- `hugegraph-test/src/main/java/org/apache/hugegraph/` - Test suites
  - `unit/` - Unit tests
  - `core/` - Core functionality tests
  - `api/` - API integration tests
  - `tinkerpop/` - TinkerPop compliance tests

## PD Module (hugegraph-pd)
- `hg-pd-core/` - Core PD logic
- `hg-pd-service/` - Service implementation
- `hg-pd-client/` - Client library
- `hg-pd-grpc/src/main/proto/` - Protocol definitions
- `hg-pd-dist/src/assembly/static/` - Distribution files

## Store Module (hugegraph-store)
- `hg-store-core/` - Core storage logic
- `hg-store-node/` - Storage node
- `hg-store-client/` - Client library
- `hg-store-grpc/src/main/proto/` - Protocol definitions
- `hg-store-dist/src/assembly/static/` - Distribution files

## Commons Module (hugegraph-commons)
- Shared utilities, RPC framework

## Struct Module (hugegraph-struct)
- Data structure definitions (must be built before PD and Store)

## Distribution Module (install-dist)
- `release-docs/` - LICENSE, NOTICE, licenses/
- `scripts/dependency/` - Dependency management scripts
- `target/` - Build output (hugegraph-<version>.tar.gz)
