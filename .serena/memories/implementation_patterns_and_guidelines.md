# Implementation Patterns and Guidelines

## Backend Development

### Backend Architecture Pattern
- All backends extend abstractions from `hugegraph-server/hugegraph-core`
- Implement the `BackendStore` interface
- Each backend is a separate Maven module under `hugegraph-server/`
- Backend selection configured in `hugegraph.properties` via `backend` property

### Available Backends
- **RocksDB** (default, embedded): `hugegraph-rocksdb`
- **HStore** (distributed, production): `hugegraph-hstore`
- **Legacy** (≤1.5.0): MySQL, PostgreSQL, Cassandra, ScyllaDB, HBase, Palo

### Backend Testing Profiles
- `memory`: In-memory backend for fast unit tests
- `rocksdb`: RocksDB for realistic local tests
- `hbase`: HBase for distributed scenarios
- `hstore`: HStore for production-like distributed tests

## gRPC Protocol Development

### Protocol Buffer Definitions
- PD protos: `hugegraph-pd/hg-pd-grpc/src/main/proto/`
- Store protos: `hugegraph-store/hg-store-grpc/src/main/proto/`

### Code Generation
When modifying `.proto` files:
1. Run `mvn clean compile` to regenerate gRPC stubs
2. Generated Java code goes to `*/grpc/` packages
3. Output location: `target/generated-sources/protobuf/`
4. Generated files excluded from Apache RAT checks
5. All inter-service communication uses gRPC

## Authentication System

### Default State
- Authentication **disabled by default**
- Enable via `bin/enable-auth.sh` or configuration
- **Required for production deployments**

### Implementation Location
`hugegraph-server/hugegraph-api/src/main/java/org/apache/hugegraph/api/auth/`

### Multi-Level Security Model
- Users, Groups, Projects, Targets, Access control

## TinkerPop Integration

### Compliance
- Full Apache TinkerPop 3 implementation
- Custom optimization strategies
- Supports both Gremlin and OpenCypher query languages

### Query Language Support
- **Gremlin**: Native via TinkerPop integration
- **OpenCypher**: Implementation in `hugegraph-api/opencypher/`

## Testing Patterns

### Test Suite Organization
- **UnitTestSuite**: Pure unit tests, no external dependencies
- **CoreTestSuite**: Core functionality tests with backend
- **ApiTestSuite**: REST API integration tests
- **StructureStandardTest**: TinkerPop structure compliance
- **ProcessStandardTest**: TinkerPop process compliance

### Backend Selection in Tests
Use Maven profiles:
```bash
-P core-test,memory      # Fast in-memory
-P core-test,rocksdb     # Persistent local
-P api-test,rocksdb      # API with persistent backend
```

## Distribution and Packaging

### Creating Distribution
```bash
mvn clean package -DskipTests
```
Output: `install-dist/target/hugegraph-<version>.tar.gz`

## Code Organization

### Package Structure
```
org.apache.hugegraph
├── backend/        # Backend implementations
├── api/           # REST API endpoints
├── core/          # Core graph engine
├── schema/        # Schema definitions
├── traversal/     # Traversal and query processing
├── task/          # Background tasks
├── auth/          # Authentication/authorization
└── util/          # Utilities
```

### Module Dependencies
- Commons is shared by all modules
- Struct must be built before PD and Store
- Backend modules depend on core
- Test module depends on all server modules
