# AGENTS.md

This file provides guidance to an AI coding tool when working with code in this repository.

## Project Overview

HugeGraph PD (Placement Driver) is a meta server for distributed HugeGraph deployments, responsible for:
- Service discovery and registration
- Partition information storage and management
- Store node monitoring and scheduling
- Metadata coordination using Raft consensus

**Technology Stack**:
- Java 11+ (required)
- Apache Maven 3.5+
- gRPC + Protocol Buffers for RPC communication
- JRaft (Ant Design's Raft implementation) for consensus
- RocksDB for metadata persistence
- Spring Boot for REST APIs and dependency injection

## Module Architecture

HugeGraph PD is a Maven multi-module project with 8 modules:

```
hugegraph-pd/
├── hg-pd-grpc       # Protocol Buffers definitions and generated stubs
├── hg-pd-common     # Shared utilities and common interfaces
├── hg-pd-core       # Core PD logic (Raft, metadata, services)
├── hg-pd-service    # gRPC service implementations and REST APIs
├── hg-pd-client     # Java client library for PD
├── hg-pd-cli        # Command-line interface tools
├── hg-pd-test       # Integration and unit tests
└── hg-pd-dist       # Distribution assembly (bin scripts, configs)
```

### Module Dependency Flow

```
hg-pd-grpc (protos)
    ↓
hg-pd-common (utilities)
    ↓
hg-pd-core (Raft + metadata stores)
    ↓
hg-pd-service (gRPC + REST endpoints)
    ↓
hg-pd-dist (assembly)

hg-pd-client (depends on hg-pd-grpc, hg-pd-common)
hg-pd-cli (depends on hg-pd-client)
hg-pd-test (depends on hg-pd-core, hg-pd-service)
```

### Core Components (hg-pd-core)

**Metadata Stores** (`meta/` package):
- `MetadataRocksDBStore`: RocksDB-backed persistence layer
- `PartitionMeta`: Partition assignment and shard group management
- `StoreInfoMeta`: Store node information and health status
- `TaskInfoMeta`: Distributed task coordination
- `IdMetaStore`, `ConfigMetaStore`, `DiscoveryMetaStore`: Domain-specific metadata

**Services** (root package):
- `PartitionService`: Partition allocation, balancing, and splitting
- `StoreNodeService`: Store registration, heartbeat processing, status monitoring
- `StoreMonitorDataService`: Metrics collection and time-series data storage
- `TaskScheduleService`: Automated partition patrol and rebalancing
- `KvService`, `IdService`, `ConfigService`, `LogService`: Utility services

**Raft Layer** (`raft/` package):
- `RaftEngine`: Raft group management and leadership
- `RaftStateMachine`: State machine applying metadata operations
- `RaftTaskHandler`: Async task execution via Raft proposals
- `KVOperation`, `KVStoreClosure`: Raft operation abstractions

**Service Layer** (hg-pd-service):
- `ServiceGrpc`: Main gRPC service endpoint (partition, store, discovery RPCs)
- `PartitionAPI`: REST API for partition management
- `PDPulseService`: Heartbeat and monitoring
- `DiscoveryService`: Service discovery and registration

### gRPC Protocol Definitions

Located in `hg-pd-grpc/src/main/proto/`:
- `pdpb.proto`: Main PD service (GetMembers, RegisterStore, GetPartition)
- `metapb.proto`: Metadata objects (Partition, Shard, Store, Graph)
- `meta.proto`: Store and partition metadata
- `discovery.proto`: Service discovery protocol
- `kv.proto`: Key-value operations
- `pd_pulse.proto`: Heartbeat and monitoring
- `pd_watch.proto`: Watch notifications
- `metaTask.proto`: Task coordination

**Important**: Generated Java code from `.proto` files is excluded from source control (`.gitignore`) and Apache RAT checks (Jacoco config). Regenerate after proto changes.

## Build & Development Commands

### Building PD Module

```bash
# From hugegraph root directory, build PD and dependencies
mvn clean package -pl hugegraph-pd -am -DskipTests

# From hugegraph-pd directory, build all modules
mvn clean install -DskipTests

# Build with tests
mvn clean install

# Build distribution package only
mvn clean package -pl hg-pd-dist -am -DskipTests
# Output: hugegraph-pd/apache-hugegraph-pd-<version>.tar.gz
```

### Running Tests

PD tests use Maven profiles defined in `pom.xml`:

```bash
# All tests (default profiles active)
mvn test

# Specific test profile
mvn test -P pd-core-test
mvn test -P pd-common-test
mvn test -P pd-client-test
mvn test -P pd-rest-test

# Single test class (from hugegraph-pd directory)
mvn test -pl hg-pd-test -am -Dtest=StoreNodeServiceTest
mvn test -pl hg-pd-test -am -Dtest=PartitionServiceTest

# From hugegraph root directory
mvn test -pl hugegraph-pd/hg-pd-test -am
```

Test files are located in `hg-pd-test/src/main/java/` (note: not `src/test/java`).

### Regenerating gRPC Stubs

```bash
# After modifying .proto files
mvn clean compile

# Generated files location:
# target/generated-sources/protobuf/java/
# target/generated-sources/protobuf/grpc-java/
```

### Code Quality

```bash
# License header check (Apache RAT)
mvn apache-rat:check

# Clean build artifacts
mvn clean
# This also removes: *.tar, *.tar.gz, .flattened-pom.xml
```

## Running HugeGraph PD

### Distribution Structure

After building, extract the tarball:
```
apache-hugegraph-pd-<version>/
├── bin/
│   ├── start-hugegraph-pd.sh    # Start PD server
│   ├── stop-hugegraph-pd.sh     # Stop PD server
│   └── util.sh                  # Utility functions
├── conf/
│   ├── application.yml          # Main configuration
│   ├── application.yml.template # Configuration template
│   ├── log4j2.xml              # Logging configuration
│   └── verify-license.json     # License verification (optional)
├── lib/                        # JAR dependencies
├── logs/                       # Runtime logs
└── pd_data/                    # RocksDB metadata storage (created at runtime)
```

### Starting PD

```bash
cd apache-hugegraph-pd-<version>/
bin/start-hugegraph-pd.sh

# With custom GC options
bin/start-hugegraph-pd.sh -g g1

# With custom JVM options
bin/start-hugegraph-pd.sh -j "-Xmx8g -Xms4g"

# With OpenTelemetry enabled
bin/start-hugegraph-pd.sh -y true
```

Default ports:
- gRPC: 8686 (configure in `application.yml`: `grpc.port`)
- REST API: 8620 (configure in `application.yml`: `server.port`)
- Raft: 8610 (configure in `application.yml`: `raft.address`)

JVM memory defaults (in `start-hugegraph-pd.sh`):
- Max heap: 32 GB
- Min heap: 512 MB

### Stopping PD

```bash
bin/stop-hugegraph-pd.sh
```

This sends SIGTERM to the PD process (tracked in `bin/pid`).

## Key Configuration (application.yml)

### Critical Settings for Distributed Deployment

```yaml
grpc:
  host: 127.0.0.1        # MUST change to actual IPv4 address in production
  port: 8686

raft:
  address: 127.0.0.1:8610         # This node's Raft address
  peers-list: 127.0.0.1:8610      # Comma-separated list of all PD peers
                                   # Example: 192.168.1.1:8610,192.168.1.2:8610,192.168.1.3:8610

pd:
  data-path: ./pd_data             # RocksDB metadata storage path
  initial-store-count: 1           # Min stores required for cluster availability
  initial-store-list: 127.0.0.1:8500  # Auto-activated store nodes (grpc_ip:grpc_port)
  patrol-interval: 1800            # Partition rebalancing interval (seconds)

partition:
  default-shard-count: 1           # Replicas per partition (typically 3 in production)
  store-max-shard-count: 12        # Max partitions per store

store:
  max-down-time: 172800            # Seconds before store is permanently offline (48h)
  monitor_data_enabled: true       # Enable metrics collection
  monitor_data_interval: 1 minute  # Metrics collection interval
  monitor_data_retention: 1 day    # Metrics retention period
```

### Common Configuration Errors

1. **Raft peer discovery failure**: `raft.peers-list` must include all PD nodes' `raft.address` values
2. **Store connection issues**: `grpc.host` must be a reachable IP (not `127.0.0.1`) for distributed deployments
3. **Split-brain scenarios**: Always run 3 or 5 PD nodes in production for Raft quorum
4. **Partition imbalance**: Adjust `patrol-interval` for faster/slower rebalancing

## Development Workflows

### Adding a New gRPC Service

1. Define `.proto` messages and service in `hg-pd-grpc/src/main/proto/`
2. Run `mvn compile` to generate Java stubs
3. Implement service in `hg-pd-service/src/main/java/.../service/`
4. Register service in gRPC server initialization (check existing `ServiceGrpc.java` pattern)
5. Add client methods in `hg-pd-client/` if needed

### Adding a New Metadata Store

1. Create meta class in `hg-pd-core/src/main/java/.../meta/`
2. Use `MetadataRocksDBStore` as the underlying persistence layer
3. Implement metadata operations as Raft proposals via `RaftTaskHandler`
4. Add corresponding service methods in `hg-pd-core/.../Service.java`
5. Expose via gRPC in `hg-pd-service/`

### Modifying Partition Logic

- Core partition logic: `hg-pd-core/.../PartitionService.java` (69KB file, 2000+ lines)
- Key methods:
  - `splitPartition()`: Partition splitting logic
  - `updatePartitionLeader()`: Leader election handling
  - `balancePartitions()`: Auto-balancing algorithm
  - `getPartitionByCode()`: Partition routing
- All partition changes must go through Raft consensus
- Test with `hg-pd-test/.../core/PartitionServiceTest.java`

### Debugging Raft Issues

- Enable Raft logging in `conf/log4j2.xml`: Set `com.alipay.sofa.jraft` to DEBUG
- Check Raft state: Leader election happens in `RaftEngine.java`
- Raft snapshots stored in `pd_data/raft/snapshot/`
- Raft logs stored in `pd_data/raft/log/`

## Testing Strategy

### Test Organization

Tests are in `hg-pd-test/src/main/java/` (non-standard location):
- `BaseTest.java`: Base class with common setup
- `core/`: Core service tests (PartitionService, StoreNodeService, etc.)
- Suite tests: `PDCoreSuiteTest.java` runs all core tests

### Running Integration Tests

```bash
# From hugegraph root, run PD integration tests
mvn test -pl hugegraph-pd/hg-pd-test -am

# These tests start embedded PD instances and verify:
# - Raft consensus and leader election
# - Partition allocation and balancing
# - Store heartbeat and monitoring
# - Metadata persistence and recovery
```

## Docker Deployment

### Building Docker Image

```bash
# From hugegraph root directory
docker build -f hugegraph-pd/Dockerfile -t hugegraph-pd:latest .
```

The Dockerfile uses multi-stage build:
1. Stage 1: Build with Maven
2. Stage 2: Runtime with OpenJDK 11

### Running in Docker

```bash
# Single PD node (development)
docker run -d -p 8620:8620 -p 8686:8686 -p 8610:8610 \
  -v /path/to/pd_data:/hugegraph-pd/pd_data \
  hugegraph-pd:latest

# For production clusters, use Docker Compose or Kubernetes
# See: hugegraph-server/hugegraph-dist/docker/example/
```

Exposed ports: 8620 (REST), 8686 (gRPC), 8610 (Raft)

## Cross-Module Dependencies

When working on PD:
- **hugegraph-struct**: Protocol definitions shared with hugegraph-store
  - Build struct first: `mvn install -pl hugegraph-struct -am -DskipTests`
  - Required before building PD if struct changed
- **hugegraph-commons**: RPC framework, locks, and configuration utilities
  - Changes to commons may affect PD's `hg-pd-common` module

## CI/CD Integration

PD tests run in `pd-store-ci.yml` GitHub Actions workflow:
- Triggered on pushes to PD module files
- Runs `mvn test -pl hugegraph-pd/hg-pd-test -am`
- JaCoCo coverage excludes generated gRPC code and config classes

## Important Notes

### Generated Code Exclusions
- `hg-pd-grpc/src/main/java/` is excluded from git (see `.gitignore`)
- Apache RAT skips `**/grpc/**.*` (see `pom.xml` Jacoco config)
- Always run `mvn clean compile` after pulling proto changes

### Raft Consensus Requirements
- PD uses JRaft for distributed consensus
- All metadata writes are Raft proposals (see `KVOperation`, `KVStoreClosure`)
- Raft group requires 3 or 5 nodes for fault tolerance in production
- Single-node mode (peers-list with one address) is for development only

### Store Interaction
- PD does not store graph data; it only stores metadata about store nodes and partitions
- Actual graph data resides in hugegraph-store nodes
- PD coordinates store nodes but doesn't handle data plane traffic

### Version Compatibility
- PD version must match hugegraph-server and hugegraph-store versions
- Version managed via `${revision}` property (inherited from parent POM)
- Current version: 1.7.0
