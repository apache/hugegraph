# HugeGraph Store

[![License](https://img.shields.io/badge/license-Apache%202-0E78BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Version](https://img.shields.io/badge/version-1.7.0-blue)](https://github.com/apache/hugegraph)

> **Note**: From revision 1.5.0, the HugeGraph-Store code has been adapted to this location.

## Overview

HugeGraph Store is a distributed storage backend for HugeGraph that provides high availability, horizontal scalability, and strong consistency for production graph database deployments. Built on RocksDB and Apache JRaft, it serves as the data plane for large-scale graph workloads requiring enterprise-grade reliability.

### Core Capabilities

- **Distributed Storage**: Hash-based partitioning with automatic data distribution across multiple Store nodes
- **High Availability**: Multi-replica data replication using Raft consensus, tolerating node failures without data loss
- **Horizontal Scalability**: Dynamic partition allocation and rebalancing for seamless cluster expansion
- **Query Optimization**: Advanced query pushdown (filter, aggregation, index) and multi-partition parallel execution
- **Metadata Coordination**: Tight integration with HugeGraph PD for cluster management and service discovery
- **High Performance**: gRPC-based communication with streaming support for large result sets

### Technology Stack

- **Storage Engine**: RocksDB 7.7.3 (optimized for graph workloads)
- **Consensus Protocol**: Apache JRaft (Ant Financial's Raft implementation)
- **RPC Framework**: gRPC + Protocol Buffers
- **Deployment**: Java 11+, Docker/Kubernetes support

### When to Use HugeGraph Store

**Use Store for**:
- Production deployments requiring high availability (99.9%+ uptime)
- Workloads exceeding single-node storage capacity (100GB+)
- Multi-tenant or high-concurrency scenarios (1000+ QPS)
- Environments requiring horizontal scalability and fault tolerance

**Use RocksDB Backend for**:
- Development and testing environments
- Single-node deployments with moderate data size (<100GB)
- Embedded scenarios where simplicity is preferred over distribution

---

## Architecture

HugeGraph Store is a Maven multi-module project consisting of 9 modules:

| Module | Description |
|--------|-------------|
| **hg-store-grpc** | gRPC protocol definitions (7 `.proto` files) and generated Java stubs for Store communication |
| **hg-store-common** | Shared utilities, query abstractions, constants, and buffer management |
| **hg-store-rocksdb** | RocksDB abstraction layer with session management and optimized scan iterators |
| **hg-store-core** | Core storage engine: partition management, Raft integration, metadata coordination, business logic |
| **hg-store-client** | Java client library for applications to connect to Store cluster and perform operations |
| **hg-store-node** | Store node server implementation with gRPC services, Raft coordination, and PD integration |
| **hg-store-cli** | Command-line utilities for Store administration and debugging |
| **hg-store-test** | Comprehensive unit and integration tests for all Store components |
| **hg-store-dist** | Distribution assembly: packaging, configuration templates, startup scripts |

### Three-Tier Architecture

```
Client Layer (hugegraph-server)
    ↓ (hg-store-client connects via gRPC)
Store Node Layer (hg-store-node)
    ├─ gRPC Services (Session, Query, State)
    ├─ Partition Engines (each partition = one Raft group)
    └─ PD Integration (heartbeat, partition assignment)
         ↓
Storage Engine Layer (hg-store-core + hg-store-rocksdb)
    ├─ HgStoreEngine (manages all partition engines)
    ├─ PartitionEngine (per-partition Raft state machine)
    └─ RocksDB (persistent storage)
```

### Key Architectural Features

- **Partition-based Distribution**: Data is split into partitions (default: hash-based) and distributed across Store nodes
- **Raft Consensus per Partition**: Each partition is a separate Raft group with 1-3 replicas (typically 3 in production)
- **PD Coordination**: Store nodes register with PD for partition assignment, metadata synchronization, and health monitoring
- **Query Pushdown**: Filters, aggregations, and index scans are pushed to Store nodes for parallel execution

For detailed architecture, Raft consensus mechanisms, and partition management, see [Distributed Architecture](docs/distributed-architecture.md).

---

## Quick Start

### Prerequisites

- **Java**: 11 or higher
- **Maven**: 3.5 or higher
- **HugeGraph PD Cluster**: Store requires a running PD cluster for metadata coordination (see [PD README](../hugegraph-pd/README.md))
- **Disk Space**: At least 10GB per Store node for data and Raft logs
- **Network**: Low-latency network (<5ms) between Store nodes for Raft consensus

### Build

**Important**: Build `hugegraph-struct` first, as it's a required dependency.

From the project root:

```bash
# Build struct module
mvn install -pl hugegraph-struct -am -DskipTests

# Build Store and all dependencies
mvn clean package -pl hugegraph-store/hugegraph-store-dist -am -DskipTests
```

The assembled distribution will be available at:
```
hugegraph-store/apache-hugegraph-store-incubating-1.7.0/lib/hg-store-node-1.7.0.jar```
```

### Configuration

Extract the distribution package and edit `conf/application.yml`:

#### Core Settings

| Parameter | Default | Description |
|-----------|---------|-------------|
| `pdserver.address` | `localhost:8686` | **Required**: PD cluster endpoints (comma-separated, e.g., `192.168.1.10:8686,192.168.1.11:8686`) |
| `grpc.host` | `127.0.0.1` | gRPC server bind address (**use actual IP for production**) |
| `grpc.port` | `8500` | gRPC server port for client connections |
| `raft.address` | `127.0.0.1:8510` | Raft service address for this Store node |
| `raft.snapshotInterval` | `1800` | Raft snapshot interval in seconds (30 minutes) |
| `server.port` | `8520` | REST API port for management and metrics |
| `app.data-path` | `./storage` | Directory for RocksDB data storage (supports multiple paths for multi-disk setups) |
| `app.fake-pd` | `false` | Enable built-in PD mode for standalone testing (not for production) |

#### Single-Node Development Example (with fake-pd)

```yaml
pdserver:
  address: localhost:8686  # Ignored when fake-pd is true

grpc:
  host: 127.0.0.1
  port: 8500

raft:
  address: 127.0.0.1:8510
  snapshotInterval: 1800

server:
  port: 8520

app:
  data-path: ./storage
  fake-pd: true  # Built-in PD mode (development only)
```

#### 3-Node Cluster Example (production)

**Prerequisites**: A running 3-node PD cluster at `192.168.1.10:8686`, `192.168.1.11:8686`, `192.168.1.12:8686`

**Store Node 1** (`192.168.1.20`):
```yaml
pdserver:
  address: 192.168.1.10:8686,192.168.1.11:8686,192.168.1.12:8686

grpc:
  host: 192.168.1.20
  port: 8500

raft:
  address: 192.168.1.20:8510

app:
  data-path: ./storage
  fake-pd: false
```

**Store Node 2** (`192.168.1.21`):
```yaml
pdserver:
  address: 192.168.1.10:8686,192.168.1.11:8686,192.168.1.12:8686

grpc:
  host: 192.168.1.21
  port: 8500

raft:
  address: 192.168.1.21:8510

app:
  data-path: ./storage
  fake-pd: false
```

**Store Node 3** (`192.168.1.22`):
```yaml
pdserver:
  address: 192.168.1.10:8686,192.168.1.11:8686,192.168.1.12:8686

grpc:
  host: 192.168.1.22
  port: 8500

raft:
  address: 192.168.1.22:8510

app:
  data-path: ./storage
  fake-pd: false
```

For detailed configuration options, RocksDB tuning, and deployment topologies, see [Deployment Guide](docs/deployment-guide.md).

### Run

Start the Store server:

```bash
# Replace {version} with your hugegraph version
cd apache-hugegraph-store-incubating-{version}

# Start Store node
bin/start-hugegraph-store.sh

# Stop Store node
bin/stop-hugegraph-store.sh

# Restart Store node
bin/restart-hugegraph-store.sh
```

#### Startup Options

```bash
bin/start-hugegraph-store.sh [-g GC_TYPE] [-j "JVM_OPTIONS"]
```

- `-g`: GC type (`g1` or `ZGC`, default: `g1`)
- `-j`: Custom JVM options (e.g., `-j "-Xmx16g -Xms8g"`)

Default JVM memory settings (defined in `start-hugegraph-store.sh`):
- Max heap: 32GB
- Min heap: 512MB

### Verify Deployment

Check if Store is running and registered with PD:

```bash
# Check process
ps aux | grep hugegraph-store

# Test gRPC endpoint (requires grpcurl)
grpcurl -plaintext localhost:8500 list

# Check REST API health
curl http://localhost:8520/v1/health

# Check logs
tail -f logs/hugegraph-store.log

# Verify registration with PD (from PD node)
curl http://localhost:8620/v1/stores
```

For production deployment, see [Deployment Guide](docs/deployment-guide.md) and [Best Practices](docs/best-practices.md).

---

## Integration with HugeGraph Server

HugeGraph Store serves as a pluggable backend for HugeGraph Server. To use Store as the backend:

### 1. Configure HugeGraph Server Backend

Edit `hugegraph-server/conf/graphs/<graph-name>.properties`:

```properties
# Backend configuration
backend=hstore
serializer=binary

# Store connection (PD addresses)
store.provider=org.apache.hugegraph.backend.store.hstore.HstoreProvider
store.pd_peers=192.168.1.10:8686,192.168.1.11:8686,192.168.1.12:8686

# Connection pool settings
store.max_sessions=4
store.session_timeout=30000
```

### 2. Start HugeGraph Server

Ensure PD and Store clusters are running, then start HugeGraph Server:

```bash
cd hugegraph-server
bin/init-store.sh  # Initialize schema
bin/start-hugegraph.sh
```

### 3. Verify Backend

```bash
# Check backend via REST API
curl --location --request GET 'http://localhost:8080/metrics/backend' \
--header 'Authorization: Bearer <YOUR_ACCESS_TOKEN>'
# Response should show:
# {"backend": "hstore", "nodes": [...]}
```

---

## Testing

Run Store tests:

```bash
# All tests (from hugegraph root)
mvn test -pl hugegraph-store/hg-store-test -am

# Specific test module
mvn test -pl hugegraph-store/hg-store-test -am -Dtest=HgStoreEngineTest

# From hugegraph-store directory
cd hugegraph-store
mvn test
```

### Test Profiles

Store tests are organized into 6 profiles (all active by default):

- `store-client-test`: Client library tests
- `store-core-test`: Core storage and partition management tests
- `store-common-test`: Common utilities and query abstraction tests
- `store-rocksdb-test`: RocksDB abstraction layer tests
- `store-server-test`: Store node server and gRPC service tests
- `store-raftcore-test`: Raft consensus integration tests

For development workflows and debugging, see [Development Guide](docs/development-guide.md).

---

## Docker

### Build Docker Image

From the project root:

```bash
docker build -f hugegraph-store/Dockerfile -t hugegraph-store:latest .
```

### Run Container

```bash
docker run -d \
  -p 8520:8520 \
  -p 8500:8500 \
  -p 8510:8510 \
  -v /path/to/conf:/hugegraph-store/conf \
  -v /path/to/storage:/hugegraph-store/storage \
  -e PD_ADDRESS=192.168.1.10:8686,192.168.1.11:8686 \
  --name hugegraph-store \
  hugegraph-store:latest
```

**Exposed Ports**:
- `8520`: REST API (management, metrics)
- `8500`: gRPC (client connections)
- `8510`: Raft consensus

### Docker Compose Example

For a complete HugeGraph distributed deployment (PD + Store + Server), see:

```
hugegraph-server/hugegraph-dist/docker/example/
```

For Docker and Kubernetes deployment details, see [Deployment Guide](docs/deployment-guide.md).

---

## Documentation

Comprehensive documentation for HugeGraph Store:

| Documentation | Description |
|---------------|-------------|
| [Distributed Architecture](docs/distributed-architecture.md) | Deep dive into three-tier architecture, Raft consensus, partition management, and PD coordination |
| [Deployment Guide](docs/deployment-guide.md) | Production deployment topologies, configuration reference, Docker/Kubernetes setup |
| [Integration Guide](docs/integration-guide.md) | Integrating Store with HugeGraph Server, client API usage, migrating from other backends |
| [Query Engine](docs/query-engine.md) | Query pushdown mechanisms, multi-partition queries, gRPC API reference |
| [Operations Guide](docs/operations-guide.md) | Monitoring and metrics, troubleshooting common issues, backup and recovery, rolling upgrades |
| [Best Practices](docs/best-practices.md) | Hardware sizing, performance tuning, security configuration, high availability design |
| [Development Guide](docs/development-guide.md) | Development environment setup, module architecture, testing strategies, contribution workflow |

---

## Production Deployment Notes

### Cluster Topology

**Minimum Cluster** (development/testing):
- 3 PD nodes
- 3 Store nodes
- 1-3 Server nodes

**Recommended Production Cluster**:
- 3-5 PD nodes (odd number for Raft quorum)
- 6-12 Store nodes (depends on data size and throughput)
- 3-6 Server nodes (depends on query load)

**Large-Scale Cluster**:
- 5 PD nodes
- 12+ Store nodes (horizontal scaling)
- 6+ Server nodes (load balancing)

### High Availability

- Store uses Raft consensus for leader election and data replication
- Each partition has 1-3 replicas (default: 3 in production)
- Cluster can tolerate up to `(N-1)/2` Store node failures per partition (e.g., 1 failure in 3-replica setup)
- Automatic failover and leader re-election (typically <10 seconds)
- PD provides cluster-wide coordination and metadata consistency

### Partition Strategy

- **Default Partitioning**: Hash-based (configurable in PD)
- **Partition Count**: Recommended 3-5x the number of Store nodes for balanced distribution
- **Replica Count**: 3 replicas per partition for production (configurable)
- **Rebalancing**: Automatic partition rebalancing triggered by PD patrol (default: 30 minutes interval)

### Network Requirements

- **Latency**: <5ms between Store nodes for Raft consensus performance
- **Bandwidth**: 1Gbps+ recommended for data replication and query traffic
- **Ports**: Ensure firewall allows traffic on 8500 (gRPC), 8510 (Raft), 8520 (REST)
- **Topology**: Consider rack-aware or availability-zone-aware placement for fault isolation

### Monitoring

Store exposes metrics via:
- **REST API**: `http://<store-host>:8520/actuator/metrics`
- **Health Check**: `http://<store-host>:8520/actuator/health`
- **Prometheus Integration**: Metrics exported in Prometheus format

**Key Metrics to Monitor**:
- Raft leader election count and duration
- Partition count and distribution
- RocksDB read/write latency and throughput
- gRPC request QPS and error rate
- Disk usage and I/O metrics

For detailed operational guidance, see [Operations Guide](docs/operations-guide.md) and [Best Practices](docs/best-practices.md).

---

## Community

- **Website**: https://hugegraph.apache.org
- **Documentation**: https://hugegraph.apache.org/docs/
- **GitHub**: https://github.com/apache/hugegraph
- **Mailing List**: dev@hugegraph.apache.org
- **Issue Tracker**: https://github.com/apache/hugegraph/issues

## Contributing

Contributions are welcome! Please read our [Development Guide](docs/development-guide.md) and follow the Apache HugeGraph contribution guidelines.

For development workflows, code structure, and testing strategies, see the [Development Guide](docs/development-guide.md).

## License

HugeGraph Store is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).

---

**Status**: BETA (from v1.5.0+)

HugeGraph Store is under active development. While suitable for production use, APIs and configurations may evolve. Please report issues via GitHub or the mailing list.
