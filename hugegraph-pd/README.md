# HugeGraph PD

[![License](https://img.shields.io/badge/license-Apache%202-0E78BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Version](https://img.shields.io/badge/version-1.7.0-blue)](https://github.com/apache/hugegraph)

## Overview

HugeGraph PD (Placement Driver) is a meta server that provides cluster management and coordination services for HugeGraph distributed deployments. It serves as the central control plane responsible for:

- **Service Discovery**: Automatic registration and discovery of Store and Server nodes
- **Partition Management**: Dynamic partition allocation, balancing, and rebalancing across Store nodes
- **Metadata Storage**: Centralized storage of cluster metadata, configuration, and state information
- **Node Scheduling**: Intelligent scheduling and load balancing of graph operations
- **Health Monitoring**: Continuous health checks and failure detection via heartbeat mechanism

PD uses [SOFA-jraft](https://github.com/sofastack/sofa-jraft) for Raft consensus and RocksDB for persistent metadata storage, ensuring high availability and consistency in distributed environments.

## Architecture

HugeGraph PD is a Maven multi-module project consisting of 8 modules:

| Module | Description |
|--------|-------------|
| **hg-pd-grpc** | gRPC protocol definitions (`.proto` files) and generated Java stubs for inter-service communication |
| **hg-pd-common** | Shared utilities, constants, and helper classes used across PD modules |
| **hg-pd-core** | Core PD logic: Raft integration, metadata stores, partition allocation, store monitoring, task scheduling |
| **hg-pd-service** | gRPC service implementations and REST API (Spring Boot) for management and metrics |
| **hg-pd-client** | Java client library for applications to communicate with PD cluster |
| **hg-pd-cli** | Command-line utilities for PD administration and debugging |
| **hg-pd-test** | Unit and integration tests for all PD components |
| **hg-pd-dist** | Distribution assembly: packaging, configuration templates, startup scripts |

For detailed architecture and design, see [Architecture Documentation](docs/architecture.md).

## Quick Start

### Prerequisites

- **Java**: 11 or higher
- **Maven**: 3.5 or higher
- **Disk Space**: At least 1GB for PD data directory

### Build

From the project root (build PD and all dependencies):

```bash
mvn clean package -pl hugegraph-pd -am -DskipTests
```

Or build from the `hugegraph-pd` directory:

```bash
cd hugegraph-pd
mvn clean install -DskipTests
```

The assembled distribution will be available at:
```
hugegraph-pd/hg-pd-dist/target/hugegraph-pd-<version>.tar.gz
```

### Run

Extract the distribution package and start PD:

```bash
tar -xzf hugegraph-pd-<version>.tar.gz
cd hugegraph-pd-<version>

# Start PD server
bin/start-hugegraph-pd.sh

# Stop PD server
bin/stop-hugegraph-pd.sh
```

#### Startup Options

```bash
bin/start-hugegraph-pd.sh [-g GC_TYPE] [-j "JVM_OPTIONS"] [-y ENABLE_OTEL]
```

- `-g`: GC type (`g1` or `ZGC`, default: `g1`)
- `-j`: Custom JVM options (e.g., `-j "-Xmx4g -Xms4g"`)
- `-y`: Enable OpenTelemetry tracing (`true` or `false`, default: `false`)

### Configuration

Key configuration file: `conf/application.yml`

#### Core Settings

| Parameter | Default | Description |
|-----------|---------|-------------|
| `grpc.host` | `127.0.0.1` | gRPC server bind address (use actual IP for production) |
| `grpc.port` | `8686` | gRPC server port |
| `server.port` | `8620` | REST API port for management and metrics |
| `raft.address` | `127.0.0.1:8610` | Raft service address for this PD node |
| `raft.peers-list` | `127.0.0.1:8610` | Comma-separated list of all PD nodes in the Raft cluster |
| `pd.data-path` | `./pd_data` | Directory for storing PD metadata and Raft logs |

#### Single-Node Example

```yaml
grpc:
  host: 127.0.0.1
  port: 8686

server:
  port: 8620

raft:
  address: 127.0.0.1:8610
  peers-list: 127.0.0.1:8610

pd:
  data-path: ./pd_data
```

#### 3-Node Cluster Example

For a production 3-node PD cluster, configure each node:

**Node 1** (`192.168.1.10`):
```yaml
grpc:
  host: 192.168.1.10
  port: 8686
raft:
  address: 192.168.1.10:8610
  peers-list: 192.168.1.10:8610,192.168.1.11:8610,192.168.1.12:8610
```

**Node 2** (`192.168.1.11`):
```yaml
grpc:
  host: 192.168.1.11
  port: 8686
raft:
  address: 192.168.1.11:8610
  peers-list: 192.168.1.10:8610,192.168.1.11:8610,192.168.1.12:8610
```

**Node 3** (`192.168.1.12`):
```yaml
grpc:
  host: 192.168.1.12
  port: 8686
raft:
  address: 192.168.1.12:8610
  peers-list: 192.168.1.10:8610,192.168.1.11:8610,192.168.1.12:8610
```

For detailed configuration options and production tuning, see [Configuration Guide](docs/configuration.md).

### Verify Deployment

Check if PD is running:

```bash
# Check process
ps aux | grep hugegraph-pd

# Test REST API
curl http://localhost:8620/actuator/health

# Check logs
tail -f logs/hugegraph-pd.log
```

## gRPC API

PD exposes several gRPC services for cluster management. Key services include:

- **PD Service** (`PDGrpc`): Store registration, partition queries, leader election
- **KV Service** (`KvServiceGrpc`): Distributed key-value operations for metadata
- **Watch Service** (`HgPdWatchGrpc`): Watch for partition and store changes
- **Pulse Service** (`HgPdPulseGrpc`): Heartbeat and health monitoring

Proto definitions are located in:
```
hugegraph-pd/hg-pd-grpc/src/main/proto/
```

For API reference and usage examples, see [API Reference](docs/api-reference.md).

## Testing

Run PD tests:

```bash
# All PD tests
mvn test -pl hugegraph-pd/hg-pd-test -am

# Specific test class
mvn test -pl hugegraph-pd/hg-pd-test -am -Dtest=YourTestClass
```

## Docker

Build PD Docker image:

```bash
# From project root
docker build -f hugegraph-pd/Dockerfile -t hugegraph-pd:latest .

# Run container
docker run -d \
  -p 8620:8620 \
  -p 8686:8686 \
  -p 8610:8610 \
  -v /path/to/conf:/hugegraph-pd/conf \
  -v /path/to/data:/hugegraph-pd/pd_data \
  --name hugegraph-pd \
  hugegraph-pd:latest
```

For Docker Compose examples with HugeGraph Store and Server, see:
```
hugegraph-server/hugegraph-dist/docker/example/
```

## Documentation

- [Architecture Documentation](docs/architecture.md) - System design, module details, and interaction flows
- [API Reference](docs/api-reference.md) - gRPC API definitions and usage examples
- [Configuration Guide](docs/configuration.md) - Configuration options and production tuning
- [Development Guide](docs/development.md) - Build, test, and contribution workflows

## Production Deployment Notes

### Cluster Size

- **Minimum**: 3 nodes (Raft quorum requirement)
- **Recommended**: 3 or 5 nodes for production (odd numbers for Raft election)

### High Availability

- PD uses Raft consensus for leader election and data replication
- Cluster can tolerate up to `(N-1)/2` node failures (e.g., 1 failure in 3-node cluster)
- Leader handles all write operations; followers handle read operations

### Network Requirements

- Ensure low latency (<5ms) between PD nodes for Raft consensus
- Open required ports: `8620` (REST), `8686` (gRPC), `8610` (Raft)

### Monitoring

PD exposes metrics via REST API at:
- Health check: `http://<pd-host>:8620/actuator/health`
- Metrics: `http://<pd-host>:8620/actuator/metrics`

## Community

- **Documentation**: https://hugegraph.apache.org/docs/
- **GitHub**: https://github.com/apache/hugegraph

## Contributing

Contributions are welcome! Please read our [Development Guide](docs/development.md) and follow the Apache HugeGraph contribution guidelines.
