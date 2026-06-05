# HugeGraph PD Configuration Guide

This document provides comprehensive configuration guidance for HugeGraph PD, including parameter descriptions, deployment scenarios, and production tuning recommendations.

## Table of Contents

- [Configuration File Overview](#configuration-file-overview)
- [Core Configuration Parameters](#core-configuration-parameters)
- [Deployment Scenarios](#deployment-scenarios)
- [Production Tuning](#production-tuning)
- [Logging Configuration](#logging-configuration)
- [Monitoring and Metrics](#monitoring-and-metrics)

## Configuration File Overview

### Configuration Files

PD uses the following configuration files (located in `conf/` directory):

| File | Purpose |
|------|---------|
| `application.yml` | Main PD configuration (gRPC, Raft, storage, etc.) |
| `log4j2.xml` | Logging configuration (log levels, appenders, rotation) |
| `verify-license.json` | License verification configuration (optional) |

### Configuration Hierarchy

```
application.yml
├── spring              # Spring Boot framework settings
├── management          # Actuator endpoints and metrics
├── logging             # Log configuration file location
├── license             # License verification (optional)
├── grpc                # gRPC server settings
├── server              # REST API server settings
├── pd                  # PD-specific settings
├── raft                # Raft consensus settings
├── store               # Store node management settings
└── partition           # Partition management settings
```

## Core Configuration Parameters

### gRPC Settings

Controls the gRPC server for inter-service communication.

```yaml
grpc:
  host: 127.0.0.1    # gRPC bind address
  port: 8686         # gRPC server port
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `grpc.host` | String | `127.0.0.1` | **IMPORTANT**: Must be set to actual IP address (not `127.0.0.1`) for distributed deployments. Store and Server nodes connect to this address. In Docker bridge networking, set this to the container hostname (e.g., `pd0`) via `HG_PD_GRPC_HOST` env var. |
| `grpc.port` | Integer | `8686` | gRPC server port. Ensure this port is accessible from Store and Server nodes. |

**Production Notes**:
- Set `grpc.host` to the node's actual IP address (e.g., `192.168.1.10`)
- Avoid using `0.0.0.0` as it may cause service discovery issues
- Ensure firewall allows incoming connections on `grpc.port`

### REST API Settings

Controls the REST API server for management and monitoring.

```yaml
server:
  port: 8620    # REST API port
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `server.port` | Integer | `8620` | REST API port for health checks, metrics, and management operations. |

**Endpoints**:
- Health check: `http://<host>:8620/actuator/health`
- Metrics: `http://<host>:8620/actuator/metrics`
- Prometheus: `http://<host>:8620/actuator/prometheus`

### Raft Consensus Settings

Controls Raft consensus for PD cluster coordination.

```yaml
raft:
  address: 127.0.0.1:8610                  # This node's Raft address
  peers-list: 127.0.0.1:8610               # All PD nodes in the cluster
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `raft.address` | String | `127.0.0.1:8610` | Raft service address for this PD node. Format: `<ip>:<port>`. Must be unique across all PD nodes. |
| `raft.peers-list` | String | `127.0.0.1:8610` | Comma-separated list of all PD nodes' Raft addresses. Used for cluster formation and leader election. |

**Critical Rules**:
1. `raft.address` must be unique for each PD node
2. `raft.peers-list` must be **identical** on all PD nodes
3. `raft.peers-list` must contain all PD nodes (including this node)
4. Use actual IP addresses, not `127.0.0.1`, for multi-node clusters
5. Cluster size should be odd (3, 5, 7) for optimal Raft quorum

**Example** (3-node cluster):
```yaml
# Node 1
raft:
  address: 192.168.1.10:8610
  peers-list: 192.168.1.10:8610,192.168.1.11:8610,192.168.1.12:8610

# Node 2
raft:
  address: 192.168.1.11:8610
  peers-list: 192.168.1.10:8610,192.168.1.11:8610,192.168.1.12:8610

# Node 3
raft:
  address: 192.168.1.12:8610
  peers-list: 192.168.1.10:8610,192.168.1.11:8610,192.168.1.12:8610
```

### Docker Bridge Network Deployment

When deploying PD in Docker with bridge networking (e.g., `docker/docker-compose-3pd-3store-3server.yml`), container hostnames are used instead of IP addresses. Configuration is injected via `HG_PD_*` environment variables:

```yaml
# pd0 — set via HG_PD_RAFT_ADDRESS and HG_PD_RAFT_PEERS_LIST env vars
raft:
  address: pd0:8610
  peers-list: pd0:8610,pd1:8610,pd2:8610

# pd1
raft:
  address: pd1:8610
  peers-list: pd0:8610,pd1:8610,pd2:8610

# pd2
raft:
  address: pd2:8610
  peers-list: pd0:8610,pd1:8610,pd2:8610
```

The `grpc.host` must also use the container hostname (e.g., `pd0`) set via `HG_PD_GRPC_HOST`. Do not use `127.0.0.1` or `0.0.0.0` in bridge networking mode.

See [docker/README.md](../../docker/README.md) for the full environment variable reference.

### PD Core Settings

Controls PD-specific behavior.

```yaml
pd:
  data-path: ./pd_data              # Metadata storage path
  patrol-interval: 1800             # Partition rebalancing interval (seconds)
  initial-store-count: 1            # Minimum stores for cluster availability
  initial-store-list: 127.0.0.1:8500  # Auto-activated stores
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `pd.data-path` | String | `./pd_data` | Directory for RocksDB metadata storage and Raft logs. Ensure sufficient disk space and fast I/O (SSD recommended). |
| `pd.patrol-interval` | Integer | `1800` | Interval (in seconds) for partition health patrol and automatic rebalancing. Lower values = more frequent checks. |
| `pd.initial-store-count` | Integer | `1` | Minimum number of Store nodes required for cluster to be operational. Set to expected initial store count. |
| `pd.initial-store-list` | String | `127.0.0.1:8500` | Comma-separated list of Store gRPC addresses to auto-activate on startup. Useful for bootstrapping. |

**Production Recommendations**:
- `pd.data-path`: Use dedicated SSD with at least 50GB free space
- `pd.patrol-interval`:
  - Development: `300` (5 minutes) for fast testing
  - Production: `1800` (30 minutes) to reduce overhead
  - Large clusters: `3600` (1 hour)
- `pd.initial-store-count`: Set to expected initial store count (e.g., `3` for 3 stores)

### Store Management Settings

Controls how PD monitors and manages Store nodes.

```yaml
store:
  max-down-time: 172800              # Store permanent failure threshold (seconds)
  monitor_data_enabled: true         # Enable metrics collection
  monitor_data_interval: 1 minute    # Metrics collection interval
  monitor_data_retention: 1 day      # Metrics retention period
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `store.max-down-time` | Integer | `172800` | Time (in seconds) after which a Store is considered permanently offline and its partitions are reallocated. Default: 48 hours. |
| `store.monitor_data_enabled` | Boolean | `true` | Enable collection of Store metrics (CPU, memory, disk, partition count). |
| `store.monitor_data_interval` | Duration | `1 minute` | Interval for collecting Store metrics. Format: `<value> <unit>` (second, minute, hour). |
| `store.monitor_data_retention` | Duration | `1 day` | Retention period for historical metrics. Format: `<value> <unit>` (day, month, year). |

**Production Recommendations**:
- `store.max-down-time`:
  - Development: `300` (5 minutes) for fast failover testing
  - Production: `86400` (24 hours) to avoid false positives during maintenance
  - Conservative: `172800` (48 hours) for network instability
- `store.monitor_data_interval`:
  - High-frequency monitoring: `10 seconds`
  - Standard: `1 minute`
  - Low overhead: `5 minutes`
- `store.monitor_data_retention`:
  - Short-term: `1 day`
  - Standard: `7 days`
  - Long-term: `30 days` (requires more disk space)

### Partition Settings

Controls partition allocation and replication.

```yaml
partition:
  default-shard-count: 1              # Replicas per partition
  store-max-shard-count: 12           # Max partitions per store
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `partition.default-shard-count` | Integer | `1` | Number of replicas per partition. Typically `3` in production for high availability. |
| `partition.store-max-shard-count` | Integer | `12` | Maximum number of partition replicas a single Store can hold. Used for initial partition allocation. |

**Initial Partition Count Calculation**:
```
initial_partitions = (store_count * store_max_shard_count) / default_shard_count
```

**Example**:
- 3 stores, `store-max-shard-count=12`, `default-shard-count=3`
- Initial partitions: `(3 * 12) / 3 = 12` partitions
- Each store hosts: `12 * 3 / 3 = 12` shards (4 partitions as leader + 8 as follower)

**Production Recommendations**:
- `partition.default-shard-count`:
  - Development/Testing: `1` (no replication)
  - Production: `3` (standard HA configuration)
  - Critical systems: `5` (maximum fault tolerance)
- `partition.store-max-shard-count`:
  - Small deployment: `10-20`
  - Medium deployment: `50-100`
  - Large deployment: `200-500`
  - Limit based on Store disk capacity and expected data volume

### Management and Metrics

Controls Spring Boot Actuator endpoints for monitoring.

```yaml
management:
  metrics:
    export:
      prometheus:
        enabled: true    # Enable Prometheus metrics export
  endpoints:
    web:
      exposure:
        include: "*"     # Expose all actuator endpoints
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `management.metrics.export.prometheus.enabled` | Boolean | `true` | Enable Prometheus-compatible metrics at `/actuator/prometheus`. |
| `management.endpoints.web.exposure.include` | String | `"*"` | Actuator endpoints to expose. `"*"` = all, or specify comma-separated list (e.g., `"health,metrics"`). |

## Deployment Scenarios

### Single-Node Deployment (Development/Testing)

Minimal configuration for local development.

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
  patrol-interval: 300              # Fast rebalancing for testing
  initial-store-count: 1
  initial-store-list: 127.0.0.1:8500

store:
  max-down-time: 300                # Fast failover for testing
  monitor_data_enabled: true
  monitor_data_interval: 10 seconds
  monitor_data_retention: 1 day

partition:
  default-shard-count: 1            # No replication
  store-max-shard-count: 10
```

**Characteristics**:
- Single PD node (no HA)
- No replication (`default-shard-count=1`)
- Fast rebalancing for quick testing
- Suitable for development, not for production

### 3-Node Cluster Deployment (Production Standard)

Recommended configuration for production deployments.

#### Node 1: 192.168.1.10

```yaml
grpc:
  host: 192.168.1.10
  port: 8686

server:
  port: 8620

raft:
  address: 192.168.1.10:8610
  peers-list: 192.168.1.10:8610,192.168.1.11:8610,192.168.1.12:8610

pd:
  data-path: /data/pd/metadata
  patrol-interval: 1800
  initial-store-count: 3
  initial-store-list: 192.168.1.20:8500,192.168.1.21:8500,192.168.1.22:8500

store:
  max-down-time: 86400              # 24 hours
  monitor_data_enabled: true
  monitor_data_interval: 1 minute
  monitor_data_retention: 7 days

partition:
  default-shard-count: 3            # Triple replication
  store-max-shard-count: 50
```

#### Node 2: 192.168.1.11

```yaml
grpc:
  host: 192.168.1.11
  port: 8686

server:
  port: 8620

raft:
  address: 192.168.1.11:8610
  peers-list: 192.168.1.10:8610,192.168.1.11:8610,192.168.1.12:8610

pd:
  data-path: /data/pd/metadata
  patrol-interval: 1800
  initial-store-count: 3
  initial-store-list: 192.168.1.20:8500,192.168.1.21:8500,192.168.1.22:8500

store:
  max-down-time: 86400
  monitor_data_enabled: true
  monitor_data_interval: 1 minute
  monitor_data_retention: 7 days

partition:
  default-shard-count: 3
  store-max-shard-count: 50
```

#### Node 3: 192.168.1.12

```yaml
grpc:
  host: 192.168.1.12
  port: 8686

server:
  port: 8620

raft:
  address: 192.168.1.12:8610
  peers-list: 192.168.1.10:8610,192.168.1.11:8610,192.168.1.12:8610

pd:
  data-path: /data/pd/metadata
  patrol-interval: 1800
  initial-store-count: 3
  initial-store-list: 192.168.1.20:8500,192.168.1.21:8500,192.168.1.22:8500

store:
  max-down-time: 86400
  monitor_data_enabled: true
  monitor_data_interval: 1 minute
  monitor_data_retention: 7 days

partition:
  default-shard-count: 3
  store-max-shard-count: 50
```

**Characteristics**:
- 3 PD nodes for high availability
- Tolerates 1 PD node failure
- Triple replication (`default-shard-count=3`)
- 3 Store nodes specified in `initial-store-list`
- Standard monitoring and metrics collection

**Network Requirements**:
- Low latency (<5ms) between PD nodes for Raft
- Open ports: 8620 (REST), 8686 (gRPC), 8610 (Raft)

### 5-Node Cluster Deployment (High Availability)

Configuration for mission-critical deployments requiring maximum fault tolerance.

```yaml
# Node 1: 192.168.1.10
grpc:
  host: 192.168.1.10
  port: 8686

raft:
  address: 192.168.1.10:8610
  peers-list: 192.168.1.10:8610,192.168.1.11:8610,192.168.1.12:8610,192.168.1.13:8610,192.168.1.14:8610

pd:
  data-path: /data/pd/metadata
  patrol-interval: 3600             # Lower frequency for large clusters
  initial-store-count: 5
  initial-store-list: 192.168.1.20:8500,192.168.1.21:8500,192.168.1.22:8500,192.168.1.23:8500,192.168.1.24:8500

store:
  max-down-time: 172800             # 48 hours (conservative)
  monitor_data_enabled: true
  monitor_data_interval: 1 minute
  monitor_data_retention: 30 days   # Long-term retention

partition:
  default-shard-count: 3            # Or 5 for extreme HA
  store-max-shard-count: 100
```

**Characteristics**:
- 5 PD nodes for maximum HA
- Tolerates 2 PD node failures
- 5 Store nodes for data distribution
- Lower patrol frequency to reduce overhead
- Long-term metrics retention (30 days)

## Production Tuning

### JVM Tuning

JVM options are specified via the startup script (`bin/start-hugegraph-pd.sh`).

#### Memory Configuration

```bash
# Option 1: Via startup script flag
bin/start-hugegraph-pd.sh -j "-Xmx8g -Xms8g"

# Option 2: Edit start-hugegraph-pd.sh directly
JAVA_OPTIONS="-Xmx8g -Xms8g -XX:+UseG1GC"
```

**Recommendations by Cluster Size**:

| Cluster Size | Partitions | Heap Size | Notes |
|--------------|------------|-----------|-------|
| Small (1-3 stores, <100 partitions) | <100 | `-Xmx2g -Xms2g` | Development/testing |
| Medium (3-10 stores, 100-1000 partitions) | 100-1000 | `-Xmx4g -Xms4g` | Standard production |
| Large (10-50 stores, 1000-10000 partitions) | 1000-10000 | `-Xmx8g -Xms8g` | Large production |
| X-Large (50+ stores, 10000+ partitions) | 10000+ | `-Xmx16g -Xms16g` | Enterprise scale |

**Key Principles**:
- Set `-Xms` equal to `-Xmx` to avoid heap resizing
- Reserve at least 2GB for OS and off-heap memory
- Monitor GC pause times and adjust accordingly

#### Garbage Collection

**G1GC (Default, Recommended)**:
```bash
bin/start-hugegraph-pd.sh -g g1 -j "-Xmx8g -Xms8g \
  -XX:MaxGCPauseMillis=200 \
  -XX:G1HeapRegionSize=16m \
  -XX:InitiatingHeapOccupancyPercent=45"
```

- **MaxGCPauseMillis**: Target GC pause time (200ms recommended)
- **G1HeapRegionSize**: Region size (16m for 8GB heap)
- **InitiatingHeapOccupancyPercent**: When to trigger concurrent GC (45% recommended)

**ZGC (Low-Latency, Java 11+)**:
```bash
bin/start-hugegraph-pd.sh -g ZGC -j "-Xmx8g -Xms8g \
  -XX:ZCollectionInterval=30"
```

- Ultra-low pause times (<10ms)
- Recommended for latency-sensitive deployments
- Requires Java 11+ (Java 15+ for production)

#### GC Logging

```bash
-Xlog:gc*:file=logs/gc.log:time,uptime,level,tags:filecount=10,filesize=100M
```

### Raft Tuning

Raft parameters are typically sufficient with defaults, but can be tuned for specific scenarios.

#### Election Timeout

Increase election timeout for high-latency networks.

**Default**: 1000ms (1 second)

**Tuning** (requires code changes in `RaftEngine.java`):
```java
// In hg-pd-core/.../raft/RaftEngine.java
nodeOptions.setElectionTimeoutMs(3000);  // 3 seconds
```

**When to Increase**:
- Network latency >10ms between PD nodes
- Frequent false leader elections
- Cross-datacenter deployments

#### Snapshot Interval

Control how often Raft snapshots are created.

**Default**: 3600 seconds (1 hour)

**Tuning** (in `RaftEngine.java`):
```java
nodeOptions.setSnapshotIntervalSecs(7200);  // 2 hours
```

**Recommendations**:
- **Frequent snapshots** (1800s): Faster recovery, more I/O overhead
- **Infrequent snapshots** (7200s): Less I/O, slower recovery

### Disk I/O Optimization

#### RocksDB Configuration

PD uses RocksDB for metadata storage. Optimize for your workload.

**SSD Optimization** (default, recommended):
- RocksDB uses default settings optimized for SSD
- No configuration changes needed

**HDD Optimization** (not recommended):
If using HDD (not recommended for production):
```java
// In MetadataRocksDBStore.java, customize RocksDB options
Options options = new Options()
    .setCompactionStyle(CompactionStyle.LEVEL)
    .setWriteBufferSize(64 * 1024 * 1024)  // 64MB
    .setMaxWriteBufferNumber(3)
    .setLevelCompactionDynamicLevelBytes(true);
```

**Key Metrics to Monitor**:
- Disk I/O utilization
- RocksDB write stalls
- Compaction backlog

### Network Tuning

#### gRPC Connection Pooling

For high-throughput scenarios, tune gRPC connection pool size.

**Client-Side** (in `PDClient`):
```java
PDConfig config = PDConfig.builder()
    .pdServers("192.168.1.10:8686,192.168.1.11:8686,192.168.1.12:8686")
    .maxChannels(5)  // Number of gRPC channels per PD node
    .build();
```

**Recommendations**:
- Low traffic: `maxChannels=1`
- Medium traffic: `maxChannels=3-5`
- High traffic: `maxChannels=10+`

#### TCP Tuning (Linux)

Optimize OS-level TCP settings for low latency.

```bash
# Increase TCP buffer sizes
sysctl -w net.core.rmem_max=16777216
sysctl -w net.core.wmem_max=16777216
sysctl -w net.ipv4.tcp_rmem="4096 87380 16777216"
sysctl -w net.ipv4.tcp_wmem="4096 65536 16777216"

# Reduce TIME_WAIT connections
sysctl -w net.ipv4.tcp_tw_reuse=1
sysctl -w net.ipv4.tcp_fin_timeout=30
```

### Monitoring and Alerting

#### Key Metrics to Monitor

| Metric | Threshold | Action |
|--------|-----------|--------|
| PD Leader Changes | >2 per hour | Investigate network stability, increase election timeout |
| Raft Log Lag | >1000 entries | Check follower disk I/O, network latency |
| Store Heartbeat Failures | >5% | Check Store node health, network connectivity |
| Partition Imbalance | >20% deviation | Reduce `patrol-interval`, check rebalancing logic |
| GC Pause Time | >500ms | Tune GC settings, increase heap size |
| Disk Usage (`pd.data-path`) | >80% | Clean up old snapshots, expand disk, increase `monitor_data_retention` |

#### Prometheus Scrape Configuration

```yaml
scrape_configs:
  - job_name: 'hugegraph-pd'
    static_configs:
      - targets:
          - '192.168.1.10:8620'
          - '192.168.1.11:8620'
          - '192.168.1.12:8620'
    metrics_path: '/actuator/prometheus'
    scrape_interval: 15s
```

#### Grafana Dashboards

Key panels to create:
- **PD Cluster Status**: Leader, follower count, Raft state
- **Store Health**: Online/offline stores, heartbeat success rate
- **Partition Distribution**: Partitions per store, leader distribution
- **Performance**: QPS, latency (p50, p95, p99)
- **System Resources**: CPU, memory, disk I/O, network

## Logging Configuration

### log4j2.xml

Located at `conf/log4j2.xml`.

#### Log Levels

```xml
<Loggers>
    <!-- PD application logs -->
    <Logger name="org.apache.hugegraph.pd" level="INFO"/>

    <!-- Raft consensus logs (verbose, set to WARN in production) -->
    <Logger name="com.alipay.sofa.jraft" level="WARN"/>

    <!-- RocksDB logs -->
    <Logger name="org.rocksdb" level="WARN"/>

    <!-- gRPC logs -->
    <Logger name="io.grpc" level="WARN"/>

    <!-- Root logger -->
    <Root level="INFO">
        <AppenderRef ref="RollingFile"/>
        <AppenderRef ref="Console"/>
    </Root>
</Loggers>
```

**Recommendations**:
- **Development**: Set PD logger to `DEBUG` for detailed tracing
- **Production**: Use `INFO` (default) or `WARN` for lower overhead
- **Troubleshooting**: Temporarily set specific package to `DEBUG`

#### Log Rotation

```xml
<RollingFile name="RollingFile" fileName="logs/hugegraph-pd.log"
             filePattern="logs/hugegraph-pd-%d{yyyy-MM-dd}-%i.log.gz">
    <PatternLayout>
        <Pattern>%d{ISO8601} [%t] %-5level %logger{36} - %msg%n</Pattern>
    </PatternLayout>
    <Policies>
        <TimeBasedTriggeringPolicy interval="1" modulate="true"/>
        <SizeBasedTriggeringPolicy size="100 MB"/>
    </Policies>
    <DefaultRolloverStrategy max="30"/>
</RollingFile>
```

**Configuration**:
- **Size**: Rotate when log file reaches 100MB
- **Time**: Rotate daily
- **Retention**: Keep last 30 log files

## Monitoring and Metrics

### Health Check

```bash
curl http://localhost:8620/actuator/health
```

**Response** (healthy):
```json
{
  "status": "UP"
}
```

### Metrics Endpoint

```bash
curl http://localhost:8620/actuator/metrics
```

**Available Metrics**:
- `pd.raft.state`: Raft state (0=Follower, 1=Candidate, 2=Leader)
- `pd.store.count`: Number of stores by state
- `pd.partition.count`: Total partitions
- `jvm.memory.used`: JVM memory usage
- `jvm.gc.pause`: GC pause times

### Prometheus Metrics

```bash
curl http://localhost:8620/actuator/prometheus
```

**Sample Output**:
```
# HELP pd_raft_state Raft state
# TYPE pd_raft_state gauge
pd_raft_state 2.0

# HELP pd_store_count Store count by state
# TYPE pd_store_count gauge
pd_store_count{state="Up"} 3.0
pd_store_count{state="Offline"} 0.0

# HELP pd_partition_count Total partitions
# TYPE pd_partition_count gauge
pd_partition_count 36.0
```

## Configuration Validation

### Pre-Deployment Checklist

- [ ] `grpc.host` set to actual IP address or container hostname (not `127.0.0.1`). For Docker bridge networking use container hostname via `HG_PD_GRPC_HOST` env var.
- [ ] `raft.address` unique for each PD node
- [ ] `raft.peers-list` identical on all PD nodes
- [ ] `raft.peers-list` contains all PD node addresses
- [ ] `pd.data-path` has sufficient disk space (>50GB)
- [ ] `pd.initial-store-count` matches expected store count
- [ ] `partition.default-shard-count` = 3 (for production HA)
- [ ] Ports accessible from Store/Server nodes (8620, 8686, 8610)
- [ ] NTP synchronized across all nodes

### Configuration Validation Tool

```bash
# Check Raft configuration
grep -A2 "^raft:" conf/application.yml

# Verify peers list on all nodes
for node in 192.168.1.{10,11,12}; do
    echo "Node $node:"
    ssh $node "grep peers-list /path/to/conf/application.yml"
done

# Check port accessibility
nc -zv 192.168.1.10 8620 8686 8610
```

## Summary

Key configuration guidelines:
- **Single-node**: Use defaults with `127.0.0.1` addresses
- **3-node cluster**: Standard production setup with triple replication
- **5-node cluster**: Maximum HA with increased fault tolerance
- **JVM tuning**: Allocate 4-8GB heap for typical production deployments
- **Monitoring**: Enable Prometheus metrics and create Grafana dashboards

For architecture details, see [Architecture Documentation](architecture.md).

For API usage, see [API Reference](api-reference.md).

For development, see [Development Guide](development.md).
