# Best Practices

Production best practices for deploying, configuring, and operating HugeGraph Store at scale.

## Table of Contents

- [Hardware Sizing](#hardware-sizing)
- [Performance Tuning](#performance-tuning)
- [Security Configuration](#security-configuration)
- [High Availability Design](#high-availability-design)
- [Cost Optimization](#cost-optimization)

---

## Hardware Sizing

### Store Node Recommendations

#### Small Deployment (< 1TB data, < 10K QPS)

**Specifications**:
- CPU: 8-16 cores
- Memory: 32-64 GB
- Disk: 500GB-1TB SSD
- Network: 1 Gbps

**Configuration**:
```yaml
# application-pd.yml
rocksdb:
  total_memory_size: 24000000000   # 24GB (75% of 32GB)
  block_cache_size: 16000000000    # 16GB
  write_buffer_size: 134217728     # 128MB
```

**JVM Settings**:
```bash
# start-hugegraph-store.sh
JAVA_OPTS="-Xms8g -Xmx8g -XX:+UseG1GC"
```

#### Medium Deployment (1-10TB data, 10-50K QPS)

**Specifications**:
- CPU: 16-32 cores
- Memory: 64-128 GB
- Disk: 1-5TB NVMe SSD
- Network: 10 Gbps

**Configuration**:
```yaml
rocksdb:
  total_memory_size: 64000000000   # 64GB (50% of 128GB)
  block_cache_size: 48000000000    # 48GB
  write_buffer_size: 268435456     # 256MB
  max_write_buffer_number: 8
  max_background_jobs: 12
```

**JVM Settings**:
```bash
JAVA_OPTS="-Xms16g -Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
```

#### Large Deployment (10TB+ data, 50K+ QPS)

**Specifications**:
- CPU: 32-64 cores
- Memory: 128-256 GB
- Disk: 5-20TB NVMe SSD (multiple disks)
- Network: 25 Gbps

**Configuration**:
```yaml
rocksdb:
  total_memory_size: 128000000000  # 128GB
  block_cache_size: 96000000000    # 96GB
  write_buffer_size: 536870912     # 512MB
  max_write_buffer_number: 12
  max_background_jobs: 20

app:
  data-path: /data1,/data2,/data3  # Multiple disks for parallelism
```

**JVM Settings**:
```bash
JAVA_OPTS="-Xms32g -Xmx32g -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:G1ReservePercent=20"
```

### PD Node Recommendations

**Specifications**:
- CPU: 4-8 cores
- Memory: 8-16 GB
- Disk: 50-100GB SSD (for Raft logs and metadata)
- Network: 1 Gbps

**Note**: PD is lightweight; resources primarily for Raft and metadata storage

### Network Requirements

**Latency**:
- Within Store cluster: < 1ms (ideal), < 5ms (acceptable)
- Store to PD: < 10ms
- Server to Store: < 10ms

**Bandwidth**:
- Store internal (Raft replication): 1 Gbps minimum, 10 Gbps recommended
- Server to Store: 10 Gbps for high-throughput workloads

**Testing**:
```bash
# Latency test between nodes
ping -c 100 192.168.1.21

# Bandwidth test
iperf3 -s  # On target node
iperf3 -c 192.168.1.21 -t 30  # On source node
```

---

## Performance Tuning

### RocksDB Tuning

#### Write-Heavy Workloads

**Goal**: Minimize write amplification

**Configuration**:
```yaml
rocksdb:
  # Larger write buffers (reduce L0 file count)
  write_buffer_size: 536870912       # 512MB
  max_write_buffer_number: 12
  min_write_buffer_number_to_merge: 4

  # Delayed compaction (batch more writes)
  level0_file_num_compaction_trigger: 8  # Default: 4
  level0_slowdown_writes_trigger: 20     # Default: 20
  level0_stop_writes_trigger: 36         # Default: 36

  # Larger SST files (reduce file count)
  target_file_size_base: 268435456   # 256MB
  max_bytes_for_level_base: 2147483648  # 2GB

  # More background jobs
  max_background_jobs: 20
```

**Trade-off**: Higher memory usage, longer flush times

#### Read-Heavy Workloads

**Goal**: Maximize cache hit rate

**Configuration**:
```yaml
rocksdb:
  # Large block cache
  block_cache_size: 96000000000      # 96GB

  # Pin L0/L1 index and filters in cache
  cache_index_and_filter_blocks: true
  pin_l0_filter_and_index_blocks_in_cache: true

  # Bloom filters (reduce disk reads)
  bloom_filter_bits_per_key: 10

  # Compression
  compression_type: lz4              # Fast decompression
```

**Trade-off**: Higher memory usage

#### Balanced Workloads

**Configuration**:
```yaml
rocksdb:
  total_memory_size: 64000000000
  block_cache_size: 48000000000      # 75% to cache
  write_buffer_size: 268435456       # 25% to writes
  max_write_buffer_number: 8
  max_background_jobs: 12
```

### Raft Tuning

#### Low-Latency Writes

**Goal**: Minimize Raft commit latency

**Configuration**:
```yaml
raft:
  # Reduce snapshot interval (smaller snapshots, faster transfer)
  snapshotInterval: 900              # 15 minutes

  # Increase disruptor buffer (reduce contention)
  disruptorBufferSize: 4096

  # Reduce max log file size (faster log rotation)
  max-log-file-size: 1073741824      # 1GB
```

**JRaft Internal Settings** (in code, advanced):
- `electionTimeoutMs`: 1000-2000 (faster leader election)
- `snapshotIntervalSecs`: 900 (align with config)

#### High-Throughput Writes

**Goal**: Maximize write throughput

**Configuration**:
```yaml
raft:
  # Larger snapshots (reduce snapshot frequency)
  snapshotInterval: 3600             # 60 minutes

  # Large buffer
  disruptorBufferSize: 8192

  # Large log files (reduce rotation overhead)
  max-log-file-size: 10737418240     # 10GB
```

### gRPC Tuning

**Server-Side** (in `application.yml`):
```yaml
grpc:
  max-inbound-message-size: 1048576000  # 1000MB (for large batches)
  netty-server-boss-threads: 4
  netty-server-worker-threads: 32       # 2x CPU cores
  netty-server-max-connection-idle: 3600000  # 1 hour
```

**Client-Side** (in HugeGraph Server config):
```properties
# hugegraph.properties
store.grpc_max_inbound_message_size=1048576000
store.max_sessions=8               # Per Store node
store.rpc_timeout=30000            # 30 seconds
```

### JVM Tuning

**G1GC Settings** (recommended for large heaps):
```bash
JAVA_OPTS="
  -Xms32g -Xmx32g                   # Fixed heap size
  -XX:+UseG1GC                      # G1 garbage collector
  -XX:MaxGCPauseMillis=200          # Target pause time
  -XX:G1ReservePercent=20           # Reserve for to-space
  -XX:InitiatingHeapOccupancyPercent=45  # GC trigger
  -XX:+ParallelRefProcEnabled       # Parallel reference processing
  -XX:+UnlockExperimentalVMOptions
  -XX:G1NewSizePercent=30           # Young generation size
  -XX:G1MaxNewSizePercent=40
"
```

**ZGC Settings** (for ultra-low latency, Java 11+):
```bash
JAVA_OPTS="
  -Xms32g -Xmx32g
  -XX:+UseZGC                       # ZGC (sub-10ms pauses)
  -XX:ZCollectionInterval=120       # GC interval (seconds)
  -XX:+UnlockDiagnosticVMOptions
"
```

**Monitoring GC**:
```bash
# Enable GC logging
JAVA_OPTS="$JAVA_OPTS -Xlog:gc*:file=logs/gc.log:time,uptime,level,tags"

# Analyze GC logs
tail -f logs/gc.log | grep "Pause"
```

---

## Security Configuration

### Network Security

#### Firewall Rules

**Store Nodes**:
```bash
# Allow gRPC from Server nodes only
iptables -A INPUT -p tcp --dport 8500 -s 192.168.1.30/28 -j ACCEPT  # Server subnet
iptables -A INPUT -p tcp --dport 8500 -j DROP

# Allow Raft from Store nodes only
iptables -A INPUT -p tcp --dport 8510 -s 192.168.1.20/28 -j ACCEPT  # Store subnet
iptables -A INPUT -p tcp --dport 8510 -j DROP

# Allow REST API from admin subnet only
iptables -A INPUT -p tcp --dport 8520 -s 192.168.1.0/24 -j ACCEPT
iptables -A INPUT -p tcp --dport 8520 -j DROP
```

**PD Nodes**:
```bash
# Allow gRPC from Server and Store
iptables -A INPUT -p tcp --dport 8686 -s 192.168.1.0/24 -j ACCEPT
iptables -A INPUT -p tcp --dport 8686 -j DROP

# Allow Raft from PD nodes only
iptables -A INPUT -p tcp --dport 8610 -s 192.168.1.10/28 -j ACCEPT
iptables -A INPUT -p tcp --dport 8610 -j DROP
```

#### TLS Encryption (Future Enhancement)

**Note**: TLS for gRPC is planned but not yet implemented in current version.

**Planned Configuration**:
```yaml
grpc:
  tls:
    enabled: true
    cert-file: /path/to/server.crt
    key-file: /path/to/server.key
    ca-file: /path/to/ca.crt
```

### Access Control

**Current State**: HugeGraph Store does not have built-in authentication. Access control is enforced at the Server layer.

**Recommendations**:
1. **Network Isolation**: Deploy Store in private subnet, inaccessible from public internet
2. **Server Authentication**: Enable HugeGraph Server authentication
3. **Role-Based Access**: Use Server's RBAC for user permissions

**Enable Server Authentication**:
```bash
# In HugeGraph Server
bin/enable-auth.sh

# Configure users and roles via REST API
curl -X POST "http://localhost:8080/graphspaces/{graph_spcace_name}/graphs/{graph}/auth/users" \
     -H "Content-Type: application/json" \
     -d '{
         "user_name": "admin",
         "user_password": "password123"
     }'
```

### Data Encryption

#### Encryption at Rest

**Option 1**: Filesystem-level encryption (recommended)
```bash
# Use LUKS for disk encryption
cryptsetup luksFormat /dev/sdb
cryptsetup open /dev/sdb store_data
mkfs.ext4 /dev/mapper/store_data
mount /dev/mapper/store_data /data
```

**Option 2**: Application-level encryption
- Encrypt data before writing to Store (in Server)
- Decrypt after reading from Store
- Trade-off: Performance overhead

#### Encryption in Transit

**Current**: gRPC without TLS (plaintext)

**Mitigation**:
- Use VPN or encrypted network tunnel (WireGuard, IPSec)
- Deploy in trusted private network

---

## High Availability Design

### Fault Domains

**Rack-Aware Deployment**:
```
Rack 1: Store1, Store4
Rack 2: Store2, Store5
Rack 3: Store3, Store6

Partition 1 replicas: Store1 (Rack 1), Store2 (Rack 2), Store3 (Rack 3)
→ Tolerates single rack failure
```

**Configure in PD** (advanced, requires code changes):
- Label Store nodes with rack/zone information
- PD placement policy: Avoid placing replicas in same rack

**Zone-Aware Deployment**:
```
Zone A: 3 PD, 3 Store, 3 Server
Zone B: 2 PD, 3 Store, 3 Server
Zone C: 0 PD, 3 Store, 0 Server

→ Tolerates entire zone failure (Zone C data loss acceptable if read-only)
```

### Replica Configuration

**Production Standard**: 3 replicas per partition

**High Availability**: 5 replicas per partition
- Tolerates 2 node failures
- Higher write latency (need 3/5 quorum)
- Higher storage cost (1.67x vs 1x)

**Configure in PD**:
```yaml
partition:
  default-shard-count: 5  # 5 replicas
```

### Split-Brain Prevention

**Raft Quorum**: Always use odd number of replicas (3, 5, 7)

**PD Cluster**: Always use odd number of nodes (3, 5)

**Network Partition Handling**:
- Majority partition continues operating
- Minority partition rejects writes (no quorum)
- When network heals, minority syncs from majority

### Monitoring for HA

**Alerts**:
1. Store node down: Alert immediately
2. PD leader lost: Alert if >1 minute
3. Partition without leader: Alert immediately
4. Replica count < 3: Alert (data at risk)

**Dashboards**:
- Cluster topology (node status)
- Partition distribution (replica health)
- Raft leader distribution

---

## Cost Optimization

### Storage Cost

**Compression**:
```yaml
rocksdb:
  # LZ4: Fast, 2-3x compression
  compression_type: lz4

  # Zstd: Slower, 4-6x compression (for cold data)
  bottommost_compression_type: zstd
  bottommost_compression_opts: "level=6"
```

**Benchmark**:
- No compression: 100GB
- LZ4: 40GB (60% savings, negligible CPU)
- Zstd: 20GB (80% savings, 10-20% CPU overhead)

**Compaction**:
- Enable periodic compaction to reclaim space
- Monitor `rocksdb.disk.usage` metric

### Compute Cost

**Right-Size Nodes**:
- Monitor CPU usage: <50% average → downsize
- Monitor memory usage: <60% average → downsize

**Auto-Scaling** (Kubernetes):
```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: hugegraph-store
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: StatefulSet
    name: hugegraph-store
  minReplicas: 3
  maxReplicas: 12
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
```

### Network Cost

**Reduce Cross-Zone Traffic**:
- Co-locate Server and Store in same zone
- Use zone-aware routing (if supported)

**Compression**:
- Enable gRPC compression (reduces bandwidth)
```yaml
grpc:
  enable-compression: true
```

### Operational Cost

**Automation**:
- Automate backups (cron jobs)
- Automate monitoring (Prometheus + Grafana)
- Automate alerting (PagerDuty, Slack)

**Reduce Manual Intervention**:
- Enable auto-rebalancing (PD patrol)
- Enable auto-scaling (if using Kubernetes)

---

## Summary Checklist

### Deployment Checklist

- [ ] Hardware meets specifications for workload size
- [ ] Network latency < 5ms between Store nodes
- [ ] Firewall rules configured (ports 8500, 8510, 8520)
- [ ] PD cluster deployed (3 or 5 nodes)
- [ ] Store cluster deployed (3+ nodes)
- [ ] Disk encryption enabled (if required)
- [ ] Backup strategy defined and tested

### Performance Checklist

- [ ] RocksDB memory = 50-75% of total memory
- [ ] JVM heap sized appropriately (8-32GB)
- [ ] Raft snapshot interval tuned (900-3600s)
- [ ] gRPC connection pool configured
- [ ] Indexes created for selective queries
- [ ] Compression enabled for storage

### Security Checklist

- [ ] Store nodes in private subnet
- [ ] Firewall rules restrict access
- [ ] HugeGraph Server authentication enabled
- [ ] Encryption at rest configured (if required)
- [ ] Regular security audits scheduled

### High Availability Checklist

- [ ] 3 replicas per partition (minimum)
- [ ] Replicas distributed across racks/zones
- [ ] Monitoring and alerting configured
- [ ] Disaster recovery plan documented and tested
- [ ] Backup and restore tested

### Cost Optimization Checklist

- [ ] Compression enabled (LZ4 or Zstd)
- [ ] Node sizes right-sized for actual usage
- [ ] Auto-scaling configured (if applicable)
- [ ] Cross-zone traffic minimized
- [ ] Regular cost reviews scheduled

---

For operational procedures, see [Operations Guide](operations-guide.md).

For development and debugging, see [Development Guide](development-guide.md).
