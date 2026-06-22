# Operations Guide

This guide covers monitoring, troubleshooting, backup & recovery, and operational procedures for HugeGraph Store in production.

## Table of Contents

- [Monitoring and Metrics](#monitoring-and-metrics)
- [Common Issues and Troubleshooting](#common-issues-and-troubleshooting)
- [Backup and Recovery](#backup-and-recovery)
- [Capacity Management](#capacity-management)
- [Rolling Upgrades](#rolling-upgrades)

---

## Monitoring and Metrics

### Metrics Endpoints

**Store Node Metrics**:
```bash
# Health check
curl http://<store-host>:8520/actuator/health

# All metrics
curl http://<store-host>:8520/actuator/metrics

# Specific metric
curl http://<store-host>:8520/actuator/metrics/jvm.memory.used
```

**PD Metrics**:
```bash
curl http://<pd-host>:8620/actuator/metrics
```

### Key Metrics to Monitor

#### 1. Raft Metrics

**Metric**: `raft.leader.election.count`
- **Description**: Number of leader elections
- **Normal**: 0-1 per hour (initial election)
- **Warning**: >5 per hour (network issues or node instability)

**Metric**: `raft.log.apply.latency`
- **Description**: Time to apply Raft log entries (ms)
- **Normal**: <10ms (p99)
- **Warning**: >50ms (disk I/O bottleneck)

**Metric**: `raft.snapshot.create.duration`
- **Description**: Snapshot creation time (ms)
- **Normal**: <30,000ms (30 seconds)
- **Warning**: >60,000ms (large partition or slow disk)

#### 2. RocksDB Metrics

**Metric**: `rocksdb.read.latency`
- **Description**: RocksDB read latency (microseconds)
- **Normal**: <1000μs (1ms) for p99
- **Warning**: >5000μs (5ms) - check compaction or cache hit rate

**Metric**: `rocksdb.write.latency`
- **Description**: RocksDB write latency (microseconds)
- **Normal**: <2000μs (2ms) for p99
- **Warning**: >10000μs (10ms) - check compaction backlog

**Metric**: `rocksdb.compaction.pending`
- **Description**: Number of pending compactions
- **Normal**: 0-2
- **Warning**: >5 (write stall likely)

**Metric**: `rocksdb.block.cache.hit.rate`
- **Description**: Block cache hit rate (%)
- **Normal**: >90%
- **Warning**: <70% (increase cache size)

#### 3. Partition Metrics

**Metric**: `partition.count`
- **Description**: Number of partitions on this Store node
- **Normal**: Evenly distributed across nodes
- **Warning**: >2x average (rebalancing needed)

**Metric**: `partition.leader.count`
- **Description**: Number of Raft leaders on this node
- **Normal**: ~partitionCount / 3 (for 3 replicas)
- **Warning**: 0 (node cannot serve writes)

**Queries**:
```bash
# Check partition distribution
curl  http://localhost:8620/v1/partitionsAndStats 

# Example output (imbalanced):
# {
#   {
#   "partitions": {}, 
#   "partitionStats: {}"
#   }
# }
```

#### 4. gRPC Metrics

**Metric**: `grpc.request.qps`
- **Description**: Requests per second
- **Normal**: Depends on workload
- **Warning**: Sudden drops (connection issues)

**Metric**: `grpc.request.latency`
- **Description**: gRPC request latency (ms)
- **Normal**: <20ms for p99
- **Warning**: >100ms (network or processing bottleneck)

**Metric**: `grpc.error.rate`
- **Description**: Error rate (errors/sec)
- **Normal**: <1% of QPS
- **Warning**: >5% (investigate errors)

#### 5. System Metrics

**Disk Usage**:
```bash
# Check Store data directory
df -h | grep storage

# Recommended: <80% full
# Warning: >90% full
```

**Memory Usage**:
```bash
# JVM heap usage
curl http://192.168.1.20:8520/actuator/metrics/jvm.memory.used

# RocksDB memory (block cache + memtables)
curl http://192.168.1.20:8520/actuator/metrics/rocksdb.memory.usage
```

**CPU Usage**:
```bash
# Overall CPU
top -p $(pgrep -f hugegraph-store)

# Recommended: <70% average
# Warning: >90% sustained
```

### Prometheus Integration

**Configure Prometheus** (`prometheus.yml`):
```yaml
scrape_configs:
  - job_name: 'hugegraph-store'
    static_configs:
      - targets:
          - '192.168.1.20:8520'
          - '192.168.1.21:8520'
          - '192.168.1.22:8520'
    metrics_path: '/actuator/prometheus'
    scrape_interval: 15s
```

**Grafana Dashboard**: Import HugeGraph Store dashboard (JSON available in project)

### Alert Rules

**Example Prometheus Alerts** (`alerts.yml`):
```yaml
groups:
  - name: hugegraph-store
    rules:
      # Raft leader elections too frequent
      - alert: FrequentLeaderElections
        expr: rate(raft_leader_election_count[5m]) > 0.01
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Frequent Raft leader elections on {{ $labels.instance }}"

      # RocksDB write stall
      - alert: RocksDBWriteStall
        expr: rocksdb_compaction_pending > 10
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "RocksDB write stall on {{ $labels.instance }}"

      # Disk usage high
      - alert: HighDiskUsage
        expr: disk_used_percent > 85
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Disk usage >85% on {{ $labels.instance }}"

      # Store node down
      - alert: StoreNodeDown
        expr: up{job="hugegraph-store"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Store node {{ $labels.instance }} is down"
```

---

## Common Issues and Troubleshooting

### Issue 1: Raft Leader Election Failures

**Symptoms**:
- Write requests fail with "No leader"
- Frequent leader elections in logs
- `raft.leader.election.count` metric increasing rapidly

**Diagnosis**:
```bash
# Check Store logs
tail -f logs/hugegraph-store.log | grep "Raft election"

# Check network latency between Store nodes
ping 192.168.1.21
ping 192.168.1.22

# Check Raft status (via PD)
curl http://192.168.1.10:8620/pd/v1/partitions | jq '.[] | select(.leader == null)'
```

**Root Causes**:
1. **Network Partition**: Store nodes cannot communicate
2. **High Latency**: Network latency >50ms between nodes
3. **Disk I/O Stall**: Raft log writes timing out
4. **Clock Skew**: System clocks out of sync

**Solutions**:
1. **Fix Network**: Check switches, firewalls, routing
2. **Reduce Latency**: Deploy nodes in same datacenter/zone
3. **Check Disk**: Use `iostat -x 1` to check disk I/O
4. **Sync Clocks**: Use NTP to synchronize system clocks
   ```bash
   ntpdate -u pool.ntp.org
   ```

---

### Issue 2: Partition Imbalance

**Symptoms**:
- Some Store nodes have 2x more partitions than others
- Uneven disk usage across Store nodes
- Some nodes overloaded, others idle

**Diagnosis**:
```bash
# Check partition distribution
curl  http://localhost:8620/v1/partitionsAndStats 

# Example output (imbalanced):
# {
#   {
#   "partitions": {}, 
#   "partitionStats: {}"
#   }
# }
```

**Root Causes**:
1. **New Store Added**: Partitions not yet rebalanced
2. **PD Patrol Disabled**: Auto-rebalancing not running
3. **Rebalancing Too Slow**: `patrol-interval` too high

**Solutions**:
1. **Trigger Manual Rebalance** (via PD API):
   ```bash
   curl http://192.168.1.10:8620/v1/balanceLeaders
   ```

2. **Reduce Patrol Interval** (in PD `application.yml`):
   ```yaml
   pd:
     patrol-interval: 600  # Rebalance every 10 minutes (instead of 30)
   ```

3. **Check PD Logs**:
   ```bash
   tail -f logs/hugegraph-pd.log | grep "balance"
   ```

4. **Wait**: Rebalancing is gradual (may take hours for large datasets)

---

### Issue 3: Data Migration Slow

**Symptoms**:
- Partition migration takes hours
- Raft snapshot transfer stalled
- High network traffic but low progress

**Diagnosis**:
```bash
# Check Raft snapshot status
tail -f logs/hugegraph-store.log | grep snapshot

# Check network throughput
iftop -i eth0

# Check disk I/O during snapshot
iostat -x 1
```

**Root Causes**:
1. **Large Partitions**: Partitions >10GB take long to transfer
2. **Network Bandwidth**: Limited bandwidth (<100Mbps)
3. **Disk I/O**: Slow disk on target Store

**Solutions**:
1. **Increase Snapshot Interval** (reduce snapshot size):
   ```yaml
   raft:
     snapshotInterval: 900  # Snapshot every 15 minutes
   ```

2. **Increase Network Bandwidth**: Use 1Gbps+ network

3. **Parallelize Migration**: PD migrates one partition at a time by default
   - Edit PD configuration to allow concurrent migrations (advanced)

4. **Monitor Progress**:
   ```bash
   # Check partition state transitions
   curl http://192.168.1.10:8620/v1/partitions | grep -i migrating
   ```

---

### Issue 4: RocksDB Performance Degradation

**Symptoms**:
- Query latency increasing over time
- `rocksdb.read.latency` >5ms
- `rocksdb.compaction.pending` >5

**Diagnosis**:
```bash
# Check Store logs for compaction
tail -f logs/hugegraph-store.log | grep compaction
```

**Root Causes**:
1. **Write Amplification**: Too many compactions
2. **Low Cache Hit Rate**: Block cache too small
3. **SST File Proliferation**: Too many SST files in L0

**Solutions**:
1. **Increase Block Cache** (in `application-pd.yml`):
   ```yaml
   rocksdb:
     block_cache_size: 32000000000  # 32GB (from 16GB)
   ```

2. **Increase Write Buffer** (reduce L0 files):
   ```yaml
   rocksdb:
     write_buffer_size: 268435456  # 256MB (from 128MB)
     max_write_buffer_number: 8    # More memtables
   ```

3. **Restart Store Node** (last resort, triggers compaction on startup):
   ```bash
   bin/stop-hugegraph-store.sh
   bin/start-hugegraph-store.sh
   ```

---

### Issue 5: Store Node Unresponsive

**Symptoms**:
- gRPC requests timing out
- Health check fails
- CPU or memory at 100%

**Diagnosis**:
```bash
# Check if process is alive
ps aux | grep hugegraph-store

# Check CPU/memory
top -p $(pgrep -f hugegraph-store)

# Check logs
tail -100 logs/hugegraph-store.log

# Check for OOM killer
dmesg | grep -i "out of memory"

# Check disk space
df -h
```

**Root Causes**:
1. **Out of Memory (OOM)**: JVM heap exhausted
2. **Disk Full**: No space for Raft logs or RocksDB writes
3. **Thread Deadlock**: Internal deadlock in Store code
4. **Network Saturation**: Too many concurrent requests

**Solutions**:
1. **OOM**:
   - Increase JVM heap: Edit `start-hugegraph-store.sh`, set `Xmx32g`
   - Restart Store node

2. **Disk Full**:
   - Clean up old Raft snapshots:
     ```bash
     rm -rf storage/raft/partition-*/snapshot/*  # Keep only latest
     ```
   - Add more disk space

3. **Thread Deadlock**:
   - Take thread dump:
     ```bash
     jstack $(pgrep -f hugegraph-store) > threaddump.txt
     ```
   - Restart Store node
   - Report to HugeGraph team with thread dump

4. **Network Saturation**:
   - Check connection count:
     ```bash
     netstat -an | grep :8500 | wc -l
     ```
   - Reduce `store.max_sessions` in Server config
   - Add more Store nodes to distribute load

---

## Backup and Recovery

### Backup Strategies

#### Strategy 1: Snapshot-Based Backup

**Frequency**: Daily or weekly

**Process**:
```bash
# On each Store node
cd storage

# Create snapshot (Raft snapshots)
# Snapshots are automatically created by Raft every `snapshotInterval` seconds
# Locate latest snapshot:
find raft/partition-*/snapshot -name "snapshot_*" -type d | sort | tail -5

# Copy to backup location
tar -czf backup-store1-$(date +%Y%m%d).tar.gz raft/partition-*/snapshot/*

# Upload to remote storage
scp backup-store1-*.tar.gz backup-server:/backups/
```

**Pros**:
- Fast backup (no downtime)
- Point-in-time recovery

**Cons**:
- Requires all Store nodes to be backed up
- May miss recent writes (since last snapshot)

### Disaster Recovery Procedures

#### Scenario 1: Single Store Node Failure

**Impact**: Partitions with replicas on this node lose one replica

**Action**:
1. **No immediate action needed**: Remaining replicas continue serving
2. **Monitor**: Check if Raft leaders re-elected
   ```bash
   curl http://192.168.1.10:8620/v1/partitions | grep leader
   ```

3. **Replace Failed Node**:
   - Deploy new Store node with same configuration
   - PD automatically assigns partitions to new node
   - Wait for data replication (may take hours)

4. **Verify**: Check partition distribution
   ```bash
    curl  http://localhost:8620/v1/partitionsAndStats
   ```

#### Scenario 2: Complete Store Cluster Failure

**Impact**: All data inaccessible

**Action**:
1. **Restore PD Cluster** (if also failed):
   - Deploy 3 new PD nodes
   - Restore PD metadata from backup
   - Start PD nodes

2. **Restore Store Cluster**:
   - Deploy 3 new Store nodes
   - Extract backup on each node:
     ```bash
     cd storage
     tar -xzf /backups/backup-store1-20250129.tar.gz
     ```

3. **Start Store Nodes**:
   ```bash
   bin/start-hugegraph-store.sh
   ```

4. **Verify Data**:
   ```bash
   # Check via Server
   curl http://192.168.1.30:8080/graphspaces/{graphspaces_name}/graphs/{graph_name}/vertices?limit=10
   ```

#### Scenario 3: Data Corruption

**Impact**: RocksDB corruption on one or more partitions

**Action**:
1. **Identify Corrupted Partition**:
   ```bash
   # Check logs for corruption errors
   tail -f logs/hugegraph-store.log | grep -i corrupt
   ```

2. **Stop Store Node**:
   ```bash
   bin/stop-hugegraph-store.sh
   ```

3. **Delete Corrupted Partition Data**:
   ```bash
   # Assuming partition 5 is corrupted
   rm -rf storage/raft/partition-5
   ```

4. **Restart Store Node**:
   ```bash
   bin/start-hugegraph-store.sh
   ```

5. **Re-replicate Data**:
   - Raft automatically re-replicates from healthy replicas
   - Monitor replication progress:
     ```bash
     tail -f logs/hugegraph-store.log | grep "snapshot install"
     ```

---

## Capacity Management

### Monitoring Capacity

**Disk Usage**:
```bash
# Per Store node
du -sh storage/

# Expected growth rate: Track over weeks
```

**Partition Count**:
```bash
# Current partition count
curl http://192.168.1.10:8620/v1/partitionsAndStatus

# Recommendation: 3-5x Store node count
# Example: 6 Store nodes → 18-30 partitions
```

### Adding Store Nodes

**When to Add**:
- Disk usage >80% on existing nodes
- CPU usage >70% sustained
- Query latency increasing

**Process**:
1. **Deploy New Store Node**:
   ```bash
   # Follow deployment guide
   tar -xzf apache-hugegraph-store-incubating-1.7.0.tar.gz
   cd apache-hugegraph-store-incubating-1.7.0

   # Configure and start
   vi conf/application.yml
   bin/start-hugegraph-store.sh
   ```

2. **Verify Registration**:
   ```bash
   curl http://192.168.1.10:8620/v1/stores
   # New Store should appear
   ```

3. **Trigger Rebalancing** (optional):
   ```bash
   curl -X POST http://192.168.1.10:8620/v1/balanceLeaders
   ```

4. **Monitor Rebalancing**:
   ```bash
   # Watch partition distribution
   watch -n 10 'curl http://192.168.1.10:8620/v1/partitionsAndStatus'
   ```

5. **Verify**: Wait for even distribution (may take hours)

### Removing Store Nodes

**When to Remove**:
- Decommissioning hardware
- Downsizing cluster (off-peak hours)

**Process**:
1. **Mark Store for Removal** (via PD API):
    ```bash
    curl --location --request POST 'http://localhost:8080/store/123' \
    --header 'Content-Type: application/json' \
    --data-raw '{
    "storeState": "Off"
    }'
    ```
   Refer to API definition in `StoreAPI::setStore`

2. **Wait for Migration**:
   - PD migrates all partitions off this Store

3. **Stop Store Node**:
   ```bash
   bin/stop-hugegraph-store.sh
   ```

4. **Remove from PD** (optional):

---

## Rolling Upgrades

### Upgrade Strategy

**Goal**: Upgrade cluster with zero downtime

**Prerequisites**:
- Version compatibility: Check release notes
- Backup: Take full backup before upgrade
- Testing: Test upgrade in staging environment

### Upgrade Procedure

#### Step 1: Upgrade Store Nodes (one at a time)

**Node 1**:
```bash
# Stop Store node
bin/stop-hugegraph-store.sh

# Backup current version
mv apache-hugegraph-store-incubating-1.7.0 apache-hugegraph-store-incubating-1.7.0-backup

# Extract new version
tar -xzf apache-hugegraph-store-incubating-1.8.0.tar.gz
cd apache-hugegraph-store-incubating-1.8.0

# Copy configuration from backup
cp ../apache-hugegraph-store-incubating-1.7.0-backup/conf/application.yml conf/

# Start new version
bin/start-hugegraph-store.sh

# Verify
curl http://192.168.1.20:8520/v1/health
tail -f logs/hugegraph-store.log
```

**Wait 5-10 minutes**, then repeat for Node 2, then Node 3.

#### Step 2: Upgrade PD Nodes (one at a time)

Same process as Store, but upgrade PD cluster first or last (check release notes).

#### Step 3: Upgrade Server Nodes (one at a time)

```bash
# Stop Server
bin/stop-hugegraph.sh

# Upgrade and restart
# (same process as Store)

bin/start-hugegraph.sh
```

### Rollback Procedure

If upgrade fails:

```bash
# Stop new version
bin/stop-hugegraph-store.sh

# Restore backup
rm -rf apache-hugegraph-store-incubating-1.8.0
mv apache-hugegraph-store-incubating-1.7.0-backup apache-hugegraph-store-incubating-1.7.0
cd apache-hugegraph-store-incubating-1.7.0

# Restart old version
bin/start-hugegraph-store.sh
```

---

For performance tuning, see [Best Practices](best-practices.md).

For development and debugging, see [Development Guide](development-guide.md).
