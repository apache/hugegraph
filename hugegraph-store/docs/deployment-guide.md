# Deployment Guide

This guide provides comprehensive instructions for deploying HugeGraph Store in various environments, from development to production clusters.

## Table of Contents

- [Deployment Topologies](#deployment-topologies)
- [Configuration Reference](#configuration-reference)
- [Deployment Steps](#deployment-steps)
- [Docker Deployment](#docker-deployment)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Verification and Testing](#verification-and-testing)

---

## Deployment Topologies

### Topology 1: Minimal Development Setup

**Use Case**: Local development and testing

**Components**:
- 1 PD node (fake-pd mode or real PD)
- 1 Store node
- 1 Server node (optional)

**Configuration**:

**Store Node** (with fake-pd):
```yaml
pdserver:
  address: localhost:8686

grpc:
  host: 127.0.0.1
  port: 8500

raft:
  address: 127.0.0.1:8510

app:
  data-path: ./storage
  fake-pd: true  # Built-in PD mode
```

**Characteristics**:
- ✅ Simple setup, fast startup
- ✅ No external PD cluster required
- ❌ No high availability
- ❌ No data replication
- ❌ Not for production

---

### Topology 2: Small Production Cluster

**Use Case**: Small production deployments, testing environments

**Components**:
- 3 PD nodes
- 3 Store nodes
- 2-3 Server nodes

**Architecture**:
```
┌─────────────────────────────────────────────────┐
│  Client Applications                             │
└──────────────┬──────────────────────────────────┘
               │
        ┌──────┴──────┬──────────────┐
        │             │              │
   ┌────▼────┐   ┌────▼────┐   ┌────▼────┐
   │ Server1 │   │ Server2 │   │ Server3 │
   │ :8080   │   │ :8080   │   │ :8080   │
   └────┬────┘   └────┬────┘   └────┬────┘
        │             │              │
        └──────┬──────┴──────┬───────┘
               │             │
        ┌──────▼─────────────▼──────┐
        │   PD Cluster (3 nodes)    │
        │   192.168.1.10:8686       │
        │   192.168.1.11:8686       │
        │   192.168.1.12:8686       │
        └──────┬────────────────────┘
               │
        ┌──────┴──────┬──────────────┐
        │             │              │
   ┌────▼────┐   ┌────▼────┐   ┌────▼────┐
   │ Store1  │   │ Store2  │   │ Store3  │
   │ :8500   │   │ :8500   │   │ :8500   │
   │ Raft:   │◄──┤ Raft:   │◄──┤ Raft:   │
   │ :8510   │   │ :8510   │   │ :8510   │
   └─────────┘   └─────────┘   └─────────┘
```

**IP Allocation Example**:
- PD: 192.168.1.10-12
- Store: 192.168.1.20-22
- Server: 192.168.1.30-32

**Partition Configuration** (in PD):
```yaml
partition:
  default-shard-count: 3     # 3 replicas per partition
  store-max-shard-count: 12  # Max 12 partitions per Store
```

**Capacity**:
- Data size: Up to 1TB (with proper disk)
- QPS: ~5,000-10,000 queries/second
- Availability: Tolerates 1 node failure per component

**Characteristics**:
- ✅ High availability (HA)
- ✅ Data replication (3 replicas)
- ✅ Automatic failover
- ✅ Production-ready
- ⚠️ Limited horizontal scalability

---

### Topology 3: Medium Production Cluster

**Use Case**: Medium-scale production deployments

**Components**:
- 3 PD nodes
- 6-9 Store nodes
- 3-6 Server nodes

**Architecture**:
```
Load Balancer (Nginx/HAProxy)
        │
   ┌────┴────┬────────┬────────┬────────┐
   │         │        │        │        │
Server1  Server2  Server3  Server4  Server5
   │         │        │        │        │
   └────┬────┴────┬───┴────┬───┴────┬───┘
        │         │        │        │
    PD Cluster (3 nodes)
        │
   ┌────┴────┬────────┬────────┬────────┬────────┐
   │         │        │        │        │        │
Store1   Store2   Store3   Store4   Store5   Store6
  (Rack 1)  (Rack 1) (Rack 2) (Rack 2) (Rack 3) (Rack 3)
```

**Rack-Aware Placement** (configured in PD):
- Distribute replicas across racks for fault isolation
- Each partition has replicas on different racks

**Partition Configuration**:
```yaml
partition:
  default-shard-count: 3        # 3 replicas
  store-max-shard-count: 20     # More partitions per Store
```

**Capacity**:
- Data size: 5-10TB
- QPS: ~20,000-50,000 queries/second
- Availability: Tolerates rack-level failures

**Characteristics**:
- ✅ High availability with rack isolation
- ✅ Better horizontal scalability
- ✅ Higher throughput
- ⚠️ More complex deployment

---

### Topology 4: Large-Scale Cluster

**Use Case**: Large-scale production deployments with high throughput

**Components**:
- 5 PD nodes
- 12+ Store nodes
- 6+ Server nodes

**Architecture**:
```
         Load Balancer Layer
                │
        ┌───────┴───────┐
        │               │
   Server Pool     Server Pool
   (Zone A)        (Zone B)
        │               │
        └───────┬───────┘
                │
         PD Cluster (5 nodes)
         (Multi-Zone)
                │
        ┌───────┴───────────┐
        │                   │
  Store Pool (Zone A)  Store Pool (Zone B)
  6-12 nodes           6-12 nodes
```

**Multi-Zone Deployment**:
- PD: 5 nodes across 2-3 availability zones
- Store: Distributed across zones with zone-aware replica placement
- Server: Load-balanced across zones

**Partition Configuration**:
```yaml
partition:
  default-shard-count: 3
  store-max-shard-count: 30-50  # High partition count for load distribution
```

**Capacity**:
- Data size: 20TB+
- QPS: 100,000+ queries/second
- Availability: Tolerates zone-level failures

**Characteristics**:
- ✅ Maximum availability and scalability
- ✅ Zone-level fault tolerance
- ✅ Elastic scaling
- ⚠️ Complex operational overhead

---

### Topology 5: Co-located Deployment

**Use Case**: Resource optimization, smaller deployments

**Components**:
- 3 nodes, each running: PD + Store + Server

**Architecture**:
```
Node 1 (192.168.1.10)          Node 2 (192.168.1.11)          Node 3 (192.168.1.12)
┌─────────────────────┐        ┌─────────────────────┐        ┌─────────────────────┐
│ Server :8080        │        │ Server :8080        │        │ Server :8080        │
│ PD     :8686, :8620 │        │ PD     :8686, :8620 │        │ PD     :8686, :8620 │
│ Store  :8500, :8510 │        │ Store  :8500, :8510 │        │ Store  :8500, :8510 │
└─────────────────────┘        └─────────────────────┘        └─────────────────────┘
```

**Port Allocation** (per node):
- Server: 8080 (REST), 8182 (Gremlin)
- PD: 8686 (gRPC), 8620 (REST), 8610 (Raft)
- Store: 8500 (gRPC), 8520 (REST), 8510 (Raft)

**Characteristics**:
- ✅ Lower hardware cost (fewer machines)
- ✅ Simplified networking
- ⚠️ Resource contention between components
- ⚠️ Lower fault isolation (node failure affects all components)

**Recommendations**:
- Use for small to medium workloads
- Ensure sufficient CPU (16+ cores) and memory (64GB+) per node
- Use separate disks for Store data and PD metadata

---

## Configuration Reference

### PD Configuration

File: `hugegraph-pd/conf/application.yml`

```yaml
# PD gRPC Server
grpc:
  host: 192.168.1.10          # Bind address (use actual IP)
  port: 8686                  # gRPC port

# PD REST API
server:
  port: 8620

# Raft Configuration
raft:
  address: 192.168.1.10:8610                                   # This PD's Raft address
  peers-list: 192.168.1.10:8610,192.168.1.11:8610,192.168.1.12:8610  # All PD nodes

# PD Data Path
pd:
  data-path: ./pd_data
  initial-store-count: 3      # Min stores before auto-activation
  initial-store-list: 192.168.1.20:8500,192.168.1.21:8500,192.168.1.22:8500  # Auto-activate stores

# Partition Settings
partition:
  default-shard-count: 3      # Replicas per partition
  store-max-shard-count: 20   # Max partitions per Store node

# Store Monitoring
store:
  max-down-time: 172800       # Seconds before marking Store permanently offline (48h)
  monitor_data_enabled: true
  monitor_data_interval: 1 minute
  monitor_data_retention: 7 days
```

### Store Configuration

File: `hugegraph-store/conf/application.yml`

```yaml
# PD Connection
pdserver:
  address: 192.168.1.10:8686,192.168.1.11:8686,192.168.1.12:8686  # PD cluster endpoints

# Store gRPC Server
grpc:
  host: 192.168.1.20                    # Bind address (use actual IP)
  port: 8500                            # gRPC port for client connections
  max-inbound-message-size: 1000MB      # Max request size
  netty-server-max-connection-idle: 3600000  # Connection idle timeout (ms)

# Store REST API
server:
  port: 8520                            # REST API for management/metrics

# Raft Configuration
raft:
  address: 192.168.1.20:8510            # Raft RPC address
  snapshotInterval: 1800                # Snapshot interval (seconds)
  disruptorBufferSize: 1024             # Raft log buffer
  max-log-file-size: 10737418240        # Max log file: 10GB

# Data Storage
app:
  data-path: ./storage                  # Data directory (supports multiple paths: ./storage,/data1,/data2)
  fake-pd: false                        # Use real PD cluster
```

File: `hugegraph-store/conf/application-pd.yml` (RocksDB tuning)

```yaml
rocksdb:
  # Memory Configuration
  total_memory_size: 32000000000        # Total memory for RocksDB (32GB)
  write_buffer_size: 134217728          # Memtable size (128MB)
  max_write_buffer_number: 6            # Max memtables
  min_write_buffer_number_to_merge: 2   # Min memtables to merge

  # Compaction
  level0_file_num_compaction_trigger: 4
  max_background_jobs: 8                # Background compaction/flush threads

  # Block Cache
  block_cache_size: 16000000000         # Block cache (16GB)

  # SST File Size
  target_file_size_base: 134217728      # Target SST size (128MB)
  max_bytes_for_level_base: 1073741824  # L1 size (1GB)
```

### Server Configuration

File: `hugegraph-server/conf/graphs/hugegraph.properties`

```properties
# Backend Type
backend=hstore
serializer=binary

# Store Connection
store.provider=org.apache.hugegraph.backend.store.hstore.HstoreProvider
store.pd_peers=192.168.1.10:8686,192.168.1.11:8686,192.168.1.12:8686

# Connection Pool
store.max_sessions=4
store.session_timeout=30000

# Graph Configuration
graph.name=hugegraph
```

---

## Deployment Steps

### Step 1: Prerequisites

**On all nodes**:

```bash
# Check Java version (11+ required)
java -version

# Check Maven (for building from source)
mvn -version

# Check network connectivity
ping 192.168.1.10
ping 192.168.1.11

# Check available disk space
df -h

# Open required ports (firewall)
# PD: 8620, 8686, 8610
# Store: 8500, 8510, 8520
# Server: 8080, 8182
```

**Disk Recommendations**:
- PD: 50GB+ (for metadata and Raft logs)
- Store: 500GB+ per node (depends on data size)
- Server: 20GB (for logs and temp data)

---

### Step 2: Deploy PD Cluster

**On each PD node**:

```bash
# Extract PD distribution
tar -xzf apache-hugegraph-pd-incubating-1.7.0.tar.gz
cd apache-hugegraph-pd-incubating-1.7.0

# Edit configuration
vi conf/application.yml
# Update grpc.host, raft.address, raft.peers-list
```

**Node 1** (192.168.1.10):
```yaml
grpc:
  host: 192.168.1.10
  port: 8686
raft:
  address: 192.168.1.10:8610
  peers-list: 192.168.1.10:8610,192.168.1.11:8610,192.168.1.12:8610
```

**Node 2** (192.168.1.11):
```yaml
grpc:
  host: 192.168.1.11
  port: 8686
raft:
  address: 192.168.1.11:8610
  peers-list: 192.168.1.10:8610,192.168.1.11:8610,192.168.1.12:8610
```

**Node 3** (192.168.1.12):
```yaml
grpc:
  host: 192.168.1.12
  port: 8686
raft:
  address: 192.168.1.12:8610
  peers-list: 192.168.1.10:8610,192.168.1.11:8610,192.168.1.12:8610
```

**Start PD nodes**:

```bash
# On each PD node
bin/start-hugegraph-pd.sh

# Check logs
tail -f logs/hugegraph-pd.log

# Verify PD is running
curl http://localhost:8620/actuator/health
```

**Verify PD cluster**:

```bash
# Check cluster members
curl http://192.168.1.10:8620/v1/members

# Expected output:
{
  "message":"OK",
  "data":{
    "pdLeader":null,
    "pdList":[{
      "raftUrl":"127.0.0.1:8610",
      "grpcUrl":"",
      "restUrl":"",
      "state":"Offline",
      "dataPath":"",
      "role":"Leader",
      "replicateState":"",
      "serviceName":"-PD",
      "serviceVersion":"1.7.0",
      "startTimeStamp":1761818483830
      }],
    "stateCountMap":{
      "Offline":1
      },
      "numOfService":1,
      "state":"Cluster_OK",
      "numOfNormalService":0
      },
    "status":0
}
```

---

### Step 3: Deploy Store Cluster

**On each Store node**:

```bash
# Extract Store distribution
tar -xzf apache-hugegraph-store-incubating-1.7.0.tar.gz
cd apache-hugegraph-store-incubating-1.7.0

# Edit configuration
vi conf/application.yml
```

**Store Node 1** (192.168.1.20):
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

**Store Node 2** (192.168.1.21):
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

**Store Node 3** (192.168.1.22):
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

**Start Store nodes**:

```bash
# On each Store node
bin/start-hugegraph-store.sh

# Check logs
tail -f logs/hugegraph-store.log

# Verify Store is running
curl http://localhost:8520/v1/health
```

**Verify Store registration with PD**:

```bash
# Query PD for registered stores
curl http://192.168.1.10:8620/v1/stores

# Expected output:
{
  "message":"OK",
  "data":{
    "stores":[{
      "storeId":"1783423547167821026",
      "address":"192.168.1.10:8500",
      "raftAddress":"192.168.1.10:8510",
      "version":"","state":"Up",
      "deployPath":"/Users/user/incubator-hugegraph/hugegraph-store/hg-store-node/target/classes/",
      "dataPath":"./storage",
      "startTimeStamp":1761818547335,
      "registedTimeStamp":1761818547335,
      "lastHeartBeat":1761818727631,
      "capacity":245107195904,
      "available":118497292288,
      "partitionCount":0,
      "graphSize":0,
      "keyCount":0,
      "leaderCount":0,
      "serviceName":"192.168.1.10:8500-store",
      "serviceVersion":"",
      "serviceCreatedTimeStamp":1761818547000,
      "partitions":[]}],
      "stateCountMap":{"Up":1},
      "numOfService":1,
      "numOfNormalService":1
      },
      "status":0
}
```

---

### Step 4: Deploy HugeGraph Server

**On each Server node**:

```bash
# Extract Server distribution
tar -xzf apache-hugegraph-incubating-1.7.0.tar.gz
cd apache-hugegraph-incubating-1.7.0

# Configure backend
vi conf/graphs/hugegraph.properties
```

**Configuration**:
```properties
backend=hstore
serializer=binary

store.provider=org.apache.hugegraph.backend.store.hstore.HstoreProvider
store.pd_peers=192.168.1.10:8686,192.168.1.11:8686,192.168.1.12:8686

store.max_sessions=4
store.session_timeout=30000

graph.name=hugegraph
```

**Initialize and start**:

```bash
# Initialize schema (only needed once)
bin/init-store.sh

# Start Server
bin/start-hugegraph.sh

# Check logs
tail -f logs/hugegraph-server.log

# Verify Server is running
curl http://localhost:8080/versions
```

---

## Docker Deployment

### Docker Compose: Complete Cluster

File: `docker-compose.yml`

```yaml
version: '3.8'

services:
  # PD Cluster (3 nodes)
  pd1:
    image: hugegraph/hugegraph-pd:1.7.0
    container_name: hugegraph-pd1
    ports:
      - "8686:8686"
      - "8620:8620"
      - "8610:8610"
    environment:
      - GRPC_HOST=pd1
      - RAFT_ADDRESS=pd1:8610
      - RAFT_PEERS=pd1:8610,pd2:8610,pd3:8610
    networks:
      - hugegraph-net

  pd2:
    image: hugegraph/hugegraph-pd:1.7.0
    container_name: hugegraph-pd2
    ports:
      - "8687:8686"
    environment:
      - GRPC_HOST=pd2
      - RAFT_ADDRESS=pd2:8610
      - RAFT_PEERS=pd1:8610,pd2:8610,pd3:8610
    networks:
      - hugegraph-net

  pd3:
    image: hugegraph/hugegraph-pd:1.7.0
    container_name: hugegraph-pd3
    ports:
      - "8688:8686"
    environment:
      - GRPC_HOST=pd3
      - RAFT_ADDRESS=pd3:8610
      - RAFT_PEERS=pd1:8610,pd2:8610,pd3:8610
    networks:
      - hugegraph-net

  # Store Cluster (3 nodes)
  store1:
    image: hugegraph/hugegraph-store:1.7.0
    container_name: hugegraph-store1
    ports:
      - "8500:8500"
      - "8510:8510"
      - "8520:8520"
    environment:
      - PD_ADDRESS=pd1:8686,pd2:8686,pd3:8686
      - GRPC_HOST=store1
      - RAFT_ADDRESS=store1:8510
    volumes:
      - store1-data:/hugegraph-store/storage
    depends_on:
      - pd1
      - pd2
      - pd3
    networks:
      - hugegraph-net

  store2:
    image: hugegraph/hugegraph-store:1.7.0
    container_name: hugegraph-store2
    ports:
      - "8501:8500"
    environment:
      - PD_ADDRESS=pd1:8686,pd2:8686,pd3:8686
      - GRPC_HOST=store2
      - RAFT_ADDRESS=store2:8510
    volumes:
      - store2-data:/hugegraph-store/storage
    depends_on:
      - pd1
      - pd2
      - pd3
    networks:
      - hugegraph-net

  store3:
    image: hugegraph/hugegraph-store:1.7.0
    container_name: hugegraph-store3
    ports:
      - "8502:8500"
    environment:
      - PD_ADDRESS=pd1:8686,pd2:8686,pd3:8686
      - GRPC_HOST=store3
      - RAFT_ADDRESS=store3:8510
    volumes:
      - store3-data:/hugegraph-store/storage
    depends_on:
      - pd1
      - pd2
      - pd3
    networks:
      - hugegraph-net

  # Server (2 nodes)
  server1:
    image: hugegraph/hugegraph:1.7.0
    container_name: hugegraph-server1
    ports:
      - "8080:8080"
    environment:
      - BACKEND=hstore
      - PD_PEERS=pd1:8686,pd2:8686,pd3:8686
    depends_on:
      - store1
      - store2
      - store3
    networks:
      - hugegraph-net

  server2:
    image: hugegraph/hugegraph:1.7.0
    container_name: hugegraph-server2
    ports:
      - "8081:8080"
    environment:
      - BACKEND=hstore
      - PD_PEERS=pd1:8686,pd2:8686,pd3:8686
    depends_on:
      - store1
      - store2
      - store3
    networks:
      - hugegraph-net

networks:
  hugegraph-net:
    driver: bridge

volumes:
  store1-data:
  store2-data:
  store3-data:
```

**Deploy**:

```bash
# Start cluster
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f store1

# Stop cluster
docker-compose down
```

---

## Kubernetes Deployment

### StatefulSet: Store Cluster

File: `hugegraph-store-statefulset.yaml`

```yaml
apiVersion: v1
kind: Service
metadata:
  name: hugegraph-store
  labels:
    app: hugegraph-store
spec:
  clusterIP: None  # Headless service
  selector:
    app: hugegraph-store
  ports:
    - name: grpc
      port: 8500
    - name: raft
      port: 8510
    - name: rest
      port: 8520

---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: hugegraph-store
spec:
  serviceName: hugegraph-store
  replicas: 3
  selector:
    matchLabels:
      app: hugegraph-store
  template:
    metadata:
      labels:
        app: hugegraph-store
    spec:
      containers:
      - name: store
        image: hugegraph/hugegraph-store:1.7.0
        ports:
        - containerPort: 8500
          name: grpc
        - containerPort: 8510
          name: raft
        - containerPort: 8520
          name: rest
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: PD_ADDRESS
          value: "hugegraph-pd-0.hugegraph-pd:8686,hugegraph-pd-1.hugegraph-pd:8686,hugegraph-pd-2.hugegraph-pd:8686"
        - name: GRPC_HOST
          value: "$(POD_NAME).hugegraph-store"
        - name: RAFT_ADDRESS
          value: "$(POD_NAME).hugegraph-store:8510"
        volumeMounts:
        - name: data
          mountPath: /hugegraph-store/storage
        resources:
          requests:
            cpu: "2"
            memory: "8Gi"
          limits:
            cpu: "4"
            memory: "16Gi"
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 500Gi
      storageClassName: fast-ssd
```

**Deploy**:

```bash
# Create namespace
kubectl create namespace hugegraph

# Deploy PD cluster (prerequisite)
kubectl apply -f hugegraph-pd-statefulset.yaml -n hugegraph

# Deploy Store cluster
kubectl apply -f hugegraph-store-statefulset.yaml -n hugegraph

# Check pods
kubectl get pods -n hugegraph

# Check Store logs
kubectl logs -f hugegraph-store-0 -n hugegraph

# Access Store service
kubectl port-forward svc/hugegraph-store 8500:8500 -n hugegraph
```

---

## Verification and Testing

### Health Check

```bash
# PD health
curl http://192.168.1.10:8620/v1/health

# Store health
curl http://192.168.1.20:8520/v1/health
```

### Cluster Status

```bash
# PD cluster members
curl http://192.168.1.10:8620/v1/members

# Registered stores
curl http://192.168.1.10:8620/v1/stores

# Partitions
curl http://192.168.1.10:8620/v1/partitions

# Graph list
curl http://192.168.1.10:8620/v1/graphs
```

### Basic Operations Test

```bash
# Create vertex via Server
curl -X POST "http://192.168.1.30:8080/graphspaces/{graphspace_name}/graphs/{graph_name}/graph/vertices" \
     -H "Content-Type: application/json" \
     -d '{
         "label": "person",
         "properties": {
             "name": "marko",
             "age": 29
         }
     }'

# Query vertex (using -u if auth is enabled)
curl -u admin:admin \
     -X GET "http://localhost:8080/graphspaces/{graphspace_name}/graphs/graphspace_name}/graph/vertices/{graph_id}
```

### Performance Baseline Test

```bash
# Install HugeGraph-Loader (for bulk loading)
tar -xzf apache-hugegraph-loader-1.7.0.tar.gz

# Run benchmark
bin/hugegraph-loader.sh -g hugegraph -f ./example/struct.json -s ./example/schema.groovy
```

For production monitoring and troubleshooting, see [Operations Guide](operations-guide.md).

For performance tuning, see [Best Practices](best-practices.md).
