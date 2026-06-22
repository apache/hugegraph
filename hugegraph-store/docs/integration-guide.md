# Integration Guide

This guide explains how to integrate HugeGraph Store with HugeGraph Server, use the client library, and migrate from other storage backends.

## Table of Contents

- [Backend Configuration](#backend-configuration)
- [Client Library Usage](#client-library-usage)
- [Integration with PD](#integration-with-pd)
- [Migration from Other Backends](#migration-from-other-backends)
- [Multi-Graph Configuration](#multi-graph-configuration)
- [Troubleshooting Integration Issues](#troubleshooting-integration-issues)

---

## Backend Configuration

### Configuring HugeGraph Server to Use Store

HugeGraph Store is configured as a pluggable backend in HugeGraph Server.

#### Step 1: Edit Graph Configuration

File: `hugegraph-server/conf/graphs/<graph-name>.properties`

**Basic Configuration**:
```properties
# Backend type
backend=hstore
serializer=binary

# Store provider class
store.provider=org.apache.hugegraph.backend.store.hstore.HstoreProvider

# PD cluster endpoints (required)
store.pd_peers=192.168.1.10:8686,192.168.1.11:8686,192.168.1.12:8686

# Connection pool
store.max_sessions=4
store.session_timeout=30000

# Graph name
graph.name=hugegraph
```

**Advanced Configuration**:
```properties
# gRPC settings
store.grpc_max_inbound_message_size=104857600  # 100MB

# Retry settings
store.max_retries=3
store.retry_interval=1000  # milliseconds

# Batch settings
store.batch_size=500

# Timeout settings
store.rpc_timeout=30000  # RPC timeout in milliseconds
```

#### Step 2: Initialize Schema

```bash
cd hugegraph-server

# Initialize backend storage (creates system schema)
bin/init-store.sh

# Expected output:
# Initializing HugeGraph Store backend...
# Connecting to PD: 192.168.1.10:8686,192.168.1.11:8686,192.168.1.12:8686
# Creating system tables...
# Initialization completed successfully
```

**What happens during initialization**:
1. Server connects to PD cluster
2. PD provides Store node addresses
3. Server creates system schema (internal metadata tables)
4. Server creates graph-specific schema tables

#### Step 3: Start HugeGraph Server

```bash
# Start server
bin/start-hugegraph.sh

# Check logs
tail -f logs/hugegraph-server.log

# Look for successful backend initialization:
# INFO  o.a.h.b.s.h.HstoreProvider - HStore backend initialized successfully
# INFO  o.a.h.b.s.h.HstoreProvider - Connected to PD: 192.168.1.10:8686
# INFO  o.a.h.b.s.h.HstoreProvider - Discovered 3 Store nodes
```

#### Step 4: Verify Backend

```bash
# Check backend via REST API
curl --location --request GET 'http://localhost:8080/metrics/backend' \
--header 'Authorization: Bearer <YOUR_ACCESS_TOKEN>'
# Response should show:
# {"backend": "hstore", "nodes": [...]}
```

---

## Client Library Usage

The `hg-store-client` module provides a Java client for directly interacting with Store clusters (typically used by HugeGraph Server, but can be used standalone).

### Maven Dependency

```xml
<dependency>
    <groupId>org.apache.hugegraph</groupId>
    <artifactId>hugegraph-client</artifactId>
    <version>1.7.0</version>
</dependency>
```

### Basic Usage

#### 1. Single Example

```java
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hugegraph.driver.GraphManager;
import org.apache.hugegraph.driver.GremlinManager;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.driver.SchemaManager;
import org.apache.hugegraph.structure.constant.T;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Path;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.structure.gremlin.Result;
import org.apache.hugegraph.structure.gremlin.ResultSet;

public class SingleExample {

    public static void main(String[] args) throws IOException {
        // If connect failed will throw a exception.
        HugeClient hugeClient = HugeClient.builder("http://localhost:8080",
                        "hugegraph")
                .build();

        SchemaManager schema = hugeClient.schema();

        schema.propertyKey("name").asText().ifNotExist().create();
        schema.propertyKey("age").asInt().ifNotExist().create();
        schema.propertyKey("city").asText().ifNotExist().create();
        schema.propertyKey("weight").asDouble().ifNotExist().create();
        schema.propertyKey("lang").asText().ifNotExist().create();
        schema.propertyKey("date").asDate().ifNotExist().create();
        schema.propertyKey("price").asInt().ifNotExist().create();

        schema.vertexLabel("person")
                .properties("name", "age", "city")
                .primaryKeys("name")
                .ifNotExist()
                .create();

        schema.vertexLabel("software")
                .properties("name", "lang", "price")
                .primaryKeys("name")
                .ifNotExist()
                .create();

        schema.indexLabel("personByCity")
                .onV("person")
                .by("city")
                .secondary()
                .ifNotExist()
                .create();

        schema.indexLabel("personByAgeAndCity")
                .onV("person")
                .by("age", "city")
                .secondary()
                .ifNotExist()
                .create();

        schema.indexLabel("softwareByPrice")
                .onV("software")
                .by("price")
                .range()
                .ifNotExist()
                .create();

        schema.edgeLabel("knows")
                .sourceLabel("person")
                .targetLabel("person")
                .properties("date", "weight")
                .ifNotExist()
                .create();

        schema.edgeLabel("created")
                .sourceLabel("person").targetLabel("software")
                .properties("date", "weight")
                .ifNotExist()
                .create();

        schema.indexLabel("createdByDate")
                .onE("created")
                .by("date")
                .secondary()
                .ifNotExist()
                .create();

        schema.indexLabel("createdByWeight")
                .onE("created")
                .by("weight")
                .range()
                .ifNotExist()
                .create();

        schema.indexLabel("knowsByWeight")
                .onE("knows")
                .by("weight")
                .range()
                .ifNotExist()
                .create();

        GraphManager graph = hugeClient.graph();
        Vertex marko = graph.addVertex(T.LABEL, "person", "name", "marko",
                "age", 29, "city", "Beijing");
        Vertex vadas = graph.addVertex(T.LABEL, "person", "name", "vadas",
                "age", 27, "city", "Hongkong");
        Vertex lop = graph.addVertex(T.LABEL, "software", "name", "lop",
                "lang", "java", "price", 328);
        Vertex josh = graph.addVertex(T.LABEL, "person", "name", "josh",
                "age", 32, "city", "Beijing");
        Vertex ripple = graph.addVertex(T.LABEL, "software", "name", "ripple",
                "lang", "java", "price", 199);
        Vertex peter = graph.addVertex(T.LABEL, "person", "name", "peter",
                "age", 35, "city", "Shanghai");

        marko.addEdge("knows", vadas, "date", "2016-01-10", "weight", 0.5);
        marko.addEdge("knows", josh, "date", "2013-02-20", "weight", 1.0);
        marko.addEdge("created", lop, "date", "2017-12-10", "weight", 0.4);
        josh.addEdge("created", lop, "date", "2009-11-11", "weight", 0.4);
        josh.addEdge("created", ripple, "date", "2017-12-10", "weight", 1.0);
        peter.addEdge("created", lop, "date", "2017-03-24", "weight", 0.2);

        GremlinManager gremlin = hugeClient.gremlin();
        System.out.println("==== Path ====");
        ResultSet resultSet = gremlin.gremlin("g.V().outE().path()").execute();
        Iterator<Result> results = resultSet.iterator();
        results.forEachRemaining(result -> {
            System.out.println(result.getObject().getClass());
            Object object = result.getObject();
            if (object instanceof Vertex) {
                System.out.println(((Vertex) object).id());
            } else if (object instanceof Edge) {
                System.out.println(((Edge) object).id());
            } else if (object instanceof Path) {
                List<Object> elements = ((Path) object).objects();
                elements.forEach(element -> {
                    System.out.println(element.getClass());
                    System.out.println(element);
                });
            } else {
                System.out.println(object);
            }
        });

        hugeClient.close();
    }
}

```

#### 2. Batch Example

```java
import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.driver.GraphManager;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.driver.SchemaManager;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Vertex;

public class BatchExample {

    public static void main(String[] args) {
        // If connect failed will throw a exception.
        HugeClient hugeClient = HugeClient.builder("http://localhost:8080",
                                                   "hugegraph")
                                          .build();

        SchemaManager schema = hugeClient.schema();

        schema.propertyKey("name").asText().ifNotExist().create();
        schema.propertyKey("age").asInt().ifNotExist().create();
        schema.propertyKey("lang").asText().ifNotExist().create();
        schema.propertyKey("date").asDate().ifNotExist().create();
        schema.propertyKey("price").asInt().ifNotExist().create();

        schema.vertexLabel("person")
              .properties("name", "age")
              .primaryKeys("name")
              .ifNotExist()
              .create();

        schema.vertexLabel("person")
              .properties("price")
              .nullableKeys("price")
              .append();

        schema.vertexLabel("software")
              .properties("name", "lang", "price")
              .primaryKeys("name")
              .ifNotExist()
              .create();

        schema.indexLabel("softwareByPrice")
              .onV("software").by("price")
              .range()
              .ifNotExist()
              .create();

        schema.edgeLabel("knows")
              .link("person", "person")
              .properties("date")
              .ifNotExist()
              .create();

        schema.edgeLabel("created")
              .link("person", "software")
              .properties("date")
              .ifNotExist()
              .create();

        schema.indexLabel("createdByDate")
              .onE("created").by("date")
              .secondary()
              .ifNotExist()
              .create();

        // get schema object by name
        System.out.println(schema.getPropertyKey("name"));
        System.out.println(schema.getVertexLabel("person"));
        System.out.println(schema.getEdgeLabel("knows"));
        System.out.println(schema.getIndexLabel("createdByDate"));

        // list all schema objects
        System.out.println(schema.getPropertyKeys());
        System.out.println(schema.getVertexLabels());
        System.out.println(schema.getEdgeLabels());
        System.out.println(schema.getIndexLabels());

        GraphManager graph = hugeClient.graph();

        Vertex marko = new Vertex("person").property("name", "marko")
                                           .property("age", 29);
        Vertex vadas = new Vertex("person").property("name", "vadas")
                                           .property("age", 27);
        Vertex lop = new Vertex("software").property("name", "lop")
                                           .property("lang", "java")
                                           .property("price", 328);
        Vertex josh = new Vertex("person").property("name", "josh")
                                          .property("age", 32);
        Vertex ripple = new Vertex("software").property("name", "ripple")
                                              .property("lang", "java")
                                              .property("price", 199);
        Vertex peter = new Vertex("person").property("name", "peter")
                                           .property("age", 35);

        Edge markoKnowsVadas = new Edge("knows").source(marko).target(vadas)
                                                .property("date", "2016-01-10");
        Edge markoKnowsJosh = new Edge("knows").source(marko).target(josh)
                                               .property("date", "2013-02-20");
        Edge markoCreateLop = new Edge("created").source(marko).target(lop)
                                                 .property("date",
                                                           "2017-12-10");
        Edge joshCreateRipple = new Edge("created").source(josh).target(ripple)
                                                   .property("date",
                                                             "2017-12-10");
        Edge joshCreateLop = new Edge("created").source(josh).target(lop)
                                                .property("date", "2009-11-11");
        Edge peterCreateLop = new Edge("created").source(peter).target(lop)
                                                 .property("date",
                                                           "2017-03-24");

        List<Vertex> vertices = new ArrayList<>();
        vertices.add(marko);
        vertices.add(vadas);
        vertices.add(lop);
        vertices.add(josh);
        vertices.add(ripple);
        vertices.add(peter);

        List<Edge> edges = new ArrayList<>();
        edges.add(markoKnowsVadas);
        edges.add(markoKnowsJosh);
        edges.add(markoCreateLop);
        edges.add(joshCreateRipple);
        edges.add(joshCreateLop);
        edges.add(peterCreateLop);

        vertices = graph.addVertices(vertices);
        vertices.forEach(vertex -> System.out.println(vertex));

        edges = graph.addEdges(edges, false);
        edges.forEach(edge -> System.out.println(edge));

        hugeClient.close();
    }
}
```

---

## Integration with PD

### Service Discovery Flow

```
1. Server/Client starts with PD addresses
   ↓
2. Connect to PD cluster (try each peer until success)
   ↓
3. Query PD for Store node list
   ↓
4. PD returns Store nodes and their addresses
   ↓
5. Client establishes gRPC connections to Store nodes
   ↓
6. Client queries PD for partition metadata
   ↓
7. Client caches partition → Store mapping
   ↓
8. For each operation:
   - Hash key to determine partition
   - Look up partition's leader Store
   - Send request to leader Store
```

## Migration from Other Backends

### RocksDB Embedded to Store

**Use Case**: Migrating from single-node RocksDB backend to distributed Store

#### Step 1: Backup Existing Data

```bash
# Using HugeGraph-Tools (Backup & Restore)
cd hugegraph-tools

# Backup graph data
bin/hugegraph-backup.sh \
  --graph hugegraph \
  --directory /backup/hugegraph-20250129 \
  --format json

# Backup completes, creates:
# /backup/hugegraph-20250129/
#   ├── schema.json
#   ├── vertices.json
#   └── edges.json
```

#### Step 2: Deploy Store Cluster

Follow [Deployment Guide](deployment-guide.md) to deploy PD and Store clusters.

#### Step 3: Configure Server for Store Backend

Edit `conf/graphs/hugegraph.properties`:

```properties
# Change from:
# backend=rocksdb

# To:
backend=hstore
store.provider=org.apache.hugegraph.backend.store.hstore.HstoreProvider
store.pd_peers=192.168.1.10:8686,192.168.1.11:8686,192.168.1.12:8686
```

#### Step 4: Initialize Store Backend

```bash
# Initialize Store backend (creates schema)
bin/init-store.sh
```

#### Step 5: Restore Data

```bash
# Restore data to Store backend
cd hugegraph-tools

bin/hugegraph-restore.sh \
  --graph hugegraph \
  --directory /backup/hugegraph-20250129 \
  --format json

# Restore progress:
# Restoring schema... (100%)
# Restoring vertices... (1,000,000 vertices)
# Restoring edges... (5,000,000 edges)
# Restore completed successfully
```

#### Step 6: Verify Migration

```bash
# Check vertex count
curl http://localhost:8080/graphspaces/{graphspace_name}/graphs/{graph_name}/graph/vertices

# Check edge count
curl http://localhost:8080/graphspaces/{graphspace_name}/graphs/{graph_name}/graph/edges

# Run sample queries
curl http://localhost:8080/graphspaces/{graphspace_name}/graphs/{graph_name}/graph/vertices/{id}
```

---

### MySQL/PostgreSQL to Store

**Use Case**: Migrating from relational database backends

#### Option 1: Using Backup & Restore (Recommended)

Same steps as RocksDB migration above.

#### Option 2: Using HugeGraph-Loader (For ETL)

If you need to transform data during migration:

```bash
# 1. Export data from MySQL backend
# (Use mysqldump or HugeGraph API)

# 2. Create loader config
cat > load_config.json <<EOF
{
  "vertices": [
    {
      "label": "person",
      "input": {
        "type": "file",
        "path": "vertices_person.csv"
      },
      "mapping": {
        "id": "id",
        "name": "name",
        "age": "age"
      }
    }
  ],
  "edges": [
    {
      "label": "knows",
      "source": ["person_id"],
      "target": ["friend_id"],
      "input": {
        "type": "file",
        "path": "edges_knows.csv"
      }
    }
  ]
}
EOF

# 3. Load data into Store backend
cd hugegraph-loader
bin/hugegraph-loader.sh -g hugegraph -f load_config.json
```

---

### Cassandra/HBase to Store

**Use Case**: Migrating from legacy distributed backends

**Recommended Approach**: Backup & Restore

1. **Backup from old backend**:
   ```bash
   # With old backend configured
   bin/hugegraph-backup.sh --graph hugegraph --directory /backup/data
   ```

2. **Switch to Store backend** (reconfigure Server)

3. **Restore to Store**:
   ```bash
   # With Store backend configured
   bin/hugegraph-restore.sh --graph hugegraph --directory /backup/data
   ```

**Estimated Time**:
- 1 million vertices + 5 million edges: ~10-30 minutes
- 10 million vertices + 50 million edges: ~1-3 hours
- 100 million vertices + 500 million edges: ~10-30 hours

**Performance Tips**:
- Use `--batch-size 1000` for faster loading
- Run restore on a Server instance close to Store nodes (low latency)
- Temporarily increase `store.batch_size` during migration

---

## Multi-Graph Configuration

HugeGraph supports multiple graphs with different backends or configurations.

### Example: Multiple Graphs with Store

**Graph 1**: Main production graph
```properties
# conf/graphs/production.properties
backend=hstore
store.provider=org.apache.hugegraph.backend.store.hstore.HstoreProvider
store.pd_peers=192.168.1.10:8686,192.168.1.11:8686,192.168.1.12:8686
graph.name=production
```

**Graph 2**: Analytics graph
```properties
# conf/graphs/analytics.properties
backend=hstore
store.provider=org.apache.hugegraph.backend.store.hstore.HstoreProvider
store.pd_peers=192.168.1.10:8686,192.168.1.11:8686,192.168.1.12:8686
graph.name=analytics
```

**Access**:
```bash
# Production graph
curl "http://192.168.1.30:8080/graphspaces/{graphspace_name}/graphs/production/graph/vertices" 

# Analytics graph
curl "http://192.168.1.30:8080/graphspaces/{graphspace_name}/graphs/analytics/graph/vertices" 
```

### Mixed Backend Configuration

**Graph 1**: Store backend (distributed)
```properties
# conf/graphs/main.properties
backend=hstore
store.pd_peers=192.168.1.10:8686
graph.name=main
```

**Graph 2**: RocksDB backend (local)
```properties
# conf/graphs/local.properties
backend=rocksdb
rocksdb.data_path=./rocksdb-data
graph.name=local
```

---

## Troubleshooting Integration Issues

### Issue 1: Server Cannot Connect to PD

**Symptoms**:
```
ERROR o.a.h.b.s.h.HstoreProvider - Failed to connect to PD cluster
```

**Diagnosis**:
```bash
# Check PD is running
curl http://192.168.1.10:8620/v1/health

# Check network connectivity
telnet 192.168.1.10 8686

# Check Server logs
tail -f logs/hugegraph-server.log | grep PD
```

**Solutions**:
1. Verify `store.pd_peers` addresses are correct
2. Ensure PD cluster is running and accessible
3. Check firewall rules (port 8686 must be open)
4. Try connecting to each PD peer individually

---

### Issue 2: Slow Query Performance

**Symptoms**:
- Queries take >5 seconds
- High latency in Server logs

**Diagnosis**:
```bash
# Check Store node health
curl http://192.168.1.20:8520/v1/health

# Check partition distribution
curl http://192.168.1.10:8620/v1/partitions

# Check if queries are using indexes
# (Enable query logging in Server)
```

**Solutions**:
1. **Create indexes**: Ensure label and property indexes exist
2. **Increase Store nodes**: If data exceeds capacity of 3 nodes
3. **Tune RocksDB**: See [Best Practices](best-practices.md)
4. **Enable query pushdown**: Ensure Server is using Store's query API

---

### Issue 3: Write Failures

**Symptoms**:
```
ERROR o.a.h.b.s.h.HstoreSession - Write operation failed: Raft leader not found
```

**Diagnosis**:
```bash
# Check Store logs for Raft errors
tail -f logs/hugegraph-store.log | grep Raft

# Check partition leaders
curl http://192.168.1.10:8620/v1/partitions | grep leader

# Check Store node states
curl http://192.168.1.10:8620/v1/stores
```

**Solutions**:
1. **Wait for leader election**: If recent failover, wait 10-30 seconds
2. **Check Store node health**: Ensure all Store nodes are online
3. **Check disk space**: Ensure Store nodes have sufficient disk
4. **Restart affected Store node**: If Raft is stuck

---

### Issue 4: Data Inconsistency After Migration

**Symptoms**:
- Vertex/edge counts don't match
- Some data missing after restore

**Diagnosis**:
```bash
# Compare counts
curl http://localhost:8080/graphspaces/{graphspace_name}/graphs/{graph_name}/graph/vertices
# vs expected count from backup

# Check for restore errors
tail -f logs/hugegraph-tools.log | grep ERROR
```

**Solutions**:
1. **Re-run restore**: Delete graph and restore again
   ```bash
   # Clear graph
   curl -X DELETE http://localhost:8080/graphspaces/{graphspace_name}/graphs/{graph_name}/graph/vertices/{id}

   # Restore
   bin/hugegraph-restore.sh --graph hugegraph --directory /backup/data
   ```

2. **Verify backup integrity**: Check backup files are complete
3. **Increase timeout**: If restore timed out, increase `store.rpc_timeout`

---

### Issue 5: Memory Leaks in Client

**Symptoms**:
- Server memory grows over time
- OutOfMemoryError after running for hours

**Diagnosis**:
```bash
# Monitor Server memory
jstat -gc <server-pid> 1000

# Heap dump analysis
jmap -dump:format=b,file=heap.bin <server-pid>
```

**Solutions**:
1. **Close sessions**: Ensure `HgStoreSession.close()` is called
2. **Tune connection pool**: Reduce `store.max_sessions` if too high
3. **Increase heap**: Increase Server JVM heap size
   ```bash
   # In start-hugegraph.sh
   JAVA_OPTS="-Xms4g -Xmx8g"
   ```

---

For operational monitoring and troubleshooting, see [Operations Guide](operations-guide.md).

For performance optimization, see [Best Practices](best-practices.md).
