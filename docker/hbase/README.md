# HBase Backend Testing with Docker

This guide explains how to start HBase locally with Docker, verify it is working, and validate HugeGraph API operations.

> **All commands in this guide should be run from the repository root** unless otherwise noted.
> **Security note**: The HBase Docker build enforces SHA512 verification by default and fails when checksum download/parsing/validation fails. Only use `--build-arg ALLOW_UNVERIFIED_DOWNLOAD=true` for trusted test environments with restricted networks.

---

## Quick Start

### 0. (Optional) Build the HBase Docker Image
```bash
docker compose -f docker/hbase/docker-compose.hbase.yml build --no-cache hbase
```

### 1. Start HBase with Docker

```bash
docker compose -f docker/hbase/docker-compose.hbase.yml up -d
```

### 2. Wait for HBase to be Ready (~2 minutes)

```bash
# Check ZooKeeper connectivity
nc -z localhost 2181 && echo "Ready" || echo "Not ready"

# Or watch the logs
docker compose -f docker/hbase/docker-compose.hbase.yml logs
```

### 3. (Optional) Clean Up Leftover HBase Tables

For reruns, drop any leftover HugeGraph tables after the container is up:

```bash
docker exec hg-hbase-test bash -c '
  for t in $(echo "list" | hbase shell -n 2>/dev/null | grep "^default_hugegraph"); do
    echo "disable '"'"'$t'"'"'; drop '"'"'$t'"'"'"
  done | hbase shell
'
```

Verify tables are gone before proceeding:

```bash
docker exec hg-hbase-test bash -lc "echo 'list' | hbase shell -n"
# Expected: TABLE (empty), 0 row(s)
```


### 4. Configure and Init the HugeGraph Server (required for API tests)

> This step is only needed for HugeGraph API sanity checks.

> **Prerequisite**: Run `mvn clean package -DskipTests` from the repository root to generate the distribution. This creates an `apache-hugegraph-<version>/` directory with all necessary binaries and configs.

Set backend to HBase in the server config:

```bash
SERVER_DIR="$(find . -maxdepth 3 -type d -path './apache-hugegraph-*/apache-hugegraph-server-*' | head -n 1)"
SERVER_DIR="${SERVER_DIR#./}"
[ -n "$SERVER_DIR" ] || { echo "HugeGraph server runtime not found. Run mvn clean package -DskipTests first."; exit 1; }
CONF="$SERVER_DIR/conf/graphs/hugegraph.properties"

# Switch backend to hbase
perl -pi -e 's/^backend=.*/backend=hbase/'     "$CONF"
perl -pi -e 's/^serializer=.*/serializer=hbase/' "$CONF"

# Uncomment HBase connection settings
perl -pi -e 's/^#(hbase\.hosts=.*)/$1/'        "$CONF"
perl -pi -e 's/^#(hbase\.port=.*)/$1/'         "$CONF"
perl -pi -e 's/^#(hbase\.znode_parent=.*)/$1/' "$CONF"
```

Initialize HBase tables and start the server:

```bash
printf 'pa\npa\n' | "$SERVER_DIR/bin/init-store.sh"
"$SERVER_DIR/bin/start-hugegraph.sh" -t 60
```

After `init-store.sh`, you can verify the tables were created:

```bash
docker exec hg-hbase-test bash -lc "echo 'list' | hbase shell -n"
```

---

## Docker Compose Services

### HBase Container

- **Image**: `hugegraph/hbase:2.6.5`
- **Container Name**: `hg-hbase-test`
- **Hostname**: `hbase`
- **Ports**:
  - `2181` - ZooKeeper (embedded)
  - `16000` - HBase Master RPC
  - `16010` - HBase Master Web UI (http://localhost:16010)
  - `16020` - HBase RegionServer RPC
  - `16030` - HBase RegionServer Web UI (http://localhost:16030)
- **Health Check**: ZooKeeper connectivity on port 2181
- **Startup Time**: ~90-120 seconds

---

## Manual Verification

### 1. Check Container is Healthy

```bash
docker compose -f docker/hbase/docker-compose.hbase.yml ps
docker logs hg-hbase-test | tail -50
```

### 2. Check ZooKeeper Connectivity

```bash
# From host machine
nc -z localhost 2181 && echo "ZooKeeper OK" || echo "ZooKeeper not ready"

# From inside the container
docker exec hg-hbase-test nc -z localhost 2181 && echo "Ready" || echo "Not ready"
```

### 3. Check HBase Master and RegionServer Web UIs

```bash
# HBase Master Web UI (should return HTML)
curl -s http://localhost:16010 | head -20

# RegionServer Web UI
curl -s http://localhost:16030 | head -20

# Or open in browser
open http://localhost:16010
```

### 4. Verify HBase Tables via Shell

```bash
# List all tables (should show HugeGraph tables after init-store)
docker exec hg-hbase-test bash -lc "echo 'list' | hbase shell -n"

# Check a specific table exists (example: after backend init)
docker exec hg-hbase-test bash -lc 'echo "describe '"'"'default_hugegraph:g_v'"'"'" | hbase shell -n'
```

### 5. Verify HBase Logs for Errors

```bash
# Check for any ERROR lines in HBase logs
docker exec hg-hbase-test bash -lc "grep -i error /opt/hbase/logs/*.log | tail -20"

# Tail live logs (run from repo root)
docker compose -f docker/hbase/docker-compose.hbase.yml logs
```

> **Known benign messages** — these are safe to ignore in standalone mode:
> - `SASL config status: Will not attempt to authenticate using SASL (unknown error)` — ZooKeeper SASL is not configured; standalone HBase does not need it.
> - `Invalid configuration, only one server specified (ignoring)` — expected when running a single-node ZooKeeper.
> - `NoClassDefFoundError: org/eclipse/jetty/...` — Jetty UI dependency missing in the container; does not affect HBase or ZooKeeper functionality.

---

## Manual API Sanity (curl)

These steps assume the HugeGraph server is running at `http://localhost:8080` with auth enabled (`admin/pa`).

> **Note on Idempotency**: Schema creation calls below use `"check_exist": false`, which skips strict "already exists" checks for matching schema definitions. If a re-submitted schema conflicts with an existing definition, HugeGraph can still return an error.
>
> **Prerequisite**: The HBase backend tables must be initialized before any API calls will work. If you see `TableNotFoundException` errors, re-run `init-store.sh` (see Step 0 below or the Quick Start section).

### 0. Initialize Backend and Start Server

> **Skip this if the server is already running.** This step is required the first time or after a full cleanup.
>
> **Prerequisite**: Run `mvn clean package -DskipTests` from the repository root first to generate the distribution.

```bash
SERVER_DIR="$(find . -maxdepth 3 -type d -path './apache-hugegraph-*/apache-hugegraph-server-*' | head -n 1)"
SERVER_DIR="${SERVER_DIR#./}"
[ -n "$SERVER_DIR" ] || { echo "HugeGraph server runtime not found. Run mvn clean package -DskipTests first."; exit 1; }

# Initialize HBase tables (enter password 'pa' when prompted)
printf 'pa\npa\n' | "$SERVER_DIR/bin/init-store.sh"

# Start the server (wait up to 60s for startup)
"$SERVER_DIR/bin/start-hugegraph.sh" -t 60
```

Verify the server is up before continuing:

### 1. Check Server is Up

```bash
curl -s http://localhost:8080/versions | python3 -m json.tool
```

### 2. List Graphs

```bash
curl -s -u admin:pa http://localhost:8080/graphs | python3 -m json.tool
```

### 3. Create Property Keys

Create multiple property keys for testing. Re-running with the same schema returns the existing definition.

```bash
# Text property
curl -s -u admin:pa -X POST \
  http://localhost:8080/graphs/hugegraph/schema/propertykeys \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "email",
    "data_type": "TEXT",
    "cardinality": "SINGLE",
    "check_exist": false
  }' | python3 -m json.tool

# Numeric property
curl -s -u admin:pa -X POST \
  http://localhost:8080/graphs/hugegraph/schema/propertykeys \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "age",
    "data_type": "INT",
    "cardinality": "SINGLE",
    "check_exist": false
  }' | python3 -m json.tool
```

### 4. Create a Vertex Label

```bash
curl -s -u admin:pa -X POST \
  http://localhost:8080/graphs/hugegraph/schema/vertexlabels \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "user",
    "id_strategy": "PRIMARY_KEY",
    "primary_keys": ["email"],
    "properties": ["email", "age"],
    "check_exist": false
  }' | python3 -m json.tool
```

### 5. Add Vertices

```bash
# Add first vertex
curl -s -u admin:pa -X POST \
  http://localhost:8080/graphs/hugegraph/graph/vertices \
  -H 'Content-Type: application/json' \
  -d '{
    "label": "user",
    "properties": {"email": "alice@example.com", "age": 30}
  }' | python3 -m json.tool

# Add second vertex
curl -s -u admin:pa -X POST \
  http://localhost:8080/graphs/hugegraph/graph/vertices \
  -H 'Content-Type: application/json' \
  -d '{
    "label": "user",
    "properties": {"email": "bob@example.com", "age": 25}
  }' | python3 -m json.tool
```

### 6. List Vertices

```bash
curl -s --compressed -u admin:pa \
  "http://localhost:8080/graphs/hugegraph/graph/vertices" \
  | python3 -m json.tool
```

### 7. Run a Gremlin Query

```bash
curl -s --compressed -u admin:pa -X POST \
  http://localhost:8080/gremlin \
  -H 'Content-Type: application/json' \
  -d '{
    "gremlin": "g.V().limit(5)",
    "bindings": {},
    "language": "gremlin-groovy",
    "aliases": {
      "g": "__g_DEFAULT-hugegraph"
    }
  }' | python3 -m json.tool
```

---

## Troubleshooting

### "The port 8182 has already been used" on Startup

Port 8182 (Gremlin WebSocket) is held by a stale Java process from a previous server run. The pid file may be missing so `stop-hugegraph.sh` won't find it.

```bash
# Find the process holding port 8182
lsof -i :8182

SERVER_DIR="$(find . -maxdepth 3 -type d -path './apache-hugegraph-*/apache-hugegraph-server-*' | head -n 1)"
SERVER_DIR="${SERVER_DIR#./}"
[ -n "$SERVER_DIR" ] || { echo "HugeGraph server runtime not found. Run mvn clean package -DskipTests first."; exit 1; }

# Graceful stop (works if pid file exists)
"$SERVER_DIR/bin/stop-hugegraph.sh"

# If still running, kill by PID from lsof output above
kill <PID>

# Verify port is free before restarting
lsof -i :8182 || echo "Port 8182 is free"

# Now start the server
"$SERVER_DIR/bin/start-hugegraph.sh" -t 60
```

### API Returns `TableNotFoundException`

If you see `org.apache.hadoop.hbase.TableNotFoundException` when calling schema or graph APIs, the HBase backend tables have not been initialized (or were dropped). Re-run `init-store.sh`:

```bash
SERVER_DIR="$(find . -maxdepth 3 -type d -path './apache-hugegraph-*/apache-hugegraph-server-*' | head -n 1)"
SERVER_DIR="${SERVER_DIR#./}"
[ -n "$SERVER_DIR" ] || { echo "HugeGraph server runtime not found. Run mvn clean package -DskipTests first."; exit 1; }
"$SERVER_DIR/bin/stop-hugegraph.sh"
printf 'pa\npa\n' | "$SERVER_DIR/bin/init-store.sh"
"$SERVER_DIR/bin/start-hugegraph.sh" -t 60
```

### HBase Container Fails to Start

```bash
# Check container status and logs
docker compose -f docker/hbase/docker-compose.hbase.yml ps
docker compose -f docker/hbase/docker-compose.hbase.yml logs --tail 50 hbase
docker inspect hg-hbase-test | grep -A 5 "State"
```

**Common causes**:

1. **Port conflict** (port 2181 already in use)
   ```bash
   lsof -i :2181
   # Kill the process or change the port mapping in docker/hbase/docker-compose.hbase.yml
   ```

2. **Insufficient memory** — Docker Desktop: Settings → Resources → Memory → set to at least 4 GB

3. **Stale ZooKeeper data**
   ```bash
   docker compose -f docker/hbase/docker-compose.hbase.yml down -v
   docker compose -f docker/hbase/docker-compose.hbase.yml up -d
   ```

### Memory Issues During Build or Setup

```bash
export MAVEN_OPTS="-Xmx2g -Xms1g"
mvn clean package -DskipTests
```

---

## Cleanup

### 1. Stop the HugeGraph Server

```bash
SERVER_DIR="$(find . -maxdepth 3 -type d -path './apache-hugegraph-*/apache-hugegraph-server-*' | head -n 1)"
SERVER_DIR="${SERVER_DIR#./}"
[ -n "$SERVER_DIR" ] || { echo "HugeGraph server runtime not found. Run mvn clean package -DskipTests first."; exit 1; }
"$SERVER_DIR/bin/stop-hugegraph.sh"
```

### 2. Drop HugeGraph Tables from HBase

HugeGraph creates tables in the `default_hugegraph` namespace (e.g. `default_hugegraph:g_v`, `default_hugegraph:g_oe`, etc.).

Drop all HugeGraph tables (disable then drop each one):

```bash
docker exec hg-hbase-test bash -c '
  for t in $(echo "list" | hbase shell -n 2>/dev/null | grep "^default_hugegraph"); do
    echo "disable '"'"'$t'"'"'; drop '"'"'$t'"'"'"
  done | hbase shell
'
```

Verify tables are gone:

```bash
docker exec hg-hbase-test bash -lc "echo 'list' | hbase shell -n"
# Expected: TABLE (empty), 0 row(s)
```

### 3. Stop and Remove HBase Container

```bash
# Stop and remove HBase container + volumes
docker compose -f docker/hbase/docker-compose.hbase.yml down -v

# Verify containers are stopped
docker ps | grep hbase
```

---

## References

- **HBase Official Docs**: https://hbase.apache.org/
- **HugeGraph HBase Backend**: `hugegraph-server/hugegraph-hbase/`
- **Docker Compose Reference**: `docker/hbase/docker-compose.hbase.yml`
