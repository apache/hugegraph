# RocksDB Cloud Storage Distributed Smoke Test with MinIO

This guide covers the automated test and manual setup for the **rocksdb cloud storage distributed backend** with MinIO (S3-compatible object storage). Each store node has its own isolated cloud storage bucket for durability.

- `docker/cloud-storage/test-rocksdb-cloud-distributed.sh` — Automated smoke test (server `backend=hstore` + 3 stores with rocksdb cloud storage + separate per-store cloud storage bucket sync)

> **All commands must be run from the repository root.**

---

## Architecture

```
HugeGraph Server  (backend=hstore)
   └── Stateless coordinator
         ├── Routes all graph operations to store nodes
         └── No local data persistence

PD (Placement Driver) + 3 Store nodes (Raft consensus)
   └── Each store: embedded RocksDB + cloud storage sync (separate bucket per store)
         ├── store0 → RocksDB + Cloud sync → Cloud storage bucket: store0-rocksdb
         ├── store1 → RocksDB + Cloud sync → Cloud storage bucket: store1-rocksdb
         └── store2 → RocksDB + Cloud sync → Cloud storage bucket: store2-rocksdb
```

> **Key architectural point:** Fully distributed with cloud-sync durability controlled by one mode flag:
> - Server (`backend=hstore`) is **stateless** — all graph data is in stores
> - Each store runs **embedded RocksDB** with cloud storage module enabled
> - Store 0 syncs to isolated `store0-rocksdb` cloud storage bucket (independent credentials + quota possible)
> - Store 1 syncs to isolated `store1-rocksdb` cloud storage bucket  
> - Store 2 syncs to isolated `store2-rocksdb` cloud storage bucket
> - Graph data is **Raft-replicated** across stores; each store's local RocksDB is cloud storage-backed

**Port mappings (localhost → container):**

| Service           | Host Port | Purpose            |
|-------------------|-----------|--------------------|
| MinIO API         | 9000      | S3-compatible API  |
| MinIO Console     | 9001      | Web UI             |
| PD REST           | 8620      | Health / API       |
| PD gRPC           | 8686      | Store registration |
| Store 0 REST      | 8520      | Health             |
| Store 1 REST      | 8521      | Health             |
| Store 2 REST      | 8522      | Health             |
| HugeGraph Server  | 8080      | REST API           |

> **Note on initialization timing:** The server (`backend=hstore`) may take **2-5 minutes** to fully initialize after all store nodes become healthy. The server health check (`/versions` endpoint) returns 200 quickly, but graph operations only succeed after the hstore backend has fully connected to and synchronized with all store nodes. The test script waits for the first successful graph API call before attempting schema operations.

---

## Data Loss & Reliability

**📖 For detailed information on data loss scenarios and risk mitigation, see:**

- **[Architecture](./ARCHITECTURE.md)** — Failure modes, recovery behavior, and configuration trade-offs

**Key takeaway:**
- `rocksdb.cloud.synchronous_sst_upload_mode=true` => synchronous cloud upload
- `rocksdb.cloud.synchronous_sst_upload_mode=false` => periodic background reconcile mode
- ✅ **Single/double store failure**: ZERO data loss (Raft replication protects)
- ⚠️ **Catastrophic disk loss (all 3 stores)**: Possible loss of recent writes if not yet synced to cloud (typically 30-60 seconds)
- 🛡️ **Mitigation**: Use persistent storage + monitoring. See [Architecture](./ARCHITECTURE.md) for configuration tuning.

---

## Quick Start (Automated)

The automated script handles everything end-to-end. Use this for reliable testing of server
`backend=hstore` (stateless coordinator), plus required store-side cloud storage sync checks.

### Step 1 — Build or auto-build images

The server and store nodes both need the rocksdb cloud storage backend.

**Option A: Build manually first, then run test:**

```bash
docker build -t hugegraph/server:rocksdb-cloud-local -f hugegraph-server/Dockerfile .
docker build -t hugegraph/store:rocksdb-cloud-local -f hugegraph-store/Dockerfile .

chmod +x docker/cloud-storage/test-rocksdb-cloud-distributed.sh

HG_SERVER_IMAGE=hugegraph/server:rocksdb-cloud-local \
HG_STORE_IMAGE=hugegraph/store:rocksdb-cloud-local \
  ./docker/cloud-storage/test-rocksdb-cloud-distributed.sh
```

**Option B: Let the script build images automatically:**

```bash
chmod +x docker/cloud-storage/test-rocksdb-cloud-distributed.sh

AUTO_BUILD_SERVER_IMAGE=true \
AUTO_BUILD_STORE_IMAGE=true \
  ./docker/cloud-storage/test-rocksdb-cloud-distributed.sh
```

(Optional) verify the generated server backend explicitly:

```bash
DRY_RUN=true ./docker/cloud-storage/test-rocksdb-cloud-distributed.sh
grep -n '^backend=' docker/cloud-storage/.generated/hugegraph.properties
# expected: backend=hstore
```

The script:
- Generates a docker-compose file with all port bindings
- Starts MinIO + PD + 3 Store nodes + HugeGraph Server (hstore backend, stateless)
- Waits for all services to be healthy
- Creates MinIO buckets for each store: `store0-rocksdb`, `store1-rocksdb`, `store2-rocksdb`
- **Optionally** (default): Creates schema and writes/reads vertices via server REST API
- **Optionally** (default): Verifies store-side cloud storage mode and cloud objects
- Cleans up (unless `KEEP_UP=true`)

**Two modes of operation:**

1. **Full automated smoke test** (default): Creates schema, writes test data, verifies S3 sync, then cleans up.
   ```bash
   ./docker/HStore-On-S3/test-rocksdb-cloud-distributed.sh
   ```

2. **Environment setup only** (`SKIP_SMOKE_TESTS=true`): Starts services and keeps them running for your own manual tests (useful for debugging or custom workflows).
   ```bash
   SKIP_SMOKE_TESTS=true KEEP_UP=true \
     AUTO_BUILD_SERVER_IMAGE=true \
     AUTO_BUILD_STORE_IMAGE=true \
     ./docker/HStore-On-S3/test-rocksdb-cloud-distributed.sh
   ```

### Override options

```bash
# Auto-build both server and store images from source
AUTO_BUILD_SERVER_IMAGE=true \
AUTO_BUILD_STORE_IMAGE=true \
  ./docker/cloud-storage/test-rocksdb-cloud-distributed.sh

# Keep containers running after test (for inspection)
KEEP_UP=true HG_SERVER_IMAGE=hugegraph/server:rocksdb-cloud-local \
HG_STORE_IMAGE=hugegraph/store:rocksdb-cloud-local \
  ./docker/cloud-storage/test-rocksdb-cloud-distributed.sh

# Skip automated smoke tests — use script for environment setup only (manual testing mode)
SKIP_SMOKE_TESTS=true KEEP_UP=true \
AUTO_BUILD_SERVER_IMAGE=true \
AUTO_BUILD_STORE_IMAGE=true \
  ./docker/cloud-storage/test-rocksdb-cloud-distributed.sh

# Dry run: only generate compose/config files without starting services
DRY_RUN=true ./docker/cloud-storage/test-rocksdb-cloud-distributed.sh

# Use custom image tags
HG_SERVER_IMAGE=hugegraph/server:my-tag \
HG_STORE_IMAGE=hugegraph/store:my-tag \
  ./docker/cloud-storage/test-rocksdb-cloud-distributed.sh

# Cloud-first mode is DEFAULT: each write commit waits for cloud storage sync before ack
HG_SERVER_IMAGE=hugegraph/server:rocksdb-cloud-local \
HG_STORE_IMAGE=hugegraph/store:rocksdb-cloud-local \
  ./docker/cloud-storage/test-rocksdb-cloud-distributed.sh

# Optional: periodic fallback mode (disable synchronous cloud upload)
STORE_ROCKSDB_CLOUD_SYNCHRONOUS_SST_UPLOAD_MODE=false \
STORE_ROCKSDB_CLOUD_SYNC_INTERVAL_SECONDS=60 \
HG_SERVER_IMAGE=hugegraph/server:rocksdb-cloud-local \
HG_STORE_IMAGE=hugegraph/store:rocksdb-cloud-local \
  ./docker/cloud-storage/test-rocksdb-cloud-distributed.sh

# Tune periodic background sync interval (seconds)
STORE_ROCKSDB_CLOUD_SYNC_INTERVAL_SECONDS=60 \
HG_SERVER_IMAGE=hugegraph/server:rocksdb-cloud-local \
HG_STORE_IMAGE=hugegraph/store:rocksdb-cloud-local \
  ./docker/cloud-storage/test-rocksdb-cloud-distributed.sh
```

---

## Manual Setup (Step-by-Step)

Use this for learning, debugging, or exploring the REST API interactively.

> **Note:** The automated script above is more reliable. Use it if the manual steps fail.

### Prerequisites

Run the automated Quick Start with `KEEP_UP=true` to retain containers:

```bash
KEEP_UP=true \
AUTO_BUILD_SERVER_IMAGE=true \
AUTO_BUILD_STORE_IMAGE=true \
  ./docker/cloud-storage/test-rocksdb-cloud-distributed.sh
```

Once the test completes successfully and containers are running, proceed with steps below.

---

### Step 1 - Create graph schema

> Schema persists across restarts via rocksdb-cloud. If you see `ExistedException` errors, schema already exists — skip to Step 2.

```bash
# Idempotent helper: create property key only if missing
create_pk() {
  local name="$1" dtype="$2"
  local found=$(curl -s --compressed "http://localhost:8080/graphs/hugegraph/schema/propertykeys/$name" \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('name',''))" 2>/dev/null)
  [[ "$found" == "$name" ]] && { echo "  ✓ property key '$name' exists"; return; }
  curl -s -X POST http://localhost:8080/graphs/hugegraph/schema/propertykeys \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"$name\",\"data_type\":\"$dtype\",\"cardinality\":\"SINGLE\"}" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print('  ✓ created property key:', d.get('property_key',{}).get('name','?'))"
}

# Idempotent helper: create vertex label only if missing
create_vl() {
  local name="$1" props="$2"
  local found=$(curl -s --compressed "http://localhost:8080/graphs/hugegraph/schema/vertexlabels/$name" \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('name',''))" 2>/dev/null)
  [[ "$found" == "$name" ]] && { echo "  ✓ vertex label '$name' exists"; return; }
  curl -s -X POST http://localhost:8080/graphs/hugegraph/schema/vertexlabels \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"$name\",\"id_strategy\":\"AUTOMATIC\",\"properties\":$props}" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print('  ✓ created vertex label:', d.get('name','?'))"
}

# Property keys
create_pk "name" "TEXT"
create_pk "age"  "INT"
create_pk "city" "TEXT"

# Vertex labels
create_vl "person"   '["name","age","city"]'
create_vl "location" '["name"]'

# Edge label
FOUND_EL=$(curl -s --compressed http://localhost:8080/graphs/hugegraph/schema/edgelabels/lives_in \
  | python3 -c "import sys,json; print(json.load(sys.stdin).get('name',''))" 2>/dev/null)
if [[ "$FOUND_EL" == "lives_in" ]]; then
  echo "  ✓ edge label 'lives_in' exists"
else
  curl -s -X POST http://localhost:8080/graphs/hugegraph/schema/edgelabels \
    -H 'Content-Type: application/json' \
    -d '{"name":"lives_in","source_label":"person","target_label":"location"}' \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print('  ✓ created edge label:', d.get('name','?'))"
fi

echo "✓ Schema ready"
```

---

### Step 2 - Add vertices and edges

```bash
insert_vertex() {
  local label="$1" props="$2"
  # Write body and status to temp vars; avoid head -n -1 which fails on macOS
  local tmpfile=$(mktemp)
  local code=$(curl -s -o "$tmpfile" -w "%{http_code}" -X POST \
    http://localhost:8080/graphs/hugegraph/graph/vertices \
    -H 'Content-Type: application/json' \
    -d "{\"label\":\"$label\",\"properties\":$props}")
  local body=$(cat "$tmpfile")
  rm -f "$tmpfile"
  if [[ "$code" == "201" || "$code" == "200" ]]; then
    echo "$body" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])"
  else
    echo "ERROR: HTTP $code — $body" >&2
    echo ""
  fi
}

echo "Inserting vertices..."
PERSON_1=$(insert_vertex "person" '{"name":"Alice","age":25,"city":"San Francisco"}')
echo "  ✓ Alice: $PERSON_1"

PERSON_2=$(insert_vertex "person" '{"name":"Bob","age":30,"city":"New York"}')
echo "  ✓ Bob: $PERSON_2"

LOCATION=$(insert_vertex "location" '{"name":"San Francisco"}')
echo "  ✓ Location: $LOCATION"

echo "Inserting edge (lives_in)..."
if [[ -n "$PERSON_1" && -n "$LOCATION" ]]; then
  tmpfile=$(mktemp)
  code=$(curl -s -o "$tmpfile" -w "%{http_code}" -X POST \
    http://localhost:8080/graphs/hugegraph/graph/edges \
    -H 'Content-Type: application/json' \
    -d "{\"label\":\"lives_in\",\"outV\":$PERSON_1,\"inV\":$LOCATION,\"properties\":{}}")
  body=$(cat "$tmpfile"); rm -f "$tmpfile"
  if [[ "$code" == "201" || "$code" == "200" ]]; then
    EDGE_ID=$(echo "$body" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])" 2>/dev/null)
    echo "  ✓ Edge id: $EDGE_ID"
  else
    echo "  ✗ Failed (HTTP $code): $body"
  fi
else
  echo "  ✗ Skipped — vertex IDs not available"
fi
```

---

### Step 3 - Execute graph queries

```bash
echo "=== All vertices ==="
curl -s --compressed http://localhost:8080/graphs/hugegraph/graph/vertices | python3 -m json.tool

echo "=== All edges ==="
curl -s --compressed http://localhost:8080/graphs/hugegraph/graph/edges | python3 -m json.tool

echo "=== Vertex by id (Alice) ==="
curl -s --compressed "http://localhost:8080/graphs/hugegraph/graph/vertices/${PERSON_1}" | python3 -m json.tool
```

---


### Step 4 - Cleanup

```bash
# Option A: Using the same COMPOSE_PROJECT_NAME as the test
COMPOSE_PROJECT_NAME=hg-rocksdb-cloud-dist \
  docker compose -f docker/cloud-storage/.generated/docker-compose.rocksdb-cloud-distributed.yml down -v

# Option B: If Option A doesn't work, use explicit project name flag
docker compose -p hg-rocksdb-cloud-dist -f docker/cloud-storage/.generated/docker-compose.rocksdb-cloud-distributed.yml down -v

# Option C: If neither works, clean up manually
docker stop hg-minio-test hg-pd-dist hg-store0-dist hg-store1-dist hg-store2-dist hg-server-test 2>/dev/null || true
docker rm hg-minio-test hg-pd-dist hg-store0-dist hg-store1-dist hg-store2-dist hg-server-test 2>/dev/null || true
docker volume rm hg-rocksdb-cloud-dist_hg-minio-data hg-rocksdb-cloud-dist_hg-pd-data \
  hg-rocksdb-cloud-dist_hg-store0-data hg-rocksdb-cloud-dist_hg-store1-data hg-rocksdb-cloud-dist_hg-store2-data 2>/dev/null || true
docker network rm hg-rocksdb-cloud-dist_hg-net 2>/dev/null || true
```

---

## Troubleshooting

### `backend is illegal: rocksdb-cloud`

**Symptom** in server logs:
```
[WARN]  The config option 'hugegraph-hstore.*' / 'rocksdb.cloud.*' is redundant
[ERROR] Failed to load backend store provider: backend is illegal: hstore
```

**Cause:** Using a pre-built server image that doesn't include the hstore backend module, OR misconfigured hugegraph.properties.

**Fix:**
```bash
# Build from source — this includes all backend modules (including hstore)
docker build -t hugegraph/server:rocksdb-cloud-local -f hugegraph-server/Dockerfile .

# Verify server backend is hstore (not rocksdb-cloud)
grep -n '^backend=' docker/cloud-storage/.generated/hugegraph.properties
# expected output: backend=hstore

# Re-run with the built image
HG_SERVER_IMAGE=hugegraph/server:rocksdb-cloud-local \
  ./docker/cloud-storage/test-rocksdb-cloud-distributed.sh
```

---

### `The specified bucket does not exist` (Cloud storage 404)

**Symptom** in store logs (e.g., `docker logs hg-store0-dist`):
```
Failed to sync data to cloud storage on close ... The specified bucket does not exist (Status Code: 404)
```

**Cause:** Store node started before its cloud storage bucket was created.

**Fix:**
```bash
NETWORK_NAME="${COMPOSE_PROJECT_NAME:-hg-rocksdb-cloud-dist}_hg-net"

# Verify MinIO is healthy
curl -fsS http://localhost:9000/minio/health/live

# Create per-store cloud storage buckets
docker run --rm --network "$NETWORK_NAME" --entrypoint /bin/sh minio/mc:latest -c \
  "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null && \
   mc mb --ignore-existing local/store0-rocksdb && \
   mc mb --ignore-existing local/store1-rocksdb && \
   mc mb --ignore-existing local/store2-rocksdb && \
   mc ls local/"

# Restart all store containers to reconnect to cloud storage
for i in 0 1 2; do
  docker restart hg-store${i}-dist
done
sleep 30
curl http://localhost:8080/versions
```

---

### Server not responding on `:8080`

The full stack (MinIO + PD + 3 Stores + Server) can take **2-3 minutes** to fully initialize.

```bash
# Check all services health
docker compose -f docker/cloud-storage/.generated/docker-compose.rocksdb-cloud-distributed.yml ps

# Check port is published to host
docker ps --format "table {{.Names}}\t{{.Ports}}" | grep hg-server-test

# Check server logs for errors
docker logs hg-server-test 2>&1 | tail -50

# Check dependency health
curl http://localhost:8620/v1/health   # PD
curl http://localhost:8520/v1/health   # Store 0

# Wait and retry
sleep 60 && curl http://localhost:8080/versions
```

**Common causes:**
- `Waiting for partition assignment...` — Stores still joining the Raft cluster (wait longer or check store health)
- `backend is illegal` — wrong server image (build from source, see above)
- `bucket does not exist` — Cloud storage bucket not created before server start (see above)
- Port not listed in `docker ps` — stack started before port bindings were added; regenerate and restart

---

### `Connection refused` on port 8080

Ports are not published to the host. The generated compose file must include port bindings.

```bash
# Tear down and regenerate (script includes port bindings)
docker compose -f docker/cloud-storage/.generated/docker-compose.rocksdb-cloud-distributed.yml down -v
export COMPOSE_PROJECT_NAME=hg-rocksdb-cloud-dist
DRY_RUN=true ./docker/cloud-storage/test-rocksdb-cloud-distributed.sh
docker compose -f docker/cloud-storage/.generated/docker-compose.rocksdb-cloud-distributed.yml up -d

# Verify ports are published
docker ps --format "table {{.Names}}\t{{.Ports}}"
```

---

### `ExistedException` when creating schema

```
The property key 'name' has existed
```

**This is not an error.** Schema persists via rocksdb-cloud from a previous run. Skip to Step 3.

---

### Store node cloud storage prefix empty after sync interval

**Symptom:** `mc ls local/hugegraph-rocksdb/store0/` returns no results even after waiting.

**Causes & fixes:**

1. **Store image does not support `cloud_enabled`** — the `rocksdb.cloud_enabled` property was
   added in HugeGraph Store 1.7.0. Older images ignore it.
   ```bash
   # Confirm the entrypoint logged the cloud storage settings
   docker logs hg-store0-dist 2>&1 | grep "rocksdb.cloud"
   # If nothing is printed, build from source
   docker build -t hugegraph/store:rocksdb-cloud-local -f hugegraph-store/Dockerfile .
   HG_STORE_IMAGE=hugegraph/store:rocksdb-cloud-local \
     HG_SERVER_IMAGE=hugegraph/server:rocksdb-cloud-local \
     ./docker/cloud-storage/test-rocksdb-cloud-distributed.sh
   ```

2. **Sync interval not yet elapsed** — each store node flushes SST files to cloud storage every
   `STORE_ROCKSDB_CLOUD_SYNC_INTERVAL_SECONDS` seconds (default 30). Wait longer or set:
   ```bash
   STORE_ROCKSDB_CLOUD_SYNC_INTERVAL_SECONDS=5 \
     HG_SERVER_IMAGE=hugegraph/server:rocksdb-cloud-local \
     ./docker/cloud-storage/test-rocksdb-cloud-distributed.sh
   ```

3. **Bucket does not exist** — ensure the cloud storage bucket was created before the stores started
   (see `The specified bucket does not exist` troubleshooting entry above).

4. **Temporary debug-only bypass (not recommended for this smoke test)**:
   ```bash
   STORE_ROCKSDB_CLOUD_ENABLED=false \
     HG_SERVER_IMAGE=hugegraph/server:rocksdb-cloud-local \
     ./docker/cloud-storage/test-rocksdb-cloud-distributed.sh
   ```
   The script is expected to fail fast in this mode because per-store cloud storage writes are required.

---

### `NullPointerException` when creating schema

**Symptom** in script output:
```
[rocksdb-cloud-distributed-smoke] create property key not ready yet (attempt 1/45, http=500)
[rocksdb-cloud-distributed-smoke] create property key response: {"exception":"class java.lang.NullPointerException",...
"org.apache.hugegraph.core.GraphManager.graph(GraphManager.java:1963)"...
```

**Cause:** The hstore backend hasn't fully initialized graph operations yet. The server's `/versions` endpoint responds quickly, but accessing the graph backend takes longer as it needs to:
- Connect to the Placement Driver (PD)
- Register with and synchronize with all store nodes
- Load the graph database

**Fix:** The test script now waits for the first successful graph API call before attempting schema operations. If you're making manual requests:

```bash
# Poll until graph operations are available
while ! curl -fsS http://localhost:8080/graphs/hugegraph/graph/vertices >/dev/null 2>&1; do
  sleep 3
  echo "Waiting for graph backend to initialize..."
done

# Now safe to create schema
curl -X POST http://localhost:8080/graphs/hugegraph/schema/propertykeys \
  -H 'Content-Type: application/json' \
  -d '{"name":"test","data_type":"TEXT","cardinality":"SINGLE"}'
```

**Prevention:** Always wait for `GET /graphs/{name}/graph/vertices` to respond with HTTP 200 before attempting any write operations (schema creation, vertex/edge inserts).

---

### `PD unreachable, pd.peers=127.0.0.1:8686` in server logs

**Symptom** in `docker logs hg-server-test`:
```
PD unreachable, pd.peers=127.0.0.1:8686
Failed to listen ... to pd
Waiting for partition assignment...
```

**Cause:** The server container is running in Docker network mode and must use the PD service name (`pd:8686`). If `pd.peers` is missing, HugeGraph falls back to `127.0.0.1:8686`, which is incorrect inside the container.

**Fix:** Ensure generated graph config uses `pd.peers` (not `pdserver.address`):

```bash
grep -n '^pd\.peers=' docker/HStore-On-S3/.generated/hugegraph.properties
# expected: pd.peers=pd:8686
```

In this smoke test, `hugegraph.properties` is mounted read-only into the server container, so avoid passing `HG_SERVER_PD_PEERS` as an env override (the entrypoint would try to edit the mounted file and fail).

Then restart via the smoke script.

---

### Edge create HTTP 400: `The properties of edge can't be null`

Some HugeGraph versions require the edge create payload to include a `properties` field,
even when the edge label has no properties.

Use:

```bash
-d "{\"label\":\"lives_in\",\"outV\":$PERSON_1,\"inV\":$LOCATION,\"properties\":{}}"
```

---

### Services failing or in restart loop

```bash
# Inspect individual service logs
docker logs hg-pd-dist     | tail -50
docker logs hg-store0-dist | tail -50
docker logs hg-minio-test  | tail -30

# Increase Docker resources
# Mac: Docker Desktop → Settings → Resources → Memory (recommend 8GB+, 4 CPUs+)

# Clean restart
COMPOSE_PROJECT_NAME=hg-rocksdb-cloud-dist \
  docker compose -f docker/cloud-storage/.generated/docker-compose.rocksdb-cloud-distributed.yml down -v
# Then re-run Step 1
```

---

### `docker compose down -v` not removing containers

**Symptom:** Running the cleanup command leaves containers, volumes, or networks behind.

**Cause:** The `COMPOSE_PROJECT_NAME` environment variable is not set when running `docker compose down`, so it uses the directory name (`.generated`) instead of the original project name (`hg-rocksdb-cloud-dist`), causing it to look for the wrong compose project.

**Fix:** Use one of the cleanup options in Step 4:

```bash
# Recommended: Set COMPOSE_PROJECT_NAME explicitly
COMPOSE_PROJECT_NAME=hg-rocksdb-cloud-dist \
  docker compose -f docker/cloud-storage/.generated/docker-compose.rocksdb-cloud-distributed.yml down -v

# Or: Use the -p flag
docker compose -p hg-rocksdb-cloud-dist -f docker/cloud-storage/.generated/docker-compose.rocksdb-cloud-distributed.yml down -v
```

---

## References

- **Automated test script**: `docker/cloud-storage/test-rocksdb-cloud-distributed.sh`
- **MinIO Docs**: https://min.io/docs/minio/container/index.html
- **Phase 2 Lease Integration**: `hugegraph-store/PHASE2_LEASE_INTEGRATION.md`
- **RocksDB Tuning Guide**: https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
