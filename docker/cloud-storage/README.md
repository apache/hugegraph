# Cloud Storage (MinIO + HStore) Local Stack

This stack runs:

- MinIO (S3-compatible storage)
- 1 PD node
- 3 Store nodes
- `hg-store-cloud-s3` plugin loaded from local build artifacts

The goal is to validate that RocksDB SST file events are mirrored to S3.

## Prerequisites

- Docker + Docker Compose
- Java 11+
- Maven 3.5+
- `curl` and `jq` (optional, for manual API testing)

From repo root, build all modules:

```bash
mvn clean package -DskipTests
```

This builds the necessary artifacts for the Docker images.

## Quick Start

Detect repo root first, then run the script:

```bash
export REPO_ROOT="$(git rev-parse --show-toplevel)"
test -d "${REPO_ROOT}/docker/cloud-storage"

cd "${REPO_ROOT}/docker/cloud-storage"
chmod +x ./scripts/*.sh ./entrypoints/*.sh
./scripts/test-graph-queries-and-sst.sh

# For manual testing steps, keep the stack running after test
./scripts/test-graph-queries-and-sst.sh --keep-stack

# For manual graph creation and testing (infrastructure only, no auto-load)
./scripts/test-graph-queries-and-sst.sh --no-load
```

## Test Output Files

After running `make test` or `./scripts/test-graph-queries-and-sst.sh`, check `.generated/` for:

- **`FULL-TEST-REPORT.txt`** — Complete test summary
- **`test-report.txt`** — Graph query test results
- **`minio-verification.txt`** — MinIO S3 object listing and SST count
- **`store-cluster.log`** — All store node logs with RocksDB/S3 events
- **`cli-load.log`** — Data load job output from hg-store-cli
- **`load-data.tsv`** — Generated test data file (200k entries)

## Manual Verification Steps

**Prerequisites:** Complete Step 1 first and wait for the infrastructure ready message.

For hands-on validation with manual graph creation and queries, start the cluster using the automated script with `--keep-stack`, then follow interactive steps. **All commands use `$REPO_ROOT` to reference paths relative to the repository root.**

**Note:** These steps verify end-to-end SST upload by:
1. Creating a graph schema
2. Adding test vertices and edges
3. Verifying data distribution across nodes
4. Restarting store nodes to flush SST files to RocksDB
5. Confirming SST files are uploaded to MinIO buckets

### Step 0: Set Repository Root

Before starting, set the `$REPO_ROOT` variable to point to the repository root. You can run this from anywhere:

```bash
export REPO_ROOT=$(git rev-parse --show-toplevel)
```

Verify the variable is set correctly:

```bash
echo $REPO_ROOT
```

### Step 1: Start Cluster with Infrastructure Only (No Auto-Load)

Use the automated test script to build images, start the cluster, and **skip the automatic data load** so you can create your own graph structure:

```bash
$REPO_ROOT/docker/cloud-storage/scripts/test-graph-queries-and-sst.sh --no-load
```

After Step 1 starts the stack, resolve the Docker network name for `docker run --network` commands:

```bash
export HG_NET="${HG_NET:-cloud-storage-net}"
if docker network inspect "$HG_NET" >/dev/null 2>&1; then
  echo "Using Docker network: $HG_NET"
elif docker network inspect cloud-storage-test_hg-net >/dev/null 2>&1; then
  export HG_NET="cloud-storage-test_hg-net"
  echo "Using Docker network: $HG_NET"
else
  echo "No cloud-storage Docker network found. Ensure Step 1 completed successfully."
fi
```

This will:
- Build local artifacts (PD, Store, Store CLI, S3 plugin)
- Build Docker images
- Start MinIO + PD + 3 Store nodes
- Wait for all services to be healthy
- **Skip** initial data load phase
- **Keep the stack running** for manual steps

The output will indicate when infrastructure is ready.

### Step 2: Verify Cluster Health

Verify all services are accessible. This may take a few minutes as the HugeGraph server needs time to initialize:

```bash
# Wait for services to be healthy (may take 2-3 minutes on first run)
echo "Waiting for services to be healthy..."
for i in {1..60}; do
  pd_ok=$(curl -fsS http://127.0.0.1:8620/v1/health >/dev/null 2>&1 && echo "yes" || echo "no")
  store0_ok=$(curl -fsS http://127.0.0.1:8520/v1/health >/dev/null 2>&1 && echo "yes" || echo "no")
  store1_ok=$(curl -fsS http://127.0.0.1:8521/v1/health >/dev/null 2>&1 && echo "yes" || echo "no")
  store2_ok=$(curl -fsS http://127.0.0.1:8522/v1/health >/dev/null 2>&1 && echo "yes" || echo "no")
  server_ok=$(curl -fsS http://127.0.0.1:8080/graphs >/dev/null 2>&1 && echo "yes" || echo "no")
  
  if [[ "$pd_ok" == "yes" && "$store0_ok" == "yes" && "$store1_ok" == "yes" && "$store2_ok" == "yes" && "$server_ok" == "yes" ]]; then
    break
  fi
  
  echo "  Attempt $i/60: PD=$pd_ok Store0=$store0_ok Store1=$store1_ok Store2=$store2_ok Server=$server_ok"
  sleep 2
done

# Final verification
curl -fsS http://127.0.0.1:8620/v1/health >/dev/null 2>&1 && echo "✓ PD OK" || echo "✗ PD FAILED"
curl -fsS http://127.0.0.1:8520/v1/health >/dev/null 2>&1 && echo "✓ Store0 OK" || echo "✗ Store0 FAILED"
curl -fsS http://127.0.0.1:8521/v1/health >/dev/null 2>&1 && echo "✓ Store1 OK" || echo "✗ Store1 FAILED"
curl -fsS http://127.0.0.1:8522/v1/health >/dev/null 2>&1 && echo "✓ Store2 OK" || echo "✗ Store2 FAILED"
curl -fsS http://127.0.0.1:8080/graphs >/dev/null 2>&1 && echo "✓ Server OK" || echo "✗ Server FAILED"
```

If all show "✓ OK", proceed to Step 3. If any show "✗ FAILED", check logs:
```bash
docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml logs --tail=50 server
```

**Note:** The `/graphs` endpoint returns a clean 200 OK response, making it the most reliable health check for the HugeGraph server. The `/gremlin` endpoint requires query parameters.

### Step 3: Create Graph Schema

Schema persists across restarts via rocksdb-cloud. If you see `ExistedException`, schema already exists.

```bash
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

create_pk "name" "TEXT"
create_pk "age"  "INT"
create_pk "city" "TEXT"

create_vl "person"   '["name","age","city"]'
create_vl "location" '["name"]'

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

### Step 4: Add Vertices and Edges

Add sufficient data to trigger RocksDB compaction (minimum 150+ vertices):

```bash
insert_vertex() {
  local label="$1" props="$2"
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
    echo "ERROR: HTTP $code - $body" >&2
    echo ""
  fi
}

echo "Inserting test vertices (150+)..."
for i in {1..150}; do
  age=$((20 + i % 50))
  city=$((i % 5))
  vid=$(insert_vertex "person" "{\"name\":\"person_$i\",\"age\":$age,\"city\":\"city_$city\"}")
  if (( i % 50 == 0 )); then
    echo "  ✓ Inserted $i vertices"
  fi
done

echo "Adding location vertices..."
for i in {0..4}; do
  insert_vertex "location" "{\"name\":\"city_$i\"}" >/dev/null
done

echo "✓ Inserted 150+ test vertices (sufficient for RocksDB compaction)"
```

**Why 150+ vertices?** Smaller datasets may not trigger RocksDB compaction. 150+ vertices across 3 store nodes ensures enough write activity to generate SST files.

### Step 5: Execute Graph Queries (Optional)

Verify the data was stored:

```bash
echo "=== Vertex count ==="
curl -s --compressed http://localhost:8080/graphs/hugegraph/graph/vertices | python3 -c "import sys,json; data=json.load(sys.stdin); print('Total vertices:', len(data.get('vertices',[])))"

echo "=== Sample vertex ==="
curl -s --compressed http://localhost:8080/graphs/hugegraph/graph/vertices | python3 -c "import sys,json; data=json.load(sys.stdin); vertices=data.get('vertices', []); print(json.dumps(vertices[0], indent=2) if vertices else 'No vertices')" | head -10
```

**Note:** Querying 150+ vertices can return large result sets. To keep this step fast, the above commands just check the count and show a sample.

### Step 6: Verify Data Distribution

Verify data has been distributed across store nodes:

```bash
echo "=== Partition Info (before flush) ==="
for i in 0 1 2; do
  port=$((8520 + i))
  echo ""
  echo "Store$i (port $port):"
  curl -s http://127.0.0.1:$port/v1/partitions | python3 -c "import sys,json; data=json.load(sys.stdin); print(f'  Partitions: {len(data.get(\"partitions\",[])) if isinstance(data.get(\"partitions\"), list) else \"N/A\"}')" 2>/dev/null || echo "  (unable to retrieve)"
done
```

**Expected:** Each store should show partition information, indicating data distribution across the cluster.

### Step 7: Flush Data to S3 (Restart Store Nodes)

To trigger SST file uploads to S3, restart the Store nodes. This forces RocksDB to:
1. Flush in-memory data to disk as SST files
2. Trigger the cloud storage event listener
3. Upload SST files to MinIO buckets

```bash
echo "Restarting store nodes to flush data to S3..."
# Use container names so this works for both static compose and generated test compose
docker restart cloud-storage-store0 cloud-storage-store1 cloud-storage-store2

echo "Waiting for stores to restart and flush..."
sleep 20

echo "Verifying store health after restart..."
for i in 0 1 2; do
  port=$((8520 + i))
  curl -fsS http://127.0.0.1:$port/v1/health >/dev/null 2>&1 && echo "✓ Store$i OK" || echo "✗ Store$i FAILED"
done
```

**What happens behind the scenes:**
- Docker restarts the store containers
- RocksDB initializes and detects in-memory data
- RocksDB writes all data as SST files to disk
- Cloud storage listener detects flush events
- SST files are uploaded to MinIO in parallel
- Stores become healthy and rejoin cluster

### Step 8: Final S3 Verification (After Flush)

Verify that SST files have been successfully uploaded to MinIO buckets:

```bash
echo "=== Checking MinIO buckets after flush ==="
docker run --rm --entrypoint /bin/sh --network "$HG_NET" minio/mc:RELEASE.2025-08-13T08-35-41Z \
  -c 'mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null && \
              for b in hugegraph-store0 hugegraph-store1 hugegraph-store2; do \
                echo ""; \
                echo "### Bucket: $b"; \
                total_count=$(mc ls --recursive local/$b | wc -l); \
                sst_count=$(mc find local/$b --name "*.sst" | wc -l); \
                printf "  Total objects: %d\n" "$total_count"; \
                printf "  SST files:     %d\n" "$sst_count"; \
                if (( sst_count > 0 )); then \
                  echo "  Sample SST files (first 3):"; \
                  mc find local/$b --name "*.sst" | head -3; \
                fi; \
              done'
```

**What successful upload looks like:**
```
### Bucket: hugegraph-store0
  Total objects: 8
  SST files:     6
  Sample SST files (first 3):
    local/hugegraph-store0/partition-1/0000000001.sst
    local/hugegraph-store0/partition-1/0000000002.sst
    local/hugegraph-store0/partition-1/manifest

### Bucket: hugegraph-store1
  Total objects: 9
  SST files:     7
  ...

### Bucket: hugegraph-store2
  Total objects: 8
  SST files:     6
  ...
```

**Success criteria:**
- ✅ All three buckets show `Total objects: > 0`
- ✅ All three buckets show `SST files: > 0`
- ✅ SST counts are roughly balanced across buckets
- ✅ No errors from mc command

**If buckets are still empty after flush:**
1. Check store logs for upload errors: `docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml logs store0 | grep -i "s3\|cloud\|error"`
2. Verify cloud storage was initialized: `docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml logs store0 | grep -i "Cloud storage provider.*initialized\|S3CloudStorageProvider initialized"`
3. Check that data was written: `docker exec cloud-storage-store0 sh -lc 'find /hugegraph-store/storage -name "*.sst" | wc -l'`

### Step 9 (Optional): Destroy the Cluster

When done with manual verification, stop and clean up:

```bash
docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml down
```

To also remove data and logs directories:

```bash
docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml down
rm -rf $REPO_ROOT/docker/cloud-storage/data/ $REPO_ROOT/docker/cloud-storage/logs/
```

To reset everything including built Docker images (for a fresh start):

```bash
docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml down
rm -rf $REPO_ROOT/docker/cloud-storage/data/ $REPO_ROOT/docker/cloud-storage/logs/ $REPO_ROOT/docker/cloud-storage/.artifacts/
docker rmi \
  hugegraph-pd-cloud-storage:local \
  hugegraph-store-cloud-storage:local \
  2>/dev/null || true
```


## Architecture Notes

- **Entrypoints**: Store containers inject `cloud.storage.*` config via `SPRING_APPLICATION_JSON` in `entrypoints/store-entrypoint.sh`.
- **Config precedence**: values injected by `SPRING_APPLICATION_JSON` override defaults in `/hugegraph-store/conf/application.yml`.
- **Plugin Loading**: Store startup uses `PropertiesLauncher` with `-Dloader.path=/hugegraph-store/plugins` to discover S3 provider JAR via `ServiceLoader`
- **Logging**: Store containers run with `DEBUG` level for:
  - `org.apache.hugegraph.store.node.cloud`
  - `org.apache.hugegraph.rocksdb.access`
- **Networking**: Containers run on `cloud-storage-net` (static compose) or `cloud-storage-test_hg-net` (default generated test stack)
- **Storage**: Local bind mounts under `data/` and `logs/` for inspection
- **Bucket layout**: each Store node writes SSTs to a dedicated bucket (`store0 -> hugegraph-store0`, `store1 -> hugegraph-store1`, `store2 -> hugegraph-store2`)

### Troubleshooting Manual Verification Steps

**Step 2: Server not becoming healthy**
```bash
# Check if server container is running
docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml ps server

# Check server logs
docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml logs --tail=100 server

# Verify port 8080 is accessible and /graphs endpoint works
curl -v http://127.0.0.1:8080/graphs
```

**Note:** The server health check uses `/graphs` endpoint (HTTP 200 OK), not `/gremlin` (requires query parameter).

**Step 3-4: Graph API errors (HTTP errors)**
```bash
# Verify server is healthy
curl -fsS http://127.0.0.1:8080/graphs && echo "Server OK" || echo "Server not ready"

# Check if graph exists
curl -s http://localhost:8080/graphs/hugegraph/graph/vertices | python3 -c "import sys,json; print(json.load(sys.stdin).keys())"

# Check server logs for errors
docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml logs server | grep -i "error\|exception" | tail -20
```

**Step 6: Partition info showing empty**
```bash
# Data may still be in RocksDB memory, not yet in partitions
# This is normal - proceed to Step 7 to flush

# Or check if data was actually written
docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml logs store0 | grep -i "vertex\|edge\|insert" | tail -10
```

**Step 7: Stores not coming back healthy after restart**
```bash
# Check individual store health
for i in 0 1 2; do
  port=$((8520 + i))
  echo "Store$i:"
  curl -v http://127.0.0.1:$port/v1/health 2>&1 | grep -E "HTTP/|Connection refused"
done

# Check store logs for startup errors
docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml logs store0 | tail -50 | grep -i "error\|exception"
```

**Step 8: Buckets showing 0 files after flush**

This is the most common issue. Debug step-by-step:

1. **Verify stores have data on disk:**
   ```bash
   docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml exec store0 \
     find /hugegraph-store/storage -name "*.sst" | wc -l
   ```
   If output is 0, no SST files were created. Go back to Step 4 and ensure data was inserted.

2. **Verify cloud storage was initialized:**
   ```bash
   docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml logs store0 | \
     grep -i "S3CloudStorageProvider\|cloud storage" | head -5
   ```
   If no output, plugin didn't load. Check `/hugegraph-store/plugins/` for `hg-store-cloud-s3.jar`.

3. **Check for S3 upload errors:**
   ```bash
   docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml logs store0 | \
     grep -i "upload\|s3\|error" | tail -20
   ```

4. **Verify MinIO connectivity:**
   ```bash
   docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml exec store0 \
     curl -v http://minio:9000/minio/health/live
   ```

5. **Manually check MinIO buckets:**
   ```bash
   export HG_NET="cloud-storage-net"
   docker run --rm --entrypoint /bin/sh --network "$HG_NET" minio/mc:RELEASE.2025-08-13T08-35-41Z \
     -c 'mc alias set local http://minio:9000 minioadmin minioadmin && mc ls local/'
   ```

### Troubleshooting: General Infrastructure Issues

### Store crashes with SIGSEGV on ARM64 (libjvm.so)

If store logs show a fatal JVM crash like:

```text
SIGSEGV ... libjvm.so ... linux-aarch64
```

this is typically seen on ARM64 hosts with JVM + RocksDB startup.

**On ARM64 hosts, the script automatically applies safe JVM defaults:**
- Uses Java 17 runtime image (not Java 11)
- Applies conservative JVM flags: `-XX:+UseSerialGC -XX:-UseCompressedOops -XX:-UseCompressedClassPointers`

Simply run without special flags:

```bash
$REPO_ROOT/docker/cloud-storage/scripts/test-graph-queries-and-sst.sh --no-load
```

The script detects your host architecture and applies appropriate defaults automatically.

### Custom overrides on ARM64

If you need different JVM flags or runtime images:

```bash
HG_DOCKER_DEFAULT_PLATFORM=linux/amd64 \
HG_PD_JAVA_RUNTIME_IMAGE=eclipse-temurin:17-jre \
HG_STORE_JAVA_RUNTIME_IMAGE=eclipse-temurin:17-jre \
HG_STORE_JAVA_OPTS="-XX:+UseSerialGC -XX:-UseCompressedOops -XX:-UseCompressedClassPointers" \
$REPO_ROOT/docker/cloud-storage/scripts/test-graph-queries-and-sst.sh --no-load
```

But in most cases, running without overrides should work:

```bash
$REPO_ROOT/docker/cloud-storage/scripts/test-graph-queries-and-sst.sh --no-load
```

If old containers are still running, force recreate with fresh env:

```bash
docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml down
$REPO_ROOT/docker/cloud-storage/scripts/test-graph-queries-and-sst.sh --no-load

# Verify container runtime JDK actually changed
docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml run --rm --entrypoint java store0 -version
```

If the crash persists, inspect the generated JVM error file from store logs/data path:

```bash
docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml logs store0 | tail -200
docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml exec store0 ls -lah /hugegraph-store/hs_err_pid*.log
```

### No SST files appear in MinIO

- Short test runs may not trigger compaction; SST deletion especially requires heavier/longer load
- Check store logs for upload errors:
  ```bash
  docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml logs store0 | grep -i "cloud\|s3"
  ```
- Verify MinIO is accessible:
  ```bash
  docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml exec minio mc ls local/ 2>&1 | head -20
  ```

### Query tests show FAILED

- Ensure all store nodes are healthy: check `/v1/health` endpoints
- Manually test: `curl -fsS http://127.0.0.1:8520/v1/partitions | jq`
- Check store logs with:
  ```bash
  docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml logs store0 store1 store2
  ```

### Keep stack running for debugging

```bash
# Run test but keep stack up
KEEP_STACK=1 $REPO_ROOT/docker/cloud-storage/scripts/test-graph-queries-and-sst.sh
```

Then inspect manually:
```bash
docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml logs store0 store1 store2
docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml exec store0 ls -la /hugegraph-store/storage
```

Stop manually when done:
```bash
docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml down
```

## Summary of Manual Verification Workflow

This manual verification process validates the complete SST upload pipeline:

1. **Step 1:** Start the infrastructure (MinIO, PD, 3 Store nodes, HugeGraph Server)
2. **Step 2:** Wait for all services to become healthy
3. **Step 3:** Create graph schema (property keys, vertex labels, edge labels)
4. **Step 4:** Load 150+ test vertices to trigger compaction
5. **Step 5:** (Optional) Verify data was stored
6. **Step 6:** Verify data distribution across store nodes
7. **Step 7:** Restart store nodes to flush SST files to disk and upload to MinIO
8. **Step 8:** Verify SST files are present in MinIO buckets
9. **Step 9:** Cleanup when done

**Success = Non-zero SST file counts in all three buckets after Step 8**

## What Gets Verified

✅ Graph schema creation works  
✅ Vertex/edge insertion works  
✅ Data distribution across 3 store nodes  
✅ RocksDB SST file generation on restart  
✅ Cloud storage plugin uploads SST files to MinIO  
✅ Multiple buckets receive files consistently

## Notes

- S3 provider JAR must be in `/hugegraph-store/plugins/` for `ServiceLoader` discovery
- Plugin dependency staging filters extra SLF4J binding jars to avoid duplicate binding warnings at runtime
- Cloud settings are read from environment variables at store startup time
- **Minimum data size:** 150+ vertices is recommended to trigger RocksDB SST file generation. Very small datasets may not create any SST files.
- MinIO buckets are pre-created by `minio-init` service: `hugegraph-store0`, `hugegraph-store1`, `hugegraph-store2`
