# Cloud Storage (MinIO + HStore) Local Stack

This stack runs:

- MinIO (S3-compatible storage)
- 1 PD node
- 3 Store nodes
- `hg-store-cloud-s3` plugin loaded from local build artifacts

The goal is to validate that:

1. RocksDB SST files are mirrored to S3.
2. Recovery metadata (`MANIFEST-*`, `CURRENT`, `OPTIONS-*`) is mirrored alongside SSTs.
3. Together, these uploaded files are enough for cloud recovery after local RocksDB data loss.

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
```

```bash
# (Optional) Keep the stack running after the test for manual inspection
./scripts/test-graph-queries-and-sst.sh --keep-stack
```

```bash

# (Optional) Start infrastructure only (skip data creation + validation tests)
./scripts/test-graph-queries-and-sst.sh --keep-stack --infra-only
```

## Test Output Files

After running `make test` or `./scripts/test-graph-queries-and-sst.sh`, check `.generated/` for:

- **`FULL-TEST-REPORT.txt`** — Complete test summary
- **`test-report.txt`** — Graph query test results
- **`minio-verification.txt`** — MinIO S3 object listing and SST count
- **`store-cluster.log`** — All store node logs with RocksDB/S3 events
- **`cli-load.log`** — Data load job output from hg-store-cli
- **`load-data.tsv`** — Generated test data file (200k entries)

## Manual Verification (Core Flow)

**Prerequisites:** Complete Core Step 1 first and wait for the infrastructure ready message.

For hands-on validation with manual graph creation and queries, start the cluster using the
automated script with `--keep-stack`, then follow interactive steps.

**Core goals in this flow:**
1. Create schema and load enough data to generate SST files
2. Flush stores to trigger cloud upload
3. Verify SST + recovery metadata in MinIO UI

### Step 1: Set Environment and Start Infrastructure (No Auto-Load)

Use the automated test script to build images and start the stack. For manual-only flows,
prefer `--infra-only` to skip scripted data creation and validation checks while keeping the stack up:

```bash
export REPO_ROOT="$(git rev-parse --show-toplevel)"
echo "$REPO_ROOT"

$REPO_ROOT/docker/cloud-storage/scripts/test-graph-queries-and-sst.sh --keep-stack --infra-only
```

#### Shared Helper: Resolve `HG_NET` Once Per Shell

Run this once in your current shell after infrastructure is up. Re-run it if you open a new shell.

```bash
resolve_hg_net() {
  local net
  net=$(docker inspect cloud-storage-minio \
    --format '{{range $k, $v := .NetworkSettings.Networks}}{{println $k}}{{end}}' \
    2>/dev/null | head -n1 || true)

  if [[ -z "$net" ]]; then
    if docker network inspect cloud-storage-test_hg-net >/dev/null 2>&1; then
      net="cloud-storage-test_hg-net"
    elif docker network inspect cloud-storage-net >/dev/null 2>&1; then
      net="cloud-storage-net"
    fi
  fi

  [[ -n "$net" ]] || {
    echo "No cloud-storage Docker network found. Ensure Core Step 1 completed successfully." >&2
    return 1
  }

  export HG_NET="$net"
  echo "Using Docker network: $HG_NET"
}

resolve_hg_net
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

Optional quick sanity check before flush:

```bash
curl -s --compressed http://localhost:8080/graphs/hugegraph/graph/vertices \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('Total vertices:', len(d.get('vertices',[])))"
```

### Step 5: Manual Flush + MinIO UI Verification

Use this step when you want a purely manual verification flow in the UI.

1. Open MinIO Console: `http://localhost:9001` (default credentials: `minioadmin` / `minioadmin`).
2. Open buckets `hugegraph-store0`, `hugegraph-store1`, and `hugegraph-store2`.
3. In each bucket, note the current SST/metadata state before flush:
   - `*.sst` objects
   - recovery metadata files (`CURRENT`, `MANIFEST-*`, `OPTIONS-*`)
   - latest modified timestamps shown in the object list

Trigger flush on all store nodes:

```bash
echo "=== Trigger flush on all stores ==="
for p in 8520 8521 8522; do
  echo "Flushing store REST port ${p}..."
  curl -fsS "http://127.0.0.1:${p}/test/flush" >/dev/null
done
echo "Flush requests sent"
```

Wait for asynchronous cloud upload, then refresh MinIO UI:

```bash
echo "Waiting for flush upload to reach MinIO..."
sleep 20
```

Re-check the same three buckets in MinIO UI.

**Success criteria:**
- ✅ You can browse all three buckets in MinIO UI
- ✅ `*.sst` objects exist in each bucket after flush
- ✅ Recovery metadata files (`CURRENT`, `MANIFEST-*`, `OPTIONS-*`) are visible
- ✅ At least one object timestamp advances after flush (new upload activity)

If the UI does not update after ~20 seconds, wait another 20-40 seconds and refresh again.

## Advanced Scenarios (Optional)

### Advanced 1: Strict Script Assertions

Run this if you want script-based strict assertions in addition to the manual UI checks in Core Step 5.
The commands are wrapped in a guarded subshell so any `exit 1` fails only this block, not your
terminal (even if `set -e` was enabled earlier in the shell).

```bash

if ! (
# Requires the helper from Core Step 1.
type resolve_hg_net >/dev/null 2>&1 || { echo "Run Core Step 1 helper first"; exit 1; }
resolve_hg_net

echo "=== Assert bucket object + SST counts ==="
docker run --rm --entrypoint /bin/sh --network "$HG_NET" minio/mc:RELEASE.2025-08-13T08-35-41Z \
  -c 'if mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1; then \
        :; \
      elif mc alias set local http://cloud-storage-minio:9000 minioadmin minioadmin >/dev/null 2>&1; then \
        :; \
      else \
        echo "ERROR: cannot reach MinIO at minio:9000 or cloud-storage-minio:9000" >&2; \
        exit 1; \
      fi; \
      fail=0; \
      for b in hugegraph-store0 hugegraph-store1 hugegraph-store2; do \
        total_count=$(mc ls --recursive local/$b 2>/dev/null | wc -l); \
        sst_count=$(mc find local/$b --name "*.sst" 2>/dev/null | wc -l); \
        echo "### $b: total=$total_count sst=$sst_count"; \
        if [ "$total_count" -eq 0 ] || [ "$sst_count" -eq 0 ]; then \
          echo "  ERROR: expected total>0 and sst>0" >&2; \
          fail=1; \
        fi; \
      done; \
      exit "$fail"'

echo "=== Assert metadata set (CURRENT / MANIFEST-* / OPTIONS-*) ==="
docker run --rm --entrypoint /bin/sh --network "$HG_NET" minio/mc:RELEASE.2025-08-13T08-35-41Z \
  -c 'if mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1; then \
        :; \
      elif mc alias set local http://cloud-storage-minio:9000 minioadmin minioadmin >/dev/null 2>&1; then \
        :; \
      else \
        echo "ERROR: cannot reach MinIO at minio:9000 or cloud-storage-minio:9000" >&2; \
        exit 1; \
      fi; \
      fail=0; \
      for b in hugegraph-store0 hugegraph-store1 hugegraph-store2; do \
        current=$(mc find local/$b --name CURRENT 2>/dev/null | wc -l); \
        manifest=$(mc find local/$b --name "MANIFEST-*" 2>/dev/null | wc -l); \
        options=$(mc find local/$b --name "OPTIONS-*" 2>/dev/null | wc -l); \
        sst=$(mc find local/$b --name "*.sst" 2>/dev/null | wc -l); \
        echo "### $b: CURRENT=$current MANIFEST=$manifest OPTIONS=$options SST=$sst"; \
        if [ "$current" -lt 1 ] || [ "$manifest" -lt 1 ] || [ "$options" -lt 1 ] || [ "$sst" -lt 1 ]; then \
          echo "  ERROR: missing required recovery files" >&2; \
          fail=1; \
        fi; \
      done; \
      exit "$fail"'

); then
  echo "Advanced 1 assertions failed (see errors above)"
fi
```

**Success criteria:**
- ✅ Both assertion commands exit with status `0`
- ✅ All buckets have `total > 0` and `sst > 0`
- ✅ All buckets include `CURRENT`, `MANIFEST-*`, `OPTIONS-*`, and `*.sst`

**If buckets are still empty after flush:**
1. Check store logs for upload errors: `docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml logs store0 | grep -i "s3\|cloud\|error"`
2. Verify cloud storage was initialized: `docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml logs store0 | grep -i "Cloud storage provider.*initialized\|S3CloudStorageProvider initialized"`
3. Check that data was written: `docker exec cloud-storage-store0 sh -lc 'find /hugegraph-store/storage -name "*.sst" | wc -l'`

### Advanced 2: Clear Semantics & No-Orphan Re-hydration

This step validates what `graph.clear()` guarantees for cloud storage. Clearing a graph runs a
per-key-range delete (`deleteRange`) on the partition's RocksDB instance — it does **not** purge the
whole remote DB prefix. That instance can be shared by multiple graphs, so a whole-prefix purge would
destroy co-tenant graphs' cloud data; instead the cleared range converges in cloud through normal
SST mirroring/compaction. The durable guarantee under test is therefore: **after `clear()` and a store
restart, the graph does not re-hydrate stale data** (vertex count stays `0`).

Because the prefix is intentionally not purged, the object count under the prefix is expected to remain
non-zero after clear (live `CURRENT`/`MANIFEST-*`/`OPTIONS-*` and not-yet-compacted SSTs) — that is
by design, not a leak. Whole-prefix purge is exercised only by real DB deletion (see Advanced 3).

> **Warning:** This deletes graph data. Run it after Core Step 5 (and Advanced 1 if you use it),
> then re-run Core Steps 3-5 if you want to repeat the flush/recovery setup.

#### Manual Flow: Before Clear -> Clear -> After Clear -> Restart Check

1. Resolve Docker network and detect one graph DB prefix in MinIO.

```bash
GRAPH_NAME=hugegraph
type resolve_hg_net >/dev/null 2>&1 || { echo "Run Core Step 1 helper first"; exit 1; }
resolve_hg_net

BUCKET=""
DB_PREFIX=""
for CANDIDATE_BUCKET in hugegraph-store0 hugegraph-store1 hugegraph-store2; do
  MATCH=$(docker run --rm --entrypoint /bin/sh --network "$HG_NET" \
    minio/mc:RELEASE.2025-08-13T08-35-41Z \
    -c 'if mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1; then \
          :; \
        elif mc alias set local http://cloud-storage-minio:9000 minioadmin minioadmin >/dev/null 2>&1; then \
          :; \
        else \
          echo "ERROR: cannot reach MinIO at minio:9000 or cloud-storage-minio:9000" >&2; \
          exit 1; \
        fi; \
        mc find local/'"$CANDIDATE_BUCKET"' --name CURRENT 2>/dev/null' \
    | grep "/${GRAPH_NAME}/" | head -n1 || true)
  if [[ -n "$MATCH" ]]; then
    BUCKET="$CANDIDATE_BUCKET"
    DB_PREFIX="${MATCH#local/${CANDIDATE_BUCKET}/}"
    DB_PREFIX="${DB_PREFIX%/CURRENT}"
    break
  fi
done

echo "Detected DB prefix: bucket=${BUCKET}, prefix=${DB_PREFIX}"
[[ -n "$BUCKET" && -n "$DB_PREFIX" ]] || {
  echo "Failed to detect DB prefix for ${GRAPH_NAME}"
  exit 1
}
```

2. Observe objects before clear (CLI + optional MinIO UI check).

```bash
BEFORE_COUNT=$(docker run --rm --entrypoint /bin/sh --network "$HG_NET" \
  minio/mc:RELEASE.2025-08-13T08-35-41Z \
  -c 'if mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1; then \
        :; \
      elif mc alias set local http://cloud-storage-minio:9000 minioadmin minioadmin >/dev/null 2>&1; then \
        :; \
      else \
        echo "ERROR: cannot reach MinIO at minio:9000 or cloud-storage-minio:9000" >&2; \
        exit 1; \
      fi; \
      mc ls --recursive local/'"$BUCKET"'/'"$DB_PREFIX"' 2>/dev/null | wc -l')

echo "Objects before clear: ${BEFORE_COUNT}"

docker run --rm --entrypoint /bin/sh --network "$HG_NET" \
  minio/mc:RELEASE.2025-08-13T08-35-41Z \
  -c 'if mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1; then \
        :; \
      elif mc alias set local http://cloud-storage-minio:9000 minioadmin minioadmin >/dev/null 2>&1; then \
        :; \
      else \
        echo "ERROR: cannot reach MinIO at minio:9000 or cloud-storage-minio:9000" >&2; \
        exit 1; \
      fi; \
      mc ls --recursive local/'"$BUCKET"'/'"$DB_PREFIX"' 2>/dev/null | head -20'
```

Optional UI view: open `http://localhost:9001`, go to `${BUCKET}/${DB_PREFIX}`, and note object
names / timestamps.

3. Run clear action.

```bash
curl -s -X DELETE "http://localhost:8080/graphs/${GRAPH_NAME}/clear" \
  --get --data-urlencode "confirm_message=I'm sure to delete all data" | head -c 200
echo
sleep 5
```

4. Observe objects after clear (expected non-zero).

```bash
AFTER_COUNT=$(docker run --rm --entrypoint /bin/sh --network "$HG_NET" \
  minio/mc:RELEASE.2025-08-13T08-35-41Z \
  -c 'if mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1; then \
        :; \
      elif mc alias set local http://cloud-storage-minio:9000 minioadmin minioadmin >/dev/null 2>&1; then \
        :; \
      else \
        echo "ERROR: cannot reach MinIO at minio:9000 or cloud-storage-minio:9000" >&2; \
        exit 1; \
      fi; \
      mc ls --recursive local/'"$BUCKET"'/'"$DB_PREFIX"' 2>/dev/null | wc -l')

echo "Objects after clear (expected non-zero): ${AFTER_COUNT}"

docker run --rm --entrypoint /bin/sh --network "$HG_NET" \
  minio/mc:RELEASE.2025-08-13T08-35-41Z \
  -c 'if mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1; then \
        :; \
      elif mc alias set local http://cloud-storage-minio:9000 minioadmin minioadmin >/dev/null 2>&1; then \
        :; \
      else \
        echo "ERROR: cannot reach MinIO at minio:9000 or cloud-storage-minio:9000" >&2; \
        exit 1; \
      fi; \
      mc ls --recursive local/'"$BUCKET"'/'"$DB_PREFIX"' 2>/dev/null | head -20'
```

5. Manual no-orphan re-hydration verification.

```bash
echo "Vertex count immediately after clear (expected 0):"
curl -s --compressed "http://localhost:8080/graphs/${GRAPH_NAME}/graph/vertices" \
  | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('vertices',[])))"

docker restart cloud-storage-store0 cloud-storage-store1 cloud-storage-store2
sleep 20

echo "Vertex count after store restart (must remain 0):"
curl -s --compressed "http://localhost:8080/graphs/${GRAPH_NAME}/graph/vertices" \
  | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('vertices',[])))"
```

If the post-restart count is non-zero, stale data was re-hydrated (unexpected).

#### Optional Automation

Run the one-shot helper if you prefer scripted execution of the same checks:

```bash
bash "$REPO_ROOT/docker/cloud-storage/scripts/test-advanced-clear-semantics.sh"
```

Optional overrides:

```bash
GRAPH_NAME=hugegraph RESTART_WAIT_SECONDS=20 \
  bash "$REPO_ROOT/docker/cloud-storage/scripts/test-advanced-clear-semantics.sh"
```

**Success criteria:**
- ✅ `Objects before clear` is non-zero for the detected prefix
- ✅ `Objects after clear` is typically still non-zero (shared prefix is not purged by design)
- ✅ Vertex count immediately after clear is `0`
- ✅ Restart check returns vertex count `0` — cleared data is not re-hydrated from cloud
- ℹ️ Whole-prefix purge is validated by Advanced 3

### Advanced 3: DB Delete Callbacks (`onDBDeleteBegin` + `onDBDeleted`)

This step validates the DB-destroy lifecycle callbacks (not truncate/clear):

- `onDBDeleteBegin` writes a tombstone object (`_DELETED`) for the DB prefix
- `onDBDeleted` purges the DB prefix from cloud storage

It uses the **test-only** Store endpoint `/test/raftDelete/{groupId}` to destroy one partition
engine, which triggers `destroyGraphDB(...)` internally.

> **Important:** This is a **simulation of GraphDB deletion** via an internal test endpoint.
> It is useful for callback behavior verification, but it is not the production graph-delete API path.
> Use Advanced 2 to validate production `clear()` semantics and no-orphan re-hydration.

> **Warning:** This is destructive and intended only for callback verification. Run it near the
> end of manual testing. After this step, restart from Core Step 1 for a fresh cluster state.

Run this phase through the main end-to-end harness:

```bash
bash "$REPO_ROOT/docker/cloud-storage/scripts/test-graph-queries-and-sst.sh" --keep-stack
```

What this phase does:
1. picks a partition id and cloud DB prefix,
2. writes a probe object,
3. triggers `/test/raftDelete/{groupId}`,
4. polls store logs for callback markers,
5. verifies the prefix is fully purged.

**Success criteria:**
- ✅ Logs include `Cloud DB tombstone written` (from `onDBDeleteBegin`)
- ✅ Logs include `Cloud DB purge completed` (from `onDBDeleted`, may appear up to ~60s later)
- ✅ `Objects after db delete` is `0` for the detected DB prefix

### Advanced 4: Local Disk-Loss Recovery (Automated)

This scenario is covered by the main end-to-end harness (`recovery-test` phase):

```bash
bash "$REPO_ROOT/docker/cloud-storage/scripts/test-graph-queries-and-sst.sh" --keep-stack
```

What this phase does:
1. checks baseline vertex count (> 0),
2. triggers `/test/flush` on all stores,
3. verifies every bucket contains recovery metadata (`CURRENT` / `MANIFEST-*` / `OPTIONS-*`) and SSTs,
4. wipes each store's local state-machine data while preserving `raft/` and `snapshot/`,
5. restarts stores and validates recovered vertex count matches baseline,
6. checks logs for restore-consistency errors and stale `.hyd-tmp` leftovers.


**Success criteria:**
- ✅ Baseline vertex count is non-zero before the wipe
- ✅ Recovery metadata + SSTs exist in all three buckets before the wipe
- ✅ Recovered vertex count after restart equals baseline
- ✅ No `Cloud restore inconsistent` log errors or stale `.hyd-tmp` files

## Cleanup (Optional)

When done with manual verification, stop and clean up. Use the **same project and compose file
the harness created the stack with** — the committed `docker-compose.yml` declares
`name: cloud-storage`, so a `down` against it targets a different project and leaves the
`cloud-storage-test` stack running:

```bash
export COMPOSE_PROJECT_NAME=cloud-storage-test
CF="$REPO_ROOT/docker/cloud-storage/.generated/docker-compose.yml"
docker compose -f "$CF" down
```

To also remove the store data volumes and the generated artifacts:

```bash
export COMPOSE_PROJECT_NAME=cloud-storage-test
CF="$REPO_ROOT/docker/cloud-storage/.generated/docker-compose.yml"
docker compose -f "$CF" down -v
rm -rf $REPO_ROOT/docker/cloud-storage/.generated/
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

**Optional partition check shows empty**
```bash
# Data may still be in RocksDB memory, not yet in partitions
# This is normal - proceed to Core Step 5 to flush

# Or check if data was actually written
docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml logs store0 | grep -i "vertex\|edge\|insert" | tail -10
```

**Core Step 5: Flush endpoint fails or stores become unhealthy**
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

**Core Step 5 / Advanced 1: Buckets showing 0 files after flush**

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
   type resolve_hg_net >/dev/null 2>&1 || { echo "Run Core Step 1 helper first"; exit 1; }
   resolve_hg_net
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
$REPO_ROOT/docker/cloud-storage/scripts/test-graph-queries-and-sst.sh
```

The script detects your host architecture and applies appropriate defaults automatically.

### Custom overrides on ARM64

If you need different JVM flags or runtime images:

```bash
HG_DOCKER_DEFAULT_PLATFORM=linux/amd64 \
HG_PD_JAVA_RUNTIME_IMAGE=eclipse-temurin:17-jre \
HG_STORE_JAVA_RUNTIME_IMAGE=eclipse-temurin:17-jre \
HG_STORE_JAVA_OPTS="-XX:+UseSerialGC -XX:-UseCompressedOops -XX:-UseCompressedClassPointers" \
$REPO_ROOT/docker/cloud-storage/scripts/test-graph-queries-and-sst.sh
```

But in most cases, running without overrides should work:

```bash
$REPO_ROOT/docker/cloud-storage/scripts/test-graph-queries-and-sst.sh
```

If old containers are still running, force recreate with fresh env:

```bash
docker compose -f $REPO_ROOT/docker/cloud-storage/docker-compose.yml down
$REPO_ROOT/docker/cloud-storage/scripts/test-graph-queries-and-sst.sh

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

Use this sequence for the end-to-end manual flow:

1. **Core Step 1:** Set environment and start infrastructure (`--keep-stack --infra-only`)
2. **Core Step 2:** Wait for all services to become healthy
3. **Core Step 3:** Create graph schema
4. **Core Step 4:** Load 150+ vertices
5. **Core Step 5:** Flush stores and verify SST + metadata in MinIO UI
6. **Advanced 1 (optional):** Run strict script assertions
7. **Advanced 2 (optional):** Verify `clear()` semantics and no-orphan re-hydration
8. **Advanced 3 (optional):** Verify DB delete callbacks and remote prefix purge
9. **Advanced 4 (optional):** Simulate local disk loss and validate cloud re-hydration
10. **Cleanup (optional):** Tear down containers/volumes

- **Note:** For manual Core Step 3+ workflows, always use Core Step 1 with `--infra-only` so
  automation starts infrastructure but doesn't mutate graph state.

For automated validation (including local state wipe + cloud re-hydration checks), run:

```bash
$REPO_ROOT/docker/cloud-storage/scripts/test-graph-queries-and-sst.sh --keep-stack
```


**Success = SST and recovery metadata appear in all three buckets after Core Step 5**
(or after Advanced 1 strict checks).

## What Gets Verified

✅ Graph schema creation works  
✅ Vertex/edge insertion works  
✅ RocksDB SST file generation via explicit flush/compaction  
✅ Cloud storage plugin uploads SST files to MinIO  
✅ Multiple buckets receive files consistently  
✅ A consistent `CURRENT` + `MANIFEST-*` + `OPTIONS-*` metadata set is mirrored alongside SSTs  
✅ `clear()` keeps graph data empty after restart (optional Advanced 2)  
✅ `clear()` does not require whole-prefix purge in shared cloud DB layouts (optional Advanced 2)  
✅ DB delete callbacks write tombstone and purge remote DB prefix (optional advanced step)  
✅ A store recovers all data from cloud after losing its local RocksDB state (automated script)

## Notes

- S3 provider JAR must be in `/hugegraph-store/plugins/` for `ServiceLoader` discovery
- Plugin dependency staging filters extra SLF4J binding jars to avoid duplicate binding warnings at runtime
- Cloud settings are read from environment variables at store startup time
- **Minimum data size:** 150+ vertices is recommended to trigger RocksDB SST file generation. Very small datasets may not create any SST files.
- MinIO buckets are pre-created by `minio-init` service: `hugegraph-store0`, `hugegraph-store1`, `hugegraph-store2`



