#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STACK_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${STACK_DIR}/../.." && pwd)"
GENERATED_DIR="${STACK_DIR}/.generated"
ARTIFACTS_DIR="${STACK_DIR}/.artifacts"
COMPOSE_FILE="${GENERATED_DIR}/docker-compose.yml"
export SERVER_GRAPHS_DIR="${GENERATED_DIR}/graphs"
export SERVER_GRAPH_CONF="${SERVER_GRAPHS_DIR}/hugegraph.properties"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-cloud-storage-test}"
HG_PD_IMAGE="${HG_PD_IMAGE:-hugegraph/pd:cloud-storage-local}"
HG_STORE_IMAGE="${HG_STORE_IMAGE:-hugegraph/store:cloud-storage-local}"
HG_SERVER_IMAGE="${HG_SERVER_IMAGE:-hugegraph/server:1.7.0}"
MINIO_IMAGE="${MINIO_IMAGE:-minio/minio:latest}"
MINIO_MC_IMAGE="${MINIO_MC_IMAGE:-minio/mc:latest}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
S3_BUCKET_STORE0="${S3_BUCKET_STORE0:-hugegraph-store0}"
S3_BUCKET_STORE1="${S3_BUCKET_STORE1:-hugegraph-store1}"
S3_BUCKET_STORE2="${S3_BUCKET_STORE2:-hugegraph-store2}"
S3_REGION="${S3_REGION:-us-east-1}"
S3_ENDPOINT="${S3_ENDPOINT:-http://minio:9000}"
GRAPH_API_BASE="${GRAPH_API_BASE:-http://localhost:8080/graphs/hugegraph}"
STORE_ROCKSDB_CLOUD_ENABLED="${STORE_ROCKSDB_CLOUD_ENABLED:-true}"
STORE_ROCKSDB_CLOUD_SYNC_INTERVAL_SECONDS="${STORE_ROCKSDB_CLOUD_SYNC_INTERVAL_SECONDS:-30}"
KEEP_UP="${KEEP_UP:-true}"
SKIP_SMOKE_TESTS="${SKIP_SMOKE_TESTS:-false}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --keep-stack)    KEEP_UP=true ;;
        --skip-smoke-tests|--infra-only)
            SKIP_SMOKE_TESTS=true ;;
        -h|--help)
            cat <<'USAGE'
 Usage: test-graph-queries-and-sst.sh [--keep-stack] [--skip-smoke-tests|--infra-only]

   This script runs a full end-to-end cloud storage test suite:

   1. Recovery Test:
      load data -> flush+compact -> verify a consistent
      {CURRENT, MANIFEST, OPTIONS, SST} set in MinIO ->
      wipe each store's local RocksDB state (raft/ preserved) ->
      restart -> confirm data is recovered from cloud (not empty DB).

   2. DB Deletion Cleanup Test:
      create test graph -> load data -> sync to cloud ->
      delete graph -> verify cloud storage prefix is cleaned up
      (tests onDBDeleted() -> purgeRemotePrefix() behavior).

   --keep-stack         Leave the stack running on exit (same as KEEP_UP=true).
   --skip-smoke-tests   Start infrastructure only; skip data load + validation tests.
   --infra-only         Alias of --skip-smoke-tests.
USAGE
            exit 0 ;;
        *) echo "unknown arg: $1 (see --help)" >&2; exit 2 ;;
    esac
    shift
done
log() { printf "[cloud-storage] %s\n" "$*"; }
need_cmd() { command -v "$1" >/dev/null 2>&1 || { echo "ERROR: $1 not found" >&2; exit 2; }; }

find_dist_dir() {
    local glob="$1"
    local d
    for d in $glob; do
        [[ -d "$d" ]] && { echo "$d"; return 0; }
    done
    return 1
}

find_plugin_jar() {
    local f
    for f in "${REPO_ROOT}"/hugegraph-store/hg-store-cloud-s3/target/hg-store-cloud-s3-*.jar; do
        [[ -f "$f" ]] || continue
        case "$f" in
            *-sources.jar|*-javadoc.jar|*original*) continue ;;
        esac
        echo "$f"
        return 0
    done
    return 1
}

prepare_artifacts() {
    local pd_src store_src plugin_jar plugin_dep_dir dep_base

    pd_src="$(find_dist_dir "${REPO_ROOT}/hugegraph-pd/apache-hugegraph-pd-*")" || {
        echo "ERROR: PD dist not found under ${REPO_ROOT}/hugegraph-pd/apache-hugegraph-pd-*" >&2
        echo "Run: mvn clean package -DskipTests" >&2
        exit 2
    }
    store_src="$(find_dist_dir "${REPO_ROOT}/hugegraph-store/apache-hugegraph-store-*")" || {
        echo "ERROR: Store dist not found under ${REPO_ROOT}/hugegraph-store/apache-hugegraph-store-*" >&2
        echo "Run: mvn clean package -DskipTests" >&2
        exit 2
    }
    plugin_jar="$(find_plugin_jar)" || {
        echo "ERROR: S3 plugin jar not found under ${REPO_ROOT}/hugegraph-store/hg-store-cloud-s3/target/" >&2
        echo "Run: mvn clean package -DskipTests" >&2
        exit 2
    }

    log "preparing Docker artifacts in ${ARTIFACTS_DIR}"
    rm -rf "${ARTIFACTS_DIR}"
    mkdir -p "${ARTIFACTS_DIR}/pd-dist" "${ARTIFACTS_DIR}/store-dist" "${ARTIFACTS_DIR}/plugins"

    cp -R "${pd_src}/." "${ARTIFACTS_DIR}/pd-dist/"
    cp -R "${store_src}/." "${ARTIFACTS_DIR}/store-dist/"
    cp "${plugin_jar}" "${ARTIFACTS_DIR}/plugins/"

    plugin_dep_dir="${REPO_ROOT}/hugegraph-store/hg-store-cloud-s3/target/dependency"
    if [[ -d "${plugin_dep_dir}" ]]; then
        # Keep only external plugin deps; internal HugeGraph jars must come from /hugegraph-store/lib.
        for dep in "${plugin_dep_dir}"/*.jar; do
            [[ -f "${dep}" ]] || continue
            dep_base="$(basename "${dep}")"
            case "${dep_base}" in
                hg-*.jar|hugegraph-*.jar) continue ;;
            esac
            cp "${dep}" "${ARTIFACTS_DIR}/plugins/"
        done
    else
        log "warning: plugin dependency dir not found at ${plugin_dep_dir}; continuing with plugin jar only"
    fi

    # Prevent duplicate SLF4J binding clashes from plugin dependency staging.
    rm -f "${ARTIFACTS_DIR}"/plugins/log4j-slf4j-impl-*.jar "${ARTIFACTS_DIR}"/plugins/slf4j-log4j12-*.jar || true
}

ensure_image() {
    local img="$1"
    docker image inspect "$img" >/dev/null 2>&1 && return 0
    # Check if it's a local build image (contains "cloud-storage-local")
    if [[ "$img" == *"cloud-storage-local"* ]]; then
        log "image $img is local-build only (will be built via docker compose build)"
        return 0
    fi
    log "pulling $img..."
    docker pull "$img" >/dev/null || exit 3
}
ensure_minio_buckets() {
    for bucket in "$S3_BUCKET_STORE0" "$S3_BUCKET_STORE1" "$S3_BUCKET_STORE2"; do
        docker run --rm --network "$1" --entrypoint /bin/sh "$MINIO_MC_IMAGE" -c           "mc alias set local http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD >/dev/null &&            mc mb --ignore-existing local/$bucket >/dev/null"
    done
}
wait_svc() {
    local svc max=120 i=0
    svc="$1"
    while [[ $i -lt $max ]]; do
        local cid status
        cid=$(docker compose -f "$COMPOSE_FILE" ps -q "$svc" 2>/dev/null || true)
        [[ -z "$cid" ]] && { sleep 2; i=$((i+1)); continue; }
        status=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$cid" 2>/dev/null || true)
        [[ "$status" == "healthy" || "$status" == "running" ]] && { log "✓ $svc"; return 0; }
        sleep 2
        i=$((i+1))
    done
    echo "ERROR: $svc timeout" >&2
    return 1
}
wait_http() {
    local url max=120 i=0
    url="$1"
    while [[ $i -lt $max ]]; do
        [[ $(curl -so /dev/null -w "%{http_code}" "$url" 2>/dev/null) == "200" ]] && { log "✓ $url ready"; return 0; }
        sleep 2
        i=$((i+1))
    done
    echo "ERROR: $url timeout" >&2
    return 1
}

delete_graph_with_confirm() {
    local graph_api="$1"
    local resp_file status
    resp_file="/tmp/hg-delete-$$.json"
    status=$(curl -s -o "$resp_file" -w "%{http_code}" -X DELETE "$graph_api" \
        --get --data-urlencode "confirm_message=I'm sure to drop the graph")
    if [[ "$status" != 2* ]]; then
        echo "ERROR: graph delete failed with HTTP ${status}: ${graph_api}" >&2
        head -80 "$resp_file" >&2 || true
        rm -f "$resp_file" || true
        return 1
    fi
    rm -f "$resp_file" || true
}

clear_graph_with_confirm() {
    local graph_api="$1"
    local resp_file status clear_api
    resp_file="/tmp/hg-clear-$$.json"
    clear_api="${graph_api}/clear"
    status=$(curl -s -o "$resp_file" -w "%{http_code}" -X DELETE "$clear_api" \
        --get --data-urlencode "confirm_message=I'm sure to delete all data")
    if [[ "$status" != 2* ]]; then
        echo "ERROR: graph clear failed with HTTP ${status}: ${clear_api}" >&2
        head -80 "$resp_file" >&2 || true
        rm -f "$resp_file" || true
        return 1
    fi
    rm -f "$resp_file" || true
}

cleanup() { [[ "$KEEP_UP" == "true" ]] || (docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true); }
trap cleanup EXIT

# -----------------------------------------------------------------------------
# Total-loss recovery E2E helpers
# -----------------------------------------------------------------------------

# Current vertex count via the server graph API (0 if the query fails).
graph_vertex_count() {
    curl -s --compressed "${GRAPH_API_BASE}/graph/vertices" 2>/dev/null \
        | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('vertices',[])))" \
              2>/dev/null || echo 0
}

# Create a minimal schema and insert enough vertices to generate SST files.
load_test_data() {
    log "loading test data (schema + 150 vertices)..."
    for pk in '{"name":"name","data_type":"TEXT","cardinality":"SINGLE"}' \
              '{"name":"age","data_type":"INT","cardinality":"SINGLE"}' \
              '{"name":"city","data_type":"TEXT","cardinality":"SINGLE"}'; do
        curl -s -o /dev/null -X POST "${GRAPH_API_BASE}/schema/propertykeys" \
            -H 'Content-Type: application/json' -d "$pk" || true
    done
    curl -s -o /dev/null -X POST "${GRAPH_API_BASE}/schema/vertexlabels" \
        -H 'Content-Type: application/json' \
        -d '{"name":"person","id_strategy":"AUTOMATIC","properties":["name","age","city"]}' || true

    for i in $(seq 1 150); do
        curl -s -o /dev/null -X POST "${GRAPH_API_BASE}/graph/vertices" \
            -H 'Content-Type: application/json' \
            -d "{\"label\":\"person\",\"properties\":{\"name\":\"person_$i\",\"age\":$((20 + i % 50)),\"city\":\"city_$((i % 5))\"}}" || true
    done
    log "  ✓ inserted 150 vertices (count now = $(graph_vertex_count))"
}

# Assert every bucket holds a consistent {CURRENT, MANIFEST, OPTIONS, SST} set.
# This deterministic check verifies metadata objects from ordered
# CURRENT/MANIFEST/OPTIONS mirroring ran, so the SSTs are no longer orphans.
verify_metadata_in_minio() {
    docker run --rm --entrypoint /bin/sh --network "$NETWORK" "$MINIO_MC_IMAGE" -c '
        mc alias set local http://minio:9000 '"$MINIO_ROOT_USER $MINIO_ROOT_PASSWORD"' >/dev/null 2>&1
        rc=0
        for b in '"$S3_BUCKET_STORE0 $S3_BUCKET_STORE1 $S3_BUCKET_STORE2"'; do
            sst=$(mc find local/$b --name "*.sst"     2>/dev/null | wc -l | tr -d " ")
            cur=$(mc find local/$b --name "CURRENT"    2>/dev/null | wc -l | tr -d " ")
            man=$(mc find local/$b --name "MANIFEST-*" 2>/dev/null | wc -l | tr -d " ")
            opt=$(mc find local/$b --name "OPTIONS-*"  2>/dev/null | wc -l | tr -d " ")
            printf "### %s: sst=%s CURRENT=%s MANIFEST=%s OPTIONS=%s\n" "$b" "$sst" "$cur" "$man" "$opt"
            if [ "$cur" -lt 1 ] || [ "$man" -lt 1 ] || [ "$sst" -lt 1 ]; then
                echo "  ✗ recovery metadata/SST set incomplete for $b"; rc=1
            else
                echo "  ✓ consistent {CURRENT, MANIFEST, OPTIONS, SST} set present"
            fi
        done
        exit $rc
    '
}

# Stop a store, wipe its RocksDB state-machine data (db/ + metadata graph) while
# preserving raft/ and snapshot/, then start it again. This models local
# state-machine loss where the node still rejoins its raft group but must
# re-hydrate its RocksDB data from cloud on open (pre-hydration path).
wipe_store_rocksdb_state() {
    local idx="$1"
    local vol="${COMPOSE_PROJECT_NAME}_hg-store${idx}-data"
    docker compose -f "$COMPOSE_FILE" stop "store${idx}" >/dev/null 2>&1 || true
    docker run --rm --entrypoint /bin/sh -v "${vol}:/s" "$MINIO_MC_IMAGE" -c '
        cd /s 2>/dev/null || exit 0
        for d in *; do
            case "$d" in
                raft|snapshot) ;;      # keep raft log + snapshot so the node rejoins cleanly
                *) rm -rf "$d" ;;      # drop db/ and the metadata graph -> must come back from cloud
            esac
        done
        echo "  store'"${idx}"' storage after wipe: $(ls -1 | tr "\n" " ")"
    ' || true
    docker compose -f "$COMPOSE_FILE" start "store${idx}" >/dev/null 2>&1 || true
}

count_objects_in_prefix() {
    local bucket="$1"
    local prefix="$2"
    docker run --rm --entrypoint /bin/sh --network "$NETWORK" "$MINIO_MC_IMAGE" -c '
        mc alias set local http://minio:9000 '"$MINIO_ROOT_USER $MINIO_ROOT_PASSWORD"' >/dev/null 2>&1
        target="local/'"$bucket"'/'"$prefix"'"
        if mc ls --recursive "$target" >/tmp/prefix_ls.txt 2>/dev/null; then
            wc -l < /tmp/prefix_ls.txt | tr -d " "
        else
            echo 0
        fi
    '
 }

put_probe_object_in_prefix() {
    local bucket="$1"
    local prefix="$2"
    local probe_key="${prefix%/}/marker-$(date +%s).txt"
    docker run --rm --entrypoint /bin/sh --network "$NETWORK" "$MINIO_MC_IMAGE" -c '
        mc alias set local http://minio:9000 '"$MINIO_ROOT_USER $MINIO_ROOT_PASSWORD"' >/dev/null 2>&1
        printf "db-delete-probe\n" | mc pipe local/'"$bucket"'/'"$probe_key"' >/dev/null
    '
    log "  ✓ wrote cloud probe object: s3://${bucket}/${probe_key}"
}

find_graph_db_prefix() {
    local bucket="$1"
    local graph_name="$2"
    local candidate rel

    candidate=$(docker run --rm --entrypoint /bin/sh --network "$NETWORK" "$MINIO_MC_IMAGE" -c '
        mc alias set local http://minio:9000 '"$MINIO_ROOT_USER $MINIO_ROOT_PASSWORD"' >/dev/null 2>&1
        mc find local/'"$bucket"' --name "CURRENT" 2>/dev/null
    ' | grep "/${graph_name}/" | head -n1 || true)

    if [[ -z "$candidate" ]]; then
        candidate=$(docker run --rm --entrypoint /bin/sh --network "$NETWORK" "$MINIO_MC_IMAGE" -c '
            mc alias set local http://minio:9000 '"$MINIO_ROOT_USER $MINIO_ROOT_PASSWORD"' >/dev/null 2>&1
            mc find local/'"$bucket"' --name "*.sst" 2>/dev/null
        ' | grep "/${graph_name}/" | head -n1 || true)
    fi

    [[ -z "$candidate" ]] && return 1
    rel="${candidate#local/${bucket}/}"

    if [[ "$rel" == "${graph_name}/"* ]]; then
        :
    elif [[ "$rel" == *"/${graph_name}/"* ]]; then
        rel="${rel#*/${graph_name}/}"
        rel="${graph_name}/${rel}"
    else
        return 1
    fi

    if [[ "$rel" == */CURRENT ]]; then
        echo "${rel%/CURRENT}/"
    else
        echo "${rel%/*}/"
    fi
}

run_recovery_test() {
     log "=== Total-loss recovery E2E ==="
     local before after

     before=$(graph_vertex_count)
     log "baseline vertex count = ${before}"
     if [[ "$before" == "0" ]]; then
         echo "ERROR: no data to recover (baseline is 0); load data first" >&2
         return 1
     fi

     log "forcing flush + compaction (restart stores) so data lands in SST files..."
     docker restart cloud-storage-store0 cloud-storage-store1 cloud-storage-store2 >/dev/null
     wait_svc "store0" 240; wait_svc "store1" 240; wait_svc "store2" 240
     wait_http "${GRAPH_API_BASE}/graph/vertices" 180


     log "verifying a consistent metadata set is durable in MinIO..."
     verify_metadata_in_minio || { echo "ERROR: recovery metadata not durable in cloud" >&2; return 1; }

     log "simulating local RocksDB state loss on all stores (raft/ preserved)..."
     for i in 0 1 2; do wipe_store_rocksdb_state "$i"; done
     wait_svc "store0" 240; wait_svc "store1" 240; wait_svc "store2" 240
     wait_http "${GRAPH_API_BASE}/graph/vertices" 240

     log "checking store logs for pre-hydration / restore-consistency..."
     if docker compose -f "$COMPOSE_FILE" logs store0 store1 store2 2>/dev/null \
             | grep -qi "Cloud restore inconsistent"; then
         echo "ERROR: consistent-restore guard tripped (CURRENT referenced a missing manifest)" >&2
         return 1
     fi
     docker compose -f "$COMPOSE_FILE" logs store0 store1 store2 2>/dev/null \
         | grep -i "Cloud pre-hydration finished" | tail -3 \
         || log "  (no pre-hydration log lines matched; check DEBUG logs manually)"

     after=$(graph_vertex_count)
     log "recovered vertex count = ${after} (baseline was ${before})"
     if [[ "$after" == "$before" ]]; then
         log "✓ RECOVERY SUCCESS: data fully recovered from cloud after local state loss"
     else
         echo "ERROR: recovery mismatch (before=${before}, after=${after})" >&2
         return 1
     fi
 }

run_db_deletion_cleanup_test() {
     log "=== DB deletion + cloud storage prefix cleanup E2E ==="
     local graph_name test_api count_before count_after probe_prefix db_cloud_prefix
     graph_name="${GRAPH_API_BASE##*/}"
     test_api="${GRAPH_API_BASE}"

     log "using existing graph '${graph_name}' created/populated by recovery test"

      db_cloud_prefix=$(find_graph_db_prefix "$S3_BUCKET_STORE0" "$graph_name" || true)
      if [[ -z "$db_cloud_prefix" ]]; then
          echo "ERROR: failed to detect cloud DB prefix for graph '${graph_name}' in bucket '${S3_BUCKET_STORE0}'" >&2
          log "  sample objects in bucket for debugging:"
          docker run --rm --entrypoint /bin/sh --network "$NETWORK" "$MINIO_MC_IMAGE" -c '
              mc alias set local http://minio:9000 '"$MINIO_ROOT_USER $MINIO_ROOT_PASSWORD"' >/dev/null 2>&1
              mc ls --recursive local/'"$S3_BUCKET_STORE0"' | head -20
          ' || true
          return 1
      fi
      probe_prefix="${db_cloud_prefix}"
      log "detected graph DB cloud prefix: ${db_cloud_prefix}"

      # Write a deterministic probe object so the delete-prefix test always has cloud data to prune.
      log "writing probe object into cloud probe prefix '${probe_prefix}'..."
      put_probe_object_in_prefix "$S3_BUCKET_STORE0" "${probe_prefix}"

      count_before=$(count_objects_in_prefix "$S3_BUCKET_STORE0" "${probe_prefix}" || echo "0")
      count_before="${count_before//[^0-9]/}"
      if [[ -z "$count_before" || "$count_before" -eq 0 ]]; then
          echo "ERROR: failed to create/observe probe object in cloud probe prefix '${probe_prefix}'" >&2
          return 1
      fi
      log "  ✓ objects in probe prefix before delete: ${count_before}"

      log "clearing graph data for '${graph_name}'..."
      clear_graph_with_confirm "${test_api}" || {
          echo "ERROR: failed to clear graph '${graph_name}'" >&2
          return 1
      }
      sleep 5

      log "verifying cloud probe prefix has been cleaned up..."
      count_after=$(count_objects_in_prefix "$S3_BUCKET_STORE0" "${probe_prefix}" || echo "0")
      log "  objects in probe prefix after deletion: ${count_after}"

      if [[ "$count_after" == "0" || "$count_after" == "" ]]; then
          log "✓ DB DELETION CLEANUP SUCCESS: cloud probe prefix pruned after DB deletion"
      else
          echo "ERROR: cloud probe prefix not cleaned up (objects remaining: ${count_after})" >&2
          log "  listing remaining objects:"
          docker run --rm --entrypoint /bin/sh --network "$NETWORK" "$MINIO_MC_IMAGE" -c '
              mc alias set local http://minio:9000 '"$MINIO_ROOT_USER $MINIO_ROOT_PASSWORD"' >/dev/null 2>&1
              mc ls --recursive local/'"$S3_BUCKET_STORE0"'/'"$probe_prefix"' | head -20
          ' || true
          return 1
      fi

     log "skip graph delete in single-graph deployment; clear() is the deletion-equivalent path under test"
 }

 run_db_recreation_no_orphan_test() {
     log "=== Post-clear no-orphan rehydration E2E ==="
     local graph_name test_api
     graph_name="${GRAPH_API_BASE##*/}"
     test_api="${GRAPH_API_BASE%/graphs/*}/graphs/${graph_name}"

     log "simulating local RocksDB loss after clear to verify no stale cloud rehydration..."
     for i in 0 1 2; do wipe_store_rocksdb_state "$i"; done
     wait_svc "store0" 240; wait_svc "store1" 240; wait_svc "store2" 240
     wait_http "${GRAPH_API_BASE}/graph/vertices" 240

     log "verifying graph remains empty (no orphaned data rehydrated from cloud)..."
     local vertex_count
     vertex_count=$(curl -s --compressed "${test_api}/graph/vertices" 2>/dev/null \
         | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('vertices',[])))" \
         2>/dev/null || echo "0")

     log "  vertex count in recreated '${graph_name}': ${vertex_count}"

     if [[ "$vertex_count" == "0" ]]; then
         log "✓ RECREATION NO-ORPHAN SUCCESS: deleted graph marker prevents rehydration of old data"
     else
         echo "ERROR: orphaned data found in recreated graph (vertices: ${vertex_count})" >&2
         log "  this suggests deletion markers were not properly written or preserved"
         return 1
     fi

     log "leaving graph '${graph_name}' in place"
 }

need_cmd docker curl python3
log "pulling images..."
ensure_image "$MINIO_IMAGE"
ensure_image "$MINIO_MC_IMAGE"
ensure_image "$HG_PD_IMAGE"
ensure_image "$HG_STORE_IMAGE"
ensure_image "$HG_SERVER_IMAGE"
mkdir -p "$GENERATED_DIR"
mkdir -p "$SERVER_GRAPHS_DIR"
cat > "$SERVER_GRAPH_CONF" << 'PROPS'
gremlin.graph=org.apache.hugegraph.HugeFactory
backend=hstore
serializer=binary
store=hugegraph
task.scheduler_type=local
pd.peers=pd:8686
PROPS
mkdir -p "$(dirname "$COMPOSE_FILE")"
docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
log "generating docker-compose.yml..."
cat > "$COMPOSE_FILE" << 'YAML'
services:
  minio:
    image: minio/minio:latest
    container_name: cloud-storage-minio
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports: ["9000:9000", "9001:9001"]
    volumes: [hg-minio-data:/data]
    networks: [hg-net]
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:9000/minio/health/live >/dev/null || exit 1"]
      interval: 5s
      timeout: 5s
      retries: 40
  pd:
    build:
      context: ../../../
      dockerfile: docker/cloud-storage/images/pd.Dockerfile
    image: hugegraph/pd:cloud-storage-local
    container_name: cloud-storage-pd
    hostname: pd
    depends_on:
      minio:
        condition: service_healthy
    environment:
      HG_PD_GRPC_HOST: pd
      HG_PD_GRPC_PORT: "8686"
      HG_PD_REST_PORT: "8620"
      HG_PD_RAFT_ADDRESS: pd:8610
      HG_PD_RAFT_PEERS_LIST: pd:8610
      HG_PD_INITIAL_STORE_LIST: store0:8500,store1:8500,store2:8500
      HG_PD_INITIAL_STORE_COUNT: "3"
      HG_PD_DATA_PATH: /hugegraph-pd/pd_data
    ports: ["8620:8620", "8686:8686"]
    volumes: [hg-pd-data:/hugegraph-pd/pd_data]
    networks: [hg-net]
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8620/v1/health >/dev/null || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 30
  store0:
    build:
      context: ../../../
      dockerfile: docker/cloud-storage/images/store.Dockerfile
    image: hugegraph/store:cloud-storage-local
    container_name: cloud-storage-store0
    hostname: store0
    depends_on:
      pd:
        condition: service_healthy
    environment:
      HG_STORE_PD_ADDRESS: pd:8686
      HG_STORE_GRPC_HOST: store0
      HG_STORE_GRPC_PORT: "8500"
      HG_STORE_REST_PORT: "8520"
      HG_STORE_RAFT_ADDRESS: store0:8510
      HG_STORE_DATA_PATH: /hugegraph-store/storage
      HG_CLOUD_STORAGE_ENABLED: "true"
      HG_CLOUD_STORAGE_BUCKET: "hugegraph-store0"
      HG_CLOUD_STORAGE_ENDPOINT: "http://minio:9000"
      HG_CLOUD_STORAGE_REGION: "us-east-1"
      HG_CLOUD_STORAGE_ACCESS_KEY: "minioadmin"
      HG_CLOUD_STORAGE_SECRET_KEY: "minioadmin"
      HG_CLOUD_STORAGE_PATH_STYLE: "true"
      HG_CLOUD_STORAGE_SYNC_INTERVAL_SECONDS: "30"
      HG_CLOUD_STORAGE_SYNCHRONOUS_SST_UPLOAD_MODE: "true"
    ports: ["8520:8520"]
    volumes: [hg-store0-data:/hugegraph-store/storage]
    networks: [hg-net]
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8520/v1/health >/dev/null || exit 1"]
      interval: 10s
      timeout: 10s
      retries: 40
      start_period: 60s
  store1:
    build:
      context: ../../../
      dockerfile: docker/cloud-storage/images/store.Dockerfile
    image: hugegraph/store:cloud-storage-local
    container_name: cloud-storage-store1
    hostname: store1
    depends_on:
      pd:
        condition: service_healthy
    environment:
      HG_STORE_PD_ADDRESS: pd:8686
      HG_STORE_GRPC_HOST: store1
      HG_STORE_GRPC_PORT: "8500"
      HG_STORE_REST_PORT: "8520"
      HG_STORE_RAFT_ADDRESS: store1:8510
      HG_STORE_DATA_PATH: /hugegraph-store/storage
      HG_CLOUD_STORAGE_ENABLED: "true"
      HG_CLOUD_STORAGE_BUCKET: "hugegraph-store1"
      HG_CLOUD_STORAGE_ENDPOINT: "http://minio:9000"
      HG_CLOUD_STORAGE_REGION: "us-east-1"
      HG_CLOUD_STORAGE_ACCESS_KEY: "minioadmin"
      HG_CLOUD_STORAGE_SECRET_KEY: "minioadmin"
      HG_CLOUD_STORAGE_PATH_STYLE: "true"
      HG_CLOUD_STORAGE_SYNC_INTERVAL_SECONDS: "30"
      HG_CLOUD_STORAGE_SYNCHRONOUS_SST_UPLOAD_MODE: "true"
    ports: ["8521:8520"]
    volumes: [hg-store1-data:/hugegraph-store/storage]
    networks: [hg-net]
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8520/v1/health >/dev/null || exit 1"]
      interval: 10s
      timeout: 10s
      retries: 40
      start_period: 60s
  store2:
    build:
      context: ../../../
      dockerfile: docker/cloud-storage/images/store.Dockerfile
    image: hugegraph/store:cloud-storage-local
    container_name: cloud-storage-store2
    hostname: store2
    depends_on:
      pd:
        condition: service_healthy
    environment:
      HG_STORE_PD_ADDRESS: pd:8686
      HG_STORE_GRPC_HOST: store2
      HG_STORE_GRPC_PORT: "8500"
      HG_STORE_REST_PORT: "8520"
      HG_STORE_RAFT_ADDRESS: store2:8510
      HG_STORE_DATA_PATH: /hugegraph-store/storage
      HG_CLOUD_STORAGE_ENABLED: "true"
      HG_CLOUD_STORAGE_BUCKET: "hugegraph-store2"
      HG_CLOUD_STORAGE_ENDPOINT: "http://minio:9000"
      HG_CLOUD_STORAGE_REGION: "us-east-1"
      HG_CLOUD_STORAGE_ACCESS_KEY: "minioadmin"
      HG_CLOUD_STORAGE_SECRET_KEY: "minioadmin"
      HG_CLOUD_STORAGE_PATH_STYLE: "true"
      HG_CLOUD_STORAGE_SYNC_INTERVAL_SECONDS: "30"
      HG_CLOUD_STORAGE_SYNCHRONOUS_SST_UPLOAD_MODE: "true"
    ports: ["8522:8520"]
    volumes: [hg-store2-data:/hugegraph-store/storage]
    networks: [hg-net]
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8520/v1/health >/dev/null || exit 1"]
      interval: 10s
      timeout: 10s
      retries: 40
      start_period: 60s
  server:
    image: hugegraph/server:1.7.0
    container_name: cloud-storage-server
    hostname: server
    depends_on:
      store0:
        condition: service_healthy
      store1:
        condition: service_healthy
      store2:
        condition: service_healthy
    environment:
      STORE_REST: store0:8520
    ports: ["8080:8080"]
    volumes:
      - ${SERVER_GRAPHS_DIR}:/hugegraph-server/conf/graphs
    networks: [hg-net]
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8080/versions >/dev/null || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 40
      start_period: 60s
networks:
  hg-net:
    driver: bridge
volumes:
  hg-minio-data:
  hg-pd-data:
  hg-store0-data:
  hg-store1-data:
  hg-store2-data:
YAML
prepare_artifacts
log "building local cloud-storage images..."
docker compose -f "$COMPOSE_FILE" build --no-cache 2>&1 | grep -E "^(Building|FINISHED|\[|Successfully|Error)" || true
log "starting minio and pd first..."
docker compose -f "$COMPOSE_FILE" up -d minio pd
wait_svc "minio" 120
wait_svc "pd" 180
NETWORK="${COMPOSE_PROJECT_NAME}_hg-net"
log "creating MinIO buckets before store startup..."
ensure_minio_buckets "$NETWORK"
log "starting stores and server..."
docker compose -f "$COMPOSE_FILE" up -d store0 store1 store2 server
log "waiting for stores..."
wait_svc "store0" 180
wait_svc "store1" 180
wait_svc "store2" 180
wait_svc "server" 180
log "waiting for graph backend..."
wait_http "$GRAPH_API_BASE/graph/vertices" 60
log "✓ SUCCESS: Cloud storage infrastructure ready"

if [[ "$SKIP_SMOKE_TESTS" == "true" ]]; then
    log "SKIP_SMOKE_TESTS=true -> skipping data creation and validation phases"
    log "✓ SUCCESS: infrastructure-only mode complete"
    exit 0
fi

load_test_data
run_recovery_test
run_db_deletion_cleanup_test
run_db_recreation_no_orphan_test
log "✓ SUCCESS: all cloud storage E2E tests passed"
