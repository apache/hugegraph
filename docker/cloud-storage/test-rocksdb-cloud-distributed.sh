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
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
GENERATED_DIR="${SCRIPT_DIR}/.generated"
COMPOSE_FILE="${GENERATED_DIR}/docker-compose.rocksdb-cloud-distributed.yml"
SERVER_GRAPH_CONF="${GENERATED_DIR}/hugegraph.properties"

export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-hg-rocksdb-cloud-dist}"

HG_PD_IMAGE="${HG_PD_IMAGE:-hugegraph/pd:1.7.0}"
HG_STORE_IMAGE="${HG_STORE_IMAGE:-hugegraph/store:1.7.0}"
HG_SERVER_IMAGE="${HG_SERVER_IMAGE:-hugegraph/server:1.7.0}"
MINIO_IMAGE="${MINIO_IMAGE:-minio/minio:latest}"
MINIO_MC_IMAGE="${MINIO_MC_IMAGE:-minio/mc:latest}"

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
S3_BUCKET_STORE0="${S3_BUCKET_STORE0:-store0-rocksdb}"
S3_BUCKET_STORE1="${S3_BUCKET_STORE1:-store1-rocksdb}"
S3_BUCKET_STORE2="${S3_BUCKET_STORE2:-store2-rocksdb}"
S3_REGION="${S3_REGION:-us-east-1}"
S3_ENDPOINT="${S3_ENDPOINT:-http://minio:9000}"
GRAPH_API_BASE="${GRAPH_API_BASE:-http://localhost:8080/graphs/hugegraph}"

SERVER_PORT="${SERVER_PORT:-8080}"

# Store cloud sync is required in this smoke test: each store writes SST updates to S3.
STORE_ROCKSDB_CLOUD_ENABLED="${STORE_ROCKSDB_CLOUD_ENABLED:-true}"
STORE_ROCKSDB_CLOUD_SYNC_INTERVAL_SECONDS="${STORE_ROCKSDB_CLOUD_SYNC_INTERVAL_SECONDS:-30}"
STORE_ROCKSDB_CLOUD_SYNCHRONOUS_SST_UPLOAD_MODE="${STORE_ROCKSDB_CLOUD_SYNCHRONOUS_SST_UPLOAD_MODE:-true}"


AUTO_BUILD_SERVER_IMAGE="${AUTO_BUILD_SERVER_IMAGE:-true}"
AUTO_BUILD_STORE_IMAGE="${AUTO_BUILD_STORE_IMAGE:-true}"
KEEP_UP="${KEEP_UP:-true}"
DRY_RUN="${DRY_RUN:-false}"
SKIP_SMOKE_TESTS="${SKIP_SMOKE_TESTS:-false}"

log() {
    printf '[rocksdb-cloud-distributed-smoke] %s\n' "$*"
}

need_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "ERROR: command not found: $1" >&2
        exit 2
    fi
}

ensure_image_available() {
    local image="$1"
    if docker image inspect "$image" >/dev/null 2>&1; then
        return 0
    fi
    log "pulling image: ${image}"
    if ! docker pull "$image" >/dev/null; then
        echo "ERROR: failed to pull image '${image}'. Set an explicit tag via env if needed." >&2
        exit 3
    fi
}

ensure_minio_buckets() {
    local network_name="$1"
    log "ensuring MinIO buckets exist (one per store node)"
    for bucket in "$S3_BUCKET_STORE0" "$S3_BUCKET_STORE1" "$S3_BUCKET_STORE2"; do
        log "  creating bucket: ${bucket}"
        docker run --rm --network "${network_name}" --entrypoint /bin/sh "${MINIO_MC_IMAGE}" -c \
          "mc alias set local http://minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD} >/dev/null && \
           mc mb --ignore-existing local/${bucket} >/dev/null"
    done
}

server_api() {
    compose exec -T server curl -sSf "$@"
}

check_rocksdb_cloud_backend_ready() {
    local logs
    logs="$(compose logs --no-color server 2>/dev/null || true)"
    if echo "$logs" | grep -q "backend is illegal"; then
        echo "ERROR: server backend is not accepted — check image and hugegraph.properties" >&2
        return 1
    fi
}

# Verify each store node has enabled rocksdb-cloud sync by checking its logs for the
# cloud_enabled=true setting emitted by the entrypoint.
check_store_rocksdb_cloud_enabled() {
    if [[ "${STORE_ROCKSDB_CLOUD_ENABLED}" != "true" ]]; then
        echo "ERROR: STORE_ROCKSDB_CLOUD_ENABLED must be true for this S3-first smoke test" >&2
        return 1
    fi

    local all_ok=true
    for svc in store0 store1 store2; do
        local logs
        logs="$(compose logs --no-color "$svc" 2>/dev/null || true)"
        if echo "$logs" | grep -q "rocksdb.cloud_enabled=true"; then
            log "store cloud-backend check OK: ${svc} has cloud_enabled=true"
        else
            log "ERROR: ${svc} logs do not confirm rocksdb.cloud_enabled=true"
            all_ok=false
        fi
    done

    if [[ "$all_ok" != "true" ]]; then
        echo "ERROR: one or more store nodes are not running with rocksdb cloud enabled" >&2
        return 1
    fi
}

# Verify that each store's bucket contains at least one object.
verify_store_s3_objects() {
    if [[ "${STORE_ROCKSDB_CLOUD_ENABLED}" != "true" ]]; then
        echo "ERROR: STORE_ROCKSDB_CLOUD_ENABLED must be true for per-store S3 verification" >&2
        return 1
    fi

    local network_name="$1"
    local any_fail=false

    local -a buckets=("$S3_BUCKET_STORE0" "$S3_BUCKET_STORE1" "$S3_BUCKET_STORE2")
    local -a store_ids=(0 1 2)

    for i in "${!store_ids[@]}"; do
        local store_id="${store_ids[$i]}"
        local bucket="${buckets[$i]}"
        log "verifying MinIO objects for store${store_id} in bucket: ${bucket}"
        local count
        count="$(docker run --rm --network "${network_name}" --entrypoint /bin/sh "${MINIO_MC_IMAGE}" -c \
          "mc alias set local http://minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD} >/dev/null && \
           mc ls local/${bucket}/ --recursive 2>/dev/null | wc -l" | tr -d '[:space:]')"
        if [[ -z "$count" || "$count" == "0" ]]; then
            log "ERROR: no S3 objects found for store${store_id} in bucket '${bucket}'"
            any_fail=true
        else
            log "store${store_id} S3 object count in bucket '${bucket}': ${count}"
        fi
    done

    if [[ "$any_fail" == "true" ]]; then
        echo "ERROR: expected every store bucket to contain S3 objects, but at least one is empty" >&2
        return 1
    fi
}

post_json_with_retry() {
    local name="$1"
    local url="$2"
    local payload="$3"
    local max_retry="${4:-30}"
    local sleep_seconds="${5:-2}"
    local i=1

    while [[ "$i" -le "$max_retry" ]]; do
        local raw
        local code
        local body
        raw="$(compose exec -T server curl -sS \
                 -X POST "$url" \
                 -H 'Content-Type: application/json' \
                 -d "$payload" \
                 -w $'\n%{http_code}' || true)"
        code="${raw##*$'\n'}"
        body="${raw%$'\n'*}"

        if [[ "$code" =~ ^2[0-9][0-9]$ || "$code" == "409" ]]; then
            log "${name} ready (http=${code})"
            return 0
        fi

        log "${name} not ready yet (attempt ${i}/${max_retry}, http=${code})"
        if [[ -n "$body" ]]; then
            log "${name} response: ${body}"
        fi
        sleep "$sleep_seconds"
        i=$((i + 1))
    done

    echo "ERROR: ${name} failed after ${max_retry} attempts" >&2
    return 1
}

wait_http_ok() {
    local name="$1"
    local url="$2"
    local max_retry="${3:-120}"
    local sleep_seconds="${4:-2}"
    local i="1"

    while [[ "$i" -le "$max_retry" ]]; do
        local code="000"
        code="$(curl -sS -o /dev/null -w '%{http_code}' "$url" || true)"
        if [[ "$code" == "200" ]]; then
            log "healthy: ${name} (${url})"
            return 0
        fi
        sleep "$sleep_seconds"
        i=$((i + 1))
    done

    echo "ERROR: ${name} did not become healthy: ${url}" >&2
    return 1
}

wait_service_healthy() {
    local service="$1"
    local max_retry="${2:-120}"
    local sleep_seconds="${3:-2}"
    local i="1"

    while [[ "$i" -le "$max_retry" ]]; do
        local cid=""
        local status=""
        cid="$(compose ps -q "$service" 2>/dev/null || true)"
        if [[ -n "$cid" ]]; then
            status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$cid" 2>/dev/null || true)"
            if [[ "$status" == "healthy" || "$status" == "running" ]]; then
                log "healthy: ${service} (container status=${status})"
                return 0
            fi
        fi
        sleep "$sleep_seconds"
        i=$((i + 1))
    done

    echo "ERROR: service '${service}' did not become healthy" >&2
    return 1
}

compose() {
    docker compose -f "$COMPOSE_FILE" "$@"
}

cleanup() {
    if [[ "$KEEP_UP" == "true" ]]; then
        log "KEEP_UP=true, leaving compose stack running"
        return
    fi
    if [[ -f "$COMPOSE_FILE" ]]; then
        log "stopping compose stack"
        compose down -v --remove-orphans >/dev/null 2>&1 || true
    fi
}

on_error() {
    log "test failed, dumping short diagnostics"
    compose ps || true
    for svc in minio pd store0 store1 store2 server; do
        compose logs --tail=120 "$svc" || true
    done
}

trap cleanup EXIT
trap on_error ERR

need_cmd docker
need_cmd curl
need_cmd python3

ensure_image_available "$MINIO_IMAGE"
ensure_image_available "$MINIO_MC_IMAGE"

mkdir -p "$GENERATED_DIR"

if [[ "$AUTO_BUILD_SERVER_IMAGE" == "true" ]]; then
    log "building server image ${HG_SERVER_IMAGE} from source"
    docker build -t "$HG_SERVER_IMAGE" -f "${REPO_ROOT}/hugegraph-server/Dockerfile" "$REPO_ROOT"
fi

if [[ "$AUTO_BUILD_STORE_IMAGE" == "true" ]]; then
    log "building store image ${HG_STORE_IMAGE} from source"
    docker build -t "$HG_STORE_IMAGE" -f "${REPO_ROOT}/hugegraph-store/Dockerfile" "$REPO_ROOT"
fi

cat > "$SERVER_GRAPH_CONF" <<EOF
# Auto-generated by test-rocksdb-cloud-distributed.sh
# Server uses backend=hstore — stateless, routes all graph data through store nodes.
gremlin.graph=org.apache.hugegraph.HugeFactory
backend=hstore
serializer=binary
store=hugegraph

task.scheduler_type=local
task.schedule_period=10
task.retry=0
task.wait_timeout=10

search.text_analyzer=jieba
search.text_analyzer_mode=INDEX

pd.peers=pd:8686
EOF

cat > "$COMPOSE_FILE" <<EOF
services:
  minio:
    image: ${MINIO_IMAGE}
    container_name: hg-minio-test
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - hg-minio-data:/data
    networks: [hg-net]
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:9000/minio/health/live >/dev/null || exit 1"]
      interval: 5s
      timeout: 5s
      retries: 40
      start_period: 10s

  pd:
    image: ${HG_PD_IMAGE}
    container_name: hg-pd-dist
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
    ports:
      - "8620:8620"
      - "8686:8686"
    volumes:
      - hg-pd-data:/hugegraph-pd/pd_data
    networks: [hg-net]
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8620/v1/health >/dev/null || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 30
      start_period: 30s

  store0:
    image: ${HG_STORE_IMAGE}
    container_name: hg-store0-dist
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
      HG_STORE_ROCKSDB_CLOUD_ENABLED: "${STORE_ROCKSDB_CLOUD_ENABLED}"
      HG_STORE_ROCKSDB_CLOUD_BUCKET: "${S3_BUCKET_STORE0}"
      HG_STORE_ROCKSDB_CLOUD_ENDPOINT: "${S3_ENDPOINT}"
      HG_STORE_ROCKSDB_CLOUD_REGION: "${S3_REGION}"
      HG_STORE_ROCKSDB_CLOUD_ACCESS_KEY: "${MINIO_ROOT_USER}"
      HG_STORE_ROCKSDB_CLOUD_SECRET_KEY: "${MINIO_ROOT_PASSWORD}"
      HG_STORE_ROCKSDB_CLOUD_PATH_STYLE: "true"
      HG_STORE_ROCKSDB_CLOUD_OBJECT_PREFIX: ""
      HG_STORE_ROCKSDB_CLOUD_SYNC_INTERVAL_SECONDS: "${STORE_ROCKSDB_CLOUD_SYNC_INTERVAL_SECONDS}"
      HG_STORE_ROCKSDB_CLOUD_SYNC_INCREMENTAL: "true"
      HG_STORE_ROCKSDB_CLOUD_SYNCHRONOUS_SST_UPLOAD_MODE: "${STORE_ROCKSDB_CLOUD_SYNCHRONOUS_SST_UPLOAD_MODE}"
    ports:
      - "8520:8520"
    volumes:
      - hg-store0-data:/hugegraph-store/storage
    networks: [hg-net]
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8520/v1/health >/dev/null || exit 1"]
      interval: 10s
      timeout: 10s
      retries: 40
      start_period: 60s

  store1:
    image: ${HG_STORE_IMAGE}
    container_name: hg-store1-dist
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
      HG_STORE_ROCKSDB_CLOUD_ENABLED: "${STORE_ROCKSDB_CLOUD_ENABLED}"
      HG_STORE_ROCKSDB_CLOUD_BUCKET: "${S3_BUCKET_STORE1}"
      HG_STORE_ROCKSDB_CLOUD_ENDPOINT: "${S3_ENDPOINT}"
      HG_STORE_ROCKSDB_CLOUD_REGION: "${S3_REGION}"
      HG_STORE_ROCKSDB_CLOUD_ACCESS_KEY: "${MINIO_ROOT_USER}"
      HG_STORE_ROCKSDB_CLOUD_SECRET_KEY: "${MINIO_ROOT_PASSWORD}"
      HG_STORE_ROCKSDB_CLOUD_PATH_STYLE: "true"
      HG_STORE_ROCKSDB_CLOUD_OBJECT_PREFIX: ""
      HG_STORE_ROCKSDB_CLOUD_SYNC_INTERVAL_SECONDS: "${STORE_ROCKSDB_CLOUD_SYNC_INTERVAL_SECONDS}"
      HG_STORE_ROCKSDB_CLOUD_SYNC_INCREMENTAL: "true"
      HG_STORE_ROCKSDB_CLOUD_SYNCHRONOUS_SST_UPLOAD_MODE: "${STORE_ROCKSDB_CLOUD_SYNCHRONOUS_SST_UPLOAD_MODE}"
    ports:
      - "8521:8520"
    volumes:
      - hg-store1-data:/hugegraph-store/storage
    networks: [hg-net]
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8520/v1/health >/dev/null || exit 1"]
      interval: 10s
      timeout: 10s
      retries: 40
      start_period: 60s

  store2:
    image: ${HG_STORE_IMAGE}
    container_name: hg-store2-dist
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
      HG_STORE_ROCKSDB_CLOUD_ENABLED: "${STORE_ROCKSDB_CLOUD_ENABLED}"
      HG_STORE_ROCKSDB_CLOUD_BUCKET: "${S3_BUCKET_STORE2}"
      HG_STORE_ROCKSDB_CLOUD_ENDPOINT: "${S3_ENDPOINT}"
      HG_STORE_ROCKSDB_CLOUD_REGION: "${S3_REGION}"
      HG_STORE_ROCKSDB_CLOUD_ACCESS_KEY: "${MINIO_ROOT_USER}"
      HG_STORE_ROCKSDB_CLOUD_SECRET_KEY: "${MINIO_ROOT_PASSWORD}"
      HG_STORE_ROCKSDB_CLOUD_PATH_STYLE: "true"
      HG_STORE_ROCKSDB_CLOUD_OBJECT_PREFIX: ""
      HG_STORE_ROCKSDB_CLOUD_SYNC_INTERVAL_SECONDS: "${STORE_ROCKSDB_CLOUD_SYNC_INTERVAL_SECONDS}"
      HG_STORE_ROCKSDB_CLOUD_SYNC_INCREMENTAL: "true"
      HG_STORE_ROCKSDB_CLOUD_SYNCHRONOUS_SST_UPLOAD_MODE: "${STORE_ROCKSDB_CLOUD_SYNCHRONOUS_SST_UPLOAD_MODE}"
    ports:
      - "8522:8520"
    volumes:
      - hg-store2-data:/hugegraph-store/storage
    networks: [hg-net]
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8520/v1/health >/dev/null || exit 1"]
      interval: 10s
      timeout: 10s
      retries: 40
      start_period: 60s

  server:
    image: ${HG_SERVER_IMAGE}
    container_name: hg-server-test
    hostname: server
    depends_on:
      store0:
        condition: service_healthy
      store1:
        condition: service_healthy
      store2:
        condition: service_healthy
    ports:
      - "8080:8080"
    volumes:
      - ${SERVER_GRAPH_CONF}:/hugegraph-server/conf/graphs/hugegraph.properties:ro
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
EOF

if [[ "$DRY_RUN" == "true" ]]; then
    log "DRY_RUN=true, generated files only"
    log "compose: ${COMPOSE_FILE}"
    log "server conf: ${SERVER_GRAPH_CONF}"
    exit 0
fi

log "starting compose stack"
compose down -v --remove-orphans >/dev/null 2>&1 || true
compose up -d

wait_service_healthy "minio" 120 2

NETWORK_NAME="${COMPOSE_PROJECT_NAME}_hg-net"
ensure_minio_buckets "${NETWORK_NAME}"

wait_service_healthy "pd" 180 2
wait_service_healthy "store0" 180 2
wait_service_healthy "store1" 180 2
wait_service_healthy "store2" 180 2
wait_service_healthy "server" 180 2
check_rocksdb_cloud_backend_ready
check_store_rocksdb_cloud_enabled

BASE_URL="${GRAPH_API_BASE}"

# Wait for hstore backend to fully initialize by testing a graph API endpoint
log "waiting for server graph backend to initialize..."
wait_http_ok "server graph backend" "${BASE_URL}/graph/vertices" 60 3

if [[ "${SKIP_SMOKE_TESTS}" == "true" ]]; then
  log "SKIP_SMOKE_TESTS=true — skipping automated tests, environment is ready for manual testing"
  log "Environment Details:"
  log "  - Server:  http://localhost:${SERVER_PORT}"
  log "  - MinIO:   http://localhost:9000 (minioadmin/minioadmin)"
  log "  - Graph API: ${BASE_URL}"
  log "  - Stores:  store0 (8520), store1 (8521), store2 (8522)"
  log "  - S3 Buckets: ${S3_BUCKET_STORE0}, ${S3_BUCKET_STORE1}, ${S3_BUCKET_STORE2}"
  if [[ "${KEEP_UP}" != "true" ]]; then
    log "Tip: To keep containers running for manual testing, use: SKIP_SMOKE_TESTS=true KEEP_UP=true ./test-rocksdb-cloud-distributed.sh"
  fi
else
  log "creating schema"
  post_json_with_retry \
    "create property key" \
    "${BASE_URL}/schema/propertykeys" \
    '{"name":"cloud_key","data_type":"TEXT","cardinality":"SINGLE","check_exist":true}' \
    30 3

  post_json_with_retry \
    "create vertex label" \
    "${BASE_URL}/schema/vertexlabels" \
    '{"name":"cloud_vertex","id_strategy":"AUTOMATIC","properties":["cloud_key"],"check_exist":true}' \
    45 2

  log "writing vertices"
  for i in 1 2 3 4 5; do
    server_api -X POST "${BASE_URL}/graph/vertices" \
      -H 'Content-Type: application/json' \
      -d "{\"label\":\"cloud_vertex\",\"properties\":{\"cloud_key\":\"smoke-${i}\"}}" \
      >/dev/null
  done

  log "verifying read path"
  VERTICES_JSON="$(server_api --compressed "${BASE_URL}/graph/vertices")"
  python3 - <<'PY' "$VERTICES_JSON"
import json
import sys
payload = json.loads(sys.argv[1])
vertices = payload.get("vertices", [])
if not vertices:
    raise SystemExit("no vertices returned from graph API")
if not any(v.get("properties", {}).get("cloud_key", "").startswith("smoke-") for v in vertices):
    raise SystemExit("expected smoke-* cloud_key in vertices response")
print(f"vertex_check_ok count={len(vertices)}")
PY


  # Wait one sync interval and verify each store has uploaded files to its own S3 prefix.
  log "waiting ${STORE_ROCKSDB_CLOUD_SYNC_INTERVAL_SECONDS}s for store rocksdb-cloud sync to complete..."
  sleep "${STORE_ROCKSDB_CLOUD_SYNC_INTERVAL_SECONDS}"
  verify_store_s3_objects "${NETWORK_NAME}"

  log "SUCCESS: rocksdb-cloud distributed smoke test passed"
fi

