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
export SERVER_GRAPH_CONF="${GENERATED_DIR}/hugegraph.properties"
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
cleanup() { [[ "$KEEP_UP" == "true" ]] || (docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true); }
trap cleanup EXIT
need_cmd docker curl python3
log "pulling images..."
ensure_image "$MINIO_IMAGE"
ensure_image "$MINIO_MC_IMAGE"
ensure_image "$HG_PD_IMAGE"
ensure_image "$HG_STORE_IMAGE"
ensure_image "$HG_SERVER_IMAGE"
mkdir -p "$GENERATED_DIR"
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
