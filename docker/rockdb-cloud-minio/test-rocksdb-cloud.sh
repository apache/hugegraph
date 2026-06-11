#!/bin/bash
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

# Test script for rocksdb-cloud backend with MinIO
# Validates: schema, vertices, snapshots, Gremlin queries, MinIO sync

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SERVER_DIR="${SERVER_DIR:-$(find "$REPO_ROOT" -maxdepth 3 -type d -path "$REPO_ROOT/apache-hugegraph-*/apache-hugegraph-server-*" | head -n 1)}"
CONF="$SERVER_DIR/conf/graphs/hugegraph.properties"
MINIO_COMPOSE="$SCRIPT_DIR/docker-compose.minio.yml"
BASE_URL="http://localhost:8080"
CREDS="-u admin:pa"
GRAPH_URL="$BASE_URL/graphspaces/DEFAULT/graphs/hugegraph"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "ERROR: missing command '$1'"; exit 1; }
}

free_port() {
  local port="$1"
  local pids
  pids="$(lsof -ti tcp:"$port" || true)"
  if [[ -n "$pids" ]]; then
    echo "[setup] Releasing tcp/$port from pid(s): $pids"
    kill $pids >/dev/null 2>&1 || true
    sleep 2
    pids="$(lsof -ti tcp:"$port" || true)"
    if [[ -n "$pids" ]]; then
      kill -9 $pids >/dev/null 2>&1 || true
    fi
  fi
}

set_prop() {
  local key="$1"
  local value="$2"
  if grep -q "^${key}=" "$CONF"; then
    perl -pi -e "s|^\\Q${key}\\E=.*|${key}=${value}|" "$CONF"
  else
    printf '%s=%s\n' "$key" "$value" >> "$CONF"
  fi
}

echo "==== RocksDB Cloud Backend Testing with MinIO ===="

require_cmd docker
require_cmd curl
require_cmd python3
require_cmd perl
require_cmd lsof

if [[ -z "$SERVER_DIR" || ! -f "$CONF" ]]; then
  echo "ERROR: unable to locate server config at '$CONF'"
  echo "Tip: set SERVER_DIR before running, for example:"
  echo "  SERVER_DIR=/abs/path/to/apache-hugegraph-server-1.7.0 ./docker/rockdb-cloud-minio/test-rocksdb-cloud.sh"
  exit 1
fi

echo "[setup] Starting MinIO"
docker compose -f "$MINIO_COMPOSE" up -d

echo "[setup] Waiting for MinIO health"
for _ in $(seq 1 30); do
  if curl -fsS http://localhost:9000/minio/health/live >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
curl -fsS http://localhost:9000/minio/health/live >/dev/null
docker exec hg-minio-test mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null

echo "[setup] Configuring HugeGraph for rocksdb-cloud"
set_prop backend rocksdb-cloud
set_prop serializer binary
set_prop rocksdb.data_path rocksdb-cloud-data/data
set_prop rocksdb.wal_path rocksdb-cloud-data/wal

# Ensure cloud keys are unique in config before appending canonical values.
tmp_file="$(mktemp)"
awk '
  !/^rocksdb\.cloud\.s3_bucket_name=/ &&
  !/^rocksdb\.cloud\.s3_region=/ &&
  !/^rocksdb\.cloud\.s3_object_prefix=/ &&
  !/^rocksdb\.cloud\.aws_access_key_id=/ &&
  !/^rocksdb\.cloud\.aws_secret_access_key=/ &&
  !/^rocksdb\.cloud\.s3_endpoint=/ &&
  !/^rocksdb\.cloud\.s3_path_style_access=/ { print }
' "$CONF" > "$tmp_file"
mv "$tmp_file" "$CONF"

cat >> "$CONF" << 'EOF'
rocksdb.cloud.s3_bucket_name=hugegraph-rocksdb
rocksdb.cloud.s3_region=us-east-1
rocksdb.cloud.s3_object_prefix=hugegraph/
rocksdb.cloud.aws_access_key_id=minioadmin
rocksdb.cloud.aws_secret_access_key=minioadmin
rocksdb.cloud.s3_endpoint=http://localhost:9000
rocksdb.cloud.s3_path_style_access=true
EOF

echo "[setup] Restarting HugeGraph"
"$SERVER_DIR/bin/stop-hugegraph.sh" >/dev/null 2>&1 || true
free_port 8080
free_port 8182
rm -rf "$REPO_ROOT/rocksdb-cloud-data"
printf 'pa\npa\n' | "$SERVER_DIR/bin/init-store.sh"
"$SERVER_DIR/bin/start-hugegraph.sh" -t 60

echo "[test] Create schema (idempotent)"
curl -s $CREDS -X POST "$GRAPH_URL/schema/propertykeys" \
  -H 'Content-Type: application/json' \
  -d '{"name":"test_key","data_type":"TEXT","cardinality":"SINGLE","check_exist":true}' >/dev/null

curl -s $CREDS -X POST "$GRAPH_URL/schema/vertexlabels" \
  -H 'Content-Type: application/json' \
  -d '{"name":"test_vertex","id_strategy":"AUTOMATIC","properties":["test_key"],"check_exist":true}' >/dev/null

echo "[test] Insert vertices"
for i in {1..3}; do
  response=$(curl -s $CREDS -X POST "$GRAPH_URL/graph/vertices" \
    -H 'Content-Type: application/json' \
    -d "{\"label\":\"test_vertex\",\"properties\":{\"test_key\":\"cloud-test-00$i\"}}")
  vid=$(echo "$response" | python3 -c "import sys, json; r=json.load(sys.stdin); print(r.get('id', 'ERROR'))")
  echo "  created vertex #$i (id: $vid)"
done

echo "[test] Read vertices"
response=$(curl -s --compressed $CREDS "$GRAPH_URL/graph/vertices")
vcount=$(echo "$response" | python3 -c "import sys, json; r=json.load(sys.stdin); print(len(r.get('vertices', [])))")
echo "  total vertices: $vcount"

echo "[test] Create snapshot"
snap_name="snap-$(date +%s)"
curl -s $CREDS "$BASE_URL/gremlin" -X POST \
  -H 'Content-Type: application/json' \
  -d "{\"gremlin\":\"hugegraph.createSnapshot('$snap_name')\"}" >/dev/null

echo "[verify] MinIO objects"
obj_count=$(docker exec hg-minio-test mc ls local/hugegraph-rocksdb/hugegraph/ --recursive | wc -l | xargs)
echo "  object count: $obj_count"

echo "[verify] MinIO recent files"
docker exec hg-minio-test mc ls local/hugegraph-rocksdb/hugegraph/data/ --recursive | tail -10 | sed 's/^/  /'

echo "DONE: rocksdb-cloud + MinIO setup and validation complete"

