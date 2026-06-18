#!/bin/bash
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
#
set -euo pipefail

log() { echo "[hugegraph-store-entrypoint] $*"; }

require_env() {
    local name="$1"
    if [[ -z "${!name:-}" ]]; then
        echo "ERROR: missing required env '${name}'" >&2; exit 2
    fi
}

json_escape() {
    local s="$1"
    s=${s//\\/\\\\}; s=${s//\"/\\\"}; s=${s//$'\n'/}
    printf "%s" "$s"
}

# ── Guard deprecated vars ─────────────────────────────────────────────
migrate_env() {
    local old_name="$1" new_name="$2"

    if [[ -n "${!old_name:-}" && -z "${!new_name:-}" ]]; then
        log "WARN: deprecated env '${old_name}' detected; mapping to '${new_name}'"
        export "${new_name}=${!old_name}"
    fi
}

migrate_env "PD_ADDRESS"   "HG_STORE_PD_ADDRESS"
migrate_env "GRPC_HOST"    "HG_STORE_GRPC_HOST"
migrate_env "RAFT_ADDRESS" "HG_STORE_RAFT_ADDRESS"
# ── Required vars ─────────────────────────────────────────────────────
require_env "HG_STORE_PD_ADDRESS"
require_env "HG_STORE_GRPC_HOST"
require_env "HG_STORE_RAFT_ADDRESS"

# ── Defaults ──────────────────────────────────────────────────────────
: "${HG_STORE_GRPC_PORT:=8500}"
: "${HG_STORE_REST_PORT:=8520}"
: "${HG_STORE_DATA_PATH:=/hugegraph-store/storage}"
: "${HG_STORE_PARTITION_LEASE_ENABLED:=false}"
: "${HG_STORE_PARTITION_LEASE_TTL_SECONDS:=30}"
: "${HG_STORE_PARTITION_LEASE_RENEW_INTERVAL_SECONDS:=20}"

# ── RocksDB-Cloud defaults (all optional; cloud sync disabled unless HG_STORE_ROCKSDB_CLOUD_ENABLED=true) ──
: "${HG_STORE_ROCKSDB_CLOUD_ENABLED:=false}"
: "${HG_STORE_ROCKSDB_CLOUD_BUCKET:=hugegraph-rocksdb}"
: "${HG_STORE_ROCKSDB_CLOUD_ENDPOINT:=}"
: "${HG_STORE_ROCKSDB_CLOUD_REGION:=us-east-1}"
: "${HG_STORE_ROCKSDB_CLOUD_ACCESS_KEY:=}"
: "${HG_STORE_ROCKSDB_CLOUD_SECRET_KEY:=}"
: "${HG_STORE_ROCKSDB_CLOUD_PATH_STYLE:=true}"
# Each store node should use a unique prefix, e.g. "store0", "store1", "store2"
: "${HG_STORE_ROCKSDB_CLOUD_OBJECT_PREFIX:=store}"
: "${HG_STORE_ROCKSDB_CLOUD_SYNC_INTERVAL_SECONDS:=60}"
: "${HG_STORE_ROCKSDB_CLOUD_SYNC_INCREMENTAL:=true}"
: "${HG_STORE_ROCKSDB_CLOUD_CLOUD_FIRST_MODE:=true}"

# ── Build SPRING_APPLICATION_JSON ─────────────────────────────────────
SPRING_APPLICATION_JSON="$(cat <<JSON
{
  "pdserver": { "address": "$(json_escape "${HG_STORE_PD_ADDRESS}")" },
  "grpc":     { "host": "$(json_escape "${HG_STORE_GRPC_HOST}")",
                "port": "$(json_escape "${HG_STORE_GRPC_PORT}")" },
  "raft":     { "address": "$(json_escape "${HG_STORE_RAFT_ADDRESS}")" },
  "server":   { "port": "$(json_escape "${HG_STORE_REST_PORT}")" },
  "app":      { "data-path": "$(json_escape "${HG_STORE_DATA_PATH}")" },
  "store":    {
                 "partition-lease-enabled": "$(json_escape "${HG_STORE_PARTITION_LEASE_ENABLED}")",
                 "partition-lease-ttl-seconds": "$(json_escape "${HG_STORE_PARTITION_LEASE_TTL_SECONDS}")",
                 "partition-lease-renew-interval-seconds": "$(json_escape "${HG_STORE_PARTITION_LEASE_RENEW_INTERVAL_SECONDS}")"
               },
  "rocksdb":  {
                 "cloud_enabled": "$(json_escape "${HG_STORE_ROCKSDB_CLOUD_ENABLED}")",
                 "cloud_bucket": "$(json_escape "${HG_STORE_ROCKSDB_CLOUD_BUCKET}")",
                 "cloud_endpoint": "$(json_escape "${HG_STORE_ROCKSDB_CLOUD_ENDPOINT}")",
                 "cloud_region": "$(json_escape "${HG_STORE_ROCKSDB_CLOUD_REGION}")",
                 "cloud_access_key": "$(json_escape "${HG_STORE_ROCKSDB_CLOUD_ACCESS_KEY}")",
                 "cloud_secret_key": "$(json_escape "${HG_STORE_ROCKSDB_CLOUD_SECRET_KEY}")",
                 "cloud_path_style": "$(json_escape "${HG_STORE_ROCKSDB_CLOUD_PATH_STYLE}")",
                 "cloud_object_prefix": "$(json_escape "${HG_STORE_ROCKSDB_CLOUD_OBJECT_PREFIX}")",
                 "cloud_sync_interval_seconds": "$(json_escape "${HG_STORE_ROCKSDB_CLOUD_SYNC_INTERVAL_SECONDS}")",
                 "cloud_sync_incremental": "$(json_escape "${HG_STORE_ROCKSDB_CLOUD_SYNC_INCREMENTAL}")",
                 "cloud_cloud_first_mode": "$(json_escape "${HG_STORE_ROCKSDB_CLOUD_CLOUD_FIRST_MODE}")"
               }
}
JSON
)"
export SPRING_APPLICATION_JSON

log "effective config:"
log "  pdserver.address=${HG_STORE_PD_ADDRESS}"
log "  grpc.host=${HG_STORE_GRPC_HOST}"
log "  grpc.port=${HG_STORE_GRPC_PORT}"
log "  raft.address=${HG_STORE_RAFT_ADDRESS}"
log "  server.port=${HG_STORE_REST_PORT}"
log "  app.data-path=${HG_STORE_DATA_PATH}"
log "  store.partition-lease-enabled=${HG_STORE_PARTITION_LEASE_ENABLED}"
log "  store.partition-lease-ttl-seconds=${HG_STORE_PARTITION_LEASE_TTL_SECONDS}"
log "  store.partition-lease-renew-interval-seconds=${HG_STORE_PARTITION_LEASE_RENEW_INTERVAL_SECONDS}"
log "  rocksdb.cloud_enabled=${HG_STORE_ROCKSDB_CLOUD_ENABLED}"
if [[ "${HG_STORE_ROCKSDB_CLOUD_ENABLED}" == "true" ]]; then
    log "  rocksdb.cloud_bucket=${HG_STORE_ROCKSDB_CLOUD_BUCKET}"
    log "  rocksdb.cloud_endpoint=${HG_STORE_ROCKSDB_CLOUD_ENDPOINT}"
    log "  rocksdb.cloud_region=${HG_STORE_ROCKSDB_CLOUD_REGION}"
    log "  rocksdb.cloud_object_prefix=${HG_STORE_ROCKSDB_CLOUD_OBJECT_PREFIX}"
    log "  rocksdb.cloud_sync_interval_seconds=${HG_STORE_ROCKSDB_CLOUD_SYNC_INTERVAL_SECONDS}"
    log "  rocksdb.cloud_sync_incremental=${HG_STORE_ROCKSDB_CLOUD_SYNC_INCREMENTAL}"
    log "  rocksdb.cloud_cloud_first_mode=${HG_STORE_ROCKSDB_CLOUD_CLOUD_FIRST_MODE}"
fi

./bin/start-hugegraph-store.sh -d false -j "${JAVA_OPTS:-}"
