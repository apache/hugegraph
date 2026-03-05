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

# ── Build SPRING_APPLICATION_JSON ─────────────────────────────────────
SPRING_APPLICATION_JSON="$(cat <<JSON
{
  "pdserver": { "address": "$(json_escape "${HG_STORE_PD_ADDRESS}")" },
  "grpc":     { "host": "$(json_escape "${HG_STORE_GRPC_HOST}")",
                "port": "$(json_escape "${HG_STORE_GRPC_PORT}")" },
  "raft":     { "address": "$(json_escape "${HG_STORE_RAFT_ADDRESS}")" },
  "server":   { "port": "$(json_escape "${HG_STORE_REST_PORT}")" },
  "app":      { "data-path": "$(json_escape "${HG_STORE_DATA_PATH}")" }
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

./bin/start-hugegraph-store.sh -j "${JAVA_OPTS:-}"
tail -f /dev/null
