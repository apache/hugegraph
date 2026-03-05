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

log() { echo "[hugegraph-pd-entrypoint] $*"; }

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

migrate_env() {
    local old_name="$1" new_name="$2"

    if [[ -n "${!old_name:-}" && -z "${!new_name:-}" ]]; then
        log "WARN: deprecated env '${old_name}' detected; mapping to '${new_name}'"
        export "${new_name}=${!old_name}"
    fi
}

migrate_env "GRPC_HOST"             "HG_PD_GRPC_HOST"
migrate_env "RAFT_ADDRESS"          "HG_PD_RAFT_ADDRESS"
migrate_env "RAFT_PEERS"            "HG_PD_RAFT_PEERS_LIST"
migrate_env "PD_INITIAL_STORE_LIST" "HG_PD_INITIAL_STORE_LIST"

# ── Required vars ─────────────────────────────────────────────────────
require_env "HG_PD_GRPC_HOST"
require_env "HG_PD_RAFT_ADDRESS"
require_env "HG_PD_RAFT_PEERS_LIST"
require_env "HG_PD_INITIAL_STORE_LIST"

: "${HG_PD_GRPC_PORT:=8686}"
: "${HG_PD_REST_PORT:=8620}"
: "${HG_PD_DATA_PATH:=/hugegraph-pd/pd_data}"
: "${HG_PD_INITIAL_STORE_COUNT:=1}"

SPRING_APPLICATION_JSON="$(cat <<JSON
{
  "grpc":   { "host": "$(json_escape "${HG_PD_GRPC_HOST}")",
              "port": "$(json_escape "${HG_PD_GRPC_PORT}")" },
  "server": { "port": "$(json_escape "${HG_PD_REST_PORT}")" },
  "raft":   { "address":    "$(json_escape "${HG_PD_RAFT_ADDRESS}")",
              "peers-list": "$(json_escape "${HG_PD_RAFT_PEERS_LIST}")" },
  "pd":     { "data-path":          "$(json_escape "${HG_PD_DATA_PATH}")",
              "initial-store-list": "$(json_escape "${HG_PD_INITIAL_STORE_LIST}")" ,
              "initial-store-count": ${HG_PD_INITIAL_STORE_COUNT} }
}
JSON
)"
export SPRING_APPLICATION_JSON

log "effective config:"
log "  grpc.host=${HG_PD_GRPC_HOST}"
log "  grpc.port=${HG_PD_GRPC_PORT}"
log "  server.port=${HG_PD_REST_PORT}"
log "  raft.address=${HG_PD_RAFT_ADDRESS}"
log "  raft.peers-list=${HG_PD_RAFT_PEERS_LIST}"
log "  pd.initial-store-list=${HG_PD_INITIAL_STORE_LIST}"
log "  pd.initial-store-count=${HG_PD_INITIAL_STORE_COUNT}"
log "  pd.data-path=${HG_PD_DATA_PATH}"

./bin/start-hugegraph-pd.sh -j "${JAVA_OPTS:-}"
tail -f /dev/null
