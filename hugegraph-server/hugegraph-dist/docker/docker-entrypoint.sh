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

DOCKER_FOLDER="./docker"
INIT_FLAG_FILE="init_complete"
GRAPH_CONF="./conf/graphs/hugegraph.properties"
REST_SERVER_CONF="./conf/rest-server.properties"

mkdir -p "${DOCKER_FOLDER}"

log() { echo "[hugegraph-server-entrypoint] $*"; }

set_prop() {
    local key="$1" val="$2" file="$3"
    local esc_key esc_val key_re

    esc_key=$(printf '%s' "$key" | sed -e 's/[][(){}.^$*+?|\\/]/\\&/g')
    esc_val=$(printf '%s' "$val" | sed -e 's/[&|\\~]/\\&/g')
    key_re="^[[:space:]]*${esc_key}([[:space:]]*[:=]|[[:space:]]+|[[:space:]]*$)"

    if grep -qE "${key_re}" "${file}"; then
        sed -ri "0,/${key_re}/!{/${key_re}/d;}" "${file}"
        sed -ri "0,/${key_re}/s~${key_re}.*~${key}=${esc_val}~" "${file}"
    else
        printf '%s=%s\n' "$key" "$val" >> "${file}"
    fi
}

migrate_env() {
    local old_name="$1" new_name="$2"

    if [[ -n "${!old_name:-}" && -z "${!new_name:-}" ]]; then
        log "WARN: deprecated env '${old_name}' detected; mapping to '${new_name}'"
        export "${new_name}=${!old_name}"
    fi
}

migrate_env "BACKEND"  "HG_SERVER_BACKEND"
migrate_env "PD_PEERS" "HG_SERVER_PD_PEERS"

# ── Map env → properties file ─────────────────────────────────────────
[[ -n "${HG_SERVER_BACKEND:-}"  ]] && set_prop "backend"  "${HG_SERVER_BACKEND}"  "${GRAPH_CONF}"
[[ -n "${HG_SERVER_PD_PEERS:-}" ]] && set_prop "pd.peers" "${HG_SERVER_PD_PEERS}" "${GRAPH_CONF}"

# Normalized once here and reused by the init-flag guard below. The accepted
# spellings are the ones HugeConfig accepts, case-insensitive: commons-lang 2.x
# BooleanUtils, reached through commons-configuration 1.x PropertyConverter.
# That set excludes 0 and 1, which commons-lang3 would have taken. Anything
# outside it is rejected now rather than touching the init flag for a value the
# server is going to refuse anyway.
INIT_STORE_ENABLED=$(printf '%s' "${HG_SERVER_INIT_STORE_ENABLED:-}" |
                     tr -d '[:space:]' | tr '[:upper:]' '[:lower:]')
case "${INIT_STORE_ENABLED}" in
    "" | y | t | yes | on | true | n | f | no | off | false) ;;
    *) log "ERROR: invalid HG_SERVER_INIT_STORE_ENABLED" \
           "'${HG_SERVER_INIT_STORE_ENABLED}'"
       exit 1 ;;
esac
[[ -n "${INIT_STORE_ENABLED}" ]] && \
    set_prop "init_store.enabled" "${INIT_STORE_ENABLED}" "${REST_SERVER_CONF}"

# ── Build wait-storage env ─────────────────────────────────────────────
WAIT_ENV=()
[[ -n "${HG_SERVER_BACKEND:-}"  ]] && WAIT_ENV+=("hugegraph.backend=${HG_SERVER_BACKEND}")
[[ -n "${HG_SERVER_PD_PEERS:-}" ]] && WAIT_ENV+=("hugegraph.pd.peers=${HG_SERVER_PD_PEERS}")

# ── Init store ────────────────────────────────────────────────────────
# init-store owns the marker: it skips re-initialization when the marker is
# present and writes it only after it has actually initialized. Deciding here
# would mean guessing from the environment variable, which says nothing about
# a config mounted with the property already set. Absolute, so the in-Java
# existence check agrees with the guard below no matter where init-store.sh
# leaves its working directory.
INIT_MARKER_PATH="$(cd "${DOCKER_FOLDER}" && pwd)/${INIT_FLAG_FILE}"
export HG_SERVER_INIT_COMPLETE_MARKER="${INIT_MARKER_PATH}"

if [[ ! -f "${INIT_MARKER_PATH}" ]]; then
    if (( ${#WAIT_ENV[@]} > 0 )); then
        env "${WAIT_ENV[@]}" ./bin/wait-storage.sh
    else
        ./bin/wait-storage.sh
    fi

    if [[ -z "${PASSWORD:-}" ]]; then
        log "init hugegraph with non-auth mode"
        ./bin/init-store.sh
    else
        log "init hugegraph with auth mode"
        ./bin/enable-auth.sh
        # init-store reads the password from stdin, and a disabled one returns
        # before it gets there, so say plainly that PASSWORD is being dropped
        case "${INIT_STORE_ENABLED}" in
            n | f | no | off | false)
                log "WARN: PASSWORD is ignored while init-store is disabled;" \
                    "the admin is created on the PD startup path from" \
                    "'auth.admin_pa', which defaults to the public value 'pa'" ;;
        esac
        echo "${PASSWORD}" | ./bin/init-store.sh
    fi
else
    log "HugeGraph initialization already done. Revalidating the config..."
    # The marker skips re-initialization inside init-store, not init-store
    # itself: a disabled one must pass its fail-closed check on every startup,
    # because the marker may predate this configuration or this release and
    # says nothing about whether the admin the current config relies on is
    # reachable. An enabled one returns at the marker, before it touches the
    # backend or reads stdin, so neither wait-storage nor PASSWORD is needed.
    ./bin/init-store.sh
fi

./bin/start-hugegraph.sh -j "${JAVA_OPTS:-}" -t 120

# Post-startup cluster stabilization check (hstore only — rocksdb has no partitions)
ACTUAL_BACKEND=$(grep -E '^[[:space:]]*backend[[:space:]]*=' "${GRAPH_CONF}" | head -n 1 | sed 's/.*=//' | tr -d '[:space:]' || true)
if [[ "${ACTUAL_BACKEND}" == "hstore" ]]; then
    STORE_REST="${STORE_REST:-store:8520}"
    export STORE_REST
    ./bin/wait-partition.sh || log "WARN: partitions not assigned yet"
fi

PID=$(cat ./bin/pid 2>/dev/null || true)
if [[ -n "$PID" ]]; then
    trap 'kill -TERM "$PID" 2>/dev/null; while kill -0 "$PID" 2>/dev/null; do sleep 1; done; exit 0' TERM INT
    tail --pid="$PID" -f /dev/null
    exit 1
fi
