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

encode_prop_value() {
    local value="$1" encoded="" char
    local i

    LC_ALL=C
    for ((i = 0; i < ${#value}; i++)); do
        char="${value:i:1}"
        case "${char}" in
            "\\") encoded+="\\\\" ;;
            " ") encoded+="\\ " ;;
            $'\t') encoded+="\\t" ;;
            $'\n') encoded+="\\n" ;;
            $'\r') encoded+="\\r" ;;
            $'\f') encoded+="\\f" ;;
            *) encoded+="${char}" ;;
        esac
    done
    printf '%s' "${encoded}"
}

set_prop_encoded() {
    local key="$1" encoded_val="$2" file="$3"
    local esc_key esc_val key_re

    esc_key=$(printf '%s' "$key" | sed -e 's/[][(){}.^$*+?|\\/]/\\&/g')
    esc_val=$(printf '%s' "$encoded_val" | sed -e 's/[&|\\~]/\\&/g')
    key_re="^[[:space:]]*${esc_key}([[:space:]]*[:=]|[[:space:]]+|[[:space:]]*$)"

    if grep -qE "${key_re}" "${file}"; then
        if sed --version >/dev/null 2>&1; then
            # GNU sed supports the 0,/regexp/ address used by the Linux
            # images. Keep the first property and remove later duplicates.
            sed -ri "0,/${key_re}/!{/${key_re}/d;}" "${file}"
            sed -ri "0,/${key_re}/s~${key_re}.*~${key}=${esc_val}~" "${file}"
        else
            # BSD sed (macOS) has neither -r nor the GNU line-0 address. Find
            # the matching lines explicitly, delete duplicates from the end,
            # then replace the first line in place.
            first_line=$(grep -nE "${key_re}" "${file}" | head -n 1 | cut -d: -f1)
            duplicate_lines=$(grep -nE "${key_re}" "${file}" |
                              cut -d: -f1 | tail -n +2 | sort -rn)
            while IFS= read -r duplicate; do
                [[ -z "${duplicate}" ]] || \
                    sed -E -i '' "${duplicate}d" "${file}"
            done <<< "${duplicate_lines}"
            sed -E -i '' "${first_line}s~${key_re}.*~${key}=${esc_val}~" \
                "${file}"
        fi
    else
        printf '%s=%s\n' "$key" "$encoded_val" >> "${file}"
    fi
}

set_prop() {
    local key="$1" val="$2" file="$3"

    set_prop_encoded "$key" "$(encode_prop_value "$val")" "$file"
}

get_prop_encoded() {
    local key="$1" file="$2"
    local esc_key

    esc_key=$(printf '%s' "$key" | sed -e 's/[][(){}.^$*+?|\\/]/\\&/g')
    sed -nE \
        "s~^[[:space:]]*${esc_key}([[:space:]]*[:=][[:space:]]*|[[:space:]]+)(.*)$~\\2~p" \
        "${file}" | head -n 1
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

ROCKSDB_PROVIDER="${HG_SERVER_ROCKSDB_PROVIDER:-rocksdb}"
case "${ROCKSDB_PROVIDER}" in
    rocksdb | topling) ;;
    *) log "ERROR: HG_SERVER_ROCKSDB_PROVIDER must be rocksdb or topling"
       exit 1 ;;
esac

REQUESTED_BACKEND="${HG_SERVER_BACKEND:-$(get_prop_encoded "backend" "${GRAPH_CONF}")}"
if [[ "${REQUESTED_BACKEND}" == "hstore" ]]; then
    TOPLING_JAR=$(find ./lib -path '*/topling/rocksdbjni*.jar' \
                       -print -quit 2>/dev/null || true)
    if [[ "${ROCKSDB_PROVIDER}" == "topling" ]]; then
        log "ERROR: an HStore Server cannot select a local ToplingDB runtime"
        exit 1
    fi
    if [[ -n "${TOPLING_JAR}" ||
          -e ./library/librocksdbjni-linux64.so ]]; then
        log "ERROR: an HStore Server image must not contain a local" \
            "ToplingDB runtime"
        exit 1
    fi
fi

if [[ -n "${HG_SERVER_DATA_PATH:-}" ]]; then
    ROCKSDB_DATA_ROOT="${HG_SERVER_DATA_PATH}"
elif [[ "${ROCKSDB_PROVIDER}" == "topling" ]]; then
    ROCKSDB_DATA_ROOT="$(pwd)/topling-data"
else
    ROCKSDB_DATA_ROOT="$(pwd)/rocksdb-data"
fi
ENFORCE_PROVIDER_MARKER="${HG_SERVER_ENFORCE_PROVIDER_MARKER:-false}"
case "${ENFORCE_PROVIDER_MARKER}" in
    true | false) ;;
    *) log "ERROR: HG_SERVER_ENFORCE_PROVIDER_MARKER must be true or false"
       exit 1 ;;
esac
if [[ "${REQUESTED_BACKEND}" == "rocksdb" ]]; then
    ./bin/verify-rocksdb-provider.sh server "${ROCKSDB_PROVIDER}" \
        "${ROCKSDB_DATA_ROOT}" "${ENFORCE_PROVIDER_MARKER}"
    LEGACY_INIT_MARKER="${DOCKER_FOLDER}/${INIT_FLAG_FILE}"
    DOCKER_FOLDER="${ROCKSDB_DATA_ROOT}/.hugegraph-state"
    mkdir -p "${DOCKER_FOLDER}"
    if [[ "${ROCKSDB_PROVIDER}" == "rocksdb" &&
          -f "${LEGACY_INIT_MARKER}" &&
          ! -e "${DOCKER_FOLDER}/${INIT_FLAG_FILE}" ]]; then
        cp "${LEGACY_INIT_MARKER}" "${DOCKER_FOLDER}/${INIT_FLAG_FILE}"
        log "migrated the legacy RocksDB initialization marker"
    fi
fi

if [[ -n "${HG_SERVER_AUTH_TOKEN_SECRET:-}" ]]; then
    LC_ALL=C
    if (( ${#HG_SERVER_AUTH_TOKEN_SECRET} < 32 )); then
        log "ERROR: HG_SERVER_AUTH_TOKEN_SECRET must be at least 32 bytes"
        exit 1
    fi
fi

# How long the entrypoint lets the server take to answer on its REST port
# before it gives up and ends the container. An orchestrator that already
# owns this budget through a startup probe needs to raise it, otherwise the
# container terminates a JVM that is still starting and the probe never gets
# to decide. Validated here so a bad value fails before init-store runs,
# rather than reaching the arithmetic in wait_for_startup: that deadline is
# $((now_s + timeout_s)), which wraps negative near the 64-bit ceiling and
# makes the wait exit before its first probe, the very failure this variable
# exists to avoid. The five-digit bound keeps this comparison in range too,
# and a day is already far past any real start. Plain '-' rather than ':-',
# so an explicitly empty value is rejected instead of quietly becoming the
# default: Compose interpolation such as ${SOME_VAR:-} yields empty, not
# unset, whenever the host variable is missing.
SERVER_STARTUP_TIMEOUT_MAX_S=86400
SERVER_STARTUP_TIMEOUT_S="${HG_SERVER_STARTUP_TIMEOUT_S-120}"
if [[ ! "${SERVER_STARTUP_TIMEOUT_S}" =~ ^[1-9][0-9]{0,4}$ ]] ||
   (( SERVER_STARTUP_TIMEOUT_S > SERVER_STARTUP_TIMEOUT_MAX_S )); then
    log "ERROR: HG_SERVER_STARTUP_TIMEOUT_S must be a whole number of" \
        "seconds from 1 to ${SERVER_STARTUP_TIMEOUT_MAX_S}," \
        "got '${SERVER_STARTUP_TIMEOUT_S}'"
    exit 1
fi

if [[ -n "${PASSWORD:-}" &&
      "${HG_SERVER_REQUIRE_AUTH_TOKEN_SECRET:-false}" == "true" &&
      -z "${HG_SERVER_AUTH_TOKEN_SECRET:-}" ]]; then
    log "ERROR: HG_SERVER_AUTH_TOKEN_SECRET is required when authentication is enabled"
    exit 1
fi

AUTH_TOKEN_SECRET_ENCODED=""
if [[ -n "${PASSWORD:-}" && -z "${HG_SERVER_AUTH_TOKEN_SECRET:-}" ]]; then
    rest_secret=$(get_prop_encoded "auth.token_secret" "${REST_SERVER_CONF}")
    graph_secret=$(get_prop_encoded "auth.token_secret" "${GRAPH_CONF}")
    if [[ -n "${rest_secret}" ]]; then
        AUTH_TOKEN_SECRET_ENCODED="${rest_secret}"
        if [[ -n "${graph_secret}" && "${graph_secret}" != "${rest_secret}" ]]; then
            log "WARN: authentication token secrets differ; using REST secret"
        fi
    elif [[ -n "${graph_secret}" ]]; then
        AUTH_TOKEN_SECRET_ENCODED="${graph_secret}"
    else
        HG_SERVER_AUTH_TOKEN_SECRET=$(head -c 32 /dev/urandom | base64 | tr -d '\n')
        log "generated a shared authentication token secret"
    fi
fi

# ── Map env → properties file ─────────────────────────────────────────
[[ -n "${HG_SERVER_BACKEND:-}"  ]] && set_prop "backend"  "${HG_SERVER_BACKEND}"  "${GRAPH_CONF}"
set_prop "rocksdb.provider" "${ROCKSDB_PROVIDER}" "${GRAPH_CONF}"
if [[ "${REQUESTED_BACKEND}" == "rocksdb" ]]; then
    set_prop "rocksdb.data_path" "${ROCKSDB_DATA_ROOT}/data" "${GRAPH_CONF}"
    set_prop "rocksdb.wal_path" "${ROCKSDB_DATA_ROOT}/wal" "${GRAPH_CONF}"
fi
[[ -n "${HG_SERVER_PD_PEERS:-}" ]] && set_prop "pd.peers" "${HG_SERVER_PD_PEERS}" "${GRAPH_CONF}"
[[ -n "${HG_SERVER_USE_PD:-}" ]] && \
    set_prop "usePD" "${HG_SERVER_USE_PD}" "${REST_SERVER_CONF}"
[[ -n "${HG_SERVER_PD_PEERS:-}" ]] && \
    set_prop "pd.peers" "${HG_SERVER_PD_PEERS}" "${REST_SERVER_CONF}"
[[ -n "${HG_SERVER_CLUSTER:-}" ]] && \
    set_prop "cluster" "${HG_SERVER_CLUSTER}" "${REST_SERVER_CONF}"
[[ -n "${HG_SERVER_REST_URL:-}" ]] && set_prop "restserver.url" \
    "${HG_SERVER_REST_URL}" "${REST_SERVER_CONF}"
[[ -n "${HG_SERVER_MIN_FREE_MEMORY:-}" ]] && set_prop "restserver.min_free_memory" \
    "${HG_SERVER_MIN_FREE_MEMORY}" "${REST_SERVER_CONF}"
if [[ -n "${HG_SERVER_AUTH_TOKEN_SECRET:-}" ]]; then
    set_prop "auth.token_secret" "${HG_SERVER_AUTH_TOKEN_SECRET}" \
        "${REST_SERVER_CONF}"
    set_prop "auth.token_secret" "${HG_SERVER_AUTH_TOKEN_SECRET}" "${GRAPH_CONF}"
elif [[ -n "${AUTH_TOKEN_SECRET_ENCODED}" ]]; then
    set_prop_encoded "auth.token_secret" "${AUTH_TOKEN_SECRET_ENCODED}" \
        "${REST_SERVER_CONF}"
    set_prop_encoded "auth.token_secret" "${AUTH_TOKEN_SECRET_ENCODED}" \
        "${GRAPH_CONF}"
fi
if [[ -n "${PASSWORD:-}" ]]; then
    set_prop "auth.admin_pa" "${PASSWORD}" "${REST_SERVER_CONF}"
    # This script is idempotent and must run outside the initialization guard:
    # an upgrade can preserve the marker from an unauthenticated deployment.
    ./bin/enable-auth.sh
fi

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
        # init-store reads the password from stdin, and a disabled one returns
        # before it gets there, so say plainly that PASSWORD is being dropped
        case "${INIT_STORE_ENABLED}" in
            n | f | no | off | false)
                log "init-store does not read PASSWORD while disabled;" \
                    "the entrypoint applies it through 'auth.admin_pa' for" \
                    "the PD startup path" ;;
        esac
        printf '%s\n' "${PASSWORD}" | ./bin/init-store.sh
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

PID_FILE="./bin/pid"
START_PID=""
PID=""

read_server_pid() {
    local candidate

    candidate=$(cat "$PID_FILE" 2>/dev/null || true)
    if [[ "$candidate" =~ ^[1-9][0-9]*$ ]]; then
        printf '%s' "$candidate"
    fi
}

# A restarted container can retain the previous process id in its writable
# layer. Only trust a pid file created by the startup launched below.
rm -f "$PID_FILE"

# shellcheck disable=SC2329  # Invoked by the TERM/INT trap below.
shutdown_server() {
    local server_pid="${PID:-}"

    if [[ -z "$server_pid" ]]; then
        server_pid=$(read_server_pid)
    fi
    if [[ -n "$server_pid" ]]; then
        kill -TERM -- "$server_pid" 2>/dev/null || true
    fi
    if [[ -n "${START_PID:-}" ]]; then
        kill -TERM -- "$START_PID" 2>/dev/null || true
    fi
    while [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; do
        # kill -0 remains true for an unreaped zombie. Do not keep the
        # container alive after the JVM has already completed shutdown.
        if [[ -r "/proc/$server_pid/stat" ]]; then
            PROCESS_STATE=$(awk '{ print $3 }' "/proc/$server_pid/stat" \
                            2>/dev/null || true)
            if [[ "$PROCESS_STATE" == "Z" ]]; then
                break
            fi
        fi
        sleep 1
    done
    exit 0
}
trap shutdown_server TERM INT

./bin/start-hugegraph.sh -j "${JAVA_OPTS:-}" -t "${SERVER_STARTUP_TIMEOUT_S}" &
START_PID=$!
wait "$START_PID"
START_PID=""

# Post-startup cluster stabilization check (hstore only — rocksdb has no partitions)
ACTUAL_BACKEND=$(grep -E '^[[:space:]]*backend[[:space:]]*=' "${GRAPH_CONF}" | head -n 1 | sed 's/.*=//' | tr -d '[:space:]' || true)
if [[ "${ACTUAL_BACKEND}" == "hstore" ]]; then
    STORE_REST="${STORE_REST:-store:8520}"
    export STORE_REST
    ./bin/wait-partition.sh || log "WARN: partitions not assigned yet"
fi

PID=$(read_server_pid)
if [[ -n "$PID" ]]; then
    tail --pid="$PID" -f /dev/null
    exit 1
fi
