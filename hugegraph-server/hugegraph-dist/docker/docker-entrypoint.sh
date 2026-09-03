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

# Property reading/writing goes through props.awk, which implements the
# java.util.Properties grammar HugeConfig applies (escapes, `:`/whitespace
# separators, continuations, first-definition-wins duplicates).  grep/sed
# rewrites disagree with it on mounted or upgraded configs, silently
# producing two definitions of one key.  Values move through environment
# variables rather than argv so a PASSWORD never shows up in `ps` output.
PROPS_AWK="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/props.awk"
if [[ ! -f "${PROPS_AWK}" ]]; then
    log "ERROR: props.awk not found next to the entrypoint"
    exit 1
fi

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

    PROPS_MODE=set PROPS_KEY="${key}" \
        PROPS_VALUE_ENCODED="${encoded_val}" PROPS_FILE="${file}" \
        awk -f "${PROPS_AWK}" /dev/null
}

set_prop() {
    local key="$1" val="$2" file="$3"

    set_prop_encoded "$key" "$(encode_prop_value "$val")" "$file"
}

get_prop_encoded() {
    local key="$1" file="$2"

    PROPS_MODE=get PROPS_KEY="${key}" PROPS_FILE="${file}" \
        awk -f "${PROPS_AWK}" /dev/null
}

# First uncommented `authenticator:` inside the gremlin-server.yaml
# authentication block.  snakeyaml resolves duplicate top-level keys to the
# last one, but a mounted file carrying two authentication blocks is
# pathological; report the first and let the mismatch WARN handle it.
get_yaml_authenticator() {
    local yaml="./conf/gremlin-server.yaml"

    [[ -f "${yaml}" ]] || return 0
    awk '
        /^[ \t]*#/ { next }
        /^[ \t]*authentication[ \t]*:/ { inblk = 1; next }
        inblk && /^[ \t]+authenticator[ \t]*:/ {
            line = $0
            sub(/^[ \t]*authenticator[ \t]*:[ \t]*/, "", line)
            sub(/[,:].*$/, "", line)
            print line
            exit
        }
    ' "./conf/gremlin-server.yaml"
}

# enable-auth.sh appends definitions to files it did not write.  On a
# mounted config those appended definitions are duplicates the two parsers
# resolve in opposite directions — HugeConfig (commons-configuration) takes
# the first, snakeyaml takes the last — so Gremlin and REST can land on
# different authenticators with no error from either.  Normalize both sides
# to one definition of the same authenticator here; enable-auth.sh's
# per-file guards then make its appends no-ops on anything already set.
align_auth_config() {
    local rest_auth yaml_auth

    rest_auth=$(get_prop_encoded "auth.authenticator" "${REST_SERVER_CONF}")
    yaml_auth=$(get_yaml_authenticator)
    if [[ -n "${rest_auth}" && -n "${yaml_auth}" && "${rest_auth}" != "${yaml_auth}" ]]; then
        log "WARN: REST and Gremlin name different authenticators" \
            "('${rest_auth}' vs '${yaml_auth}'); leaving both untouched"
        return
    fi
    if [[ -z "${rest_auth}" && -z "${yaml_auth}" ]]; then
        export AUTHENTICATOR_CLASS="org.apache.hugegraph.auth.StandardAuthenticator"
    elif [[ -n "${yaml_auth}" ]]; then
        set_prop_encoded "auth.authenticator" "${yaml_auth}" "${REST_SERVER_CONF}"
    else
        export AUTHENTICATOR_CLASS="${rest_auth}"
    fi
    # auth.graph_store and the gremlin.graph flip are left to enable-auth.sh,
    # which appends/rewrites only what is absent or still the plain default.
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

if [[ -n "${HG_SERVER_AUTH_TOKEN_SECRET:-}" ]]; then
    LC_ALL=C
    if (( ${#HG_SERVER_AUTH_TOKEN_SECRET} < 32 )); then
        log "ERROR: HG_SERVER_AUTH_TOKEN_SECRET must be at least 32 bytes"
        exit 1
    fi
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
    align_auth_config
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
