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

source ./bin/util.sh
record_jvm_startup_diagnostics
run_startup_step() {
    local status=0
    "$@" || status=$?
    if [[ "$status" -ne 0 ]]; then
        diagnose_jvm_startup_failure
    fi
    return "$status"
}

# Keep typed/list mappings on their existing encoder; auth strings use native-reader escapes below.
# TODO: relax Helm's password-space guard once its minimum Server image includes this auth encoder.
encode_prop_value() {
    local value="$1" encoded="" char
    local i

    local LC_ALL=C
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
        sed -ri "0,/${key_re}/!{/${key_re}/d;}" "${file}"
        sed -ri "0,/${key_re}/s~${key_re}.*~${key}=${esc_val}~" "${file}"
    else
        printf '%s=%s\n' "$key" "$encoded_val" >> "${file}"
    fi
}

set_prop() {
    local key="$1" val="$2" file="$3"

    set_prop_encoded "$key" "$(encode_prop_value "$val")" "$file"
}

# Auth values are strings, not typed/list options. Commons keeps escaped spaces
# and trims a physical trailing space before detecting line continuation.
encode_auth_prop_value() {
    local value="$1" encoding=UTF-8
    if (( $(printf '%s' "${value}" | LC_ALL=C tr -d '\040-\176' | wc -c) > 0 )); then
        # Retain legacy byte values that the ISO-8859-1 reader already accepted.
        if ! printf '%s' "${value}" | iconv -f UTF-8 -t UTF-8 >/dev/null 2>&1; then
            encoding=ISO-8859-1
        fi
        # The Server properties reader is ISO-8859-1. Standard UTF-16 escapes
        # preserve UTF-8 input and prevent trimming boundary control characters.
        printf '%s' "${value}" | iconv -f "${encoding}" -t UTF-16BE | od -An -v -tx1 | \
            tr -d ' \n' | sed 's/..../\\u&/g'
    else
        encode_prop_value "${value}" | sed 's/\\ /\\u0020/g'
    fi
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
migrate_env "SERVICE_ID" "HG_SERVER_SERVICE_ID"
migrate_env "GRAPH_SPACE" "HG_SERVER_GRAPHSPACE"

# The Server reads these properties as ISO-8859-1. Reject unsupported input
# before writing UTF-8 bytes that would select a different service identity.
for identity_name in HG_SERVER_SERVICE_ID HG_SERVER_GRAPHSPACE; do
    if [[ -n "${!identity_name:-}" ]] &&
       (( $(printf '%s' "${!identity_name}" | LC_ALL=C tr -d '\040-\176' | wc -c) > 0 )); then
        log "ERROR: ${identity_name} must be printable ASCII (bytes 0x20-0x7E)"
        exit 1
    fi
done

# Match the Server API graph-space naming contract after legacy migration.
if [[ -n "${HG_SERVER_GRAPHSPACE:-}" ]] && ! (
    export LC_ALL=C
    [[ "${HG_SERVER_GRAPHSPACE}" =~ ^(DEFAULT|[a-z][a-z0-9_]{0,47})$ ]]
); then
    log "ERROR: HG_SERVER_GRAPHSPACE must be DEFAULT or a name starting with" \
        "a lowercase letter, followed by lowercase letters, digits or underscores (1-48 characters)"
    exit 1
fi

if [[ -n "${HG_SERVER_AUTH_TOKEN_SECRET:-}" ]]; then
    if (( $(printf '%s' "${HG_SERVER_AUTH_TOKEN_SECRET}" | LC_ALL=C wc -c) < 32 )); then
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

# Encode authentication strings before changing either configuration file.
if [[ -n "${HG_SERVER_AUTH_TOKEN_SECRET:-}" ]]; then
    AUTH_TOKEN_SECRET_ENCODED=$(encode_auth_prop_value "${HG_SERVER_AUTH_TOKEN_SECRET}")
fi
AUTH_ADMIN_PASSWORD_ENCODED=""
if [[ -n "${PASSWORD:-}" ]]; then
    AUTH_ADMIN_PASSWORD_ENCODED=$(encode_auth_prop_value "${PASSWORD}")
fi

# Same accepted spellings as HG_SERVER_INIT_STORE_ENABLED below, checked
# before the mapping so an invalid value leaves both conf files untouched.
# URLs hold no whitespace, and the Server splits the list on bare commas, so
# "a, b" would otherwise register an escaped space in front of the second URL.
URLS_TO_PD=$(printf '%s' "${HG_SERVER_URLS_TO_PD:-}" | tr -d '[:space:]')
# Only surrounding whitespace is trimmed, so a value such as "t rue" stays invalid.
DEPLOY_IN_K8S="${HG_SERVER_DEPLOY_IN_K8S:-}"
DEPLOY_IN_K8S="${DEPLOY_IN_K8S#"${DEPLOY_IN_K8S%%[![:space:]]*}"}"
DEPLOY_IN_K8S="${DEPLOY_IN_K8S%"${DEPLOY_IN_K8S##*[![:space:]]}"}"
DEPLOY_IN_K8S=$(printf '%s' "${DEPLOY_IN_K8S}" | tr '[:upper:]' '[:lower:]')
case "${DEPLOY_IN_K8S}" in
    "" | y | t | yes | on | true | n | f | no | off | false) ;;
    *) log "ERROR: invalid HG_SERVER_DEPLOY_IN_K8S '${HG_SERVER_DEPLOY_IN_K8S}'"
       exit 1 ;;
esac

# ── Map env → properties file ─────────────────────────────────────────
# TODO: drop the Helm chart's (helm/hugegraph) rest-server.properties wrapper once its minimum
# Server image has the server.urls_to_pd and server.deploy_in_k8s mappings below. A read-only root
# filesystem also needs the conf files on a writable path, since set_prop in this script edits them
# in place; the chart refuses readOnlyRootFilesystem until then.
[[ -n "${HG_SERVER_BACKEND:-}"  ]] && set_prop "backend"  "${HG_SERVER_BACKEND}"  "${GRAPH_CONF}"
[[ -n "${HG_SERVER_PD_PEERS:-}" ]] && set_prop "pd.peers" "${HG_SERVER_PD_PEERS}" "${GRAPH_CONF}"
[[ -n "${HG_SERVER_USE_PD:-}" ]] && \
    set_prop "usePD" "${HG_SERVER_USE_PD}" "${REST_SERVER_CONF}"
[[ -n "${HG_SERVER_PD_PEERS:-}" ]] && \
    set_prop "pd.peers" "${HG_SERVER_PD_PEERS}" "${REST_SERVER_CONF}"
[[ -n "${HG_SERVER_CLUSTER:-}" ]] && \
    set_prop "cluster" "${HG_SERVER_CLUSTER}" "${REST_SERVER_CONF}"
if [[ -n "${HG_SERVER_SERVICE_ID:-}" ]]; then
    # Service names can contain spaces. Commons Configuration keeps a backslash
    # before a literal space, but decodes Unicode escapes, including at the edges.
    service_id_encoded=$(encode_prop_value "${HG_SERVER_SERVICE_ID}" | sed 's/\\ /\\u0020/g')
    set_prop_encoded "server.service_id" "${service_id_encoded}" "${REST_SERVER_CONF}"
fi
[[ -n "${HG_SERVER_GRAPHSPACE:-}" ]] && \
    set_prop "server.graphspace" "${HG_SERVER_GRAPHSPACE}" "${REST_SERVER_CONF}"
[[ -n "${HG_SERVER_REST_URL:-}" ]] && set_prop "restserver.url" \
    "${HG_SERVER_REST_URL}" "${REST_SERVER_CONF}"
# The address list this Server registers in PD, read only when
# server.deploy_in_k8s is true; otherwise it registers restserver.url.
[[ -n "${URLS_TO_PD}" ]] && set_prop "server.urls_to_pd" \
    "${URLS_TO_PD}" "${REST_SERVER_CONF}"
[[ -n "${DEPLOY_IN_K8S}" ]] && set_prop "server.deploy_in_k8s" \
    "${DEPLOY_IN_K8S}" "${REST_SERVER_CONF}"
[[ -n "${HG_SERVER_MIN_FREE_MEMORY:-}" ]] && set_prop "restserver.min_free_memory" \
    "${HG_SERVER_MIN_FREE_MEMORY}" "${REST_SERVER_CONF}"
if [[ -n "${AUTH_TOKEN_SECRET_ENCODED}" ]]; then
    set_prop_encoded "auth.token_secret" "${AUTH_TOKEN_SECRET_ENCODED}" \
        "${REST_SERVER_CONF}"
    set_prop_encoded "auth.token_secret" "${AUTH_TOKEN_SECRET_ENCODED}" \
        "${GRAPH_CONF}"
fi
if [[ -n "${PASSWORD:-}" ]]; then
    set_prop_encoded "auth.admin_pa" "${AUTH_ADMIN_PASSWORD_ENCODED}" "${REST_SERVER_CONF}"
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
        run_startup_step ./bin/init-store.sh
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
        printf '%s\n' "${PASSWORD}" | run_startup_step ./bin/init-store.sh
    fi
else
    log "HugeGraph initialization already done. Revalidating the config..."
    # The marker skips re-initialization inside init-store, not init-store
    # itself: a disabled one must pass its fail-closed check on every startup,
    # because the marker may predate this configuration or this release and
    # says nothing about whether the admin the current config relies on is
    # reachable. An enabled one returns at the marker, before it touches the
    # backend or reads stdin, so neither wait-storage nor PASSWORD is needed.
    run_startup_step ./bin/init-store.sh
fi

# Own the foreground launcher so wait can recover the JVM's actual status.
# A daemon JVM is not our child; watching its PID with tail loses that status.
LAUNCHER_PID=""
READINESS_PID=""
PARTITION_PID=""
stop_server_and_exit() {
    local exit_code="$1"
    # dumb-init can signal the group before we forward to its children.
    # Ignore duplicate signals while those children finish shutting down.
    trap '' TERM INT
    if [[ -n "$READINESS_PID" ]]; then
        kill -TERM "$READINESS_PID" 2>/dev/null || true
    fi
    if [[ -n "$PARTITION_PID" ]]; then
        kill -TERM "$PARTITION_PID" 2>/dev/null || true
    fi
    if [[ -n "$LAUNCHER_PID" ]]; then
        kill -TERM "$LAUNCHER_PID" 2>/dev/null || true
    fi
    if [[ -n "$PARTITION_PID" ]]; then
        wait "$PARTITION_PID" 2>/dev/null || true
    fi
    if [[ -n "$READINESS_PID" ]]; then
        wait "$READINESS_PID" 2>/dev/null || true
    fi
    if [[ -n "$LAUNCHER_PID" ]]; then
        wait "$LAUNCHER_PID" 2>/dev/null || true
    fi
    exit "$exit_code"
}
trap 'stop_server_and_exit 143' TERM
trap 'stop_server_and_exit 130' INT

./bin/start-hugegraph.sh -d false -j "${JAVA_OPTS:-}" -t "${SERVER_STARTUP_TIMEOUT_S}" &
LAUNCHER_PID=$!
REST_SERVER_URL=$(read_property "$REST_SERVER_CONF" "restserver.url")
# GNU timeout owns the probe group, including a stalled curl. Background it
# so wait is interruptible; a foreground probe would defer our signal traps.
timeout "${SERVER_STARTUP_TIMEOUT_S}s" bash -c '
    source ./bin/util.sh
    wait_for_startup "$@"
' bash "$LAUNCHER_PID" 'HugeGraphServer' "$REST_SERVER_URL/graphs" \
       "$SERVER_STARTUP_TIMEOUT_S" &
READINESS_PID=$!
READY_STATUS=0
wait "$READINESS_PID" || READY_STATUS=$?
READINESS_PID=""
if [[ "$READY_STATUS" -ne 0 ]]; then
    log "ERROR: HugeGraphServer did not become ready; see the container logs"
    diagnose_jvm_startup_failure
    if kill -0 "$LAUNCHER_PID" 2>/dev/null; then
        # Keep the startup deadline even if the JVM ignores TERM. Container
        # termination cleans up any remaining processes; don't wait forever.
        kill -TERM "$LAUNCHER_PID" 2>/dev/null || true
        exit 1
    fi
    # Preserve an early failure, but a normal exit before readiness is still
    # an unsuccessful start.
    STATUS=0
    wait "$LAUNCHER_PID" || STATUS=$?
    [[ "$STATUS" -ne 0 ]] || STATUS=1
    exit "$STATUS"
fi

# Post-startup cluster stabilization check (hstore only — rocksdb has no partitions)
ACTUAL_BACKEND=$(grep -E '^[[:space:]]*backend[[:space:]]*=' "${GRAPH_CONF}" | head -n 1 | sed 's/.*=//' | tr -d '[:space:]' || true)
if [[ "${ACTUAL_BACKEND}" == "hstore" ]]; then
    STORE_REST="${STORE_REST:-store:8520}"
    export STORE_REST
    ./bin/wait-partition.sh &
    PARTITION_PID=$!
    wait "$PARTITION_PID" || log "WARN: partitions not assigned yet"
    PARTITION_PID=""
fi

if wait "$LAUNCHER_PID"; then
    exit 0
else
    exit $?
fi
