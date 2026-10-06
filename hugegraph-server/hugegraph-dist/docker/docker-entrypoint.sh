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
# separators, CR/CRLF/LF line terminators, continuations, first-definition-wins
# duplicates).  grep/sed rewrites disagree with it on mounted or upgraded
# configs, silently producing two definitions of one key.  Values move through
# environment variables rather than argv so a PASSWORD never shows up in `ps`
# output.
#
# props.awk lives in the packaged bin/ directory because bin/enable-auth.sh
# reads properties with it too, and that assembly fileSet is what both the
# release tarball and this image are built from.  Beside the entrypoint is only
# where the source tree and the tests put it.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
props_from_env="${PROPS_AWK:-}"
yaml_from_env="${YAMLSCAN_AWK:-}"
PROPS_AWK=""
for candidate in "${props_from_env}" "${HERE}/props.awk" "${HERE}/bin/props.awk"; do
    if [[ -n "${candidate}" && -f "${candidate}" ]]; then
        PROPS_AWK="${candidate}"
        break
    fi
done
if [[ -z "${PROPS_AWK}" ]]; then
    log "ERROR: props.awk not found beside the entrypoint or in bin/"
    exit 1
fi

YAMLSCAN=""
for candidate in "${yaml_from_env}" "${HERE}/yamlscan.awk"; do
    if [[ -n "${candidate}" && -f "${candidate}" ]]; then
        YAMLSCAN="${candidate}"
        break
    fi
done
if [[ -z "${YAMLSCAN}" ]]; then
    log "ERROR: yamlscan.awk not found beside the entrypoint"
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
            # \u0020 and not `\ `: both readers turn it back into a space, but
            # a line right-trimmed before the continuation check -- which is how
            # commons-configuration reads it -- leaves the backslash of `\ `
            # behind at the end of the line, and that swallows the property
            # written under it.  A secret ending in a space used to move
            # auth.authenticator inside the password value.
            " ") encoded+="\\u0020" ;;
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

# The value as java.util.Properties hands it to the server, escapes resolved.
# Compare against this, not the on-disk bytes: `backend=h\u0073tore` is a legal
# spelling of hstore that the JVM reads as hstore and a string compare against
# the raw text does not.
get_prop_decoded() {
    local key="$1" file="$2"

    PROPS_MODE=get PROPS_DECODED=1 PROPS_KEY="${key}" PROPS_FILE="${file}" \
        awk -f "${PROPS_AWK}" /dev/null
}

# What the top-level authentication mapping of gremlin-server.yaml says about
# authentication, as one of three states: none, named, nameless.
#
# The question and its answer live in yamlscan.awk, which reads the mapping the
# way snakeyaml presents it to the server: only a column-0 `authentication`
# mapping counts, only its direct `authenticator` child names a class, comment
# text never counts as content, and a nested `config.authenticator` belongs to
# the config map rather than to the server.  Those distinctions are the whole
# decision -- an earlier grep-shaped version of this function reported `named`
# for `authentication: {} # authenticator: X` and for a class nested under
# `config:`, which passed the REST/Gremlin parity check while Gremlin was
# running on AllowAllAuthenticator.
yaml_auth_state() {
    local yaml="./conf/gremlin-server.yaml"

    [[ -f "${yaml}" ]] || { echo "none"; return 0; }
    awk -f "${YAMLSCAN}" "${yaml}"
}

# Authentication has to be configured on both sides or on neither.  A mounted
# config carrying only one is refused rather than completed: the entrypoint
# cannot know which class the operator means, and finishing the other side from
# a guessed default is how Gremlin ends up on AllowAllAuthenticator while REST
# enforces StandardAuthenticator.  A mapping that names no authenticator is
# refused by itself, because enable-auth.sh guards on the presence of that
# mapping and would otherwise write only the REST side.
check_auth_sides() {
    local rest=0 yaml=0 state rest_value

    state=$(yaml_auth_state)
    if [[ "${state}" == "nameless" ]]; then
        log "ERROR: gremlin-server.yaml carries a top-level authentication" \
            "mapping that names no authenticator; add an authenticator entry" \
            "to it or remove the mapping, then restart."
        return 1
    fi
    # A nonzero status here means the reader could not answer at all -- props.awk
    # exits 2 rather than guess, e.g. for a file that splices another one with an
    # commons-configuration `include`.  Calling that "configured on one side"
    # would send the operator to the wrong file, and calling it absent is the
    # direction that lets REST start open beside a Gremlin that authenticates.
    if ! rest_value=$(get_prop_encoded "auth.authenticator" "${REST_SERVER_CONF}"); then
        log "ERROR: cannot read auth.authenticator from ${REST_SERVER_CONF};" \
            "see the reason above, fix it, then restart."
        return 1
    fi
    if [[ -n "${rest_value}" ]]; then
        rest=1
    fi
    if [[ "${state}" == "named" ]]; then
        yaml=1
    fi
    if (( rest == yaml )); then
        return 0
    fi
    log "ERROR: authentication is configured in only one of" \
        "rest-server.properties (auth.authenticator) and" \
        "gremlin-server.yaml (authentication.authenticator);" \
        "configure both or neither, then restart."
    return 1
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
# Both sides have to agree whether authentication is on, whatever the reason
# the container was started for.  Running this only inside the PASSWORD branch
# below left a mounted rest-server.properties that carried auth.authenticator
# with no matching yaml mapping completely unvalidated: with no PASSWORD the
# entrypoint skipped the check, enable-auth.sh never ran, and the server came
# up with REST enforcing and Gremlin open.  A refusal exits under set -e.
check_auth_sides

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

./bin/start-hugegraph.sh -j "${JAVA_OPTS:-}" -t "${SERVER_STARTUP_TIMEOUT_S}"

# Post-startup cluster stabilization check (hstore only — rocksdb has no partitions)
# Read through props.awk so a mounted config using the `:` or bare-whitespace
# separator is seen at all, and first-definition-wins matches HugeConfig; the
# grep this replaces only ever accepted `=`.  Decoded, because this is compared
# against a literal: the JVM reads `backend=h\u0073tore` as hstore while the
# on-disk bytes are not that string, and the comparison deciding to skip
# wait-partition.sh is how startup continued before partitions were assigned.
# Trailing whitespace is dropped here rather than in the reader, which reports
# the value verbatim apart from the escapes java.util.Properties resolves.
ACTUAL_BACKEND=$(get_prop_decoded "backend" "${GRAPH_CONF}" | tr -d '[:space:]' || true)
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
