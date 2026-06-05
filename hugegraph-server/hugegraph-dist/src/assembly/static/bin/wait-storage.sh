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

function abs_path() {
    SOURCE="${BASH_SOURCE[0]}"
    while [[ -h "$SOURCE" ]]; do
        DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    cd -P "$(dirname "$SOURCE")" && pwd
}

BIN=$(abs_path)
TOP="$(cd "$BIN"/../ && pwd)"
GRAPH_CONF="$TOP/conf/graphs/hugegraph.properties"
WAIT_STORAGE_TIMEOUT_S=300

. "$BIN"/util.sh

log() {
  echo "[wait-storage] $1"
}

PD_AUTH_ARGS="-u ${PD_AUTH_USER:-store}:${PD_AUTH_PASSWORD:-admin}"

function key_exists {
    local key=$1
    local file_name=$2
    grep -q -E "^\s*${key}\s*=\.*" ${file_name}
}

function update_key {
    local key=$1
    local val=$2
    local file_name=$3
    sed -ri "s#^(\s*${key}\s*=).*#\\1${val}#" ${file_name}
}

function add_key {
    local key=$1
    local val=$2
    local file_name=$3
    echo "${key}=${val}" >> ${file_name}
}

# apply config from env
while IFS=' ' read -r envvar_key envvar_val; do
    if [[ "${envvar_key}" =~ hugegraph\. ]] && [[ -n ${envvar_val} ]]; then
        envvar_key=${envvar_key#"hugegraph."}
        if key_exists ${envvar_key} ${GRAPH_CONF}; then
            update_key ${envvar_key} ${envvar_val} ${GRAPH_CONF}
        else
            add_key ${envvar_key} ${envvar_val} ${GRAPH_CONF}
        fi
    fi
done < <(env | sort -r | awk -F= '{ st = index($0, "="); print $1 " " substr($0, st+1) }')

# wait for storage
if env | grep '^hugegraph\.' > /dev/null; then
    if [ -n "${WAIT_STORAGE_TIMEOUT_S:-}" ]; then

        PD_PEERS="${hugegraph_pd_peers:-}"
        if [ -z "$PD_PEERS" ]; then
            PD_PEERS=$(grep -E "^\s*pd\.peers\s*=" "$GRAPH_CONF" | sed 's/.*=\s*//' | tr -d ' ')
        fi

        if [ -n "$PD_PEERS" ]; then
            : "${HG_SERVER_PD_REST_ENDPOINT:=}"

            if [ -n "${HG_SERVER_PD_REST_ENDPOINT}" ]; then
                PD_REST_LIST="${HG_SERVER_PD_REST_ENDPOINT}"
            else
                PD_REST_LIST=$(echo "$PD_PEERS" | sed 's/:8686/:8620/g')
            fi

            export PD_REST_LIST
            log "PD REST peers = $PD_REST_LIST"
            log "Timeout = ${WAIT_STORAGE_TIMEOUT_S}s"

            timeout "${WAIT_STORAGE_TIMEOUT_S}s" bash -c "

              log() { echo '[wait-storage] '\"\$1\"; }

              check_any_pd() {
                for peer in \$(echo \"\$PD_REST_LIST\" | tr ',' ' '); do
                  if curl ${PD_AUTH_ARGS} -f -s http://\${peer}/v1/health >/dev/null 2>&1; then
                    echo \"\$peer\"
                    return 0
                  fi
                done
                return 1
              }

              until PD_REST=\$(check_any_pd); do
                log 'No PD peer ready yet, retrying in 5s'
                sleep 5
              done
              log \"PD health check PASSED via \$PD_REST\"

              until curl ${PD_AUTH_ARGS} -f -s \
                    http://\${PD_REST}/v1/stores 2>/dev/null | \
                    grep -qi '\"state\"[[:space:]]*:[[:space:]]*\"Up\"'; do
                log 'No Up store yet, retrying in 5s'
                sleep 5
              done

              log 'Store registration check PASSED'
              log 'Storage backend is VIABLE'
            " || { echo "[wait-storage] ERROR: Timeout waiting for storage backend"; exit 1; }

        else
            log "No pd.peers configured, skipping storage wait"
        fi
    fi
fi
