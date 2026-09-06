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
WAIT_STORAGE_PD_CONNECT_TIMEOUT_S=2
WAIT_STORAGE_PD_MAX_TIMEOUT_S=3

. "$BIN"/util.sh

log() {
  echo "[wait-storage] $1"
}

# PD REST credential. PD checks the password against its auth.secret-key and
# ships no default, so this has to be provided by the deployment.
# The value is deliberately kept out of the inner script's source text and out
# of curl's argv: the inner shell reads it from the environment and hands it to
# curl on stdin as a config file.
PD_AUTH_USER="${PD_AUTH_USER:-store}"
PD_AUTH_PASSWORD="${PD_AUTH_PASSWORD:-}"
if [ -z "${PD_AUTH_PASSWORD}" ]; then
  log "WARN: PD_AUTH_PASSWORD is empty; PD will answer 401 unless it runs without auth"
fi
# curl -K reads one option per line and takes the value as a quoted string
# whose only escapes are \\ \" \t \n \r \v. Backslash first, then the rest;
# an unescaped line break would end the option early and send a truncated
# credential (curl then warns that the remainder is an unknown option).
escape_curlrc() {
  local v=$1
  v=${v//\\/\\\\}
  v=${v//\"/\\\"}
  v=${v//$'\n'/\\n}
  v=${v//$'\r'/\\r}
  v=${v//$'\t'/\\t}
  v=${v//$'\v'/\\v}
  printf '%s' "$v"
}
PD_AUTH_CURL_USER=$(escape_curlrc "${PD_AUTH_USER}")
PD_AUTH_CURL_PASSWORD=$(escape_curlrc "${PD_AUTH_PASSWORD}")
export PD_AUTH_CURL_USER PD_AUTH_CURL_PASSWORD

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

              # curl stays out of the grep pipeline so its status code is
              # readable: a 401 is a wrong secret, not a storage problem, and
              # retrying it for 300s only hides that.
              check_any_pd_stores() {
                for peer in \$(echo \"\$PD_REST_LIST\" | tr ',' ' '); do
                  body=\$(printf 'user = \"%s:%s\"\n' \
                           \"\$PD_AUTH_CURL_USER\" \"\$PD_AUTH_CURL_PASSWORD\" | \
                         curl -K - -s -w '\n%{http_code}' \
                         --connect-timeout ${WAIT_STORAGE_PD_CONNECT_TIMEOUT_S} \
                         --max-time ${WAIT_STORAGE_PD_MAX_TIMEOUT_S} \
                         \"http://\${peer}/v1/stores\" 2>/dev/null)
                  code=\${body##*\$'\n'}
                  if [ \"\$code\" = 401 ]; then
                    log \"ERROR: PD at \${peer} refused the credential (401): PD_AUTH_PASSWORD must match PD's auth.secret-key\" >&2
                    return 2
                  fi
                  if printf '%s' \"\$body\" | grep -qi '\"state\"[[:space:]]*:[[:space:]]*\"Up\"'; then
                    echo \"\$peer\"
                    return 0
                  fi
                done
                return 1
              }

              until PD_REST=\$(check_any_pd_stores); do
                if [ \$? -eq 2 ]; then exit 2; fi
                log 'No Up store yet, retrying in 5s'
                sleep 5
              done
              log \"Store registration check PASSED via \$PD_REST\"
              log 'Storage backend is VIABLE'
            " || {
                rc=$?
                if [ "$rc" -eq 124 ]; then
                    echo "[wait-storage] ERROR: Timeout waiting for storage backend"
                else
                    echo "[wait-storage] ERROR: storage wait aborted, see the message above"
                fi
                exit 1
            }

        else
            log "No pd.peers configured, skipping storage wait"
        fi
    fi
fi
