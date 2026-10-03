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

OPEN_MONITOR="false"
OPEN_SECURITY_CHECK="true"
# change to "true" to enable telemetry(Trace) by default
OPEN_TELEMETRY="false"
DAEMON="true"
#VERBOSE=""
GC_OPTION=""
USER_OPTION=""
SERVER_STARTUP_TIMEOUT_S=30

# TODO: move abs_path function to shell like util.sh
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
SCRIPTS="$TOP/scripts"

. "$BIN"/util.sh

# Note: keep ':' in the end of the string to indicate the option needs a value
while getopts "c:d:g:i:j:l:m:o:p:s:t:y:" arg; do
     case ${arg} in
         c) CONF_OVERRIDE="$OPTARG" ;;
         d) DAEMON="$OPTARG" ;;
         g) GC_OPTION="$OPTARG" ;;
         i) PID_FILE_OVERRIDE="$OPTARG" ;;
         j) USER_OPTION="$OPTARG" ;;
         l) LOGS_OVERRIDE="$OPTARG" ;;
         m) OPEN_MONITOR="$OPTARG" ;;
         o) PLUGINS_OVERRIDE="$OPTARG" ;;
         p) PRELOAD="$OPTARG" ;;
         s) OPEN_SECURITY_CHECK="$OPTARG" ;;
         t) SERVER_STARTUP_TIMEOUT_S="$OPTARG" ;;
         # Telemetry is used to collect metrics, traces and logs
         y) OPEN_TELEMETRY="$OPTARG" ;;
         # Note: update usage info when the params changed
         ?) exit_with_usage_help ;;
     esac
done

# Canonicalize relative path overrides to absolute paths
CONF_OVERRIDE="$(canonicalize_dir "$CONF_OVERRIDE")"
LOGS_OVERRIDE="$(canonicalize_dir "$LOGS_OVERRIDE")"
PLUGINS_OVERRIDE="$(canonicalize_dir "$PLUGINS_OVERRIDE")"
PID_FILE_OVERRIDE="$(canonicalize_file "$PID_FILE_OVERRIDE")"

CONF="${CONF_OVERRIDE:-$TOP/conf}"
LOGS="${LOGS_OVERRIDE:-$TOP/logs}"
PID_FILE="${PID_FILE_OVERRIDE:-$BIN/pid}"
PLUGINS="${PLUGINS_OVERRIDE:-$TOP/plugins}"

export CONF_OVERRIDE LOGS_OVERRIDE PID_FILE_OVERRIDE PLUGINS_OVERRIDE

if [[ "$OPEN_MONITOR" != "true" && "$OPEN_MONITOR" != "false" ]]; then
    exit_with_usage_help
fi

if [[ "$OPEN_SECURITY_CHECK" != "true" && "$OPEN_SECURITY_CHECK" != "false" ]]; then
    exit_with_usage_help
fi

GREMLIN_SERVER_URL=$(read_property "$CONF/rest-server.properties" "gremlinserver.url")
if [ -z "$GREMLIN_SERVER_URL" ]; then
    GREMLIN_SERVER_URL="http://127.0.0.1:8182"
fi
REST_SERVER_URL=$(read_property "$CONF/rest-server.properties" "restserver.url")

check_port "$GREMLIN_SERVER_URL"
check_port "$REST_SERVER_URL"

# Note: Only download hugegraph-server.keystore when we config https (check the conf file)
if [[ $REST_SERVER_URL == https* && ! -e "${CONF}/hugegraph-server.keystore" ]]; then
    download "${CONF}" "https://github.com/apache/hugegraph-doc/raw/binary-1.5/dist/server/hugegraph-server.keystore"
fi

if [ ! -d "$LOGS" ]; then
    mkdir -p "$LOGS"
fi

GREMLIN_SERVER_CONF="gremlin-server.yaml"
if [[ $PRELOAD == "true" ]]; then
    GREMLIN_SERVER_CONF="gremlin-server-preload.yaml"
    EXAMPLE_SCRIPT="example-preload.groovy"
    cp "${CONF}"/gremlin-server.yaml "${CONF}/${GREMLIN_SERVER_CONF}"
    cp "${SCRIPTS}"/example.groovy "${SCRIPTS}/${EXAMPLE_SCRIPT}"
    sed -i -e "s/empty-sample.groovy/$EXAMPLE_SCRIPT/g" "${CONF}/${GREMLIN_SERVER_CONF}"
    sed -i -e '/registerBackends/d; /serverStarted/d' "${SCRIPTS}/${EXAMPLE_SCRIPT}"
fi

if [[ $DAEMON == "true" ]]; then
    echo "Starting HugeGraphServer in daemon mode..."
    "${BIN}"/hugegraph-server.sh "${CONF}/${GREMLIN_SERVER_CONF}" "${CONF}"/rest-server.properties \
    "${OPEN_SECURITY_CHECK}" "${USER_OPTION}" "${GC_OPTION}" "${OPEN_TELEMETRY}" &

    PID="$!"
    # Write pid to file
    echo "$PID" > "$PID_FILE"

    trap 'kill $PID; exit' SIGHUP SIGINT SIGQUIT SIGTERM

    wait_for_startup ${PID} 'HugeGraphServer' "$REST_SERVER_URL/graphs" "${SERVER_STARTUP_TIMEOUT_S}" || {
        if [[ "${STDOUT_MODE:-false}" == "true" ]]; then
            echo "See 'docker logs' for HugeGraphServer log output." >&2
        else
            echo "See $LOGS/hugegraph-server.log for HugeGraphServer log output." >&2
        fi
        exit 1
    }
    disown

    if [ "$OPEN_MONITOR" == "true" ]; then
        if ! "$BIN"/start-monitor.sh; then
            echo "Failed to open monitor, please start it manually"
        fi
        echo "An HugeGraphServer monitor task has been append to crontab"
    fi
else
    echo "Starting HugeGraphServer in foreground mode..."
    "${BIN}"/hugegraph-server.sh "${CONF}/${GREMLIN_SERVER_CONF}" "${CONF}"/rest-server.properties \
    "${OPEN_SECURITY_CHECK}" "${USER_OPTION}" "${GC_OPTION}" "${OPEN_TELEMETRY}" &
    PID="$!"
    # Write pid to file
    echo "$PID" > "$PID_FILE"
    trap 'kill $PID; wait $PID; exit $?' SIGHUP SIGINT SIGQUIT SIGTERM
    wait $PID
    exit $?
fi
