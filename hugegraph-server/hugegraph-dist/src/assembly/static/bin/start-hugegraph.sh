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
CONF="$TOP/conf"
LOGS="$TOP/logs"
SCRIPTS="$TOP/scripts"
PID_FILE="$BIN/pid"

. "$BIN"/util.sh

# Note: keep ':' in the end of the string to indicate the option needs a value
while getopts "d:g:m:p:s:j:t:y:" arg; do
    case ${arg} in
        d) DAEMON="$OPTARG" ;;
        g) GC_OPTION="$OPTARG" ;;
        m) OPEN_MONITOR="$OPTARG" ;;
        p) PRELOAD="$OPTARG" ;;
        s) OPEN_SECURITY_CHECK="$OPTARG" ;;
        j) USER_OPTION="$OPTARG" ;;
        t) SERVER_STARTUP_TIMEOUT_S="$OPTARG" ;;
        # Telemetry is used to collect metrics, traces and logs
        y) OPEN_TELEMETRY="$OPTARG" ;;
        # Note: update usage info when the params changed
        ?) exit_with_usage_help ;;
    esac
done

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
else
    echo "Starting HugeGraphServer in foreground mode..."
    "${BIN}"/hugegraph-server.sh "${CONF}/${GREMLIN_SERVER_CONF}" "${CONF}"/rest-server.properties \
    "${OPEN_SECURITY_CHECK}" "${USER_OPTION}" "${GC_OPTION}" "${OPEN_TELEMETRY}"
fi

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
    if [[ $DAEMON == "true" ]]; then
        exit 1
    fi
}
disown

if [ "$OPEN_MONITOR" == "true" ]; then
    if ! "$BIN"/start-monitor.sh; then
        echo "Failed to open monitor, please start it manually"
    fi
    echo "An HugeGraphServer monitor task has been append to crontab"
fi
