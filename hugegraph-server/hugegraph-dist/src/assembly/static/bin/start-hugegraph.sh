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

reject_unsafe_path "$CONF_OVERRIDE" "-c conf dir" crlf || exit 1
reject_unsafe_path "$LOGS_OVERRIDE" "-l logs dir" crlf || exit 1
reject_unsafe_path "$PID_FILE_OVERRIDE" "-i pid file" crlf || exit 1
# The plugins dir is joined into ':'-separated classpath/agent settings
reject_unsafe_path "$PLUGINS_OVERRIDE" "-o plugins dir" classpath || exit 1

# Canonicalize relative path overrides to absolute paths.
CONF_OVERRIDE="$(canonicalize_dir "$CONF_OVERRIDE" nocreate)" || exit 1
LOGS_OVERRIDE="$(canonicalize_dir "$LOGS_OVERRIDE")" || exit 1
PLUGINS_OVERRIDE="$(canonicalize_dir "$PLUGINS_OVERRIDE")" || exit 1
PID_FILE_OVERRIDE="$(canonicalize_file "$PID_FILE_OVERRIDE")" || exit 1

# Re-validate the *effective*, canonicalized plugins path: cd/pwd resolves a
# relative override against the current working directory, which can
# introduce a ':' (or other unsafe char) that wasn't present in the raw value
# (e.g. launching from a directory such as "/tmp/work:one").
reject_unsafe_path "$PLUGINS_OVERRIDE" "-o plugins dir (resolved)" classpath || exit 1

# The monitor persists these values into a crontab line, where '%' is special to cron
if [ "$OPEN_MONITOR" == "true" ]; then
    reject_unsafe_path "$CONF_OVERRIDE" "conf dir (-c)" cron || exit 1
    reject_unsafe_path "$LOGS_OVERRIDE" "logs dir (-l)" cron || exit 1
    reject_unsafe_path "$PLUGINS_OVERRIDE" "plugins dir (-o)" cron || exit 1
    reject_unsafe_path "$PID_FILE_OVERRIDE" "pid file (-i)" cron || exit 1
    reject_unsafe_path "$JAVA_HOME" "JAVA_HOME" cron || exit 1
    reject_unsafe_path "$TOP" "install directory" cron || exit 1
fi

CONF="${CONF_OVERRIDE:-$TOP/conf}"
LOGS="${LOGS_OVERRIDE:-$TOP/logs}"
PID_FILE="${PID_FILE_OVERRIDE:-$BIN/pid}"
PLUGINS="${PLUGINS_OVERRIDE:-$TOP/plugins}"

ensure_path_writable "$(dirname "$PID_FILE")"

export CONF_OVERRIDE LOGS_OVERRIDE PID_FILE_OVERRIDE PLUGINS_OVERRIDE

if [[ "$OPEN_MONITOR" != "true" && "$OPEN_MONITOR" != "false" ]]; then
    exit_with_usage_help
fi

if [[ "$OPEN_SECURITY_CHECK" != "true" && "$OPEN_SECURITY_CHECK" != "false" ]]; then
    exit_with_usage_help
fi

# Verify the config files exist before reading them
ensure_file_readable "$CONF/rest-server.properties" "REST server config" || exit 1
ensure_file_readable "$CONF/gremlin-server.yaml" "Gremlin server config" || exit 1

GREMLIN_SERVER_URL=$(read_property "$CONF/rest-server.properties" "gremlinserver.url")
if [ -z "$GREMLIN_SERVER_URL" ]; then
    GREMLIN_SERVER_URL="http://127.0.0.1:8182"
fi
REST_SERVER_URL=$(read_property "$CONF/rest-server.properties" "restserver.url")

# 'graphs' is read by HugeGraphServer/GraphManager relative to the working
# directory (set to $TOP below), not relative to $CONF.
GRAPHS_DIR=$(read_property "$CONF/rest-server.properties" "graphs")
reject_relative_conf_path "$GRAPHS_DIR" "graphs" "$CONF/rest-server.properties" || exit 1

check_port "$GREMLIN_SERVER_URL"
check_port "$REST_SERVER_URL"

# Note: Only check/download hugegraph-server.keystore when we config https.
# 'ssl.keystore_file' (ServerOptions.SSL_KEYSTORE_FILE) defaults to the
# relative 'conf/hugegraph-server.keystore' and, like 'graphs' above, is
# opened by RestServer/Grizzly's SSLContextConfigurator relative to the
# working directory ($TOP below), not relative to $CONF. So under -c this
# must be rejected the same way, and the download must target the path the
# JVM will actually open rather than unconditionally ${CONF}.
if [[ $REST_SERVER_URL == https* ]]; then
    KEYSTORE_FILE=$(read_property "$CONF/rest-server.properties" "ssl.keystore_file")
    if [ -z "$KEYSTORE_FILE" ]; then
        KEYSTORE_FILE="conf/hugegraph-server.keystore"
    fi
    reject_relative_conf_path "$KEYSTORE_FILE" "ssl.keystore_file" "$CONF/rest-server.properties" || exit 1

    case "$KEYSTORE_FILE" in
        /*) KEYSTORE_PATH="$KEYSTORE_FILE" ;;
        *) KEYSTORE_PATH="$TOP/$KEYSTORE_FILE" ;;
    esac

    if [ ! -e "$KEYSTORE_PATH" ]; then
        download "$(dirname "$KEYSTORE_PATH")" "https://github.com/apache/hugegraph-doc/raw/binary-1.5/dist/server/hugegraph-server.keystore"
    fi
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

function forward_signal_and_wait() {
    local signal="$1"
    local exit_code="$2"

    # A process-group signal and the entrypoint's forwarding can arrive twice.
    # Keep the wrapper alive until it has reaped its child in either case.
    trap '' SIGHUP SIGINT SIGQUIT SIGTERM
    if kill -0 "$PID" 2>/dev/null; then
        kill "-$signal" "$PID" 2>/dev/null || true
        # The foreground wait is interrupted before the trap runs, so retry
        # until the child has exited and been reaped.
        while kill -0 "$PID" 2>/dev/null; do
            wait "$PID" 2>/dev/null || true
        done
    fi
    exit "$exit_code"
}

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
            echo "See the container logs ('docker logs' or 'kubectl logs') for HugeGraphServer log output." >&2
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
    trap 'forward_signal_and_wait HUP 129' SIGHUP
    # The background JVM can inherit an ignored SIGINT disposition, so use
    # SIGTERM to guarantee that Ctrl-C shuts it down while retaining exit 130.
    trap 'forward_signal_and_wait TERM 130' SIGINT
    # Forward TERM instead of QUIT: the JVM only dumps threads on SIGQUIT
    # and keeps running, which would leave the wait loop below stuck.
    trap 'forward_signal_and_wait TERM 131' SIGQUIT
    trap 'forward_signal_and_wait TERM 143' SIGTERM
    wait $PID
    exit $?
fi
