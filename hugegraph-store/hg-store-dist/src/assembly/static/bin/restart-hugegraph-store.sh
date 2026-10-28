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
    while [ -h "$SOURCE" ]; do
        DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    echo "$( cd -P "$( dirname "$SOURCE" )" && pwd )"
}

BIN=$(abs_path)
TOP="$(cd "$BIN"/../ && pwd)"

DAEMON_ARG=""
GC_ARG=""
USER_ARG=""
TELEMETRY_ARG=""
CONF_ARG="${CONF_OVERRIDE:-}"
LOGS_ARG="${LOGS_OVERRIDE:-}"
PLUGINS_ARG="${PLUGINS_OVERRIDE:-}"
PID_ARG="${PID_FILE_OVERRIDE:-}"

while getopts "d:c:g:i:j:l:o:y:" arg; do
    case ${arg} in
        d) DAEMON_ARG="$OPTARG" ;;
        c) CONF_ARG="$OPTARG" ;;
        g) GC_ARG="$OPTARG" ;;
        i) PID_ARG="$OPTARG" ;;
        j) USER_ARG="$OPTARG" ;;
        l) LOGS_ARG="$OPTARG" ;;
        o) PLUGINS_ARG="$OPTARG" ;;
        y) TELEMETRY_ARG="$OPTARG" ;;
        ?) echo "USAGE: $0 [-d true|false] [-c conf_dir] [-g g1] [-i pid_file] [-j opts] [-l logs_dir] [-o plugins_dir] [-y true|false]" && exit 1 ;;
    esac
done

. "$BIN"/util.sh

# -c/-l/-o are validated only by start-hugegraph-store.sh, which runs *after*
# the stop below. Check them here so an unusable override is rejected while the
# Store is still up, instead of stopping it and then failing to start it again.
# -i needs no check here: stop-hugegraph-store.sh validates it first, before it
# touches the running process.
reject_unsafe_path "$CONF_ARG" "-c conf dir" crlf || exit 1
reject_unsafe_path "$LOGS_ARG" "-l logs dir" crlf || exit 1
# The plugins dir is joined into ':'-separated classpath/agent settings
reject_unsafe_path "$PLUGINS_ARG" "-o plugins dir" classpath || exit 1

# The checks above only reject unsafe characters; they don't prove -c/-l/-o
# are actually usable. Resolve them exactly as start-hugegraph-store.sh will
# and run its writability/readability guards here too, so a bad
# override is caught while the current Store is still up and serving traffic
RESTART_CONF_OVERRIDE="$(canonicalize_dir "$CONF_ARG" nocreate)" || exit 1
RESTART_LOGS_OVERRIDE="$(canonicalize_dir "$LOGS_ARG")" || exit 1
RESTART_PLUGINS_OVERRIDE="$(canonicalize_dir "$PLUGINS_ARG")" || exit 1
RESTART_PID_FILE_OVERRIDE="$(canonicalize_file "$PID_ARG")" || exit 1

# Re-validate the *effective*, canonicalized plugins path: cd/pwd resolves a
# relative override against the current working directory, which can
# introduce a ':' (or other unsafe char) that wasn't present in the raw value
# (e.g. launching from a directory such as "/tmp/work:one").
reject_unsafe_path "$RESTART_PLUGINS_OVERRIDE" "-o plugins dir (resolved)" classpath || exit 1

RESTART_CONF="${RESTART_CONF_OVERRIDE:-$TOP/conf}"
RESTART_LOGS="${RESTART_LOGS_OVERRIDE:-$TOP/logs}"
RESTART_PLUGINS="${RESTART_PLUGINS_OVERRIDE:-$TOP/plugins}"
RESTART_PID_FILE="${RESTART_PID_FILE_OVERRIDE:-$BIN/pid}"

ensure_path_writable "$RESTART_LOGS"
ensure_path_writable "$RESTART_PLUGINS"
ensure_path_writable "$(dirname "$RESTART_PID_FILE")"

if [ -n "$RESTART_CONF_OVERRIDE" ]; then
    ensure_file_readable "${RESTART_CONF}/application.yml" "Store config" || exit 1
fi

# stop-hugegraph-store.sh only understands -i (the pid file).Forward the -i flag to stop script
STOP_ARGS=()
[ -n "$PID_ARG" ] && STOP_ARGS+=(-i "$PID_ARG")

# Forward every override to start so the restarted instance also keeps the overriden
# conf/logs/pid/plugins/daemon/gc/telemetry settings
START_ARGS=()
[ -n "$DAEMON_ARG" ]    && START_ARGS+=(-d "$DAEMON_ARG")
[ -n "$CONF_ARG" ]      && START_ARGS+=(-c "$CONF_ARG")
[ -n "$GC_ARG" ]        && START_ARGS+=(-g "$GC_ARG")
[ -n "$PID_ARG" ]       && START_ARGS+=(-i "$PID_ARG")
[ -n "$USER_ARG" ]      && START_ARGS+=(-j "$USER_ARG")
[ -n "$LOGS_ARG" ]      && START_ARGS+=(-l "$LOGS_ARG")
[ -n "$PLUGINS_ARG" ]   && START_ARGS+=(-o "$PLUGINS_ARG")
[ -n "$TELEMETRY_ARG" ] && START_ARGS+=(-y "$TELEMETRY_ARG")

# Run stop and start as separate processes, and check the stop exit code:
# a failed stop leaves the old JVM running, so starting a new one anyway
# would look like a successful restart while the stale process stays up.
bash "$BIN"/stop-hugegraph-store.sh "${STOP_ARGS[@]}" || exit 1
bash "$BIN"/start-hugegraph-store.sh "${START_ARGS[@]}"
