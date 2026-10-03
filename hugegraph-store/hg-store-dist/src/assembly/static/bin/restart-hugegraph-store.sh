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

DAEMON_ARG=""
CONF_ARG=""
GC_ARG=""
PID_ARG=""
USER_ARG=""
LOGS_ARG=""
PLUGINS_ARG=""
TELEMETRY_ARG=""

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

# Run stop and start as separate processes so that:
bash "$BIN"/stop-hugegraph-store.sh "${STOP_ARGS[@]}"
bash "$BIN"/start-hugegraph-store.sh "${START_ARGS[@]}"
