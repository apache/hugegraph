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

BIN="$(abs_path)"
TOP="$(cd "$BIN"/../ && pwd)"

. "$BIN"/util.sh

if [ "$JAVA_HOME" == "" ]; then
    echo "Must set JAVA_HOME environment variable and install JDK >= 17"
    exit 1
fi

# Monitor HugeGraphServer every minute, if the server crashes then restart it.
# Modify the frequency according to actual needs carefully.

# Persist any path overrides (-c/-l/-i/-o) that the caller (start-hugegraph.sh) exported.
# Every value goes through cron_quote: it validates (CR/LF, '%') and emits ONE complete
# POSIX shell word.
JAVA_HOME_Q="$(cron_quote "$JAVA_HOME" "JAVA_HOME")" || exit 1
MONITOR_Q="$(cron_quote "$TOP/bin/monitor-hugegraph.sh" "monitor script path")" || exit 1

CRONTAB_JOB="*/1 * * * * export JAVA_HOME=$JAVA_HOME_Q &&"
if [ -n "$CONF_OVERRIDE" ]; then
    CONF_Q="$(cron_quote "$CONF_OVERRIDE" "CONF_OVERRIDE")" || exit 1
    CRONTAB_JOB="$CRONTAB_JOB export CONF_OVERRIDE=$CONF_Q &&"
fi
if [ -n "$LOGS_OVERRIDE" ]; then
    LOGS_Q="$(cron_quote "$LOGS_OVERRIDE" "LOGS_OVERRIDE")" || exit 1
    CRONTAB_JOB="$CRONTAB_JOB export LOGS_OVERRIDE=$LOGS_Q &&"
fi
if [ -n "$PID_FILE_OVERRIDE" ]; then
    PID_Q="$(cron_quote "$PID_FILE_OVERRIDE" "PID_FILE_OVERRIDE")" || exit 1
    CRONTAB_JOB="$CRONTAB_JOB export PID_FILE_OVERRIDE=$PID_Q &&"
fi
if [ -n "$PLUGINS_OVERRIDE" ]; then
    PLUGINS_Q="$(cron_quote "$PLUGINS_OVERRIDE" "PLUGINS_OVERRIDE")" || exit 1
    CRONTAB_JOB="$CRONTAB_JOB export PLUGINS_OVERRIDE=$PLUGINS_Q &&"
fi
CRONTAB_JOB="$CRONTAB_JOB $MONITOR_Q"

crontab_append "$CRONTAB_JOB"
