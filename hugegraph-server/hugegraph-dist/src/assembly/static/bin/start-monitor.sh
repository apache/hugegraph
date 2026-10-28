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
MONITOR_RAW="$TOP/bin/monitor-hugegraph.sh"
MONITOR_Q="$(cron_quote "$MONITOR_RAW" "monitor script path")" || exit 1

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

# Keys cover both the current quoted form and the legacy unquoted form (older
# start-monitor.sh versions wrote "... && $TOP/bin/monitor-hugegraph.sh"
# without quoting), so an in-place upgrade detects an existing job as a
# duplicate instead of installing a second one alongside it.
# The legacy key is anchored with the literal "&& " prefix (not just the bare
# path): grep -F does substring matching, so an unanchored bare path would
# also match as a SUBSTRING of a different install's longer path (e.g. the
# key for /opt/hg matches a crontab line for /data/opt/hg), causing a
# false-positive "already installed" here and a wrong-job removal in
# stop-monitor.sh.
MONITOR_LEGACY_KEY="&& $MONITOR_RAW"
crontab_append "$CRONTAB_JOB" "$MONITOR_Q" "$MONITOR_LEGACY_KEY"
