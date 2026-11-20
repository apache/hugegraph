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

# Must match the serialized form start-monitor.sh saves (one quoted shell word).
# shell_quote (not cron_quote) so removal is not blocked by validation.
MONITOR_RAW="$TOP/bin/monitor-hugegraph.sh"
MONITOR_Q="$(shell_quote "$MONITOR_RAW")"

# Match both the current quoted form and the legacy unquoted form (older
# start-monitor.sh versions wrote "... && $TOP/bin/monitor-hugegraph.sh"
# without quoting). Otherwise a job installed before this quoting fix survives
# an in-place upgrade: grep -F never matches it, removal silently no-ops, and
# the stale cron entry can restart the server within a minute.
# The legacy key is anchored with the literal "&& " prefix (not just the bare
# path): grep -F does substring matching, so an unanchored bare path would
# also match as a SUBSTRING of a different install's longer path (e.g. the
# key for /opt/hg matches a crontab line for /data/opt/hg), removing the
# wrong install's job.
MONITOR_KEY="$MONITOR_Q"
MONITOR_LEGACY_KEY="&& $MONITOR_RAW"

# Both keys additionally depend only on $TOP, so two instances of one install
# with different -i pid files would otherwise be indistinguishable.
# Only disambiguate by pid file when -i is explicitly set (stop-hugegraph.sh
# exports PID_FILE_OVERRIDE before calling this script in that case): the
# default single-instance (no -i) key and job stay byte-for-byte unchanged so
# jobs installed by older, pre-this-fix versions are still removed on upgrade.
# Must stay in sync with start-monitor.sh's keys.
if [ -n "$PID_FILE_OVERRIDE" ]; then
    PID_FILE_Q="$(shell_quote "$PID_FILE_OVERRIDE")"
    MONITOR_KEY="$MONITOR_Q # pidfile:$PID_FILE_Q"
    MONITOR_LEGACY_KEY="&& $MONITOR_RAW # pidfile:$PID_FILE_OVERRIDE"
fi
crontab_remove "$MONITOR_KEY" "$MONITOR_LEGACY_KEY"
