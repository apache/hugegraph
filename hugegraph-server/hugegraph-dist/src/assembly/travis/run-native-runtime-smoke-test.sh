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
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "USAGE: $0 SERVER_DIR" >&2
    exit 1
fi

TRAVIS_DIR=$(cd "$(dirname "$0")" && pwd)
SERVER_DIR=$(cd "$1" && pwd)
SERVER_URL=${SERVER_URL:-http://127.0.0.1:8080}
EXPECTED_ARCH=${EXPECTED_ARCH:-riscv64}
SERVER_START_ATTEMPTED=false
SERVER_START_COUNT=0
SERVER_STARTUP_TIMEOUT=${SERVER_STARTUP_TIMEOUT:-300}
SERVER_START_COMMAND_TIMEOUT=$((SERVER_STARTUP_TIMEOUT + 30))
STORE_DUMP_TIMEOUT=${STORE_DUMP_TIMEOUT:-120}
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/hugegraph-native-runtime-smoke.XXXXXX")
RUN_ID="$(date +%s)_$$"

export EXPECTED_ARCH

cleanup() {
    local status=$?
    trap - EXIT
    if [[ "$SERVER_START_ATTEMPTED" == "true" ]]; then
        "$SERVER_DIR/bin/stop-hugegraph.sh" -m false || status=1
    fi
    rm -rf "$WORK_DIR"
    exit "$status"
}
trap cleanup EXIT

start_server() {
    if command -v timeout >/dev/null 2>&1; then
        TIMEOUT_COMMAND=timeout
    elif command -v gtimeout >/dev/null 2>&1; then
        TIMEOUT_COMMAND=gtimeout
    else
        echo "Required command is unavailable: timeout or gtimeout" >&2
        return 1
    fi
    local start_options=(-t "$SERVER_STARTUP_TIMEOUT")
    if [[ -n "${SERVER_JAVA_OPTIONS:-}" ]]; then
        start_options+=(-j "$SERVER_JAVA_OPTIONS")
    fi
    SERVER_START_COUNT=$((SERVER_START_COUNT + 1))
    local startup_log="$WORK_DIR/startup-$SERVER_START_COUNT.log" status=0
    # Daemon stdout must go to a file so a pipe reader does not wait for JVM exit.
    "$TIMEOUT_COMMAND" --foreground --kill-after=15s \
            "${SERVER_START_COMMAND_TIMEOUT}s" \
            "$SERVER_DIR/bin/start-hugegraph.sh" "${start_options[@]}" \
            >"$startup_log" 2>&1 || status=$?
    cat "$startup_log"
    return "$status"
}

"$TRAVIS_DIR/run-rocksdb-jni-smoke-test.sh" "$SERVER_DIR" 2>&1 | tee "$WORK_DIR/jni.log"

"$SERVER_DIR/bin/init-store.sh" 2>&1 | tee "$WORK_DIR/init-store.log"
SERVER_START_ATTEMPTED=true
start_server
"$TRAVIS_DIR/run-server-e2e-smoke-test.sh" "$SERVER_URL" create "$RUN_ID" | \
    tee "$WORK_DIR/create.log"
grep -q '^server-e2e-smoke-create-ok$' "$WORK_DIR/create.log"

"$SERVER_DIR/bin/stop-hugegraph.sh" -m false
"$TIMEOUT_COMMAND" --foreground --kill-after=15s "${STORE_DUMP_TIMEOUT}s" \
        "$SERVER_DIR/bin/dump-store.sh" 2>&1 | tee "$WORK_DIR/store-dump.log"
grep -q 'Dump table VERTEX (offset 0 limit 20):' "$WORK_DIR/store-dump.log"
grep -Fq "riscv-smoke-v1-$RUN_ID" "$WORK_DIR/store-dump.log"
grep -Fq "riscv-smoke-v2-$RUN_ID" "$WORK_DIR/store-dump.log"
echo "store-dump-smoke-ok"

start_server
"$TRAVIS_DIR/run-server-e2e-smoke-test.sh" "$SERVER_URL" verify "$RUN_ID" | \
    tee "$WORK_DIR/verify.log"
grep -q '^server-e2e-smoke-verify-ok$' "$WORK_DIR/verify.log"

LOG_INPUTS=("$WORK_DIR")
if [[ "${STDOUT_MODE:-false}" != true ]] || \
   [[ -n "$(find "$SERVER_DIR/logs" -type f -name '*.log' -print -quit 2>/dev/null)" ]]; then
    LOG_INPUTS+=("$SERVER_DIR/logs")
fi
"$TRAVIS_DIR/check-java-runtime-logs.sh" --native "${LOG_INPUTS[@]}"

echo "native-runtime-smoke-ok"
