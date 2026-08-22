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
        "$SERVER_DIR/bin/stop-hugegraph.sh" -m false >/dev/null 2>&1 || status=1
    fi
    rm -rf "$WORK_DIR"
    exit "$status"
}
trap cleanup EXIT

start_server() {
    if ! command -v timeout >/dev/null 2>&1; then
        echo "Required command is unavailable: timeout" >&2
        return 1
    fi
    timeout --foreground --kill-after=15s \
            "${SERVER_START_COMMAND_TIMEOUT}s" \
            "$SERVER_DIR/bin/start-hugegraph.sh" -t "$SERVER_STARTUP_TIMEOUT"
}

"$TRAVIS_DIR/run-rocksdb-jni-smoke-test.sh" "$SERVER_DIR"

"$SERVER_DIR/bin/init-store.sh"
SERVER_START_ATTEMPTED=true
start_server
"$TRAVIS_DIR/run-server-e2e-smoke-test.sh" "$SERVER_URL" create "$RUN_ID" | \
    tee "$WORK_DIR/create.log"
grep -q '^server-e2e-smoke-create-ok$' "$WORK_DIR/create.log"

"$SERVER_DIR/bin/stop-hugegraph.sh" -m false
timeout --foreground --kill-after=15s "${STORE_DUMP_TIMEOUT}s" \
        "$SERVER_DIR/bin/dump-store.sh" | tee "$WORK_DIR/store-dump.log"
grep -q 'Dump table VERTEX (offset 0 limit 20):' "$WORK_DIR/store-dump.log"
grep -Fq "riscv-smoke-v1-$RUN_ID" "$WORK_DIR/store-dump.log"
grep -Fq "riscv-smoke-v2-$RUN_ID" "$WORK_DIR/store-dump.log"
echo "store-dump-smoke-ok"

start_server
"$TRAVIS_DIR/run-server-e2e-smoke-test.sh" "$SERVER_URL" verify "$RUN_ID" | \
    tee "$WORK_DIR/verify.log"
grep -q '^server-e2e-smoke-verify-ok$' "$WORK_DIR/verify.log"

if [[ -d "$SERVER_DIR/logs" ]] && \
   grep -Eirq 'undefined symbol|UnsatisfiedLinkError|UnsupportedClassVersionError|NoClassDefFoundError' \
        "$SERVER_DIR/logs"; then
    echo "Native linkage or Java compatibility error found in $SERVER_DIR/logs" >&2
    grep -Eirn 'undefined symbol|UnsatisfiedLinkError|UnsupportedClassVersionError|NoClassDefFoundError' \
        "$SERVER_DIR/logs" >&2
    exit 1
fi

echo "native-runtime-smoke-ok"
