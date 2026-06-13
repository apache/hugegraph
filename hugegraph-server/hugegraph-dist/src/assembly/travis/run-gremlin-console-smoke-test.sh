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

SERVER_DIR=${1:?Usage: run-gremlin-console-smoke-test.sh <server-dir>}
SMOKE_MARKER="gremlin-console-smoke-ok"

if [ ! -d "$SERVER_DIR" ]; then
    echo "Server directory does not exist: $SERVER_DIR"
    exit 1
fi

if [ ! -x "$SERVER_DIR/bin/gremlin-console.sh" ]; then
    echo "Gremlin Console script is not executable: $SERVER_DIR/bin/gremlin-console.sh"
    exit 1
fi

TMP_DIR=${TMPDIR:-/tmp}
TMP_WORK_DIR=$(mktemp -d "${TMP_DIR}/hugegraph-gremlin-console-smoke.XXXXXX")
SMOKE_SCRIPT="${TMP_WORK_DIR}/smoke.groovy"
SMOKE_LOG="${TMP_WORK_DIR}/smoke.log"

cleanup() {
    rm -rf "$TMP_WORK_DIR"
}
trap cleanup EXIT

cat > "$SMOKE_SCRIPT" <<EOF
println("${SMOKE_MARKER}-" + (1 + 1))
EOF

(
    cd "$SERVER_DIR"
    bin/gremlin-console.sh -- -e "$SMOKE_SCRIPT"
) | tee "$SMOKE_LOG"

grep -q "$SMOKE_MARKER" "$SMOKE_LOG"
