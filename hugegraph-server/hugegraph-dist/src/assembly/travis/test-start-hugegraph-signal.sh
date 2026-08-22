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
# Verifies that foreground start-hugegraph.sh exits on SIGINT when its
# background server child ignores SIGINT, as a JVM may do in this launch mode.

set -uo pipefail

START_SCRIPT="${1:-}"
if [[ -z "$START_SCRIPT" ]]; then
    echo "Usage: $0 <path-to-start-hugegraph.sh>"
    exit 2
fi

if [[ ! -f "$START_SCRIPT" ]]; then
    echo "ERROR: start script not found: $START_SCRIPT"
    exit 2
fi

if ! command -v timeout >/dev/null 2>&1; then
    echo "SKIP: required tool 'timeout' not found"
    exit 77
fi

TEST_ROOT=$(mktemp -d)
PID_FILE="$TEST_ROOT/bin/pid"

cleanup() {
    if [[ -s "$PID_FILE" ]]; then
        kill -TERM "$(cat "$PID_FILE")" 2>/dev/null || true
    fi
    rm -rf "$TEST_ROOT"
}
trap cleanup EXIT

mkdir -p "$TEST_ROOT/bin" "$TEST_ROOT/conf" "$TEST_ROOT/logs" "$TEST_ROOT/scripts"
cp "$START_SCRIPT" "$TEST_ROOT/bin/start-hugegraph.sh"

cat > "$TEST_ROOT/bin/util.sh" <<'EOF'
read_property() {
    local file="$1"
    local property="$2"
    grep "^${property}=" "$file" | head -n 1 | cut -d '=' -f 2-
}

check_port() {
    :
}
EOF

cat > "$TEST_ROOT/bin/hugegraph-server.sh" <<'EOF'
#!/bin/bash
trap 'exit 0' TERM
trap '' INT
while true; do
    sleep 1
done
EOF

cat > "$TEST_ROOT/conf/rest-server.properties" <<'EOF'
gremlinserver.url=http://127.0.0.1:8182
restserver.url=http://127.0.0.1:8080
EOF

chmod +x "$TEST_ROOT/bin/start-hugegraph.sh" "$TEST_ROOT/bin/hugegraph-server.sh"

export PID_FILE
export START_SCRIPT="$TEST_ROOT/bin/start-hugegraph.sh"

timeout --signal=TERM --kill-after=5s 10s bash -c '
    target_pid=$$
    (
        while [[ ! -s "$PID_FILE" ]]; do
            sleep 0.05
        done
        sleep 0.1
        kill -INT "$target_pid"
    ) &
    exec "$START_SCRIPT" -d false
'
ACTUAL_EXIT=$?

if [[ "$ACTUAL_EXIT" -ne 130 ]]; then
    echo "FAIL: expected exit 130 after SIGINT, got $ACTUAL_EXIT"
    exit 1
fi

if [[ -s "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "FAIL: server child is still running after SIGINT"
    exit 1
fi

echo "PASS: SIGINT terminates the foreground wrapper and its server child"
