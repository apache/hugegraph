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
# test-start-hugegraph.sh — Tests for start-hugegraph.sh foreground mode fix
#
# Baseline (unmodified code):  Tests 1, 2, 4 PASS — Tests 3, 5 FAIL
# After chunk 1 fix:           All 5 tests PASS
#
# Usage: ./test-start-hugegraph.sh [path-to-hugegraph-root]
#   path-to-hugegraph-root: path to the extracted HugeGraph server dist directory
#                           defaults to current directory if not provided
#
# Note: Tests require crontab access. On macOS Catalina+ this may
#       require Full Disk Access permission in System Preferences.
# Note: Assumes rocksdb backend. init-store.sh is run automatically if needed.

set -uo pipefail

HUGEGRAPH_ROOT="${1:-$(pwd)}"
BIN="$HUGEGRAPH_ROOT/bin"
START_SCRIPT="$BIN/start-hugegraph.sh"
STOP_SCRIPT="$BIN/stop-hugegraph.sh"
PID_FILE="$BIN/pid"
SERVER_URL="http://127.0.0.1:8080"
STARTUP_WAIT=60   # seconds to wait for server HTTP to respond
SETTLE_WAIT=5     # seconds after kill before checking process exit
WAIT_TIMEOUT=30   # seconds for timeout on wait calls
PASS=0
FAIL=0
ERRORS=()

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() {
    echo -e "${GREEN}  PASS${NC} $1"
    PASS=$((PASS + 1))
}

fail() {
    echo -e "${RED}  FAIL${NC} $1"
    ERRORS+=("$1")
    FAIL=$((FAIL + 1))
}

info() {
    echo -e "${YELLOW}  ....${NC} $1"
}

section() {
    echo ""
    echo "── $1 ──"
}

# Kill server, clear pid file, kill ports, clear crontab monitor entry
cleanup() {
    info "Cleaning up..."
    "$STOP_SCRIPT" >/dev/null 2>&1 || true
    if [[ -s "$PID_FILE" ]]; then
        kill "$(cat "$PID_FILE")" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
    # kill anything still holding server ports so check_port doesn't fail next test
    lsof -ti :8080 | xargs kill -9 2>/dev/null || true
    lsof -ti :8182 | xargs kill -9 2>/dev/null || true
    lsof -ti :8088 | xargs kill -9 2>/dev/null || true
    sleep 3
    crontab -l 2>/dev/null | grep -v monitor-hugegraph | crontab - 2>/dev/null || true
}

# Wait until server HTTP endpoint responds (200 or 401) or timeout
wait_for_server() {
    local elapsed=0
    while (( elapsed < STARTUP_WAIT )); do
        local status
        status=$(curl -s -o /dev/null -w "%{http_code}" \
            "$SERVER_URL/versions" 2>/dev/null || echo "000")
        if [[ "$status" == "200" || "$status" == "401" ]]; then
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    return 1
}

# Wait until bin/pid is non-empty or timeout
wait_for_pid_file() {
    local elapsed=0
    while [[ ! -s "$PID_FILE" ]] && (( elapsed < 30 )); do
        sleep 1
        elapsed=$((elapsed + 1))
    done
}

# Wait for a background script PID to exit, with timeout
# Usage: wait_script_exit <script_pid>
# Returns the real exit code of the process, or kills it after WAIT_TIMEOUT seconds
# NOTE: must be called from the same shell that spawned script_pid so that
#       bash's built-in wait can track it as a child process
wait_script_exit() {
    local script_pid="$1"
    # background killer fires after timeout if process hasn't exited yet
    ( sleep "$WAIT_TIMEOUT" && kill "$script_pid" 2>/dev/null ) &
    local killer_pid=$!
    wait "$script_pid" 2>/dev/null
    local exit_code=$?
    # cancel the killer if process already exited
    kill "$killer_pid" 2>/dev/null || true
    wait "$killer_pid" 2>/dev/null || true
    return $exit_code
}

# ── preflight ─────────────────────────────────────────────────────────────────

echo ""
echo "start-hugegraph.sh chunk 1 test suite"
echo "root: $HUGEGRAPH_ROOT"
echo ""

if [[ ! -f "$START_SCRIPT" ]]; then
    echo -e "${RED}ERROR:${NC} $START_SCRIPT not found."
    echo "       Pass the HugeGraph dist root as \$1"
    exit 1
fi

if [[ ! -f "$STOP_SCRIPT" ]]; then
    echo -e "${RED}ERROR:${NC} $STOP_SCRIPT not found."
    exit 1
fi

for tool in lsof crontab curl java; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "SKIP: required tool '$tool' not found — skipping test suite"
        exit 77
    fi
done

# start-monitor.sh requires JAVA_HOME
if [[ -z "${JAVA_HOME:-}" ]]; then
    if command -v /usr/libexec/java_home >/dev/null 2>&1; then
        export JAVA_HOME="$(/usr/libexec/java_home 2>/dev/null)"
    fi
fi

# Warn if JAVA_HOME could not be determined
if [[ -z "${JAVA_HOME:-}" ]]; then
    echo -e "${YELLOW}WARN${NC} JAVA_HOME is not set"
    echo "       Test 4 may fail because start-monitor.sh requires JAVA_HOME"
    echo "       Export JAVA_HOME before running this script"
fi

# Run init-store.sh once if RocksDB has not been initialized yet
if [[ ! -d "$HUGEGRAPH_ROOT/rocksdb-data" ]]; then
    info "RocksDB not initialized — running init-store.sh..."
    if "$BIN/init-store.sh" >/dev/null 2>&1; then
        pass "init-store.sh completed successfully"
    else
        echo -e "${RED}ERROR:${NC} init-store.sh failed. Cannot run tests."
        exit 1
    fi
else
    info "RocksDB already initialized, skipping init-store.sh"
fi

cleanup

# ── test 1: daemon mode regression ───────────────────────────────────────────

section "Test 1 — daemon mode regression"

info "Starting in daemon mode (waiting up to ${STARTUP_WAIT}s for HTTP)..."
"$START_SCRIPT" -d true -t "$STARTUP_WAIT" >/dev/null 2>&1
EXIT_CODE=$?

if [[ $EXIT_CODE -eq 0 ]]; then
    pass "script returned exit 0 in daemon mode"
else
    fail "script returned non-zero exit $EXIT_CODE in daemon mode"
fi

wait_for_pid_file
DAEMON_PID=$(cat "$PID_FILE" 2>/dev/null || echo "")

if [[ -n "$DAEMON_PID" ]]; then
    pass "bin/pid written with PID $DAEMON_PID"
else
    fail "bin/pid is empty or missing after daemon start"
fi

if [[ -n "$DAEMON_PID" ]] && ps -p "$DAEMON_PID" >/dev/null 2>&1; then
    pass "Java process is running (pid $DAEMON_PID)"
else
    fail "Java process not found for pid '$DAEMON_PID'"
fi

if wait_for_server; then
    pass "server HTTP endpoint responding at $SERVER_URL"
else
    fail "server HTTP endpoint not responding after ${STARTUP_WAIT}s"
fi

info "Stopping server..."
"$STOP_SCRIPT" >/dev/null 2>&1
sleep 3

if [[ -n "$DAEMON_PID" ]] && ! ps -p "$DAEMON_PID" >/dev/null 2>&1; then
    pass "stop-hugegraph.sh successfully killed the process"
else
    fail "process still running after stop-hugegraph.sh"
fi

cleanup

# ── test 2: foreground mode blocks ───────────────────────────────────────────

section "Test 2 — foreground mode blocks until Java exits"

info "Starting in foreground mode (backgrounded for observation)..."
"$START_SCRIPT" -d false >/dev/null 2>&1 &
SCRIPT_PID=$!

info "Waiting up to ${STARTUP_WAIT}s for server to come up..."
if wait_for_server; then
    info "Server is up"
else
    info "Server did not respond — continuing to check blocking behavior"
fi

# Primary assertion: script must still be running while Java is alive
if ps -p "$SCRIPT_PID" >/dev/null 2>&1; then
    pass "script is still running (blocking correctly)"
else
    fail "script exited early — foreground mode is not blocking"
fi

# Secondary observations only (the actual bug is tested in Test 3)
wait_for_pid_file
FG_PID=$(cat "$PID_FILE" 2>/dev/null || echo "")

if [[ -n "$FG_PID" ]]; then
    info "pid file written with PID $FG_PID"
else
    info "pid file not written in foreground mode (expected before chunk 1 fix)"
fi

if [[ -n "$FG_PID" ]] && ps -p "$FG_PID" >/dev/null 2>&1; then
    info "Java process running (pid $FG_PID)"
else
    info "Java PID not available (expected before chunk 1 fix)"
fi

if [[ -n "$FG_PID" ]]; then
    info "Killing Java process..."
    kill "$FG_PID" 2>/dev/null || true
    sleep "$SETTLE_WAIT"

    if ! ps -p "$SCRIPT_PID" >/dev/null 2>&1; then
        info "script exited after Java was killed"
    else
        info "script still running after Java was killed"
        kill "$SCRIPT_PID" 2>/dev/null || true
    fi
else
    kill "$SCRIPT_PID" 2>/dev/null || true
fi

cleanup

# ── test 3: exit code propagates ─────────────────────────────────────────────

section "Test 3 — foreground PID tracking and exit propagation"

info "Starting in foreground mode..."
"$START_SCRIPT" -d false >/dev/null 2>&1 &
SCRIPT_PID=$!

info "Waiting up to ${STARTUP_WAIT}s for server..."
if wait_for_server; then
    info "Server is up"
else
    info "Server did not respond — continuing"
fi

wait_for_pid_file
FG_PID=$(cat "$PID_FILE" 2>/dev/null || echo "")

if [[ -n "$FG_PID" ]]; then
    pass "bin/pid written with PID $FG_PID"
else
    fail "bin/pid is empty or missing in foreground mode"
fi

if [[ -n "$FG_PID" ]] && ps -p "$FG_PID" >/dev/null 2>&1; then
    pass "Java process running (pid $FG_PID)"
else
    fail "Java process not found for pid '$FG_PID'"
fi

if [[ -z "$FG_PID" ]]; then
    fail "could not get PID from pid file — skipping exit code check"
    kill "$SCRIPT_PID" 2>/dev/null || true
else
    info "Hard-killing Java with SIGKILL (pid $FG_PID)..."
    kill -9 "$FG_PID" 2>/dev/null || true

    wait_script_exit "$SCRIPT_PID"
    ACTUAL_EXIT=$?

    if [[ $ACTUAL_EXIT -ne 0 ]]; then
        pass "script exited non-zero ($ACTUAL_EXIT) after SIGKILL — Docker restart will fire"
    else
        fail "script exited 0 after SIGKILL — Docker would NOT restart (exit code lost)"
    fi

    if [[ $ACTUAL_EXIT -eq 137 ]]; then
        pass "exit code is 137 (128+9 for SIGKILL) — correctly propagated"
    elif [[ $ACTUAL_EXIT -ne 0 ]]; then
        info "exit code was $ACTUAL_EXIT (not 137 — may be shell-wrapped, acceptable if non-zero)"
    fi
fi

cleanup

# ── test 4: monitor registers in daemon mode ──────────────────────────────────

section "Test 4 — -m true registers cron job in daemon mode"

info "Clearing crontab..."
crontab -l 2>/dev/null | grep -v monitor-hugegraph | crontab - 2>/dev/null || true

info "Starting in daemon mode with -m true (waiting up to ${STARTUP_WAIT}s)..."
"$START_SCRIPT" -d true -m true -t "$STARTUP_WAIT" >/dev/null 2>&1
EXIT_CODE=$?

if [[ $EXIT_CODE -eq 0 ]]; then
    pass "daemon started successfully with -m true"
else
    fail "daemon start failed with exit $EXIT_CODE — monitor may not have registered"
fi

if crontab -l 2>/dev/null | grep -q "monitor-hugegraph"; then
    pass "cron job registered for monitor-hugegraph.sh"
else
    fail "cron job NOT registered — OPEN_MONITOR block broken in daemon mode"
fi

info "Stopping server..."
"$STOP_SCRIPT" -m false >/dev/null 2>&1 || true

cleanup

# ── test 5: SIGTERM forwarded to Java in foreground mode ─────────────────────

section "Test 5 — SIGTERM forwarded to Java in foreground mode"

info "Starting in foreground mode..."
"$START_SCRIPT" -d false >/dev/null 2>&1 &
SCRIPT_PID=$!

info "Waiting up to ${STARTUP_WAIT}s for server..."
if wait_for_server; then
    info "Server is up"
else
    info "Server did not respond — continuing"
fi

wait_for_pid_file
FG_PID=$(cat "$PID_FILE" 2>/dev/null || echo "")

if [[ -z "$FG_PID" ]]; then
    fail "could not get Java PID — skipping signal forwarding check"
    kill "$SCRIPT_PID" 2>/dev/null || true
else
    info "Sending SIGTERM to wrapper script (pid $SCRIPT_PID)..."
    kill -TERM "$SCRIPT_PID" 2>/dev/null || true

    wait_script_exit "$SCRIPT_PID"
    ACTUAL_EXIT=$?

    # If the trap fired correctly, the wrapper's `wait $PID` already reaped Java.
    # If wait_script_exit timed out (killer fired), Java may still be running — also a failure.
    if ! ps -p "$FG_PID" >/dev/null 2>&1; then
        pass "Java process terminated after SIGTERM sent to wrapper"
    else
        fail "Java process still running after SIGTERM — signal not forwarded"
        kill "$FG_PID" 2>/dev/null || true
    fi

    if [[ $ACTUAL_EXIT -ne 0 ]]; then
        pass "wrapper script exited non-zero ($ACTUAL_EXIT) after SIGTERM"
    else
        fail "wrapper script exited 0 after SIGTERM — exit code not propagated"
    fi

    if [[ $ACTUAL_EXIT -eq 143 ]]; then
        pass "exit code is 143 (128+15 for SIGTERM) — correctly propagated"
    elif [[ $ACTUAL_EXIT -ne 0 ]]; then
        info "exit code was $ACTUAL_EXIT (not 143 — may be shell-wrapped, acceptable if non-zero)"
    fi
fi

cleanup

# ── summary ───────────────────────────────────────────────────────────────────

echo ""
echo "════════════════════════════════"
echo -e "  Results: ${GREEN}$PASS passed${NC}  ${RED}$FAIL failed${NC}"
echo "════════════════════════════════"

if [[ ${#ERRORS[@]} -gt 0 ]]; then
    echo ""
    echo "Failed tests:"
    for err in "${ERRORS[@]}"; do
        echo -e "  ${RED}✗${NC} $err"
    done
fi

echo ""
[[ $FAIL -eq 0 ]] && exit 0 || exit 1
