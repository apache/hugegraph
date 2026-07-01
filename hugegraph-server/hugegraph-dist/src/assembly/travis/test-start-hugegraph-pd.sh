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
# test-start-hugegraph-pd.sh — Tests for start-hugegraph-pd.sh foreground mode fix
#
# Baseline (unmodified code):  Tests 1 PASS — Tests 2, 3, 4 FAIL
# After chunk 2 fix:           All 4 tests PASS
#
# Usage: ./test-start-hugegraph-pd.sh [path-to-pd-dist-root]
#   path-to-pd-dist-root: path to extracted PD dist e.g.
#                         hugegraph-pd/apache-hugegraph-pd-1.7.0/
#                         defaults to current directory if not provided

set -uo pipefail

PD_ROOT="${1:-$(pwd)}"
BIN="$PD_ROOT/bin"
START_SCRIPT="$BIN/start-hugegraph-pd.sh"
PID_FILE="$BIN/pid"
PD_URL="http://localhost:8620"
STARTUP_WAIT=60   # seconds to wait for PD HTTP to respond
SETTLE_WAIT=5     # seconds after kill before checking exit
WAIT_TIMEOUT=30   # seconds for wait_script_exit timeout

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

cleanup() {
    info "Cleaning up..."
    if [[ -s "$PID_FILE" ]]; then
        kill "$(cat "$PID_FILE")" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
    rm -rf "$PD_ROOT/logs/"
    # kill anything still holding the PD port
    lsof -ti :8620 | xargs kill -9 2>/dev/null || true
    lsof -ti :8686 | xargs kill -9 2>/dev/null || true
    sleep 3
}

# Wait until PD health endpoint responds or timeout
wait_for_pd() {
    local elapsed=0
    while (( elapsed < STARTUP_WAIT )); do
        local status
        status=$(curl -s -o /dev/null -w "%{http_code}" \
            "$PD_URL/v1/health" 2>/dev/null || echo "000")
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

# Wait for a background script PID to exit with timeout
# Must be called from the same shell that spawned script_pid
wait_script_exit() {
    local script_pid="$1"
    ( sleep "$WAIT_TIMEOUT" && kill "$script_pid" 2>/dev/null ) &
    local killer_pid=$!
    wait "$script_pid" 2>/dev/null
    local exit_code=$?
    kill "$killer_pid" 2>/dev/null || true
    wait "$killer_pid" 2>/dev/null || true
    return $exit_code
}

# ── preflight ─────────────────────────────────────────────────────────────────

echo ""
echo "start-hugegraph-pd.sh chunk 2 test suite"
echo "root: $PD_ROOT"
echo ""

if [[ ! -f "$START_SCRIPT" ]]; then
    echo -e "${RED}ERROR:${NC} $START_SCRIPT not found."
    echo "       Pass the PD dist root as \$1"
    exit 1
fi

for tool in lsof curl java; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "SKIP: required tool '$tool' not found — skipping test suite"
        exit 77
    fi
done

cleanup

# ── test 1: daemon mode regression ───────────────────────────────────────────

section "Test 1 — daemon mode regression"

info "Starting in daemon mode (no -d flag, default behavior)..."
"$START_SCRIPT" >/dev/null 2>&1 &
SCRIPT_PID=$!

# daemon mode: script backgrounds Java and exits quickly
sleep 5
if ! ps -p "$SCRIPT_PID" >/dev/null 2>&1; then
    pass "script exited after backgrounding Java (daemon mode)"
else
    info "script still running after 5s (may still be starting)"
    wait_script_exit "$SCRIPT_PID" || true
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

info "Waiting up to ${STARTUP_WAIT}s for PD health endpoint..."
if wait_for_pd; then
    pass "PD health endpoint responding at $PD_URL/v1/health"
else
    fail "PD health endpoint not responding after ${STARTUP_WAIT}s"
fi

cleanup

# ── test 2: foreground mode blocks ───────────────────────────────────────────

section "Test 2 — foreground mode blocks until Java exits"

info "Starting in foreground mode (-d false)..."
"$START_SCRIPT" -d false >/dev/null 2>&1 &
SCRIPT_PID=$!

info "Waiting up to ${STARTUP_WAIT}s for PD to come up..."
if wait_for_pd; then
    info "PD is up"
else
    info "PD did not respond — continuing to check blocking behavior"
fi

# foreground mode: script should still be running (blocked on Java)
if ps -p "$SCRIPT_PID" >/dev/null 2>&1; then
    pass "script is still running (blocking correctly)"
else
    fail "script exited early — foreground mode is not blocking"
fi

wait_for_pid_file
FG_PID=$(cat "$PID_FILE" 2>/dev/null || echo "")

if [[ -n "$FG_PID" ]]; then
    info "pid file written with PID $FG_PID"
else
    info "pid file not written (expected before chunk 2 fix)"
fi

# cleanup: kill Java or the script
if [[ -n "$FG_PID" ]]; then
    kill "$FG_PID" 2>/dev/null || true
else
    kill "$SCRIPT_PID" 2>/dev/null || true
fi
sleep "$SETTLE_WAIT"

if ! ps -p "$SCRIPT_PID" >/dev/null 2>&1; then
    info "script exited after Java was killed"
else
    kill "$SCRIPT_PID" 2>/dev/null || true
fi

cleanup

# ── test 3: exit code propagates ─────────────────────────────────────────────

section "Test 3 — exit code propagates from Java"

info "Starting in foreground mode (-d false)..."
"$START_SCRIPT" -d false >/dev/null 2>&1 &
SCRIPT_PID=$!

info "Waiting up to ${STARTUP_WAIT}s for PD..."
if wait_for_pd; then
    info "PD is up"
else
    info "PD did not respond — continuing"
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
    fail "could not get PID — skipping exit code check"
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

# ── test 4: SIGTERM forwarded to Java ────────────────────────────────────────

section "Test 4 — SIGTERM forwarded to Java in foreground mode"

info "Starting in foreground mode (-d false)..."
"$START_SCRIPT" -d false >/dev/null 2>&1 &
SCRIPT_PID=$!

info "Waiting up to ${STARTUP_WAIT}s for PD..."
if wait_for_pd; then
    info "PD is up"
else
    info "PD did not respond — continuing"
fi

wait_for_pid_file
FG_PID=$(cat "$PID_FILE" 2>/dev/null || echo "")

if [[ -z "$FG_PID" ]]; then
    fail "could not get Java PID — skipping SIGTERM check"
    kill "$SCRIPT_PID" 2>/dev/null || true
else
    info "Sending SIGTERM to process (pid $SCRIPT_PID)..."
    # In foreground mode with exec, SCRIPT_PID == FG_PID after exec replaces the shell
    # Signal goes directly to Java — no wrapper to forward through
    kill -TERM "$SCRIPT_PID" 2>/dev/null || true

    wait_script_exit "$SCRIPT_PID"
    ACTUAL_EXIT=$?

    if ! ps -p "$FG_PID" >/dev/null 2>&1; then
        pass "Java process terminated after SIGTERM"
    else
        fail "Java process still running after SIGTERM — signal not delivered"
        kill "$FG_PID" 2>/dev/null || true
    fi

    if [[ $ACTUAL_EXIT -ne 0 ]]; then
        pass "process exited non-zero ($ACTUAL_EXIT) after SIGTERM"
    else
        fail "process exited 0 after SIGTERM — unexpected"
    fi

    if [[ $ACTUAL_EXIT -eq 143 ]]; then
        pass "exit code is 143 (128+15 for SIGTERM) — correctly propagated"
    elif [[ $ACTUAL_EXIT -ne 0 ]]; then
        info "exit code was $ACTUAL_EXIT (not 143 — acceptable if non-zero)"
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
