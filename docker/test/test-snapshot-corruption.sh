#!/usr/bin/env bash
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


# test-snapshot-corruption.sh — deterministic reproducer for the HStore snapshot corruption bug
#
# Requires: Docker Desktop >= 20.10, >= 12 GB allocated to Docker, Docker Compose v2
# Run from the repo root:
#   bash docker/hbase/test/test-snapshot-corruption.sh            # confirm bug is present (buggy image)
#   bash docker/hbase/test/test-snapshot-corruption.sh --fixed    # confirm bug is absent (fixed image)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/../docker-compose-3pd-3store-3server.yml"
HUGEGRAPH_VERSION="${HUGEGRAPH_VERSION:-1.7.0}"
VOLUME_PREFIX="hugegraph-3x3"
STORE_LOG="hugegraph-store.log"
FIXED_MODE=false
[[ "${1:-}" == "--fixed" ]] && FIXED_MODE=true

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log()  { echo -e "${GREEN}[repro]${NC} $*"; }
warn() { echo -e "${YELLOW}[repro]${NC} $*"; }
fail() { echo -e "${RED}[repro] FAIL${NC} $*" >&2; exit 1; }

JAR_SOURCE="$REPO_ROOT/hugegraph-store/hg-store-node/target/hg-store-node-${HUGEGRAPH_VERSION}.jar"

if $FIXED_MODE; then
    STORE_IMAGE="${STORE_IMAGE:-hugegraph/store:patched}"
    if [[ "$STORE_IMAGE" == "hugegraph/store:patched" ]] && \
       ! docker image inspect "hugegraph/store:patched" >/dev/null 2>&1; then
        log "Patched image not found — building from source..."
        if [[ ! -f "$JAR_SOURCE" ]]; then
            log "  Compiling hugegraph-store (this takes a minute)..."
            mvn package -pl hugegraph-store/hg-store-node -am -DskipTests -q \
                -f "$REPO_ROOT/pom.xml"
        fi
        BUILD_CTX="$(mktemp -d)"
        cp "$JAR_SOURCE" "$BUILD_CTX/hg-store-node-${HUGEGRAPH_VERSION}.jar"
        cat > "$BUILD_CTX/Dockerfile" <<EOF
FROM hugegraph/store:${HUGEGRAPH_VERSION}
COPY hg-store-node-${HUGEGRAPH_VERSION}.jar /hugegraph-store/lib/hg-store-node-${HUGEGRAPH_VERSION}.jar
EOF
        docker build -t "hugegraph/store:patched" "$BUILD_CTX" >/dev/null
        log "  Built hugegraph/store:patched."
    fi
else
    STORE_IMAGE="${STORE_IMAGE:-hugegraph/store:${HUGEGRAPH_VERSION}}"
fi

log "Store image: $STORE_IMAGE  (fixed-mode: $FIXED_MODE)"
log "Compose File: $COMPOSE_FILE"

# If a non-default store image is requested, write a temporary compose override that
# replaces the store image — without modifying the committed compose file.
OVERRIDE_FILE=""
if [ "$STORE_IMAGE" != "hugegraph/store:${HUGEGRAPH_VERSION}" ]; then
    OVERRIDE_FILE="$(mktemp /tmp/snapshot-test-override-XXXXXX.yml)"
    cat > "$OVERRIDE_FILE" <<EOF
services:
  store0:
    image: ${STORE_IMAGE}
  store1:
    image: ${STORE_IMAGE}
  store2:
    image: ${STORE_IMAGE}
EOF
    log "  Using override file $OVERRIDE_FILE for image $STORE_IMAGE"
fi
# Build the compose -f arguments (base file always first; override appended when set)
COMPOSE_ARGS="-f $COMPOSE_FILE"
[ -n "$OVERRIDE_FILE" ] && COMPOSE_ARGS="$COMPOSE_ARGS -f $OVERRIDE_FILE"
# Clean up temp files on exit (BUILD_CTX trap already set in --fixed mode above)
trap '[ -n "$OVERRIDE_FILE" ] && rm -f "$OVERRIDE_FILE"; [ -n "${BUILD_CTX:-}" ] && rm -rf "$BUILD_CTX"' EXIT

wait_http() {
    local url=$1 label=$2 tries=${3:-60}
    log "Waiting for $label..."
    for i in $(seq 1 "$tries"); do
        if curl -fsS "$url" >/dev/null 2>&1; then log "$label up."; return 0; fi
        sleep 3
    done
    fail "$label not healthy after $((tries * 3))s"
}

# ── Step 1: Start cluster ─────────────────────────────────────────────────────
log "Step 1: Tearing down any previous run and starting a clean cluster..."
HUGEGRAPH_VERSION="$HUGEGRAPH_VERSION" \
    docker compose $COMPOSE_ARGS down -v --remove-orphans 2>&1 | tail -3 || true
HUGEGRAPH_VERSION="$HUGEGRAPH_VERSION" \
    docker compose $COMPOSE_ARGS up -d \
        --scale server0=0 --scale server1=0 --scale server2=0 \
        2>&1 | grep -E "Started|Healthy|healthy" | tail -5 || true

wait_http "http://localhost:8620/v1/health" "pd0"    60
wait_http "http://localhost:8520/v1/health" "store0" 60
wait_http "http://localhost:8521/v1/health" "store1" 60
wait_http "http://localhost:8522/v1/health" "store2" 60

# Raft partition dirs are created lazily when the server first registers a graph.
# Start server0 just long enough for init-store to run, then stop it.
# We only need the init_complete flag to be written — we do NOT wait for /versions
# because start-hugegraph.sh has a 120s JVM-ready timeout that can expire on a cold
# distributed cluster, causing the entrypoint to exit and Docker to restart the
# container, resetting the timer indefinitely.
if ! docker exec hg-store0 sh -c 'ls /hugegraph-store/storage/raft/ 2>/dev/null | grep -qE "^[0-9]{5}$"' 2>/dev/null; then
    log "  Fresh cluster: starting server0 briefly to initialise partitions..."
    HUGEGRAPH_VERSION="$HUGEGRAPH_VERSION" \
    HUGEGRAPH_STORE_IMAGE="$STORE_IMAGE" \
        docker compose $COMPOSE_ARGS up -d server0 2>&1 | tail -2 || true

    log "  Waiting for init-store to complete (up to 120s)..."
    for i in $(seq 1 40); do
        if docker exec hg-server0 test -f /hugegraph-server/docker/init_complete 2>/dev/null; then
            log "  init_complete flag found after ~$((i * 3))s."
            break
        fi
        sleep 3
    done
    docker exec hg-server0 test -f /hugegraph-server/docker/init_complete 2>/dev/null \
        || fail "init-store did not complete within 120s"

    # Give Raft groups a moment to create their partition dirs
    sleep 5
    docker compose $COMPOSE_ARGS stop server0 2>/dev/null || true
    log "  server0 stopped — partitions initialised."
fi

# ── Step 2: Ensure committed snapshots exist on store0 ───────────────────────
log "Step 2: Flushing + snapshotting all store nodes..."
for port in 8520 8521 8522; do
    curl -fsS "http://localhost:${port}/test/flush"    >/dev/null && log "  :${port} flush OK"
    curl -fsS "http://localhost:${port}/test/snapshot" >/dev/null && log "  :${port} snapshot triggered"
done
log "Waiting 20s for Raft snapshot commits..."
sleep 20

SNAP_COUNT=$(docker run --rm \
    -v "${VOLUME_PREFIX}_hg-store0-data:/hugegraph-store/storage" \
    busybox sh -c 'find /hugegraph-store/storage/raft -name "data" -type d | wc -l')
log "store0 has $SNAP_COUNT committed snapshot data/ directories."
[ "$SNAP_COUNT" -ge 1 ] || fail "No committed snapshots on store0. Retry."

# ── Step 3: Stop all stores ───────────────────────────────────────────────────
log "Step 3: Stopping all store nodes..."
docker stop hg-store0 hg-store1 hg-store2 >/dev/null
log "All stores stopped."

# ── Step 4: Corrupt one partition snapshot on store0 ─────────────────────────
# Two sub-cases of the bug:
#
#  Sub-case A (race: state==doing at snapshot-save time) — tested in default mode:
#    onSnapshotSave returns early → neither data/ nor should_not_load written.
#    Snapshot dir has only __raft_snapshot_meta.
#    On load: goes straight to loadSnapshot(missingDir) → "not exists" → stuck.
#    Fix 1 (throw instead of return) prevents this snapshot from ever being committed.
#
#  Sub-case B (JVM killed after should_not_load but before data/ completes) — tested in --fixed mode:
#    Snapshot dir has __raft_snapshot_meta + should_not_load, but no data/.
#    On load (buggy): shouldNotLoad() == true → silent return → partition silently has no data.
#    On load (fixed): Fix 2 detects data/ is missing → logs warning → falls through to
#                     loadSnapshot → throws "not exists" → JRaft signals error → leader rescues.
#
log "Step 4: Corrupting one snapshot on store0 (sub-case $( $FIXED_MODE && echo B || echo A ))..."
TARGET=$(docker run --rm \
    -v "${VOLUME_PREFIX}_hg-store0-data:/hugegraph-store/storage" \
    busybox sh -c '
        for meta in $(find /hugegraph-store/storage/raft -name "__raft_snapshot_meta" | sort); do
            snap=$(dirname "$meta")
            if [ -d "$snap/data" ] && [ -f "$snap/should_not_load" ]; then
                echo "$snap"; break
            fi
        done
    ')

[ -n "$TARGET" ] || fail "No suitable snapshot found (need data/ + should_not_load + __raft_snapshot_meta)"

PARTITION_ID=$(echo "$TARGET" | grep -oE '/[0-9]{5}/' | head -1 | tr -d '/')
SNAP_NAME=$(basename "$TARGET")

if $FIXED_MODE; then
    # Sub-case B: remove only data/, keep should_not_load.
    # Buggy image: shouldNotLoad() fires, silently returns — no error logged.
    # Fixed image (Fix 2): detects data/ missing, logs warning, falls through.
    log "  Target: partition $PARTITION_ID / $SNAP_NAME"
    log "  Removing data/ only — keeping should_not_load (sub-case B)"
    docker run --rm \
        -v "${VOLUME_PREFIX}_hg-store0-data:/hugegraph-store/storage" \
        busybox sh -c "rm -rf '${TARGET}/data'
                       echo 'Contents after corruption:'; ls '${TARGET}'"
else
    # Sub-case A: remove both data/ and should_not_load — exactly what the race produces.
    log "  Target: partition $PARTITION_ID / $SNAP_NAME"
    log "  Removing data/ and should_not_load — leaving only __raft_snapshot_meta (sub-case A)"
    docker run --rm \
        -v "${VOLUME_PREFIX}_hg-store0-data:/hugegraph-store/storage" \
        busybox sh -c "rm -rf '${TARGET}/data' '${TARGET}/should_not_load'
                       echo 'Contents after corruption:'; ls '${TARGET}'"
fi

# ── Step 5: Start store0 alone ────────────────────────────────────────────────
log "Step 5: Starting store0 alone (no peers — prevents leader snapshot rescue)..."
docker start hg-store0 >/dev/null
log "Polling store0 logs for snapshot load result (up to 90s)..."
for i in $(seq 1 45); do
    if docker exec hg-store0 grep -qE "not exists|Fail to init|onSnapshotLoad success|warn.*corrupt" \
            /hugegraph-store/logs/$STORE_LOG 2>/dev/null; then
        log "  Snapshot load result detected after ~$((i * 2))s."
        break
    fi
    sleep 2
done
sleep 3

APP_LOGS=$(docker exec hg-store0 cat /hugegraph-store/logs/$STORE_LOG 2>/dev/null || true)

# ── Step 6: Evaluate result ───────────────────────────────────────────────────
if $FIXED_MODE; then
    # Sub-case B: we corrupted should_not_load+data/ (kept should_not_load, removed data/).
    # Buggy behaviour: shouldNotLoad() silently returns — "skip to load snapshot" logged, no error.
    # Fixed behaviour (Fix 2): detects data/ is missing → logs the warn line → falls through
    #   → loadSnapshot throws "not exists" → JRaft signals error (visible in logs).
    # The key assertion: the warn line IS present, proving Fix 2 caught the corrupt snapshot
    # instead of silently accepting it.
    log "Step 6: Verifying Fix 2 — should_not_load + missing data/ must be caught, not silently skipped..."
    WARN_LINE="should_not_load flag present but data dir"
    if grep -q "$WARN_LINE" <<< "$APP_LOGS"; then
        echo ""
        echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo -e "${GREEN}  FIX 2 VERIFIED — corrupt snapshot detected, not silently accepted${NC}"
        echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo ""
        echo "Key log lines:"
        grep -E "$WARN_LINE|not exists|Fail to init" <<< "$APP_LOGS" | head -6 | sed 's/^/  /' || true
    else
        fail "Fix 2 did not fire — warn line not found. The corrupt snapshot was silently accepted."
    fi
else
    log "Step 6: Checking logs for the bug..."
    if grep -q "not exists" <<< "$APP_LOGS"; then
        echo ""
        echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo -e "${RED}  BUG REPRODUCED — partition ${PARTITION_ID} is stuck${NC}"
        echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo ""
        echo "Key log lines:"
        grep -E "not exists|Fail to init|onSnapshotLoad failed|StateMachine on error" \
            <<< "$APP_LOGS" | head -6 | sed 's/^/  /' || true
    else
        fail "Expected error lines not found. Check: docker exec hg-store0 cat /hugegraph-store/logs/$STORE_LOG"
    fi

    # ── Step 7: Health endpoint still 200 ────────────────────────────────────
    log "Step 7: Health endpoint check..."
    HTTP_CODE="000"
    for i in $(seq 1 10); do
        HTTP_CODE=$(curl -sw "%{http_code}" http://localhost:8520/v1/health -o /dev/null 2>/dev/null || echo "000")
        [ "$HTTP_CODE" != "000" ] && break
        sleep 3
    done
    warn "  /v1/health → HTTP $HTTP_CODE  (200 = misleading — broken partition is invisible)"

    # ── Step 8: Restart does not recover ─────────────────────────────────────
    log "Step 8: Confirming plain restart does not recover partition $PARTITION_ID..."
    # Record log line count before restart so we only examine lines written after it.
    LOG_LINES_BEFORE=$(docker exec hg-store0 wc -l /hugegraph-store/logs/$STORE_LOG 2>/dev/null | awk '{print $1}' || echo 0)
    docker stop hg-store0 >/dev/null
    docker start hg-store0 >/dev/null
    for i in $(seq 1 45); do
        NEW_LINES=$(docker exec hg-store0 \
            awk "NR > $LOG_LINES_BEFORE" /hugegraph-store/logs/$STORE_LOG 2>/dev/null || true)
        if grep -qE "not exists|Fail to init" <<< "$NEW_LINES"; then
            log "  Error lines found after ~$((i * 2))s."
            break
        fi
        sleep 2
    done
    POST_RESTART_LOGS=$(docker exec hg-store0 \
        awk "NR > $LOG_LINES_BEFORE" /hugegraph-store/logs/$STORE_LOG 2>/dev/null || true)
    if grep -q "not exists" <<< "$POST_RESTART_LOGS"; then
        warn "  Confirmed: restart loops again. The node is permanently stuck."
    else
        warn "  Error lines not found in post-restart output — JVM may need more time:"
        warn "    docker exec hg-store0 tail -20 /hugegraph-store/logs/$STORE_LOG"
    fi
fi

# ── Step 9: Restore full store cluster ───────────────────────────────────────
log "Step 9: Restoring store cluster (store1 + store2)..."
docker start hg-store1 hg-store2 >/dev/null
log "  Leader will install a fresh snapshot on store0 for partition $PARTITION_ID."

echo ""
echo -e "${GREEN}  Run complete. Clean up with:${NC}"
echo "    docker compose -f docker/docker-compose-3pd-3store-3server.yml down -v"
