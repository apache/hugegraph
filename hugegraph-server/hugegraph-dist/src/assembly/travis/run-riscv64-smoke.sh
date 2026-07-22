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
# run-riscv64-smoke.sh — Disposable RISC-V (linux/riscv64) end-to-end smoke test.
#
# Builds the HugeGraph Server distribution on the contributor's native host
# (x86-64 / arm64), then runs it inside an emulated linux/riscv64 container to
# verify the RocksDB backend + Gremlin stack actually work on RISC-V via QEMU.
#
# This is a CORRECTNESS gate, not a performance benchmark. QEMU user-mode
# emulation is several times slower than native; expect the full run (build +
# init + REST + Gremlin + restart) to take many minutes the first time.
#
# It requires Docker with QEMU/binfmt_misc multi-platform support (Docker
# Desktop ships this; on Linux install it with:
#   docker run --privileged --rm tonistiigi/binfmt --install all
# No host RISC-V packages are installed by this script.
#
# It removes ONLY what it creates: a uniquely-named container, volume, and
# locally-built image. The QEMU emulator registration is left untouched
# (remove it with: docker run --privileged --rm tonistiigi/binfmt --uninstall all).
#
# Usage:
#   ./run-riscv64-smoke.sh [path-to-server-dist.tar.gz]
#   ./run-riscv64-smoke.sh            # builds the dist under hugegraph-server/
#
set -euo pipefail

ARCH=riscv64
PLATFORM="linux/${ARCH}"
# Ubuntu 24.04 publishes openjdk-11-jre-headless for linux/riscv64 (glibc).
BASE_IMAGE="ubuntu:24.04"

REPO_ROOT=$(cd "$(dirname "$0")/../../../../.." && pwd)

# Use disk-backed temp: the extracted server (~1GB) blows out a small RAM tmpfs
# /tmp. Default to a work dir on the same (large) filesystem as the repo.
: "${TMPDIR:=$REPO_ROOT/.riscv64-smoke-tmp}"
mkdir -p "$TMPDIR"
export TMPDIR

# Unique, traceable names so cleanup removes only our own resources.
STAMP=$(date +%Y%m%d%H%M%S)
TAG="hugegraph-riscv64-smoke:${STAMP}"
# KEEP: don't auto-cleanup so a failed/finished run stays pokeable.
VOL="hugegraph-riscv64-data-${STAMP}"
CONTAINER="hugegraph-riscv64-smoke-${STAMP}"

DIST_ARG="${1:-}"
DIST_TAR=""
SERVER_DIR=""
BUILD_CTX=""
ROCKS_BUILD=""

GREEN=$'\033[0;32m'; RED=$'\033[0;31m'; NC=$'\033[0m'
pass() { echo "${GREEN}PASS${NC} $*"; }
fail() { echo "${RED}FAIL${NC} $*" >&2; }

# Check if the container is still alive (returns 0 if running).
container_alive() {
    $DOCKER ps --format '{{.Names}}' 2>/dev/null | grep -qx "$CONTAINER"
}

# Run docker exec with container-liveness guard + exit-code logging.
# Usage: safe_exec $DOCKER exec <container> <cmd> [args...]
# Handles multi-word DOCKER values (e.g. "sudo docker") via eval.
# Returns the command's output on stdout; logs failures to stderr.
safe_exec() {
    local rc
    if ! container_alive; then
        fail "container '$CONTAINER' is no longer running (before: $*)"
        dump_health_diagnostics
        return 1
    fi
    set +e
    eval "$@"
    rc=$?
    set -e
    if [[ $rc -ne 0 ]]; then
        fail "command failed (exit $rc): $*"
        dump_health_diagnostics
        return 1
    fi
}

# DOCKER: allow callers to override (e.g. "sudo docker"). Defaults to plain docker.
DOCKER=${DOCKER:-docker}

# Dump container diagnostics to stderr. Defined early so the EXIT trap can
# call it even if the script fails before the main body.
dump_health_diagnostics() {
    echo "----- health-check diagnostics ($CONTAINER) -----" >&2
    $DOCKER inspect --format \
        'state={{.State.Status}} exit={{.State.ExitCode}} error={{.State.Error}}' \
        "$CONTAINER" >&2 || true

    if $DOCKER ps --format '{{.Names}}' 2>/dev/null | grep -qx "$CONTAINER"; then
        echo "----- listening processes -----" >&2
        $DOCKER exec "$CONTAINER" sh -c \
            'lsof -nP -iTCP:8080 -sTCP:LISTEN || true' >&2 || true
        for endpoint in versions graphs; do
            echo "----- GET /$endpoint -----" >&2
            $DOCKER exec "$CONTAINER" curl -i -sS --connect-timeout 5 --max-time 15 \
                "http://127.0.0.1:8080/$endpoint" >&2 || true
        done
    fi

    echo "----- recent container logs -----" >&2
    $DOCKER logs --tail 100 "$CONTAINER" >&2 || true
    echo "----- end health-check diagnostics -----" >&2
}

_EXIT_CODE=0
_exit_handler() {
    local rc=$?
    # If the script failed, log the exit code so the root cause isn't lost.
    if [[ $rc -ne 0 ]]; then
        echo "" >&2
        echo "==> Script failed with exit code $rc" >&2
        if container_alive; then
            dump_health_diagnostics
        else
            echo "==> container '$CONTAINER' is not running (may have been OOM-killed or crashed)" >&2
            # Best-effort: check docker inspect for OOMKilled + dump any surviving logs
            $DOCKER inspect --format \
                'OOMKilled={{.State.OOMKilled}} ExitCode={{.State.ExitCode}} Status={{.State.Status}}' \
                "$CONTAINER" >&2 2>/dev/null || true
            $DOCKER logs --tail 80 "$CONTAINER" >&2 2>/dev/null || true
        fi
    fi
    _cleanup
}

_cleanup() {
    # KEEP=1: leave everything standing for live poking. Still dump logs so a
    # failure's root cause is visible; skip all teardown.
    if [[ -n "${KEEP:-}" ]]; then
        echo "==> KEEP=1 set — skipping cleanup. Resources left standing:" >&2
        echo "    container: $CONTAINER" >&2
        echo "    volume:    $VOL" >&2
        echo "    image:     $TAG" >&2
        return 0
    fi
    # Dump container logs first so a failure's root cause isn't destroyed by cleanup.
    if $DOCKER ps -a --format '{{.Names}}' 2>/dev/null | grep -qx "$CONTAINER"; then
        echo "----- container logs ($CONTAINER) -----" >&2
        $DOCKER logs --tail 60 "$CONTAINER" >&2 || true
        echo "----- end logs -----" >&2
        # The startup wrapper hides the real error in the server log file; dump it
        # from the stopped container (docker cp works on exited containers).
        echo "----- hugegraph-server.log -----" >&2
        $DOCKER cp "$CONTAINER:/server/logs/hugegraph-server.log" - 2>/dev/null \
            | tar -xO 2>/dev/null | tail -n 80 >&2 || true
        echo "----- end hugegraph-server.log -----" >&2
    fi
    $DOCKER rm -f "$CONTAINER" >/dev/null 2>&1 || true
    # Only remove the volume we created.
    $DOCKER volume rm "$VOL" >/dev/null 2>&1 || true
    # Only remove the image we tagged; never prune the host's image store.
    $DOCKER rmi "$TAG" >/dev/null 2>&1 || true
    # Remove disk-backed temp work dirs (build context holds the ~1GB extract).
    [[ -n "$BUILD_CTX"   && -d "$BUILD_CTX"   ]] && rm -rf "$BUILD_CTX"
    [[ -n "$ROCKS_BUILD" && -d "$ROCKS_BUILD" ]] && rm -rf "$ROCKS_BUILD"
}
trap _exit_handler EXIT

need() { command -v "$1" >/dev/null 2>&1 || { fail "missing required command: $1"; exit 1; }; }
need docker
# buildx is required: the legacy builder ignores --platform.
$DOCKER buildx version >/dev/null 2>&1 || { fail "docker buildx is required (pacman -S docker-buildx)"; exit 1; }

# Register QEMU user-mode emulation for riscv64 if the kernel can't already run
# it (otherwise the build/run fails with 'exec format error'). binfmt handlers
# do NOT survive reboots, so a fresh checkout needs this — makes the smoke test
# self-contained on any x86 host. Pinned tag: :latest 403s on Docker Hub.
if ! ls /proc/sys/fs/binfmt_misc/qemu-riscv64 >/dev/null 2>&1; then
    echo "==> Registering QEMU riscv64 emulation (binfmt)"
    $DOCKER run --privileged --rm tonistiigi/binfmt:qemu-v8.1.5 --install riscv64 >/dev/null
fi

# 1. Resolve the Server distribution tarball (build on host if not supplied).
#    Build from the ROOT pom (not hugegraph-server/): the root parent pom
#    `hugegraph` and the root sibling `hugegraph-struct` must be installed
#    into ~/.m2 first, or module resolution fails on the ${revision} parent.
if [[ -n "$DIST_ARG" ]]; then
    DIST_TAR="$DIST_ARG"
else
    echo "==> Building HugeGraph Server distribution on host (native platform)"
    mvn -f "$REPO_ROOT/pom.xml" package -e -B -ntp \
        -Dmaven.test.skip=true -Dmaven.javadoc.skip=true -Drat.skip=true \
        -pl hugegraph-server/hugegraph-dist -am
    DIST_TAR=$(ls -t "$REPO_ROOT"/hugegraph-server/apache-hugegraph-server-*.tar.gz 2>/dev/null | head -n1 || true)
fi
[[ -f "$DIST_TAR" ]] || { fail "server distribution tarball not found"; exit 1; }
echo "==> Using distribution: $DIST_TAR"

# Extract straight into the build context's server/ dir — no second copy
# (the extracted tree is ~1GB; copying it twice doubled the temp footprint).
BUILD_CTX=$(mktemp -d "hugegraph-riscv64-ctx.XXXXXX")
SERVER_DIR="$BUILD_CTX/server"
mkdir -p "$SERVER_DIR"
tar -xzf "$DIST_TAR" -C "$SERVER_DIR" --strip-components=1

# Force the RocksDB backend for the smoke run.
CONF="$SERVER_DIR/conf/graphs/hugegraph.properties"
grep -qE '^[[:space:]]*backend[[:space:]]*=' "$CONF" \
    && sed -i 's/^[[:space:]]*backend[[:space:]]*=.*/backend=rocksdb/' "$CONF" \
    || echo "backend=rocksdb" >> "$CONF"
grep -qE '^[[:space:]]*serializer[[:space:]]*=' "$CONF" \
    && sed -i 's/^[[:space:]]*serializer[[:space:]]*=.*/serializer=binary/' "$CONF" \
    || echo "serializer=binary" >> "$CONF"

# 2. Build a tiny RISC-V runtime image on top of the glibc base.
#    libatomic1 is installed and exposed via LD_LIBRARY_PATH (+ a /usr/lib
#    symlink) so RocksDB's __atomic_compare_exchange_1 symbol resolves
#    automatically — no manual step for the user. If the JNI still lacks the
#    symbol, the RocksDB step below fails loudly instead of being treated as
#    the goal.
# Reuse the repo's own container entrypoint: it runs init-store.sh (create the
# RocksDB store) BEFORE start-hugegraph.sh. Hand-rolling the start command
# skips init and the server aborts with an empty log (the failure we hit).
cp "$REPO_ROOT/hugegraph-server/hugegraph-dist/docker/docker-entrypoint.sh" \
    "$BUILD_CTX/server/docker-entrypoint.sh"
# The entrypoint hardcodes 'start-hugegraph.sh ... -t 120'. Under QEMU emulation
# the analyzer dictionary loads twice (~3.5 min each) plus Gremlin/Groovy warmup,
# so the server needs well over 600s to bind 8080. Raise the startup timeout.
sed -i 's/-t 120/-t 1800/' "$BUILD_CTX/server/docker-entrypoint.sh"
cat > "$BUILD_CTX/Dockerfile" <<EOF
FROM ${BASE_IMAGE}
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get -q update && apt-get -q install -y --no-install-recommends --no-install-suggests openjdk-11-jre-headless libatomic1 curl jq dumb-init lsof procps && apt-get clean && rm -rf /var/lib/apt/lists/*
# Force libatomic to load before the JNI: rocksdbjni:8.10.2's RISC-V
# .so has an undefined __atomic_compare_exchange_1 that only libatomic
# satisfies. LD_LIBRARY_PATH alone is NOT enough (verified: symbol stays
# unresolved); LD_PRELOAD guarantees libatomic loads first into every
# Java process (the RocksDB smoke test AND the server).
# Path is the constant riscv64 multiarch dir confirmed inside the image;
# dpkg-architecture is unreliable under QEMU emulation, so hardcode it.
RUN ln -sf /usr/lib/riscv64-linux-gnu/libatomic.so.1 /usr/lib/libatomic.so.1
WORKDIR /server
COPY server/ /server/
RUN chmod 755 /server/docker-entrypoint.sh && sed -i "s#^restserver.url.*#restserver.url=http://127.0.0.1:8080#" /server/conf/rest-server.properties
# The Ubuntu openjdk-11 RISC-V build lacks -XX:G1RSetUpdatingPauseTimePercent,
# which start-hugegraph.sh always appends for the default G1GC. It is a tuning
# hint, not required for correctness, so strip it to let the JVM launch.
RUN sed -i "s/-XX:G1RSetUpdatingPauseTimePercent=5//" /server/bin/hugegraph-server.sh
# JAVA_OPTS mirrors the official image (auth proxy needs --add-exports on JDK 11).
# STDOUT_MODE=true routes the server log to stdout so 'docker logs' shows failures.
ENV JAVA_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+UseContainerSupport -XX:MaxRAMPercentage=50 --add-exports=java.base/jdk.internal.reflect=ALL-UNNAMED" LD_PRELOAD=/usr/lib/libatomic.so.1 HUGEGRAPH_HOME=server STDOUT_MODE=true
EXPOSE 8080
ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["/server/docker-entrypoint.sh"]
EOF

echo "==> Building RISC-V runtime image $TAG"
$DOCKER buildx build --platform "$PLATFORM" -t "$TAG" "$BUILD_CTX" --load

# 3. Start the server in the background; trap handles cleanup.
echo "==> Starting server container on ${PLATFORM}"
$DOCKER run -d --name "$CONTAINER" --platform "$PLATFORM" \
    -p 8080 --memory=4g -v "$VOL:/server" "$TAG"

# Wait until /graphs answers 200 (server auto-inits + starts via entrypoint).
# This is the same readiness endpoint used by start-hugegraph.sh. /versions is
# asserted separately after the server has completed its startup sequence.
wait_health() {
    # This probe starts while the entrypoint is still running init-store.sh.
    # Under QEMU the analyzer dictionary loads once during initialization and
    # again during server startup, so the full end-to-end wait needs to exceed
    # the entrypoint's 1800s startup timeout as well as initialization time.
    local elapsed=0 limit=3600
    while (( elapsed < limit )); do
        # Bail fast if the container died instead of polling a corpse for 10 min.
        if ! $DOCKER ps --format '{{.Names}}' 2>/dev/null | grep -qx "$CONTAINER"; then
            echo "==> container '$CONTAINER' is no longer running" >&2
            dump_health_diagnostics
            return 1
        fi
        local code
        code=$($DOCKER exec "$CONTAINER" \
            curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8080/graphs 2>/dev/null || true)
        code=${code:-000}
        [[ "$code" == "200" ]] && return 0
        sleep 5; elapsed=$((elapsed+5))
    done
    echo "==> health check timed out after ${limit}s (last /graphs HTTP status: $code)" >&2
    dump_health_diagnostics
    return 1
}

# 4. Architecture assertion.
echo "==> Verifying architecture"
container_alive || { fail "container died before architecture check"; exit 1; }
ARCH_OK=$($DOCKER exec "$CONTAINER" uname -m)
[[ "$ARCH_OK" == "riscv64" ]] || { fail "architecture=$ARCH_OK (expected riscv64)"; exit 1; }
pass "architecture: riscv64"

# 5. RocksDB open/put/get/close via a real Java program against the JNI.
echo "==> RocksDB open/put/get/close test"
# If the JNI is not linked to libatomic this step's java process dies with
# "undefined symbol: __atomic_compare_exchange_1" — that is the signal to fix
# the linkage, NOT the expected final result.
ROCKS_TEST='import org.rocksdb.*;
public class RocksSmoke {
    public static void main(String[] a) throws Exception {
        RocksDB.loadLibrary();
        try (Options o = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(o, "/tmp/rocksdb-smoke")) {
            byte[] k = "key".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] v = "value".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            db.put(k, v);
            byte[] got = db.get(k);
            if (!java.util.Arrays.equals(v, got))
                throw new AssertionError("rocksdb get mismatch: " + new String(got));
            db.close();
        }
        System.out.println("ROCKS_OK");
    }
}'
container_alive || { fail "container died before RocksDB test"; exit 1; }
ROCKSJAR=$($DOCKER exec "$CONTAINER" sh -c 'ls /server/lib/rocksdbjni-*.jar 2>/dev/null | head -n1')
[[ -n "$ROCKSJAR" ]] || { fail "rocksdbjni jar not found in dist"; exit 1; }
# Compile the smoke class on the HOST (which has a JDK); the runtime image
# ships only a JRE (no javac). The .class is arch-independent bytecode,
# so an x86-compiled class runs fine under the riscv64 JRE.
ROCKS_BUILD=$(mktemp -d "rocks-smoke.XXXXXX")
echo "$ROCKS_TEST" > "$ROCKS_BUILD/RocksSmoke.java"
javac -cp "$SERVER_DIR"/lib/rocksdbjni-*.jar "$ROCKS_BUILD/RocksSmoke.java" -d "$ROCKS_BUILD" \
    || { fail "failed to compile RocksSmoke on host (need a JDK)"; exit 1; }
$DOCKER cp "$ROCKS_BUILD/RocksSmoke.class" "$CONTAINER:/tmp/RocksSmoke.class"
# ponytail: set +e around the pipeline so pipefail doesn't kill the script
# before the grep below can report the actual failure.
set +e
safe_exec "$DOCKER exec $CONTAINER java -cp /tmp:$ROCKSJAR RocksSmoke" 2>&1 \
    | tee "$ROCKS_BUILD/rocks-out.log"
JAVA_RC=${PIPESTATUS[0]}
set -e
grep -q "ROCKS_OK" "$ROCKS_BUILD/rocks-out.log" || {
    fail "RocksDB open/put/get/close failed (java exit=$JAVA_RC, check JNI/libatomic linkage)"
    exit 1
}
pass "RocksDB: open / put / get / close"

# 6. init already ran via entrypoint; confirm health, then REST CRUD + Gremlin.
wait_health || { fail "server did not become healthy (GET /graphs)"; exit 1; }
pass "HugeGraph: init / health (GET /graphs -> 200)"

# ponytail: --max-time 300 because QEMU makes every REST call slow; upgrade to
# per-endpoint tuning if individual calls need more headroom.
container_alive || { fail "container died before REST phase"; exit 1; }
# Wait for /versions to return valid JSON — the server's /graphs health check
# may pass before the REST API layer is fully initialized.
REST_READY=""
for _retry in 1 2 3 4 5 6 7 8 9 10; do
    REST_READY=$($DOCKER exec "$CONTAINER" sh -c 'curl -s --max-time 30 http://localhost:8080/versions && echo' || true)
    if [[ "$REST_READY" == *'"core"'* ]]; then break; fi
    echo "  waiting for REST API (attempt $_retry)..." >&2
    sleep 10
done
[[ "$REST_READY" == *'"core"'* ]] || { fail "REST API not ready after retries: $REST_READY"; exit 1; }

# Create schema, a vertex, and an edge; read them back.
# Schema order matters (commits are synchronous, but each element must exist
# before the one that references it): property key "name" -> vertex label
# "person" (declares name) -> edge label "knows" -> vertices -> edge.
# Each POST asserts HTTP 2xx so a bad request fails loud, not silent.
echo "==> REST schema + CRUD"
rest_post() { # $1=path $2=json
    local body code response rc
    set +e
    response=$($DOCKER exec "$CONTAINER" curl -sS -w $'\n%{http_code}' -X POST \
        "http://localhost:8080/graphs/hugegraph/$1" \
        -H 'Content-Type: application/json' -d "$2")
    rc=$?
    set -e
    if [[ $rc -ne 0 ]]; then
        fail "REST POST $1 failed (curl/docker exec exit $rc): $response"
        exit 1
    fi
    code=${response##*$'\n'}
    body=${response%$'\n'*}
    # Tolerate "already exists" (400 + ExistedException) for idempotent schema ops.
    if [[ "$code" == 400 && "$body" == *ExistedException* ]]; then
        printf '%s\n' "$body"
        return 0
    fi
    [[ "$code" == 2* ]] || { fail "REST POST $1 failed (HTTP $code): $body"; exit 1; }
    printf '%s\n' "$body"
}

# GET a REST endpoint, capture response to a temp file.
# Usage: rest_get <path> [curl-extra-args...]
# Sets REST_BODY (response body) and REST_CODE (HTTP status).
rest_get() {
    local rsp
    rsp=$($DOCKER exec "$CONTAINER" curl -sS --compressed -w $'\n%{http_code}' \
        -H 'Accept: application/json' "$@")
    REST_CODE=${rsp##*$'\n'}
    REST_BODY=${rsp%$'\n'*}
}

# Assert a jq expression against the last REST_BODY.
# Usage: assert_json '.field == "value"'
assert_json() {
    local expr="$1"; shift
    if ! printf '%s' "$REST_BODY" | $DOCKER exec -i "$CONTAINER" jq --exit-status "$expr" "$@" >/dev/null 2>&1; then
        fail "assertion failed: $expr"
        echo "  response body: ${REST_BODY:0:300}" >&2
        exit 1
    fi
}
rest_post "schema/propertykeys" \
    '{"name":"name","data_type":"TEXT","cardinality":"SINGLE"}' >/dev/null
rest_post "schema/vertexlabels" \
    '{"name":"person","id_strategy":"DEFAULT","properties":["name"],"primary_keys":["name"]}' >/dev/null
rest_post "schema/edgelabels" \
    '{"name":"knows","source_label":"person","target_label":"person","properties":[],"frequency":"SINGLE"}' >/dev/null
ALICE=$(rest_post "graph/vertices" '{"label":"person","properties":{"name":"alice"}}')
BOB=$(rest_post "graph/vertices" '{"label":"person","properties":{"name":"bob"}}')
ALICE_ID=$(printf '%s\n' "$ALICE" | sed -n 's/.*"id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')
BOB_ID=$(printf '%s\n' "$BOB" | sed -n 's/.*"id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')
[[ -n "$ALICE_ID" && -n "$BOB_ID" ]] || { fail "REST vertex response did not contain IDs"; exit 1; }
rest_post "graph/edges" \
    "{\"label\":\"knows\",\"outV\":\"$ALICE_ID\",\"outVLabel\":\"person\",\"inV\":\"$BOB_ID\",\"inVLabel\":\"person\",\"properties\":{}}" >/dev/null

container_alive || { fail "container died during REST CRUD"; exit 1; }
# Verify data persisted using jq (robust against binary/JSON format changes).
rest_get "http://localhost:8080/graphs/hugegraph/graph/vertices"
assert_json '.vertices | length > 0'
rest_get "http://localhost:8080/graphs/hugegraph/graph/edges"
assert_json '.edges | length > 0'
pass "HugeGraph: REST CRUD (vertex + edge created)"

# 7. Gremlin query — proves the full TinkerPop/Gremlin stack runs on JDK 11.
echo "==> Gremlin query"
container_alive || { fail "container died before Gremlin query"; exit 1; }
GREM=$($DOCKER exec "$CONTAINER" curl -s --compressed -X POST http://localhost:8182/ \
    -H 'Content-Type: application/json' \
    -d '{"gremlin":"g.V().hasLabel(\"person\").count()","language":"gremlin-groovy","aliases":{"g":"__g_DEFAULT-hugegraph"}}')
# Use jq to check the count — handles whitespace and type variations.
echo "$GREM" | $DOCKER exec -i "$CONTAINER" jq -e '.result.data[0] == 2' >/dev/null 2>&1 \
    || { fail "Gremlin query did not return expected count (got: $GREM)"; exit 1; }
pass "HugeGraph: Gremlin query (g.V().count() == 2)"

# 8. Restart and verify persistence from the volume.
echo "==> Restart + persistence check"
container_alive || { fail "container died before restart"; exit 1; }
$DOCKER restart "$CONTAINER" >/dev/null
wait_health || { fail "server not healthy after restart"; exit 1; }
container_alive || { fail "container died after restart health check"; exit 1; }
rest_get "http://localhost:8080/graphs/hugegraph/graph/vertices"
assert_json '.vertices | length > 0'
pass "HugeGraph: restart / persistence"

# 9. Final summary (matches the issue's expected output format).
echo ""
pass "architecture: riscv64"
pass "RocksDB: open / put / get / close"
pass "HugeGraph: init / health / REST CRUD / Gremlin / restart / persistence"
pass "cleanup: container, volume, and image removed"
