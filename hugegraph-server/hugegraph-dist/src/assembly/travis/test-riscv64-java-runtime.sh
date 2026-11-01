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

TRAVIS_DIR=$(cd "$(dirname "$0")" && pwd)
SMOKE_SCRIPT="$TRAVIS_DIR/run-rocksdb-jni-smoke-test.sh"
NATIVE_SMOKE_SCRIPT="$TRAVIS_DIR/run-native-runtime-smoke-test.sh"
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/hugegraph-riscv64-java-test.XXXXXX")
MOCK_BIN="$WORK_DIR/bin"
MOCK_JAVA_HOME="$WORK_DIR/java-home"
SERVER_DIR="$WORK_DIR/server"

cleanup() {
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

mkdir -p "$MOCK_BIN" "$MOCK_JAVA_HOME/bin" "$SERVER_DIR/bin" "$SERVER_DIR/lib"

cat > "$MOCK_BIN/uname" <<'EOF'
#!/bin/bash
echo "${MOCK_ARCH:-riscv64}"
EOF

cat > "$MOCK_JAVA_HOME/bin/java" <<'EOF'
#!/bin/bash
set -euo pipefail

JAVA_VERSION=${MOCK_JAVA_VERSION:-17.0.20}
JAVA_VENDOR=${MOCK_JAVA_VENDOR:-Eclipse Adoptium}

case "${1:-}" in
    -version)
        echo "openjdk version \"$JAVA_VERSION\"" >&2
        ;;
    -XshowSettings:properties)
        echo "    java.vm.name = OpenJDK 64-Bit Server VM" >&2
        echo "    java.vm.vendor = $JAVA_VENDOR" >&2
        echo "    java.vm.version = $JAVA_VERSION+8" >&2
        echo "    java.vm.info = mixed mode, sharing" >&2
        echo "openjdk version \"$JAVA_VERSION\"" >&2
        ;;
    -cp)
        if [[ "${MOCK_ARCH:-riscv64}" == arm64 && -n "${LD_PRELOAD:-}" ]]; then
            echo "ARM JNI smoke inherited RISC-V native preload" >&2
            exit 1
        fi
        echo "rocksdb-jni-smoke-ok"
        ;;
    *)
        echo "Unexpected Java arguments: $*" >&2
        exit 1
        ;;
esac
EOF

cat > "$SERVER_DIR/bin/util.sh" <<'EOF'
#!/bin/bash
configure_riscv64_libatomic() {
    if [[ "$(uname -m)" == riscv64 ]]; then
        LD_PRELOAD=libatomic.so.1
    fi
}
EOF

cat > "$SERVER_DIR/bin/init-store.sh" <<'EOF'
#!/bin/bash
exit 42
EOF

chmod +x "$MOCK_BIN/uname" "$MOCK_JAVA_HOME/bin/java" \
         "$SERVER_DIR/bin/init-store.sh"

run_smoke() {
    env -u LD_PRELOAD \
        PATH="$MOCK_BIN:$PATH" \
        JAVA_HOME="$MOCK_JAVA_HOME" \
        "$@" "$SMOKE_SCRIPT" "$SERVER_DIR"
}

run_native_smoke() {
    env -u LD_PRELOAD \
        PATH="$MOCK_BIN:$PATH" \
        JAVA_HOME="$MOCK_JAVA_HOME" \
        "$@" "$NATIVE_SMOKE_SCRIPT" "$SERVER_DIR"
}

if ! DEFAULT_OUTPUT=$(run_smoke 2>&1); then
    echo "$DEFAULT_OUTPUT" >&2
    echo "RISC-V smoke rejected the Java 17 baseline" >&2
    exit 1
fi
grep -q '^rocksdb-jni-smoke-ok$' <<< "$DEFAULT_OUTPUT"

set +e
NATIVE_OUTPUT=$(run_native_smoke 2>&1)
NATIVE_STATUS=$?
set -e
if [[ $NATIVE_STATUS -ne 42 ]]; then
    echo "$NATIVE_OUTPUT" >&2
    echo "Native smoke did not reach the controlled post-JNI boundary" >&2
    exit 1
fi
grep -q '^rocksdb-jni-smoke-ok$' <<< "$NATIVE_OUTPUT"

# macOS passes the setup-java version through the native wrapper unchanged.
for EXPECTED_VERSION in 17 17.0.20 21.0.1; do
    set +e
    VERSION_OUTPUT=$(run_native_smoke MOCK_ARCH=arm64 EXPECTED_ARCH=arm64 \
                     "EXPECTED_JAVA_MAJOR=$EXPECTED_VERSION" \
                     "MOCK_JAVA_VERSION=$EXPECTED_VERSION" 2>&1)
    VERSION_STATUS=$?
    set -e
    if [[ $VERSION_STATUS -ne 42 ]]; then
        echo "$VERSION_OUTPUT" >&2
        echo "ARM native smoke rejected expected version $EXPECTED_VERSION" >&2
        exit 1
    fi
    grep -q '^rocksdb-jni-smoke-ok$' <<< "$VERSION_OUTPUT"
done
if MAJOR_OUTPUT=$(run_smoke MOCK_ARCH=arm64 EXPECTED_ARCH=arm64 \
                  EXPECTED_JAVA_MAJOR=21.0.1 MOCK_JAVA_VERSION=17.0.20 2>&1); then
    echo "$MAJOR_OUTPUT" >&2
    echo "ARM smoke accepted a mismatched Java major" >&2
    exit 1
fi
grep -Fq 'Expected Java 21, got 17.0.20' <<< "$MAJOR_OUTPUT"

for JAVA_MAJOR_MISMATCH in 11.0.31 21.0.8; do
    if MAJOR_OUTPUT=$(run_smoke \
                      "MOCK_JAVA_VERSION=$JAVA_MAJOR_MISMATCH" 2>&1); then
        echo "$MAJOR_OUTPUT" >&2
        echo "RISC-V smoke accepted Java $JAVA_MAJOR_MISMATCH" >&2
        exit 1
    fi
    grep -Fq "Expected Java 17, got $JAVA_MAJOR_MISMATCH" <<< "$MAJOR_OUTPUT"
done

EXPECTED_ARGS=(
    EXPECTED_JAVA_MAJOR=17
    EXPECTED_RISCV64_JAVA_VERSION=17.0.20
    "EXPECTED_RISCV64_JAVA_VENDOR=Eclipse Adoptium"
)
if ! EXPECTED_OUTPUT=$(run_smoke "${EXPECTED_ARGS[@]}" 2>&1); then
    echo "$EXPECTED_OUTPUT" >&2
    echo "RISC-V smoke rejected the configured Temurin 17 runtime" >&2
    exit 1
fi
grep -q '^rocksdb-jni-smoke-ok$' <<< "$EXPECTED_OUTPUT"

if VERSION_OUTPUT=$(run_smoke "${EXPECTED_ARGS[@]}" \
                    MOCK_JAVA_VERSION=17.0.21 2>&1); then
    echo "$VERSION_OUTPUT" >&2
    echo "RISC-V smoke accepted an unexpected Java version" >&2
    exit 1
fi
grep -Fq 'Expected RISC-V Java 17.0.20, got 17.0.21' <<< "$VERSION_OUTPUT"

if VENDOR_OUTPUT=$(run_smoke "${EXPECTED_ARGS[@]}" \
                   "MOCK_JAVA_VENDOR=Unknown Vendor" 2>&1); then
    echo "$VENDOR_OUTPUT" >&2
    echo "RISC-V smoke accepted an unexpected Java vendor" >&2
    exit 1
fi
grep -Fq \
    'Expected RISC-V Java vendor Eclipse Adoptium, got Unknown Vendor' \
    <<< "$VENDOR_OUTPUT"

# Exercise the native wrapper through restart using controlled service commands.
# An isolated PATH makes the gtimeout fallback independent of the host platform.
RUNTIME_BIN="$WORK_DIR/runtime-bin"
RUNTIME_TRAVIS="$WORK_DIR/travis"
RUNTIME_LOG="$WORK_DIR/runtime.log"
START_LOG="$WORK_DIR/start.log"
STOP_LOG="$WORK_DIR/stop.log"
PHASE_LOG="$WORK_DIR/phase.log"
RUN_ID_FILE="$WORK_DIR/run-id"
mkdir -p "$RUNTIME_BIN" "$RUNTIME_TRAVIS" "$WORK_DIR/tmp" "$SERVER_DIR/logs"
printf '%s\n' 'mock service log' > "$SERVER_DIR/logs/server.log"
for command in dirname date mktemp rm tee grep awk cut getconf find; do
    ln -s "$(command -v "$command")" "$RUNTIME_BIN/$command"
done
cp "$MOCK_BIN/uname" "$RUNTIME_BIN/uname"
cp "$NATIVE_SMOKE_SCRIPT" "$SMOKE_SCRIPT" "$TRAVIS_DIR/check-java-runtime-logs.sh" "$RUNTIME_TRAVIS/"

cat > "$RUNTIME_BIN/timeout" <<'EOF'
#!/bin/bash
set -euo pipefail
printf '%s' "${0##*/}" >> "$RUNTIME_LOG"
printf ' <%s>' "$@" >> "$RUNTIME_LOG"
printf '\n' >> "$RUNTIME_LOG"
if [[ "${MOCK_START_TIMEOUT:-false}" == true && "$4" == */start-hugegraph.sh ]]; then
    exit 124
fi
shift 3
exec "$@"
EOF

cat > "$SERVER_DIR/bin/init-store.sh" <<'EOF'
#!/bin/bash
exit 0
EOF

cat > "$SERVER_DIR/bin/start-hugegraph.sh" <<'EOF'
#!/bin/bash
set -euo pipefail
printf 'arch=%s' "$EXPECTED_ARCH" >> "$START_LOG"
printf ' <%s>' "$@" >> "$START_LOG"
printf '\n' >> "$START_LOG"
printf '%s\n' "${MOCK_START_OUTPUT:-}"
EOF

cat > "$SERVER_DIR/bin/stop-hugegraph.sh" <<'EOF'
#!/bin/bash
printf '%s\n' "$*" >> "$STOP_LOG"
exit "${MOCK_STOP_STATUS:-0}"
EOF

cat > "$SERVER_DIR/bin/dump-store.sh" <<'EOF'
#!/bin/bash
set -euo pipefail
RUN_ID=$(cat "$RUN_ID_FILE")
printf '%s\n' 'Dump table VERTEX (offset 0 limit 20):' \
       "riscv-smoke-v1-$RUN_ID" "riscv-smoke-v2-$RUN_ID"
EOF

cat > "$RUNTIME_TRAVIS/run-server-e2e-smoke-test.sh" <<'EOF'
#!/bin/bash
set -euo pipefail
printf '%s\n' "$2" >> "$PHASE_LOG"
printf '%s\n' "$3" > "$RUN_ID_FILE"
printf 'server-e2e-smoke-%s-ok\n' "$2"
EOF

ln -s "$(command -v cat)" "$RUNTIME_BIN/cat"
chmod +x "$RUNTIME_BIN/timeout" "$SERVER_DIR/bin/"*.sh \
         "$RUNTIME_TRAVIS/"*.sh

run_restart_smoke() {
    rm -f "$RUNTIME_LOG" "$START_LOG" "$STOP_LOG" "$PHASE_LOG" "$RUN_ID_FILE"
    env -u LD_PRELOAD -u SERVER_JAVA_OPTIONS \
        PATH="$RUNTIME_BIN" JAVA_HOME="$MOCK_JAVA_HOME" TMPDIR="$WORK_DIR/tmp" \
        RUNTIME_LOG="$RUNTIME_LOG" START_LOG="$START_LOG" STOP_LOG="$STOP_LOG" \
        PHASE_LOG="$PHASE_LOG" RUN_ID_FILE="$RUN_ID_FILE" \
        "$@" "$RUNTIME_TRAVIS/run-native-runtime-smoke-test.sh" "$SERVER_DIR"
}

if ! RESTART_OUTPUT=$(run_restart_smoke SERVER_STARTUP_TIMEOUT=7 STORE_DUMP_TIMEOUT=9 2>&1); then
    echo "$RESTART_OUTPUT" >&2
    echo "RISC-V native smoke failed the controlled restart" >&2
    exit 1
fi
grep -q '^native-runtime-smoke-ok$' <<< "$RESTART_OUTPUT"
[[ $(wc -l < "$START_LOG") -eq 2 ]]
[[ $(wc -l < "$STOP_LOG") -eq 2 ]]
grep -q '^arch=riscv64 <-t> <7>$' "$START_LOG"
grep -Fq 'timeout <--foreground> <--kill-after=15s> <37s>' "$RUNTIME_LOG"
grep -Fq 'timeout <--foreground> <--kill-after=15s> <9s>' "$RUNTIME_LOG"
[[ $(cat "$PHASE_LOG") == $'create\nverify' ]]

mv "$RUNTIME_BIN/timeout" "$RUNTIME_BIN/gtimeout"
if ! ARM_OUTPUT=$(run_restart_smoke MOCK_ARCH=arm64 EXPECTED_ARCH=arm64 \
                  'SERVER_JAVA_OPTIONS=-Xms512m -Xmx2g' 2>&1); then
    echo "$ARM_OUTPUT" >&2
    echo "ARM native smoke failed the controlled restart" >&2
    exit 1
fi
grep -q '^native-runtime-smoke-ok$' <<< "$ARM_OUTPUT"
[[ $(grep -c '^arch=arm64 <-t> <300> <-j> <-Xms512m -Xmx2g>$' "$START_LOG") -eq 2 ]]
[[ $(grep -c '^gtimeout ' "$RUNTIME_LOG") -eq 3 ]]
[[ $(cat "$PHASE_LOG") == $'create\nverify' ]]

if COMPATIBILITY_OUTPUT=$(run_restart_smoke MOCK_ARCH=arm64 EXPECTED_ARCH=arm64 \
                         'MOCK_START_OUTPUT=java.lang.reflect.InaccessibleObjectException: denied' 2>&1); then
    echo "$COMPATIBILITY_OUTPUT" >&2
    echo "Native smoke accepted a caught Java compatibility error" >&2
    exit 1
fi
grep -Fq 'Java runtime compatibility error' <<< "$COMPATIBILITY_OUTPUT"

set +e
TIMEOUT_OUTPUT=$(run_restart_smoke MOCK_ARCH=arm64 EXPECTED_ARCH=arm64 \
                 MOCK_START_TIMEOUT=true 2>&1)
TIMEOUT_STATUS=$?
set -e
if [[ $TIMEOUT_STATUS -ne 124 || -e "$PHASE_LOG" || $(wc -l < "$STOP_LOG") -ne 1 ]]; then
    echo "$TIMEOUT_OUTPUT" >&2
    echo "Native startup timeout did not preserve failure and clean up" >&2
    exit 1
fi

rm "$RUNTIME_BIN/gtimeout"
set +e
MISSING_TIMEOUT_OUTPUT=$(run_restart_smoke 2>&1)
MISSING_TIMEOUT_STATUS=$?
set -e
[[ $MISSING_TIMEOUT_STATUS -eq 1 ]]
grep -Fq 'Required command is unavailable: timeout or gtimeout' <<< "$MISSING_TIMEOUT_OUTPUT"
[[ $(wc -l < "$STOP_LOG") -eq 1 ]]
for TEMP_DIR in "$WORK_DIR/tmp"/hugegraph-native-runtime-smoke.*; do
    [[ ! -e "$TEMP_DIR" ]]
done

echo "PASS: RISC-V Java runtime contract"
echo "PASS: native restart, ARM heap, timeout and cleanup contracts"
