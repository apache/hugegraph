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
echo riscv64
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
    LD_PRELOAD=libatomic.so.1
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

echo "PASS: RISC-V Java runtime contract"
