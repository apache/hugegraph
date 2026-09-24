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
EXPECTED_ARCH=${EXPECTED_ARCH:-}
EXPECTED_JAVA_MAJOR=${EXPECTED_JAVA_MAJOR:-11}
ACTUAL_ARCH=$(uname -m)

if [[ -n "${JAVA_HOME:-}" ]]; then
    JAVA_CMD="$JAVA_HOME/bin/java"
else
    JAVA_CMD=$(command -v java || true)
fi
if [[ -z "$JAVA_CMD" || ! -x "$JAVA_CMD" ]]; then
    echo "Java executable is unavailable: ${JAVA_CMD:-not found}" >&2
    exit 1
fi

if [[ -n "$EXPECTED_ARCH" && "$ACTUAL_ARCH" != "$EXPECTED_ARCH" ]]; then
    echo "Expected architecture $EXPECTED_ARCH, got $ACTUAL_ARCH" >&2
    exit 1
fi

JAVA_VERSION=$("$JAVA_CMD" -version 2>&1 | \
               awk -F '"' '/version/ {print $2; exit}')
JAVA_MAJOR=${JAVA_VERSION%%.*}
if [[ "$JAVA_MAJOR" == "1" ]]; then
    JAVA_MAJOR=$(echo "$JAVA_VERSION" | cut -d. -f2)
fi
if [[ "$JAVA_MAJOR" != "$EXPECTED_JAVA_MAJOR" ]]; then
    echo "Expected Java $EXPECTED_JAVA_MAJOR, got $JAVA_VERSION" >&2
    exit 1
fi

JAVA_PROPERTIES=$("$JAVA_CMD" -XshowSettings:properties -version 2>&1)
java_property() {
    local key=$1
    awk -F ' = ' -v key="$key" '{
        name = $1
        sub(/^[[:space:]]*/, "", name)
        if (name == key) {
            print $2
            exit
        }
    }' <<< "$JAVA_PROPERTIES"
}

JAVA_VM_NAME=$(java_property java.vm.name)
JAVA_VM_VENDOR=$(java_property java.vm.vendor)
JAVA_VM_VERSION=$(java_property java.vm.version)
JAVA_VM_INFO=$(java_property java.vm.info)

if [[ -z "$JAVA_VM_NAME" || -z "$JAVA_VM_VENDOR" || -z "$JAVA_VM_INFO" ]]; then
    echo "Failed to read Java VM properties" >&2
    exit 1
fi

echo "Architecture: $ACTUAL_ARCH"
echo "Java: $JAVA_VERSION"
echo "Java VM: $JAVA_VM_NAME ($JAVA_VM_VENDOR, $JAVA_VM_VERSION, $JAVA_VM_INFO)"
if command -v getconf >/dev/null 2>&1; then
    echo "C library: $(getconf GNU_LIBC_VERSION 2>/dev/null || echo unknown)"
fi

. "$SERVER_DIR/bin/util.sh"
configure_riscv64_libatomic

if [[ "$ACTUAL_ARCH" == "riscv64" ]]; then
    EXPECTED_RISCV64_JAVA_VERSION=${EXPECTED_RISCV64_JAVA_VERSION:-11.0.31.28}
    if [[ "$JAVA_VERSION" != "$EXPECTED_RISCV64_JAVA_VERSION" ]]; then
        echo "Expected RISC-V Java $EXPECTED_RISCV64_JAVA_VERSION, got $JAVA_VERSION" >&2
        exit 1
    fi
    if [[ "$JAVA_VM_NAME" != "OpenJDK 64-Bit Server VM" ]]; then
        echo "Expected RISC-V Server VM, got $JAVA_VM_NAME" >&2
        exit 1
    fi
    if [[ "$JAVA_VM_VENDOR" != "Alibaba" ]]; then
        echo "Expected RISC-V Java vendor Alibaba, got $JAVA_VM_VENDOR" >&2
        exit 1
    fi
    if [[ "$JAVA_VM_INFO" != *"mixed mode"* ]]; then
        echo "Expected RISC-V Java mixed mode, got $JAVA_VM_INFO" >&2
        exit 1
    fi
    if [[ "${LD_PRELOAD:-}" != *"libatomic.so.1"* ]]; then
        echo "libatomic.so.1 was not added to LD_PRELOAD on riscv64" >&2
        exit 1
    fi
    echo "LD_PRELOAD: $LD_PRELOAD"
fi

WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/hugegraph-rocksdb-jni-smoke.XXXXXX")
SMOKE_LOG="$WORK_DIR/rocksdb-jni-smoke.log"
export ROCKSDB_SMOKE_DIR="$WORK_DIR/db"

cleanup() {
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

"$JAVA_CMD" -cp "$SERVER_DIR/lib/*" groovy.ui.GroovyMain \
            "$TRAVIS_DIR/rocksdb-jni-smoke.groovy" | tee "$SMOKE_LOG"

if ! grep -q '^rocksdb-jni-smoke-ok$' "$SMOKE_LOG"; then
    echo "RocksDB JNI smoke marker not found" >&2
    exit 1
fi
