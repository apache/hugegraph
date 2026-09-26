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

set -Eeuo pipefail

PROVIDER="${1:?Usage: $0 <provider> <component-dir>}"
COMPONENT_DIR="${2:?Usage: $0 <provider> <component-dir>}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

TEST_ROOT=$(mktemp -d /tmp/hugegraph-rocksdb-runtime.XXXXXX)
cleanup() {
    rm -rf "$TEST_ROOT"
}
trap cleanup EXIT

case "$PROVIDER" in
    rocksdb)
        EXPECTED_NATIVE_PATH=none
        ;;
    topling)
        EXPECTED_NATIVE_PATH="$COMPONENT_DIR/library/librocksdbjni-linux64.so"
        if [ ! -f "$EXPECTED_NATIVE_PATH" ]; then
            echo "Error: Topling native library was not installed for the component" >&2
            exit 1
        fi
        ;;
    *)
        echo "Error: unsupported provider '$PROVIDER'" >&2
        exit 1
        ;;
esac

if compgen -G "$COMPONENT_DIR/lib/rocksdbjni*.jar" >/dev/null; then
    if [ "$PROVIDER" = "topling" ]; then
        JAR="${TOPLING_RUNTIME_CLASSPATH:-}"
    else
        JAR=$(ls -1 "$COMPONENT_DIR"/lib/rocksdbjni*.jar 2>/dev/null |
              sort -V | tail -1 || true)
    fi
else
    if [ "$PROVIDER" = "topling" ]; then
        JAR=$(ls -1 "$COMPONENT_DIR"/lib/topling/rocksdbjni*.jar 2>/dev/null |
              sort -V | tail -1 || true)
    else
        BOOT_JAR=$(ls -1 "$COMPONENT_DIR"/lib/*.jar 2>/dev/null |
                   sort -V | tail -1 || true)
        NESTED_JAR=$(unzip -Z1 "$BOOT_JAR" 'BOOT-INF/lib/rocksdbjni*.jar' |
                     sort -V | tail -1 || true)
        if [ -z "$NESTED_JAR" ]; then
            echo "Error: no embedded rocksdbjni JAR found in $BOOT_JAR" >&2
            exit 1
        fi
        JAR="$TEST_ROOT/rocksdbjni.jar"
        unzip -p "$BOOT_JAR" "$NESTED_JAR" > "$JAR"
    fi
fi
if [ -z "$JAR" ]; then
    echo "Error: no rocksdbjni JAR found for provider '$PROVIDER'" >&2
    exit 1
fi

if [ "$PROVIDER" = "topling" ]; then
    EASY_MIGRATE_CONF="${TOPLINGDB_EASY_MIGRATE_CONF:-}"
    if [ -z "$EASY_MIGRATE_CONF" ]; then
        for candidate in \
            "$COMPONENT_DIR/conf/toplingdb.yaml" \
            "$COMPONENT_DIR/conf/rocksdb_store.yaml" \
            "$COMPONENT_DIR/conf/rocksdb_pd.yaml"; do
            if [ -f "$candidate" ]; then
                EASY_MIGRATE_CONF="$candidate"
                break
            fi
        done
    fi
    if [ -z "$EASY_MIGRATE_CONF" ] || [ ! -r "$EASY_MIGRATE_CONF" ]; then
        echo "Error: readable ToplingDB Easy Migrate config is required" >&2
        exit 1
    fi

    NATIVE_DIR="$(dirname "$EXPECTED_NATIVE_PATH")"
    TEST_LD_LIBRARY_PATH="$NATIVE_DIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
    TEST_LD_PRELOAD="${LD_PRELOAD:-}"
    case ":$TEST_LD_PRELOAD:" in
        *":$EXPECTED_NATIVE_PATH:"*) ;;
        *) TEST_LD_PRELOAD="$EXPECTED_NATIVE_PATH${TEST_LD_PRELOAD:+:$TEST_LD_PRELOAD}" ;;
    esac

    TOPLINGDB_EASY_MIGRATE_CONF="$EASY_MIGRATE_CONF" \
        LD_LIBRARY_PATH="$TEST_LD_LIBRARY_PATH" \
        LD_PRELOAD="$TEST_LD_PRELOAD" \
        java -cp "$JAR" "$SCRIPT_DIR/RocksDBRuntimeSmokeTest.java" \
        "$PROVIDER" "$TEST_ROOT/db" "$EXPECTED_NATIVE_PATH"
else
    env -u TOPLINGDB_EASY_MIGRATE_CONF \
        java -cp "$JAR" "$SCRIPT_DIR/RocksDBRuntimeSmokeTest.java" \
        "$PROVIDER" "$TEST_ROOT/db" "$EXPECTED_NATIVE_PATH"
fi
