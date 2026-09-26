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

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <server-dist> <pd-dist> <store-dist>" >&2
    exit 1
fi

COMPONENT_DIRS=("$1" "$2" "$3")
TEST_ROOT="$(mktemp -d)"
FAKE_BIN="$TEST_ROOT/fake-bin"

mkdir -p "$FAKE_BIN"
printf '%s\n' \
    '#!/bin/sh' \
    'case "${1:-}" in' \
    '  -s) echo Linux ;;' \
    '  -m) echo x86_64 ;;' \
    '  *) echo Linux ;;' \
    'esac' > "$FAKE_BIN/uname"
printf '%s\n' '#!/bin/sh' 'echo "all dependencies resolved"' > "$FAKE_BIN/ldd"
chmod +x "$FAKE_BIN/uname" "$FAKE_BIN/ldd"

cleanup() {
    rm -rf "$TEST_ROOT"
}
trap cleanup EXIT

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

reference_checksum=""
for component_dir in "${COMPONENT_DIRS[@]}"; do
    for helper in common-topling.sh prepare-topling.sh preload-topling.sh \
                  verify-rocksdb-provider.sh; do
        if [ ! -x "$component_dir/bin/$helper" ]; then
            fail "missing executable helper: $component_dir/bin/$helper"
        fi
    done

    if find "$component_dir/lib" -path '*/topling/*' -print -quit | grep -q .; then
        fail "standard artifact contains a Topling JAR: $component_dir"
    fi
    if [ -e "$component_dir/library/librocksdbjni-linux64.so" ]; then
        fail "standard artifact contains a Topling native library: $component_dir"
    fi

    env -u TOPLINGDB_EASY_MIGRATE_CONF PATH="$FAKE_BIN:$PATH" bash -c '
        source "$1/bin/preload-topling.sh"
        test -z "${TOPLINGDB_EASY_MIGRATE_CONF:-}"
        test -z "${TOPLING_RUNTIME_CLASSPATH:-}"
    ' _ "$component_dir"

    checksum=$(sha256sum "$component_dir/bin/preload-topling.sh" |
               awk '{ print $1 }')
    if [ -z "$reference_checksum" ]; then
        reference_checksum="$checksum"
    elif [ "$checksum" != "$reference_checksum" ]; then
        fail "packaged runtime selectors differ across components"
    fi

    fixture="$TEST_ROOT/$(basename "$component_dir")"
    cp -R "$component_dir" "$fixture"
    if TOPLINGDB_ROCKSDB_PROVIDER=topling \
       PATH="$FAKE_BIN:$PATH" bash -c \
            'source "$1/bin/preload-topling.sh"' _ "$fixture" \
            > "$fixture/preload.out" 2>&1; then
        fail "standard artifact accepted Topling without a local runtime: $component_dir"
    fi
    if ! grep -Fq "no prepared ToplingDB JAR found in $fixture/lib/topling/" \
            "$fixture/preload.out"; then
        sed -n '1,120p' "$fixture/preload.out" >&2
        fail "missing component-local runtime error: $component_dir"
    fi

    echo "PASS: standard artifact is isolated and fail-fast: $component_dir"
done
