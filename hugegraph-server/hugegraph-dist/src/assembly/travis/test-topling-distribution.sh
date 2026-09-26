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
    echo "Usage: $0 <server|pd|store> <standard-dir> <topling-dir>" >&2
    exit 1
fi

COMPONENT="$1"
STANDARD_DIR="$2"
TOPLING_DIR="$3"
TOPLING_TAR="$TOPLING_DIR.tar.gz"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

case "$COMPONENT" in
    server) CONFIG_FILE="$TOPLING_DIR/conf/graphs/hugegraph.properties" ;;
    pd) CONFIG_FILE="$TOPLING_DIR/conf/application.yml" ;;
    store) CONFIG_FILE="$TOPLING_DIR/conf/application-pd.yml" ;;
    *) fail "unsupported component: $COMPONENT" ;;
esac

if [ "${TOPLING_EXPECT_DIRTY_STANDARD:-false}" = "true" ]; then
    [ -e "$STANDARD_DIR/bin/pid" ] ||
        fail "standard distribution has no PID test fixture"
    case "$COMPONENT" in
        server)
            [ -e "$STANDARD_DIR/logs/topling-ci-marker" ] ||
                fail "standard Server has no logs test fixture"
            ;;
        pd)
            [ -L "$STANDARD_DIR/pd_data" ] ||
                fail "standard PD has no pd_data symlink test fixture"
            ;;
        store)
            [ -e "$STANDARD_DIR/storage" ] ||
                fail "standard Store has no storage test fixture"
            ;;
    esac
fi

for helper in common-topling.sh prepare-topling.sh preload-topling.sh \
              verify-rocksdb-provider.sh; do
    [ -x "$STANDARD_DIR/bin/$helper" ] ||
        fail "standard distribution is missing helper: $helper"
    [ -x "$TOPLING_DIR/bin/$helper" ] ||
        fail "Topling distribution is missing helper: $helper"
done

if find "$STANDARD_DIR/lib" -path '*/topling/*' -print -quit | grep -q .; then
    fail "standard distribution contains a Topling JAR"
fi
[ ! -e "$STANDARD_DIR/library/librocksdbjni-linux64.so" ] ||
    fail "standard distribution contains a Topling native library"

TOPLING_JAR=$(find "$TOPLING_DIR/lib/topling" -maxdepth 1 \
                   -name 'rocksdbjni*.jar' -print -quit)
[ -n "$TOPLING_JAR" ] || fail "Topling distribution contains no Topling JAR"
NATIVE_LIBRARY="$TOPLING_DIR/library/librocksdbjni-linux64.so"
[ -r "$NATIVE_LIBRARY" ] || fail "Topling distribution contains no native library"
[ -r "$TOPLING_DIR/lib/topling/runtime.properties" ] ||
    fail "Topling distribution contains no runtime marker"
grep -qx 'provider=topling' "$TOPLING_DIR/lib/topling/runtime.properties" ||
    fail "Topling runtime marker has the wrong provider"
grep -Eq '^[[:space:]]*(rocksdb\.provider=|provider:[[:space:]]*)topling([[:space:]]|$)' \
    "$CONFIG_FILE" || fail "Topling provider is not selected in $CONFIG_FILE"

for runtime_path in bin/pid logs pd_data rocksdb-data storage; do
    if [ -e "$TOPLING_DIR/$runtime_path" ] ||
       [ -L "$TOPLING_DIR/$runtime_path" ]; then
        fail "runtime state leaked into Topling directory: $runtime_path"
    fi
done

[ -r "$TOPLING_TAR" ] || fail "Topling archive not found: $TOPLING_TAR"
ARCHIVE_LIST=$(tar -tzf "$TOPLING_TAR")
if grep -Eq '/(bin/pid|logs|pd_data|rocksdb-data|storage)(/|$)' \
        <<<"$ARCHIVE_LIST"; then
    fail "runtime state leaked into Topling archive"
fi
grep -Eq '/lib/topling/runtime.properties$' <<<"$ARCHIVE_LIST" ||
    fail "Topling archive contains no runtime marker"

if ldd "$NATIVE_LIBRARY" 2>/dev/null | grep -q 'not found'; then
    ldd "$NATIVE_LIBRARY" >&2 || true
    fail "Topling native library has unresolved dependencies"
fi

source "$TOPLING_DIR/bin/preload-topling.sh"
[ "$TOPLING_RUNTIME_CLASSPATH" = "$TOPLING_JAR" ] ||
    fail "Topling classpath does not use the component-local JAR"
[ "$TOPLING_ACTIVE_NATIVE" = "$NATIVE_LIBRARY" ] ||
    fail "Topling preload does not use the component-local native library"

echo "PASS: clean component-local Topling distribution: $TOPLING_DIR"
