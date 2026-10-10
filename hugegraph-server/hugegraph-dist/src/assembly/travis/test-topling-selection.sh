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
# Selection-only fixtures; real JNI preparation and live services are separate gates.
set -Eeuo pipefail
SOURCE_BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")/../static/bin" && pwd)"
TEST_ROOT=$(mktemp -d)
trap 'rm -rf "$TEST_ROOT"' EXIT
mkdir -p "$TEST_ROOT/fake-bin"
printf '%s\n' '#!/bin/sh' 'case "$1" in -s) echo Linux ;; -m) echo x86_64 ;; esac' > "$TEST_ROOT/fake-bin/uname"
chmod +x "$TEST_ROOT/fake-bin/uname"
for component in server pd store; do
    mkdir -p "$TEST_ROOT/$component/bin" "$TEST_ROOT/$component/conf"
    cp "$SOURCE_BIN/preload-topling.sh" "$TEST_ROOT/$component/bin/"
    # Default startup works without prepared assets and never selects optional JNI.
    env -u TOPLINGDB_ROCKSDB_PROVIDER -u LD_PRELOAD -u TOPLING_ACTIVE_NATIVE \
        -u TOPLING_RUNTIME_CLASSPATH bash -euc 'source "$1"; test -z "${TOPLING_RUNTIME_CLASSPATH:-}"' \
        _ "$TEST_ROOT/$component/bin/preload-topling.sh"
    mkdir "$TEST_ROOT/$component/topling"
    touch "$TEST_ROOT/$component/topling/rocksdbjni.jar" "$TEST_ROOT/$component/topling/librocksdbjni-linux64.so"
    (cd "$TEST_ROOT/$component/topling" && sha256sum rocksdbjni.jar librocksdbjni-linux64.so > easy-migrate.sha256)
    printf '%s\n' 'http: {auto_start_http: false}' > "$TEST_ROOT/$component/conf/toplingdb.yaml"
done
for order in 'pd store' 'store pd' 'server pd'; do
    read -r first second <<< "$order"
    PATH="$TEST_ROOT/fake-bin:$PATH" bash -ec '
        export TOPLINGDB_ROCKSDB_PROVIDER=topling
        unset TOPLINGDB_EASY_MIGRATE_CONF TOPLING_AUTO_MIGRATE_CONF
        export LD_LIBRARY_PATH=/unrelated CLASSPATH=unrelated.jar
        source "$1/$2/bin/preload-topling.sh"
        bash -ec '"'"'source "$1/$2/bin/preload-topling.sh"
            test "$TOPLINGDB_EASY_MIGRATE_CONF" = "$1/$2/conf/toplingdb.yaml"
        '"'"' _ "$1" "$3"
        # Reproduce the Server launcher exporting its selected JAR to children.
        export CLASSPATH="$TOPLING_RUNTIME_CLASSPATH::unrelated.jar::"
        TOPLINGDB_ROCKSDB_PROVIDER=rocksdb bash -ec '"'"'source "$1/$2/bin/preload-topling.sh"
            test "$CLASSPATH" = :unrelated.jar::
            test -z "${TOPLING_RUNTIME_CLASSPATH:-}"
        '"'"' _ "$1" "$3"
        source "$1/$3/bin/preload-topling.sh"
        test "$CLASSPATH" = :unrelated.jar::
        test "$TOPLING_RUNTIME_CLASSPATH" = "$1/$3/topling/rocksdbjni.jar"
        test "$TOPLINGDB_EASY_MIGRATE_CONF" = "$1/$3/conf/toplingdb.yaml"
        export TOPLINGDB_EASY_MIGRATE_CONF="$1/$2/conf/toplingdb.yaml"
        source "$1/$3/bin/preload-topling.sh"
        test "$TOPLINGDB_EASY_MIGRATE_CONF" = "$1/$2/conf/toplingdb.yaml"
        export CLASSPATH="$TOPLING_RUNTIME_CLASSPATH:$CLASSPATH"
        export TOPLINGDB_ROCKSDB_PROVIDER=rocksdb
        source "$1/$2/bin/preload-topling.sh"
        test -z "${TOPLING_RUNTIME_CLASSPATH:-}"
        test "$TOPLINGDB_EASY_MIGRATE_CONF" = "$1/$2/conf/toplingdb.yaml"
        test "$LD_LIBRARY_PATH" = /unrelated
        test "$CLASSPATH" = :unrelated.jar::
    ' _ "$TEST_ROOT" "$first" "$second"
done
# LD_PRELOAD accepts colon and space separators. Keep caller entries after our JNI.
for inherited in '/unrelated/liballoc.so:/unrelated/libtrace.so' \
                 '/unrelated/liballoc.so /unrelated/libtrace.so'; do
    PATH="$TEST_ROOT/fake-bin:$PATH" bash -ec '
        unset TOPLING_ACTIVE_NATIVE TOPLING_RUNTIME_CLASSPATH
        export TOPLINGDB_ROCKSDB_PROVIDER=topling LD_PRELOAD="$2"
        source "$1/server/bin/preload-topling.sh"
        test "$LD_PRELOAD" = "$1/server/topling/librocksdbjni-linux64.so:$2"
        # Switching components must replace only the runtime we previously selected.
        source "$1/pd/bin/preload-topling.sh"
        test "$LD_PRELOAD" = "$1/pd/topling/librocksdbjni-linux64.so:$2"
        export TOPLINGDB_ROCKSDB_PROVIDER=rocksdb
        source "$1/store/bin/preload-topling.sh"
        test "$LD_PRELOAD" = "$2"
    ' _ "$TEST_ROOT" "$inherited"
done
# Reject competing JNI before adding the selected runtime; unrelated entries survive.
for provider in unset rocksdb topling; do
    for inherited in '/other/librocksdbjni.so:/unrelated/libtrace.so' \
                     '/unrelated/libtrace.so /other/librocksdbjni-linux64.so' \
                      '/other/libfrocksdbjni.so' "$TEST_ROOT/server/topling/librocksdbjni-linux64.so"; do
        if [ "$provider" = topling ] && [ "$inherited" = "$TEST_ROOT/server/topling/librocksdbjni-linux64.so" ]; then
            continue
        fi
        if PATH="$TEST_ROOT/fake-bin:$PATH" bash -ec '
            unset TOPLING_ACTIVE_NATIVE TOPLING_RUNTIME_CLASSPATH
            export LD_PRELOAD="$2"
            if [ "$3" = unset ]; then
                unset TOPLINGDB_ROCKSDB_PROVIDER
            else
                export TOPLINGDB_ROCKSDB_PROVIDER="$3"
            fi
            source "$1/server/bin/preload-topling.sh"
        ' _ "$TEST_ROOT" "$inherited" "$provider" > "$TEST_ROOT/error" 2>&1; then
            echo "FAIL: competing RocksDB JNI was accepted in $provider mode" >&2; exit 1
        fi
        grep -q 'competing RocksDB JNI in LD_PRELOAD' "$TEST_ROOT/error"
    done
done
# Older or modified preparations must be verified again before launch.
for state in missing stale; do
    receipt="$TEST_ROOT/server/topling/easy-migrate.sha256"
    if [ "$state" = missing ]; then
        mv "$receipt" "$receipt.saved"
    else
        printf 'changed' >> "$TEST_ROOT/server/topling/rocksdbjni.jar"
    fi
    if PATH="$TEST_ROOT/fake-bin:$PATH" TOPLINGDB_ROCKSDB_PROVIDER=topling \
       bash -ec 'source "$1"' _ "$TEST_ROOT/server/bin/preload-topling.sh" > "$TEST_ROOT/error" 2>&1; then
        echo 'FAIL: unverified preparation was accepted' >&2; exit 1
    fi
    grep -q 'unverified EasyMigrate runtime' "$TEST_ROOT/error"
    [ "$state" != missing ] || mv "$receipt.saved" "$receipt"
done
if TOPLINGDB_ROCKSDB_PROVIDER=invalid bash -ec 'source "$1"' _ \
    "$TEST_ROOT/server/bin/preload-topling.sh" > "$TEST_ROOT/error" 2>&1; then
    echo 'FAIL: invalid provider was accepted' >&2; exit 1
fi
echo 'PASS: standard default, explicit selection, component and inherited configuration isolation'
