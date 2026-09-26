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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIST_ROOT="$(cd "$SCRIPT_DIR"/../../.. && pwd)"
PRELOAD_SOURCE="$DIST_ROOT/src/assembly/static/bin/preload-topling.sh"
TEST_ROOT="$(mktemp -d)"
COMPONENT_ROOT="$TEST_ROOT/component"
FAKE_BIN="$TEST_ROOT/fake-bin"

cleanup() {
    rm -rf "$TEST_ROOT"
}
trap cleanup EXIT

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

expect_failure() {
    local name="$1"
    local expected="$2"
    shift 2
    local output

    if output=$("$@" 2>&1); then
        fail "$name unexpectedly succeeded"
    fi
    if ! grep -Fq "$expected" <<<"$output"; then
        echo "$output" >&2
        fail "$name did not report: $expected"
    fi
    echo "PASS: $name"
}

reset_fixture() {
    rm -rf "$COMPONENT_ROOT" "$FAKE_BIN"
    mkdir -p "$COMPONENT_ROOT/bin" "$COMPONENT_ROOT/conf/graphs" \
             "$COMPONENT_ROOT/lib/topling" "$FAKE_BIN"
    cp "$PRELOAD_SOURCE" "$COMPONENT_ROOT/bin/preload-topling.sh"

    printf '%s\n' \
        '#!/bin/sh' \
        'case "${1:-}" in' \
        '  -s) echo Linux ;;' \
        '  -m) echo x86_64 ;;' \
        '  *) echo Linux ;;' \
        'esac' > "$FAKE_BIN/uname"
    printf '%s\n' '#!/bin/sh' 'echo "all dependencies resolved"' > "$FAKE_BIN/ldd"
    chmod +x "$FAKE_BIN/uname" "$FAKE_BIN/ldd"
}

source_preload() {
    PATH="$FAKE_BIN:$PATH" bash -c 'source "$1"' _ \
        "$COMPONENT_ROOT/bin/preload-topling.sh"
}

source_preload_override() {
    TOPLINGDB_ROCKSDB_PROVIDER="$1" PATH="$FAKE_BIN:$PATH" \
        bash -c 'source "$1"' _ \
        "$COMPONENT_ROOT/bin/preload-topling.sh"
}

reset_fixture
source_preload
echo "PASS: unset provider selects standard RocksDB without Topling runtime"

printf '%s\n' 'rocksdb.provider=not-topling' \
    > "$COMPONENT_ROOT/conf/graphs/hugegraph.properties"
expect_failure "invalid provider" \
               "invalid rocksdb.provider 'not-topling'" \
               source_preload

expect_failure "invalid provider override" \
               "invalid TOPLINGDB_ROCKSDB_PROVIDER 'not-topling'" \
               source_preload_override not-topling

printf '%s\n' 'rocksdb.provider=rocksdb' \
    > "$COMPONENT_ROOT/conf/graphs/hugegraph.properties"
printf '%s\n' 'rocksdb.provider=topling' \
    > "$COMPONENT_ROOT/conf/graphs/second.properties"
expect_failure "conflicting providers" \
               "conflicting rocksdb.provider values" \
               source_preload

rm -f "$COMPONENT_ROOT/conf/graphs/second.properties"
printf '%s\n' 'rocksdb.provider=topling' \
    > "$COMPONENT_ROOT/conf/graphs/hugegraph.properties"
mkdir -p "$TEST_ROOT/apache-hugegraph-server/lib/topling"
touch "$TEST_ROOT/apache-hugegraph-server/lib/topling/rocksdbjni-stray.jar"
expect_failure "component-local JAR is required" \
               "no prepared ToplingDB JAR found in $COMPONENT_ROOT/lib/topling/" \
               source_preload

touch "$COMPONENT_ROOT/lib/topling/rocksdbjni-topling.jar"
expect_failure "Easy Migrate configuration is required" \
               "required ToplingDB Easy Migrate config not found" \
               source_preload

printf '%s\n' 'http:' '  auto_start_http: false' \
    > "$COMPONENT_ROOT/conf/toplingdb.yaml"
expect_failure "component-local native library is required" \
               "prepared ToplingDB native library not found" \
               source_preload

mkdir -p "$COMPONENT_ROOT/library"
touch "$COMPONENT_ROOT/library/librocksdbjni-linux64.so"
printf '%s\n' '#!/bin/sh' 'echo "libmissing.so => not found"' > "$FAKE_BIN/ldd"
chmod +x "$FAKE_BIN/ldd"
expect_failure "unresolved native dependency" \
               "native library has unresolved system dependencies" \
               source_preload

printf '%s\n' '#!/bin/sh' 'echo "all dependencies resolved"' > "$FAKE_BIN/ldd"
chmod +x "$FAKE_BIN/ldd"
PATH="$FAKE_BIN:$PATH" bash -c '
    source "$1"
    test "$TOPLING_RUNTIME_CLASSPATH" = "$2/lib/topling/rocksdbjni-topling.jar"
    test "$TOPLINGDB_EASY_MIGRATE_CONF" = "$2/conf/toplingdb.yaml"
    test "$TOPLING_ACTIVE_NATIVE" = "$2/library/librocksdbjni-linux64.so"
' _ "$COMPONENT_ROOT/bin/preload-topling.sh" "$COMPONENT_ROOT"
echo "PASS: valid component-local Topling runtime is selected"

printf '%s\n' 'rocksdb.provider=rocksdb' \
    > "$COMPONENT_ROOT/conf/graphs/hugegraph.properties"
TOPLINGDB_ROCKSDB_PROVIDER=topling PATH="$FAKE_BIN:$PATH" bash -c '
    source "$1"
    test "$TOPLING_RUNTIME_CLASSPATH" = "$2/lib/topling/rocksdbjni-topling.jar"
' _ "$COMPONENT_ROOT/bin/preload-topling.sh" "$COMPONENT_ROOT"
echo "PASS: provider override selects ToplingDB before JVM startup"

printf '%s\n' 'rocksdb.provider=topling' \
    > "$COMPONENT_ROOT/conf/graphs/hugegraph.properties"
TOPLINGDB_ROCKSDB_PROVIDER=rocksdb PATH="$FAKE_BIN:$PATH" bash -c '
    source "$1"
    test -z "${TOPLING_RUNTIME_CLASSPATH:-}"
    test -z "${TOPLINGDB_EASY_MIGRATE_CONF:-}"
' _ "$COMPONENT_ROOT/bin/preload-topling.sh"
echo "PASS: provider override selects standard RocksDB without Topling preload"

# A Topling distribution must also be able to select standard RocksDB without
# putting its optional runtime JAR on the HugeGraph Server classpath. Exercise
# the launcher with a fake JVM and inspect the actual -cp argument rather than
# relying on a source-text assertion.
SERVER_LAUNCHER_ROOT="$TEST_ROOT/server launcher"
SERVER_JAVA_CAPTURE="$TEST_ROOT/server-java-classpath"
mkdir -p "$SERVER_LAUNCHER_ROOT/bin" "$SERVER_LAUNCHER_ROOT/conf" \
         "$SERVER_LAUNCHER_ROOT/lib/topling" "$SERVER_LAUNCHER_ROOT/ext" \
         "$SERVER_LAUNCHER_ROOT/plugins" "$SERVER_LAUNCHER_ROOT/logs"
mkdir -p "$SERVER_LAUNCHER_ROOT/lib/hugegraph-shadow/classes"
mkdir -p "$SERVER_LAUNCHER_ROOT/lib/hugegraph-shadow.jar"
cp "$DIST_ROOT/src/assembly/static/bin/hugegraph-server.sh" \
   "$SERVER_LAUNCHER_ROOT/bin/hugegraph-server.sh"
cp "$DIST_ROOT/src/assembly/static/bin/init-store.sh" \
   "$SERVER_LAUNCHER_ROOT/bin/init-store.sh"
cp "$DIST_ROOT/src/assembly/static/bin/preload-topling.sh" \
   "$SERVER_LAUNCHER_ROOT/bin/preload-topling.sh"
cp "$DIST_ROOT/src/assembly/static/bin/util.sh" \
   "$SERVER_LAUNCHER_ROOT/bin/util.sh"
touch "$SERVER_LAUNCHER_ROOT/lib/hugegraph-server-bootstrap.jar" \
      "$SERVER_LAUNCHER_ROOT/lib/topling/log4j-slf4j-impl-topling.jar" \
      "$SERVER_LAUNCHER_ROOT/lib/topling/hugegraph-topling.jar" \
      "$SERVER_LAUNCHER_ROOT/lib/topling/rocksdbjni-topling.jar"
ln -s "$SERVER_LAUNCHER_ROOT/lib/topling/log4j-slf4j-impl-topling.jar" \
      "$SERVER_LAUNCHER_ROOT/lib/log4j-slf4j-impl-topling-alias.jar"
ln -s "$SERVER_LAUNCHER_ROOT/lib/topling/hugegraph-topling.jar" \
      "$SERVER_LAUNCHER_ROOT/lib/hugegraph-topling-alias.jar"
ln -s "$SERVER_LAUNCHER_ROOT/lib/topling" \
      "$SERVER_LAUNCHER_ROOT/lib/topling-alias"
printf '%s\n' \
    'gremlinserver.url=http://127.0.0.1:43123' \
    'restserver.url=http://127.0.0.1:43124' \
    > "$SERVER_LAUNCHER_ROOT/conf/rest-server.properties"
printf '%s\n' \
    '#!/bin/sh' \
    'if [ "${1:-}" = "-server" ]; then shift; fi' \
    'if [ "${1:-}" = "-version" ]; then' \
    '  echo '\''openjdk version "11.0.32"'\'' >&2' \
    '  exit 0' \
    'fi' \
    'while [ "$#" -gt 0 ]; do' \
    '  if [ "$1" = "-cp" ]; then' \
    '    printf "%s\\n" "$2" > "$JAVA_CAPTURE"' \
    '    exit 0' \
    '  fi' \
    '  shift' \
    'done' \
    'exit 0' \
    > "$FAKE_BIN/java"
chmod +x "$FAKE_BIN/java" "$SERVER_LAUNCHER_ROOT/bin/hugegraph-server.sh" \
          "$SERVER_LAUNCHER_ROOT/bin/init-store.sh" \
          "$SERVER_LAUNCHER_ROOT/bin/preload-topling.sh"
JAVA_HOME='' JAVA_OPTIONS=-Xmx64m STDOUT_MODE=true CLASSPATH='' \
    JAVA_CAPTURE="$SERVER_JAVA_CAPTURE" PATH="$FAKE_BIN:$PATH" \
    bash "$SERVER_LAUNCHER_ROOT/bin/hugegraph-server.sh" \
         "$SERVER_LAUNCHER_ROOT/conf/gremlin-server.yaml" \
         "$SERVER_LAUNCHER_ROOT/conf/rest-server.properties" true \
         >/dev/null 2>&1
grep -Fq "$SERVER_LAUNCHER_ROOT/lib/hugegraph-server-bootstrap.jar" \
    "$SERVER_JAVA_CAPTURE" ||
    fail "Server launcher omitted the standard bootstrap JAR"
if grep -Fq "$SERVER_LAUNCHER_ROOT/lib/topling/" "$SERVER_JAVA_CAPTURE"; then
    fail "standard provider launcher leaked the optional Topling classpath"
fi
for leaked in \
    "$SERVER_LAUNCHER_ROOT/lib/log4j-slf4j-impl-topling-alias.jar" \
    "$SERVER_LAUNCHER_ROOT/lib/hugegraph-topling-alias.jar" \
    "$SERVER_LAUNCHER_ROOT/lib/topling-alias/" \
    "$SERVER_LAUNCHER_ROOT/lib/hugegraph-shadow"; do
    if grep -Fq "$leaked" "$SERVER_JAVA_CAPTURE"; then
        fail "standard provider launcher followed a Topling symlink: $leaked"
    fi
done
echo "PASS: standard provider launcher excludes Topling JARs"

SERVER_INIT_CAPTURE="$TEST_ROOT/server-init-classpath"
JAVA_HOME='' CLASSPATH='' JAVA_CAPTURE="$SERVER_INIT_CAPTURE" \
    PATH="$FAKE_BIN:$PATH" bash "$SERVER_LAUNCHER_ROOT/bin/init-store.sh" \
    >/dev/null 2>&1
grep -Fq "$SERVER_LAUNCHER_ROOT/lib/hugegraph-server-bootstrap.jar" \
    "$SERVER_INIT_CAPTURE" ||
    fail "init-store omitted the standard bootstrap JAR"
if grep -Fq "$SERVER_LAUNCHER_ROOT/lib/topling/" "$SERVER_INIT_CAPTURE"; then
    fail "standard provider init-store leaked the optional Topling classpath"
fi
for leaked in \
    "$SERVER_LAUNCHER_ROOT/lib/log4j-slf4j-impl-topling-alias.jar" \
    "$SERVER_LAUNCHER_ROOT/lib/hugegraph-topling-alias.jar" \
    "$SERVER_LAUNCHER_ROOT/lib/topling-alias/" \
    "$SERVER_LAUNCHER_ROOT/lib/hugegraph-shadow"; do
    if grep -Fq "$leaked" "$SERVER_INIT_CAPTURE"; then
        fail "standard provider init-store followed a Topling symlink: $leaked"
    fi
done
echo "PASS: standard provider init-store excludes Topling JARs"
