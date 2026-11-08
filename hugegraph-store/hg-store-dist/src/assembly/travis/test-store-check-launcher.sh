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
# Launcher-selection fixtures; native database inspection is tested separately.
set -Eeuo pipefail
SOURCE_BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")/../static/bin" && pwd)"
REPO_ROOT="$(cd "$SOURCE_BIN/../../../../../.." && pwd)"
SHARED_BIN="$REPO_ROOT/hugegraph-server/hugegraph-dist/src/assembly/static/bin"
TEST_ROOT=$(mktemp -d)
TEST_ROOT=$(cd "$TEST_ROOT" && pwd -P)
trap 'rm -rf "$TEST_ROOT"' EXIT
TOP="$TEST_ROOT/checker distribution"
mkdir -p "$TOP/bin" "$TOP/lib" "$TOP/conf" "$TEST_ROOT/java/bin" "$TEST_ROOT/fake-bin"
cp "$SOURCE_BIN/hugegraph-store-check.sh" "$SHARED_BIN/preload-topling.sh" "$TOP/bin/"
printf '%s\n' '# module options' '--add-opens=java.base/java.lang=ALL-UNNAMED' > "$TOP/bin/jvm-module.options"
touch "$TOP/lib/hg-store-node-test.jar"
cat > "$TEST_ROOT/java/bin/java" <<'JAVA'
#!/bin/bash
printf '%s\0' "$@" > "$CAPTURE.args"
printf '%s\n' "${TOPLING_RUNTIME_CLASSPATH:-}" > "$CAPTURE.runtime"
JAVA
cat > "$TEST_ROOT/fake-bin/uname" <<'UNAME'
#!/bin/bash
case "$1" in -m) echo x86_64 ;; *) echo Linux ;; esac
UNAME
chmod +x "$TEST_ROOT/java/bin/java" "$TEST_ROOT/fake-bin/uname"

run_checker() {
    env -u LD_PRELOAD -u TOPLING_ACTIVE_NATIVE -u TOPLING_RUNTIME_CLASSPATH \
        -u TOPLINGDB_EASY_MIGRATE_CONF -u TOPLING_AUTO_MIGRATE_CONF \
        JAVA_HOME="$TEST_ROOT/java" PATH="$TEST_ROOT/fake-bin:$PATH" \
        TOPLINGDB_ROCKSDB_PROVIDER="$1" CAPTURE="$TEST_ROOT/capture" \
        bash "$TOP/bin/hugegraph-store-check.sh" --check \
        --config "$TOP/conf" --data-path "$TOP/data with spaces" > "$TEST_ROOT/output" 2>&1
}

for provider in rocksdb topling; do
    expected="$TOP/lib/hg-store-node-test.jar"
    if [[ "$provider" == topling ]]; then
        mkdir -p "$TOP/topling"
        touch "$TOP/topling/rocksdbjni.jar" "$TOP/topling/librocksdbjni-linux64.so"
        (cd "$TOP/topling" && sha256sum rocksdbjni.jar librocksdbjni-linux64.so > easy-migrate.sha256)
        printf '%s\n' 'http: {auto_start_http: false}' > "$TOP/conf/toplingdb.yaml"
        expected="$TOP/topling/rocksdbjni.jar:$expected"
    fi
    run_checker "$provider" || { cat "$TEST_ROOT/output"; exit 1; }
    args=()
    while IFS= read -r -d '' arg; do
        args+=("$arg")
    done < "$TEST_ROOT/capture.args"
    [[ ${#args[@]} -eq 10 ]]
    [[ "${args[0]}" == --add-opens=java.base/java.lang=ALL-UNNAMED ]]
    [[ "${args[1]}" == -Dloader.main=org.apache.hugegraph.store.node.GraphIdCheckCommand ]]
    [[ "${args[2]}" == -cp && "${args[3]}" == "$expected" ]]
    [[ "${args[4]}" == org.springframework.boot.loader.PropertiesLauncher ]]
    [[ "${args[5]}" == --check && "${args[6]}" == --config && "${args[7]}" == "$TOP/conf" ]]
    [[ "${args[8]}" == --data-path && "${args[9]}" == "$TOP/data with spaces" ]]
    if [[ "$provider" == topling ]]; then
        [[ "$(cat "$TEST_ROOT/capture.runtime")" == "$TOP/topling/rocksdbjni.jar" ]]
    else
        [[ ! -s "$TEST_ROOT/capture.runtime" || "$(cat "$TEST_ROOT/capture.runtime")" == '' ]]
    fi
done

# Do not start Java with a modified or incompletely prepared provider runtime.
printf 'modified' >> "$TOP/topling/rocksdbjni.jar"
rm "$TEST_ROOT/capture.args"
if run_checker topling; then
    echo 'FAIL: unverified checker runtime was accepted' >&2
    exit 1
fi
grep -q 'unverified EasyMigrate runtime' "$TEST_ROOT/output"
[[ ! -e "$TEST_ROOT/capture.args" ]]
echo 'PASS: checker standard/Topling classpaths, quoted argv, module flags and unverified-runtime refusal'
