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
# Complete launcher fixtures with Java/download substitutes, not business JVM or JNI execution.
set -Eeuo pipefail
SOURCE_BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")/../static/bin" && pwd)"
REPO_ROOT="$(cd "$SOURCE_BIN/../../../../../.." && pwd)"
SHARED_BIN="$REPO_ROOT/hugegraph-server/hugegraph-dist/src/assembly/static/bin"
TEST_ROOT=$(mktemp -d)
TEST_ROOT=$(cd "$TEST_ROOT" && pwd -P)
trap 'rm -rf "$TEST_ROOT"' EXIT
mkdir -p "$TEST_ROOT/fake-bin" "$TEST_ROOT/java/bin"
cat > "$TEST_ROOT/fake-bin/uname" <<'MOCK'
#!/bin/bash
case "$1" in -m) echo "$FIXTURE_ARCH" ;; *) echo Linux ;; esac
MOCK
cat > "$TEST_ROOT/fake-bin/ps" <<'MOCK'
#!/bin/bash
exit 0
MOCK
cat > "$TEST_ROOT/java/bin/java" <<'MOCK'
#!/bin/bash
if [ "$1" = -version ]; then
    printf '%s\n' "${LD_PRELOAD:-}" > "$VERSION_CAPTURE"
    echo 'openjdk version "17.0.1"' >&2
else
    printf '%s\n' "${LD_PRELOAD:-}" > "$MAIN_CAPTURE"
    printf '%s\n' "$@" > "$MAIN_CAPTURE.args"
fi
MOCK
chmod +x "$TEST_ROOT/fake-bin/"* "$TEST_ROOT/java/bin/java"

run_launcher() {
    local arch="$1" download="$2" provider="$3" inherited="$4" name="$5"
    local top="$TEST_ROOT/$name"
    local -a provider_env=(-u TOPLINGDB_ROCKSDB_PROVIDER)
    [ "$provider" = default ] || provider_env=("TOPLINGDB_ROCKSDB_PROVIDER=$provider")
    mkdir -p "$top/bin" "$top/conf" "$top/lib" "$top/topling"
    cp "$SOURCE_BIN/start-hugegraph-store.sh" "$top/bin/"
    cp "$SHARED_BIN/preload-topling.sh" "$top/bin/"
    cat > "$top/bin/util.sh" <<'MOCK'
# Substitute only utility calls needed by this launcher fixture.
ensure_path_writable() { mkdir -p "$1"; }
download_and_verify() {
    [ "$FIXTURE_DOWNLOAD" = success ] || return 1
    touch "$2"
}
MOCK
    touch "$top/lib/hg-store-node-fixture.jar" "$top/topling/rocksdbjni.jar" \
          "$top/topling/librocksdbjni-linux64.so"
    (cd "$top/topling" && sha256sum rocksdbjni.jar librocksdbjni-linux64.so > easy-migrate.sha256)
    printf '%s\n' 'http: {auto_start_http: false}' > "$top/conf/toplingdb.yaml"
    VERSION_CAPTURE="$top/version-preloads" MAIN_CAPTURE="$top/main-preloads" \
        FIXTURE_ARCH="$arch" FIXTURE_DOWNLOAD="$download" JAVA_HOME="$TEST_ROOT/java" \
        JAVA_OPTIONS=-Xmx64m STDOUT_MODE=true LD_PRELOAD="$inherited" \
        PATH="$TEST_ROOT/fake-bin:$PATH" \
        env -u TOPLING_ACTIVE_NATIVE -u TOPLING_RUNTIME_CLASSPATH -u TOPLINGDB_EASY_MIGRATE_CONF \
            -u TOPLING_AUTO_MIGRATE_CONF "${provider_env[@]}" bash "$top/bin/start-hugegraph-store.sh" -d false \
        > "$top/launcher.log" 2>&1
}

# Both allocation-download paths preserve tracing/allocator entries before the selector runs.
for arch in x86_64 aarch64 arm64; do
    for download in success failure; do
        for separator in colon space; do
            inherited=/caller/libtrace.so:/caller/liballoc.so
            [ "$separator" != space ] || inherited='/caller/libtrace.so /caller/liballoc.so'
            name="$arch-$download-$separator"
            top="$TEST_ROOT/$name"
            jemalloc="$top/bin/libjemalloc.so"
            [ "$arch" = x86_64 ] || jemalloc="$top/bin/libjemalloc_aarch64.so"
            expected="$inherited"
            [ "$download" != success ] || expected="$jemalloc:$inherited"
            run_launcher "$arch" "$download" rocksdb "$inherited" "$name" || {
                cat "$top/launcher.log" >&2; exit 1;
            }
            test "$(cat "$top/version-preloads")" = "$expected"
            test "$(cat "$top/main-preloads")" = "$expected"
            grep -qx -- '-jar' "$top/main-preloads.args"

            default_top="$TEST_ROOT/$name-default"
            default_expected="${expected//$top/$default_top}"
            run_launcher "$arch" "$download" default "$inherited" "$name-default"
            test "$(cat "$default_top/main-preloads")" = "$default_expected"

            # A successful allocator download must not hide recognizable competing JNI.
            for provider in default rocksdb topling; do
                bad="$name-conflict-$provider"
                if run_launcher "$arch" "$download" "$provider" \
                   "$inherited:/other/librocksdbjni.so" "$bad"; then
                    echo "FAIL: competing JNI escaped the full $arch/$download launcher" >&2
                    exit 1
                fi
                grep -q 'competing RocksDB JNI in LD_PRELOAD' "$TEST_ROOT/$bad/launcher.log"
                test ! -e "$TEST_ROOT/$bad/main-preloads"
                grep -q '/caller/libtrace.so' "$TEST_ROOT/$bad/version-preloads"
                grep -q '/caller/liballoc.so' "$TEST_ROOT/$bad/version-preloads"
            done
        done
    done

    # A caller already using the same allocator keeps its order without a duplicate entry.
    name="$arch-already-loaded"
    jemalloc="$TEST_ROOT/$name/bin/libjemalloc.so"
    [ "$arch" = x86_64 ] || jemalloc="$TEST_ROOT/$name/bin/libjemalloc_aarch64.so"
    inherited="/caller/libtrace.so:$jemalloc:/caller/liballoc.so"
    run_launcher "$arch" success rocksdb "$inherited" "$name"
    test "$(cat "$TEST_ROOT/$name/main-preloads")" = "$inherited"
done

# Topling is supported only on x86_64; selected JNI precedes allocator and caller libraries.
for download in success failure; do
    name="topling-$download"
    top="$TEST_ROOT/$name"
    inherited=/caller/libtrace.so:/caller/liballoc.so
    expected="$inherited"
    [ "$download" != success ] || expected="$top/bin/libjemalloc.so:$expected"
    run_launcher x86_64 "$download" topling "$inherited" "$name"
    test "$(cat "$top/main-preloads")" = "$top/topling/librocksdbjni-linux64.so:$expected"
    grep -qx -- '-cp' "$top/main-preloads.args"
    grep -qx 'org.springframework.boot.loader.JarLauncher' "$top/main-preloads.args"
    name="arm-topling-$download"
    if run_launcher aarch64 "$download" topling "$inherited" "$name"; then
        echo 'FAIL: Topling was accepted on unsupported ARM' >&2; exit 1
    fi
    grep -q 'Topling requires Linux x86_64' "$TEST_ROOT/$name/launcher.log"
    test ! -e "$TEST_ROOT/$name/main-preloads"
done
echo 'PASS: complete Store launcher preserves preloads and rejects competing JNI across allocator paths'
