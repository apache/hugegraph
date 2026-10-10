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

# Exercise each launcher without starting an application, then ask the real JVM
# to report the effective flags. Capturing argv alone misses option precedence.
set -euo pipefail

SOURCE_ROOT=$(cd "${1:?Usage: $0 SOURCE_ROOT}" && pwd)
REAL_JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
REAL_JAVA=$(command -v "$REAL_JAVA")
FIXTURE=$(mktemp -d)
trap 'rm -rf "$FIXTURE"' EXIT
mkdir -p "$FIXTURE/bin" "$FIXTURE/jdk/bin" "$FIXTURE/conf" \
         "$FIXTURE/lib" "$FIXTURE/ext" "$FIXTURE/plugins" "$FIXTURE/logs"
cat > "$FIXTURE/jdk/bin/java" <<'JAVA'
#!/bin/bash
for arg in "$@"; do
    if [[ "$arg" == -version ]]; then
        exec "$REAL_JAVA" -version
    fi
done
printf '%s\n' "$@" > "$CAPTURE_FILE"
options=()
for arg in "$@"; do
    [[ "$arg" == -cp || "$arg" == -jar ]] && break
    options+=("$arg")
done
exec "$REAL_JAVA" "${options[@]}" -XX:+PrintFlagsFinal -version
JAVA
# Avoid native allocator downloads; this fixture only checks JVM configuration.
printf '%s\n' '#!/bin/sh' 'echo launcher-test' > "$FIXTURE/jdk/bin/uname"
chmod +x "$FIXTURE/jdk/bin/"*

failed=0
for component in server pd store; do
    case "$component" in
        server)
            source_bin="$SOURCE_ROOT/hugegraph-server/hugegraph-dist/src/assembly/static/bin"
            launcher=hugegraph-server.sh
            cp "$source_bin/jvm-module.options" "$FIXTURE/bin/"
            ;;
        pd)
            source_bin="$SOURCE_ROOT/hugegraph-pd/hg-pd-dist/src/assembly/static/bin"
            launcher=start-hugegraph-pd.sh
            touch "$FIXTURE/lib/hg-pd-service-test.jar"
            ;;
        store)
            source_bin="$SOURCE_ROOT/hugegraph-store/hg-store-dist/src/assembly/static/bin"
            launcher=start-hugegraph-store.sh
            rm -f "$FIXTURE/lib/"*.jar
            touch "$FIXTURE/lib/hg-store-node-test.jar"
            ;;
    esac
    cp "$source_bin/$launcher" "$source_bin/util.sh" "$FIXTURE/bin/"
    # Keep the computed-heap/-j path deterministic without probing host memory.
    printf '\nfree_memory() { echo 2048; }\n' >> "$FIXTURE/bin/util.sh"
    cp "$SOURCE_ROOT/hugegraph-server/hugegraph-dist/src/assembly/static/bin/preload-topling.sh" "$FIXTURE/bin/"
    for mode in default g1 G1 env-override cli-override serial zgc; do
        gc=""
        user_options=""
        jvm_options="-Xms64m -Xmx64m"
        collector=UseG1GC
        ihop=50
        ref_proc=true
        rset_pause=5
        override_options="-XX:InitiatingHeapOccupancyPercent=45 -XX:-ParallelRefProcEnabled
                          -XX:G1RSetUpdatingPauseTimePercent=2"
        case "$mode" in
            default) collector="" ;;
            g1|G1) gc="$mode" ;;
            env-override)
                gc=g1; jvm_options="$jvm_options $override_options"
                ihop=45; ref_proc=false; rset_pause=2 ;;
            cli-override)
                gc=g1; jvm_options=""; user_options="-Xms64m -Xmx64m $override_options"
                ihop=45; ref_proc=false; rset_pause=2 ;;
            serial) jvm_options="$jvm_options -XX:+UseSerialGC"; collector=UseSerialGC ;;
            zgc) jvm_options="$jvm_options -XX:+UseZGC"; collector=UseZGC ;;
        esac
        args=(-d false)
        [[ -z "$gc" ]] || args+=(-g "$gc")
        [[ -z "$user_options" ]] || args+=(-j "$user_options")
        if [[ "$component" == server ]]; then
            args=(conf/gremlin-server.yaml conf/rest-server.properties false
                  "$user_options" "$gc" false)
        fi
        capture="$FIXTURE/$component-$mode.args"
        output="$FIXTURE/$component-$mode.flags"
        if ! env -u GC_OPTION -u JAVA_TOOL_OPTIONS -u _JAVA_OPTIONS -u JDK_JAVA_OPTIONS -u LD_PRELOAD \
            -u TOPLINGDB_ROCKSDB_PROVIDER -u TOPLING_RUNTIME_CLASSPATH -u TOPLING_ACTIVE_NATIVE \
            JAVA_HOME="$FIXTURE/jdk" REAL_JAVA="$REAL_JAVA" JAVA_OPTIONS="$jvm_options" \
            OPEN_TELEMETRY=false STDOUT_MODE=true CAPTURE_FILE="$capture" \
            PATH="$FIXTURE/jdk/bin:$PATH" bash "$FIXTURE/bin/$launcher" "${args[@]}" > "$output" 2>&1; then
            echo "FAIL: $component $mode launcher rejected JVM options" >&2
            failed=$((failed + 1))
            continue
        fi
        if [[ -n "$collector" ]] &&
           ! grep -Eq " $collector[[:space:]]*=[[:space:]]*true " "$output" ||
           ! grep -Eq " InitiatingHeapOccupancyPercent[[:space:]]*=[[:space:]]*$ihop " "$output" ||
           ! grep -Eq " ParallelRefProcEnabled[[:space:]]*=[[:space:]]*$ref_proc " "$output" ||
           ! grep -Eq " G1RSetUpdatingPauseTimePercent[[:space:]]*=[[:space:]]*$rset_pause " "$output"; then
            echo "FAIL: $component $mode expected ${collector:-JVM default}," \
                 "IHOP=$ihop, ref=$ref_proc, RSet=$rset_pause" >&2
            grep -E " (Use.*GC|InitiatingHeapOccupancyPercent|ParallelRefProcEnabled|G1RSetUpdatingPauseTimePercent) " \
                "$output" >&2
            failed=$((failed + 1))
            continue
        fi
        if [[ -n "$gc" ]]; then
            grep -Fxq -- '-XX:+UseG1GC' "$capture"
        elif grep -Fxq -- '-XX:+UseG1GC' "$capture"; then
            echo "FAIL: $component enabled G1 without an explicit selection" >&2
            exit 1
        fi
        echo "PASS: $component $mode (${collector:-JVM default}," \
             "IHOP=$ihop, ref=$ref_proc, RSet=$rset_pause)"
    done
done
[[ $failed -eq 0 ]]
