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
# Sourced by launchers. Do not change the caller's shell flags or business config.
topling_remove_path() {
    local value="$1" remove="$2" preserve_empty="${3:-false}" entry result="" separator=""
    while true; do
        entry="${value%%:*}"
        if [ "$entry" != "$remove" ] && { [ -n "$entry" ] || [ "$preserve_empty" = true ]; }; then
            result="$result$separator$entry"
            separator=:
        fi
        [[ "$value" = *:* ]] || break
        value="${value#*:}"
    done
    printf '%s\n' "$result"
}

topling_select_runtime() {
    local top provider native jar conf inherited entry
    local -a preloads
    top="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)" || return 1
    provider="${TOPLINGDB_ROCKSDB_PROVIDER:-rocksdb}"
    case "$provider" in rocksdb | topling) ;; *)
        echo "Error: invalid TOPLINGDB_ROCKSDB_PROVIDER '$provider'" >&2; return 1 ;;
    esac
    if [ -n "${TOPLING_ACTIVE_NATIVE:-}" ]; then
        LD_PRELOAD=$(topling_remove_path "${LD_PRELOAD:-}" "$TOPLING_ACTIVE_NATIVE")
        LD_LIBRARY_PATH=$(topling_remove_path "${LD_LIBRARY_PATH:-}" "$(dirname "$TOPLING_ACTIVE_NATIVE")")
        export LD_PRELOAD LD_LIBRARY_PATH
    fi
    if [ -n "${TOPLING_RUNTIME_CLASSPATH:-}" ] && [ "${CLASSPATH+x}" = x ]; then
        CLASSPATH=$(topling_remove_path "$CLASSPATH" "$TOPLING_RUNTIME_CLASSPATH" true)
        export CLASSPATH
    fi
    unset TOPLING_ACTIVE_NATIVE TOPLING_RUNTIME_CLASSPATH
    if [ -n "${TOPLING_AUTO_MIGRATE_CONF:-}" ] &&
       [ "${TOPLINGDB_EASY_MIGRATE_CONF:-}" = "$TOPLING_AUTO_MIGRATE_CONF" ]; then
        unset TOPLINGDB_EASY_MIGRATE_CONF
    fi
    unset TOPLING_AUTO_MIGRATE_CONF
    native="$top/topling/librocksdbjni-linux64.so"
    # Two RocksDB JNI libraries can conflict before the JVM even starts.
    inherited="${LD_PRELOAD:-}"
    inherited="${inherited//:/ }"
    inherited="${inherited//$'\n'/ }"
    read -r -a preloads <<< "$inherited"
    for entry in "${preloads[@]-}"; do
        case "${entry##*/}" in librocksdbjni*.so* | libfrocksdbjni*.so*)
            if [ "$provider" != topling ] || [ "$entry" != "$native" ]; then
                echo "Error: competing RocksDB JNI in LD_PRELOAD: $entry" >&2
                return 1
            fi ;;
        esac
    done
    [ "$provider" = topling ] || return 0
    [ "$(uname -s)/$(uname -m)" = Linux/x86_64 ] || {
        echo 'Error: Topling requires Linux x86_64' >&2; return 1;
    }
    jar="$top/topling/rocksdbjni.jar"
    [ -r "$jar" ] && [ -r "$native" ] || {
        echo "Error: run $top/bin/prepare-topling.sh before enabling Topling" >&2; return 1;
    }
    [ -r "$top/topling/easy-migrate.sha256" ] &&
    (cd "$top/topling" && sha256sum -c easy-migrate.sha256 >/dev/null) || {
        echo "Error: unverified EasyMigrate runtime; prepare a fresh distribution with $top/bin/prepare-topling.sh" >&2
        return 1
    }
    conf="${TOPLINGDB_EASY_MIGRATE_CONF:-}"
    if [ -z "$conf" ]; then
        conf="$top/conf/toplingdb.yaml"
        export TOPLING_AUTO_MIGRATE_CONF="$conf"
    fi
    [ -r "$conf" ] || { echo "Error: unreadable Easy Migrate configuration: $conf" >&2; return 1; }
    export TOPLINGDB_EASY_MIGRATE_CONF="$conf"
    export LD_LIBRARY_PATH="$top/topling${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
    # The selected JNI must precede inherited preloads during native symbol lookup.
    export LD_PRELOAD="$native${LD_PRELOAD:+:$LD_PRELOAD}"
    export TOPLING_ACTIVE_NATIVE="$native" TOPLING_RUNTIME_CLASSPATH="$jar"
    echo "Selected Topling runtime: $jar; configuration: $conf"
}

if topling_select_runtime; then
    unset -f topling_select_runtime topling_remove_path
else
    unset -f topling_select_runtime topling_remove_path
    return 1 2>/dev/null || exit 1
fi
