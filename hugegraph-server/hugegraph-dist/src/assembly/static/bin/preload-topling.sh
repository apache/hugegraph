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

ORIG_SHELL_FLAGS="$-"
ORIG_PIPEFAIL="$(set -o | awk '$1 == "pipefail" { print $2 }')"
ORIG_ERR_TRAP="$(trap -p ERR)"
# Save original IFS to avoid leaking into parent shell when sourced
ORIG_IFS="${IFS}"
set -Eeuo pipefail
IFS=$'\n\t'
# Unified error capture for easy positioning
trap 'echo "[preload-topling] error at line ${LINENO}: ${BASH_COMMAND}" >&2' ERR

RUNTIME_BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPONENT_TOP="$(cd "$RUNTIME_BIN"/../ && pwd)"
COMPONENT_LIB="$COMPONENT_TOP/lib"
DEST_DIR="$COMPONENT_TOP/library"

detect_rocksdb_provider() {
    local conf_dir="$1"
    local file value provider="" seen_provider=false

    while IFS= read -r value; do
        if [ "$seen_provider" = false ]; then
            provider="$value"
            seen_provider=true
        elif [ "$provider" != "$value" ]; then
            echo "Error: conflicting rocksdb.provider values: $provider,$value" >&2
            return 1
        fi
    done < <(
        for file in "$conf_dir"/graphs/*.properties; do
            [ -f "$file" ] || continue
            awk '
                /^[[:space:]]*#/ { next }
                /^[[:space:]]*rocksdb\.provider[[:space:]]*=/ {
                    value = $0
                    sub(/^[^=]*=[[:space:]]*/, "", value)
                    sub(/[[:space:]]+#.*/, "", value)
                    gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
                    print value
                }
            ' "$file"
        done
        for file in "$conf_dir"/application*.yml; do
            [ -f "$file" ] || continue
            awk '
                /^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
                /^[^[:space:]#][^:]*:/ {
                    in_rocksdb = ($0 ~ /^rocksdb[[:space:]]*:/)
                }
                in_rocksdb && /^[[:space:]]+provider[[:space:]]*:/ {
                    value = $0
                    sub(/^[^:]*:[[:space:]]*/, "", value)
                    sub(/[[:space:]]+#.*/, "", value)
                    gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
                    if (value ~ /^"[^"]*"$/ || value ~ /^'\''[^'\'']*'\''$/) {
                        value = substr(value, 2, length(value) - 2)
                    }
                    print value
                }
            ' "$file"
        done
    )

    provider="${provider:-rocksdb}"
    case "$provider" in
        rocksdb | topling)
            echo "$provider"
            ;;
        *)
            echo "Error: invalid rocksdb.provider '$provider'; expected rocksdb or topling" >&2
            return 1
            ;;
    esac
}

PROVIDER="${TOPLINGDB_ROCKSDB_PROVIDER:-}"
if [ -n "$PROVIDER" ]; then
    case "$PROVIDER" in
        rocksdb | topling) ;;
        *)
            echo "Error: invalid TOPLINGDB_ROCKSDB_PROVIDER '$PROVIDER';" \
                 "expected rocksdb or topling" >&2
            exit 1
            ;;
    esac
else
    PROVIDER=$(detect_rocksdb_provider "$COMPONENT_TOP/conf") || exit 1
fi

remove_path_entry() {
    local value="${1:-}"
    local remove="${2:-}"
    local entry result=""
    local path_ifs="$IFS"
    IFS=:
    for entry in $value; do
        [ -n "$entry" ] && [ "$entry" != "$remove" ] || continue
        result="${result:+$result:}$entry"
    done
    IFS="$path_ifs"
    echo "$result"
}

# A parent launcher may start multiple components from one shell. Remove only
# the runtime entry previously selected by this helper before selecting ours.
if [ -n "${TOPLING_ACTIVE_NATIVE:-}" ]; then
    LD_PRELOAD=$(remove_path_entry "${LD_PRELOAD:-}" "$TOPLING_ACTIVE_NATIVE")
    LD_LIBRARY_PATH=$(remove_path_entry "${LD_LIBRARY_PATH:-}" \
                                  "$(dirname "$TOPLING_ACTIVE_NATIVE")")
    export LD_PRELOAD LD_LIBRARY_PATH
fi
if [ -n "${TOPLING_ACTIVE_JAR:-}" ]; then
    CLASSPATH=$(remove_path_entry "${CLASSPATH:-}" "$TOPLING_ACTIVE_JAR")
    export CLASSPATH
fi
unset TOPLING_ACTIVE_NATIVE TOPLING_ACTIVE_JAR TOPLING_RUNTIME_CLASSPATH

if [ "$PROVIDER" = "topling" ]; then
    # Runtime selection is read-only. Installation prepares all files beforehand.
    if [ "$(uname -s)" != "Linux" ] || [ "$(uname -m)" != "x86_64" ]; then
        echo "Error: ToplingDB runtime supports Linux x86_64 only" >&2
        exit 1
    fi
    TOPLING_JAR=$(ls -1 "$COMPONENT_LIB"/topling/rocksdbjni*.jar 2>/dev/null |
                  sort -V | tail -1 || true)
    if [ -z "$TOPLING_JAR" ]; then
        echo "Error: no prepared ToplingDB JAR found in $COMPONENT_LIB/topling/" >&2
        exit 1
    fi

    CONF_FILE="${TOPLINGDB_EASY_MIGRATE_CONF:-}"
    if [ -z "$CONF_FILE" ]; then
        CONF_FILE="$COMPONENT_TOP/conf/toplingdb.yaml"
        if [ ! -f "$CONF_FILE" ]; then
            CONF_FILE="$COMPONENT_TOP/conf/rocksdb_store.yaml"
        fi
        if [ ! -f "$CONF_FILE" ]; then
            CONF_FILE="$COMPONENT_TOP/conf/rocksdb_pd.yaml"
        fi
    fi
    if [ ! -f "$CONF_FILE" ]; then
        echo "Error: required ToplingDB Easy Migrate config not found: $CONF_FILE" >&2
        exit 1
    fi
    if [ ! -r "$CONF_FILE" ]; then
        echo "Error: ToplingDB Easy Migrate config is not readable: $CONF_FILE" >&2
        exit 1
    fi
    export TOPLINGDB_EASY_MIGRATE_CONF="$CONF_FILE"
    echo "[preload-topling] TOPLINGDB_EASY_MIGRATE_CONF=$CONF_FILE"
    NATIVE_LIBRARY="$DEST_DIR/librocksdbjni-linux64.so"
    if [ ! -r "$NATIVE_LIBRARY" ]; then
        echo "Error: prepared ToplingDB native library not found: $NATIVE_LIBRARY" >&2
        echo "       Run bin/prepare-topling.sh for this component before startup." >&2
        exit 1
    fi
    export LD_LIBRARY_PATH="$DEST_DIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
    if command -v ldd >/dev/null 2>&1 &&
       ldd "$NATIVE_LIBRARY" 2>/dev/null | grep -q 'not found'; then
        echo "Error: ToplingDB native library has unresolved system dependencies" >&2
        echo "       Install them during package/deployment preparation." >&2
        exit 1
    fi

    export LD_PRELOAD="${LD_PRELOAD:+$LD_PRELOAD:}$NATIVE_LIBRARY"
    export TOPLING_ACTIVE_NATIVE="$NATIVE_LIBRARY"
    export TOPLING_RUNTIME_CLASSPATH="$TOPLING_JAR"
    export CLASSPATH="$TOPLING_JAR${CLASSPATH:+:$CLASSPATH}"
    export TOPLING_ACTIVE_JAR="$TOPLING_JAR"
else
    unset TOPLINGDB_EASY_MIGRATE_CONF
    echo "[preload-topling] Component uses rocksdb provider"
fi

unset -f detect_rocksdb_provider remove_path_entry

# Restore original IFS
IFS="$ORIG_IFS"
if [ -n "$ORIG_ERR_TRAP" ]; then
    eval "$ORIG_ERR_TRAP"
else
    trap - ERR
fi
# Restore shell options to their state before this script was sourced
case "$ORIG_SHELL_FLAGS" in *e*) set -e ;; *) set +e ;; esac
case "$ORIG_SHELL_FLAGS" in *u*) set -u ;; *) set +u ;; esac
case "$ORIG_SHELL_FLAGS" in *E*) set -E ;; *) set +E ;; esac
if [ "$ORIG_PIPEFAIL" = "on" ]; then
    set -o pipefail
else
    set +o pipefail
fi
