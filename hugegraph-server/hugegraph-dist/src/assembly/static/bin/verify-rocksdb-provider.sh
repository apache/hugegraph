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

if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
    echo "Usage: $0 <server|pd|store> <rocksdb|topling> <data-paths> [enforce]" >&2
    exit 2
fi

COMPONENT="$1"
PROVIDER="$2"
DATA_PATHS="$3"
ENFORCE="${4:-false}"
MARKER_NAME=".hugegraph-rocksdb-provider"

fail() {
    echo "Error: $*" >&2
    exit 1
}

case "$COMPONENT" in
    server | pd | store) ;;
    *) fail "invalid RocksDB component '$COMPONENT'" ;;
esac
case "$PROVIDER" in
    rocksdb | topling) ;;
    *) fail "invalid RocksDB provider '$PROVIDER'" ;;
esac
case "$ENFORCE" in
    true | false) ;;
    *) fail "provider marker enforcement must be true or false" ;;
esac

EXPECTED_MARKER=$(printf '%s\n' \
    "format=1" \
    "component=$COMPONENT" \
    "provider=$PROVIDER")

command -v flock >/dev/null 2>&1 ||
    fail "flock is required to verify RocksDB provider markers"

verify_marker() {
    local data_path="$1"
    local pinned_path="$2"
    local marker="$pinned_path/$MARKER_NAME"
    local actual_marker

    if [ -L "$marker" ] || { [ -e "$marker" ] && [ ! -f "$marker" ]; }; then
        fail "provider marker is not a regular file: $marker"
    fi
    if [ -f "$marker" ]; then
        [ -r "$marker" ] || fail "provider marker is not readable: $marker"
        actual_marker=$(<"$marker")
        if [ "$actual_marker" != "$EXPECTED_MARKER" ]; then
            fail "provider marker mismatch in $data_path; expected" \
                 "$COMPONENT/$PROVIDER"
        fi
        return 0
    fi

    if [ "$PROVIDER" = "rocksdb" ] && [ "$ENFORCE" = "false" ]; then
        echo "[provider-marker] legacy unmarked RocksDB path accepted: $data_path"
        return 0
    fi

    if find -H "$pinned_path" -mindepth 1 -maxdepth 1 \
            ! -name lost+found -print -quit | grep -q .; then
        fail "refusing unmarked non-empty data path: $data_path"
    fi

    local temporary_marker
    temporary_marker=$(mktemp "$pinned_path/.provider-marker.XXXXXX")
    chmod 600 "$temporary_marker"
    printf '%s\n' "$EXPECTED_MARKER" > "$temporary_marker"
    if ! mv -n "$temporary_marker" "$marker"; then
        rm -f "$temporary_marker"
        fail "could not create provider marker: $marker"
    fi
    if [ -e "$temporary_marker" ]; then
        rm -f "$temporary_marker"
    fi

    actual_marker=$(<"$marker")
    if [ "$actual_marker" != "$EXPECTED_MARKER" ]; then
        fail "provider marker changed while initializing: $marker"
    fi
    echo "[provider-marker] initialized $COMPONENT/$PROVIDER at $data_path"
}

IFS=',' read -r -a PATH_LIST <<<"$DATA_PATHS"
[ "${#PATH_LIST[@]}" -gt 0 ] || fail "no RocksDB data path configured"

for raw_path in "${PATH_LIST[@]}"; do
    data_path=$(printf '%s' "$raw_path" |
                sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
    [ -n "$data_path" ] || fail "empty RocksDB data path"
    [[ "$data_path" == /* ]] ||
        fail "RocksDB data path must be absolute: $data_path"
    [ "$data_path" != "/" ] || fail "RocksDB data path cannot be /"

    [ -d "$data_path" ] ||
        fail "RocksDB data path must be an existing directory: $data_path"
    [ ! -L "$data_path" ] ||
        fail "RocksDB data path cannot be a symlink: $data_path"

    lexical_path=$(realpath -m -s "$data_path")
    physical_path=$(realpath -m "$data_path")
    [ "$lexical_path" = "$physical_path" ] ||
        fail "RocksDB data path contains a symlink: $data_path"

    exec {data_path_fd}<"$data_path"
    flock -x "$data_path_fd" ||
        fail "could not lock RocksDB data path: $data_path"
    pinned_path="/proc/self/fd/$data_path_fd"
    [ -d "$pinned_path" ] ||
        fail "could not pin RocksDB data path: $data_path"

    configured_identity=$(stat -Lc '%d:%i' "$data_path")
    pinned_identity=$(stat -Lc '%d:%i' "$pinned_path")
    [ "$configured_identity" = "$pinned_identity" ] ||
        fail "RocksDB data path changed while locking: $data_path"

    verify_marker "$data_path" "$pinned_path"

    configured_identity=$(stat -Lc '%d:%i' "$data_path")
    pinned_identity=$(stat -Lc '%d:%i' "$pinned_path")
    [ "$configured_identity" = "$pinned_identity" ] ||
        fail "RocksDB data path changed while verifying: $data_path"

    flock -u "$data_path_fd"
    exec {data_path_fd}<&-
done
