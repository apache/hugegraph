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

set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "USAGE: $0 SERVER_DIR" >&2
    exit 1
fi

SERVER_DIR=$1
LIB_DIR="$SERVER_DIR/lib"
DIST_JAR=""
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/hugegraph-dist-check.XXXXXX")

cleanup() {
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

if [[ ! -d "$LIB_DIR" ]]; then
    echo "Distribution lib directory does not exist: $LIB_DIR" >&2
    exit 1
fi

function require_jar() {
    local pattern=$1
    if ! find "$LIB_DIR" -maxdepth 1 -type f -name "$pattern" -print -quit | grep -q .; then
        echo "Missing required RocksDB distribution artifact: $pattern" >&2
        exit 1
    fi
}

function forbid_jar() {
    local pattern=$1
    local match
    match=$(find "$LIB_DIR" -maxdepth 1 -type f -name "$pattern" -print -quit)
    if [[ -n "$match" ]]; then
        echo "Unrelated backend artifact found in RocksDB-only distribution: $match" >&2
        exit 1
    fi
}

for pattern in hugegraph-core-*.jar \
               hugegraph-api-*.jar \
               hugegraph-rocksdb-*.jar \
               hugegraph-dist-*.jar \
               jamm-*.jar \
               rocksdbjni-*.jar; do
    require_jar "$pattern"
done

DIST_JAR=$(find "$LIB_DIR" -maxdepth 1 -type f \
                -name 'hugegraph-dist-*.jar' -print -quit)
DIST_JAR=$(cd "$(dirname "$DIST_JAR")" && pwd)/$(basename "$DIST_JAR")
if command -v unzip >/dev/null 2>&1; then
    BACKEND_PROPERTIES=$(unzip -p "$DIST_JAR" backend.properties)
elif command -v jar >/dev/null 2>&1; then
    (cd "$WORK_DIR" && jar xf "$DIST_JAR" backend.properties)
    BACKEND_PROPERTIES=$(cat "$WORK_DIR/backend.properties")
else
    echo "The distribution check requires unzip or a JDK jar command" >&2
    exit 1
fi

if ! grep -qx 'backends=\[rocksdb\]' <<< "$BACKEND_PROPERTIES"; then
    echo "RocksDB-only distribution has an invalid backend registry" >&2
    printf '%s\n' "$BACKEND_PROPERTIES" >&2
    exit 1
fi

for pattern in hugegraph-cassandra-*.jar \
               hugegraph-scylladb-*.jar \
               hugegraph-mysql-*.jar \
               hugegraph-palo-*.jar \
               hugegraph-hbase-*.jar \
               hugegraph-postgresql-*.jar \
               hugegraph-hstore-*.jar; do
    forbid_jar "$pattern"
done

echo "RocksDB-only distribution contains the required backend and no unrelated backend JARs"
