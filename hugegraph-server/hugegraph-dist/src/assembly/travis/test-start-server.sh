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
# Verifies that start-server.sh can run twice on one configuration, as
# run-api-test.sh does: the second start must leave the config unchanged and
# each line the script appends must be there exactly once, since a key set
# twice is read back as a list (usePD as '[true, true]').

set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
START_SERVER="${1:-${SCRIPT_DIR}/start-server.sh}"
STATIC_DIR=$(cd "${SCRIPT_DIR}/../static" && pwd)
TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/start-server-test.XXXXXX")
FAILURES=0

trap 'rm -rf "${TMP_DIR}"' EXIT

fail() {
    echo "FAIL: $1" >&2
    FAILURES=$((FAILURES + 1))
}

start_server() {
    if ! bash "$START_SERVER" "$server_dir" "$backend" > "${TMP_DIR}/start.log" 2>&1; then
        cat "${TMP_DIR}/start.log" >&2
        fail "${backend}: start-server.sh exited non-zero"
        return 1
    fi
}

# The lines that set the key must be exactly key=value, or none if value is empty
assert_key() {
    local file=$1 key=$2 value=$3 actual
    actual=$(awk -v prefix="${key}=" 'index($0, prefix) == 1' "$file" | paste -sd ' ' -)
    [[ "$actual" == "${value:+${key}=${value}}" ]] ||
        fail "${backend}: ${file##*/} has '${actual}' for ${key}, expected '${value:+${key}=${value}}'"
}

if [[ ! -f "$START_SERVER" ]]; then
    echo "ERROR: start script not found: $START_SERVER"
    exit 2
fi

for backend in hstore hbase rocksdb memory; do
    # A copy of the packaged files, with the backend initialization and the
    # Server launch stubbed
    server_dir="${TMP_DIR}/${backend}"
    mkdir -p "${server_dir}/bin" "${server_dir}/conf/graphs"
    cp "${STATIC_DIR}/bin/util.sh" "${server_dir}/bin/"
    cp "${STATIC_DIR}/conf/rest-server.properties" "${STATIC_DIR}/conf/gremlin-server.yaml" "${server_dir}/conf/"
    cp "${STATIC_DIR}/conf/graphs/hugegraph.properties" "${server_dir}/conf/graphs/"
    printf '#!/bin/bash\ncat >/dev/null\n' > "${server_dir}/bin/init-store.sh"
    printf '#!/bin/bash\n' > "${server_dir}/bin/start-hugegraph.sh"
    chmod +x "${server_dir}/bin/init-store.sh" "${server_dir}/bin/start-hugegraph.sh"

    start_server || continue
    cp -R "${server_dir}/conf" "${TMP_DIR}/${backend}-first-start"
    start_server || continue
    (cd "$TMP_DIR" && diff -ru "${backend}-first-start" "${backend}/conf") >&2 ||
        fail "${backend}: the second start changed the config as shown above"

    use_pd=""
    hbase_timeout=""
    case "$backend" in
        hstore) use_pd=true ;;
        hbase) hbase_timeout=200 ;;
    esac
    rest_conf="${server_dir}/conf/rest-server.properties"
    assert_key "$rest_conf" usePD "$use_pd"
    assert_key "$rest_conf" restserver.request_timeout "$hbase_timeout"
    assert_key "$rest_conf" gremlinserver.timeout "$hbase_timeout"
    assert_key "${server_dir}/conf/graphs/hugegraph.properties" schema.sync_deletion true
done

if [[ "$FAILURES" -gt 0 ]]; then
    echo "FAIL: ${FAILURES} check(s) failed after two starts" >&2
    exit 1
fi

echo "PASS: start-server.sh run twice sets each config line once for hstore, hbase, rocksdb and memory"
