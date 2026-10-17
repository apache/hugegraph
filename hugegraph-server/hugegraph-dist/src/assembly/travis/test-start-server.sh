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
# run-api-test.sh starts the same Server directory twice. Run start-server.sh
# twice on one fixture, with init-store and the launcher stubbed, and check
# that every key it writes appears exactly once: a duplicate usePD reached
# HugeConfig as '[true, true]' and failed the second init-store.
set -euo pipefail

TRAVIS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
STATIC_BIN="${TRAVIS_DIR}/../static/bin"
FIXTURE=$(mktemp -d "${TMPDIR:-/tmp}/start-server-test.XXXXXX")
trap 'rm -rf "${FIXTURE}"' EXIT

PASS=0
FAIL=0

expect_once() {
    # expect_once <file> <key> <value>
    local lines
    lines=$(grep -E "^[[:space:]]*${2//./\\.}[[:space:]]*[=:]" "$1" || true)
    if [[ "${lines}" == "$2=$3" ]]; then
        PASS=$((PASS + 1))
    else
        echo "  FAIL $(basename "$1"): expected one '$2=$3', got: ${lines:-nothing}" >&2
        FAIL=$((FAIL + 1))
    fi
}

for backend in hstore hbase; do
    base="${FIXTURE}/${backend}"
    mkdir -p "${base}/bin" "${base}/conf/graphs" "${base}/travis"
    cp "${TRAVIS_DIR}/start-server.sh" "${base}/travis/"
    cp "${STATIC_BIN}/util.sh" "${base}/bin/"
    printf '#!/bin/bash\ncat >/dev/null\n' > "${base}/bin/init-store.sh"
    printf '#!/bin/bash\nexit 0\n' > "${base}/bin/start-hugegraph.sh"
    chmod +x "${base}/bin/"*.sh "${base}/travis/start-server.sh"
    printf 'backend=rocksdb\nserializer=binary\n' > "${base}/conf/graphs/hugegraph.properties"
    printf 'restserver.url=http://127.0.0.1:8080\n' > "${base}/conf/rest-server.properties"
    printf 'evaluationTimeout: 30000\n' > "${base}/conf/gremlin-server.yaml"

    for run in 1 2; do
        if ! bash "${base}/travis/start-server.sh" "${base}" "${backend}" "" \
                > "${base}/start-${run}.log" 2>&1; then
            echo "  FAIL ${backend}: start-server.sh run ${run} failed" >&2
            tail -5 "${base}/start-${run}.log" >&2
            FAIL=$((FAIL + 1))
        fi
    done

    graph="${base}/conf/graphs/hugegraph.properties"
    rest="${base}/conf/rest-server.properties"
    expect_once "${graph}" "backend" "${backend}"
    expect_once "${graph}" "schema.sync_deletion" "true"
    if [[ "${backend}" == hstore ]]; then
        expect_once "${rest}" "usePD" "true"
    else
        expect_once "${rest}" "restserver.request_timeout" "200"
        expect_once "${rest}" "gremlinserver.timeout" "200"
    fi
done

echo "start-server.sh twice on one directory: ${PASS} passed, ${FAIL} failed"
[[ "${FAIL}" -eq 0 ]]
