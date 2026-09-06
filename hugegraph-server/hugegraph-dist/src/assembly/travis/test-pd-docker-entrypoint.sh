#!/usr/bin/env bash
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
# Checks that the PD Docker entrypoint turns HG_PD_AUTH_SECRET_KEY into valid
# SPRING_APPLICATION_JSON, whatever the secret contains, and that the value
# Spring would read back is the secret that went in.

set -euo pipefail

ENTRYPOINT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../.." && pwd)/hugegraph-pd/hg-pd-dist/docker/docker-entrypoint.sh"
TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/pd-entrypoint-test.XXXXXX")
trap 'rm -rf "${TMP_DIR}"' EXIT

PASS=0
FAIL=0

[[ -f "${ENTRYPOINT}" ]] || { echo "entrypoint not found at ${ENTRYPOINT}" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 required" >&2; exit 1; }

mkdir -p "${TMP_DIR}/bin"
cp "${ENTRYPOINT}" "${TMP_DIR}/docker-entrypoint.sh"
# Stand in for the launcher: record the generated config instead of starting PD
cat > "${TMP_DIR}/bin/start-hugegraph-pd.sh" <<'STUB'
#!/usr/bin/env bash
printf '%s' "${SPRING_APPLICATION_JSON}" > ./spring.json
STUB
chmod +x "${TMP_DIR}/bin/start-hugegraph-pd.sh" "${TMP_DIR}/docker-entrypoint.sh"

run_case() {
    local name="$1" secret="$2"
    local out
    if ! out=$(cd "${TMP_DIR}" && env \
        HG_PD_GRPC_HOST=pd0 \
        HG_PD_RAFT_ADDRESS=pd0:8610 \
        HG_PD_RAFT_PEERS_LIST=pd0:8610 \
        HG_PD_INITIAL_STORE_LIST=store0:8500 \
        HG_PD_AUTH_SECRET_KEY="${secret}" \
        ./docker-entrypoint.sh 2>&1); then
        echo "  FAIL ${name}: entrypoint exited non-zero"
        printf '%s\n' "${out}" | tail -3
        FAIL=$((FAIL + 1))
        return
    fi

    if ! SECRET="${secret}" python3 - "${TMP_DIR}/spring.json" <<'PY'
import json, os, sys
with open(sys.argv[1], encoding="utf-8") as fh:
    doc = json.load(fh)
got = doc["auth"]["secret-key"]
want = os.environ["SECRET"]
if got != want:
    print("  round-trip mismatch: %r != %r" % (got, want))
    sys.exit(1)
PY
    then
        echo "  FAIL ${name}: invalid JSON or secret did not round-trip"
        FAIL=$((FAIL + 1))
        return
    fi
    echo "  PASS ${name}"
    PASS=$((PASS + 1))
}

echo "PD docker-entrypoint secret override"
run_case "plain secret"        'aVerySecretValue123'
run_case "carriage return"     "$(printf 'a\rb')"
run_case "tab"                 "$(printf 'a\tb')"
run_case "double quote"        'a"b'
run_case "backslash"           'a\b'
run_case "backslash and quote" 'a\"b'
run_case "non-ascii"           'sécrèt-2026'
run_case "spaces"              'two words'

# The secret is required, and must never be echoed to the log
if (cd "${TMP_DIR}" && env \
        HG_PD_GRPC_HOST=pd0 HG_PD_RAFT_ADDRESS=pd0:8610 \
        HG_PD_RAFT_PEERS_LIST=pd0:8610 HG_PD_INITIAL_STORE_LIST=store0:8500 \
        ./docker-entrypoint.sh >/dev/null 2>&1); then
    echo "  FAIL missing secret: entrypoint started without HG_PD_AUTH_SECRET_KEY"
    FAIL=$((FAIL + 1))
else
    echo "  PASS missing secret is refused"
    PASS=$((PASS + 1))
fi

log_out=$(cd "${TMP_DIR}" && env \
    HG_PD_GRPC_HOST=pd0 HG_PD_RAFT_ADDRESS=pd0:8610 \
    HG_PD_RAFT_PEERS_LIST=pd0:8610 HG_PD_INITIAL_STORE_LIST=store0:8500 \
    HG_PD_AUTH_SECRET_KEY='do-not-log-this-value' \
    ./docker-entrypoint.sh 2>&1)
if printf '%s' "${log_out}" | grep -q 'do-not-log-this-value'; then
    echo "  FAIL secret was written to the log"
    FAIL=$((FAIL + 1))
else
    echo "  PASS secret is not logged"
    PASS=$((PASS + 1))
fi

echo "${PASS} passed, ${FAIL} failed"
[[ "${FAIL}" -eq 0 ]]
