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

set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SOURCE_BIN="${1:-$(cd "${SCRIPT_DIR}/../static/bin" && pwd)}"
TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/wait-storage-test.XXXXXX")
DIST_ROOT="${TMP_DIR}/dist"
MOCK_BIN="${TMP_DIR}/mock-bin"
CALL_LOG="${TMP_DIR}/curl-calls"
ARGS_LOG="${TMP_DIR}/curl-args"
CONFIG_LOG="${TMP_DIR}/curl-config"
COUNT_FILE="${TMP_DIR}/store-call-count"
TIMEOUT_LOG="${TMP_DIR}/timeout-arg"
CASE_OUTPUT=""
CASE_RC=0

trap 'rm -rf "${TMP_DIR}"' EXIT

fail() {
    echo "FAIL: $1" >&2
    [[ -z "${CASE_OUTPUT}" ]] || printf '%s\n' "${CASE_OUTPUT}" >&2
    exit 1
}

assert_equal() {
    local name="$1" expected="$2" actual="$3"
    [[ "${actual}" == "${expected}" ]] || \
        fail "${name}: expected '${expected}', got '${actual}'"
}

assert_output() {
    local expected="$1"
    [[ "${CASE_OUTPUT}" == *"${expected}"* ]] || \
        fail "missing output '${expected}'"
}

assert_contract() {
    ! grep -q '/v1/health' "${CALL_LOG}" || \
        fail "/v1/health must not gate readiness"
    [[ -s "${ARGS_LOG}" ]] || fail "curl was not called"
    if grep -Fq -- 'test-password' "${ARGS_LOG}"; then
        fail "credential leaked into curl argv"
    fi
    [[ -s "${CONFIG_LOG}" ]] || fail "curl was not given a credential config"
    if grep -Fv -- 'user = "test-user:test-password"' "${CONFIG_LOG}" | grep -q .; then
        fail "authentication credential was not preserved"
    fi
    if grep -Fv -- '--connect-timeout 2' "${ARGS_LOG}" | grep -q .; then
        fail "per-peer connect timeout was not preserved"
    fi
    if grep -Fv -- '--max-time 3' "${ARGS_LOG}" | grep -q .; then
        fail "per-peer maximum timeout was not preserved"
    fi
    assert_equal "outer timeout" "300s" "$(cat "${TIMEOUT_LOG}")"
}

run_case() {
    local scenario="$1" peers="$2" abort_after="$3" password="${4:-test-password}"
    : > "${CALL_LOG}"
    : > "${ARGS_LOG}"
    : > "${CONFIG_LOG}"
    : > "${COUNT_FILE}"
    : > "${TIMEOUT_LOG}"
    : > "${DIST_ROOT}/conf/graphs/hugegraph.properties"

    CASE_OUTPUT=$(env \
        PATH="${MOCK_BIN}:${PATH}" \
        MOCK_SCENARIO="${scenario}" \
        MOCK_ABORT_AFTER="${abort_after}" \
        MOCK_CALL_LOG="${CALL_LOG}" \
        MOCK_ARGS_LOG="${ARGS_LOG}" \
        MOCK_CONFIG_LOG="${CONFIG_LOG}" \
        MOCK_COUNT_FILE="${COUNT_FILE}" \
        MOCK_TIMEOUT_LOG="${TIMEOUT_LOG}" \
        HG_SERVER_PD_REST_ENDPOINT="${peers}" \
        PD_AUTH_USER="test-user" \
        PD_AUTH_PASSWORD="${password}" \
        'hugegraph.backend=hstore' \
        'hugegraph.pd.peers=config-only:8686' \
        "${DIST_ROOT}/bin/wait-storage.sh" 2>&1)
    CASE_RC=$?
}

if [[ ! -f "${SOURCE_BIN}/wait-storage.sh" || ! -f "${SOURCE_BIN}/util.sh" ]]; then
    fail "wait-storage.sh or util.sh not found under ${SOURCE_BIN}"
fi

mkdir -p "${DIST_ROOT}/bin" "${DIST_ROOT}/conf/graphs" "${MOCK_BIN}"
cp "${SOURCE_BIN}/wait-storage.sh" "${SOURCE_BIN}/util.sh" "${DIST_ROOT}/bin/"
: > "${DIST_ROOT}/conf/graphs/hugegraph.properties"

cat > "${MOCK_BIN}/timeout" <<'EOF'
#!/bin/bash
printf '%s\n' "$1" > "${MOCK_TIMEOUT_LOG}"
shift
"$@" &
command_pid=$!
ticks=0
while kill -0 "${command_pid}" 2>/dev/null; do
    count=$(cat "${MOCK_COUNT_FILE}" 2>/dev/null || true)
    count=${count:-0}
    ticks=$((ticks + 1))
    if [[ "${count}" -ge "${MOCK_ABORT_AFTER}" || "${ticks}" -ge 500 ]]; then
        kill -TERM "${command_pid}" 2>/dev/null || true
        wait "${command_pid}" 2>/dev/null || true
        exit 124
    fi
    /bin/sleep 0.01
done
wait "${command_pid}"
EOF

cat > "${MOCK_BIN}/sleep" <<'EOF'
#!/bin/bash
/bin/sleep 0.02
EOF

cat > "${MOCK_BIN}/curl" <<'EOF'
#!/bin/bash
set -u
url="${!#}"
printf '%s\n' "$*" >> "${MOCK_ARGS_LOG}"
printf '%s\n' "${url}" >> "${MOCK_CALL_LOG}"

# The credential must arrive as a config file on stdin, never in argv
for arg in "$@"; do
    if [[ "${arg}" == "-K" ]]; then
        cat >> "${MOCK_CONFIG_LOG}"
        break
    fi
done

# Honour -w like curl: append the write-out with %{http_code} substituted
fmt=""
prev=""
for arg in "$@"; do
    [[ "${prev}" == "-w" ]] && fmt="${arg}"
    prev="${arg}"
done
respond() {
    printf '%s\n' "$1"
    [[ -z "${fmt}" ]] || printf '%s' "${fmt//\\n/$'\n'}" | sed "s/%{http_code}/$2/"
}

if [[ "${url}" == */v1/health ]]; then
    printf '{}\n'
    exit 0
fi

count=$(cat "${MOCK_COUNT_FILE}" 2>/dev/null || true)
count=$((${count:-0} + 1))
printf '%s\n' "${count}" > "${MOCK_COUNT_FILE}"

if [[ "${MOCK_SCENARIO}" == "auth-401" ]]; then
    respond '{"status":-1,"error":"Unauthorized"}' 401
elif [[ "${MOCK_SCENARIO}" == "pd1-up" && \
      "${url}" == "http://pd1:8620/v1/stores" ]]; then
    respond '{"stores":[{"state":"Up"}]}' 200
elif [[ "${MOCK_SCENARIO}" == "hanging-first" && \
        "${url}" == "http://pd0:8620/v1/stores" ]]; then
    if [[ " $* " == *" --connect-timeout 2 "* && \
          " $* " == *" --max-time 3 "* ]]; then
        /bin/sleep 0.05
    else
        /bin/sleep 10
    fi
    exit 28
elif [[ "${MOCK_SCENARIO}" == "hanging-first" && \
        "${url}" == "http://pd1:8620/v1/stores" ]]; then
    respond '{"stores":[{"state":"Up"}]}' 200
elif [[ "${MOCK_SCENARIO}" == "retry" && "${count}" -eq 3 && \
        "${url}" == "http://pd0:8620/v1/stores" ]]; then
    exit 7
elif [[ "${MOCK_SCENARIO}" == "retry" && "${count}" -eq 4 && \
        "${url}" == "http://pd1:8620/v1/stores" ]]; then
    respond '{"stores":[{"state":"Up"}]}' 200
else
    respond '{"stores":[]}' 200
fi
EOF

chmod +x "${MOCK_BIN}/timeout" "${MOCK_BIN}/sleep" "${MOCK_BIN}/curl"

PD0='http://pd0:8620/v1/stores'
PD1='http://pd1:8620/v1/stores'
TWO_CALLS="${PD0}"$'\n'"${PD1}"
FOUR_CALLS="${TWO_CALLS}"$'\n'"${TWO_CALLS}"

echo "wait-storage.sh peer failover tests"

run_case "pd1-up" "pd0:8620,pd1:8620" 6
assert_equal "storeless first peer rc" "0" "${CASE_RC}"
assert_equal "configured peer order" "${TWO_CALLS}" "$(cat "${CALL_LOG}")"
assert_output "Store registration check PASSED via pd1:8620"
assert_contract
echo "  PASS storeless first peer"

run_case "pd1-up" "pd1:8620,pd0:8620" 6
assert_equal "reversed peer order rc" "0" "${CASE_RC}"
assert_equal "stop after first Up peer" "${PD1}" "$(cat "${CALL_LOG}")"
assert_contract
echo "  PASS reversed peer order"

run_case "hanging-first" "pd0:8620,pd1:8620" 4
assert_equal "hanging first peer rc" "0" "${CASE_RC}"
assert_equal "healthy peer after per-peer timeout" "${TWO_CALLS}" "$(cat "${CALL_LOG}")"
assert_output "Store registration check PASSED via pd1:8620"
assert_contract
echo "  PASS hanging first peer"

run_case "retry" "pd0:8620,pd1:8620" 8
assert_equal "retry rc" "0" "${CASE_RC}"
assert_equal "complete peer rescan" "${FOUR_CALLS}" "$(cat "${CALL_LOG}")"
assert_output "Storage backend is VIABLE"
assert_contract
echo "  PASS unavailable peer retry"

run_case "none" "pd0:8620,pd1:8620" 4
[[ "${CASE_RC}" -ne 0 ]] || fail "all-unready peers must fail closed"
assert_equal "all-unready rescan" "${FOUR_CALLS}" "$(cat "${CALL_LOG}")"
assert_output "ERROR: Timeout waiting for storage backend"
assert_contract
echo "  PASS all-unready timeout"

# A secret with CR/LF must reach curl as one escaped config line, not two.
run_case "pd1-up" "pd0:8620,pd1:8620" 6 "$(printf 'a\r\nb\\c"d')"
assert_equal "line-break secret rc" "0" "${CASE_RC}"
if grep -Fv -- 'user = "test-user:a\r\nb\\c\"d"' "${CONFIG_LOG}" | grep -q .; then
    fail "line break or quote in the secret was not escaped for curl -K"
fi
echo "  PASS line break in secret"

# A 401 is a wrong secret, not a storage problem: abort at once, name the cause.
run_case "auth-401" "pd0:8620,pd1:8620" 9
[[ "${CASE_RC}" -ne 0 ]] || fail "a 401 from PD must abort"
assert_output "refused the credential (401)"
assert_equal "no retry after 401" "${PD0}" "$(cat "${CALL_LOG}")"
[[ "${CASE_OUTPUT}" != *"Timeout waiting"* ]] || fail "401 was reported as a timeout"
echo "  PASS 401 aborts without retry"

echo "7 passed, 0 failed"
