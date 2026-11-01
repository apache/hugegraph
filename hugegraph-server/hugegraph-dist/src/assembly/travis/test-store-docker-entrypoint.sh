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
# Checks that the Store Docker entrypoint passes HG_STORE_ROCKSDB_TOTAL_MEMORY_SIZE
# into SPRING_APPLICATION_JSON as rocksdb.total_memory_size, leaves the rocksdb key
# out when the variable is unset (so conf/application-pd.yml or auto-sizing applies),
# and refuses values other than auto or a plain number of bytes.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../.." && pwd)"
ENTRYPOINT="${REPO_ROOT}/hugegraph-store/hg-store-dist/docker/docker-entrypoint.sh"
TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/store-entrypoint-test.XXXXXX")
trap 'rm -rf "${TMP_DIR}"' EXIT

PASS=0
FAIL=0

[[ -f "${ENTRYPOINT}" ]] || { echo "entrypoint not found at ${ENTRYPOINT}" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 required" >&2; exit 1; }

mkdir -p "${TMP_DIR}/bin"
cp "${ENTRYPOINT}" "${TMP_DIR}/docker-entrypoint.sh"
# Stand in for the launcher: record the generated config instead of starting Store
cat > "${TMP_DIR}/bin/start-hugegraph-store.sh" <<'STUB'
#!/usr/bin/env bash
printf '%s' "${SPRING_APPLICATION_JSON}" > ./spring.json
STUB
chmod +x "${TMP_DIR}/bin/start-hugegraph-store.sh" "${TMP_DIR}/docker-entrypoint.sh"

run_entrypoint() {
    rm -f "${TMP_DIR}/spring.json"
    # Drop any value from the caller's environment so "unset" really is unset
    (cd "${TMP_DIR}" && env -u HG_STORE_ROCKSDB_TOTAL_MEMORY_SIZE \
        HG_STORE_PD_ADDRESS=pd0:8686 \
        HG_STORE_GRPC_HOST=store0 \
        HG_STORE_RAFT_ADDRESS=store0:8510 \
        "$@" \
        ./docker-entrypoint.sh 2>&1)
}

expect_memory() {
    # expect_memory <name> <expected value, or "absent"> [NAME=VALUE ...]
    local name="$1" expected="$2"
    shift 2
    local out
    if ! out=$(run_entrypoint "$@"); then
        echo "  FAIL ${name}: entrypoint exited non-zero"
        printf '%s\n' "${out}" | tail -3
        FAIL=$((FAIL + 1))
        return
    fi
    if ! EXPECTED="${expected}" python3 - "${TMP_DIR}/spring.json" <<'PY'
import json, os, sys
with open(sys.argv[1], encoding="utf-8") as fh:
    doc = json.load(fh)
want = os.environ["EXPECTED"]
got = doc.get("rocksdb", {}).get("total_memory_size", "absent")
if got != want:
    print("  rocksdb.total_memory_size is %r, expected %r" % (got, want))
    sys.exit(1)
if doc["app"]["data-path"] != "/hugegraph-store/storage":
    print("  app.data-path was lost")
    sys.exit(1)
PY
    then
        echo "  FAIL ${name}: invalid JSON or wrong rocksdb.total_memory_size"
        FAIL=$((FAIL + 1))
        return
    fi
    echo "  PASS ${name}"
    PASS=$((PASS + 1))
}

echo "Store docker-entrypoint rocksdb memory override"
expect_memory "unset leaves the key out" absent
expect_memory "empty leaves the key out" absent HG_STORE_ROCKSDB_TOTAL_MEMORY_SIZE=
expect_memory "bytes are passed through" 2147483648 \
              HG_STORE_ROCKSDB_TOTAL_MEMORY_SIZE=2147483648
expect_memory "auto is passed through" auto HG_STORE_ROCKSDB_TOTAL_MEMORY_SIZE=auto
expect_memory "legacy 0 is passed through for auto-sizing" 0 HG_STORE_ROCKSDB_TOTAL_MEMORY_SIZE=0

for bad in '4GB' '-1' '1.5e9' '1 2' '1"}' 'automatic'; do
    if run_entrypoint HG_STORE_ROCKSDB_TOTAL_MEMORY_SIZE="${bad}" >/dev/null; then
        echo "  FAIL '${bad}' was accepted"
        FAIL=$((FAIL + 1))
    else
        echo "  PASS '${bad}' is refused"
        PASS=$((PASS + 1))
    fi
done

echo "${PASS} passed, ${FAIL} failed"
[[ "${FAIL}" -eq 0 ]]
