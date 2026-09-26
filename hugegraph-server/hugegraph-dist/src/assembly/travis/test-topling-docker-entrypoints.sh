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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR"/../../../../.. && pwd)"
TEST_ROOT="$(mktemp -d)"

cleanup() {
    rm -rf "$TEST_ROOT"
}
trap cleanup EXIT

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

prepare_fixture() {
    local component="$1"
    local entrypoint="$2"
    local start_script="$3"
    local fixture="$TEST_ROOT/$component"

    mkdir -p "$fixture/bin"
    cp "$entrypoint" "$fixture/docker-entrypoint.sh"
    cp "$PROJECT_ROOT/hugegraph-server/hugegraph-dist/src/assembly/static/bin/verify-rocksdb-provider.sh" \
       "$fixture/bin/verify-rocksdb-provider.sh"
    # shellcheck disable=SC2016
    printf '%s\n' \
        '#!/bin/bash' \
        'set -euo pipefail' \
        'printf "%s\n" "$TOPLINGDB_ROCKSDB_PROVIDER" > "$ENTRYPOINT_CAPTURE.provider"' \
        'printf "%s\n" "$SPRING_APPLICATION_JSON" > "$ENTRYPOINT_CAPTURE.json"' \
        > "$fixture/bin/$start_script"
    chmod +x "$fixture/docker-entrypoint.sh" \
             "$fixture/bin/$start_script" \
             "$fixture/bin/verify-rocksdb-provider.sh"
}

run_pd() {
    local provider="$1"
    local capture="$TEST_ROOT/pd-$provider"
    local data_path="$TEST_ROOT/pd/pd_data"

    if [ "$provider" = "topling" ]; then
        data_path="$TEST_ROOT/pd/topling-pd-data"
    fi
    mkdir -p "$data_path"
    (
        cd "$TEST_ROOT/pd"
        ENTRYPOINT_CAPTURE="$capture" \
        HG_PD_AUTH_SECRET_KEY=ci-topling-pd-secret \
        HG_PD_GRPC_HOST=pd \
        HG_PD_RAFT_ADDRESS=pd:8610 \
        HG_PD_RAFT_PEERS_LIST=pd:8610 \
        HG_PD_INITIAL_STORE_LIST=store:8500 \
        HG_PD_ROCKSDB_PROVIDER="$provider" \
        HG_PD_ENFORCE_PROVIDER_MARKER=true \
            ./docker-entrypoint.sh >/dev/null
    )
    grep -qx "$provider" "$capture.provider" ||
        fail "PD preload override does not use $provider"
    grep -Fq "\"rocksdb\": { \"provider\": \"$provider\" }" \
        "$capture.json" ||
        fail "PD Spring configuration does not use $provider"
    grep -Fq "\"data-path\":          \"$data_path\"" "$capture.json" ||
        fail "PD default data path does not follow $provider"
    grep -qx "provider=$provider" \
        "$data_path/.hugegraph-rocksdb-provider" ||
        fail "PD data path has no $provider marker"
}

run_store() {
    local provider="$1"
    local capture="$TEST_ROOT/store-$provider"
    local data_path="$TEST_ROOT/store/storage"

    if [ "$provider" = "topling" ]; then
        data_path="$TEST_ROOT/store/topling-storage"
    fi
    mkdir -p "$data_path"
    (
        cd "$TEST_ROOT/store"
        ENTRYPOINT_CAPTURE="$capture" \
        HG_STORE_PD_ADDRESS=pd:8686 \
        HG_STORE_GRPC_HOST=store \
        HG_STORE_RAFT_ADDRESS=store:8510 \
        HG_STORE_ROCKSDB_PROVIDER="$provider" \
        HG_STORE_ENFORCE_PROVIDER_MARKER=true \
            ./docker-entrypoint.sh >/dev/null
    )
    grep -qx "$provider" "$capture.provider" ||
        fail "Store preload override does not use $provider"
    grep -Fq "\"rocksdb\":  { \"provider\": \"$provider\" }" \
        "$capture.json" ||
        fail "Store Spring configuration does not use $provider"
    grep -Fq "\"data-path\": \"$data_path\"" "$capture.json" ||
        fail "Store default data path does not follow $provider"
    grep -qx "provider=$provider" \
        "$data_path/.hugegraph-rocksdb-provider" ||
        fail "Store data path has no $provider marker"
}

expect_invalid_provider() {
    local component="$1"
    local output

    if [ "$component" = "pd" ]; then
        if output=$(
            cd "$TEST_ROOT/pd"
            HG_PD_AUTH_SECRET_KEY=ci-topling-pd-secret \
            HG_PD_GRPC_HOST=pd \
            HG_PD_RAFT_ADDRESS=pd:8610 \
            HG_PD_RAFT_PEERS_LIST=pd:8610 \
            HG_PD_INITIAL_STORE_LIST=store:8500 \
            HG_PD_ROCKSDB_PROVIDER=invalid \
                ./docker-entrypoint.sh 2>&1
        ); then
            fail "PD accepted an invalid provider"
        fi
        grep -Fq "HG_PD_ROCKSDB_PROVIDER must be rocksdb or topling" \
            <<<"$output" || fail "PD invalid-provider error is not actionable"
    else
        if output=$(
            cd "$TEST_ROOT/store"
            HG_STORE_PD_ADDRESS=pd:8686 \
            HG_STORE_GRPC_HOST=store \
            HG_STORE_RAFT_ADDRESS=store:8510 \
            HG_STORE_ROCKSDB_PROVIDER=invalid \
                ./docker-entrypoint.sh 2>&1
        ); then
            fail "Store accepted an invalid provider"
        fi
        grep -Fq "HG_STORE_ROCKSDB_PROVIDER must be rocksdb or topling" \
            <<<"$output" || fail "Store invalid-provider error is not actionable"
    fi
}

prepare_fixture \
    pd \
    "$PROJECT_ROOT/hugegraph-pd/hg-pd-dist/docker/docker-entrypoint.sh" \
    start-hugegraph-pd.sh
prepare_fixture \
    store \
    "$PROJECT_ROOT/hugegraph-store/hg-store-dist/docker/docker-entrypoint.sh" \
    start-hugegraph-store.sh

for provider in rocksdb topling; do
    run_pd "$provider"
    run_store "$provider"
done
expect_invalid_provider pd
expect_invalid_provider store

SHARED_DATA="$TEST_ROOT/shared-data"
mkdir -p "$SHARED_DATA"
(
    cd "$TEST_ROOT/pd"
    ENTRYPOINT_CAPTURE="$TEST_ROOT/pd-shared-topling" \
    HG_PD_AUTH_SECRET_KEY=ci-topling-pd-secret \
    HG_PD_GRPC_HOST=pd \
    HG_PD_RAFT_ADDRESS=pd:8610 \
    HG_PD_RAFT_PEERS_LIST=pd:8610 \
    HG_PD_INITIAL_STORE_LIST=store:8500 \
    HG_PD_ROCKSDB_PROVIDER=topling \
    HG_PD_DATA_PATH="$SHARED_DATA" \
        ./docker-entrypoint.sh >/dev/null
)
if output=$(
    cd "$TEST_ROOT/pd"
    ENTRYPOINT_CAPTURE="$TEST_ROOT/pd-shared-rocksdb" \
    HG_PD_AUTH_SECRET_KEY=ci-topling-pd-secret \
    HG_PD_GRPC_HOST=pd \
    HG_PD_RAFT_ADDRESS=pd:8610 \
    HG_PD_RAFT_PEERS_LIST=pd:8610 \
    HG_PD_INITIAL_STORE_LIST=store:8500 \
    HG_PD_ROCKSDB_PROVIDER=rocksdb \
    HG_PD_DATA_PATH="$SHARED_DATA" \
        ./docker-entrypoint.sh 2>&1
); then
    fail "PD accepted a Topling data path for standard RocksDB"
fi
grep -Fq "provider marker mismatch" <<<"$output" ||
    fail "PD provider mismatch error is not actionable"
grep -qx "provider=topling" \
    "$SHARED_DATA/.hugegraph-rocksdb-provider" ||
    fail "provider mismatch changed the existing data marker"

NONEMPTY_DATA="$TEST_ROOT/nonempty-data"
mkdir -p "$NONEMPTY_DATA"
touch "$NONEMPTY_DATA/existing.sst"
PROVIDER_HELPER="$PROJECT_ROOT/hugegraph-server/hugegraph-dist/src/assembly/static/bin/verify-rocksdb-provider.sh"
if "$PROVIDER_HELPER" \
       store topling "$NONEMPTY_DATA" false >/dev/null 2>&1; then
    fail "ToplingDB accepted a non-empty unmarked data path"
fi
[ -f "$NONEMPTY_DATA/existing.sst" ] ||
    fail "failed provider validation removed existing data"
[ ! -e "$NONEMPTY_DATA/.hugegraph-rocksdb-provider" ] ||
    fail "failed provider validation created a marker"

OUTSIDE_DATA="$TEST_ROOT/outside-data"
mkdir -p "$OUTSIDE_DATA"
ln -s "$OUTSIDE_DATA" "$TEST_ROOT/data-link"
if "$PROVIDER_HELPER" \
       server topling "$TEST_ROOT/data-link/new" true >/dev/null 2>&1; then
    fail "ToplingDB accepted a data path with a symlink prefix"
fi
[ ! -e "$OUTSIDE_DATA/new" ] ||
    fail "provider validation created a directory through a symlink prefix"

CONCURRENT_DATA="$TEST_ROOT/concurrent-data"
mkdir -p "$CONCURRENT_DATA"
"$PROVIDER_HELPER" server topling "$CONCURRENT_DATA" true >/dev/null &
FIRST_PID=$!
"$PROVIDER_HELPER" server topling "$CONCURRENT_DATA" true >/dev/null &
SECOND_PID=$!
wait "$FIRST_PID" ||
    fail "first concurrent provider-marker initialization failed"
wait "$SECOND_PID" ||
    fail "second concurrent provider-marker initialization failed"
grep -qx "provider=topling" \
    "$CONCURRENT_DATA/.hugegraph-rocksdb-provider" ||
    fail "concurrent provider-marker initialization wrote the wrong marker"

echo "PASS: PD and Store Docker entrypoints keep Spring and preload providers aligned"
