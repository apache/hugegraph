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

# Prerequisite: create/populate the cloud-storage environment first via
# test-graph-queries-and-sst.sh.

set -euo pipefail

GRAPH_NAME="${GRAPH_NAME:-hugegraph}"
MC_IMAGE="${MC_IMAGE:-minio/mc:RELEASE.2025-08-13T08-35-41Z}"
MINIO_USER="${MINIO_USER:-minioadmin}"
MINIO_PASS="${MINIO_PASS:-minioadmin}"
RESTART_WAIT_SECONDS="${RESTART_WAIT_SECONDS:-20}"

BUCKETS=(hugegraph-store0 hugegraph-store1 hugegraph-store2)

log() {
    printf '[advanced-clear] %s\n' "$*"
}

die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

need_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

resolve_hg_net() {
    local net

    net=$(docker inspect cloud-storage-minio \
      --format '{{range $k, $v := .NetworkSettings.Networks}}{{println $k}}{{end}}' \
      2>/dev/null | head -n1 || true)

    if [[ -z "$net" ]]; then
        if docker network inspect cloud-storage-test_hg-net >/dev/null 2>&1; then
            net="cloud-storage-test_hg-net"
        elif docker network inspect cloud-storage-net >/dev/null 2>&1; then
            net="cloud-storage-net"
        fi
    fi

    [[ -n "$net" ]] || die "no cloud-storage Docker network found"
    export HG_NET="$net"
    log "Using Docker network: $HG_NET"
}

mc_exec() {
    local cmd="$1"

    docker run --rm --entrypoint /bin/sh --network "$HG_NET" "$MC_IMAGE" -c "
if mc alias set local http://minio:9000 $MINIO_USER $MINIO_PASS >/dev/null 2>&1; then
  :
elif mc alias set local http://cloud-storage-minio:9000 $MINIO_USER $MINIO_PASS >/dev/null 2>&1; then
  :
else
  echo 'ERROR: cannot reach MinIO at minio:9000 or cloud-storage-minio:9000' >&2
  exit 1
fi
$cmd
"
}

vertex_count() {
    curl -s --compressed "http://localhost:8080/graphs/${GRAPH_NAME}/graph/vertices" \
        | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('vertices',[])))"
}

main() {
    local bucket db_prefix candidate_bucket match
    local before_count after_count immediate_count after_restart_count

    need_cmd docker
    need_cmd curl
    need_cmd grep
    need_cmd head
    need_cmd python3

    resolve_hg_net

    bucket=""
    db_prefix=""

    # Detect one graph DB prefix from CURRENT files in any store bucket.
    for candidate_bucket in "${BUCKETS[@]}"; do
        match=$(mc_exec "mc find local/${candidate_bucket} --name CURRENT 2>/dev/null" \
            | grep "/${GRAPH_NAME}/" | head -n1 || true)
        if [[ -n "$match" ]]; then
            bucket="$candidate_bucket"
            db_prefix="${match#local/${candidate_bucket}/}"
            db_prefix="${db_prefix%/CURRENT}"
            break
        fi
    done

    [[ -n "$bucket" && -n "$db_prefix" ]] \
        || die "failed to detect DB prefix for graph '${GRAPH_NAME}' in any bucket"

    log "Detected DB prefix: bucket=${bucket}, prefix=${db_prefix}"

    before_count=$(mc_exec "mc ls --recursive local/${bucket}/${db_prefix} 2>/dev/null | wc -l")
    before_count="${before_count//[^0-9]/}"
    [[ -n "$before_count" ]] || before_count="0"
    log "Objects before clear: ${before_count}"

    mc_exec "mc ls --recursive local/${bucket}/${db_prefix} 2>/dev/null | head -20" || true

    if [[ "$before_count" == "0" ]]; then
        die "objects before clear is 0; load data first (Core Steps 3-5)"
    fi

    log "Calling clear API for graph '${GRAPH_NAME}'"
    curl -s -X DELETE "http://localhost:8080/graphs/${GRAPH_NAME}/clear" \
      --get --data-urlencode "confirm_message=I'm sure to delete all data" \
      | head -c 200
    echo

    sleep 5

    after_count=$(mc_exec "mc ls --recursive local/${bucket}/${db_prefix} 2>/dev/null | wc -l")
    after_count="${after_count//[^0-9]/}"
    [[ -n "$after_count" ]] || after_count="0"
    log "Objects after clear (expected usually non-zero): ${after_count}"

    mc_exec "mc ls --recursive local/${bucket}/${db_prefix} 2>/dev/null | head -20" || true

    immediate_count=$(vertex_count)
    log "Vertex count immediately after clear: ${immediate_count}"
    [[ "$immediate_count" == "0" ]] \
        || die "vertex count after clear is ${immediate_count}, expected 0"

    log "Restarting stores for no-orphan re-hydration check"
    docker restart cloud-storage-store0 cloud-storage-store1 cloud-storage-store2 >/dev/null
    sleep "$RESTART_WAIT_SECONDS"

    after_restart_count=$(vertex_count)
    log "Vertex count after restart: ${after_restart_count}"
    [[ "$after_restart_count" == "0" ]] \
        || die "vertex count after restart is ${after_restart_count}, expected 0"

    log "PASS: no orphan re-hydration observed"
}

main "$@"


