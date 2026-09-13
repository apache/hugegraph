#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
set -euo pipefail

compose_file=${COMPOSE_FILE:-docker/docker-compose-3pd-3store-3server.yml}
project=${COMPOSE_PROJECT_NAME:-hugegraph-3x3}
compose=(docker compose -p "$project" -f "$compose_file")
base_server=${BASE_SERVER_URL:-http://127.0.0.1:8080}
base_pd=${BASE_PD_URL:-http://127.0.0.1:8620}

curl_local() { env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY NO_PROXY=localhost,127.0.0.1,::1 no_proxy=localhost,127.0.0.1,::1 curl --noproxy '*' "$@"; }
health() { curl_local -fsS --max-time 10 "$1/v1/health" >/dev/null; }

"${compose[@]}" up -d pd0 pd1 pd2 store0 store1 store2 server0 server1 server2
for port in 8620 8621 8622; do health "http://127.0.0.1:$port"; done
for port in 8520 8521 8522; do health "http://127.0.0.1:$port"; done
for port in 8080 8081 8082; do curl_local -fsS --max-time 10 "http://127.0.0.1:$port/versions" >/dev/null; done

fixture_id="rust-gate-$(date +%Y%m%d%H%M%S%N)"
property_key="rust_gate_pk_${fixture_id//[^a-zA-Z0-9]/}"
vertex_label="rust_gate_vl_${fixture_id//[^a-zA-Z0-9]/}"
json_header='Content-Type: application/json'
post_checked() {
  local url=$1 data=$2 body code
  body=$(mktemp)
  code=$(curl_local -sS -o "$body" -w '%{http_code}' -X POST "$url" -H "$json_header" -d "$data")
  cat "$body"
  echo "HTTP $code" >&2
  [[ "$code" =~ ^2[0-9][0-9]$ ]] || return 1
}
post_checked "$base_server/graphs/hugegraph/schema/propertykeys" "{\"name\":\"$property_key\",\"data_type\":\"TEXT\",\"cardinality\":\"SINGLE\",\"properties\":[]}" >/dev/null
sleep 10
post_checked "$base_server/graphs/hugegraph/schema/vertexlabels" "{\"name\":\"$vertex_label\",\"id_strategy\":\"CUSTOMIZE_STRING\",\"properties\":[\"$property_key\"],\"primary_keys\":[],\"nullable_keys\":[]}" >/dev/null
sleep 10
post_checked "$base_server/graphs/hugegraph/graph/vertices" "{\"id\":\"$fixture_id\",\"label\":\"$vertex_label\",\"properties\":{\"$property_key\":\"committed\"}}"

"${compose[@]}" stop pd0 >/dev/null
sleep 8
health "$base_pd" || true
health http://127.0.0.1:8621
health http://127.0.0.1:8622
"${compose[@]}" start pd0 >/dev/null
sleep 15
for port in 8620 8621 8622; do health "http://127.0.0.1:$port"; done

"${compose[@]}" stop store0 >/dev/null
sleep 20
# A stopped store may legitimately block requests whose shard leader is it.
# Recovery is asserted after the process is restarted and the raft group settles.
"${compose[@]}" start store0 >/dev/null
sleep 20
curl_local -fsS --max-time 10 "$base_server/graphs/hugegraph/graph/vertices/%22$fixture_id%22" | grep -q 'committed'

echo "cluster smoke passed fixture=$fixture_id"
