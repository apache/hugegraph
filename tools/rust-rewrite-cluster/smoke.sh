#!/usr/bin/env bash
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

fixture_id="rust-gate-$(date +%Y%m%d%H%M%S)"
property_key="rust_gate_pk_${fixture_id//[^a-zA-Z0-9]/}"
vertex_label="rust_gate_vl_${fixture_id//[^a-zA-Z0-9]/}"
json_header='Content-Type: application/json'
curl_local -fsS -X POST "$base_server/graphs/hugegraph/schema/propertykeys" -H "$json_header" -d "{\"name\":\"$property_key\",\"data_type\":\"TEXT\",\"cardinality\":\"SINGLE\",\"properties\":[]}" >/dev/null
curl_local -fsS -X POST "$base_server/graphs/hugegraph/schema/vertexlabels" -H "$json_header" -d "{\"name\":\"$vertex_label\",\"id_strategy\":\"CUSTOMIZE_STRING\",\"properties\":[\"$property_key\"],\"primary_keys\":[],\"nullable_keys\":[]}" >/dev/null
curl_local -fsS -X POST "$base_server/graphs/hugegraph/graph/vertices" -H "$json_header" -d "{\"id\":\"$fixture_id\",\"label\":\"$vertex_label\",\"properties\":{\"$property_key\":\"committed\"}}" >/dev/null

"${compose[@]}" stop pd0 >/dev/null
sleep 8
health "$base_pd" || true
health http://127.0.0.1:8621
health http://127.0.0.1:8622
"${compose[@]}" start pd0 >/dev/null
sleep 15
for port in 8620 8621 8622; do health "http://127.0.0.1:$port"; done

"${compose[@]}" stop store0 >/dev/null
sleep 12
for port in 8081 8082; do curl_local -fsS --max-time 10 "http://127.0.0.1:$port/graphs/hugegraph/graph/vertices/%22$fixture_id%22" | grep -q 'committed'; done
"${compose[@]}" start store0 >/dev/null
sleep 20
curl_local -fsS --max-time 10 "$base_server/graphs/hugegraph/graph/vertices/%22$fixture_id%22" | grep -q 'committed'

echo "cluster smoke passed fixture=$fixture_id"
