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

if [[ $# -ne 3 || ( "$2" != "create" && "$2" != "verify" ) ]]; then
    echo "USAGE: $0 SERVER_URL create|verify RUN_ID" >&2
    exit 1
fi

SERVER_URL=${1%/}
MODE=$2
RUN_ID=$3

if [[ ! "$RUN_ID" =~ ^[a-zA-Z0-9_]+$ ]]; then
    echo "RUN_ID must contain only letters, numbers, and underscores" >&2
    exit 1
fi

GRAPH_PATH=/graphspaces/DEFAULT/graphs/hugegraph
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/hugegraph-server-smoke.XXXXXX")
RESPONSE_FILE="$WORK_DIR/response.json"
PROPERTY_KEY="riscv_smoke_name_$RUN_ID"
VERTEX_LABEL="riscv_smoke_node_$RUN_ID"
EDGE_LABEL="riscv_smoke_link_$RUN_ID"
VERTEX_ONE="riscv-smoke-v1-$RUN_ID"
VERTEX_TWO="riscv-smoke-v2-$RUN_ID"
CURL_CONNECT_TIMEOUT=3
CURL_PROBE_TIMEOUT=5
CURL_REQUEST_TIMEOUT=60
SERVER_READY_TIMEOUT=240

cleanup() {
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

for command in curl jq; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "Required command is unavailable: $command" >&2
        exit 1
    fi
done

if [[ -n "${HUGEGRAPH_USERNAME:-}" || -n "${HUGEGRAPH_PASSWORD:-}" ]]; then
    if [[ -z "${HUGEGRAPH_USERNAME:-}" || -z "${HUGEGRAPH_PASSWORD:-}" ]]; then
        echo "Set both HUGEGRAPH_USERNAME and HUGEGRAPH_PASSWORD" >&2
        exit 1
    fi
fi

curl_request() {
    if [[ -n "${HUGEGRAPH_USERNAME:-}" ]]; then
        curl --compressed --user "$HUGEGRAPH_USERNAME:$HUGEGRAPH_PASSWORD" "$@"
    else
        curl --compressed "$@"
    fi
}

wait_for_server() {
    local deadline=$((SECONDS + SERVER_READY_TIMEOUT))
    while ((SECONDS < deadline)); do
        if curl_request --silent --show-error --fail \
                        --connect-timeout "$CURL_CONNECT_TIMEOUT" \
                        --max-time "$CURL_PROBE_TIMEOUT" \
                        "$SERVER_URL/versions" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "HugeGraph did not become ready at $SERVER_URL" >&2
    return 1
}

request() {
    local method=$1
    local path=$2
    local expected_status=$3
    local body=${4:-}
    local status
    local curl_args=(--silent --show-error --output "$RESPONSE_FILE" \
                     --write-out '%{http_code}' --request "$method" \
                     --connect-timeout "$CURL_CONNECT_TIMEOUT" \
                     --max-time "$CURL_REQUEST_TIMEOUT" \
                     --header 'Content-Type: application/json')

    if [[ -n "$body" ]]; then
        curl_args+=(--data "$body")
    fi
    status=$(curl_request "${curl_args[@]}" "$SERVER_URL$path")
    if [[ "$status" != "$expected_status" ]]; then
        echo "$method $path returned HTTP $status, expected $expected_status" >&2
        cat "$RESPONSE_FILE" >&2
        return 1
    fi
}

assert_json() {
    local expression=$1
    shift
    if ! jq --exit-status "$@" "$expression" "$RESPONSE_FILE" >/dev/null; then
        echo "JSON assertion failed: $expression" >&2
        cat "$RESPONSE_FILE" >&2
        return 1
    fi
}

verify_graph() {
    request GET "$GRAPH_PATH/graph/vertices/%22$VERTEX_ONE%22" 200
    assert_json '.id == $id and .label == $label and .properties[$key] == "first"' \
                --arg id "$VERTEX_ONE" --arg label "$VERTEX_LABEL" \
                --arg key "$PROPERTY_KEY"

    request GET "$GRAPH_PATH/graph/vertices/%22$VERTEX_TWO%22" 200
    assert_json '.id == $id and .label == $label and .properties[$key] == "second"' \
                --arg id "$VERTEX_TWO" --arg label "$VERTEX_LABEL" \
                --arg key "$PROPERTY_KEY"

    request GET "$GRAPH_PATH/graph/edges" 200
    assert_json '.edges | any(.label == $label and .outV == $out and .inV == $in)' \
                --arg label "$EDGE_LABEL" --arg out "$VERTEX_ONE" \
                --arg in "$VERTEX_TWO"

    request POST /gremlin 200 \
        "$(jq -cn --arg query "g.V().hasLabel('$VERTEX_LABEL').count()" \
             '{gremlin:$query, bindings:{}, language:"gremlin-groovy",
               aliases:{g:"__g_DEFAULT-hugegraph"}}')"
    assert_json '.result.data == [2]'
}

wait_for_server
request GET /versions 200
assert_json 'type == "object" and length > 0'

if [[ "$MODE" == "create" ]]; then
    request POST "$GRAPH_PATH/schema/propertykeys" 202 \
        "$(jq -cn --arg name "$PROPERTY_KEY" \
             '{name:$name, data_type:"TEXT", cardinality:"SINGLE", properties:[]}')"
    assert_json '.property_key.name == $name and .task_id == 0' \
                --arg name "$PROPERTY_KEY"

    request POST "$GRAPH_PATH/schema/vertexlabels" 201 \
        "$(jq -cn --arg name "$VERTEX_LABEL" --arg key "$PROPERTY_KEY" \
             '{name:$name, id_strategy:"CUSTOMIZE_STRING", properties:[$key],
               primary_keys:[], nullable_keys:[]}')"
    assert_json '.name == $name' --arg name "$VERTEX_LABEL"

    request POST "$GRAPH_PATH/schema/edgelabels" 201 \
        "$(jq -cn --arg name "$EDGE_LABEL" --arg label "$VERTEX_LABEL" \
             '{name:$name, source_label:$label, target_label:$label,
               frequency:"SINGLE", properties:[], sort_keys:[], nullable_keys:[]}')"
    assert_json '.name == $name' --arg name "$EDGE_LABEL"

    request POST "$GRAPH_PATH/graph/vertices" 201 \
        "$(jq -cn --arg id "$VERTEX_ONE" --arg label "$VERTEX_LABEL" \
             --arg key "$PROPERTY_KEY" \
             '{id:$id, label:$label, properties:{($key):"first"}}')"
    assert_json '.id == $id' --arg id "$VERTEX_ONE"

    request POST "$GRAPH_PATH/graph/vertices" 201 \
        "$(jq -cn --arg id "$VERTEX_TWO" --arg label "$VERTEX_LABEL" \
             --arg key "$PROPERTY_KEY" \
             '{id:$id, label:$label, properties:{($key):"second"}}')"
    assert_json '.id == $id' --arg id "$VERTEX_TWO"

    request POST "$GRAPH_PATH/graph/edges" 201 \
        "$(jq -cn --arg label "$EDGE_LABEL" --arg vertexLabel "$VERTEX_LABEL" \
             --arg out "$VERTEX_ONE" --arg in "$VERTEX_TWO" \
             '{label:$label, outVLabel:$vertexLabel, inVLabel:$vertexLabel,
               outV:$out, inV:$in, properties:{}}')"
    assert_json '.label == $label and .outV == $out and .inV == $in' \
                --arg label "$EDGE_LABEL" --arg out "$VERTEX_ONE" \
                --arg in "$VERTEX_TWO"
fi

verify_graph
echo "server-e2e-smoke-$MODE-ok"
