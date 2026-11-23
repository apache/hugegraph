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

set -euo pipefail

: "${HG_PD_GRPC_HOST:=pd}"
: "${HG_PD_GRPC_PORT:=8686}"
: "${HG_PD_REST_PORT:=8620}"
: "${HG_PD_RAFT_ADDRESS:=pd:8610}"
: "${HG_PD_RAFT_PEERS_LIST:=pd:8610}"
: "${HG_PD_DATA_PATH:=/hugegraph-pd/pd_data}"
: "${HG_PD_INITIAL_STORE_LIST:=store0:8500,store1:8500,store2:8500}"
: "${HG_PD_INITIAL_STORE_COUNT:=3}"

json_escape() {
    local s="$1"
    s=${s//\\/\\\\}
    s=${s//\"/\\\"}
    s=${s//$'\n'/}
    printf "%s" "$s"
}

export SPRING_APPLICATION_JSON="$(cat <<JSON
{
  "grpc": { "host": "$(json_escape "${HG_PD_GRPC_HOST}")", "port": "$(json_escape "${HG_PD_GRPC_PORT}")" },
  "server": { "port": "$(json_escape "${HG_PD_REST_PORT}")" },
  "raft": {
    "address": "$(json_escape "${HG_PD_RAFT_ADDRESS}")",
    "peers-list": "$(json_escape "${HG_PD_RAFT_PEERS_LIST}")"
  },
  "pd": {
    "initial-store-list": "$(json_escape "${HG_PD_INITIAL_STORE_LIST}")",
    "initial-store-count": "$(json_escape "${HG_PD_INITIAL_STORE_COUNT}")"
  },
  "app": { "data-path": "$(json_escape "${HG_PD_DATA_PATH}")" }
}
JSON
)"

exec ./bin/start-hugegraph-pd.sh -d false -j "${JAVA_OPTS:-}"

