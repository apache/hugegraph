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
# Verify stop failure propagation and PID retention without signalling real processes.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../.." && pwd)"
SCRIPT="$ROOT/hugegraph-store/hg-store-dist/src/assembly/static/bin/stop-hugegraph-store.sh"
FIXTURE=$(mktemp -d)
trap 'rm -rf "$FIXTURE"' EXIT
mkdir -p "$FIXTURE/bin"
cp "$SCRIPT" "$FIXTURE/bin/stop-hugegraph-store.sh"
cat > "$FIXTURE/bin/util.sh" <<'UTIL'
kill_process_and_wait() {
    [[ "$1" == HugeGraphStoreServer && "$2" == 12345 && "$3" == 30 ]] || return 99
    return "$STOP_RESULT"
}
UTIL
printf '12345\n' > "$FIXTURE/bin/pid"
if STOP_RESULT=1 bash "$FIXTURE/bin/stop-hugegraph-store.sh"; then
    echo "Stop timeout incorrectly returned success" >&2
    exit 1
fi
[[ "$(cat "$FIXTURE/bin/pid")" == 12345 ]]
STOP_RESULT=0 bash "$FIXTURE/bin/stop-hugegraph-store.sh"
[[ ! -e "$FIXTURE/bin/pid" ]]
STOP_RESULT=0 bash "$FIXTURE/bin/stop-hugegraph-store.sh"
echo "store-stop-contract-ok"
