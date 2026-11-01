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

: "${STORE_REST:?STORE_REST not set}"

CHECK_PID=""
stop_check() {
    local exit_code="$1"
    trap '' TERM INT
    if [[ -n "$CHECK_PID" ]]; then
        # GNU timeout forwards TERM to the command group it owns, including
        # the polling shell and its current curl/sleep, then reaps the shell.
        kill -TERM "$CHECK_PID" 2>/dev/null || true
        wait "$CHECK_PID" 2>/dev/null || true
    fi
    exit "$exit_code"
}
trap 'stop_check 143' TERM
trap 'stop_check 130' INT

timeout "${WAIT_PARTITION_TIMEOUT_S:-120}s" bash -c '
until curl -fsS "http://${STORE_REST}" 2>/dev/null | \
      grep -q "\"partitionCount\":[1-9]"
do
    echo "Waiting for partition assignment..."
    sleep 5
done
' &
CHECK_PID=$!
STATUS=0
wait "$CHECK_PID" || STATUS=$?
CHECK_PID=""
[[ "$STATUS" -eq 0 ]] || exit "$STATUS"

echo "Partitions detected:"
URL="http://${STORE_REST}/v1/partitions"
echo "$URL"
# The final diagnostic needs its own bound after partition polling succeeds.
timeout "${WAIT_PARTITION_TIMEOUT_S:-120}s" curl -v "$URL" &
CHECK_PID=$!
STATUS=0
wait "$CHECK_PID" || STATUS=$?
exit "$STATUS"
