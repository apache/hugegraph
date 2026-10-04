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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UTILS="${1:-${SCRIPT_DIR}/ci-service-utils.sh}"
ACTIVE_PID=""

cleanup() {
    if [[ -n "${ACTIVE_PID}" ]]; then
        kill "${ACTIVE_PID}" 2>/dev/null || true
        wait "${ACTIVE_PID}" 2>/dev/null || true
    fi
}
trap cleanup EXIT

source "${UTILS}"

if ! declare -F process_is_running >/dev/null ||
   ! declare -F wait_for_process_exit >/dev/null; then
    echo "FAIL: process exit helpers are not available"
    exit 1
fi

sleep 10 &
ACTIVE_PID=$!
if wait_for_process_exit "${ACTIVE_PID}" 1; then
    echo "FAIL: a running process was reported as exited"
    exit 1
fi
kill "${ACTIVE_PID}" 2>/dev/null || true
wait "${ACTIVE_PID}" 2>/dev/null || true
ACTIVE_PID=""

sleep 1 &
ACTIVE_PID=$!
if ! wait_for_process_exit "${ACTIVE_PID}" 5; then
    echo "FAIL: a terminated process was reported as running"
    exit 1
fi
wait "${ACTIVE_PID}" 2>/dev/null || true
ACTIVE_PID=""

ps() {
    echo "Z"
}
if process_is_running "$$"; then
    echo "FAIL: a zombie process was reported as running"
    exit 1
fi
unset -f ps

echo "PASS: process exit helpers handle running, terminated, and zombie states"
