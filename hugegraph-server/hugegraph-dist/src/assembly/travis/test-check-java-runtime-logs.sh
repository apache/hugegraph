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

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CHECKER="$SCRIPT_DIR/check-java-runtime-logs.sh"
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/java-runtime-logs.XXXXXX")
trap 'rm -rf "$WORK_DIR"' EXIT

check_case() {
    local expected="$1" status=0
    shift
    "$CHECKER" "$@" >"$WORK_DIR/result" 2>&1 || status=$?
    if [[ $status -ne $expected ]]; then
        cat "$WORK_DIR/result" >&2
        echo "FAIL: expected $expected, got $status for $*" >&2
        exit 1
    fi
}

printf '%s\n' '[ERROR] Expected validation error: invalid input' \
    'java.lang.IllegalAccessException: expected business access failure' \
    'InaccessibleObjectException is a class name in this message' >"$WORK_DIR/normal.log"
check_case 0 "$WORK_DIR/normal.log"
touch "$WORK_DIR/empty.log"
check_case 0 "$WORK_DIR/empty.log"

for MESSAGE in \
    'java.lang.reflect.InaccessibleObjectException: module java.base does not "opens java.lang"' \
    'java.lang.IllegalAccessError: class A cannot access class B because module java.base does not export p' \
    'java.lang.IllegalAccessException: module java.base does not export p to unnamed module' \
    "Unrecognized VM option 'OldGCOption'" \
    'Unrecognized option: --unknown-option' \
    'OpenJDK warning: Ignoring option --illegal-access=permit; support was removed in 17.0' \
    'Unable to inspect cluster runtime logs: Node still running or startup log unavailable'; do
    # A child returning success after logging only to stderr reproduces the
    # failure mode that result assertions and exit-code checks cannot catch.
    bash -c 'printf "%s\n" "$1" >&2' bash "$MESSAGE" >"$WORK_DIR/positive.log" 2>&1
    check_case 1 "$WORK_DIR/positive.log"
    grep -q '^1:' "$WORK_DIR/result"
done

printf '%s\n' 'java.lang.NoClassDefFoundError: intentionally missing test dependency' >"$WORK_DIR/native.log"
check_case 0 "$WORK_DIR/native.log"
check_case 1 --native "$WORK_DIR/native.log"
printf '%s\n' 'undefined symbol: rocksdb_open' >"$WORK_DIR/native.log"
check_case 1 --native "$WORK_DIR/native.log"

mkdir -p "$WORK_DIR/logs with spaces/nested" "$WORK_DIR/no-logs"
cp "$WORK_DIR/normal.log" "$WORK_DIR/logs with spaces/application.log"
printf '%s\n' 'java.lang.reflect.InaccessibleObjectException: module java.base does not opens p' \
    >"$WORK_DIR/logs with spaces/nested/application.log.1"
check_case 1 "$WORK_DIR/logs with spaces"
rm "$WORK_DIR/logs with spaces/nested/application.log.1"
for SUFFIX in hprof log.gz log.zip log.bz2 log.xz; do
    cp "$WORK_DIR/positive.log" "$WORK_DIR/logs with spaces/heap.$SUFFIX"
done
check_case 0 "$WORK_DIR/logs with spaces"
# NUL bytes from logged keys must not hide a denied-access line elsewhere in
# the same text log. Keep the NUL-free control and explicit/directory inputs.
printf '%s\n' 'java.lang.reflect.InaccessibleObjectException: module java.base does not opens p' \
    >"$WORK_DIR/logs with spaces/binary.log"
check_case 1 "$WORK_DIR/logs with spaces/binary.log"
printf 'INFO key=\0\0\1 written\n' >>"$WORK_DIR/logs with spaces/binary.log"
check_case 1 "$WORK_DIR/logs with spaces/binary.log"
grep -q '^1:' "$WORK_DIR/result"
check_case 1 "$WORK_DIR/logs with spaces"
printf '\0java.lang.reflect.InaccessibleObjectException:\0' >"$WORK_DIR/logs with spaces/binary.log"
check_case 1 "$WORK_DIR/logs with spaces"
printf 'INFO key=\0\0\1 written\n' >"$WORK_DIR/logs with spaces/binary.log"
check_case 0 "$WORK_DIR/logs with spaces/binary.log"
check_case 0 "$WORK_DIR/logs with spaces"
check_case 2 "$WORK_DIR/no-logs"
check_case 2 "$WORK_DIR/missing.log"
check_case 2
echo 'PASS: Java runtime log checks reject compatibility errors without rejecting business failures'
