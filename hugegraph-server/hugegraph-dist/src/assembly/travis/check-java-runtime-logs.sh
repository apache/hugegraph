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

# Inspect only logs from a positive run, never a negative-test fixture or a
# whole workspace. Libraries can print these exceptions and still return 0.
PATTERN='java\.lang\.reflect\.InaccessibleObjectException:'
PATTERN+='|java\.lang\.(IllegalAccessError|IllegalAccessException):.*module .*does not'
PATTERN+='|Unrecognized VM option|Unrecognized option:'
PATTERN+='|Ignoring option .*support was removed|VM option .*is (not supported|obsolete|removed)'
PATTERN+='|Unable to inspect cluster runtime logs:'
if [[ "${1:-}" == --native ]]; then
    # Keep native-linkage checks out of suites with intentional missing classes.
    PATTERN+='|undefined symbol|UnsatisfiedLinkError|UnsupportedClassVersionError|NoClassDefFoundError'
    shift
fi
if [[ $# -eq 0 ]]; then
    echo "USAGE: $0 [--native] LOG_FILE_OR_DIRECTORY..." >&2
    exit 2
fi

STATUS=0
check_file() {
    local file="$1" matches code
    if [[ ! -f "$file" || ! -r "$file" ]]; then
        echo "Cannot read runtime log: $file" >&2
        STATUS=2
        return
    fi
    # Scan text logs even when logged keys contain NUL bytes.
    matches=$(LC_ALL=C grep -Eina -- "$PATTERN" "$file" 2>&1)
    code=$?
    if [[ $code -eq 0 ]]; then
        echo "Java runtime compatibility error in $file:" >&2
        printf '%s\n' "$matches" >&2
        [[ $STATUS -eq 2 ]] || STATUS=1
    elif [[ $code -ne 1 ]]; then
        printf '%s\n' "$matches" >&2
        STATUS=2
    fi
}

for INPUT in "$@"; do
    if [[ -d "$INPUT" ]]; then
        # Exclude heap dumps and compressed rotations, even if they contain
        # exception class names. Explicit file inputs can use any text suffix.
        FILES=$(find "$INPUT" -type f \
            \( -name '*.log' -o -name '*.log.[0-9]*' -o -name '*.out' -o -name '*.err' -o -name '*.txt' \) \
            ! -name '*.gz' ! -name '*.zip' ! -name '*.bz2' ! -name '*.xz' ! -name '*.hprof*' -print)
        if [[ $? -ne 0 || -z "$FILES" ]]; then
            echo "Cannot collect runtime logs: $INPUT" >&2
            STATUS=2
            continue
        fi
        while IFS= read -r FILE; do
            check_file "$FILE"
        done <<<"$FILES"
    else
        check_file "$INPUT"
    fi
done
exit "$STATUS"
