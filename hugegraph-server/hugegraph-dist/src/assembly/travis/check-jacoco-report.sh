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

REQUIRED_SESSIONS=()
REQUIRED_TEST_REPORTS=()
while (( $# > 0 )); do
    case "${1}" in
        --require-session)
            if (( $# < 2 )) || [[ -z "${2:-}" || "${2}" == --* ]]; then
                echo "ERROR: --require-session requires a non-empty value" >&2
                exit 1
            fi
            REQUIRED_SESSIONS+=("${2}")
            shift 2
            ;;
        --require-test-report)
            if (( $# < 2 )) || [[ -z "${2:-}" || "${2}" == --* ]]; then
                echo "ERROR: --require-test-report requires a non-empty value" >&2
                exit 1
            fi
            REQUIRED_TEST_REPORTS+=("${2}")
            shift 2
            ;;
        --*)
            echo "ERROR: unknown option: ${1}" >&2
            exit 1
            ;;
        *)
            break
            ;;
    esac
done

if (( ${#REQUIRED_SESSIONS[@]} == 0 )); then
    echo "ERROR: at least one --require-session is required" >&2
    exit 1
fi

if (( ${#REQUIRED_TEST_REPORTS[@]} == 0 )); then
    echo "ERROR: at least one --require-test-report is required" >&2
    exit 1
fi

REPORT_FILE="${1:-}"
if (( $# > 0 )); then
    shift
fi

if [[ -z "${REPORT_FILE}" || ! -s "${REPORT_FILE}" ]]; then
    echo "ERROR: JaCoCo report not found or empty: ${REPORT_FILE:-<unset>}" >&2
    exit 1
fi

if (( $# == 0 )); then
    echo "ERROR: at least one expected module is required" >&2
    exit 1
fi

for test_report in "${REQUIRED_TEST_REPORTS[@]}"; do
    if [[ ! -s "${test_report}" ]]; then
        echo "ERROR: Surefire report not found or empty: ${test_report}" >&2
        exit 1
    fi

    if ! test_count=$(python3 - "${test_report}" <<'PY'
import sys
import xml.etree.ElementTree as ET

root = ET.parse(sys.argv[1]).getroot()
print(int(root.attrib.get("tests", "0")))
PY
    ); then
        echo "ERROR: unable to parse Surefire report: ${test_report}" >&2
        exit 1
    fi
    if (( test_count <= 0 )); then
        echo "ERROR: Surefire report has no tests: ${test_report}" >&2
        exit 1
    fi
done

if ! grep -Eq '<counter type="INSTRUCTION" missed="[0-9]+" covered="[1-9][0-9]*"' \
     "${REPORT_FILE}"; then
    echo "ERROR: JaCoCo report has no covered instructions: ${REPORT_FILE}" >&2
    exit 1
fi

for session in "${REQUIRED_SESSIONS[@]}"; do
    if ! grep -Fq "<sessioninfo id=\"${session}\" " "${REPORT_FILE}"; then
        echo "ERROR: missing JaCoCo session '${session}' in ${REPORT_FILE}" >&2
        exit 1
    fi
done

for module in "$@"; do
    if ! grep -Fq "<group name=\"${module}\"" "${REPORT_FILE}"; then
        echo "ERROR: missing JaCoCo group '${module}' in ${REPORT_FILE}" >&2
        exit 1
    fi
done

echo "JaCoCo report ${REPORT_FILE} contains all expected modules"
