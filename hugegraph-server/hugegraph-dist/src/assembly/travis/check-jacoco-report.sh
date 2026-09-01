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
REQUIRED_COVERED_GROUPS=()
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
        --require-covered-group)
            if (( $# < 2 )) || [[ -z "${2:-}" || "${2}" == --* ]]; then
                echo "ERROR: --require-covered-group requires a non-empty value" >&2
                exit 1
            fi
            REQUIRED_COVERED_GROUPS+=("${2}")
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

validate_test_report() {
    local test_report="${1}"

    if [[ ! -s "${test_report}" ]]; then
        echo "ERROR: Surefire report not found or empty: ${test_report}" >&2
        return 1
    fi

    local test_counts
    if ! test_counts=$(python3 - "${test_report}" <<'PY'
import sys
import xml.etree.ElementTree as ET

root = ET.parse(sys.argv[1]).getroot()
if root.tag.rsplit("}", 1)[-1] != "testsuite" or "tests" not in root.attrib:
    raise ValueError("not a Surefire testsuite report")
test_count = int(root.attrib["tests"])
skipped_count = int(root.attrib.get("skipped", "0"))
if test_count < 0:
    raise ValueError("negative Surefire test count")
if skipped_count < 0 or skipped_count > test_count:
    raise ValueError("invalid Surefire skipped count")
print(test_count, test_count - skipped_count)
PY
    ); then
        echo "ERROR: unable to parse Surefire report: ${test_report}" >&2
        return 1
    fi
    local test_count
    local executed_count
    read -r test_count executed_count <<< "${test_counts}"
    if (( test_count <= 0 )); then
        echo "ERROR: Surefire report has no tests: ${test_report}" >&2
        return 1
    fi
    if (( executed_count <= 0 )); then
        echo "ERROR: Surefire report has no executed tests: ${test_report}" >&2
        return 1
    fi
}

for test_report in "${REQUIRED_TEST_REPORTS[@]}"; do
    validate_test_report "${test_report}" || exit 1
done

python3 - "${REPORT_FILE}" "${REQUIRED_SESSIONS[@]}" -- \
    ${REQUIRED_COVERED_GROUPS[@]+"${REQUIRED_COVERED_GROUPS[@]}"} \
    -- "$@" <<'PY' || exit 1
import sys
import xml.etree.ElementTree as ET

report_file = sys.argv[1]
session_separator = sys.argv.index("--", 2)
group_separator = sys.argv.index("--", session_separator + 1)
required_sessions = sys.argv[2:session_separator]
required_covered_groups = sys.argv[session_separator + 1:group_separator]
required_modules = sys.argv[group_separator + 1:]


def fail(message):
    print(f"ERROR: {message}", file=sys.stderr)
    sys.exit(1)


def local_name(tag):
    return tag.rsplit("}", 1)[-1]


try:
    root = ET.parse(report_file).getroot()
except (OSError, ET.ParseError) as error:
    fail(f"unable to parse JaCoCo report: {report_file}: {error}")

if local_name(root.tag) != "report":
    fail(f"unable to parse JaCoCo report: {report_file}: expected report root")

children = list(root)
instruction_counters = [
    element for element in children
    if local_name(element.tag) == "counter" and
    element.attrib.get("type") == "INSTRUCTION"
]
try:
    has_coverage = any(int(counter.attrib.get("covered", "0")) > 0
                       for counter in instruction_counters)
except ValueError as error:
    fail(f"unable to parse JaCoCo report: {report_file}: {error}")
if not has_coverage:
    fail(f"JaCoCo report has no covered instructions: {report_file}")

session_ids = {
    element.attrib.get("id") for element in children
    if local_name(element.tag) == "sessioninfo"
}
for session in required_sessions:
    if session not in session_ids:
        fail(f"missing JaCoCo session '{session}' in {report_file}")

groups_by_name = {
    element.attrib.get("name"): element for element in children
    if local_name(element.tag) == "group"
}
for module in required_modules:
    if module not in groups_by_name:
        fail(f"missing JaCoCo group '{module}' in {report_file}")

for group_name in required_covered_groups:
    group = groups_by_name.get(group_name)
    if group is None:
        fail(f"missing JaCoCo group '{group_name}' in {report_file}")
    counters = [
        element for element in list(group)
        if local_name(element.tag) == "counter" and
        element.attrib.get("type") == "INSTRUCTION"
    ]
    try:
        has_coverage = any(int(counter.attrib.get("covered", "0")) > 0
                           for counter in counters)
    except ValueError as error:
        fail(f"unable to parse JaCoCo report: {report_file}: {error}")
    if not has_coverage:
        fail(f"JaCoCo group '{group_name}' has no covered instructions: "
             f"{report_file}")
PY

echo "JaCoCo report ${REPORT_FILE} contains all expected modules"
