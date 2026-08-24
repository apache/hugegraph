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
REPO_ROOT=$(cd "${SCRIPT_DIR}/../../../../.." && pwd)

python3 - "${REPO_ROOT}" <<'PY'
import pathlib
import re
import sys
from collections import Counter

repo_root = pathlib.Path(sys.argv[1])
expected_files = {
    ".github/workflows/commons-ci.yml": {
        "build-commons": "hugegraph-commons/target/jacoco.xml",
    },
    ".github/workflows/pd-store-ci.yml": {
        "pd": "${{ env.REPORT_FILE }}",
        "store": "${{ env.REPORT_FILE }}",
        "hstore": "${{ env.REPORT_DIR }}/*.xml",
    },
    ".github/workflows/server-ci.yml": {
        "build-server": "${{ env.REPORT_DIR }}/*.xml",
    },
}
action_pattern = re.compile(
    r"^(?P<indent>\s*)(?P<dash>-\s+)?uses:\s*"
    r"(?:(?P<quote>['\"])codecov/codecov-action@"
    r"(?P<quoted>[^'\"\s]+)(?P=quote)\s*(?:#.*)?|"
    r"codecov/codecov-action@(?P<plain>\S+?)(?:\s+#.*)?\s*)$"
)
job_pattern = re.compile(r"^  (?P<job>[a-zA-Z0-9_-]+):\s*(?:#.*)?$")
version_pattern = re.compile(r"^v(?P<major>\d+)(?:[.-].*)?$")
errors = []
workflow_dir = repo_root / ".github/workflows"
workflow_paths = sorted(
    set(workflow_dir.glob("*.yml")) | set(workflow_dir.glob("*.yaml"))
)
checked_expected_workflows = set()


def indentation(line):
    return len(line) - len(line.lstrip())


def find_uploads(lines):
    uploads = []
    current_job = None

    for line_number, line in enumerate(lines, start=1):
        job_match = job_pattern.match(line)
        if job_match is not None:
            current_job = job_match.group("job")
        match = action_pattern.match(line)
        if match is None:
            continue

        uses_indent = len(match.group("indent"))
        if match.group("dash") is not None:
            uses_indent += len(match.group("dash"))
        step_indent = uses_indent - 2
        block = []
        for candidate in lines[line_number:]:
            if candidate.strip() and indentation(candidate) <= step_indent:
                break
            block.append(candidate)
        uploads.append(
            (line_number, current_job,
             match.group("quoted") or match.group("plain"), block, uses_indent)
        )

    return uploads


def read_inputs(block, uses_indent):
    inputs = {}
    in_with_block = False
    for candidate in block:
        candidate_indent = indentation(candidate)
        if candidate_indent == uses_indent and candidate.strip() == "with:":
            in_with_block = True
            continue
        if (in_with_block and candidate.strip() and
                candidate_indent <= uses_indent):
            break
        if not in_with_block or candidate_indent != uses_indent + 2:
            continue
        candidate_match = re.match(
            r"^\s*(?P<key>[a-zA-Z_]+):\s*(?P<value>.*?)\s*$",
            candidate,
        )
        if candidate_match is not None:
            inputs[candidate_match.group("key")] = candidate_match.group("value")
    return inputs


def parse_uploads(lines):
    return [
        (line_number, job, version, read_inputs(block, uses_indent))
        for line_number, job, version, block, uses_indent in find_uploads(lines)
    ]


def files_match(uploads, expected):
    actual = Counter(
        (job, inputs.get("files")) for _, job, _, inputs in uploads
    )
    return actual == Counter(expected.items())


def check_mixed_upload_indentation():
    lines = [
        "jobs:",
        "  first:",
        "    steps:",
        "      - uses: codecov/codecov-action@v5",
        "        with:",
        "          token: ${{ secrets.CODECOV_TOKEN }}",
        "          files: first.xml",
        "  second:",
        "      steps:",
        "        - uses: codecov/codecov-action@v5",
        "          with:",
        "            token: ${{ secrets.CODECOV_TOKEN }}",
        "            files: second.xml",
    ]
    expected_inputs = [
        {
            "token": "${{ secrets.CODECOV_TOKEN }}",
            "files": "first.xml",
        },
        {
            "token": "${{ secrets.CODECOV_TOKEN }}",
            "files": "second.xml",
        },
    ]
    actual_inputs = [inputs for _, _, _, inputs in parse_uploads(lines)]
    if actual_inputs != expected_inputs:
        return ["Codecov uploads with mixed indentation were parsed incorrectly"]
    return []


errors.extend(check_mixed_upload_indentation())


for workflow_path in workflow_paths:
    relative_path = str(workflow_path.relative_to(repo_root))
    lines = workflow_path.read_text(encoding="utf-8").splitlines()
    uploads = parse_uploads(lines)

    expected_workflow_files = expected_files.get(relative_path)
    if uploads and expected_workflow_files is None:
        errors.append(
            f"{relative_path}: unexpected Codecov upload workflow"
        )
    elif expected_workflow_files is not None:
        checked_expected_workflows.add(relative_path)
        if not files_match(uploads, expected_workflow_files):
            errors.append(f"{relative_path}: unexpected Codecov files inputs")

    for line_number, _, version, inputs in uploads:
        version_match = version_pattern.match(version)
        if version_match is None or int(version_match.group("major")) < 5:
            errors.append(
                f"{relative_path}:{line_number}: Codecov action {version} "
                "uses the legacy uploader"
            )

        if inputs.get("token") != "${{ secrets.CODECOV_TOKEN }}":
            errors.append(
                f"{relative_path}:{line_number}: Codecov upload must pass "
                "secrets.CODECOV_TOKEN for trusted runs"
            )
        if inputs.get("disable_search") != "true":
            errors.append(
                f"{relative_path}:{line_number}: Codecov upload must set "
                "disable_search: true"
            )
        if inputs.get("fail_ci_if_error") != "false":
            errors.append(
                f"{relative_path}:{line_number}: Codecov upload must keep "
                "fail_ci_if_error: false"
            )

for relative_path in expected_files.keys() - checked_expected_workflows:
    errors.append(f"{relative_path}: expected workflow file is missing")

if errors:
    for error in errors:
        print(f"ERROR: {error}", file=sys.stderr)
    sys.exit(1)

print("PASS: Codecov upload configuration contract")
PY
