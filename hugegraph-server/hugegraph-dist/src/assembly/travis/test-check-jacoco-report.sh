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

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../../../../.." && pwd)
VALIDATOR="${SCRIPT_DIR}/check-jacoco-report.sh"
TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/jacoco-report-test.XXXXXX")
CASE_OUTPUT=""
CASE_RC=0

trap 'rm -rf "${TMP_DIR}"' EXIT

fail() {
    echo "FAIL: $1" >&2
    [[ -z "${CASE_OUTPUT}" ]] || printf '%s\n' "${CASE_OUTPUT}" >&2
    exit 1
}

run_case() {
    CASE_OUTPUT=$("${VALIDATOR}" "$@" 2>&1)
    CASE_RC=$?
}

run_report_case() {
    run_case --require-test-report "${TMP_DIR}/tests.xml" "$@"
}

run_case_with_timeout() {
    CASE_OUTPUT=$(timeout 2 "${VALIDATOR}" "$@" 2>&1)
    CASE_RC=$?
}

assert_success() {
    [[ "${CASE_RC}" -eq 0 ]] || fail "$1 returned ${CASE_RC}"
}

assert_failure() {
    [[ "${CASE_RC}" -ne 0 ]] || fail "$1 unexpectedly succeeded"
}

assert_output() {
    [[ "${CASE_OUTPUT}" == *"$1"* ]] || fail "missing output '$1'"
}

if [[ ! -x "${VALIDATOR}" ]]; then
    fail "validator not found or not executable at ${VALIDATOR}"
fi

cat > "${TMP_DIR}/tests.xml" <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="SuiteTest" tests="2" failures="0" errors="0" skipped="0"/>
EOF

cat > "${TMP_DIR}/zero-tests.xml" <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="EmptySuiteTest" tests="0" failures="0" errors="0" skipped="0"/>
EOF

cat > "${TMP_DIR}/not-surefire.xml" <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<report tests="0"/>
EOF

echo "JaCoCo report validator tests"

run_case_with_timeout --require-session
[[ "${CASE_RC}" -ne 124 ]] || fail "missing session value timed out"
assert_failure "missing session value"
assert_output "--require-session requires a non-empty value"

run_case --require-session ""
assert_failure "empty session value"
assert_output "--require-session requires a non-empty value"

run_case --require-test-report
assert_failure "missing test report value"
assert_output "--require-test-report requires a non-empty value"

run_case --require-suite-report
assert_failure "missing suite report value"
assert_output "--require-suite-report requires a non-empty value"

run_case --require-suite-report ""
assert_failure "empty suite report value"
assert_output "--require-suite-report requires a non-empty value"

run_report_case --require-session suite-a "${TMP_DIR}/missing.xml" hg-pd-client
assert_failure "missing report"
assert_output "not found or empty"

touch "${TMP_DIR}/empty.xml"
run_report_case --require-session suite-a "${TMP_DIR}/empty.xml" hg-pd-client
assert_failure "empty report"
assert_output "not found or empty"

cat > "${TMP_DIR}/valid.xml" <<'EOF'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<report name="hg-pd-test">
  <sessioninfo id="suite-a" start="1" dump="2"/>
  <sessioninfo id="suite-b" start="3" dump="4"/>
  <group name="hg-pd-client"/>
  <group name="hg-pd-core"/>
  <counter type="INSTRUCTION" missed="7" covered="3"/>
</report>
EOF

run_report_case --require-session suite-a --require-session suite-b \
                "${TMP_DIR}/valid.xml" hg-pd-client hg-pd-core
assert_success "complete report"
assert_output "contains all expected modules"

run_case --require-session suite-a --require-session suite-b \
         "${TMP_DIR}/valid.xml" hg-pd-client hg-pd-core
assert_failure "report without required test reports"
assert_output "at least one --require-test-report is required"

run_case --require-test-report "${TMP_DIR}/missing-tests.xml" \
         --require-session suite-a --require-session suite-b \
         "${TMP_DIR}/valid.xml" hg-pd-client hg-pd-core
assert_failure "missing required test report"
assert_output "Surefire report not found or empty"

run_case --require-test-report "${TMP_DIR}/zero-tests.xml" \
         --require-session suite-a --require-session suite-b \
         "${TMP_DIR}/valid.xml" hg-pd-client hg-pd-core
assert_failure "required test report without tests"
assert_output "Surefire report has no tests"

run_case --require-suite-report "${TMP_DIR}/zero-tests.xml" \
         --require-test-report "${TMP_DIR}/tests.xml" \
         --require-session suite-a --require-session suite-b \
         "${TMP_DIR}/valid.xml" hg-pd-client hg-pd-core
assert_success "required zero-test suite report"

run_case --require-suite-report "${TMP_DIR}/missing-suite.xml" \
         --require-test-report "${TMP_DIR}/tests.xml" \
         --require-session suite-a --require-session suite-b \
         "${TMP_DIR}/valid.xml" hg-pd-client hg-pd-core
assert_failure "missing required suite report"
assert_output "Surefire report not found or empty"

run_case --require-suite-report "${TMP_DIR}/not-surefire.xml" \
         --require-test-report "${TMP_DIR}/tests.xml" \
         --require-session suite-a --require-session suite-b \
         "${TMP_DIR}/valid.xml" hg-pd-client hg-pd-core
assert_failure "required suite report with invalid root"
assert_output "unable to parse Surefire report"

run_report_case --require-session suite-a --require-session suite-b \
                "${TMP_DIR}/valid.xml"
assert_failure "report without expected modules"
assert_output "at least one expected module is required"

run_report_case --require-session suite-a --require-session suite-c \
                "${TMP_DIR}/valid.xml" hg-pd-client hg-pd-core
assert_failure "report missing a required session"
assert_output "missing JaCoCo session 'suite-c'"

sed 's/covered="3"/covered="0"/' "${TMP_DIR}/valid.xml" > "${TMP_DIR}/uncovered.xml"
run_report_case --require-session suite-a --require-session suite-b \
                "${TMP_DIR}/uncovered.xml" hg-pd-client hg-pd-core
assert_failure "report without covered instructions"
assert_output "has no covered instructions"

run_report_case --require-session suite-a --require-session suite-b \
                "${TMP_DIR}/valid.xml" hg-pd-client hg-pd-service
assert_failure "report missing an expected module"
assert_output "missing JaCoCo group 'hg-pd-service'"

python3 - "${REPO_ROOT}" <<'PY' || fail "aggregation configuration contract failed"
from pathlib import Path
import re
import sys
import xml.etree.ElementTree as ET

ROOT = Path(sys.argv[1])
NS = "{http://maven.apache.org/POM/4.0.0}"


def child_text(element, name):
    child = element.find(NS + name)
    return "" if child is None or child.text is None else child.text.strip()


def jacoco_plugin(container):
    plugins = container.find(NS + "plugins")
    assert plugins is not None
    for plugin in plugins.findall(NS + "plugin"):
        if child_text(plugin, "artifactId") == "jacoco-maven-plugin":
            return plugin
    raise AssertionError("JaCoCo plugin is missing")


def goals(plugin):
    return [goal.text.strip() for goal in plugin.findall(
        ".//" + NS + "goal") if goal.text]


def check_module(module, test_module):
    parent = ET.parse(ROOT / module / "pom.xml").getroot()
    parent_plugin = jacoco_plugin(parent.find(NS + "build"))
    assert child_text(parent_plugin, "version") == "0.8.8"
    assert child_text(parent_plugin.find(NS + "configuration"), "append") == "true"

    test = ET.parse(ROOT / module / test_module / "pom.xml").getroot()
    default_plugin = jacoco_plugin(test.find(NS + "build"))
    assert child_text(default_plugin, "version") == "0.8.8"
    assert "report-aggregate" not in goals(default_plugin)

    profile = None
    for candidate in test.findall(".//" + NS + "profile"):
        if child_text(candidate, "id") == "jacoco":
            profile = candidate
            break
    assert profile is not None
    profile_plugin = jacoco_plugin(profile.find(NS + "build"))
    assert child_text(profile_plugin, "version") == "0.8.8"
    executions = profile_plugin.findall(".//" + NS + "execution")
    aggregates = [execution for execution in executions
                  if "report-aggregate" in goals(execution)]
    assert len(aggregates) == 1
    assert child_text(aggregates[0], "phase") == "verify"


check_module("hugegraph-pd", "hg-pd-test")
check_module("hugegraph-store", "hg-store-test")

store_test = ET.parse(ROOT / "hugegraph-store/hg-store-test/pom.xml").getroot()
dependencies = store_test.find(NS + "dependencies")
assert dependencies is not None
assert not any(child_text(dep, "artifactId") == "hg-store-rocksdb"
               for dep in dependencies.findall(NS + "dependency"))
store_jacoco = next(profile for profile in store_test.findall(
    ".//" + NS + "profile") if child_text(profile, "id") == "jacoco")
profile_dependencies = store_jacoco.find(NS + "dependencies")
assert profile_dependencies is not None
assert any(child_text(dep, "artifactId") == "hg-store-rocksdb"
           for dep in profile_dependencies.findall(NS + "dependency"))

core_suite = (ROOT / "hugegraph-store/hg-store-test/src/main/java/org/apache/"
              "hugegraph/store/core/CoreSuiteTest.java").read_text()
core_suite_code = "\n".join(
    line for line in core_suite.splitlines()
    if not line.lstrip().startswith("//")
)
assert "@RunWith(Suite.class)" in core_suite_code
assert re.search(r"@Suite[.]SuiteClasses[(][{]\s*[}][)]", core_suite_code)

workflow = (ROOT / ".github/workflows/pd-store-ci.yml").read_text()
pd_job = workflow.split("\n  pd:\n", 1)[1].split("\n  store:\n", 1)[0]
store_job = workflow.split("\n  store:\n", 1)[1].split("\n  hstore:\n", 1)[0]


def assert_order(job, commands):
    positions = [job.index(command) for command in commands]
    assert positions == sorted(positions)


def validation_command(job):
    return job.split("$TRAVIS_DIR/check-jacoco-report.sh", 1)[1].split(
        "- name: Upload coverage", 1)[0]


def reports_for_option(job, option):
    pattern = re.escape(option) + (
        r'\s+\\?\s*"\$TEST_REPORT_DIR/'
        r'(TEST-[A-Za-z0-9_.]+SuiteTest[.]xml)"'
    )
    return set(re.findall(pattern, validation_command(job)))


def required_modules(job):
    command = validation_command(job).split('"$REPORT_FILE"', 1)[1]
    return set(re.findall(r"\bhg-(?:pd|store)-[a-z0-9-]+\b", command))


def selected_profiles(job, prefix):
    return set(re.findall(r"-P (" + prefix + r"-[a-z0-9-]+-test)\b", job))


assert_order(pd_job, [
    "mvn clean package",
    "mvn editorconfig:check -pl hugegraph-pd/hg-pd-test -am -ntp",
    "-P pd-common-test -Djacoco.sessionId=pd-common-test",
    "-P pd-core-test -Djacoco.sessionId=pd-core-test",
    "-P pd-client-test -Djacoco.sessionId=pd-client-test",
    "-P pd-rest-test -Djacoco.sessionId=pd-rest-test",
    "mvn verify", "--require-session pd-common-test",
    "--require-session pd-core-test", "--require-session pd-client-test",
    "--require-session pd-rest-test", "codecov/codecov-action",
])
assert pd_job.count("mvn clean") == 1
assert "hugegraph-pd/hg-pd-test/target/site/jacoco/jacoco.xml" in pd_job
assert "files: ${{ env.REPORT_FILE }}" in pd_job
assert "\n          directory:" not in pd_job
assert "mvn verify -pl hugegraph-pd/hg-pd-test -am -P jacoco \\ " \
       "-DskipTests -Deditorconfig.skip=true -ntp" in " ".join(pd_job.split())
assert selected_profiles(pd_job, "pd") == {
    "pd-common-test", "pd-core-test", "pd-client-test", "pd-rest-test",
}
assert reports_for_option(pd_job, "--require-test-report") == {
    "TEST-org.apache.hugegraph.pd.common.CommonSuiteTest.xml",
    "TEST-org.apache.hugegraph.pd.core.PDCoreSuiteTest.xml",
    "TEST-org.apache.hugegraph.pd.client.PDClientSuiteTest.xml",
    "TEST-org.apache.hugegraph.pd.rest.PDRestSuiteTest.xml",
}
assert not reports_for_option(pd_job, "--require-suite-report")
assert required_modules(pd_job) == {
    "hg-pd-grpc", "hg-pd-common", "hg-pd-client", "hg-pd-core",
    "hg-pd-service", "hg-pd-dist",
}

assert_order(store_job, [
    "mvn clean package",
    "mvn editorconfig:check -pl hugegraph-store/hg-store-test -am -ntp",
    "-P store-common-test -Djacoco.sessionId=store-common-test",
    "-P store-client-test -Djacoco.sessionId=store-client-test",
    "-P store-core-test -Djacoco.sessionId=store-core-test",
    "-P store-rocksdb-test -Djacoco.sessionId=store-rocksdb-test",
    "-P store-server-test -Djacoco.sessionId=store-server-test",
    "-P store-raftcore-test -Djacoco.sessionId=store-raftcore-test",
    "mvn verify", "--require-session store-common-test",
    "--require-session store-client-test", "--require-session store-core-test",
    "--require-session store-rocksdb-test", "--require-session store-server-test",
    "--require-session store-raftcore-test", "codecov/codecov-action",
])
assert store_job.count("mvn clean") == 1
assert "hugegraph-store/hg-store-test/target/site/jacoco/jacoco.xml" in store_job
assert "files: ${{ env.REPORT_FILE }}" in store_job
assert "\n          directory:" not in store_job
assert "mvn verify -pl hugegraph-store/hg-store-test -am -P jacoco \\ " \
       "-DskipTests -Deditorconfig.skip=true -ntp" in " ".join(store_job.split())
assert selected_profiles(store_job, "store") == {
    "store-common-test", "store-client-test", "store-core-test",
    "store-rocksdb-test", "store-server-test", "store-raftcore-test",
}
assert reports_for_option(store_job, "--require-test-report") == {
    "TEST-org.apache.hugegraph.store.common.CommonSuiteTest.xml",
    "TEST-org.apache.hugegraph.store.client.ClientSuiteTest.xml",
    "TEST-org.apache.hugegraph.store.rocksdb.RocksDbSuiteTest.xml",
    "TEST-org.apache.hugegraph.store.raftcore.RaftSuiteTest.xml",
}
assert reports_for_option(store_job, "--require-suite-report") == {
    "TEST-org.apache.hugegraph.store.core.CoreSuiteTest.xml",
    "TEST-org.apache.hugegraph.store.service.ServerSuiteTest.xml",
}
assert required_modules(store_job) == {
    "hg-store-grpc", "hg-store-common", "hg-store-client",
    "hg-store-rocksdb", "hg-store-core", "hg-store-node",
}

print("PASS: JaCoCo aggregation configuration contract")
PY

echo "PASS: JaCoCo report validator contract"
