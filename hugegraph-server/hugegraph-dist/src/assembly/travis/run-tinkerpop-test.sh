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
set -ev

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 BACKEND {structure|process|process-standard|process-feature|tinkerpop}"
    exit 2
fi

BACKEND=$1
SUITE=$2
REPORT_DIR=hugegraph-server/hugegraph-test/target/surefire-reports

function run_structure_test() {
    run_selected_test tinkerpop-structure-test "StructureStandardTest" "StructureStandardTest"
}

function run_process_test() {
    run_selected_test tinkerpop-process-test \
        "ProcessStandardTest,HugeGraphFeatureTest,HugeGraphProviderLifecycleTest" \
        "ProcessStandardTest" "HugeGraphFeatureTest" "HugeGraphProviderLifecycleTest"
}

function run_selected_test() {
    local profile=$1
    local tests=$2
    shift 2
    if [[ $# -eq 0 ]]; then
        echo "At least one expected Surefire report is required"
        exit 2
    fi
    local expected_reports=("$@")
    local expected_report
    local report

    for expected_report in "${expected_reports[@]}"; do
        report="$REPORT_DIR/TEST-org.apache.hugegraph.tinkerpop.$expected_report.xml"
        rm -f "$report"
    done
    mvn test -pl hugegraph-server/hugegraph-test -am \
        -P "$profile,$BACKEND" \
        -Dtest="$tests" \
        -Dsurefire.failIfNoSpecifiedTests=false

    for expected_report in "${expected_reports[@]}"; do
        report="$REPORT_DIR/TEST-org.apache.hugegraph.tinkerpop.$expected_report.xml"
        python3 - "$report" <<'PY'
import sys
import xml.etree.ElementTree as ET

report = ET.parse(sys.argv[1]).getroot()
if int(report.attrib["tests"]) <= int(report.attrib.get("skipped", "0")):
    sys.exit("Expected executed tests in Surefire report: " + sys.argv[1])
PY
    done
}

case "$SUITE" in
    structure)
        run_structure_test
        ;;
    process)
        run_process_test
        ;;
    process-standard)
        run_selected_test tinkerpop-process-test \
            "ProcessStandardTest,HugeGraphProviderLifecycleTest" \
            "ProcessStandardTest" \
            "HugeGraphProviderLifecycleTest"
        ;;
    process-feature)
        run_selected_test tinkerpop-process-test "HugeGraphFeatureTest" "HugeGraphFeatureTest"
        ;;
    tinkerpop)
        run_structure_test
        run_process_test
        ;;
    *)
        echo "Unsupported TinkerPop suite: $SUITE"
        exit 2
        ;;
esac
