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

set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="${SCRIPT_DIR}/run-tinkerpop-test.sh"
TEST_DIR=$(mktemp -d)
trap 'rm -rf "$TEST_DIR"' EXIT

mkdir -p "$TEST_DIR/bin"
cat > "$TEST_DIR/bin/mvn" <<'MOCK'
#!/bin/bash
set -eu
REPORT_DIR=hugegraph-server/hugegraph-test/target/surefire-reports
mkdir -p "$REPORT_DIR"
if [[ -n "$TEST_REPORT" ]]; then
    for suite in StructureStandardTest ProcessStandardTest HugeGraphFeatureTest HugeGraphProviderLifecycleTest; do
        printf '%s\n' "$TEST_REPORT" > "$REPORT_DIR/TEST-org.apache.hugegraph.tinkerpop.$suite.xml"
    done
fi
MOCK
chmod +x "$TEST_DIR/bin/mvn"

check_report() {
    local name=$1
    local report=$2
    local expected=$3
    local suite=$4
    local actual=0

    TEST_REPORT="$report" PATH="$TEST_DIR/bin:$PATH" \
        bash "$RUNNER" hstore "$suite" > "$TEST_DIR/$name.log" 2>&1 || actual=1
    if [[ "$actual" != "$expected" ]]; then
        cat "$TEST_DIR/$name.log"
        echo "FAIL: $name returned $actual, expected $expected"
        exit 1
    fi
}

cd "$TEST_DIR"
for suite in structure process process-standard process-feature tinkerpop; do
    check_report executed '<testsuite tests="1" skipped="0"/>' 0 "$suite"
    check_report partially-skipped '<testsuite tests="2" skipped="1"/>' 0 "$suite"
    check_report all-skipped '<testsuite tests="1" skipped="1"/>' 1 "$suite"
    check_report empty '<testsuite tests="0" skipped="0"/>' 1 "$suite"
    check_report malformed '<testsuite' 1 "$suite"
    # A successful earlier invocation must not leave a usable stale report.
    check_report executed-again '<testsuite tests="1" skipped="0"/>' 0 "$suite"
    check_report missing '' 1 "$suite"
done

echo "PASS: TinkerPop report gate requires executed tests and rejects stale reports"
