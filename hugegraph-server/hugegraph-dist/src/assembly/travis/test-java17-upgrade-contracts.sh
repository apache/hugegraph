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

set -euo pipefail

SERVER_ROOT_INPUT="${1:?Usage: $0 PATH_TO_SERVER_DIST PATH_TO_SOURCE_ROOT}"
SOURCE_ROOT_INPUT="${2:?Usage: $0 PATH_TO_SERVER_DIST PATH_TO_SOURCE_ROOT}"
SERVER_ROOT=$(cd "$SERVER_ROOT_INPUT" && pwd)
SOURCE_ROOT=$(cd "$SOURCE_ROOT_INPUT" && pwd)

fail() {
    echo "FAIL: $1" >&2
    exit 1
}

assert_argument() {
    local argument="$1"
    local capture="$2"
    grep -Fxq -- "$argument" "$capture" || \
        fail "missing JVM argument: $argument"
}

assert_default_test_is_tolerant() {
    local pom="$1"

    python3 - "$pom" <<'PY'
import sys
import xml.etree.ElementTree as ET

pom = sys.argv[1]
namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
root = ET.parse(pom).getroot()
value = root.find(
    "m:properties/m:surefire.failIfNoSpecifiedTests", namespace
)
if value is None or (value.text or "").strip() != "false":
    raise SystemExit(
        "{}: default-test must tolerate -Dtest misses in reactor modules".format(pom)
    )
PY
}

assert_supported_java_contract() {
    local pom="$1"

    python3 - "$pom" <<'PY'
import sys
import xml.etree.ElementTree as ET

pom = sys.argv[1]
namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
root = ET.parse(pom).getroot()
properties = root.find("m:properties", namespace)
if properties is None:
    raise SystemExit("{}: Maven properties are missing".format(pom))

release = properties.find("m:maven.compiler.release", namespace)
if release is None or (release.text or "").strip() != "17":
    raise SystemExit("{}: compiler release must remain 17".format(pom))

supported_range = properties.find("m:java.supported.version.range", namespace)
if supported_range is None or (supported_range.text or "").strip() != "[17,18)":
    raise SystemExit("{}: supported JDK range must be [17,18)".format(pom))

expected_reference = "${java.supported.version.range}"
actual_references = []
for plugin in root.findall("m:build/m:plugins/m:plugin", namespace):
    artifact_id = plugin.find("m:artifactId", namespace)
    if artifact_id is None or artifact_id.text != "maven-enforcer-plugin":
        continue
    for rule in plugin.findall(
        "m:executions/m:execution/m:configuration/m:rules/m:requireJavaVersion",
        namespace,
    ):
        version = rule.find("m:version", namespace)
        if version is not None:
            actual_references.append((version.text or "").strip())

if actual_references != [expected_reference]:
    raise SystemExit(
        "{}: requireJavaVersion must consume {} exactly once; found {}".format(
            pom, expected_reference, actual_references
        )
    )
PY
}

assert_surefire_execution_scope() {
    local pom="$1"
    shift

    python3 - "$pom" "$@" <<'PY'
import sys
import xml.etree.ElementTree as ET

pom = sys.argv[1]
expected_ids = sys.argv[2:]
namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
root = ET.parse(pom).getroot()

properties = root.find("m:properties", namespace)
if properties is not None:
    module_value = properties.find("m:surefire.failIfNoSpecifiedTests", namespace)
    if module_value is not None and (module_value.text or "").strip() == "true":
        raise SystemExit(
            "{}: leaf-wide failIfNoSpecifiedTests=true breaks default-test".format(pom)
        )

surefire = None
for plugin in root.findall("m:build/m:plugins/m:plugin", namespace):
    artifact_id = plugin.find("m:artifactId", namespace)
    if artifact_id is not None and artifact_id.text == "maven-surefire-plugin":
        surefire = plugin
        break

if surefire is None:
    raise SystemExit("{}: maven-surefire-plugin is missing".format(pom))

strict_executions = set()
for execution in surefire.findall("m:executions/m:execution", namespace):
    execution_id = execution.find("m:id", namespace)
    strict = execution.find("m:configuration/m:failIfNoSpecifiedTests", namespace)
    if execution_id is None or strict is None:
        continue
    if (strict.text or "").strip() == "true":
        strict_executions.add(execution_id.text)

missing = sorted(set(expected_ids) - strict_executions)
if missing:
    raise SystemExit(
        "{}: named Surefire executions are not strict: {}".format(
            pom, ", ".join(missing)
        )
    )
PY
}

assert_supported_java_contract "${SOURCE_ROOT}/pom.xml"
assert_default_test_is_tolerant "${SOURCE_ROOT}/pom.xml"
assert_surefire_execution_scope \
    "${SOURCE_ROOT}/hugegraph-server/hugegraph-test/pom.xml" \
    core-test unit-test api-test tinkerpop-structure-test tinkerpop-process-test
assert_surefire_execution_scope \
    "${SOURCE_ROOT}/hugegraph-pd/hg-pd-test/pom.xml" \
    pd-client-test pd-core-test pd-common-test pd-rest-test
assert_surefire_execution_scope \
    "${SOURCE_ROOT}/hugegraph-store/hg-store-test/pom.xml" \
    store-client-test store-core-test store-common-test store-rocksdb-test \
    store-server-test store-raftcore-test

MODULE_OPTIONS="${SERVER_ROOT}/bin/jvm-module.options"
SERVER_SCRIPT="${SERVER_ROOT}/bin/hugegraph-server.sh"
INIT_STORE_SCRIPT="${SERVER_ROOT}/bin/init-store.sh"
UTIL_SCRIPT="${SERVER_ROOT}/bin/util.sh"
CONF_SOURCE="${SERVER_ROOT}/conf"

for source_file in "$MODULE_OPTIONS" "$SERVER_SCRIPT" \
                   "$INIT_STORE_SCRIPT" "$UTIL_SCRIPT"; do
    [[ -f "$source_file" ]] || fail "runtime asset is missing: $source_file"
done
[[ -d "$CONF_SOURCE" ]] || fail "server conf is missing: $CONF_SOURCE"

TEMP_DIR=$(mktemp -d)
trap 'rm -rf "$TEMP_DIR"' EXIT

DIST_ROOT="${TEMP_DIR}/server"
MOCK_JAVA_HOME="${TEMP_DIR}/mock-java-home"
mkdir -p "${DIST_ROOT}"/{bin,conf,ext,lib,logs,plugins} \
         "${MOCK_JAVA_HOME}/bin"
cp "$MODULE_OPTIONS" "$SERVER_SCRIPT" "$INIT_STORE_SCRIPT" "$UTIL_SCRIPT" \
   "${DIST_ROOT}/bin/"
cp -R "${CONF_SOURCE}/." "${DIST_ROOT}/conf/"

# Model a full pre-Phase-2 conf/ directory: it has no module argfile. Both
# launchers must get the immutable runtime copy from bin/ instead.
if [[ -e "${DIST_ROOT}/conf/jvm-module.options" ]]; then
    fail "legacy conf unexpectedly contains jvm-module.options"
fi

cat > "${MOCK_JAVA_HOME}/bin/java" <<'MOCK'
#!/bin/bash
for argument in "$@"; do
    if [[ "$argument" == "-version" ]]; then
        echo 'openjdk version "17.0.0"' >&2
        exit 0
    fi
done
printf '%s\n' "$@" > "${CAPTURE_FILE:?}"
MOCK
chmod +x "${MOCK_JAVA_HOME}/bin/java" "${DIST_ROOT}/bin/hugegraph-server.sh" \
         "${DIST_ROOT}/bin/init-store.sh"

SERVER_CAPTURE="${TEMP_DIR}/server.args"
CAPTURE_FILE="$SERVER_CAPTURE" JAVA_HOME="$MOCK_JAVA_HOME" \
    JAVA_OPTIONS="-Xms512m -Xmx512m" STDOUT_MODE=true \
    "${DIST_ROOT}/bin/hugegraph-server.sh" \
    "${DIST_ROOT}/conf/gremlin-server.yaml" \
    "${DIST_ROOT}/conf/rest-server.properties" false >/dev/null
assert_argument "@${DIST_ROOT}/bin/jvm-module.options" "$SERVER_CAPTURE"

INIT_STORE_CAPTURE="${TEMP_DIR}/init-store.args"
CAPTURE_FILE="$INIT_STORE_CAPTURE" JAVA_HOME="$MOCK_JAVA_HOME" \
    "${DIST_ROOT}/bin/init-store.sh" >/dev/null
assert_argument "@${DIST_ROOT}/bin/jvm-module.options" "$INIT_STORE_CAPTURE"

echo "PASS: Java 17 upgrade contracts"
