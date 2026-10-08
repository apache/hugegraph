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

SERVER_ROOT_INPUT="${1:?Usage: $0 PATH_TO_SERVER_DIST [SOURCE_ROOT]}"
SOURCE_ROOT_INPUT="${2:-}"
SERVER_ROOT=$(cd "$SERVER_ROOT_INPUT" && pwd)
SERVER_SCRIPT="${SERVER_ROOT}/bin/hugegraph-server.sh"
CONF="${SERVER_ROOT}/conf"
SECURITY_PROPERTIES="${CONF}/java-security.properties"
JVM_MODULE_OPTIONS="${SERVER_ROOT}/bin/jvm-module.options"
# The test sets every JVM option source itself; values inherited from the runner
# would change what a JVM settles on.
unset JAVA_TOOL_OPTIONS JDK_JAVA_OPTIONS _JAVA_OPTIONS
# Launcher runs create heap dump directories named after the host. A host name
# unique to this run lets the cleanup tell them from a real Server's directories.
TEST_HOST="hgtest-$$"
export HOSTNAME="$TEST_HOST"

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

assert_argument_matching() {
    local pattern="$1"
    local capture="$2"
    grep -Eq -- "$pattern" "$capture" || \
        fail "missing JVM argument matching: $pattern"
}

assert_no_argument() {
    local pattern="$1"
    local capture="$2"
    if grep -Eq -- "$pattern" "$capture"; then
        fail "unexpected JVM argument matching: $pattern"
    fi
}

assert_source_consumer() {
    local source_file="$1"
    local expected="$2"
    [[ -f "$source_file" ]] || fail "source consumer is missing: $source_file"
    grep -Fq -- "$expected" "$source_file" ||
        fail "JVM module options consumer is not wired: $source_file"
}

assert_surefire_arg_lines() {
    local pom="$1"
    local expected="$2"
    local total
    local wired
    local jacoco_wired
    read -r total wired jacoco_wired < <(
        awk -v expected="$expected" '
            /<artifactId>maven-surefire-plugin<\/artifactId>/ {
                in_surefire = 1
            }
            in_surefire && /<argLine([[:space:]][^>]*)?>/ {
                in_arg_line = 1
                arg_line = ""
            }
            in_arg_line {
                arg_line = arg_line $0
            }
            in_arg_line && /<\/argLine>/ {
                total++
                if (index(arg_line, expected) != 0) {
                    wired++
                }
                if (index(arg_line, "@{argLine}") != 0) {
                    jacoco_wired++
                }
                in_arg_line = 0
            }
            in_surefire && /<\/plugin>/ {
                in_surefire = 0
            }
            END {
                print total + 0, wired + 0, jacoco_wired + 0
            }
        ' "$pom"
    )
    if [[ "$total" -eq 0 || "$wired" -ne "$total" ]]; then
        fail "all Surefire argLine values must use jvm-module.options: $pom"
    fi
    if [[ "$jacoco_wired" -ne "$total" ]]; then
        fail "all Surefire argLine values must preserve @{argLine}: $pom"
    fi
}

assert_no_inline_module_options() {
    local pattern
    local source_file
    pattern="--add-(exports|opens)([[:space:]]+|=)[\"']?java\\.base/|"
    pattern="${pattern}--add-modules([[:space:]]+|=)[\"']?jdk\.unsupported"
    for source_file in "$@"; do
        [[ -f "$source_file" ]] || fail "source consumer is missing: $source_file"
    done
    if grep -En -- "$pattern" "$@"; then
        fail "JVM module options must only be declared in jvm-module.options"
    fi
}

if [[ ! -x "$SERVER_SCRIPT" ]]; then
    fail "server script is not executable: $SERVER_SCRIPT"
fi
if [[ ! -f "$SECURITY_PROPERTIES" ]]; then
    fail "security properties file is missing: $SECURITY_PROPERTIES"
fi
if [[ ! -f "$JVM_MODULE_OPTIONS" ]]; then
    fail "JVM module options file is missing: $JVM_MODULE_OPTIONS"
fi

assert_argument "--add-exports=java.base/jdk.internal.reflect=ALL-UNNAMED" \
                "$JVM_MODULE_OPTIONS"
assert_argument "--add-modules=jdk.unsupported" "$JVM_MODULE_OPTIONS"
assert_argument "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED" \
                "$JVM_MODULE_OPTIONS"

if [[ -n "$SOURCE_ROOT_INPUT" ]]; then
    if [[ ! -d "$SOURCE_ROOT_INPUT" ]]; then
        fail "source root is not a directory: $SOURCE_ROOT_INPUT"
    fi
    SOURCE_ROOT=$(cd "$SOURCE_ROOT_INPUT" && pwd)
    SERVER_DIST_SOURCE="${SOURCE_ROOT}/hugegraph-server/hugegraph-dist"
    CLUSTER_SOURCE="${SOURCE_ROOT}/hugegraph-cluster-test/"\
"hugegraph-clustertest-minicluster/src/main/java/org/apache/hugegraph/ct"
    SERVER_LAUNCHER_SOURCE="${SERVER_DIST_SOURCE}/src/assembly/static/bin/"\
"hugegraph-server.sh"
    INIT_STORE_SOURCE="${SERVER_DIST_SOURCE}/src/assembly/static/bin/init-store.sh"
    SUREFIRE_POM="${SOURCE_ROOT}/hugegraph-server/hugegraph-test/pom.xml"
    TEST_JVM_MODULE_OPTIONS="${SOURCE_ROOT}/hugegraph-server/hugegraph-test/"\
"conf/jvm-test-module.options"
    COMMONS_POM="${SOURCE_ROOT}/hugegraph-commons/pom.xml"
    CLUSTER_WRAPPER="${CLUSTER_SOURCE}/node/ServerNodeWrapper.java"
    SERVER_DOCKERFILE="${SOURCE_ROOT}/hugegraph-server/Dockerfile"
    HSTORE_DOCKERFILE="${SOURCE_ROOT}/hugegraph-server/Dockerfile-hstore"
    SERVER_WORKFLOW="${SOURCE_ROOT}/.github/workflows/server-tests.yml"
    DOCKER_WORKFLOW="${SOURCE_ROOT}/.github/workflows/docker-build-ci.yml"
    UPGRADE_CONTRACT_SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/"\
"test-java17-upgrade-contracts.sh"

    [[ -x "$UPGRADE_CONTRACT_SCRIPT" ]] || \
        fail "Java 17 upgrade contract script is missing: $UPGRADE_CONTRACT_SCRIPT"
    "$UPGRADE_CONTRACT_SCRIPT" "$SERVER_ROOT" "$SOURCE_ROOT"

    assert_source_consumer "$SERVER_LAUNCHER_SOURCE" '@"${JVM_MODULE_OPTIONS}"'
    assert_source_consumer "$INIT_STORE_SOURCE" '@"${JVM_MODULE_OPTIONS}"'
    assert_surefire_arg_lines "$SUREFIRE_POM" \
        '@${project.basedir}/../hugegraph-dist/src/assembly/static/bin/jvm-module.options'
    [[ -f "$TEST_JVM_MODULE_OPTIONS" ]] || \
        fail "JVM test module options file is missing: $TEST_JVM_MODULE_OPTIONS"
    assert_argument \
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED" \
        "$TEST_JVM_MODULE_OPTIONS"
    assert_argument "--add-opens=java.base/java.lang=ALL-UNNAMED" \
                    "$TEST_JVM_MODULE_OPTIONS"
    assert_surefire_arg_lines "$SUREFIRE_POM" \
        '@${project.basedir}/conf/jvm-test-module.options'
    assert_surefire_arg_lines "$COMMONS_POM" \
        '@${project.parent.basedir}/../hugegraph-server/hugegraph-test/conf/jvm-test-module.options'
    assert_source_consumer "$CLUSTER_WRAPPER" \
        '"@" + Paths.get(SERVER_PACKAGE_PATH, BIN_DIR,'
    assert_no_inline_module_options \
        "$SERVER_LAUNCHER_SOURCE" "$INIT_STORE_SOURCE" "$SUREFIRE_POM" \
        "$COMMONS_POM" "$CLUSTER_WRAPPER" "$SERVER_DOCKERFILE" \
        "$HSTORE_DOCKERFILE" "$SERVER_WORKFLOW" "$DOCKER_WORKFLOW"
fi

if [[ -n "${JAVA_HOME:-}" ]]; then
    JAVA_BIN="${JAVA_HOME}/bin/java"
else
    JAVA_BIN="java"
fi
# Select the JVM banner line the same way the launcher does, anchored to its
# "java version"/"openjdk version" prefix: a preamble such as "Picked up
# JAVA_TOOL_OPTIONS: ..." precedes it whenever JAVA_TOOL_OPTIONS or
# _JAVA_OPTIONS is set, and an agent loaded that way may print its own
# 'version "..."' banner that an unanchored match would read instead.
JAVA_MAJOR=$($JAVA_BIN -version 2>&1 |
             awk -F'"' '/^(java|openjdk) version "/ {print $2; exit}' |
             sed 's/^1\.//' | cut -d'.' -f1)
JAVA_MAJOR="${JAVA_MAJOR%%[!0-9]*}"
if [[ -z "$JAVA_MAJOR" ]]; then
    fail "could not determine the Java major version of $JAVA_BIN"
fi
SECURITY_MANAGER_OPTION=""
if [[ "$JAVA_MAJOR" -ge 18 ]]; then
    SECURITY_MANAGER_OPTION="-Djava.security.manager=allow"
fi

TEMP_DIR=$(mktemp -d)
CRASH_NAME_FIXTURES=()
LAUNCHER_DUMP_DIRS=()
OOM_DUMP_DIR=""
SECURITY_PROPERTIES_BACKUP="${TEMP_DIR}/java-security.properties"

cleanup() {
    if [[ -d "$SECURITY_PROPERTIES" ]]; then
        rmdir "$SECURITY_PROPERTIES"
    fi
    if [[ -f "$SECURITY_PROPERTIES_BACKUP" &&
          ! -e "$SECURITY_PROPERTIES" ]]; then
        mv "$SECURITY_PROPERTIES_BACKUP" "$SECURITY_PROPERTIES"
    fi
    # Fixtures the test adds to the distribution; a failed step must not leave
    # them behind.
    if [[ ${#CRASH_NAME_FIXTURES[@]} -gt 0 ]]; then
        rm -rf "${CRASH_NAME_FIXTURES[@]}"
    fi
    if [[ -n "${OOM_DUMP_DIR:-}" ]]; then
        rm -f "$OOM_DUMP_DIR"/java_pid*.hprof
    fi
    # Each launcher run creates a heap dump directory. Remove only the empty ones
    # this run created: those named after TEST_HOST, and the few it names itself.
    local dump_dir
    for dump_dir in "${SERVER_ROOT}"/logs/heapdump_"${TEST_HOST}"_*/ \
                    ${LAUNCHER_DUMP_DIRS[@]+"${LAUNCHER_DUMP_DIRS[@]}"}; do
        if [[ -d "$dump_dir" && ! -L "${dump_dir%/}" ]]; then
            rmdir "$dump_dir" 2>/dev/null || true
        fi
    done
    rm -rf "$TEMP_DIR"
}

trap cleanup EXIT

CHECK_SOURCE="${TEMP_DIR}/ReadDnsCacheTtl.java"
cat > "$CHECK_SOURCE" <<'JAVA'
import java.security.Security;

public class ReadDnsCacheTtl {
    public static void main(String[] args) {
        String value = Security.getProperty("networkaddress.cache.ttl");
        if (args.length == 0) {
            System.out.print(value);
            return;
        }
        try {
            if (Integer.parseInt(value) <= 0) {
                System.exit(1);
            }
        } catch (NumberFormatException e) {
            System.exit(1);
        }
    }
}
JAVA

assert_valid_security_properties() {
    "$JAVA_BIN" "$@" "$CHECK_SOURCE" --validate >/dev/null ||
        fail "expected valid Java security properties: $*"
}

assert_invalid_security_properties() {
    if "$JAVA_BIN" "$@" "$CHECK_SOURCE" --validate >/dev/null 2>&1; then
        fail "expected invalid Java security properties: $*"
    fi
}

# The bootstrap only hands over to HugeGraphServer once the DNS TTL check and
# the HugeSecurityManager installation have both succeeded, so this downstream
# configuration failure is a positive signal instead of merely a nonzero exit.
assert_reached_server_startup() {
    local error_file="$1"
    local message="$2"
    grep -Fq "Failed to load yaml config file" "$error_file" || fail "$message"
    grep -Fq "org.apache.hugegraph.bootstrap.HugeGraphServerBootstrap.main" \
             "$error_file" || fail "$message"
    grep -Fq "org.apache.hugegraph.dist.HugeGraphServer.main" \
             "$error_file" || fail "$message"
}

assert_clean_bootstrap_error() {
    local error_file="$1"
    # Every launcher run now makes the JVM print "Picked up JAVA_TOOL_OPTIONS:",
    # whose paths may contain any host or directory name.
    if awk '!/^Picked up / && /Log4j|NetUtils|UnknownHost|hostname/ { found = 1 }
            END { exit !found }' "$error_file"; then
        fail "bootstrap initialized logging or hostname resolution"
    fi
}

assert_bootstrap_rejects_security_properties() {
    local error_file="${TEMP_DIR}/server-validation.err"
    if "$JAVA_BIN" "$@" \
       ${SECURITY_MANAGER_OPTION} \
       -cp "${SERVER_ROOT}/lib/*" \
       org.apache.hugegraph.bootstrap.HugeGraphServerBootstrap true \
       >/dev/null 2>"$error_file"; then
        fail "server accepted invalid Java security properties: $*"
    fi
    grep -Fq "networkaddress.cache.ttl must load as a finite positive integer" \
             "$error_file" || fail "server did not report the invalid DNS TTL"
    assert_clean_bootstrap_error "$error_file"
}

assert_bootstrap_accepts_security_properties() {
    local error_file="${TEMP_DIR}/server-validation.err"
    if "$JAVA_BIN" "$@" \
       ${SECURITY_MANAGER_OPTION} \
       -cp "${SERVER_ROOT}/lib/*" \
       org.apache.hugegraph.bootstrap.HugeGraphServerBootstrap true \
       >/dev/null 2>"$error_file"; then
        fail "server unexpectedly started without configuration arguments"
    fi
    grep -Fq "Expected validation flag and two HugeGraphServer" \
             "$error_file" || fail "valid DNS TTL did not reach argument validation"
    assert_clean_bootstrap_error "$error_file"
}

assert_bootstrap_handles_security_properties_load_failure() {
    local error_file="${TEMP_DIR}/server-validation.err"
    if "$JAVA_BIN" "$@" \
       ${SECURITY_MANAGER_OPTION} \
       -cp "${SERVER_ROOT}/lib/*" \
       org.apache.hugegraph.bootstrap.HugeGraphServerBootstrap true \
       >/dev/null 2>"$error_file"; then
        fail "server accepted unloadable Java security properties: $*"
    fi
    grep -Fq "networkaddress.cache.ttl must load as a finite positive integer" \
             "$error_file" || fail "server did not report a stable load error"
    assert_clean_bootstrap_error "$error_file"
}

assert_bootstrap_skips_security_validation() {
    local error_file="${TEMP_DIR}/server-validation.err"
    if "$JAVA_BIN" "$@" -cp "${SERVER_ROOT}/lib/*" \
       org.apache.hugegraph.bootstrap.HugeGraphServerBootstrap false \
       >/dev/null 2>"$error_file"; then
        fail "server unexpectedly started without configuration arguments"
    fi
    grep -Fq "Expected validation flag and two HugeGraphServer" \
             "$error_file" || fail "disabled DNS TTL validation was not skipped"
    assert_clean_bootstrap_error "$error_file"
}

assert_launcher_rejects_marker_bypass() {
    local marker_value="$1"
    local error_file="${TEMP_DIR}/launcher-marker-${marker_value}.err"
    if _JAVA_OPTIONS="-Dhugegraph.security.validate_dns_cache_ttl=${marker_value}" \
       JAVA_OPTIONS="" STDOUT_MODE=true "$SERVER_SCRIPT" \
       "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" true \
       "-Djava.security.properties=${INFINITE_PROPERTIES}" \
       >/dev/null 2>"$error_file"; then
        fail "_JAVA_OPTIONS marker bypassed DNS TTL validation"
    fi
    grep -Fq "networkaddress.cache.ttl must load as a finite positive integer" \
             "$error_file" || fail "launcher did not report invalid DNS TTL"
    assert_clean_bootstrap_error "$error_file"
}

assert_launcher_rejects_security_properties() {
    local properties_path="$1"
    local error_file="${TEMP_DIR}/launcher-properties.err"
    if JAVA_OPTIONS="" STDOUT_MODE=true "$SERVER_SCRIPT" \
       "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" true \
       "-Djava.security.properties=${properties_path}" \
       >/dev/null 2>"$error_file"; then
        fail "launcher accepted invalid Java security properties"
    fi
    grep -Fq "networkaddress.cache.ttl must load as a finite positive integer" \
             "$error_file" || fail "launcher did not report invalid DNS TTL"
    assert_clean_bootstrap_error "$error_file"
}

assert_launcher_accepts_security_properties() {
    local properties_path="$1"
    local error_file="${TEMP_DIR}/launcher-valid.err"
    if JAVA_OPTIONS="" STDOUT_MODE=true "$SERVER_SCRIPT" \
       "${TEMP_DIR}/missing-gremlin.yaml" \
       "${TEMP_DIR}/missing-rest.properties" true \
       "-Djava.security.properties=${properties_path}" \
       >/dev/null 2>"$error_file"; then
        fail "server unexpectedly started with missing configuration"
    fi
    if grep -Eq 'networkaddress.cache.ttl must load|Failed to install' \
                "$error_file"; then
        fail "launcher rejected valid Java security properties"
    fi
    assert_reached_server_startup "$error_file" \
        "valid Java security properties did not reach server startup"
}

assert_launcher_skips_security_validation() {
    local error_file="${TEMP_DIR}/launcher-disabled.err"
    if _JAVA_OPTIONS="-Dhugegraph.security.validate_dns_cache_ttl=true \
                      -Djava.security.properties=${INFINITE_PROPERTIES}" \
       JAVA_OPTIONS="" STDOUT_MODE=true "$SERVER_SCRIPT" \
       "${TEMP_DIR}/missing-gremlin.yaml" \
       "${TEMP_DIR}/missing-rest.properties" false \
       >/dev/null 2>"$error_file"; then
        fail "server unexpectedly started with missing configuration"
    fi
    if grep -Fq "networkaddress.cache.ttl must load" "$error_file"; then
        fail "disabled launcher unexpectedly validated DNS TTL"
    fi
    assert_reached_server_startup "$error_file" \
        "disabled security check did not reach server startup"
}

ACTUAL_TTL=$("$JAVA_BIN" \
    -Djava.security.properties="$SECURITY_PROPERTIES" "$CHECK_SOURCE")
if [[ "$ACTUAL_TTL" != "30" ]]; then
    fail "expected security property TTL 30, got: $ACTUAL_TTL"
fi

SYSTEM_PROPERTY_TTL=$("$JAVA_BIN" \
    -Dnetworkaddress.cache.ttl=99 \
    -Djava.security.properties="$SECURITY_PROPERTIES" "$CHECK_SOURCE")
if [[ "$SYSTEM_PROPERTY_TTL" != "30" ]]; then
    fail "ordinary -D property unexpectedly changed the security property"
fi

OPERATOR_PROPERTIES="${TEMP_DIR}/operator-security.properties"
echo "networkaddress.cache.ttl = 45" > "$OPERATOR_PROPERTIES"
OPERATOR_TTL=$("$JAVA_BIN" \
    -Djava.security.properties="$SECURITY_PROPERTIES" \
    -Djava.security.properties="$OPERATOR_PROPERTIES" "$CHECK_SOURCE")
if [[ "$OPERATOR_TTL" != "45" ]]; then
    fail "operator security properties override was not honored"
fi

REPLACEMENT_TTL=$("$JAVA_BIN" \
    "-Djava.security.properties=${OPERATOR_PROPERTIES}" \
    "-Djava.security.properties==${OPERATOR_PROPERTIES}" "$CHECK_SOURCE")
if [[ "$REPLACEMENT_TTL" != "45" ]]; then
    fail "operator security properties replacement was not honored"
fi

assert_valid_security_properties \
    "-Djava.security.properties=${SECURITY_PROPERTIES}" \
    "-Djava.security.properties=${OPERATOR_PROPERTIES}"
assert_valid_security_properties \
    "-Djava.security.properties==${OPERATOR_PROPERTIES}"

SPACED_PROPERTIES="${TEMP_DIR}/operator security.properties"
cp "$OPERATOR_PROPERTIES" "$SPACED_PROPERTIES"
SPACED_PROPERTIES_URL="file:${SPACED_PROPERTIES// /%20}"
assert_valid_security_properties \
    "-Djava.security.properties=${SPACED_PROPERTIES_URL}"

ESCAPED_DUPLICATE="${TEMP_DIR}/escaped-duplicate.properties"
cat > "$ESCAPED_DUPLICATE" <<'PROPERTIES'
networkaddress.cache.ttl=45
networkaddress.cache.tt\u006c=-1
PROPERTIES
assert_invalid_security_properties \
    "-Djava.security.properties=${ESCAPED_DUPLICATE}"

CONTINUED_PROPERTIES="${TEMP_DIR}/continued.properties"
cat > "$CONTINUED_PROPERTIES" <<'PROPERTIES'
unrelated.property=value\
networkaddress.cache.ttl=45
PROPERTIES
assert_invalid_security_properties \
    "-Djava.security.properties==${CONTINUED_PROPERTIES}"

INVALID_UNICODE="${TEMP_DIR}/invalid-unicode.properties"
cat > "$INVALID_UNICODE" <<'PROPERTIES'
networkaddress.cache.ttl=\u00ZZ
PROPERTIES
assert_invalid_security_properties \
    "-Djava.security.properties=${INVALID_UNICODE}"

INFINITE_PROPERTIES="${TEMP_DIR}/infinite-security.properties"
echo "networkaddress.cache.ttl=-1" > "$INFINITE_PROPERTIES"
assert_invalid_security_properties \
    "-Djava.security.properties=${INFINITE_PROPERTIES}"

MISSING_OVERRIDE="${TEMP_DIR}/missing-operator-security.properties"
assert_invalid_security_properties \
    "-Djava.security.properties=${MISSING_OVERRIDE}"

assert_bootstrap_accepts_security_properties \
    "-Djava.security.properties=${OPERATOR_PROPERTIES}"
assert_bootstrap_accepts_security_properties \
    "-Djava.security.properties==${OPERATOR_PROPERTIES}"
assert_bootstrap_accepts_security_properties \
    "-Djava.security.properties=${SPACED_PROPERTIES_URL}"
assert_bootstrap_rejects_security_properties \
    "-Djava.security.properties=${ESCAPED_DUPLICATE}"
assert_bootstrap_rejects_security_properties \
    "-Djava.security.properties==${CONTINUED_PROPERTIES}"
assert_bootstrap_rejects_security_properties \
    "-Djava.security.properties=${INFINITE_PROPERTIES}"
assert_bootstrap_rejects_security_properties \
    "-Djava.security.properties=${MISSING_OVERRIDE}"
assert_bootstrap_handles_security_properties_load_failure \
    "-Djava.security.properties=${INVALID_UNICODE}"
assert_bootstrap_skips_security_validation \
    "-Djava.security.properties=${INFINITE_PROPERTIES}"
assert_launcher_rejects_marker_bypass false
assert_launcher_rejects_marker_bypass true
assert_launcher_rejects_security_properties "$MISSING_OVERRIDE"
assert_launcher_rejects_security_properties "$INFINITE_PROPERTIES"
assert_launcher_rejects_security_properties "$INVALID_UNICODE"
assert_launcher_accepts_security_properties "$OPERATOR_PROPERTIES"
assert_launcher_skips_security_validation

# In daemon mode stderr only reaches the stdout log, so when the bootstrap
# rejects a broken operator override, the cause and the override path must be
# mirrored into the server log that start-hugegraph.sh points operators at.
SERVER_LOG="${SERVER_ROOT}/logs/hugegraph-server.log"

assert_daemon_launcher_rejects_override() {
    local properties_path="$1"
    local label="$2"
    : > "$SERVER_LOG"
    if JAVA_OPTIONS="" "$SERVER_SCRIPT" \
       "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" true \
       "-Djava.security.properties=${properties_path}" >/dev/null 2>&1; then
        fail "daemon launcher accepted a ${label} security properties override"
    fi
    grep -Fq "networkaddress.cache.ttl must load as a finite positive integer" \
             "$SERVER_LOG" ||
        fail "${label} override rejection did not reach hugegraph-server.log"
    grep -Fq -- "${properties_path}" "$SERVER_LOG" ||
        fail "${label} override path was not named in hugegraph-server.log"
}

# Invalid content behind the removed read permission keeps this case failing
# even where permission bits do not apply, e.g. when running as root.
UNREADABLE_OVERRIDE="${TEMP_DIR}/unreadable-security.properties"
echo "networkaddress.cache.ttl=-1" > "$UNREADABLE_OVERRIDE"
chmod 000 "$UNREADABLE_OVERRIDE"

assert_daemon_launcher_rejects_override "$MISSING_OVERRIDE" "missing"
assert_daemon_launcher_rejects_override "$UNREADABLE_OVERRIDE" "unreadable"
assert_daemon_launcher_rejects_override "$INFINITE_PROPERTIES" "infinite-TTL"

chmod 600 "$UNREADABLE_OVERRIDE"

MISSING_DEFAULT="${TEMP_DIR}/missing-default-security.properties"
MISSING_DEFAULT_TTL=$("$JAVA_BIN" \
    -Djava.security.properties="$MISSING_DEFAULT" \
    -Djava.security.properties="$OPERATOR_PROPERTIES" "$CHECK_SOURCE")
if [[ "$MISSING_DEFAULT_TTL" != "45" ]]; then
    fail "operator override did not replace a missing bundled properties file"
fi

MOCK_JAVA_HOME="${TEMP_DIR}/mock-java-home"
mkdir -p "${MOCK_JAVA_HOME}/bin"
cat > "${MOCK_JAVA_HOME}/bin/java" <<'MOCK'
#!/bin/bash
if [[ " $* " == *" -version "* ]]; then
    # Real JVMs print this preamble ahead of the version line whenever
    # JAVA_TOOL_OPTIONS or _JAVA_OPTIONS is set.
    if [[ -n "${MOCK_JAVA_PREAMBLE:-}" ]]; then
        echo "${MOCK_JAVA_PREAMBLE}" >&2
    fi
    echo "openjdk version \"${MOCK_JAVA_VERSION:-17}.0.0\"" >&2
    exit 0
fi
printf '%s\n' "$@" > "$CAPTURE_FILE"
printf '%s\n' "${JAVA_TOOL_OPTIONS:-}" > "${CAPTURE_FILE}.tool-options"
printf '%s\n' "${JDK_JAVA_OPTIONS:-}" > "${CAPTURE_FILE}.jdk-java-options"
printf '%s\n' "${_JAVA_OPTIONS:-}" > "${CAPTURE_FILE}.underscore-java-options"
MOCK
chmod +x "${MOCK_JAVA_HOME}/bin/java"

ENABLED_CAPTURE="${TEMP_DIR}/enabled.args"
CAPTURE_FILE="$ENABLED_CAPTURE" JAVA_HOME="$MOCK_JAVA_HOME" \
    STDOUT_MODE=true "$SERVER_SCRIPT" \
    "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" true \
    "-Doperator.marker=preserved \
     -Dhugegraph.security.validate_dns_cache_ttl=false" >/dev/null

assert_argument \
    "-Djava.security.properties=${SECURITY_PROPERTIES}" "$ENABLED_CAPTURE"
assert_argument "@${JVM_MODULE_OPTIONS}" "$ENABLED_CAPTURE"
assert_no_argument '^-Djava\.security\.manager=' "$ENABLED_CAPTURE"
assert_argument \
    "org.apache.hugegraph.bootstrap.HugeGraphServerBootstrap" "$ENABLED_CAPTURE"
assert_argument "true" "$ENABLED_CAPTURE"
assert_argument "-Doperator.marker=preserved" "$ENABLED_CAPTURE"
assert_no_argument '^-D(networkaddress\.cache\.ttl|sun\.net\.inetaddr\.ttl)=' \
                   "$ENABLED_CAPTURE"
# Heap dump and crash log defaults go first in JAVA_TOOL_OPTIONS, so the JVM's
# own precedence lets every operator source override them. Check the values a
# real JVM settles on when started the way a captured launcher run would start
# it: the JAVA_TOOL_OPTIONS, JDK_JAVA_OPTIONS and _JAVA_OPTIONS the launcher
# passed on, and the crash-related -XX: arguments of its command line.
effective_flag() {
    local capture="$1"
    local flag="$2"
    local args=()
    local line
    local flags
    while IFS= read -r line; do
        if [[ "$line" =~ ^-XX:([+-]HeapDumpOnOutOfMemoryError|HeapDumpPath=|ErrorFile=) ]]; then
            args+=("$line")
        fi
    done < "$capture"
    if ! flags=$(JAVA_TOOL_OPTIONS="$(cat "${capture}.tool-options")" \
                 JDK_JAVA_OPTIONS="$(cat "${capture}.jdk-java-options")" \
                 _JAVA_OPTIONS="$(cat "${capture}.underscore-java-options")" \
                 "$JAVA_BIN" ${args[@]+"${args[@]}"} -XX:+PrintFlagsFinal -version \
                 2>"${capture}.flags.err"); then
        fail "JVM rejected the options captured in ${capture}: $(cat "${capture}.flags.err")"
    fi
    # The value sits between "= " and the trailing "{origin}" columns and may
    # contain spaces.
    # A here-string, not a pipe: awk exits early, which would kill printf with
    # SIGPIPE under pipefail.
    awk -v flag="$flag" '$2 == flag {
        sub(/^[^=]*= /, ""); sub(/ *(\{[^}]*\} *)+$/, ""); print; exit }' <<< "$flags"
}

assert_effective_flag() {
    local capture="$1"
    local flag="$2"
    local pattern="$3"
    local value
    value=$(effective_flag "$capture" "$flag")
    [[ "$value" =~ $pattern ]] ||
        fail "effective ${flag} is '${value}', expected to match ${pattern}"
}

LOGS_PATTERN=$(printf '%s' "${SERVER_ROOT}/logs" | sed 's/[][\.*^$+?(){}|]/\\&/g')
# The names carry the host name, the launch time and, if needed, a counter,
# since a restarted container often reuses the PID, pods may share one log
# volume, HotSpot truncates an existing crash log (JDK 17+), and it will not
# write a heap dump over an existing file.
# HeapDumpPath names a directory per launch, so each JVM writes its own
# java_pid<pid>.hprof there.
NAME_ID_PATTERN="[A-Za-z0-9._-]+_[0-9]{8}-[0-9]{6}(-[0-9]+)?"
HEAP_DUMP_PATTERN="^${LOGS_PATTERN}/heapdump_${NAME_ID_PATTERN}$"
ERROR_FILE_PATTERN="^${LOGS_PATTERN}/hs_err_pid%p_${NAME_ID_PATTERN}\.log$"

assert_no_argument '^-XX:([+-]HeapDumpOnOutOfMemoryError|HeapDumpPath=|ErrorFile=)' \
                   "$ENABLED_CAPTURE"
assert_effective_flag "$ENABLED_CAPTURE" HeapDumpOnOutOfMemoryError '^true$'
assert_effective_flag "$ENABLED_CAPTURE" HeapDumpPath "$HEAP_DUMP_PATTERN"
assert_effective_flag "$ENABLED_CAPTURE" ErrorFile "$ERROR_FILE_PATTERN"
ENABLED_DUMP_DIR=$(effective_flag "$ENABLED_CAPTURE" HeapDumpPath)
[[ -d "$ENABLED_DUMP_DIR" ]] ||
    fail "launcher did not create the heap dump directory ${ENABLED_DUMP_DIR}"

# Child JVMs the Server starts inherit JAVA_TOOL_OPTIONS. Two JVMs started with
# the launcher's options, like the Server and a computer job, must each write
# their own heap dump into the launch directory instead of competing for one.
OOM_SOURCE="${TEMP_DIR}/OomCheck.java"
cat > "$OOM_SOURCE" <<'JAVA'
import java.util.ArrayList;
import java.util.List;

public class OomCheck {
    public static void main(String[] args) {
        List<long[]> hold = new ArrayList<>();
        while (true) {
            hold.add(new long[1 << 20]);
        }
    }
}
JAVA
OOM_DUMP_DIR="$ENABLED_DUMP_DIR"
for OOM_RUN in 1 2; do
    JAVA_TOOL_OPTIONS="$(cat "${ENABLED_CAPTURE}.tool-options")" \
        "$JAVA_BIN" -Xmx64m "$OOM_SOURCE" >"${TEMP_DIR}/oom-${OOM_RUN}.log" 2>&1 || true
done
OOM_DUMPS=$(find "$OOM_DUMP_DIR" -maxdepth 1 -name 'java_pid*.hprof' | wc -l)
if [[ "$OOM_DUMPS" -ne 2 ]]; then
    ls -l "$OOM_DUMP_DIR" >&2 || true
    cat "${TEMP_DIR}"/oom-*.log >&2 || true
    fail "expected a heap dump per JVM in ${OOM_DUMP_DIR}, found ${OOM_DUMPS}"
fi
rm -f "$OOM_DUMP_DIR"/java_pid*.hprof

# JAVA_OPTIONS replaces the default heap options; the crash defaults stay, and
# paths given there win.
CUSTOM_OPTIONS_CAPTURE="${TEMP_DIR}/custom-java-options.args"
CAPTURE_FILE="$CUSTOM_OPTIONS_CAPTURE" JAVA_HOME="$MOCK_JAVA_HOME" \
    JAVA_OPTIONS="-Xmx1g -XX:ErrorFile=/operator/hs_err.log \
                  -XX:HeapDumpPath=/operator/dumps" \
    STDOUT_MODE=true "$SERVER_SCRIPT" \
    "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" true >/dev/null

assert_argument "-Xmx1g" "$CUSTOM_OPTIONS_CAPTURE"
assert_effective_flag "$CUSTOM_OPTIONS_CAPTURE" HeapDumpOnOutOfMemoryError '^true$'
assert_effective_flag "$CUSTOM_OPTIONS_CAPTURE" HeapDumpPath '^/operator/dumps$'
assert_effective_flag "$CUSTOM_OPTIONS_CAPTURE" ErrorFile '^/operator/hs_err\.log$'

# The JVM, not the launcher, parses the operator's JAVA_TOOL_OPTIONS: a quoted
# opt-out works, and flag-like text inside a property value changes nothing.
QUOTED_TOOL_CAPTURE="${TEMP_DIR}/quoted-tool-options.args"
CAPTURE_FILE="$QUOTED_TOOL_CAPTURE" JAVA_HOME="$MOCK_JAVA_HOME" \
    JAVA_TOOL_OPTIONS='"-XX:-HeapDumpOnOutOfMemoryError" "-Dmarker=-XX:ErrorFile=/nope"' \
    STDOUT_MODE=true "$SERVER_SCRIPT" \
    "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" true >/dev/null

assert_effective_flag "$QUOTED_TOOL_CAPTURE" HeapDumpOnOutOfMemoryError '^false$'
assert_effective_flag "$QUOTED_TOOL_CAPTURE" ErrorFile "$ERROR_FILE_PATTERN"

# JDK_JAVA_OPTIONS, including an @argfile, also overrides the defaults.
ARGFILE="${TEMP_DIR}/jdk-java-options.args"
echo '-XX:ErrorFile=/operator/argfile-hs_err.log' > "$ARGFILE"
ARGFILE_CAPTURE="${TEMP_DIR}/argfile.args"
CAPTURE_FILE="$ARGFILE_CAPTURE" JAVA_HOME="$MOCK_JAVA_HOME" \
    JDK_JAVA_OPTIONS="@${ARGFILE}" \
    STDOUT_MODE=true "$SERVER_SCRIPT" \
    "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" true >/dev/null

assert_effective_flag "$ARGFILE_CAPTURE" ErrorFile '^/operator/argfile-hs_err\.log$'
assert_effective_flag "$ARGFILE_CAPTURE" HeapDumpPath "$HEAP_DUMP_PATTERN"

# _JAVA_OPTIONS comes after the command line, and -j lands on the command line
# when JAVA_OPTIONS is unset; both override the defaults.
UNDERSCORE_CAPTURE="${TEMP_DIR}/underscore-java-options.args"
CAPTURE_FILE="$UNDERSCORE_CAPTURE" JAVA_HOME="$MOCK_JAVA_HOME" \
    _JAVA_OPTIONS="-XX:HeapDumpPath=/operator/underscore" \
    STDOUT_MODE=true "$SERVER_SCRIPT" \
    "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" true \
    "-XX:ErrorFile=/operator/user-option-hs_err.log" >/dev/null

assert_effective_flag "$UNDERSCORE_CAPTURE" HeapDumpPath '^/operator/underscore$'
assert_effective_flag "$UNDERSCORE_CAPTURE" ErrorFile '^/operator/user-option-hs_err\.log$'

# A launch never reuses a name already taken in logs/, even within the same
# second: with a fixed clock and host name, the counter moves past an existing
# heap dump directory, a crash log and a dangling symlink. The launcher never
# removes dump directories: an empty one from an earlier launch may still be the
# target of a computer-job JVM that outlived its Server.
MOCK_DATE_BIN="${TEMP_DIR}/mock-date-bin"
mkdir -p "$MOCK_DATE_BIN"
printf '#!/bin/bash\necho 20200101-000000\n' > "${MOCK_DATE_BIN}/date"
chmod +x "${MOCK_DATE_BIN}/date"
mkdir -p "${SERVER_ROOT}/logs"
# Only fixtures this run creates are recorded for removal, so an existing file
# with the same name in the supplied distribution is left alone.
add_crash_fixture() {
    local path="$1"
    local kind="$2"
    if [[ ! -e "$path" && ! -L "$path" ]]; then
        CRASH_NAME_FIXTURES+=("$path")
        case "$kind" in
            file) : > "$path" ;;
            empty-dir) mkdir "$path" ;;
            dump-dir) mkdir "$path" && : > "${path}/java_pid1.hprof" ;;
            dangling-link) ln -s "${TEMP_DIR}/missing-target" "$path" ;;
        esac
    fi
}
USED_DUMP_DIR="${SERVER_ROOT}/logs/heapdump_test-pod-a_20200101-000000"
OLD_EMPTY_DUMP_DIR="${SERVER_ROOT}/logs/heapdump_test-pod-a_20191231-000000"
OTHER_HOST_DUMP_DIR="${SERVER_ROOT}/logs/heapdump_test-pod-c_20191231-000000"
add_crash_fixture "$USED_DUMP_DIR" dump-dir
add_crash_fixture "${SERVER_ROOT}/logs/hs_err_pid1_test-pod-a_20200101-000000-1.log" file
add_crash_fixture "${SERVER_ROOT}/logs/heapdump_test-pod-a_20200101-000000-2" dangling-link
LAUNCHER_DUMP_DIRS+=("${SERVER_ROOT}/logs/heapdump_test-pod-a_20200101-000000-3"
                     "${SERVER_ROOT}/logs/heapdump_test_pod-b_20200101-000000")
add_crash_fixture "$OLD_EMPTY_DUMP_DIR" empty-dir
add_crash_fixture "$OTHER_HOST_DUMP_DIR" empty-dir
UNIQUE_NAME_CAPTURE="${TEMP_DIR}/unique-name.args"
CAPTURE_FILE="$UNIQUE_NAME_CAPTURE" JAVA_HOME="$MOCK_JAVA_HOME" \
    HOSTNAME=test-pod-a PATH="${MOCK_DATE_BIN}:${PATH}" STDOUT_MODE=true "$SERVER_SCRIPT" \
    "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" true >/dev/null

assert_effective_flag "$UNIQUE_NAME_CAPTURE" HeapDumpPath \
    "^${LOGS_PATTERN}/heapdump_test-pod-a_20200101-000000-3$"
assert_effective_flag "$UNIQUE_NAME_CAPTURE" ErrorFile \
    "^${LOGS_PATTERN}/hs_err_pid%p_test-pod-a_20200101-000000-3\.log$"
[[ -f "${SERVER_ROOT}/logs/hs_err_pid1_test-pod-a_20200101-000000-1.log" ]] ||
    fail "launcher removed an existing crash log"
[[ -d "$OLD_EMPTY_DUMP_DIR" ]] ||
    fail "launcher removed an empty heap dump directory from an earlier launch"
[[ -d "$OTHER_HOST_DUMP_DIR" ]] ||
    fail "launcher removed another host's heap dump directory"
[[ -f "${USED_DUMP_DIR}/java_pid1.hprof" ]] ||
    fail "launcher removed an existing heap dump"

# Another pod sharing the volume gets its own names in the same second, and
# characters unsafe in a file name are replaced.
OTHER_HOST_CAPTURE="${TEMP_DIR}/other-host.args"
CAPTURE_FILE="$OTHER_HOST_CAPTURE" JAVA_HOME="$MOCK_JAVA_HOME" \
    HOSTNAME='test/pod-b' PATH="${MOCK_DATE_BIN}:${PATH}" STDOUT_MODE=true "$SERVER_SCRIPT" \
    "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" true >/dev/null

assert_effective_flag "$OTHER_HOST_CAPTURE" HeapDumpPath \
    "^${LOGS_PATTERN}/heapdump_test_pod-b_20200101-000000$"
if [[ ${#CRASH_NAME_FIXTURES[@]} -gt 0 ]]; then
    rm -rf "${CRASH_NAME_FIXTURES[@]}"
fi
CRASH_NAME_FIXTURES=()

# A launcher error before Java starts must reach stderr, which start-hugegraph.sh
# passes to the terminal or container log, as well as the server log, and the
# failed launch must not leave a heap dump directory behind.
: > "$SERVER_LOG"
PREFLIGHT_ERROR="${TEMP_DIR}/preflight.err"
PREFLIGHT_DUMP_DIR="${SERVER_ROOT}/logs/heapdump_test-preflight_20200101-000000"
LAUNCHER_DUMP_DIRS+=("$PREFLIGHT_DUMP_DIR")
if JAVA_HOME="$MOCK_JAVA_HOME" HOSTNAME=test-preflight PATH="${MOCK_DATE_BIN}:${PATH}" \
   STDOUT_MODE=true "$SERVER_SCRIPT" \
   "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" true "" "bad-gc" \
   >/dev/null 2>"$PREFLIGHT_ERROR"; then
    fail "launcher accepted an unknown GC option"
fi
grep -Fq "Unrecognized gc option: 'bad-gc'" "$PREFLIGHT_ERROR" ||
    fail "launcher preflight error did not reach stderr"
grep -Fq "Unrecognized gc option: 'bad-gc'" "$SERVER_LOG" ||
    fail "launcher preflight error did not reach the server log"
[[ ! -e "$PREFLIGHT_DUMP_DIR" ]] ||
    fail "a launch that failed before Java started left ${PREFLIGHT_DUMP_DIR}"

# The riscv64 libatomic check runs before anything else, and its error must also
# reach the server log. Mock uname and ldconfig so no libatomic is found; skip if
# this machine has one at a fixed path the launcher also probes.
RISCV_LIBATOMIC_FOUND="false"
for RISCV_CANDIDATE in /lib/riscv64-linux-gnu/libatomic.so.1 \
                       /usr/lib/riscv64-linux-gnu/libatomic.so.1 \
                       /lib64/lp64d/libatomic.so.1 /usr/lib64/lp64d/libatomic.so.1 \
                       /lib64/libatomic.so.1 /usr/lib64/libatomic.so.1; do
    if [[ -r "$RISCV_CANDIDATE" ]]; then
        RISCV_LIBATOMIC_FOUND="true"
    fi
done
if [[ "$RISCV_LIBATOMIC_FOUND" == "false" ]]; then
    MOCK_RISCV_BIN="${TEMP_DIR}/mock-riscv-bin"
    mkdir -p "$MOCK_RISCV_BIN"
    printf '#!/bin/bash\ncase "$1" in -s) echo Linux ;; -m) echo riscv64 ;; *) echo Linux ;; esac\n' \
        > "${MOCK_RISCV_BIN}/uname"
    printf '#!/bin/bash\nexit 0\n' > "${MOCK_RISCV_BIN}/ldconfig"
    chmod +x "${MOCK_RISCV_BIN}/uname" "${MOCK_RISCV_BIN}/ldconfig"
    : > "$SERVER_LOG"
    RISCV_ERROR="${TEMP_DIR}/riscv.err"
    if JAVA_HOME="$MOCK_JAVA_HOME" PATH="${MOCK_RISCV_BIN}:${PATH}" LD_PRELOAD="" \
       STDOUT_MODE=true "$SERVER_SCRIPT" \
       "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" true \
       >/dev/null 2>"$RISCV_ERROR"; then
        fail "launcher started on riscv64 without libatomic"
    fi
    grep -Fq "RISC-V RocksDB requires libatomic.so.1" "$RISCV_ERROR" ||
        fail "the riscv64 libatomic error did not reach stderr"
    grep -Fq "RISC-V RocksDB requires libatomic.so.1" "$SERVER_LOG" ||
        fail "the riscv64 libatomic error did not reach the server log"
fi

# A full or read-only logs volume must not stop the Server: when the dump
# directory cannot be created, the launcher warns and dumps into logs/ itself.
FAILING_MKDIR_BIN="${TEMP_DIR}/failing-mkdir-bin"
mkdir -p "$FAILING_MKDIR_BIN"
cat > "${FAILING_MKDIR_BIN}/mkdir" <<'MKDIR'
#!/bin/bash
for arg in "$@"; do
    case "$arg" in
        */heapdump_*) echo "mkdir: ${arg}: No space left on device" >&2; exit 1 ;;
    esac
done
exec /bin/mkdir "$@"
MKDIR
chmod +x "${FAILING_MKDIR_BIN}/mkdir"
: > "$SERVER_LOG"
NO_DUMP_DIR_CAPTURE="${TEMP_DIR}/no-dump-dir.args"
NO_DUMP_DIR_ERROR="${TEMP_DIR}/no-dump-dir.err"
CAPTURE_FILE="$NO_DUMP_DIR_CAPTURE" JAVA_HOME="$MOCK_JAVA_HOME" \
    PATH="${FAILING_MKDIR_BIN}:${PATH}" STDOUT_MODE=true "$SERVER_SCRIPT" \
    "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" true \
    >/dev/null 2>"$NO_DUMP_DIR_ERROR" ||
    fail "launcher did not start when the heap dump directory could not be created"
grep -Fq "WARN: cannot create ${SERVER_ROOT}/logs/heapdump_" "$NO_DUMP_DIR_ERROR" ||
    fail "launcher did not warn on stderr that the heap dump directory was not created"
grep -Fq "WARN: cannot create ${SERVER_ROOT}/logs/heapdump_" "$SERVER_LOG" ||
    fail "launcher did not log that the heap dump directory was not created"
assert_effective_flag "$NO_DUMP_DIR_CAPTURE" HeapDumpPath "^${LOGS_PATTERN}$"

# An unwritable logs/ is reported on stderr, where the container log keeps it.
# Root can write anywhere, so the check only runs for other users.
if [[ "$(id -u)" -ne 0 ]]; then
    mkdir -p "${TEMP_DIR}/unwritable-logs-dist/logs" "${TEMP_DIR}/unwritable-logs-dist/plugins"
    # The launcher reports the resolved path (TEMP_DIR is a symlink on macOS).
    MINI_ROOT=$(cd "${TEMP_DIR}/unwritable-logs-dist" && pwd -P)
    cp -R "${SERVER_ROOT}/bin" "${SERVER_ROOT}/conf" "${MINI_ROOT}/"
    chmod 555 "${MINI_ROOT}/logs"
    UNWRITABLE_ERROR="${TEMP_DIR}/unwritable-logs.err"
    if JAVA_HOME="$MOCK_JAVA_HOME" STDOUT_MODE=true "${MINI_ROOT}/bin/hugegraph-server.sh" \
       "${MINI_ROOT}/conf/gremlin-server.yaml" "${MINI_ROOT}/conf/rest-server.properties" true \
       >/dev/null 2>"$UNWRITABLE_ERROR"; then
        chmod 755 "${MINI_ROOT}/logs"
        fail "launcher started with an unwritable logs directory"
    fi
    chmod 755 "${MINI_ROOT}/logs"
    grep -Fq "No write permission on directory ${MINI_ROOT}/logs" "$UNWRITABLE_ERROR" ||
        fail "launcher did not report the unwritable logs directory on stderr"
fi

# With telemetry on, the launcher appends its agent to JAVA_TOOL_OPTIONS and
# keeps both the defaults and the operator's flags there. The agent jar fixture
# lives in a copy of bin/ and conf/ under TEMP_DIR, so the test never writes into
# the supplied distribution's plugins/ (or through a symlink placed there).
OT_EXPECTED_MD5=$(grep -E '^ *expected_md5=' "$SERVER_SCRIPT" | cut -d'"' -f2)
MOCK_MD5_BIN="${TEMP_DIR}/mock-md5-bin"
mkdir -p "$MOCK_MD5_BIN"
printf '#!/bin/bash\necho "%s  $1"\n' "$OT_EXPECTED_MD5" > "${MOCK_MD5_BIN}/md5sum"
chmod +x "${MOCK_MD5_BIN}/md5sum"
TELEMETRY_ROOT="${TEMP_DIR}/telemetry-dist"
mkdir -p "${TELEMETRY_ROOT}/logs" "${TELEMETRY_ROOT}/plugins"
cp -R "${SERVER_ROOT}/bin" "${SERVER_ROOT}/conf" "${TELEMETRY_ROOT}/"
: > "${TELEMETRY_ROOT}/plugins/opentelemetry-javaagent.jar"
TELEMETRY_CAPTURE="${TEMP_DIR}/telemetry.args"
CAPTURE_FILE="$TELEMETRY_CAPTURE" JAVA_HOME="$MOCK_JAVA_HOME" \
    PATH="${MOCK_MD5_BIN}:${PATH}" \
    JAVA_TOOL_OPTIONS="-XX:ErrorFile=/operator/hs_err.log" \
    STDOUT_MODE=true "${TELEMETRY_ROOT}/bin/hugegraph-server.sh" \
    "${TELEMETRY_ROOT}/conf/gremlin-server.yaml" \
    "${TELEMETRY_ROOT}/conf/rest-server.properties" true "" "" true \
    >/dev/null 2>"${TEMP_DIR}/telemetry.err" ||
    fail "launcher failed with telemetry on: $(cat "${TEMP_DIR}/telemetry.err")"

TELEMETRY_TOOL_OPTIONS=$(cat "${TELEMETRY_CAPTURE}.tool-options")
[[ "$TELEMETRY_TOOL_OPTIONS" == -XX:+HeapDumpOnOutOfMemoryError\ * ]] ||
    fail "telemetry agent setup dropped the crash-file defaults"
[[ "$TELEMETRY_TOOL_OPTIONS" == *" -XX:ErrorFile=/operator/hs_err.log "* ]] ||
    fail "telemetry agent setup dropped the operator's JAVA_TOOL_OPTIONS"
[[ "$TELEMETRY_TOOL_OPTIONS" =~ \ -javaagent:[^\ ]*/opentelemetry-javaagent\.jar$ ]] ||
    fail "telemetry agent was not added to JAVA_TOOL_OPTIONS"

JDK21_CAPTURE="${TEMP_DIR}/jdk21.args"
CAPTURE_FILE="$JDK21_CAPTURE" JAVA_HOME="$MOCK_JAVA_HOME" \
    MOCK_JAVA_VERSION=21 STDOUT_MODE=true "$SERVER_SCRIPT" \
    "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" true \
    "-Djava.security.manager=operator.Override" >/dev/null

LAST_SECURITY_MANAGER_ARGUMENT=$(grep -E '^-Djava\.security\.manager=' \
                                 "$JDK21_CAPTURE" | tail -n 1)
if [[ "$LAST_SECURITY_MANAGER_ARGUMENT" != \
      "-Djava.security.manager=allow" ]]; then
    fail "operator option overrode the JDK 18+ security manager allowance"
fi

JDK23_CAPTURE="${TEMP_DIR}/jdk23.args"
CAPTURE_FILE="$JDK23_CAPTURE" JAVA_HOME="$MOCK_JAVA_HOME" \
    MOCK_JAVA_VERSION=23 STDOUT_MODE=true "$SERVER_SCRIPT" \
    "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" true >/dev/null

assert_argument "-Djava.security.manager=allow" "$JDK23_CAPTURE"
assert_argument \
    "-Djava.security.properties=${SECURITY_PROPERTIES}" "$JDK23_CAPTURE"

JDK24_ERROR="${TEMP_DIR}/jdk24.err"
if JAVA_HOME="$MOCK_JAVA_HOME" MOCK_JAVA_VERSION=24 STDOUT_MODE=true \
   "$SERVER_SCRIPT" "${CONF}/gremlin-server.yaml" \
   "${CONF}/rest-server.properties" true >/dev/null 2>"$JDK24_ERROR"; then
    fail "launcher accepted a security-enabled JDK 24 runtime"
fi
grep -Fq "JDK 24+ removed the Security Manager" "$JDK24_ERROR" ||
    fail "launcher did not explain the JDK 24 security incompatibility"

# A version-line preamble must not hide the runtime version from the
# version-gated security options above.
VERSION_PREAMBLE="Picked up JAVA_TOOL_OPTIONS: -XX:+UseSerialGC"

PREAMBLE_JDK21_CAPTURE="${TEMP_DIR}/preamble-jdk21.args"
CAPTURE_FILE="$PREAMBLE_JDK21_CAPTURE" JAVA_HOME="$MOCK_JAVA_HOME" \
    MOCK_JAVA_VERSION=21 MOCK_JAVA_PREAMBLE="$VERSION_PREAMBLE" \
    STDOUT_MODE=true "$SERVER_SCRIPT" \
    "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" true >/dev/null

assert_argument "-Djava.security.manager=allow" "$PREAMBLE_JDK21_CAPTURE"

PREAMBLE_JDK24_ERROR="${TEMP_DIR}/preamble-jdk24.err"
if JAVA_HOME="$MOCK_JAVA_HOME" MOCK_JAVA_VERSION=24 \
   MOCK_JAVA_PREAMBLE="$VERSION_PREAMBLE" STDOUT_MODE=true \
   "$SERVER_SCRIPT" "${CONF}/gremlin-server.yaml" \
   "${CONF}/rest-server.properties" true \
   >/dev/null 2>"$PREAMBLE_JDK24_ERROR"; then
    fail "version preamble hid a security-enabled JDK 24 runtime"
fi
grep -Fq "JDK 24+ removed the Security Manager" "$PREAMBLE_JDK24_ERROR" ||
    fail "version preamble defeated the JDK 24 guard"

# An agent loaded through JAVA_TOOL_OPTIONS may print its own banner containing
# 'version "..."' ahead of the JVM's. Reading the agent's version instead of
# the runtime's would reject a supported JDK when the agent version is low ...
AGENT_PREAMBLE=$'Picked up JAVA_TOOL_OPTIONS: -javaagent:apm-agent.jar\nElastic APM agent version "7.2.0" is starting'

AGENT_JDK21_CAPTURE="${TEMP_DIR}/agent-preamble-jdk21.args"
CAPTURE_FILE="$AGENT_JDK21_CAPTURE" JAVA_HOME="$MOCK_JAVA_HOME" \
    MOCK_JAVA_VERSION=21 MOCK_JAVA_PREAMBLE="$AGENT_PREAMBLE" \
    STDOUT_MODE=true "$SERVER_SCRIPT" \
    "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" true >/dev/null

assert_argument "-Djava.security.manager=allow" "$AGENT_JDK21_CAPTURE"

# ... and trip the JDK 24+ security guard when the agent version is high.
HIGH_AGENT_PREAMBLE=$'Picked up JAVA_TOOL_OPTIONS: -javaagent:apm-agent.jar\nAPM agent version "24.0.1" is starting'

HIGH_AGENT_CAPTURE="${TEMP_DIR}/agent-preamble-jdk17.args"
HIGH_AGENT_ERROR="${TEMP_DIR}/agent-preamble-jdk17.err"
CAPTURE_FILE="$HIGH_AGENT_CAPTURE" JAVA_HOME="$MOCK_JAVA_HOME" \
    MOCK_JAVA_VERSION=17 MOCK_JAVA_PREAMBLE="$HIGH_AGENT_PREAMBLE" \
    STDOUT_MODE=true "$SERVER_SCRIPT" \
    "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" true \
    >/dev/null 2>"$HIGH_AGENT_ERROR"

if grep -Fq "JDK 24+ removed the Security Manager" "$HIGH_AGENT_ERROR"; then
    fail "agent banner version tripped the JDK 24+ guard on a supported JDK"
fi
assert_argument \
    "org.apache.hugegraph.bootstrap.HugeGraphServerBootstrap" "$HIGH_AGENT_CAPTURE"
assert_no_argument '^-Djava\.security\.manager=' "$HIGH_AGENT_CAPTURE"

JDK11_ERROR="${TEMP_DIR}/jdk11.err"
if JAVA_HOME="$MOCK_JAVA_HOME" MOCK_JAVA_VERSION=11 STDOUT_MODE=true \
   "$SERVER_SCRIPT" "${CONF}/gremlin-server.yaml" \
   "${CONF}/rest-server.properties" false >/dev/null 2>"$JDK11_ERROR"; then
    fail "launcher accepted a Java 11 runtime"
fi
grep -Fq "version >= 17, current is 11" "${SERVER_ROOT}/logs/hugegraph-server.log" ||
    fail "launcher did not report the Java 17 minimum"

JDK24_DISABLED_CAPTURE="${TEMP_DIR}/jdk24-disabled.args"
CAPTURE_FILE="$JDK24_DISABLED_CAPTURE" JAVA_HOME="$MOCK_JAVA_HOME" \
    MOCK_JAVA_VERSION=24 STDOUT_MODE=true "$SERVER_SCRIPT" \
    "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" false \
    >/dev/null

assert_no_argument '^-Djava\.security\.manager=' "$JDK24_DISABLED_CAPTURE"
assert_no_argument '^-Djava\.security\.properties=' "$JDK24_DISABLED_CAPTURE"
assert_argument "false" "$JDK24_DISABLED_CAPTURE"

DISABLED_CAPTURE="${TEMP_DIR}/disabled.args"
CAPTURE_FILE="$DISABLED_CAPTURE" JAVA_HOME="$MOCK_JAVA_HOME" \
    STDOUT_MODE=true "$SERVER_SCRIPT" \
    "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" false \
    "-Doperator.marker=preserved \
     -Dhugegraph.security.validate_dns_cache_ttl=true" >/dev/null

assert_no_argument '^-Djava\.security\.properties=' "$DISABLED_CAPTURE"
assert_no_argument '^-Djava\.security\.manager=' "$DISABLED_CAPTURE"
assert_argument \
    "org.apache.hugegraph.bootstrap.HugeGraphServerBootstrap" "$DISABLED_CAPTURE"
assert_argument "false" "$DISABLED_CAPTURE"
assert_argument "-Doperator.marker=preserved" "$DISABLED_CAPTURE"

OVERRIDE_CAPTURE="${TEMP_DIR}/override.args"
CAPTURE_FILE="$OVERRIDE_CAPTURE" JAVA_HOME="$MOCK_JAVA_HOME" \
    STDOUT_MODE=true "$SERVER_SCRIPT" \
    "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" true \
    "-Djava.security.properties=${OPERATOR_PROPERTIES}" >/dev/null

LAST_SECURITY_ARGUMENT=$(grep -E '^-Djava\.security\.properties=' \
                         "$OVERRIDE_CAPTURE" | tail -n 1)
if [[ "$LAST_SECURITY_ARGUMENT" != \
      "-Djava.security.properties=${OPERATOR_PROPERTIES}" ]]; then
    fail "operator security properties argument was overwritten"
fi

REPLACEMENT_CAPTURE="${TEMP_DIR}/replacement.args"
CAPTURE_FILE="$REPLACEMENT_CAPTURE" JAVA_HOME="$MOCK_JAVA_HOME" \
    STDOUT_MODE=true "$SERVER_SCRIPT" \
    "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" true \
    "-Djava.security.properties==${OPERATOR_PROPERTIES}" >/dev/null

LAST_SECURITY_ARGUMENT=$(grep -E '^-Djava\.security\.properties=' \
                         "$REPLACEMENT_CAPTURE" | tail -n 1)
if [[ "$LAST_SECURITY_ARGUMENT" != \
      "-Djava.security.properties==${OPERATOR_PROPERTIES}" ]]; then
    fail "operator security properties replacement was overwritten"
fi

mv "$SECURITY_PROPERTIES" "$SECURITY_PROPERTIES_BACKUP"

# An upgrade that reuses an older conf/ must say which file is missing, in the
# log that start-hugegraph.sh points operators at rather than only on stderr.
: > "$SERVER_LOG"
CAPTURE_FILE="${TEMP_DIR}/missing-bundled.args" JAVA_HOME="$MOCK_JAVA_HOME" \
    STDOUT_MODE=true "$SERVER_SCRIPT" \
    "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" true \
    >/dev/null 2>&1
grep -Fq "Missing or unreadable '${SECURITY_PROPERTIES}'" "$SERVER_LOG" ||
    fail "launcher did not name the missing bundled security properties file"

# ... but an operator override legitimately replaces the bundled policy, so the
# same missing file must not be reported as an error in that case.
for OVERRIDE_OPTION in "-Djava.security.properties=${OPERATOR_PROPERTIES}" \
                       "-Djava.security.properties==${OPERATOR_PROPERTIES}"; do
    : > "$SERVER_LOG"
    CAPTURE_FILE="${TEMP_DIR}/missing-bundled-override.args" \
        JAVA_HOME="$MOCK_JAVA_HOME" STDOUT_MODE=true "$SERVER_SCRIPT" \
        "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" true \
        "${OVERRIDE_OPTION}" >/dev/null 2>&1
    if grep -Fq "Missing or unreadable" "$SERVER_LOG"; then
        fail "launcher reported a missing bundled file despite ${OVERRIDE_OPTION}"
    fi
done

# An override that clears itself is not an override, so the error must return.
: > "$SERVER_LOG"
CAPTURE_FILE="${TEMP_DIR}/missing-bundled-cleared.args" \
    JAVA_HOME="$MOCK_JAVA_HOME" STDOUT_MODE=true "$SERVER_SCRIPT" \
    "${CONF}/gremlin-server.yaml" "${CONF}/rest-server.properties" true \
    "-Djava.security.properties=${OPERATOR_PROPERTIES} -Djava.security.properties=" \
    >/dev/null 2>&1
grep -Fq "Missing or unreadable '${SECURITY_PROPERTIES}'" "$SERVER_LOG" ||
    fail "cleared security properties override suppressed the missing-file error"

assert_invalid_security_properties \
    "-Djava.security.properties=${SECURITY_PROPERTIES}"
assert_invalid_security_properties \
    "-Djava.security.properties=${OPERATOR_PROPERTIES}" \
    "-Djava.security.properties="
assert_valid_security_properties \
    "-Djava.security.properties=" \
    "-Djava.security.properties=${OPERATOR_PROPERTIES}"

mkdir "$SECURITY_PROPERTIES"
assert_invalid_security_properties \
    "-Djava.security.properties=${SECURITY_PROPERTIES}"

rmdir "$SECURITY_PROPERTIES"
mv "$SECURITY_PROPERTIES_BACKUP" "$SECURITY_PROPERTIES"

echo "PASS: Java security properties and startup wiring"
