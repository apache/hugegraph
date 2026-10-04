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
    SERVER_WORKFLOW="${SOURCE_ROOT}/.github/workflows/server-ci.yml"
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
SECURITY_PROPERTIES_BACKUP="${TEMP_DIR}/java-security.properties"

cleanup() {
    if [[ -d "$SECURITY_PROPERTIES" ]]; then
        rmdir "$SECURITY_PROPERTIES"
    fi
    if [[ -f "$SECURITY_PROPERTIES_BACKUP" &&
          ! -e "$SECURITY_PROPERTIES" ]]; then
        mv "$SECURITY_PROPERTIES_BACKUP" "$SECURITY_PROPERTIES"
    fi
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
    if grep -Eq 'Log4j|NetUtils|UnknownHost|hostname' "$error_file"; then
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
