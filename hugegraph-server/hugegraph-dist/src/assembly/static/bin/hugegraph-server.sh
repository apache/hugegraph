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

function abs_path() {
    SOURCE="${BASH_SOURCE[0]}"
    while [[ -h "$SOURCE" ]]; do
        DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    cd -P "$(dirname "$SOURCE")" && pwd
}

if [[ $# -lt 3 ]]; then
    echo "USAGE: $0 GREMLIN_SERVER_CONF REST_SERVER_CONF OPEN_SECURITY_CHECK"
    echo " e.g.: $0 conf/gremlin-server.yaml conf/rest-server.properties true"
    exit 1
fi

BIN=$(abs_path)
TOP="$(cd "$BIN"/../ && pwd)"
CONF="$TOP/conf"
LIB="$TOP/lib"
EXT="$TOP/ext"
PLUGINS="$TOP/plugins"
LOGS="$TOP/logs"
OUTPUT=${LOGS}/hugegraph-server.log
GITHUB="https://github.com"

export HUGEGRAPH_HOME="$TOP"
. "${BIN}"/util.sh

configure_riscv64_libatomic || exit 1

# Parse the server arguments in array way
SERVER_ARGS=("$@")
GREMLIN_SERVER_CONF="${SERVER_ARGS[0]:-}"
REST_SERVER_CONF="${SERVER_ARGS[1]:-}"
OPEN_SECURITY_CHECK="${SERVER_ARGS[2]:-}"
# Param will be empty str("") if not set
USER_OPTION="${SERVER_ARGS[3]:-}"
GC_OPTION="${SERVER_ARGS[4]:-}"
OPEN_TELEMETRY="${SERVER_ARGS[5]:-}"

ensure_path_writable "$LOGS"
ensure_path_writable "$PLUGINS"

# The maximum and minimum heap memory that service can use
MAX_MEM=$((32 * 1024))
MIN_MEM=$((1 * 512))
MIN_JAVA_VERSION=11
# JDK 24 removed the Security Manager (JEP 486): "-Djava.security.manager=allow"
# is a fatal VM initialization error there and System.setSecurityManager() always
# throws, so HugeSecurityManager cannot be installed on newer runtimes.
MAX_SECURITY_JAVA_VERSION=23

# Add the slf4j-log4j12 binding
CP=$(find_standard_lib_jars "$LIB" 'log4j-slf4j-impl*.jar' |
    sort | tr '\n' ':')
# Add the jars in lib that start with "hugegraph"
CP="$CP":$(find_standard_lib_jars "$LIB" 'hugegraph*.jar' |
    sort | tr '\n' ':')
# Add the remaining jars in lib.
CP="$CP":$(find_standard_lib_jars "$LIB" '*.jar' \
    'hugegraph*' 'log4j-slf4j-impl*.jar' | sort | tr '\n' ':')
# Add the jars in ext (at any subdirectory depth)
CP="$CP":$(find -L "$EXT" -name '*.jar' | sort | tr '\n' ':')
# Add the jars in plugins (at any subdirectory depth), check "javaagent" related jars carefully
CP="$CP":$(find -L "$PLUGINS" -name '*.jar' | sort | tr '\n' ':')

# (Cygwin only) Use ; classpath separator and reformat paths for Windows ("C:\foo")
[[ $(uname) = CYGWIN* ]] && CP="$(cygpath -p -w "$CP")"

export CLASSPATH="${CLASSPATH:-}:$CP"

# Change to $BIN's parent
cd "${TOP}" || exit 1

# Find java & enable server option
if [ "$JAVA_HOME" = "" ]; then
    JAVA="java -server"
else
    JAVA="$JAVA_HOME/bin/java -server"
fi

# Pick the JVM banner line explicitly, anchored to its "java version"/"openjdk
# version" prefix: whenever JAVA_TOOL_OPTIONS or _JAVA_OPTIONS is set the JVM
# prints a preamble first ("Picked up JAVA_TOOL_OPTIONS: ..."), and an agent
# loaded that way may print its own banner containing 'version "..."' (an APM
# agent, for example), so matching any line with 'version "' can read the
# agent version instead of the runtime version.
JAVA_VERSION=$($JAVA -version 2>&1 |
               awk -F'"' '/^(java|openjdk) version "/ {print $2; exit}' |
               sed 's/^1\.//' | cut -d'.' -f1)
# Drop any pre-release suffix, e.g. "24-ea" -> "24"
JAVA_VERSION="${JAVA_VERSION%%[!0-9]*}"
if [[ -z $JAVA_VERSION || $JAVA_VERSION -lt $MIN_JAVA_VERSION ]]; then
    echo "Make sure the JDK is installed and the version >= $MIN_JAVA_VERSION, current is $JAVA_VERSION" \
         >> "${OUTPUT}"
    exit 1
fi

# Set Java options
if [ "$JAVA_OPTIONS" = "" ]; then
    XMX=$(calc_xmx $MIN_MEM $MAX_MEM)
    if [ $? -ne 0 ]; then
        echo "Failed to start HugeGraphServer, requires at least ${MIN_MEM}MB free memory" >> "${OUTPUT}"
        exit 1
    fi
    JAVA_OPTIONS="-Xms${MIN_MEM}m -Xmx${XMX}m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${LOGS} ${USER_OPTION}"

    # Rolling out detailed GC logs
    #JAVA_OPTIONS="${JAVA_OPTIONS} -XX:+UseGCLogFileRotation -XX:GCLogFileSize=10M -XX:NumberOfGCLogFiles=3 \
    #              -Xloggc:./logs/gc.log -XX:+PrintHeapAtGC -XX:+PrintGCDetails -XX:+PrintGCDateStamps"
fi

if [[ $JAVA_VERSION -gt 9 ]]; then
    JAVA_OPTIONS="${JAVA_OPTIONS} --add-exports=java.base/jdk.internal.reflect=ALL-UNNAMED \
                                  --add-modules=jdk.unsupported \
                                  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED "
fi

# Using G1GC as the default garbage collector (Recommended for large memory machines)
# mention: zgc is only available on ARM-Mac with java > 13
case "$GC_OPTION" in
    "")
        echo "Using G1GC as the default garbage collector"
        JAVA_OPTIONS="${JAVA_OPTIONS} -XX:+ParallelRefProcEnabled \
                                      -XX:InitiatingHeapOccupancyPercent=50 \
                                      -XX:G1RSetUpdatingPauseTimePercent=5"
        ;;
    zgc|ZGC)
        echo "Using ZGC as the default garbage collector (Only support Java 11+)"
        JAVA_OPTIONS="${JAVA_OPTIONS} -XX:+UseZGC -XX:+UnlockExperimentalVMOptions \
                                      -XX:ConcGCThreads=2 -XX:ParallelGCThreads=6 \
                                      -XX:ZCollectionInterval=120 -XX:ZAllocationSpikeTolerance=5 \
                                      -XX:+UnlockDiagnosticVMOptions -XX:-ZProactive"
        ;;
    *)
        echo "Unrecognized gc option: '$GC_OPTION', default use g1, options only support 'ZGC' now" >> ${OUTPUT}
        exit 1
esac

JVM_OPTIONS="-Dlog4j.configurationFile=${CONF}/log4j2.xml"
SECURITY_MANAGER_OPTION=""
if [[ ${OPEN_SECURITY_CHECK} == "true" ]]; then
    if [[ ${JAVA_VERSION} -gt ${MAX_SECURITY_JAVA_VERSION} ]]; then
        SECURITY_UNSUPPORTED_MSG=$(cat <<EOF
The security check requires Java ${MIN_JAVA_VERSION}-${MAX_SECURITY_JAVA_VERSION}, current is ${JAVA_VERSION}.
JDK 24+ removed the Security Manager (JEP 486), so HugeSecurityManager can no longer be installed.
Run the server on Java ${MAX_SECURITY_JAVA_VERSION} or lower, or start it with the security check
disabled: 'start-hugegraph.sh -s false'.
EOF
)
        echo "${SECURITY_UNSUPPORTED_MSG}" >&2
        echo "${SECURITY_UNSUPPORTED_MSG}" >> "${OUTPUT}"
        exit 1
    fi

    SECURITY_PROPERTIES="${CONF}/java-security.properties"
    if [[ ! -r ${SECURITY_PROPERTIES} ]]; then
        # An operator may deliberately replace the bundled policy with their own
        # -Djava.security.properties=<file>, which the JVM applies last and which
        # makes a missing bundled file harmless. Track the last such option, since
        # an empty value clears any earlier override. The override itself is not
        # validated here: only the JVM's own properties parsing decides what it
        # loads to, so the bootstrap stays the single validator and mirrors its
        # rejection into the server log (see hugegraph.bootstrap.error.log below).
        SECURITY_PROPERTIES_OVERRIDDEN="false"
        for OPTION in ${JAVA_OPTIONS} ${_JAVA_OPTIONS:-}; do
            case "${OPTION}" in
                -Djava.security.properties=)
                    SECURITY_PROPERTIES_OVERRIDDEN="false" ;;
                -Djava.security.properties=?*)
                    SECURITY_PROPERTIES_OVERRIDDEN="true" ;;
            esac
        done
    fi
    if [[ ! -r ${SECURITY_PROPERTIES} &&
          ${SECURITY_PROPERTIES_OVERRIDDEN:-false} == "false" ]]; then
        # The bootstrap validates the effective policy and refuses to start, but
        # its stderr goes to the stdout log in daemon mode. Name the cause here
        # so it also reaches the log start-hugegraph.sh points operators at.
        cat >> "${OUTPUT}" <<EOF
ERROR: Missing or unreadable '${SECURITY_PROPERTIES}'.
An upgraded deployment that reuses an older conf/ directory must add this file,
or supply its own -Djava.security.properties=<file> setting a finite positive
networkaddress.cache.ttl.
EOF
    fi
    JVM_OPTIONS="${JVM_OPTIONS} \
                 -Djava.security.properties=${SECURITY_PROPERTIES}"
    if [[ ${JAVA_VERSION} -ge 18 ]]; then
        # Required to install HugeSecurityManager programmatically on JDK 18+.
        SECURITY_MANAGER_OPTION="-Djava.security.manager=allow"
    fi
fi

if [ "${OPEN_TELEMETRY}" == "true" ]; then
    OT_JAR="opentelemetry-javaagent.jar"
    OT_JAR_PATH="${PLUGINS}/${OT_JAR}"

    if [[ ! -e "${OT_JAR_PATH}" ]]; then
        echo "## Downloading ${OT_JAR}..."
        download "${PLUGINS}" \
            "${GITHUB}/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.1.0/${OT_JAR}"

        if [[ ! -e "${OT_JAR_PATH}" ]]; then
            echo "## Error: Failed to download ${OT_JAR}." >>${OUTPUT}
            exit 1
        fi
    fi

    # Note: remember update it if we change the jar 
    expected_md5="e3bcbbe8ed9b6d840fa4c333b36f369f"
    actual_md5=$(md5sum "${OT_JAR_PATH}" | awk '{print $1}')

    if [[ "${expected_md5}" != "${actual_md5}" ]]; then
        echo "## Error: MD5 checksum verification failed for ${OT_JAR_PATH}." >>${OUTPUT}
        echo "## Tips: Remove the file and try again." >>${OUTPUT}
        exit 1
    fi

    # Note: check carefully if multi "javeagent" params are set
    export JAVA_TOOL_OPTIONS="-javaagent:${PLUGINS}/${OT_JAR}"
    export OTEL_TRACES_EXPORTER=otlp
    export OTEL_METRICS_EXPORTER=none
    export OTEL_LOGS_EXPORTER=none
    export OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=grpc
    # 127.0.0.1:4317 is the port of otel-collector running in Docker located in
    # 'hugegraph-server/hugegraph-dist/docker/example/docker-compose-trace.yaml'.
    # Make sure the otel-collector is running before starting HugeGraphServer.
    export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://127.0.0.1:4317
    export OTEL_RESOURCE_ATTRIBUTES=service.name=server
fi

if [[ "${STDOUT_MODE:-false}" != "true" ]]; then
    # Daemon stderr only reaches hugegraph-server-stdout.log, so a bootstrap
    # rejection of the effective DNS policy (a broken operator override
    # included) would be invisible in the log start-hugegraph.sh points
    # operators at. Let the bootstrap mirror its fatal errors there.
    JVM_OPTIONS="${JVM_OPTIONS} -Dhugegraph.bootstrap.error.log=${OUTPUT}"
fi

# Turn on security check
if [[ "${STDOUT_MODE:-false}" == "true" ]]; then
    exec ${JAVA} -Dname="HugeGraphServer" ${JVM_OPTIONS} ${JAVA_OPTIONS} \
        ${SECURITY_MANAGER_OPTION} -cp "${CLASSPATH}:" \
        org.apache.hugegraph.bootstrap.HugeGraphServerBootstrap \
        ${OPEN_SECURITY_CHECK} ${GREMLIN_SERVER_CONF} ${REST_SERVER_CONF}
else
    exec ${JAVA} -Dname="HugeGraphServer" ${JVM_OPTIONS} ${JAVA_OPTIONS} \
        ${SECURITY_MANAGER_OPTION} -cp "${CLASSPATH}:" \
        org.apache.hugegraph.bootstrap.HugeGraphServerBootstrap \
        ${OPEN_SECURITY_CHECK} ${GREMLIN_SERVER_CONF} ${REST_SERVER_CONF} \
        >> ${LOGS}/hugegraph-server-stdout.log 2>&1
fi
