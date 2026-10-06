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

# Fatal launcher errors go to stderr, which reaches the terminal or the container
# log, and, when possible, to ${OUTPUT}, the log start-hugegraph.sh points
# operators at.
report_error() {
    echo "$1" >&2
    if [[ -w "${LOGS}" ]]; then
        { echo "$1" >> "${OUTPUT}"; } 2>/dev/null || true
    fi
}

ensure_path_writable "$LOGS" report_error
ensure_path_writable "$PLUGINS" report_error

# The maximum and minimum heap memory that service can use
MAX_MEM=$((32 * 1024))
MIN_MEM=$((1 * 512))
MIN_JAVA_VERSION=17
# JDK 24 removed the Security Manager (JEP 486): "-Djava.security.manager=allow"
# is a fatal VM initialization error there and System.setSecurityManager() always
# throws, so HugeSecurityManager cannot be installed on newer runtimes.
MAX_SECURITY_JAVA_VERSION=23
JVM_MODULE_OPTIONS="${BIN}/jvm-module.options"

# Add the slf4j-log4j12 binding
CP=$(find -L $LIB -name 'log4j-slf4j-impl*.jar' | sort | tr '\n' ':')
# Add the jars in lib that start with "hugegraph"
CP="$CP":$(find -L $LIB -name 'hugegraph*.jar' | sort | tr '\n' ':')
# Add the remaining jars in lib.
CP="$CP":$(find -L $LIB -name '*.jar' \
    \! -name 'hugegraph*' \
    \! -name 'log4j-slf4j-impl*.jar' | sort | tr '\n' ':')
# Add the jars in ext (at any subdirectory depth)
CP="$CP":$(find -L $EXT -name '*.jar' | sort | tr '\n' ':')
# Add the jars in plugins (at any subdirectory depth), check "javaagent" related jars carefully
CP="$CP":$(find -L $PLUGINS -name '*.jar' | sort | tr '\n' ':')

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
    report_error "Make sure the JDK is installed and the version >= $MIN_JAVA_VERSION, current is $JAVA_VERSION"
    exit 1
fi

if [[ ! -r ${JVM_MODULE_OPTIONS} ]]; then
    report_error "Missing or unreadable JVM module options file: ${JVM_MODULE_OPTIONS}"
    exit 1
fi

# Set Java options
if [ "$JAVA_OPTIONS" = "" ]; then
    XMX=$(calc_xmx $MIN_MEM $MAX_MEM)
    if [ $? -ne 0 ]; then
        report_error "Failed to start HugeGraphServer, requires at least ${MIN_MEM}MB free memory"
        exit 1
    fi
    JAVA_OPTIONS="-Xms${MIN_MEM}m -Xmx${XMX}m ${USER_OPTION}"

    # Rolling out detailed GC logs
    #JAVA_OPTIONS="${JAVA_OPTIONS} -XX:+UseGCLogFileRotation -XX:GCLogFileSize=10M -XX:NumberOfGCLogFiles=3 \
    #              -Xloggc:./logs/gc.log -XX:+PrintHeapAtGC -XX:+PrintGCDetails -XX:+PrintGCDateStamps"
fi

# Keep heap dumps and JVM crash logs in $LOGS whatever JAVA_OPTIONS holds. The
# defaults go first in JAVA_TOOL_OPTIONS, which the JVM reads before the operator's
# own JAVA_TOOL_OPTIONS, JDK_JAVA_OPTIONS, the command line (JAVA_OPTIONS and -j)
# and _JAVA_OPTIONS, so any of those overrides them. The JVM does that parsing
# itself, including quoted options and @argfiles.
# Child JVMs the Server starts (computer jobs, for example) inherit these options,
# so every path must stay unique per JVM. ErrorFile expands %p to each JVM's PID.
# HeapDumpPath does not expand %p, but when it names an existing directory each JVM
# writes java_pid<its pid>.hprof inside it, so it points at a directory per launch.
# A restarted container often reuses the PID, and HotSpot will not overwrite an
# existing crash log or heap dump, so the names carry the host name (the pod name
# on Kubernetes, so pods sharing one log volume do not collide), the launch time,
# and a counter that skips names already used in $LOGS.
crash_files_exist() {
    [[ -e "${LOGS}/heapdump_$1" ]] && return 0
    local file
    for file in "${LOGS}"/hs_err_pid*_"$1".log; do
        [[ -e ${file} ]] && return 0
    done
    return 1
}
LAUNCH_HOST=$(printf '%s' "${HOSTNAME:-localhost}" | tr -c 'A-Za-z0-9._-' '_')
# Earlier launches on this host leave their heap dump directory behind even when
# nothing was dumped. Remove those that are empty; rmdir never removes a dump.
for HEAP_DUMP_DIR in "${LOGS}"/heapdump_"${LAUNCH_HOST}"_*/; do
    [[ -d ${HEAP_DUMP_DIR} ]] && rmdir "${HEAP_DUMP_DIR}" 2>/dev/null
done
LAUNCH_STAMP="${LAUNCH_HOST}_$(date +%Y%m%d-%H%M%S)"
LAUNCH_ID="${LAUNCH_STAMP}"
LAUNCH_SUFFIX=0
while crash_files_exist "${LAUNCH_ID}"; do
    LAUNCH_SUFFIX=$((LAUNCH_SUFFIX + 1))
    LAUNCH_ID="${LAUNCH_STAMP}-${LAUNCH_SUFFIX}"
done
HEAP_DUMP_DIR="${LOGS}/heapdump_${LAUNCH_ID}"
mkdir -p "${HEAP_DUMP_DIR}" || {
    report_error "Failed to create heap dump directory ${HEAP_DUMP_DIR}"
    exit 1
}
CRASH_OPTIONS="-XX:+HeapDumpOnOutOfMemoryError"
CRASH_OPTIONS="${CRASH_OPTIONS} \"-XX:HeapDumpPath=${HEAP_DUMP_DIR}\""
CRASH_OPTIONS="${CRASH_OPTIONS} \"-XX:ErrorFile=${LOGS}/hs_err_pid%p_${LAUNCH_ID}.log\""
export JAVA_TOOL_OPTIONS="${CRASH_OPTIONS}${JAVA_TOOL_OPTIONS:+ ${JAVA_TOOL_OPTIONS}}"

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
        echo "Using ZGC as the default garbage collector (requires Java 17 or later)"
        JAVA_OPTIONS="${JAVA_OPTIONS} -XX:+UseZGC -XX:+UnlockExperimentalVMOptions \
                                      -XX:ConcGCThreads=2 -XX:ParallelGCThreads=6 \
                                      -XX:ZCollectionInterval=120 -XX:ZAllocationSpikeTolerance=5 \
                                      -XX:+UnlockDiagnosticVMOptions -XX:-ZProactive"
        ;;
    *)
        report_error "Unrecognized gc option: '$GC_OPTION', default use g1, options only support 'ZGC' now"
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
        report_error "${SECURITY_UNSUPPORTED_MSG}"
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
        report_error "ERROR: Missing or unreadable '${SECURITY_PROPERTIES}'.
An upgraded deployment that reuses an older conf/ directory must add this file,
or supply its own -Djava.security.properties=<file> setting a finite positive
networkaddress.cache.ttl."
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
            report_error "## Error: Failed to download ${OT_JAR}."
            exit 1
        fi
    fi

    # Note: remember update it if we change the jar 
    expected_md5="e3bcbbe8ed9b6d840fa4c333b36f369f"
    actual_md5=$(md5sum "${OT_JAR_PATH}" | awk '{print $1}')

    if [[ "${expected_md5}" != "${actual_md5}" ]]; then
        report_error "## Error: MD5 checksum verification failed for ${OT_JAR_PATH}."
        report_error "## Tips: Remove the file and try again."
        exit 1
    fi

    # Note: check carefully if multi "javeagent" params are set
    # Append, so the crash-file defaults and the operator's JAVA_TOOL_OPTIONS stay.
    export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:+${JAVA_TOOL_OPTIONS} }-javaagent:${PLUGINS}/${OT_JAR}"
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
    exec ${JAVA} @"${JVM_MODULE_OPTIONS}" -Dname="HugeGraphServer" ${JVM_OPTIONS} ${JAVA_OPTIONS} \
        ${SECURITY_MANAGER_OPTION} -cp ${CLASSPATH}: \
        org.apache.hugegraph.bootstrap.HugeGraphServerBootstrap \
        ${OPEN_SECURITY_CHECK} ${GREMLIN_SERVER_CONF} ${REST_SERVER_CONF}
else
    exec ${JAVA} @"${JVM_MODULE_OPTIONS}" -Dname="HugeGraphServer" ${JVM_OPTIONS} ${JAVA_OPTIONS} \
        ${SECURITY_MANAGER_OPTION} -cp ${CLASSPATH}: \
        org.apache.hugegraph.bootstrap.HugeGraphServerBootstrap \
        ${OPEN_SECURITY_CHECK} ${GREMLIN_SERVER_CONF} ${REST_SERVER_CONF} \
        >> ${LOGS}/hugegraph-server-stdout.log 2>&1
fi
