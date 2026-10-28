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

if [ -z "$GC_OPTION" ];then
  GC_OPTION=""
fi
if [ -z "$USER_OPTION" ];then
  USER_OPTION=""
fi
if [ -z "$OPEN_TELEMETRY" ];then
  OPEN_TELEMETRY="false"
fi
if [ -z "$DAEMON" ]; then
    DAEMON="true"
fi

while getopts "c:d:g:i:j:l:o:y:" arg; do
    case ${arg} in
        c) CONF_OVERRIDE="$OPTARG" ;;
        g) GC_OPTION="$OPTARG" ;;
        i) PID_FILE_OVERRIDE="$OPTARG" ;;
        j) USER_OPTION="$OPTARG" ;;
        l) LOGS_OVERRIDE="$OPTARG" ;;
        o) PLUGINS_OVERRIDE="$OPTARG" ;;
        # Telemetry is used to collect metrics, traces and logs
        d) DAEMON="$OPTARG" ;;
        y) OPEN_TELEMETRY="$OPTARG" ;;
        ?) echo "USAGE: $0 [-d true|false] [-c conf_dir] [-g g1|ZGC] [-i pid_file] [-j xxx] [-l logs_dir] [-o plugins_dir] [-y true|false]" && exit 1 ;;
    esac
done

function abs_path() {
    SOURCE="${BASH_SOURCE[0]}"
    while [ -h "$SOURCE" ]; do
        DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    echo "$( cd -P "$( dirname "$SOURCE" )" && pwd )"
}

BIN=$(abs_path)
TOP="$(cd "$BIN"/../ && pwd)"

. "$BIN"/util.sh

# Validate the RAW overrides before canonicalization: command substitution
# strips trailing newlines, which would hide an embedded CR/LF.
reject_unsafe_path "$CONF_OVERRIDE" "-c conf dir" crlf || exit 1
reject_unsafe_path "$LOGS_OVERRIDE" "-l logs dir" crlf || exit 1
reject_unsafe_path "$PID_FILE_OVERRIDE" "-i pid file" crlf || exit 1
# The plugins dir is joined into ':'-separated classpath/agent settings
reject_unsafe_path "$PLUGINS_OVERRIDE" "-o plugins dir" classpath || exit 1

# Canonicalize relative path overrides to absolute paths.
CONF_OVERRIDE="$(canonicalize_dir "$CONF_OVERRIDE" nocreate)" || exit 1
LOGS_OVERRIDE="$(canonicalize_dir "$LOGS_OVERRIDE")" || exit 1
PLUGINS_OVERRIDE="$(canonicalize_dir "$PLUGINS_OVERRIDE")" || exit 1
PID_FILE_OVERRIDE="$(canonicalize_file "$PID_FILE_OVERRIDE")" || exit 1

# Re-validate the *effective*, canonicalized plugins path: cd/pwd resolves a
# relative override against the current working directory, which can
# introduce a ':' (or other unsafe char) that wasn't present in the raw value
# (e.g. launching from a directory such as "/tmp/work:one").
reject_unsafe_path "$PLUGINS_OVERRIDE" "-o plugins dir (resolved)" classpath || exit 1

CONF="${CONF_OVERRIDE:-$TOP/conf}"
LIB="$TOP/lib"
PLUGINS="${PLUGINS_OVERRIDE:-$TOP/plugins}"
LOGS="${LOGS_OVERRIDE:-$TOP/logs}"
OUTPUT="${LOGS}/hugegraph-pd-stdout.log"
GITHUB="https://github.com"
PID_FILE="${PID_FILE_OVERRIDE:-$BIN/pid}"
JVM_MODULE_OPTIONS="$BIN/jvm-module.options"

if [[ ! -f "$JVM_MODULE_OPTIONS" || ! -r "$JVM_MODULE_OPTIONS" ]]; then
    echo "Error: JVM module options file is missing or unreadable: $JVM_MODULE_OPTIONS" >&2
    exit 1
fi


ensure_path_writable "$LOGS"
ensure_path_writable "$PLUGINS"
ensure_path_writable "$(dirname "$PID_FILE")"

# These are resolved relative to the working directory (set to $TOP below), not
# relative to $CONF, so a relative value would silently keep reading the
# packaged conf dir even though -c points somewhere else.
if [ -n "$CONF_OVERRIDE" ]; then
    # The file is also handed to the JVM as -Dspring.config.location below.
    # Spring Boot would reject a missing location, but only after the JVM is
    # up -- by which point this script has written the PID file and exited 0,
    # so a supervisor sees a successful start and a process that dies moments
    # later. Fail here instead, while the exit code still means something.
    ensure_file_readable "${CONF}/application.yml" "PD config" || exit 1
    for PROPERTY in verify-path license-path; do
        VALUE=$(sed -n "s/^[[:space:]]*${PROPERTY}:[[:space:]]*//p" \
                "${CONF}/application.yml" | head -1 | tr -d "'\"")
        reject_relative_conf_path "$VALUE" "$PROPERTY" "${CONF}/application.yml" || exit 1
    done
fi

# The maximum and minimum heap memory that service can use
MAX_MEM=$((32 * 1024))
MIN_MEM=$((1 * 512))
EXPECT_JDK_VERSION=17

# Change to $BIN's parent
cd "${TOP}" || exit

# Find Java
if [ "$JAVA_HOME" = "" ]; then
    JAVA="java"
else
    JAVA="$JAVA_HOME/bin/java"
fi

# check jdk version
JAVA_VERSION=$("$JAVA" -version 2>&1 |
               awk -F'"' '/^(java|openjdk) version "/ {print $2; exit}' |
               sed 's/^1\.//' | cut -d'.' -f1)
JAVA_VERSION="${JAVA_VERSION%%[!0-9]*}"
if [[ -z $JAVA_VERSION || $JAVA_VERSION -lt $EXPECT_JDK_VERSION ]]; then
    echo "Please make sure that the JDK is installed and the version >= $EXPECT_JDK_VERSION"  >> "${OUTPUT}"
    exit 1
fi

# Set Java options.
# Script-generated options are kept in arrays so that overridden paths
# containing spaces (CONF/LOGS/PLUGINS) remain a single argument when passed
# to java. User-supplied strings ($JAVA_OPTIONS / $USER_OPTION) keep the
# historical whitespace-splitting contract. Built in JAVA_OPTIONS_ARR (not
# JAVA_OPTIONS) so the exported scalar JAVA_OPTIONS stays intact for any
# child inheriting this environment -- Bash arrays cannot be exported.
JAVA_OPTIONS_ENV="${JAVA_OPTIONS}"
JAVA_OPTIONS_ARR=()
if [ "$JAVA_OPTIONS_ENV" = "" ]; then
    XMX=$(calc_xmx $MIN_MEM $MAX_MEM)
    if [ $? -ne 0 ]; then
        echo "Failed to start HugeGraphPDServer, requires at least ${MIN_MEM}m free memory" \
             >> "${OUTPUT}"
        exit 1
    fi
    JAVA_OPTIONS_ARR=("-Xms${MIN_MEM}m" "-Xmx${XMX}m" "-XX:+HeapDumpOnOutOfMemoryError"
                  "-XX:HeapDumpPath=${LOGS}")

    # Split user-supplied options on whitespace (legacy behaviour).
    # Use NUL (not newline) as the read delimiter so multi-line values
    # (e.g. Helm javaOpts/Compose `|` blocks) are not silently truncated
    # at the first line.
    USER_OPTIONS=()
    read -r -d '' -a USER_OPTIONS <<< "${USER_OPTION}"$'\0' || true
    JAVA_OPTIONS_ARR+=(${USER_OPTIONS[@]+"${USER_OPTIONS[@]}"})

    # Rolling out detailed GC logs
    # If enabling this: first add
    #   reject_unsafe_path "$LOGS" "-l logs dir (GC log)" xlog || exit 1
    # before it. $LOGS is not currently guarded against ':' -- the JVM's
    # -Xlog grammar also delimits on ':', so an embedded one desyncs the
    # parser and the JVM refuses to start (see start-hugegraph-store.sh).
    #JAVA_OPTIONS_ARR+=("-Xlog:gc=info:file=${LOGS}/gc.log:time,uptime,level,tags:filecount=3,filesize=100m")
else
    read -r -d '' -a JAVA_OPTIONS_ARR <<< "${JAVA_OPTIONS_ENV}"$'\0' || true
fi

# Keep JVM/caller GC selection by default; explicitly select G1 when requested.
case "$GC_OPTION" in
    ""|g1|G1)
        if [[ "$GC_OPTION" == g1 || "$GC_OPTION" == G1 ]]; then
            echo "Using G1GC"
            JAVA_OPTIONS_ARR+=("-XX:+UseG1GC")
        else
            echo "Using JVM garbage collector configuration"
        fi
        JAVA_OPTIONS_ARR=("-XX:+ParallelRefProcEnabled" "-XX:InitiatingHeapOccupancyPercent=50"
                      "-XX:G1RSetUpdatingPauseTimePercent=5"
                      ${JAVA_OPTIONS_ARR[@]+"${JAVA_OPTIONS_ARR[@]}"})
        ;;
    zgc|ZGC)
        echo "Using ZGC as the default garbage collector (requires Java 17 or later)"
        JAVA_OPTIONS_ARR+=("-XX:+UseZGC" "-XX:+UnlockExperimentalVMOptions"
                       "-XX:ConcGCThreads=2" "-XX:ParallelGCThreads=6"
                       "-XX:ZCollectionInterval=120" "-XX:ZAllocationSpikeTolerance=5"
                       "-XX:+UnlockDiagnosticVMOptions" "-XX:-ZProactive")
        ;;
    *)
        echo "Unrecognized gc option: '$GC_OPTION', supported options: g1, ZGC" >> "${OUTPUT}"
        exit 1
esac

if [ "${OPEN_TELEMETRY}" == "true" ]; then
    OT_JAR="opentelemetry-javaagent.jar"
    OT_JAR_PATH="${PLUGINS}/${OT_JAR}"

    if [[ ! -e "${OT_JAR_PATH}" ]]; then
        echo "## Downloading ${OT_JAR}..."
        download "${PLUGINS}" \
            "${GITHUB}/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.1.0/${OT_JAR}"

        if [[ ! -e "${OT_JAR_PATH}" ]]; then
            echo "## Error: Failed to download ${OT_JAR}." >>"${OUTPUT}"
            exit 1
        fi
    fi

    # Note: remember update it if we change the jar
    expected_md5="e3bcbbe8ed9b6d840fa4c333b36f369f"
    actual_md5=$(md5sum "${OT_JAR_PATH}" | awk '{print $1}')

    if [[ "${expected_md5}" != "${actual_md5}" ]]; then
        echo "## Error: MD5 checksum verification failed for ${OT_JAR_PATH}." >>"${OUTPUT}"
        echo "## Tips: Remove the file and try again." >>"${OUTPUT}"
        exit 1
    fi

    # Note: check carefully if multi "javeagent" params are set
    # The JVM splits JAVA_TOOL_OPTIONS on whitespace, so an overridden $PLUGINS
    # containing a space would turn the agent option into two arguments. The JVM
    # honours quoted options, so wrap it in a quote the path does not contain.
    if [[ ${OT_JAR_PATH} != *"'"* ]]; then
        AGENT_QUOTE="'"
    elif [[ ${OT_JAR_PATH} != *\"* ]]; then
        AGENT_QUOTE='"'
    else
        echo "## Error: ${OT_JAR_PATH} contains both quote characters, which JAVA_TOOL_OPTIONS cannot carry." >>"${OUTPUT}"
        exit 1
    fi
    export JAVA_TOOL_OPTIONS="${AGENT_QUOTE}-javaagent:${OT_JAR_PATH}${AGENT_QUOTE}"
    export OTEL_TRACES_EXPORTER=otlp
    export OTEL_METRICS_EXPORTER=none
    export OTEL_LOGS_EXPORTER=none
    export OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=grpc
    # 127.0.0.1:4317 is the port of otel-collector running in Docker located in
    # 'hugegraph-server/hugegraph-dist/docker/example/docker-compose-trace.yaml'.
    # Make sure the otel-collector is running before starting HugeGraphPD.
    export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://127.0.0.1:4317
    export OTEL_RESOURCE_ATTRIBUTES=service.name=pd
fi

#if [ "${JMX_EXPORT_PORT}" != "" ] && [ ${JMX_EXPORT_PORT} -ne 0 ] ; then
#  JAVA_OPTIONS="${JAVA_OPTIONS} -javaagent:${LIB}/jmx_prometheus_javaagent-0.16.1.jar=${JMX_EXPORT_PORT}:${CONF}/jmx_exporter.yml"
#fi
if [ "$(ps -ef | grep -v grep | grep java | grep -cF -- "${CONF}")" -ne 0 ]; then
   echo "HugeGraphPDServer is already running..."
   exit 0
fi

JVM_OPTIONS=("-Dlog4j.configurationFile=${CONF}/log4j2.xml"
             "-Dlogging.config=${CONF}/log4j2.xml"
             "-DLOG_PATH=${LOGS}"
             "-Dbolt.log.path=${LOGS}"
             "-Dlogging.path=${LOGS}"
             "-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager")

source "$BIN/preload-topling.sh" || exit 1
BOOT_JARS=("${LIB}"/hg-pd-service-*.jar)
if [ "${#BOOT_JARS[@]}" -ne 1 ] || [ ! -f "${BOOT_JARS[0]}" ]; then
    echo "Error: expected one component executable JAR in $LIB" >&2
    exit 1
fi
JAVA_MAIN=(-jar "${BOOT_JARS[0]}")
if [ -n "${TOPLING_RUNTIME_CLASSPATH:-}" ]; then
    JAVA_MAIN=(-cp "${TOPLING_RUNTIME_CLASSPATH}:${BOOT_JARS[0]}" org.springframework.boot.loader.JarLauncher)
fi

# Turn on security check
# NOTE: "$JVM_MODULE_OPTIONS" and "$OUTPUT" are passed/redirected as single
# quoted arguments, and JVM_OPTIONS/JAVA_OPTIONS_ARR are expanded as arrays
# ("${JVM_OPTIONS[@]}"), so an overridden CONF/LOGS/PLUGINS path containing a
# space stays one argument instead of being split by word-splitting.
if [[ $DAEMON == "true" ]]; then
    echo "Starting HugeGraphPDServer in daemon mode..."
    if [[ "${STDOUT_MODE:-false}" == "true" ]]; then
        exec "${JAVA}" "@${JVM_MODULE_OPTIONS}" -Dname="HugeGraphPD" \
            "${JVM_OPTIONS[@]}" "${JAVA_OPTIONS_ARR[@]}" \
            -Dspring.config.location="${CONF}/application.yml" "${JAVA_MAIN[@]}" "$@" &
    else
        exec "${JAVA}" "@${JVM_MODULE_OPTIONS}" -Dname="HugeGraphPD" \
            "${JVM_OPTIONS[@]}" "${JAVA_OPTIONS_ARR[@]}" \
            -Dspring.config.location="${CONF}/application.yml" "${JAVA_MAIN[@]}" "$@" >> "${OUTPUT}" 2>&1 &
    fi
    PID="$!"
    # Write pid to file
    echo "$PID" > "$PID_FILE"
    echo "[+pid] $PID"
else
    echo "Starting HugeGraphPDServer in foreground mode..."
    # Write $$ before exec — exec replaces this shell with Java, so $$ becomes Java's PID
    echo "$$" > "$PID_FILE"
    echo "[+pid] $$"
    if [[ "${STDOUT_MODE:-false}" == "true" ]]; then
        exec "${JAVA}" "@${JVM_MODULE_OPTIONS}" -Dname="HugeGraphPD" \
            "${JVM_OPTIONS[@]}" "${JAVA_OPTIONS_ARR[@]}" \
            -Dspring.config.location="${CONF}/application.yml" "${JAVA_MAIN[@]}" "$@"
    else
        exec "${JAVA}" "@${JVM_MODULE_OPTIONS}" -Dname="HugeGraphPD" \
            "${JVM_OPTIONS[@]}" "${JAVA_OPTIONS_ARR[@]}" \
            -Dspring.config.location="${CONF}/application.yml" "${JAVA_MAIN[@]}" "$@" >> "${OUTPUT}" 2>&1
    fi
fi
