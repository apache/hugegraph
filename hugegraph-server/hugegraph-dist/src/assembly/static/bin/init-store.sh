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

BIN=$(abs_path)
TOP="$(cd "${BIN}"/../ && pwd)"

# Support overrides via flags (for standalone invocation) or env vars (inherited
# when launched from start-hugegraph.sh).
while getopts "c:o:" arg; do
    case ${arg} in
        c) CONF_OVERRIDE="$OPTARG" ;;
        o) PLUGINS_OVERRIDE="$OPTARG" ;;
        ?) echo "USAGE: $0 [-c conf_dir] [-o plugins_dir]" && exit 1 ;;
    esac
done

. "${BIN}"/util.sh

# Validate the RAW overrides before canonicalization: command substitution
# strips trailing newlines, which would hide an embedded CR/LF.
reject_unsafe_path "$CONF_OVERRIDE" "-c conf dir" crlf || exit 1
# The plugins dir is joined into the ':'-separated CLASSPATH
reject_unsafe_path "$PLUGINS_OVERRIDE" "-o plugins dir" classpath || exit 1

# Canonicalize relative path overrides to absolute paths
CONF_OVERRIDE="$(canonicalize_dir "$CONF_OVERRIDE" nocreate)" || exit 1
PLUGINS_OVERRIDE="$(canonicalize_dir "$PLUGINS_OVERRIDE")" || exit 1

# Re-validate the *effective*, canonicalized plugins path: cd/pwd resolves a
# relative override against the current working directory, which can
# introduce a ':' (or other unsafe char) that wasn't present in the raw value
# (e.g. launching from a directory such as "/tmp/work:one").
reject_unsafe_path "$PLUGINS_OVERRIDE" "-o plugins dir (resolved)" classpath || exit 1

CONF="${CONF_OVERRIDE:-$TOP/conf}"
LIB="$TOP/lib"
PLUGINS="${PLUGINS_OVERRIDE:-$TOP/plugins}"
JVM_MODULE_OPTIONS="${BIN}/jvm-module.options"

configure_riscv64_libatomic || exit 1
ensure_path_writable "${PLUGINS}"

# Verify the config exists before reading it: read_property returns an empty
# string for a missing file, which is indistinguishable from an unset property.
# InitStore is handed this same file below, so fail here with a clear message.
ensure_file_readable "${CONF}/rest-server.properties" "REST server config" || exit 1

# InitStore reads 'graphs' relative to the working directory (set to $TOP
# below), not relative to $CONF, so a relative value would silently initialize
# the packaged graphs instead of the ones under an overridden -c directory.
GRAPHS_DIR=$(read_property "${CONF}/rest-server.properties" "graphs")
reject_relative_conf_path "$GRAPHS_DIR" "graphs" "${CONF}/rest-server.properties" || exit 1

if [[ -n "$JAVA_HOME" ]]; then
    JAVA="$JAVA_HOME"/bin/java
else
    JAVA=java
fi

cd "${TOP}" || exit

if [[ ! -r ${JVM_MODULE_OPTIONS} ]]; then
    echo "Missing or unreadable JVM module options file: ${JVM_MODULE_OPTIONS}" >&2
    exit 1
fi

echo "Initializing HugeGraph Store..."

# Build classpath with hugegraph*.jar first to avoid class loading conflicts
CP=$(find -L "${LIB}" -name 'hugegraph*.jar' | sort | tr '\n' ':')
CP="$CP":$(find -L "${LIB}" -name '*.jar' \
    \! -path "${LIB}/arthas/*" \! -name 'hugegraph*' | sort | tr '\n' ':')
CP="$CP":$(find -L "${PLUGINS}" -name '*.jar' | sort | tr '\n' ':')
source "$BIN/preload-topling.sh" || exit 1
CP="${TOPLING_RUNTIME_CLASSPATH:+$TOPLING_RUNTIME_CLASSPATH:}$CP"
"${JAVA}" @"${JVM_MODULE_OPTIONS}" -cp "$CP" \
org.apache.hugegraph.cmd.InitStore "${CONF}"/rest-server.properties
INIT_STORE_STATUS=$?
if [[ ${INIT_STORE_STATUS} -ne 0 ]]; then
    exit "${INIT_STORE_STATUS}"
fi

echo "Initialization finished."
