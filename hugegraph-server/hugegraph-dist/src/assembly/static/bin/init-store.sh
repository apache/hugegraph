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
CONF="$TOP/conf"
LIB="$TOP/lib"
PLUGINS="$TOP/plugins"
JVM_MODULE_OPTIONS="${BIN}/jvm-module.options"

. "${BIN}"/util.sh

configure_riscv64_libatomic || exit 1
ensure_path_writable "${PLUGINS}"

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
CP="$CP":$(find -L "${LIB}" -name '*.jar' \! -name 'hugegraph*' | sort | tr '\n' ':')
CP="$CP":$(find -L "${PLUGINS}" -name '*.jar' | sort | tr '\n' ':')
"${JAVA}" @"${JVM_MODULE_OPTIONS}" -cp "$CP" \
org.apache.hugegraph.cmd.InitStore "${CONF}"/rest-server.properties
INIT_STORE_STATUS=$?
if [[ ${INIT_STORE_STATUS} -ne 0 ]]; then
    exit "${INIT_STORE_STATUS}"
fi

echo "Initialization finished."
