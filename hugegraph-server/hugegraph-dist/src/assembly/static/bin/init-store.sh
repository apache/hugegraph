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

. "${BIN}"/util.sh

ensure_path_writable "${PLUGINS}"

if [[ -n "$JAVA_HOME" ]]; then
    JAVA="$JAVA_HOME"/bin/java
    EXT="$JAVA_HOME/jre/lib/ext:$LIB:$PLUGINS"
else
    JAVA=java
    EXT="$LIB:$PLUGINS"
fi

cd "${TOP}" || exit

DEFAULT_JAVA_OPTIONS="--add-exports=java.base/jdk.internal.reflect=ALL-UNNAMED"

echo "Initializing HugeGraph Store..."

# Build classpath with hugegraph*.jar first to avoid class loading conflicts
CP=$(find -L "${LIB}" -name 'hugegraph*.jar' | sort | tr '\n' ':')
CP="$CP":$(find -L "${LIB}" -name '*.jar' \! -name 'hugegraph*' | sort | tr '\n' ':')
CP="$CP":$(find -L "${PLUGINS}" -name '*.jar' | sort | tr '\n' ':')
$JAVA -cp $CP ${DEFAULT_JAVA_OPTIONS} \
org.apache.hugegraph.cmd.InitStore "${CONF}"/rest-server.properties

echo "Initialization finished."
