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

set -euo pipefail

BIN=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
TOP=$(cd -- "$BIN/.." && pwd)
JAVA=java
if [[ -n "${JAVA_HOME:-}" ]]; then
    JAVA="$JAVA_HOME/bin/java"
fi
shopt -s nullglob
JARS=("$TOP"/lib/hg-store-node-*.jar)
if [[ ${#JARS[@]} -ne 1 ]]; then
    echo "Expected exactly one hg-store-node JAR in $TOP/lib" >&2
    exit 2
fi
MODULE_OPTIONS=()
while IFS= read -r option || [[ -n "$option" ]]; do
    [[ -z "$option" || "$option" == \#* ]] || MODULE_OPTIONS+=("$option")
done < "$BIN/jvm-module.options"
source "$BIN/preload-topling.sh"
CHECKER_CLASSPATH="${JARS[0]}"
if [[ -n "${TOPLING_RUNTIME_CLASSPATH:-}" ]]; then
    CHECKER_CLASSPATH="${TOPLING_RUNTIME_CLASSPATH}:$CHECKER_CLASSPATH"
fi
cd -- "$TOP"
exec "$JAVA" "${MODULE_OPTIONS[@]}" \
    -Dloader.main=org.apache.hugegraph.store.node.GraphIdCheckCommand \
    -cp "$CHECKER_CLASSPATH" org.springframework.boot.loader.PropertiesLauncher "$@"
