#!/usr/bin/env bash
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

BASE_PATH=$(cd "$(dirname "$0")" && pwd)
ROOT_PATH=$(cd "$BASE_PATH/../../.." && pwd)
FILE_NAME=${1:-known-dependencies.txt}
MAVEN_COMMAND=${MAVEN_COMMAND:-mvn}

if [[ "$FILE_NAME" = /* ]]; then
    OUTPUT_PATH=$FILE_NAME
else
    OUTPUT_PATH=$BASE_PATH/$FILE_NAME
fi

# Only remove this invocation's new temporary directory, never a shared folder.
DEPENDENCY_TEMP=$(mktemp -d "${TMPDIR:-/tmp}/hugegraph-dependencies.XXXXXX")
trap 'rm -rf -- "$DEPENDENCY_TEMP"' EXIT

cd "$ROOT_PATH"
"$MAVEN_COMMAND" -B -ntp dependency:copy-dependencies -DincludeScope=runtime \
    "-DoutputDirectory=$DEPENDENCY_TEMP/runtime"
REVISION=$("$MAVEN_COMMAND" -q -DforceStdout help:evaluate -Dexpression=project.version)
python3 "$BASE_PATH/dependency_inventory.py" collect \
    --runtime "$DEPENDENCY_TEMP/runtime" --root "$ROOT_PATH" \
    --revision "$REVISION" --output "$OUTPUT_PATH"
