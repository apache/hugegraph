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
set -ev

BACKEND=$1
JACOCO_PORT=$2
JACOCO_REPORT_FILE=$3

TRAVIS_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$TRAVIS_DIR/../../../../.." && pwd)

function command_available() {
    local cmd=$1
    [[ -x "$(command -v "$cmd")" ]]
}

function download_to_dir() {
    local dir=$1
    local url=$2
    local file="$dir/$(basename "$url")"

    mkdir -p "$dir"
    if command_available "curl"; then
        curl -fL "$url" -o "$file"
    elif command_available "wget"; then
        wget -P "$dir" "$url"
    else
        echo "Required curl or wget but they are unavailable"
        exit 1
    fi
}

OPTION_CLASS_FILES_BACKEND="--classfiles hugegraph-$BACKEND/target/classes/org/apache/hugegraph"
if [ "$BACKEND" == "memory" ]; then
    # hugegraph-memory is the same as hugegraph-core
    OPTION_CLASS_FILES_BACKEND=""
fi

case "$JACOCO_REPORT_FILE" in
    /*) REPORT_FILE=$JACOCO_REPORT_FILE ;;
    *) REPORT_FILE="$REPO_ROOT/$JACOCO_REPORT_FILE" ;;
esac
mkdir -p "$(dirname "$REPORT_FILE")"

cd "$REPO_ROOT/hugegraph-server/hugegraph-test"
mvn jacoco:dump@pull-test-data -Dapp.host=localhost -Dapp.port=$JACOCO_PORT -Dskip.dump=false
cd "$REPO_ROOT/hugegraph-server"

if [[ ! -e "${TRAVIS_DIR}/jacococli.jar" ]]; then
  download_to_dir "${TRAVIS_DIR}" \
                  "https://github.com/apache/hugegraph-doc/raw/binary-1.0/dist/server/jacococli.jar"
fi

java -jar $TRAVIS_DIR/jacococli.jar report hugegraph-test/target/jacoco-it.exec \
     --classfiles hugegraph-dist/target/classes/org/apache/hugegraph \
     --classfiles hugegraph-api/target/classes/org/apache/hugegraph \
     --classfiles hugegraph-core/target/classes/org/apache/hugegraph \
     ${OPTION_CLASS_FILES_BACKEND} --xml "${REPORT_FILE}"
