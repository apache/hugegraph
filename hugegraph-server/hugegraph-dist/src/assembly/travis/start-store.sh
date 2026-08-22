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

HOME_DIR=$(pwd)

PROPERTIES_FILE="$HOME_DIR/hugegraph-commons/hugegraph-common/src/main/resources/version.properties"
if [ -f "$PROPERTIES_FILE" ]; then
    set -a
    source "$PROPERTIES_FILE"
    set +a
else
    echo "Error: properties file not found at $PROPERTIES_FILE"
    exit 1
fi

STORE_DIR=$HOME_DIR/hugegraph-store/apache-hugegraph-store-$VersionInBash
TRAVIS_DIR=$(dirname "$0")

source "$TRAVIS_DIR"/ci-service-utils.sh

pushd $STORE_DIR
. bin/start-hugegraph-store.sh
wait_for_http_status HugeGraphStore http://127.0.0.1:8520/v1/health \
                     "$STORE_DIR"/bin/pid "$STORE_DIR" 90 200,401
popd
