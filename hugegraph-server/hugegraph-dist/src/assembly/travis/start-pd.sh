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
set -e

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

PD_DIR=$HOME_DIR/hugegraph-pd/apache-hugegraph-pd-$VersionInBash

# conf/application.yml ships auth.secret-key empty on purpose, so PD would
# refuse every authenticated REST request. Supply a test-only secret; it must
# match the value the PD test suites send.
export SPRING_APPLICATION_JSON='{"auth":{"secret-key":"pd-ci-test-secret-not-for-production"}}'

pushd $PD_DIR
. bin/start-hugegraph-pd.sh
sleep 10
popd
