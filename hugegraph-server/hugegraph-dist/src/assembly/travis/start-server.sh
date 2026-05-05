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

TRAVIS_DIR=$(cd "$(dirname "$0")" && pwd)
BASE_DIR=$1
BACKEND=$2
JACOCO_PORT=$3

JACOCO_DIR=${TRAVIS_DIR}
JACOCO_JAR=${JACOCO_DIR}/jacocoagent.jar

BIN=$BASE_DIR/bin
CONF=$BASE_DIR/conf/graphs/hugegraph.properties
REST_CONF=$BASE_DIR/conf/rest-server.properties
GREMLIN_CONF=$BASE_DIR/conf/gremlin-server.yaml

. "${BIN}"/util.sh

function sed_in_place() {
    local expression=$1
    local file=$2

    case "$(uname)" in
        Darwin) sed -i '' "$expression" "$file" ;;
        *) sed -i "$expression" "$file" ;;
    esac
}

case "$BACKEND" in
    memory)
        SERIALIZER=text
        ;;
    hbase)
        SERIALIZER=hbase
        ;;
    rocksdb|hstore)
        SERIALIZER=binary
        ;;
    *)
        echo "Unsupported backend: $BACKEND"
        exit 1
        ;;
esac

# Set backend and serializer
write_property "$CONF" "backend" "$BACKEND"
write_property "$CONF" "serializer" "$SERIALIZER"

# Set timeout for hbase
if [ "$BACKEND" == "hbase" ]; then
    echo "restserver.request_timeout=200" >> $REST_CONF
    echo "gremlinserver.timeout=200" >> $REST_CONF
    sed_in_place 's/evaluationTimeout.*/evaluationTimeout: 200000/' "$GREMLIN_CONF"
fi

# Set usePD=true for hstore
if [ "$BACKEND" == "hstore" ]; then
    echo "usePD=true" >> $REST_CONF
fi

# Append schema.sync_deletion=true to config file
echo "schema.sync_deletion=true" >> $CONF

JACOCO_OPTION=""
if [ -n "$JACOCO_PORT" ]; then
    if [[ ! -e "${JACOCO_JAR}" ]]; then
      download "${JACOCO_DIR}" "https://github.com/apache/hugegraph-doc/raw/binary-1.0/dist/server/jacocoagent.jar"
    fi
    JACOCO_OPTION="-javaagent:${JACOCO_JAR}=includes=*,port=${JACOCO_PORT},destfile=jacoco-it.exec,output=tcpserver"
fi

SERVER_JAVA_OPTIONS="${SERVER_JAVA_OPTIONS:-}"
if [ -n "$SERVER_JAVA_OPTIONS" ]; then
    if [ -n "$JACOCO_OPTION" ]; then
        JACOCO_OPTION="${JACOCO_OPTION} ${SERVER_JAVA_OPTIONS}"
    else
        JACOCO_OPTION="${SERVER_JAVA_OPTIONS}"
    fi
fi

echo -e "pa" | $BIN/init-store.sh
$BIN/start-hugegraph.sh -j "$JACOCO_OPTION" -t 60
