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
REPORT_DIR=$2
REPORT_FILE=$REPORT_DIR/jacoco-api-test.xml

TRAVIS_DIR=$(dirname $0)
VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
SERVER_DIR=hugegraph-server/apache-hugegraph-server-$VERSION
RAFT1_DIR=hugegraph-raft1
RAFT2_DIR=hugegraph-raft2
RAFT3_DIR=hugegraph-raft3
CONF=$SERVER_DIR/conf/graphs/hugegraph.properties
REST_SERVER_CONF=$SERVER_DIR/conf/rest-server.properties
GREMLIN_SERVER_CONF=$SERVER_DIR/conf/gremlin-server.yaml

JACOCO_PORT=36320
RAFT_TOOLS=$RAFT1_DIR/bin/raft-tools.sh
RAFT_LEADER="127.0.0.1:8091"

mvn package -Dmaven.test.skip=true

# mkdir for each raft-server
cp -r $SERVER_DIR $RAFT1_DIR
cp -r $SERVER_DIR $RAFT2_DIR
cp -r $SERVER_DIR $RAFT3_DIR

# config raft-server (must keep '/.')
cp -rf $TRAVIS_DIR/conf-raft1/. $RAFT1_DIR/conf/
cp -rf $TRAVIS_DIR/conf-raft2/. $RAFT2_DIR/conf/
cp -rf $TRAVIS_DIR/conf-raft3/. $RAFT3_DIR/conf/

# start server
$TRAVIS_DIR/start-server.sh $RAFT1_DIR $BACKEND $JACOCO_PORT || (cat $RAFT1_DIR/logs/hugegraph-server.log && exit 1) &
$TRAVIS_DIR/start-server.sh $RAFT2_DIR $BACKEND || (cat $RAFT2_DIR/logs/hugegraph-server.log && exit 1) &
$TRAVIS_DIR/start-server.sh $RAFT3_DIR $BACKEND || (cat $RAFT3_DIR/logs/hugegraph-server.log && exit 1)

export HUGEGRAPH_USERNAME=admin
export HUGEGRAPH_PASSWORD=pa
$RAFT_TOOLS --set-leader "hugegraph" "$RAFT_LEADER"

# run api-test
mvn test -pl hugegraph-server/hugegraph-test -am -P api-test,$BACKEND || (cat $RAFT1_DIR/logs/hugegraph-server.log && exit 1)

$TRAVIS_DIR/build-report.sh $BACKEND $JACOCO_PORT $REPORT_FILE

# stop server
$TRAVIS_DIR/stop-server.sh $RAFT1_DIR
$TRAVIS_DIR/stop-server.sh $RAFT2_DIR
$TRAVIS_DIR/stop-server.sh $RAFT3_DIR
