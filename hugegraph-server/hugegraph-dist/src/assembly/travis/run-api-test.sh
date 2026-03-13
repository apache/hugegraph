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
REPORT_FILE=$REPORT_DIR/jacoco-api-test-for-raft.xml

TRAVIS_DIR=$(dirname $0)
VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
SERVER_DIR=hugegraph-server/apache-hugegraph-server-$VERSION/
CONF=$SERVER_DIR/conf/graphs/hugegraph.properties
REST_SERVER_CONF=$SERVER_DIR/conf/rest-server.properties
GREMLIN_SERVER_CONF=$SERVER_DIR/conf/gremlin-server.yaml
JACOCO_PORT=36320

mvn package -Dmaven.test.skip=true -ntp

# add mysql dependency
wget -P $SERVER_DIR/lib/ https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar

if [[ ! -e "$SERVER_DIR/lib/ikanalyzer-2012_u6.jar" ]]; then
  wget -P $SERVER_DIR/lib/ https://raw.githubusercontent.com/apache/hugegraph-doc/ik_binary/dist/server/ikanalyzer-2012_u6.jar
fi

# config rest-server
sed -i '/^#*auth\.authenticator=/d' $REST_SERVER_CONF
sed -i '/^#*auth\.admin_token=/d' $REST_SERVER_CONF
echo "auth.authenticator=org.apache.hugegraph.auth.StandardAuthenticator" >> $REST_SERVER_CONF
echo "auth.admin_token=pa" >> $REST_SERVER_CONF

# config hugegraph.properties
sed -i 's/gremlin.graph=.*/gremlin.graph=org.apache.hugegraph.auth.HugeFactoryAuthProxy/' $CONF

# config gremlin-server
echo "
authentication: {
  authenticator: org.apache.hugegraph.auth.StandardAuthenticator,
  authenticationHandler: org.apache.hugegraph.auth.WsAndHttpBasicAuthHandler,
  config: {tokens: conf/rest-server.properties}
}" >> $GREMLIN_SERVER_CONF

# start server
$TRAVIS_DIR/start-server.sh $SERVER_DIR $BACKEND $JACOCO_PORT || (cat $SERVER_DIR/logs/hugegraph-server.log && exit 1)

# run api-test
mvn test -pl hugegraph-server/hugegraph-test -am -P api-test,$BACKEND || (cat $SERVER_DIR/logs/hugegraph-server.log && exit 1)

$TRAVIS_DIR/build-report.sh $BACKEND $JACOCO_PORT $REPORT_FILE

# stop server
$TRAVIS_DIR/stop-server.sh $SERVER_DIR
