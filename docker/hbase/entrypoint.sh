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

echo "Starting HBase ${HBASE_VERSION} standalone..."

# Start services explicitly to avoid SSH-based helper assumptions in containers
${HBASE_HOME}/bin/hbase-daemon.sh start zookeeper
${HBASE_HOME}/bin/hbase-daemon.sh start master
${HBASE_HOME}/bin/hbase-daemon.sh start regionserver

echo "HBase started. Waiting for ZooKeeper on port 2181..."
zk_attempts=0
until nc -z localhost 2181; do
    zk_attempts=$((zk_attempts + 1))
    if [ "$zk_attempts" -ge 120 ]; then
        echo "Timed out waiting for ZooKeeper on 2181"
        ${HBASE_HOME}/bin/hbase-daemon.sh status || true
        exit 1
    fi
    sleep 1
done
echo "ZooKeeper is ready."

echo "Waiting for HBase Master..."
master_attempts=0
until echo "status 'simple'" | ${HBASE_HOME}/bin/hbase shell -n 2>/dev/null | grep -E -q "([1-9][0-9]*[[:space:]]+live[[:space:]]+servers|[1-9][0-9]*[[:space:]]+servers|servers:[[:space:]]*[1-9])"; do
    master_attempts=$((master_attempts + 1))
    if [ "$master_attempts" -ge 180 ]; then
        echo "Timed out waiting for HBase master/regionserver readiness"
        ${HBASE_HOME}/bin/hbase-daemon.sh status || true
        tail -n 80 ${HBASE_HOME}/logs/hbase--*.out 2>/dev/null || true
        exit 1
    fi
    sleep 3
done
echo "HBase is ready. Master + RegionServer online."

# Tail all daemon logs so `docker logs` includes startup/runtime issues
exec tail -F ${HBASE_HOME}/logs/hbase--*.out ${HBASE_HOME}/logs/hbase--*.log 2>/dev/null || \
     tail -f /dev/null

