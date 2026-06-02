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

HBASE_HOSTNAME="${HBASE_HOSTNAME:-hbase}"
HBASE_MASTER_HOSTNAME="${HBASE_MASTER_HOSTNAME:-${HBASE_HOSTNAME}}"
HBASE_REGIONSERVER_HOSTNAME="${HBASE_REGIONSERVER_HOSTNAME:-${HBASE_HOSTNAME}}"
HBASE_SITE_XML="${HBASE_HOME}/conf/hbase-site.xml"

escape_sed_replacement() {
    local value="$1"
    if [[ "$value" == *$'\n'* ]]; then
        echo "Property values must not contain newlines" >&2
        exit 1
    fi
    printf '%s' "$value" | sed -e 's/[\\&|]/\\&/g'
}

set_xml_property_value() {
    local property_name="$1"
    local property_value
    property_value=$(escape_sed_replacement "$2")

    # The in-place replacement below expects the standard HBase layout where
    # <name>...</name> is followed by <value>...</value> on the next line.
    # Fail loudly if the property entry is missing to avoid silent misconfig.
    if ! grep -Fq "<name>${property_name}</name>" "${HBASE_SITE_XML}"; then
        echo "Missing required property '${property_name}' in ${HBASE_SITE_XML}" >&2
        exit 1
    fi

    sed -i "/<name>${property_name//./\\.}<\\/name>/ {n; s|<value>.*</value>|<value>${property_value}</value>|;}" "${HBASE_SITE_XML}"
}

set_xml_property_value "hbase.master.hostname" "${HBASE_MASTER_HOSTNAME}"
set_xml_property_value "hbase.regionserver.hostname" "${HBASE_REGIONSERVER_HOSTNAME}"

echo "Starting HBase ${HBASE_VERSION} standalone..."
echo "HBase container hostname fallback: ${HBASE_HOSTNAME}"
echo "HBase advertised master hostname: ${HBASE_MASTER_HOSTNAME}"
echo "HBase advertised regionserver hostname: ${HBASE_REGIONSERVER_HOSTNAME}"


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
        tail -n 80 ${HBASE_HOME}/logs/hbase-*.out ${HBASE_HOME}/logs/hbase-*.log \
            2>/dev/null || true
        exit 1
    fi
    sleep 3
done
echo "HBase is ready. Master + RegionServer online."

# Tail all daemon logs so `docker logs` includes startup/runtime issues
shopt -s nullglob
log_files=("${HBASE_HOME}/logs"/hbase-*.out "${HBASE_HOME}/logs"/hbase-*.log)
if [ ${#log_files[@]} -gt 0 ]; then
    exec tail -F "${log_files[@]}"
fi
exec tail -f /dev/null

