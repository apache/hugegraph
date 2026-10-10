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

TRAVIS_DIR=$(cd "$(dirname "$0")" && pwd)
HBASE_IMAGE="openeuler/hbase:2.6.5-oe2403sp3@sha256:9451eaec6ba241106d32421a6ab00e3b728a43689249b57f1f16b653b639f73f"
HBASE_CONTAINER="hg-ci-hbase-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}-${GITHUB_JOB:-test}"
HBASE_LABEL="hugegraph.ci.hbase=${HBASE_CONTAINER}"

cleanup() {
    # Remove only the container owned by this run/job, never other Docker resources.
    local container_id
    container_id=$(docker ps -aq --filter "label=${HBASE_LABEL}")
    if [[ -n "$container_id" ]]; then
        docker logs --tail 100 "$container_id" || true
        docker rm -f "$container_id"
    fi
}

case "${1:-}" in
    stop)
        cleanup
        exit 0
        ;;
    '')
        # The complete cold pull/start/read-write readiness path shares one budget.
        exec timeout --signal=TERM --kill-after=10s 300s bash "$0" start
        ;;
    start) ;;
    *) echo "Usage: $0 [stop]" >&2; exit 2 ;;
esac

ready=false
finish() {
    if [[ "$ready" != true ]]; then
        cleanup || true
    fi
}
trap finish EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

started=$SECONDS
docker pull "$HBASE_IMAGE"
pull_seconds=$((SECONDS - started))
version=$(docker run --rm --entrypoint /usr/local/hbase/bin/hbase "$HBASE_IMAGE" version)
[[ "$version" == *"HBase 2.6.5"* ]] || { echo "Unexpected HBase version: $version" >&2; exit 1; }

# CI runs on Linux: host networking keeps the advertised localhost RPC endpoints
# reachable by the host JVM without a separate ZooKeeper, RegionServer or HDFS.
docker run -d --name "$HBASE_CONTAINER" --label "$HBASE_LABEL" \
    --network host --memory 2g --env HBASE_HEAPSIZE=1024 \
    --mount "type=bind,source=${TRAVIS_DIR}/hbase-ci-site.xml,target=/usr/local/hbase/conf/hbase-site.xml,readonly" \
    --entrypoint /usr/local/hbase/bin/hbase "$HBASE_IMAGE" master start

# Real HBase API read/write readiness, rather than accepting an open TCP port.
# Each attempt is bounded; the outer timeout also includes JVM startup and pull.
while true; do
    [[ "$(docker inspect -f '{{.State.Running}}' "$HBASE_CONTAINER")" == true ]] || exit 1
    if timeout --signal=TERM --kill-after=2s 30s docker exec -i "$HBASE_CONTAINER" \
            /usr/local/hbase/bin/hbase shell -n <<'RUBY'
java_import org.apache.hadoop.hbase.HBaseConfiguration
java_import org.apache.hadoop.hbase.TableName
java_import org.apache.hadoop.hbase.client.ConnectionFactory
java_import org.apache.hadoop.hbase.client.TableDescriptorBuilder
java_import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder
java_import org.apache.hadoop.hbase.client.Put
java_import org.apache.hadoop.hbase.client.Get
java_import org.apache.hadoop.hbase.util.Bytes
configuration = HBaseConfiguration.create
configuration.setInt('hbase.client.operation.timeout', 10000)
configuration.setInt('hbase.rpc.timeout', 5000)
configuration.setInt('hbase.client.retries.number', 2)
connection = ConnectionFactory.createConnection(configuration)
admin = connection.getAdmin
name = TableName.valueOf('hugegraph_ci_probe')
family = Bytes.toBytes('f')
unless admin.tableExists(name)
  descriptor = TableDescriptorBuilder.newBuilder(name)
    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family)).build
  admin.createTable(descriptor)
end
table = connection.getTable(name)
row = Bytes.toBytes('row')
column = Bytes.toBytes('value')
put = Put.new(row)
put.addColumn(family, column, Bytes.toBytes('ready'))
table.put(put)
value = table.get(Get.new(row)).getValue(family, column)
raise 'HBase read/write probe failed' unless Bytes.toString(value) == 'ready'
table.close
admin.disableTable(name)
admin.deleteTable(name)
admin.close
connection.close
exit
RUBY
    then
        break
    fi
    sleep 2
done
ready=true
setup_seconds=$((SECONDS - started))
echo "HBase cold pull: ${pull_seconds}s; complete preparation: ${setup_seconds}s"
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    printf '### HBase environment\n\n- Pinned HBase 2.6.5 image\n- Image pull: %ss\n- Pull, startup and read/write probe: %ss (300s budget)\n' \
        "$pull_seconds" "$setup_seconds" >> "$GITHUB_STEP_SUMMARY"
fi
