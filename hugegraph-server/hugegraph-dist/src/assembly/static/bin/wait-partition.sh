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

: "${STORE_REST:?STORE_REST not set}"

timeout "${WAIT_PARTITION_TIMEOUT_S:-120}s" bash -c '
until curl -fsS "http://${STORE_REST}" 2>/dev/null | \
      grep -q "\"partitionCount\":[1-9]"
do
    echo "Waiting for partition assignment..."
    sleep 5
done
'

echo "Partitions detected:"
URL="http://${STORE_REST}/v1/partitions"
echo "$URL"
curl -v "$URL"
