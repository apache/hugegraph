#!/usr/bin/env bash
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

BASE_PATH=$(cd "$(dirname "$0")" && pwd)
KNOWN_FILE=${1:-$BASE_PATH/known-dependencies.txt}
CURRENT_FILE=${2:-$BASE_PATH/current-dependencies.txt}

# Missing/malformed inputs and scan failures must not produce a green diff.
python3 "$BASE_PATH/dependency_inventory.py" check \
    --known "$KNOWN_FILE" --current "$CURRENT_FILE"
