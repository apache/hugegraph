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

if [[ $# -ne 1 ]]; then
    echo "Must pass BACKEND type of hugegraph"
    exit 1
fi

BACKEND=$1
TRAVIS_DIR=$(dirname "$0")

if [ ! -d "$HOME"/downloads ]; then
    mkdir "$HOME"/downloads
fi

case $BACKEND in
    hbase)
        # TODO: replace it with hbase2.3+ to avoid java8 env
        "$TRAVIS_DIR"/install-hbase.sh
        ;;
    hstore)
        "$TRAVIS_DIR"/install-hstore.sh
        ;;
    *)
        # don't need to install for other backends
        ;;
esac
