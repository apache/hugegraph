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

BASE_PATH=$(cd "$(dirname $0)" || exit; pwd)
DEP_PATH=$BASE_PATH/all_dependencies
FILE_NAME=${1:-known-dependencies.txt}

if [[ -d $DEP_PATH ]];then
  echo "rm -r -f DEP_PATH"
  rm -r -f $DEP_PATH
fi

cd "$BASE_PATH"/../../../ || exit

mvn dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=$DEP_PATH

ls $DEP_PATH | egrep -v "^hg|hugegraph|hubble" | sort -n > $BASE_PATH/$FILE_NAME
rm -r -f $DEP_PATH
