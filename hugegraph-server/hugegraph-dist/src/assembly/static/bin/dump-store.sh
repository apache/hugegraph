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
abs_path() {
    SOURCE="${BASH_SOURCE[0]}"
    while [ -h "$SOURCE" ]; do
        DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    echo "$( cd -P "$( dirname "$SOURCE" )" && pwd )"
}

BIN=`abs_path`
TOP="$(cd $BIN/../ && pwd)"
CONF=$TOP/conf
LIB=$TOP/lib

. "${BIN}"/util.sh

configure_riscv64_libatomic || exit 1

if [ -n "$JAVA_HOME" ]; then
    JAVA="$JAVA_HOME"/bin/java
else
    JAVA=java
fi

if [ $# -eq 0 ]; then
    set -- "$CONF/graphs/hugegraph.properties"
fi
conf=$1

cd $TOP

echo "Dumping HugeGraph Store($conf)..."

dump_store_ext_jar_path=$LIB/hugegraph-dist-*.jar
for i in $LIB/*.jar; do dump_store_ext_jar_path=$dump_store_ext_jar_path:$i;  export dump_store_ext_jar_path; done
exec "$JAVA" -cp "$dump_store_ext_jar_path" \
org.apache.hugegraph.cmd.StoreDumper "$@"
