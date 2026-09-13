#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
set -uo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}/..")" && pwd)"
cd "$root"
echo '=== Java/Maven ==='
java -version 2>&1 || true
mvn -version || true
echo '=== Java 17 candidates ==='
find /usr/lib/jvm "$HOME/.sdkman/candidates/java" -maxdepth 1 -type d -iname '*17*' 2>/dev/null || true
echo '=== Groovy/ASM dependencies ==='
mvn dependency:tree -Dincludes=org.codehaus.groovy,org.ow2.asm -DskipTests || true
echo '=== Gremlin E2E ==='
mvn test -pl hugegraph-server/hugegraph-test -am -Pcore-test,memory \
  -Drat.skip=true -DskipITs -Dtest='*Gremlin*Test'
rc=$?
echo "RESULT_EXIT_CODE=$rc"
exit "$rc"
