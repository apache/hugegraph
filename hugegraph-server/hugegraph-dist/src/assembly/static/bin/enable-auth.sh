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

function abs_path() {
    SOURCE="${BASH_SOURCE[0]}"
    while [[ -h "$SOURCE" ]]; do
        DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    cd -P "$(dirname "$SOURCE")" && pwd
}

BIN=$(abs_path)
TOP="$(cd "${BIN}"/../ && pwd)"
CONF="$TOP/conf"

GREMLIN_SERVER_CONF="gremlin-server.yaml"
REST_SERVER_CONF="rest-server.properties"
GRAPH_CONF="hugegraph.properties"

# make a backup
BAK_CONF="$TOP/conf-bak"
if [ ! -d "$BAK_CONF" ]; then
    mkdir -p "$BAK_CONF"
    cp "${CONF}/${GREMLIN_SERVER_CONF}" "${BAK_CONF}/${GREMLIN_SERVER_CONF}.bak"
    cp "${CONF}/${REST_SERVER_CONF}" "${BAK_CONF}/${REST_SERVER_CONF}.bak"
    cp "${CONF}/graphs/${GRAPH_CONF}" "${BAK_CONF}/${GRAPH_CONF}.bak"
fi

# The appends below are guarded per file and match only an absent or still
# commented-out definition, so they are no-ops on any config that already
# carries authentication (e.g. a mounted one, or a re-run of this script).
# Appending unconditionally used to create duplicate definitions that the
# properties parser (first definition wins) and the yaml parser (last wins)
# resolved in opposite directions, leaving Gremlin and REST on different
# authenticators.

AUTHENTICATOR_CLASS="${AUTHENTICATOR_CLASS:-org.apache.hugegraph.auth.StandardAuthenticator}"

if ! grep -Eq '^[ \t]*authentication[ \t]*:' "${CONF}/${GREMLIN_SERVER_CONF}"; then
    sed -i -e '$a\authentication: {' \
        -e "\$a\\  authenticator: ${AUTHENTICATOR_CLASS}," \
        -e '$a\  authenticationHandler: org.apache.hugegraph.auth.WsAndHttpBasicAuthHandler,' \
        -e '$a\  config: {tokens: conf/rest-server.properties}' \
        -e '$a\}' ${CONF}/${GREMLIN_SERVER_CONF}
fi

if ! grep -Eq '^[ \t]*auth\.authenticator[ \t]*=' "${CONF}/${REST_SERVER_CONF}"; then
    sed -i -e "\$a\\auth.authenticator=${AUTHENTICATOR_CLASS}" ${CONF}/${REST_SERVER_CONF}
fi

if ! grep -Eq '^[ \t]*auth\.graph_store[ \t]*=' "${CONF}/${REST_SERVER_CONF}"; then
    sed -i -e '$a\auth.graph_store=hugegraph' ${CONF}/${REST_SERVER_CONF}
fi

if grep -Eq '^gremlin\.graph[ \t]*=org\.apache\.hugegraph\.HugeFactory[ \t]*$' "${CONF}/graphs/${GRAPH_CONF}"; then
    sed -i 's/^gremlin\.graph[ \t]*=org\.apache\.hugegraph\.HugeFactory[ \t]*$/gremlin.graph=org.apache.hugegraph.auth.HugeFactoryAuthProxy/' ${CONF}/graphs/${GRAPH_CONF}
fi
