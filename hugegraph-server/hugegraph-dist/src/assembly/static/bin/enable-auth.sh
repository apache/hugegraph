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

fail() {
    echo "enable-auth.sh: $*" >&2
    exit 1
}

# Reading and writing .properties files goes through props.awk, the same helper
# the docker entrypoint uses, because the keys below can be spelled in every way
# java.util.Properties accepts: `=`/`:`/bare-whitespace separators, a form feed
# as whitespace, `\.` or `\u002e` for the dots, and LF, CRLF or CR line
# terminators.  grep and sed see a different file.  A legal
# `gremlin\u002egraph=org.apache.hugegraph.HugeFactory` matched no pattern at
# all, so the factory was never wrapped for auth even though both servers were
# told authentication was on -- and the CR byte that the previous pattern had
# to be handed a carriage return for is now handled by the reader itself.
#
# props.awk is packaged in this same bin/ directory by the release assembly, so
# it is present in the tarball and in the image; the entrypoint also exports
# PROPS_AWK when it calls this script.
for candidate in "${PROPS_AWK:-}" "${BIN}/props.awk" "${TOP}/props.awk"; do
    if [[ -n "${candidate}" && -f "${candidate}" ]]; then
        PROPS_AWK="${candidate}"
        break
    fi
done
[[ -n "${PROPS_AWK:-}" ]] || fail "props.awk not found beside this script"

# Exit status of the reader is meaningful: 1 means the key has no definition,
# 2 means props.awk could not do its job.  Only 1 is an acceptable answer here.
props_has() {
    local status=0
    PROPS_MODE=has PROPS_KEY="$1" PROPS_FILE="$2" awk -f "${PROPS_AWK}" /dev/null || status=$?
    if (( status > 1 )); then
        fail "cannot read $2"
    fi
    return "${status}"
}

props_get() {
    local status=0 value
    value=$(PROPS_MODE=get PROPS_DECODED=1 PROPS_KEY="$1" PROPS_FILE="$2" \
        awk -f "${PROPS_AWK}" /dev/null) || status=$?
    if (( status > 0 )); then
        fail "cannot read $2"
    fi
    printf '%s' "${value}"
}

props_set() {
    # The only values written here are Java class names, whose characters need
    # no properties escaping; anything else would have to go through the
    # entrypoint's encoder first.
    case "$2" in
        *[!A-Za-z0-9_\.\$]*) fail "refusing to write an unescaped value: $2" ;;
    esac
    PROPS_MODE=set PROPS_KEY="$1" PROPS_VALUE_ENCODED="$2" PROPS_FILE="$3" \
        awk -f "${PROPS_AWK}" /dev/null || fail "cannot update $3"
}

# make a backup
BAK_CONF="$TOP/conf-bak"
if [ ! -d "$BAK_CONF" ]; then
    mkdir -p "$BAK_CONF" || fail "cannot create ${BAK_CONF}"
    cp "${CONF}/${GREMLIN_SERVER_CONF}" "${BAK_CONF}/${GREMLIN_SERVER_CONF}.bak" ||
        fail "cannot back up ${GREMLIN_SERVER_CONF}"
    cp "${CONF}/${REST_SERVER_CONF}" "${BAK_CONF}/${REST_SERVER_CONF}.bak" ||
        fail "cannot back up ${REST_SERVER_CONF}"
    cp "${CONF}/graphs/${GRAPH_CONF}" "${BAK_CONF}/${GRAPH_CONF}.bak" ||
        fail "cannot back up ${GRAPH_CONF}"
fi

# The appends below are guarded per file and skip any file that already carries
# the property, so they are no-ops on a mounted config or a re-run.  Appending
# unconditionally used to create duplicate definitions that the properties
# parser (first definition wins) and the yaml parser (last wins) resolved in
# opposite directions, leaving Gremlin and REST on different authenticators.
#
# Appended with `>>` rather than `sed -i '$a\...'`: GNU sed's `$` address never
# matches when the file has no lines, so on an empty mounted config every append
# silently did nothing.  `sed -i '$a'` also closed the previous last line for us,
# which `>>` does not, so a file without a trailing newline gets one first.
#
# Every write here has to be seen to succeed.  The docker entrypoint runs this
# script and trusts its exit status, and a partially updated tree -- REST
# configured, yaml append refused by a read-only mounted file -- is exactly the
# one-sided state the entrypoint refuses to start with.  Without errexit and
# these checks the script exited 0 on that half-done job.
append_lines() {
    local file="$1"
    shift
    if [[ ! -w "${file}" ]]; then
        fail "cannot append to ${file}: not writable"
    fi
    if [[ -s "${file}" && -n "$(tail -c 1 "${file}")" ]]; then
        printf '\n' >> "${file}" || fail "cannot append to ${file}"
    fi
    printf '%s\n' "$@" >> "${file}" || fail "cannot append to ${file}"
}

AUTHENTICATOR_CLASS="${AUTHENTICATOR_CLASS:-org.apache.hugegraph.auth.StandardAuthenticator}"

if ! grep -Eq '^[[:blank:]]*authentication[[:blank:]]*:' "${CONF}/${GREMLIN_SERVER_CONF}"; then
    append_lines "${CONF}/${GREMLIN_SERVER_CONF}" \
        'authentication: {' \
        "  authenticator: ${AUTHENTICATOR_CLASS}," \
        '  authenticationHandler: org.apache.hugegraph.auth.WsAndHttpBasicAuthHandler,' \
        '  config: {tokens: conf/rest-server.properties}' \
        '}'
fi

if ! props_has "auth.authenticator" "${CONF}/${REST_SERVER_CONF}"; then
    append_lines "${CONF}/${REST_SERVER_CONF}" "auth.authenticator=${AUTHENTICATOR_CLASS}"
fi

if ! props_has "auth.graph_store" "${CONF}/${REST_SERVER_CONF}"; then
    append_lines "${CONF}/${REST_SERVER_CONF}" 'auth.graph_store=hugegraph'
fi

# Wrap the graph factory only when it really is the plain HugeFactory, which is
# a question about the decoded value, so it goes through the same reader.
GRAPH_FACTORY=$(props_get "gremlin.graph" "${CONF}/graphs/${GRAPH_CONF}")
if [[ "${GRAPH_FACTORY}" == "org.apache.hugegraph.HugeFactory" ]]; then
    props_set "gremlin.graph" "org.apache.hugegraph.auth.HugeFactoryAuthProxy" \
        "${CONF}/graphs/${GRAPH_CONF}"
fi
