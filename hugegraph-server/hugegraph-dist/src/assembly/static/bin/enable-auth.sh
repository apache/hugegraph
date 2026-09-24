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
# it is present in the tarball and in the image.  PROPS_AWK lets a caller point
# at a different copy; the entrypoint reads that same variable for itself.
for candidate in "${PROPS_AWK:-}" "${BIN}/props.awk" "${TOP}/props.awk"; do
    if [[ -n "${candidate}" && -f "${candidate}" ]]; then
        PROPS_AWK="${candidate}"
        break
    fi
done
[[ -n "${PROPS_AWK:-}" ]] || fail "props.awk not found beside this script"

# The Gremlin half of the decision below has to be made by the same reader the
# entrypoint uses.  yamlscan.awk is not in bin/: the Dockerfile places it in the
# install home, one above this script, which is where the image layout is
# mirrored in the test tree; PROPS_AWK and YAMLSCAN_AWK cover a caller that
# keeps it elsewhere.  The release tarball carries no copy at all, so the
# fallback below has to answer on its own.
YAMLSCAN=""
for candidate in "${YAMLSCAN_AWK:-}" "${TOP}/yamlscan.awk" "${BIN}/yamlscan.awk"; do
    if [[ -n "${candidate}" && -f "${candidate}" ]]; then
        YAMLSCAN="${candidate}"
        break
    fi
done

# props_get is the only reader used here, and it treats any nonzero status from
# props.awk as an error: 2 means the file could not be read at all, which must
# not be mistaken for "the key is not there" and answered with a write.
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

# Give `$3` its default `$2` for key `$1`, in place, unless it already has a
# value.  Guarding with props_has and appending was not the same question:
# `auth.authenticator=` and a bare `auth.authenticator` line both parse to the
# empty string (measured against java.util.Properties, which also strips the
# trailing blanks of `auth.authenticator=   `), so props_has reported them as
# answered and the append was skipped -- while the entrypoint's
# check_auth_sides, which asks for the value rather than the key, counted the
# same file as unconfigured.  `loadAuthenticator("")` returns null, so REST then
# served without authentication next to a Gremlin that required it.
#
# props_set covers both shapes the guard had to split: with a definition
# present it replaces the first one where it stands (no duplicate for
# first-definition-wins to bury), with none present it appends.
ensure_rest_prop() {
    [[ -n "$(props_get "$1" "$3")" ]] && return 0
    props_set "$1" "$2" "$3"
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

# Both writes below skip a side that already carries a real value, so they are
# no-ops on a mounted config or a re-run.  Appending unconditionally used to
# create duplicate definitions that the properties parser (first definition
# wins) and the yaml parser (last wins) resolved in opposite directions, leaving
# Gremlin and REST on different authenticators.  That is why the REST side goes
# through ensure_rest_prop rather than a presence guard plus an append: a
# presence guard also lets a defined-but-empty key count as answered, and the
# appended default would then be the definition the server never reads.
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

# Does the Gremlin config already carry a top-level `authentication` mapping?
# This is the same question check_auth_sides answers, so it has to go to the same
# reader: a mapping is the server's only at column 0, comment text is not
# content, and the key may be quoted.  grep asks it differently -- it sees only
# the bare spelling, so an operator's `"authentication":` block read as absent
# and a second default block was appended beside it, after which the two servers
# can resolve the key in opposite directions while REST keeps its existing
# authenticator.  Anything other than `none` means a mapping is there and the
# append is not this script's to make.
gremlin_has_auth_block() {
    local file="$1" state
    [[ -f "${file}" ]] || return 1
    if [[ -n "${YAMLSCAN}" ]]; then
        state=$(awk -f "${YAMLSCAN}" "${file}") || fail "cannot read ${file}"
        [[ "${state}" != "none" ]]
        return
    fi
    # No parser in this layout (the plain release tarball).  Match what grep can
    # honestly answer here: a column-0 key in either quote style or none.  The
    # nested-mapping and comment cases are the ones that need the real reader,
    # and the image, where the entrypoint runs this script, always has it.
    grep -Eq "^[\"']?authentication[\"']?[[:blank:]]*:" "${file}"
}

# Only a column-0 `authentication` mapping is the Gremlin server's, which is the
# rule yamlscan.awk applies to decide the same thing for check_auth_sides.  With
# a guard that disagreed on nesting, the entrypoint read the file as `none`, so
# parity held and it called this script, but the guard saw the nested key and
# skipped the append, writing the REST side only -- StandardAuthenticator on
# REST, TinkerPop's AllowAllAuthenticator on Gremlin.
if ! gremlin_has_auth_block "${CONF}/${GREMLIN_SERVER_CONF}"; then
    append_lines "${CONF}/${GREMLIN_SERVER_CONF}" \
        'authentication: {' \
        "  authenticator: ${AUTHENTICATOR_CLASS}," \
        '  authenticationHandler: org.apache.hugegraph.auth.WsAndHttpBasicAuthHandler,' \
        '  config: {tokens: conf/rest-server.properties}' \
        '}'
fi

ensure_rest_prop "auth.authenticator" "${AUTHENTICATOR_CLASS}" "${CONF}/${REST_SERVER_CONF}"
ensure_rest_prop "auth.graph_store" "hugegraph" "${CONF}/${REST_SERVER_CONF}"

# Wrap the graph factory only when it really is the plain HugeFactory, which is
# a question about the decoded value, so it goes through the same reader.
#
# The trailing blanks come off before the comparison because the server reads
# the trimmed line: commons-configuration right-trims a property line before it
# resolves the class, so a mounted `gremlin.graph=org.apache.hugegraph.HugeFactory  `
# opens the graph through the plain factory exactly as if it carried no blanks.
# java.util.Properties by itself keeps them (measured against JDK 17), which is
# why the reader hands the value back verbatim.  Comparing the untrimmed bytes
# left such a config unwrapped: authentication on both servers, and no
# HugeFactoryAuthProxy in front of the graph, which GraphManager only warns
# about.  A factory that is not HugeFactory stays untouched either way.
GRAPH_FACTORY=$(props_get "gremlin.graph" "${CONF}/graphs/${GRAPH_CONF}")
while [[ "${GRAPH_FACTORY}" =~ [[:space:]]$ ]]; do
    GRAPH_FACTORY="${GRAPH_FACTORY%?}"
done
if [[ "${GRAPH_FACTORY}" == "org.apache.hugegraph.HugeFactory" ]]; then
    props_set "gremlin.graph" "org.apache.hugegraph.auth.HugeFactoryAuthProxy" \
        "${CONF}/graphs/${GRAPH_CONF}"
fi
