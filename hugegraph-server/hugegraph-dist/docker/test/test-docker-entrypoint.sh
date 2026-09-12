#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

entrypoint="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/docker-entrypoint.sh"
test_dir="$(mktemp -d)"
trap 'rm -rf "${test_dir}"' EXIT

# Eval the property and yaml helpers one by one.  The entrypoint's
# top-level code hard-exits when props.awk is missing, so it cannot be
# sourced directly; extracting by function name keeps this independent of
# helper order.  PROPS_AWK is recomputed below.
for fn in encode_prop_value set_prop_encoded set_prop get_prop_encoded get_prop \
          get_yaml_authenticator has_yaml_authentication_block align_auth_config; do
    eval "$(awk -v fn="${fn}" '
        index($0, fn "() {") == 1 { capture = 1 }
        capture { print }
        capture && /^}$/ { exit }
    ' "${entrypoint}")"
done
log() { echo "[hugegraph-server-entrypoint] $*"; }
PROPS_AWK="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/props.awk"
export PROPS_AWK

assert_replaced() {
    local separator="$1"
    local file="${test_dir}/config-${separator// /space}"

    printf 'init_store.enabled%sfalse\n' "${separator}" > "${file}"
    set_prop "init_store.enabled" "true" "${file}"
    [[ "$(grep -Ec '^init_store\.enabled=true$' "${file}")" -eq 1 ]]
}

assert_line_count() {
    local expected="$1" pattern="$2" file="$3"
    local actual

    actual=$(grep -Ec "${pattern}" "${file}")
    if [[ "${actual}" -ne "${expected}" ]]; then
        echo "expected ${expected} matching lines, got ${actual}" >&2
        return 1
    fi
}

assert_replaced "="
assert_replaced ": "
assert_replaced " "

duplicate_file="${test_dir}/config-duplicates"
printf '%s\n' \
    'init_store.enabled=false' \
    'init_store.enabled: false' \
    'init_store.enabled false' \
    'init_store.enabled' \
    'unrelated=true' > "${duplicate_file}"
set_prop "init_store.enabled" "true" "${duplicate_file}"
assert_line_count 1 \
    '^[[:space:]]*init_store\.enabled([[:space:]]*[:=]|[[:space:]]+|[[:space:]]*$)' \
    "${duplicate_file}"
assert_line_count 1 '^init_store\.enabled=true$' "${duplicate_file}"
grep -q '^unrelated=true$' "${duplicate_file}"

# An escaped key is one logical definition of that key, not a key with
# backslashes in its name: setting the plain key must rewrite it in place
# rather than appending a second definition whose only resolution is
# parser-dependent (and which HugeConfig then reports as a list).
escaped_file="${test_dir}/config-escaped-key"
printf '%s\n' \
    'auth\.admin_pa=old' \
    'unrelated=true' > "${escaped_file}"
set_prop "auth.admin_pa" "new" "${escaped_file}"
assert_line_count 1 '^auth\.admin_pa=new$' "${escaped_file}"
assert_line_count 1 '^unrelated=true$' "${escaped_file}"

# A value continued onto the next line is part of the same definition:
# setting the key must remove the continuation, not leave it behind as a
# stray property of its own.
continued_file="${test_dir}/config-continuation"
printf '%s\n' \
    'pd.peers 127.0.0.1:8686,\' \
    '  127.0.0.2:8686' \
    'unrelated=true' > "${continued_file}"
set_prop "pd.peers" "10.0.0.1:8686" "${continued_file}"
assert_line_count 1 '^pd\.peers=10\.0\.0\.1:8686$' "${continued_file}"
assert_line_count 1 '^unrelated=true$' "${continued_file}"
[[ "$(grep -c '127\.0\.0\.2' "${continued_file}")" -eq 0 ]]

# get_prop_encoded reads through the same grammar: separators, escapes,
# continuations, and first-definition-wins duplicates.
get_file="${test_dir}/config-get"
printf '%s\n' \
    '#comment' \
    'a\=b : colon value' \
    'multiline first \' \
    '    second' \
    'dup : one' \
    'dup=two' > "${get_file}"
[[ "$(get_prop_encoded 'a=b' "${get_file}")" == "colon value" ]]
[[ "$(get_prop_encoded 'multiline' "${get_file}")" == "first second" ]]
[[ "$(get_prop_encoded 'dup' "${get_file}")" == "one" ]]

# Appends must still happen when the file has no definition of the key,
# including when the only occurrences are inside comments.
append_file="${test_dir}/config-append"
printf '%s\n' \
    '#init_store.enabled=false' \
    'unrelated=true' > "${append_file}"
set_prop "init_store.enabled" "true" "${append_file}"
assert_line_count 1 '^init_store\.enabled=true$' "${append_file}"
assert_line_count 1 '^#init_store\.enabled=false$' "${append_file}"

# A key indented with leading whitespace is still one definition of the
# key: java.util.Properties ignores whitespace before a key, so an
# indented key must be read and rewritten in place rather than duplicated.
indented_file="${test_dir}/config-indented-key"
printf '%s\n' \
    '  auth.token_secret: old-secret' \
    'unrelated=true' > "${indented_file}"
[[ "$(get_prop_encoded 'auth.token_secret' "${indented_file}")" == "old-secret" ]]
set_prop_encoded 'auth.token_secret' 'new-secret' "${indented_file}"
assert_line_count 1 'auth\.token_secret' "${indented_file}"
assert_line_count 1 '^unrelated=true$' "${indented_file}"

# get_yaml_authenticator must agree with snakeyaml on what a mounted
# gremlin-server.yaml says: the authenticator inside the authentication
# block — quoted scalars and inline comments cleaned the way snakeyaml
# strips them — and a flow mapping on the authentication line itself.
# align_auth_config must not read an authentication block without a
# readable authenticator as "no yaml side": exporting the default there
# would override an explicit choice, so both sides stay untouched.
yaml_dir="${test_dir}/yaml"
mkdir -p "${yaml_dir}/conf"
(
    cd "${yaml_dir}" || exit 1
    REST_SERVER_CONF="./conf/rest-server.properties"
    : > "${REST_SERVER_CONF}"

    printf '%s\n' \
        'authentication:' \
        '  authenticator: "com.example.MyAuth"  # custom' \
        '  authenticationHandler: org.apache.hugegraph.auth.WsAndHttpBasicAuthHandler' \
        > conf/gremlin-server.yaml
    [[ "$(get_yaml_authenticator)" == "com.example.MyAuth" ]]

    printf '%s\n' \
        'authentication: {authenticator: com.example.FlowAuth, authenticationHandler: org.apache.hugegraph.auth.WsAndHttpBasicAuthHandler, config: {tokens: conf/rest-server.properties}}' \
        > conf/gremlin-server.yaml
    [[ "$(get_yaml_authenticator)" == "com.example.FlowAuth" ]]

    printf '%s\n' \
        'authentication:' \
        '  authenticationHandler: org.apache.hugegraph.auth.WsAndHttpBasicAuthHandler' \
        > conf/gremlin-server.yaml
    unset AUTHENTICATOR_CLASS
    align_auth_config
    [[ -z "${AUTHENTICATOR_CLASS:-}" ]]
    [[ ! -s "${REST_SERVER_CONF}" ]]

    printf '%s\n' \
        'authentication:' \
        '  authenticator: com.example.YamlAuth' \
        > conf/gremlin-server.yaml
    align_auth_config
    grep -q '^auth\.authenticator=com\.example\.YamlAuth$' "${REST_SERVER_CONF}"
)

# CRLF (Windows-saved) configs parse the way java.util.Properties reads
# them: one trailing CR is a line terminator, not part of the value, and
# a backslash before CRLF still continues the value onto the next line.
# Untouched lines keep their CR bytes on rewrite.
crlf_file="${test_dir}/config-crlf"
printf 'auth.authenticator=org.apache.hugegraph.auth.StandardAuthenticator\r\n' > "${crlf_file}"
printf 'pd.peers=a,\\\r\n  b\r\n' >> "${crlf_file}"
printf 'unrelated=true\r\n' >> "${crlf_file}"
[[ "$(get_prop_encoded 'auth.authenticator' "${crlf_file}")" == \
    "org.apache.hugegraph.auth.StandardAuthenticator" ]]
[[ "$(get_prop_encoded 'pd.peers' "${crlf_file}")" == "a,b" ]]
[[ "$(get_prop 'auth.authenticator' "${crlf_file}")" == \
    "org.apache.hugegraph.auth.StandardAuthenticator" ]]
set_prop 'auth.authenticator' 'com.example.NewAuth' "${crlf_file}"
grep -q '^auth\.authenticator=com\.example\.NewAuth$' "${crlf_file}"
[[ "$(get_prop_encoded 'pd.peers' "${crlf_file}")" == "a,b" ]]
if ! grep -q $'^unrelated=true\r$' "${crlf_file}"; then
    echo "CRLF bytes of untouched lines must be preserved" >&2
    exit 1
fi

# An escaped authenticator and a plain yaml scalar name the same class:
# the comparison unescapes first, so no spurious WARN and no skipped
# alignment.
escaped_auth_dir="${test_dir}/yaml-escaped-auth"
mkdir -p "${escaped_auth_dir}/conf"
(
    cd "${escaped_auth_dir}" || exit 1
    REST_SERVER_CONF="./conf/rest-server.properties"
    printf '%s\n' \
        'auth.authenticator=org.apache.hugegraph.auth\.StandardAuthenticator' \
        > "${REST_SERVER_CONF}"
    printf '%s\n' \
        'authentication:' \
        '  authenticator: org.apache.hugegraph.auth.StandardAuthenticator' \
        > conf/gremlin-server.yaml
    unset AUTHENTICATOR_CLASS
    align_out=$(align_auth_config 2>&1)
    [[ -z "${AUTHENTICATOR_CLASS:-}" ]]
    [[ "${align_out}" != *"different authenticators"* ]]
    grep -q '^auth\.authenticator=org\.apache\.hugegraph\.auth\.StandardAuthenticator$' \
        "${REST_SERVER_CONF}"
)
