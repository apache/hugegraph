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
# align_auth_config refuses an authentication block without a readable
# authenticator instead of treating it as "no yaml side": exporting the
# default there would override an explicit choice, and continuing would let
# enable-auth.sh write the REST side alone.
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

# align_auth_config must refuse an authentication block without a readable
# authenticator: continuing would let enable-auth.sh write the REST side
# alone (REST on StandardAuthenticator, Gremlin on TinkerPop's
# AllowAllAuthenticator default), so the entrypoint stops here instead.
    printf '%s\n' \
        'authentication:' \
        '  authenticationHandler: org.apache.hugegraph.auth.WsAndHttpBasicAuthHandler' \
        > conf/gremlin-server.yaml
    unset AUTHENTICATOR_CLASS
    if align_auth_config; then
        echo "align_auth_config must refuse an authentication block" \
            "without a readable authenticator" >&2
        exit 1
    fi
    [[ -z "${AUTHENTICATOR_CLASS:-}" ]]
    [[ ! -s "${REST_SERVER_CONF}" ]]

    printf '%s\n' \
        'authentication:' \
        '  authenticator: com.example.YamlAuth' \
        > conf/gremlin-server.yaml
    align_auth_config
    grep -q '^auth\.authenticator=com\.example\.YamlAuth$' "${REST_SERVER_CONF}"
)

# The refusal above is what keeps enable-auth.sh from writing one side:
# against the same ambiguous layout, enable-auth.sh on its own writes only
# the REST file (its yaml guard already sees an `authentication:` line),
# leaving REST on StandardAuthenticator and Gremlin on TinkerPop's
# AllowAllAuthenticator default.  The entrypoint never lets it run there
# because align_auth_config fails first under set -e.
onesided_dir="${test_dir}/yaml-onesided"
mkdir -p "${onesided_dir}/bin" "${onesided_dir}/conf/graphs"
cp "$(cd "$(dirname "${BASH_SOURCE[0]}")/../../src/assembly/static/bin" && pwd)/enable-auth.sh" \
    "${onesided_dir}/bin/enable-auth.sh"
chmod +x "${onesided_dir}/bin/enable-auth.sh"
(
    cd "${onesided_dir}" || exit 1
    REST_SERVER_CONF="./conf/rest-server.properties"
    : > conf/rest-server.properties
    printf '%s\n' \
        'gremlin.graph=org.apache.hugegraph.HugeFactory' \
        > conf/graphs/hugegraph.properties
    printf '%s\n' \
        'authentication:' \
        '  authenticationHandler: org.apache.hugegraph.auth.WsAndHttpBasicAuthHandler' \
        > conf/gremlin-server.yaml
    unset AUTHENTICATOR_CLASS
    if align_auth_config; then
        echo "align_auth_config must refuse an authentication block without a readable authenticator" >&2
        exit 1
    fi
    ./bin/enable-auth.sh
    grep -q '^auth\.authenticator=org\.apache\.hugegraph\.auth\.StandardAuthenticator$' \
        conf/rest-server.properties
    grep -q 'HugeFactoryAuthProxy' conf/graphs/hugegraph.properties
    if grep -Eq '^[[:blank:]]*authenticator[[:blank:]]*:' conf/gremlin-server.yaml; then
        echo "enable-auth.sh must not add an authenticator to the yaml block" >&2
        exit 1
    fi
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

# A set must keep the config's inode: a copy-back preserves the file's
# permissions (a 0600 config holding secrets must not come back
# umask-readable) and leaves a symlinked config pointing at its target
# instead of replacing it with a regular file.
mode_file="${test_dir}/config-mode"
printf '%s\n' 'unrelated=true' > "${mode_file}"
chmod 600 "${mode_file}"
set_prop "init_store.enabled" "true" "${mode_file}"
[[ "$(stat -c '%a' "${mode_file}")" == "600" ]]
grep -q '^init_store\.enabled=true$' "${mode_file}"
grep -q '^unrelated=true$' "${mode_file}"
[[ ! -e "${mode_file}.tmp" ]]
[[ ! -e "${mode_file}.bak" ]]

target_file="${test_dir}/config-target"
link_file="${test_dir}/config-link"
printf '%s\n' 'unrelated=true' > "${target_file}"
ln -s "${target_file}" "${link_file}"
set_prop "init_store.enabled" "true" "${link_file}"
[[ -L "${link_file}" ]]
grep -q '^init_store\.enabled=true$' "${target_file}"

# An `authenticator:` below a *sibling* mapping is not the Gremlin one.
# `get_yaml_authenticator` opens its block on `authentication:` and has to
# close it again on the next key at the same indentation, or the yaml below
# reports com.example.TlsOnly — and align_auth_config then writes that
# class into rest-server.properties, so REST authenticates with a class the
# operator only ever mentioned to an unrelated mapping.
scope_dir="${test_dir}/yaml-scope"
mkdir -p "${scope_dir}/conf"
(
    cd "${scope_dir}" || exit 1

    printf '%s\n' \
        'authentication:' \
        '  config: {tokens: conf/rest-server.properties}' \
        'ssl:' \
        '  authenticator: com.example.TlsOnly' \
        > conf/gremlin-server.yaml
    [[ -z "$(get_yaml_authenticator)" ]]

    # The block's own authenticator is still found when a sibling follows
    # it, and one deeper than the key is still inside it.
    printf '%s\n' \
        'authentication:' \
        '  authenticator: com.example.GremlinAuth' \
        '  authenticationHandler: org.apache.hugegraph.auth.WsAndHttpBasicAuthHandler' \
        'ssl:' \
        '  authenticator: com.example.TlsOnly' \
        > conf/gremlin-server.yaml
    [[ "$(get_yaml_authenticator)" == "com.example.GremlinAuth" ]]

    # A blank line does not close a YAML mapping, and neither does a
    # comment — including one that names an authenticator.
    printf '%s\n' \
        'authentication:' \
        '' \
        '#  authenticator: com.example.CommentedAuth' \
        '  authenticator: com.example.BlankLineAuth' \
        > conf/gremlin-server.yaml
    [[ "$(get_yaml_authenticator)" == "com.example.BlankLineAuth" ]]

    # Same indentation as the key means a sibling, not a member: the last
    # case a mounted file is likely to get wrong, because a two-space
    # `authentication:` under a top-level key is how some deployments
    # indent the whole block.
    printf '%s\n' \
        '  authentication:' \
        '    authenticator: com.example.IndentedAuth' \
        '  ssl:' \
        '    authenticator: com.example.TlsOnly' \
        > conf/gremlin-server.yaml
    [[ "$(get_yaml_authenticator)" == "com.example.IndentedAuth" ]]
)

# Both sides silent means "bootstrap authentication", but an operator who
# passed AUTHENTICATOR_CLASS named the class they want.  The default may
# fill that in, it may not overwrite it: enable-auth.sh appends the value
# it is given, so overwriting here put StandardAuthenticator into a
# deployment that asked for something else.
class_dir="${test_dir}/authenticator-class"
mkdir -p "${class_dir}/conf"
(
    cd "${class_dir}" || exit 1
    REST_SERVER_CONF="./conf/rest-server.properties"
    : > "${REST_SERVER_CONF}"
    printf '%s\n' 'restserver.url=http://0.0.0.0:8080' > conf/gremlin-server.yaml

    AUTHENTICATOR_CLASS=com.example.OperatorAuth
    export AUTHENTICATOR_CLASS
    align_auth_config
    [[ "${AUTHENTICATOR_CLASS}" == "com.example.OperatorAuth" ]]

    unset AUTHENTICATOR_CLASS
    align_auth_config
    [[ "${AUTHENTICATOR_CLASS}" == \
        "org.apache.hugegraph.auth.StandardAuthenticator" ]]
)

# An empty mounted config still gets its definitions.  GNU sed's `$`
# address never matches when the file has no lines, so enable-auth.sh's
# `sed -i '$a\...'` appends were silent no-ops on an empty
# rest-server.properties and an empty gremlin-server.yaml: the
# entrypoint had already written auth.admin_pa and init-store had run in
# auth mode, yet neither server was told to authenticate at all.
empty_dir="${test_dir}/empty-config"
mkdir -p "${empty_dir}/bin" "${empty_dir}/conf/graphs"
cp "$(cd "$(dirname "${BASH_SOURCE[0]}")/../../src/assembly/static/bin" && pwd)/enable-auth.sh" \
    "${empty_dir}/bin/enable-auth.sh"
chmod +x "${empty_dir}/bin/enable-auth.sh"
(
    cd "${empty_dir}" || exit 1
    : > conf/rest-server.properties
    : > conf/gremlin-server.yaml
    printf '%s\n' 'gremlin.graph=org.apache.hugegraph.HugeFactory' \
        > conf/graphs/hugegraph.properties
    unset AUTHENTICATOR_CLASS
    ./bin/enable-auth.sh
    grep -q '^auth\.authenticator=org\.apache\.hugegraph\.auth\.StandardAuthenticator$' \
        conf/rest-server.properties
    grep -q '^auth\.graph_store=hugegraph$' conf/rest-server.properties
    grep -q '^authentication: {$' conf/gremlin-server.yaml
    grep -q '^  authenticator: org\.apache\.hugegraph\.auth\.StandardAuthenticator,$' \
        conf/gremlin-server.yaml
    grep -q '^  config: {tokens: conf/rest-server\.properties}$' \
        conf/gremlin-server.yaml
    grep -q '^}' conf/gremlin-server.yaml
    grep -q 'HugeFactoryAuthProxy' conf/graphs/hugegraph.properties
    # Idempotent: a second run adds nothing to what the first one wrote.
    wc -l < conf/gremlin-server.yaml > "${test_dir}/empty-yaml-count"
    ./bin/enable-auth.sh
    [[ "$(wc -l < conf/gremlin-server.yaml)" == \
        "$(cat "${test_dir}/empty-yaml-count")" ]]

    # A config whose last line has no terminator still gets a line of its
    # own; `sed -i '$a'` closed that terminator for us.
    printf 'restserver.url=http://127.0.0.1:8080' > conf/rest-server.properties
    ./bin/enable-auth.sh
    grep -q '^auth\.authenticator=' conf/rest-server.properties
    grep -q '^restserver\.url=http://127\.0\.0\.1:8080$' conf/rest-server.properties
)

# A copy-back that fails part way must not leave a truncated config.  The
# shell's `>` truncates the destination before cat writes a byte, so
# props.awk snapshots the original first and puts it back.  The snapshot
# `cat` is replaced through PATH to fail the copy the way ENOSPC would:
# stdout here *is* the already-truncated destination, so a few bytes and a
# non-zero exit is exactly a half-written config.
failbin="${test_dir}/fakebin"
mkdir -p "${failbin}"
real_cat="$(command -v cat)"
printf '%s\n' \
    '#!/bin/sh' \
    'case "$*" in' \
    '    *.tmp) printf "auth.authenticator=par"; exit 1 ;;' \
    '    *.bak) [ -n "${FAKE_BAK_FAIL:-}" ] && exit 1' \
    'esac' \
    'exec "${FAKE_CAT_REAL}" "$@"' \
    > "${failbin}/cat"
chmod +x "${failbin}/cat"
rb_file="${test_dir}/config-rollback"
rb_expect="${test_dir}/config-rollback.expected"
printf '%s\n' \
    'auth.authenticator=org.apache.hugegraph.auth.StandardAuthenticator' \
    'auth.token_secret=s3cr3t' \
    'unrelated=true' > "${rb_file}"
cp -p "${rb_file}" "${rb_expect}"
(
    PATH="${failbin}:${PATH}"
    FAKE_CAT_REAL="${real_cat}"
    export PATH FAKE_CAT_REAL
    if set_prop 'auth.authenticator' 'com.example.HalfWritten' "${rb_file}"; then
        echo "set_prop must fail when the copy-back fails" >&2
        exit 1
    fi
) 2>/dev/null
cmp -s "${rb_file}" "${rb_expect}" || {
    echo "a failed copy-back must leave the previous content in place" >&2
    exit 1
}
# Both staging files survive on purpose: the temp file is what was being
# written, and the snapshot is the operator's way back.
[[ -e "${rb_file}.tmp" ]]
[[ -e "${rb_file}.bak" ]]
# Once the condition clears the same set goes through, and leaves nothing
# behind.
set_prop 'auth.authenticator' 'com.example.HalfWritten' "${rb_file}"
grep -q '^auth\.authenticator=com\.example\.HalfWritten$' "${rb_file}"
grep -q '^auth\.token_secret=s3cr3t$' "${rb_file}"
grep -q '^unrelated=true$' "${rb_file}"
[[ ! -e "${rb_file}.tmp" ]]
[[ ! -e "${rb_file}.bak" ]]
# When the restore fails too there is nothing left to do but say so and
# point at the snapshot, because that snapshot is the only copy of a
# working config the operator has.
printf '%s\n' \
    'auth.authenticator=org.apache.hugegraph.auth.StandardAuthenticator' \
    'auth.token_secret=s3cr3t' \
    'unrelated=true' > "${rb_file}"
rb_out=$(
    PATH="${failbin}:${PATH}"
    FAKE_CAT_REAL="${real_cat}"
    FAKE_BAK_FAIL=1
    export PATH FAKE_CAT_REAL FAKE_BAK_FAIL
    set_prop 'auth.authenticator' 'com.example.HalfWritten' "${rb_file}" 2>&1
) || true
[[ "${rb_out}" == *"${rb_file}.bak"* ]] || {
    echo "props.awk must name the snapshot when the restore also fails" >&2
    exit 1
}
# The damaged config keeps whatever the aborted copy left, and the
# snapshot still holds the last known good content.
[[ -e "${rb_file}.bak" ]]
[[ -e "${rb_file}.tmp" ]]
cmp -s "${rb_file}.bak" "${rb_expect}" || {
    echo "the snapshot must be a byte-for-byte copy of the original" >&2
    exit 1
}
