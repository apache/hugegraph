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
for fn in encode_prop_value set_prop_encoded set_prop get_prop_encoded \
          yaml_auth_state check_auth_sides; do
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

# yaml_auth_state reports whether the top-level authentication mapping names
# an authenticator, without ever reading the class: quoted scalars and inline
# comments still count as naming one, a flow mapping on the key line counts, a
# mapping with no authenticator is "nameless", and an `authentication:` nested
# under some other key is not the Gremlin mapping at all.
yaml_dir="${test_dir}/yaml"
mkdir -p "${yaml_dir}/conf"
(
    cd "${yaml_dir}" || exit 1
    state_file="conf/gremlin-server.yaml"

    want_state() {
        if [[ "$1" != "$2" ]]; then
            echo "expected yaml state '$1', got '$2'" >&2
            exit 1
        fi
    }

    printf '%s\n' 'host: 0.0.0.0' > "${state_file}"
    want_state none "$(yaml_auth_state)"

    printf '%s\n' \
        'authentication:' \
        '  authenticator: com.example.MyAuth' \
        > "${state_file}"
    want_state named "$(yaml_auth_state)"

    printf '%s\n' \
        'authentication:' \
        '  authenticator: "com.example.MyAuth"  # custom' \
        '  authenticationHandler: org.apache.hugegraph.auth.WsAndHttpBasicAuthHandler' \
        > "${state_file}"
    want_state named "$(yaml_auth_state)"

    printf '%s\n' \
        'authentication: {authenticator: com.example.FlowAuth, authenticationHandler: org.apache.hugegraph.auth.WsAndHttpBasicAuthHandler, config: {tokens: conf/rest-server.properties}}' \
        > "${state_file}"
    want_state named "$(yaml_auth_state)"

    printf '%s\n' \
        'authentication:' \
        '  authenticationHandler: org.apache.hugegraph.auth.WsAndHttpBasicAuthHandler' \
        > "${state_file}"
    want_state nameless "$(yaml_auth_state)"

    # The nested mapping belongs to someFeature, not to the Gremlin server.
    # Reading it as the Gremlin one would let com.example.Nested authenticate
    # REST while Gremlin stayed on TinkerPop's AllowAllAuthenticator default.
    printf '%s\n' \
        'someFeature:' \
        '  authentication:' \
        '    authenticator: com.example.Nested' \
        > "${state_file}"
    want_state none "$(yaml_auth_state)"

    # An authenticator that only appears after the block ends is a sibling's.
    printf '%s\n' \
        'authentication:' \
        '  tokens: conf/rest-server.properties' \
        'other:' \
        '  authenticator: com.example.Other' \
        > "${state_file}"
    want_state nameless "$(yaml_auth_state)"

    # A blank line does not close a YAML mapping.
    printf '%s\n' \
        'authentication:' \
        '  tokens: conf/rest-server.properties' \
        '' \
        '  authenticator: com.example.Later' \
        > "${state_file}"
    want_state named "$(yaml_auth_state)"

    rm -f "${state_file}"
    want_state none "$(yaml_auth_state)"
)

# check_auth_sides keeps the guarantee the class parsing used to serve: REST and
# Gremlin never end up with authentication on one side only.  Neither and both
# pass; one side, or a mapping that names no authenticator, stops the boot.
sides_dir="${test_dir}/sides"
mkdir -p "${sides_dir}/conf"
(
    cd "${sides_dir}" || exit 1
    REST_SERVER_CONF="./conf/rest-server.properties"

    must_refuse() {
        if check_auth_sides; then
            echo "check_auth_sides must refuse: $1" >&2
            exit 1
        fi
    }

    printf '%s\n' 'host: 0.0.0.0' > conf/gremlin-server.yaml
    : > "${REST_SERVER_CONF}"
    check_auth_sides

    # Both sides configured, different classes: untouched.  enable-auth.sh's
    # per-file guards then make its appends no-ops, so nothing here has to
    # know which class either side names.
    printf '%s\n' 'auth.authenticator=org.apache.hugegraph.auth.StandardAuthenticator' \
        > "${REST_SERVER_CONF}"
    printf '%s\n' 'authentication:' '  authenticator: com.example.OtherAuth' \
        > conf/gremlin-server.yaml
    check_auth_sides
    grep -Eq '^[[:blank:]]*auth[\\]?\.authenticator[[:blank:]]*([:=]|[[:blank:]])com\.example\.OtherAuth' \
        "${REST_SERVER_CONF}" && {
        echo "check_auth_sides must not copy a class into rest-server.properties" >&2
        exit 1
    }

    # One side only.
    printf '%s\n' 'auth.authenticator=com.example.MyAuth' > "${REST_SERVER_CONF}"
    printf '%s\n' 'host: 0.0.0.0' > conf/gremlin-server.yaml
    must_refuse "rest-server.properties names an authenticator and the yaml does not"

    : > "${REST_SERVER_CONF}"
    printf '%s\n' 'authentication:' '  authenticator: com.example.YamlAuth' \
        > conf/gremlin-server.yaml
    must_refuse "the yaml names an authenticator and rest-server.properties does not"

    # A mapping that names no authenticator is refused even when REST is empty:
    # enable-auth.sh guards on the presence of `authentication:`, so it would
    # write the REST file alone and leave Gremlin unauthenticated.
    printf '%s\n' 'authentication:' \
        '  authenticationHandler: org.apache.hugegraph.auth.WsAndHttpBasicAuthHandler' \
        > conf/gremlin-server.yaml
    : > "${REST_SERVER_CONF}"
    must_refuse "the yaml mapping names no authenticator"
    printf '%s\n' 'auth.authenticator=com.example.MyAuth' > "${REST_SERVER_CONF}"
    must_refuse "the yaml mapping names no authenticator and REST does"
)

# The refusal above is what keeps enable-auth.sh from writing one side:
# against the same ambiguous layout, enable-auth.sh on its own writes only
# the REST file (its yaml guard already sees an `authentication:` line),
# leaving REST on StandardAuthenticator and Gremlin on TinkerPop's
# AllowAllAuthenticator default.  The entrypoint never lets it run there
# because check_auth_sides fails first under set -e.
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
    if check_auth_sides; then
        echo "check_auth_sides must refuse a yaml mapping without an authenticator" >&2
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
set_prop 'auth.authenticator' 'com.example.NewAuth' "${crlf_file}"
grep -q '^auth\.authenticator=com\.example\.NewAuth$' "${crlf_file}"
[[ "$(get_prop_encoded 'pd.peers' "${crlf_file}")" == "a,b" ]]
if ! grep -q $'^unrelated=true\r$' "${crlf_file}"; then
    echo "CRLF bytes of untouched lines must be preserved" >&2
    exit 1
fi

# An escaped key is the same key: java.util.Properties unescapes the name, so
# `auth\.authenticator` has to be found by a read or a write of
# `auth.authenticator` instead of being treated as absent and appended beside.
# (Comparing the class across the two files went away with the yaml scalar
# parser, so only the key grammar is left to pin down here.)
escaped_auth_dir="${test_dir}/escaped-auth-key"
mkdir -p "${escaped_auth_dir}/conf"
(
    cd "${escaped_auth_dir}" || exit 1
    REST_SERVER_CONF="./conf/rest-server.properties"
    printf '%s\n' \
        'auth\.authenticator=com.example.OldAuth' \
        'unrelated=true' \
        > "${REST_SERVER_CONF}"
    [[ "$(get_prop_encoded 'auth.authenticator' "${REST_SERVER_CONF}")" == \
        "com.example.OldAuth" ]]
    set_prop 'auth.authenticator' 'com.example.NewAuth' "${REST_SERVER_CONF}"
    assert_line_count 1 'auth[\\]?\.authenticator' "${REST_SERVER_CONF}"
    grep -q '^auth\.authenticator=com\.example\.NewAuth$' "${REST_SERVER_CONF}"
    assert_line_count 1 '^unrelated=true$' "${REST_SERVER_CONF}"
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

# Two yaml shapes the scoping has to keep getting right: a sibling mapping
# that carries its own authenticator must not hide the block's, and a commented
# authenticator must not count as one.
scope_dir="${test_dir}/yaml-scope"
mkdir -p "${scope_dir}/conf"
(
    cd "${scope_dir}" || exit 1
    want_state() {
        if [[ "$1" != "$2" ]]; then
            echo "expected yaml state '$1', got '$2'" >&2
            exit 1
        fi
    }

    printf '%s\n' \
        'authentication:' \
        '  authenticator: com.example.GremlinAuth' \
        '  authenticationHandler: org.apache.hugegraph.auth.WsAndHttpBasicAuthHandler' \
        'ssl:' \
        '  authenticator: com.example.TlsOnly' \
        > conf/gremlin-server.yaml
    want_state named "$(yaml_auth_state)"

    printf '%s\n' \
        'authentication:' \
        '#  authenticator: com.example.CommentedAuth' \
        '  authenticationHandler: org.apache.hugegraph.auth.WsAndHttpBasicAuthHandler' \
        > conf/gremlin-server.yaml
    want_state nameless "$(yaml_auth_state)"
)

# Both sides silent means "bootstrap authentication", and the class then comes
# from enable-auth.sh: an operator who passed AUTHENTICATOR_CLASS gets the class
# they asked for, and only an unset one falls back to StandardAuthenticator.
# With the entrypoint no longer exporting a class of its own, this is the whole
# of the guarantee, so it is asserted where the default now lives.
class_dir="${test_dir}/authenticator-class"
(
    # A fresh tree per run: enable-auth.sh keeps its own backup of the configs
    # it writes, so re-running it over one directory is not a clean case.
    run_enable_auth() {
        local dir="$1" want="$2"
        mkdir -p "${dir}/bin" "${dir}/conf/graphs"
        cp "$(cd "$(dirname "${BASH_SOURCE[0]}")/../../src/assembly/static/bin" && pwd)/enable-auth.sh" \
            "${dir}/bin/enable-auth.sh"
        chmod +x "${dir}/bin/enable-auth.sh"
        printf '%s\n' 'gremlin.graph=org.apache.hugegraph.HugeFactory' \
            > "${dir}/conf/graphs/hugegraph.properties"
        : > "${dir}/conf/rest-server.properties"
        : > "${dir}/conf/gremlin-server.yaml"
        (
            cd "${dir}" || exit 1
            if [[ -n "${want}" ]]; then
                AUTHENTICATOR_CLASS="${want}"
                export AUTHENTICATOR_CLASS
            else
                unset AUTHENTICATOR_CLASS
            fi
            ./bin/enable-auth.sh
        )
    }

    run_enable_auth "${class_dir}/operator" "com.example.OperatorAuth"
    grep -q '^auth\.authenticator=com\.example\.OperatorAuth$' \
        "${class_dir}/operator/conf/rest-server.properties"
    grep -q '^  authenticator: com\.example\.OperatorAuth,$' \
        "${class_dir}/operator/conf/gremlin-server.yaml"

    run_enable_auth "${class_dir}/default" ""
    grep -q '^auth\.authenticator=org\.apache\.hugegraph\.auth\.StandardAuthenticator$' \
        "${class_dir}/default/conf/rest-server.properties"
    grep -q '^  authenticator: org\.apache\.hugegraph\.auth\.StandardAuthenticator,$' \
        "${class_dir}/default/conf/gremlin-server.yaml"
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

# A value whose encoded form ends in an odd number of backslashes must not be
# written at all.  The entrypoint copies an existing secret between files with
# set_prop_encoded, replaying the raw bytes, and on disk `key=abc\` as the last
# line of a mounted config reads back as no property at all under
# commons-configuration2 (what HugeConfig extends).  Written into a file where
# it is no longer last, it turns the following line into a continuation of the
# secret: the server then sees neither the secret nor that property, and the
# entrypoint has published a credential nothing will read.
bs_file="${test_dir}/config-trailing-backslash"
bs_pristine="${test_dir}/config-trailing-backslash.pristine"
printf '%s\n' 'unrelated=true' > "${bs_file}"
cp "${bs_file}" "${bs_pristine}"
if set_prop_encoded 'auth.token_secret' 'abc\' "${bs_file}" 2>/dev/null; then
    echo "props.awk must refuse a value ending in an odd number of backslashes" >&2
    exit 1
fi
cmp -s "${bs_file}" "${bs_pristine}" || {
    echo "a refused set must leave the config byte-for-byte untouched" >&2
    exit 1
}

# An escaped backslash — two of them — is not a continuation, so it stays
# writable and replays byte for byte.  Built from parts because a doubled
# backslash inside one literal is easy to write and hard to read back.
bs='\'
two_bs="abc${bs}${bs}"
set_prop_encoded 'auth.token_secret' "${two_bs}" "${bs_file}"
[[ "$(get_prop_encoded 'auth.token_secret' "${bs_file}")" == "${two_bs}" ]]
assert_line_count 1 '^unrelated=true$' "${bs_file}"
