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
          get_prop_decoded yaml_auth_state check_auth_sides; do
    eval "$(awk -v fn="${fn}" '
        index($0, fn "() {") == 1 { capture = 1 }
        capture { print }
        capture && /^}$/ { exit }
    ' "${entrypoint}")"
done
log() { echo "[hugegraph-server-entrypoint] $*"; }
static_bin="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../src/assembly/static/bin" && pwd)"
docker_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROPS_AWK="${static_bin}/props.awk"
YAMLSCAN="${docker_dir}/yamlscan.awk"
export PROPS_AWK YAMLSCAN

# enable-auth.sh reads and writes .properties through props.awk, which the
# release assembly packages in the same bin/ directory.  A test tree that runs
# the script therefore has to carry both, or it is not the layout it ships in.
# yamlscan.awk goes one level up, in the install home, exactly where the
# Dockerfile puts it: the guard in enable-auth.sh has to answer the Gremlin
# question with the same reader check_auth_sides uses, so a tree without it
# would be testing the tarball fallback rather than the image.
install_enable_auth() {
    local dir="$1"
    mkdir -p "${dir}/bin"
    cp "${static_bin}/enable-auth.sh" "${dir}/bin/enable-auth.sh"
    cp "${static_bin}/props.awk" "${dir}/bin/props.awk"
    cp "${docker_dir}/yamlscan.awk" "${dir}/yamlscan.awk"
    chmod +x "${dir}/bin/enable-auth.sh"
}

# ── What this host can actually be asked about ─────────────────────────
# CI runs these assertions on Ubuntu, where every one of them means what it
# says.  Developed against a Windows host, three things silently stop being
# observations about props.awk and become observations about the platform:
# MSYS gawk opens text files in translation mode and drops the CR of a CRLF
# pair, chmod does not affect the mode stat reports, and a symlinked config is
# not a symlink.  Each group is therefore gated on a probe of the host, and a
# skipped group says so out loud rather than passing quietly.
skip() { echo "note: skipped $1 -- this host cannot exercise it; it runs under CI" >&2; }

probe="${test_dir}/probe"

awk_sees_crlf_cr=0
if [[ "$(printf 'x\r\n' | awk 'NR == 1 { print length($0) }')" == "2" ]]; then
    awk_sees_crlf_cr=1
fi
awk_sees_lone_cr=0
if [[ "$(printf 'a\rb' | awk 'NR == 1 { print length($0) }')" == "3" ]]; then
    awk_sees_lone_cr=1
fi

host_keeps_chmod=0
printf '%s\n' x > "${probe}"
chmod 600 "${probe}"
[[ "$(stat -c '%a' "${probe}")" == "600" ]] && host_keeps_chmod=1
rm -f "${probe}"

host_keeps_symlink=0
printf '%s\n' x > "${probe}-t"
ln -s "${probe}-t" "${probe}-l" 2>/dev/null && [[ -L "${probe}-l" ]] && host_keeps_symlink=1
rm -f "${probe}-t" "${probe}-l"

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
install_enable_auth "${onesided_dir}"
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
# The backslash goes through %s on purpose: in one format string, `\\\r` is
# reduced to a backslash followed by the letter r by some printf
# implementations, which quietly turns this continuation case into a plain line
# and makes the assertions below pass for the wrong reason.
printf '%s\r\n' 'pd.peers=a,\' >> "${crlf_file}"
printf '  b\r\n' >> "${crlf_file}"
printf 'unrelated=true\r\n' >> "${crlf_file}"
[[ "$(get_prop_encoded 'auth.authenticator' "${crlf_file}")" == \
    "org.apache.hugegraph.auth.StandardAuthenticator" ]]
[[ "$(get_prop_encoded 'pd.peers' "${crlf_file}")" == "a,b" ]]
set_prop 'auth.authenticator' 'com.example.NewAuth' "${crlf_file}"
grep -q '^auth\.authenticator=com\.example\.NewAuth$' "${crlf_file}"
[[ "$(get_prop_encoded 'pd.peers' "${crlf_file}")" == "a,b" ]]
if (( awk_sees_crlf_cr )); then
    if ! grep -q $'^unrelated=true\r$' "${crlf_file}"; then
        echo "CRLF bytes of untouched lines must be preserved" >&2
        exit 1
    fi
else
    skip "the CRLF byte check"
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
grep -q '^init_store\.enabled=true$' "${mode_file}"
grep -q '^unrelated=true$' "${mode_file}"
[[ ! -e "${mode_file}.tmp" ]]
[[ ! -e "${mode_file}.bak" ]]
if (( host_keeps_chmod )); then
    [[ "$(stat -c '%a' "${mode_file}")" == "600" ]]
else
    skip "the config-mode-preservation check"
fi

target_file="${test_dir}/config-target"
link_file="${test_dir}/config-link"
if (( host_keeps_symlink )); then
    # The whole block has to be gated, not just the -L check: where ln -s
    # produces a copy instead, writing the link updates a regular file and the
    # target stays untouched, which would fail for the host's reason.
    printf '%s\n' 'unrelated=true' > "${target_file}"
    ln -s "${target_file}" "${link_file}"
    set_prop "init_store.enabled" "true" "${link_file}"
    [[ -L "${link_file}" ]] || {
        echo "a set must not replace a symlinked config with a regular file" >&2
        exit 1
    }
    grep -q '^init_store\.enabled=true$' "${target_file}"
else
    skip "the symlinked-config check"
fi

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
        mkdir -p "${dir}/conf/graphs"
        install_enable_auth "${dir}"
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
install_enable_auth "${empty_dir}"
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

# ── CR-only line terminators ──────────────────────────────────────────
# java.util.Properties ends a line at a bare CR as well, so a config written
# that way holds one property per CR-separated chunk.  Reading it with a
# \n-only split made the entire file one record: only the first key was ever
# seen, and rewriting that key replaced the record with a single line, which
# silently deleted every property after it -- including auth.authenticator, so
# the file the server then read had no authentication configured at all.
if (( awk_sees_lone_cr )); then
    cr_file="${test_dir}/config-cr"
    printf 'graph=a\rpd.peers=b\rauth.authenticator=org.apache.hugegraph.auth.StandardAuthenticator\r' \
        > "${cr_file}"

    [[ "$(get_prop_encoded 'graph' "${cr_file}")" == "a" ]]
    [[ "$(get_prop_encoded 'pd.peers' "${cr_file}")" == "b" ]]
    [[ "$(get_prop_encoded 'auth.authenticator' "${cr_file}")" == \
        "org.apache.hugegraph.auth.StandardAuthenticator" ]]

    cp "${cr_file}" "${cr_file}.before"
    set_prop 'graph' 'org.apache.hugegraph.auth.HugeFactoryAuthProxy' "${cr_file}"

    # Every key that was there before is still there afterwards, with the
    # values the rewrite was not about.
    [[ "$(get_prop_encoded 'pd.peers' "${cr_file}")" == "b" ]] || {
        echo "a CR-only config lost pd.peers when an unrelated key was rewritten" >&2
        exit 1
    }
    [[ "$(get_prop_encoded 'auth.authenticator' "${cr_file}")" == \
        "org.apache.hugegraph.auth.StandardAuthenticator" ]] || {
        echo "a CR-only config lost auth.authenticator when an unrelated key was rewritten" >&2
        exit 1
    }
    [[ "$(get_prop_encoded 'graph' "${cr_file}")" == \
        "org.apache.hugegraph.auth.HugeFactoryAuthProxy" ]]
    # One definition per key, so the rewrite replaced rather than appended.
    # Counted on CR folded to LF because grep only ever starts a new line at
    # LF, and a CR-only file is a single line to it.
    count_records() {
        local pattern="$1" file="$2"
        # grep exits 1 on a zero count, which errexit would take as the
        # interesting failure; the printed number is the answer here.
        tr '\r' '\n' < "${file}" | grep -Ec "${pattern}" || true
    }
    [[ "$(count_records '^graph=' "${cr_file}")" == "1" ]] || {
        echo "a CR-only rewrite must leave exactly one graph definition" >&2
        exit 1
    }
    [[ "$(count_records '^auth\.authenticator=' "${cr_file}")" == "1" ]] || {
        echo "a CR-only rewrite must leave exactly one auth.authenticator definition" >&2
        exit 1
    }

    # Mixed terminators in one file, the state an upgraded mounted volume
    # actually reaches: CRLF from a Windows edit, CR from an old store(), LF
    # from the image.
    mix_file="${test_dir}/config-mixed-eol"
    printf 'graph=a\rpd.peers=b\nauth.authenticator=c\r\nunrelated=d\n' > "${mix_file}"
    [[ "$(get_prop_encoded 'graph' "${mix_file}")" == "a" ]]
    [[ "$(get_prop_encoded 'pd.peers' "${mix_file}")" == "b" ]]
    [[ "$(get_prop_encoded 'auth.authenticator' "${mix_file}")" == "c" ]]
    [[ "$(get_prop_encoded 'unrelated' "${mix_file}")" == "d" ]]
fi

# ── Form feed is separator whitespace to Java ─────────────────────────
# java.util.Properties counts \f as whitespace on both sides of the key/value
# boundary, so `auth.authenticator<FF>=...` is that property.  Recognising only
# space and tab parsed the form feed into the key name instead, and a mounted
# config written that way read as unconfigured -- which the guards then answered
# by appending a second, competing definition.
ff_file="${test_dir}/config-formfeed"
printf 'auth.authenticator\fs=org.apache.hugegraph.auth.StandardAuthenticator\n' > "${ff_file}"
[[ "$(get_prop_encoded 'auth.authenticator' "${ff_file}")" == \
    "s=org.apache.hugegraph.auth.StandardAuthenticator" ]] || {
    echo "a form feed before the separator must end the key, as it does in Java" >&2
    exit 1
}
printf 'auth.authenticator\forg.apache.hugegraph.auth.X\n' > "${ff_file}"
[[ "$(get_prop_encoded 'auth.authenticator' "${ff_file}")" == \
    "org.apache.hugegraph.auth.X" ]]
printf 'auth.authenticator=\f1\n' > "${ff_file}"
[[ "$(get_prop_encoded 'auth.authenticator' "${ff_file}")" == "1" ]]
printf '\fauth.authenticator=1\n' > "${ff_file}"
[[ "$(get_prop_encoded 'auth.authenticator' "${ff_file}")" == "1" ]]
# A line that is only form feed whitespace is blank to Java, not a property.
printf '\f\f\ngraph=a\n' > "${ff_file}"
[[ "$(get_prop_encoded 'graph' "${ff_file}")" == "a" ]]
assert_line_count 1 '^graph=a$' "${ff_file}"

# has-mode answers "is this key defined" without confusing an empty definition
# with no definition, which is what an append guard needs: appending a default
# on top of `auth.authenticator=` leaves the empty first definition in force.
has_file="${test_dir}/config-has"
printf 'auth.authenticator=\n' > "${has_file}"
if ! PROPS_MODE=has PROPS_KEY='auth.authenticator' PROPS_FILE="${has_file}" \
        awk -f "${PROPS_AWK}" /dev/null; then
    echo "PROPS_MODE=has must report an empty definition as present" >&2
    exit 1
fi
if PROPS_MODE=has PROPS_KEY='auth.graph_store' PROPS_FILE="${has_file}" \
        awk -f "${PROPS_AWK}" /dev/null; then
    echo "PROPS_MODE=has must report an absent key as absent" >&2
    exit 1
fi
# An unreadable file must not read as "absent": status 2 is what tells a caller
# wearing errexit to stop rather than append a default over a file it could not
# read.
status=0
PROPS_MODE=has PROPS_KEY='k' PROPS_FILE="${test_dir}/no-such-file" \
    awk -f "${PROPS_AWK}" /dev/null 2>/dev/null || status=$?
if (( status != 2 )); then
    echo "PROPS_MODE=has must exit 2 for an unreadable file, got ${status}" >&2
    exit 1
fi

# get with PROPS_DECODED=1 hands back the value as the server would see it,
# which is what a guard that compares a class name needs.
dec_file="${test_dir}/config-decoded"
printf 'gremlin\\u002egraph=org.apache.hugegraph.HugeFactory\n' > "${dec_file}"
[[ "$(PROPS_MODE=get PROPS_DECODED=1 PROPS_KEY='gremlin.graph' \
      PROPS_FILE="${dec_file}" awk -f "${PROPS_AWK}" /dev/null)" == \
    "org.apache.hugegraph.HugeFactory" ]]
[[ "$(PROPS_MODE=get PROPS_KEY='gremlin\u002egraph' PROPS_FILE="${dec_file}" \
      awk -f "${PROPS_AWK}" /dev/null)" == "" ]]

# ── gremlin.graph spelled with a Unicode escape still gets wrapped ──────
# \u002e is a dot to java.util.Properties, so this is the plain HugeFactory and
# enable-auth.sh has to route authentication through it.  The grep/sed pair
# matched only a literal or backslash-escaped dot, missed this one, and left the
# graph factory unwrapped while both servers had been told authentication was
# on -- the one remaining path where the REST side was configured and the graph
# behind it was not.
u2e_dir="${test_dir}/u2e-wrap"
mkdir -p "${u2e_dir}/conf/graphs"
install_enable_auth "${u2e_dir}"
: > "${u2e_dir}/conf/rest-server.properties"
: > "${u2e_dir}/conf/gremlin-server.yaml"
printf '%s\n' 'gremlin\u002egraph=org.apache.hugegraph.HugeFactory' \
    > "${u2e_dir}/conf/graphs/hugegraph.properties"
(
    cd "${u2e_dir}" || exit 1
    unset AUTHENTICATOR_CLASS
    ./bin/enable-auth.sh
    if [[ "$(PROPS_MODE=get PROPS_DECODED=1 PROPS_KEY='gremlin.graph' \
             PROPS_FILE=./conf/graphs/hugegraph.properties \
             awk -f "${PROPS_AWK}" /dev/null)" != \
          "org.apache.hugegraph.auth.HugeFactoryAuthProxy" ]]; then
        echo "a gremlin.graph key written as \\u002e must still be wrapped" >&2
        exit 1
    fi
    # One definition, not the original left behind plus a new one.
    if [[ "$(grep -c 'HugeFactory' ./conf/graphs/hugegraph.properties)" != "1" ]]; then
        echo "wrapping a \\u002e-escaped key must not leave the old definition" >&2
        exit 1
    fi
)

# A CR-only graph config wraps too, and keeps the keys around it.
if (( awk_sees_lone_cr )); then
    crwrap_dir="${test_dir}/cr-wrap"
    mkdir -p "${crwrap_dir}/conf/graphs"
    install_enable_auth "${crwrap_dir}"
    : > "${crwrap_dir}/conf/rest-server.properties"
    : > "${crwrap_dir}/conf/gremlin-server.yaml"
    printf 'gremlin.graph=org.apache.hugegraph.HugeFactory\rbackend=rocksdb\r' \
        > "${crwrap_dir}/conf/graphs/hugegraph.properties"
    (
        cd "${crwrap_dir}" || exit 1
        unset AUTHENTICATOR_CLASS
        ./bin/enable-auth.sh
        [[ "$(get_prop_encoded 'backend' ./conf/graphs/hugegraph.properties)" == \
            "rocksdb" ]] || {
            echo "wrapping a CR-only graph config dropped a later key" >&2
            exit 1
        }
        [[ "$(get_prop_encoded 'gremlin.graph' ./conf/graphs/hugegraph.properties)" == \
            "org.apache.hugegraph.auth.HugeFactoryAuthProxy" ]]
    )
fi

# ── A failed append must fail the script ──────────────────────────────
# The entrypoint runs enable-auth.sh and trusts its exit status, so a run that
# configures REST and then cannot write the yaml has to say so.  Without
# errexit and per-write checks it exited 0 on exactly that half-done tree: the
# mounted read-only gremlin-server.yaml made the yaml append fail while both
# rest-server.properties appends succeeded.
ro_dir="${test_dir}/read-only-yaml"
mkdir -p "${ro_dir}/conf/graphs"
install_enable_auth "${ro_dir}"
: > "${ro_dir}/conf/rest-server.properties"
printf 'host: 8182\n' > "${ro_dir}/conf/gremlin-server.yaml"
printf '%s\n' 'gremlin.graph=org.apache.hugegraph.HugeFactory' \
    > "${ro_dir}/conf/graphs/hugegraph.properties"
(
    cd "${ro_dir}" || exit 1
    unset AUTHENTICATOR_CLASS
    chmod 444 conf/gremlin-server.yaml
    status=0
    ./bin/enable-auth.sh 2>/dev/null || status=$?
    chmod 644 conf/gremlin-server.yaml
    if (( status == 0 )); then
        echo "enable-auth.sh must exit nonzero when a config append fails" >&2
        exit 1
    fi
    if grep -Eq '^[[:blank:]]*authentication[[:blank:]]*:' conf/gremlin-server.yaml; then
        echo "the unwritable yaml file must not have been changed" >&2
        exit 1
    fi
)

# ── yaml_auth_state answers about the mapping, not about the text ───────
# Each case below is a mounted gremlin-server.yaml that a grep-shaped reader
# calls named while the Gremlin server runs without an authenticator.  Reported
# parity on such a file is how REST ends up enforcing and Gremlin open, so the
# reader follows the mapping structure instead of the substring.
yaml_case() {
    local want="$1" desc="$2" dir
    shift 2
    dir="${test_dir}/yaml-$(printf '%s' "${desc}" | tr -c 'A-Za-z0-9' '-')"
    mkdir -p "${dir}/conf"
    printf '%s\n' "$@" > "${dir}/conf/gremlin-server.yaml"
    (
        cd "${dir}" || exit 1
        got=$(yaml_auth_state)
        if [[ "${got}" != "${want}" ]]; then
            echo "yaml_auth_state: ${desc}: got ${got}, want ${want}" >&2
            exit 1
        fi
    )
}

# A flow mapping that names nothing, with a commented-out authenticator behind
# it: the text is there, the key is not.
yaml_case nameless "flow empty with authenticator in a comment" \
    'authentication: {} # authenticator: org.apache.hugegraph.auth.StandardAuthenticator'
# A comment line inside the mapping is not the end of it, so a valid
# deployment with a note between the keys must not be refused.
yaml_case named "column-zero comment inside the mapping" \
    'authentication:' \
    '# configured by the operator' \
    '  authenticator: org.apache.hugegraph.auth.StandardAuthenticator'
# config is its own map, so an authenticator under it is the token store
# configuration and not the server authenticator.
yaml_case nameless "authenticator nested under config" \
    'authentication:' \
    '  config:' \
    '    authenticator: org.apache.hugegraph.auth.StandardAuthenticator'
yaml_case nameless "authenticator nested inside a flow config" \
    'authentication: {config: {authenticator: org.apache.hugegraph.auth.StandardAuthenticator}}'
# The positive cases a wrong reader must keep accepting.
yaml_case named "plain block child" \
    'authentication:' \
    '  authenticator: org.apache.hugegraph.auth.StandardAuthenticator'
yaml_case named "direct flow child with siblings" \
    'authentication: {config: {tokens: conf/rest-server.properties}, authenticator: org.apache.hugegraph.auth.StandardAuthenticator}'
yaml_case named "quoted key" \
    'authentication:' \
    '  "authenticator": org.apache.hugegraph.auth.StandardAuthenticator'
# An authenticator key that names no class leaves the server on
# AllowAllAuthenticator, so it is the nameless case.
yaml_case nameless "direct authenticator with no value" \
    'authentication:' \
    '  authenticator:'
yaml_case nameless "direct authenticator set to null" \
    'authentication:' \
    '  authenticator: null'
# An `authentication:` belonging to another mapping is not the server's.
yaml_case none "authentication nested under another key" \
    'server:' \
    '  authentication:' \
    '    authenticator: org.apache.hugegraph.auth.StandardAuthenticator'
yaml_case none "no authentication anywhere" \
    'host: 8182' \
    'port: 1'
# A sibling key at column zero closes the mapping; an authenticator after it
# belongs to the sibling, not to authentication.
yaml_case nameless "sibling key closes the mapping" \
    'authentication:' \
    '  handler: org.apache.hugegraph.auth.WsAndHttpBasicAuthHandler' \
    'metrics:' \
    '  authenticator: org.apache.hugegraph.auth.StandardAuthenticator'
# What snakeyaml resolves to null names no class, and it resolves null
# case-insensitively, so NULL, Null and nUll are the refusal case just as null
# is.  An explicit !!null says it outright, and an empty quoted scalar is the
# empty string, for which loadAuthenticator returns null.  Reporting any of
# these as named is the one direction that cannot be forgiven: REST would
# enforce while Gremlin ran on AllowAllAuthenticator.
for nullish in 'null' 'NULL' 'Null' 'nUll' '~' '!!null' '!!null ~' '""' "''"; do
    yaml_case nameless "authenticator set to the null spelling [${nullish}]" \
        'authentication:' \
        "  authenticator: ${nullish}"
done
# The same values in a flow mapping, where the reader has to reach the value at
# all: a quoted class name used to arrive empty, which refused a valid mounted
# config before the server started.
yaml_case named "unquoted class in a flow mapping" \
    'authentication: {authenticator: org.apache.hugegraph.auth.StandardAuthenticator}'
yaml_case named "double quoted class in a flow mapping" \
    'authentication: {authenticator: "org.apache.hugegraph.auth.StandardAuthenticator"}'
yaml_case named "single quoted class in a flow mapping" \
    "authentication: {authenticator: 'org.apache.hugegraph.auth.StandardAuthenticator'}"
yaml_case named "double quoted class between flow siblings" \
    'authentication: {config: {tokens: conf/rest-server.properties}, authenticator: "org.apache.hugegraph.auth.StandardAuthenticator"}'
# Quoting a scalar makes it a string rather than the null node, so "null" names
# a class the server fails to load loudly at startup; that is not the silent
# no-authenticator state the plain spellings above are.  Pinned so a later
# tightening of the null rules cannot move it without saying so here.
yaml_case named "quoted null is a string, not the null node" \
    'authentication:' \
    '  authenticator: "null"'
# A nested mapping is still the config map even when the value inside it is
# quoted, and an empty quoted scalar is the empty string the server reads as no
# authenticator.
yaml_case nameless "class nested under a flow config, quoted" \
    'authentication: {config: {authenticator: "org.apache.hugegraph.auth.StandardAuthenticator"}}'
yaml_case nameless "flow value that is an empty quoted string" \
    'authentication: {authenticator: ""}'
# A tag, not the text, decides the type of a scalar, and a type this scanner
# cannot resolve is refused rather than guessed at.
yaml_case nameless "explicit str tag, a type this scanner cannot resolve" \
    'authentication:' \
    '  authenticator: !!str org.apache.hugegraph.auth.StandardAuthenticator'

# ── Mounted one-sided config is refused with no PASSWORD ───────────────
# check_auth_sides used to run only inside the PASSWORD branch, so a mounted
# rest-server.properties that already carried auth.authenticator and a yaml
# without a matching mapping was never validated at all: the entrypoint skipped
# the check, never called enable-auth.sh, and started the server with REST
# enforcing and Gremlin open.  The parity check now runs on every start.
mounted_dir="${test_dir}/mounted-one-sided"
mkdir -p "${mounted_dir}/conf/graphs"
(
    cd "${mounted_dir}" || exit 1
    # check_auth_sides reads these two paths, which the entrypoint sets at the
    # top of a run; this block calls the guard directly, as the other unit
    # groups here do.
    REST_SERVER_CONF="./conf/rest-server.properties"
    GRAPH_CONF="./conf/graphs/hugegraph.properties"
    printf '%s\n' \
        'restserver.url=http://127.0.0.1:8080' \
        'auth.authenticator=org.apache.hugegraph.auth.StandardAuthenticator' \
        > conf/rest-server.properties
    printf '%s\n' 'host: 8182' > conf/gremlin-server.yaml
    printf '%s\n' 'backend=rocksdb' > conf/graphs/hugegraph.properties
    if check_auth_sides; then
        echo "check_auth_sides must refuse REST configured with yaml not" >&2
        exit 1
    fi
    # And it accepts the two balanced states, so this is not just a refusal:
    printf '%s\n' \
        'authentication:' \
        '  authenticator: org.apache.hugegraph.auth.StandardAuthenticator' \
        > conf/gremlin-server.yaml
    check_auth_sides
    printf '%s\n' 'host: 8182' > conf/gremlin-server.yaml
    printf '%s\n' \
        'restserver.url=http://127.0.0.1:8080' \
        > conf/rest-server.properties
    check_auth_sides
)

# ── Trees the entrypoint hands to enable-auth.sh ───────────────────────
# check_auth_sides and enable-auth.sh answer one question from two files, so
# they have to answer it the same way.  Every case below is a tree that
# check_auth_sides ACCEPTS -- which is why the entrypoint goes on to run
# enable-auth.sh -- and where the old guard here wrote only one side: it asked
# whether a key or a block was present, while the entrypoint asks whether a
# value names a class.  Those disagree for an `authentication:` nested under
# another feature and for a defined-but-empty `auth.authenticator`, and the
# result was REST enforcing StandardAuthenticator beside a Gremlin left on
# TinkerPop's AllowAllAuthenticator.
parity_dir="${test_dir}/enable-auth-parity"

# bootstrap <dir>: the layout the script ships in, with props.awk beside it.
bootstrap_tree() {
    local dir="$1"
    rm -rf "${dir}"
    mkdir -p "${dir}/conf/graphs"
    install_enable_auth "${dir}"
    printf '%s\n' 'gremlin.graph=org.apache.hugegraph.HugeFactory' \
        > "${dir}/conf/graphs/hugegraph.properties"
    : > "${dir}/conf/gremlin-server.yaml"
    : > "${dir}/conf/rest-server.properties"
}

# The class the server would read, through the same reader rather than through
# grep: an appended second definition looks correct to grep and is invisible
# here, which is the failure these cases are about.
rest_class() {
    PROPS_MODE=get PROPS_DECODED=1 PROPS_KEY=auth.authenticator \
        PROPS_FILE="$1/conf/rest-server.properties" awk -f "${PROPS_AWK}" /dev/null
}

gremlin_state() {
    ( cd "$1" && yaml_auth_state )
}

# accepted_then_both_sides <case> <dir> -- refuse to test a tree the
# entrypoint would never run the script on, then require both sides named.
# REST_SERVER_CONF is a top-level assignment in docker-entrypoint.sh and this
# group evals only the functions, so each call has to carry it: unset, the REST
# side reads as unconfigured whatever the file says, and a one-sided tree would
# be waved through the very guard being asserted.
sides_agree() {
    ( cd "$1" && REST_SERVER_CONF="./conf/rest-server.properties" check_auth_sides )
}

accepted_then_both_sides() {
    local desc="$1" dir="$2" state class
    if ! sides_agree "${dir}" >/dev/null 2>&1; then
        echo "${desc}: check_auth_sides refused this tree, so enable-auth.sh
  is never reached -- the case no longer tests what it was written for" >&2
        exit 1
    fi
    ( cd "${dir}" && unset AUTHENTICATOR_CLASS && ./bin/enable-auth.sh ) || {
        echo "${desc}: enable-auth.sh failed" >&2
        exit 1
    }
    state=$(gremlin_state "${dir}")
    if [[ "${state}" != "named" ]]; then
        echo "${desc}: gremlin-server.yaml is ${state}, not named" >&2
        exit 1
    fi
    class=$(rest_class "${dir}")
    if [[ "${class}" != "org.apache.hugegraph.auth.StandardAuthenticator" ]]; then
        echo "${desc}: rest-server.properties reads back [${class}]" >&2
        exit 1
    fi
    if [[ "$(grep -c '^auth\.authenticator' "${dir}/conf/rest-server.properties")" != "1" ]]; then
        echo "${desc}: auth.authenticator has more than one definition" >&2
        exit 1
    fi
    # Parity has to survive the run, not just the files: a tree the script
    # leaves one-sided must not still pass the guard that let it through.
    if ! sides_agree "${dir}"; then
        echo "${desc}: check_auth_sides rejects the tree enable-auth.sh left" >&2
        exit 1
    fi
}

# An `authentication:` that belongs to another mapping is not the server's, so
# the script owns the whole of the Gremlin side and has to write it.
bootstrap_tree "${parity_dir}/nested"
printf '%s\n' 'host: 0.0.0.0' 'someFeature:' '  authentication:' \
    '    authenticator: com.example.Nested' \
    > "${parity_dir}/nested/conf/gremlin-server.yaml"
printf '%s\n' 'restserver.url=http://127.0.0.1:8080' \
    > "${parity_dir}/nested/conf/rest-server.properties"
accepted_then_both_sides "nested authentication mapping" "${parity_dir}/nested"
# The other feature keeps its own block untouched, and the block written for
# the server is the one at column 0.
grep -q '^authentication: {$' "${parity_dir}/nested/conf/gremlin-server.yaml"
grep -q '^  authentication:$' "${parity_dir}/nested/conf/gremlin-server.yaml"
grep -q '^    authenticator: com\.example\.Nested$' \
    "${parity_dir}/nested/conf/gremlin-server.yaml"

# Both empty spellings, plus a whitespace value: each parses to the empty
# string, so each is the unconfigured side and has to be filled in place.
for empty in 'auth.authenticator=' 'auth.authenticator' 'auth.authenticator=   '; do
    bootstrap_tree "${parity_dir}/empty"
    printf '%s\n' 'host: 0.0.0.0' > "${parity_dir}/empty/conf/gremlin-server.yaml"
    printf '%s\n' "${empty}" 'unrelated=true' \
        > "${parity_dir}/empty/conf/rest-server.properties"
    accepted_then_both_sides "empty definition [${empty}]" "${parity_dir}/empty"
    # The placeholder is rewritten where it stood; unrelated content is kept.
    grep -q '^unrelated=true$' "${parity_dir}/empty/conf/rest-server.properties"
    [[ "$(head -1 "${parity_dir}/empty/conf/rest-server.properties")" == \
        'auth.authenticator=org.apache.hugegraph.auth.StandardAuthenticator' ]]
done

# A value the operator did write is never a default's target.  This tree is
# accepted because both sides already name the same class, and the script has
# to leave it alone rather than replace it with StandardAuthenticator.
bootstrap_tree "${parity_dir}/operator"
printf '%s\n' 'authentication:' '  authenticator: com.example.OperatorAuth' \
    > "${parity_dir}/operator/conf/gremlin-server.yaml"
printf '%s\n' 'auth.authenticator=com.example.OperatorAuth' \
    > "${parity_dir}/operator/conf/rest-server.properties"
if ! sides_agree "${parity_dir}/operator" >/dev/null 2>&1; then
    echo "operator class tree: check_auth_sides refused" >&2
    exit 1
fi
( cd "${parity_dir}/operator" && unset AUTHENTICATOR_CLASS && ./bin/enable-auth.sh )
if [[ "$(rest_class "${parity_dir}/operator")" != "com.example.OperatorAuth" ]]; then
    echo "operator class must survive the default write: got [$(rest_class "${parity_dir}/operator")]" >&2
    exit 1
fi
if ! sides_agree "${parity_dir}/operator"; then
    echo "operator class tree lost parity" >&2
    exit 1
fi

# ── The Gremlin guard and check_auth_sides have to read the same key ────
# enable-auth.sh skips the yaml append when the file already carries a
# top-level authentication mapping.  A grep that only knew the bare spelling
# called an operator's "authentication": block absent and appended a second
# one beside it, after which the two servers resolve the key in opposite
# directions while REST keeps the authenticator the operator named.
top_level_auth_keys() {
    local file="$1" sq="'"
    grep -Ec "^[\"${sq}]?authentication[\"${sq}]?[[:blank:]]*:" "${file}"
}

quoted_key_tree() {  # <dir> <with yamlscan.awk: yes|no>
    local dir="$1" with_scan="$2"
    bootstrap_tree "${dir}"
    [[ "${with_scan}" == "yes" ]] || rm -f "${dir}/yamlscan.awk"
    printf '%s\n' 'host: 0.0.0.0' '"authentication":' \
        '  authenticator: com.example.OperatorAuth' > "${dir}/conf/gremlin-server.yaml"
    printf '%s\n' 'restserver.url=http://127.0.0.1:8080' \
        'auth.authenticator=com.example.OperatorAuth' \
        > "${dir}/conf/rest-server.properties"
    if ! sides_agree "${dir}" >/dev/null 2>&1; then
        echo "quoted top-level key (${with_scan}): check_auth_sides refused" >&2
        exit 1
    fi
    ( cd "${dir}" && unset AUTHENTICATOR_CLASS && ./bin/enable-auth.sh ) || {
        echo "quoted top-level key (${with_scan}): enable-auth.sh failed" >&2
        exit 1
    }
    if [[ "$(top_level_auth_keys "${dir}/conf/gremlin-server.yaml")" != "1" ]]; then
        echo "quoted top-level key (${with_scan}): the append duplicated the" \
            "operator block, got $(top_level_auth_keys "${dir}/conf/gremlin-server.yaml")" >&2
        cat "${dir}/conf/gremlin-server.yaml" >&2
        exit 1
    fi
    if [[ "$(gremlin_state "${dir}")" != "named" ]]; then
        echo "quoted top-level key (${with_scan}): yaml is no longer named" >&2
        exit 1
    fi
    if [[ "$(rest_class "${dir}")" != "com.example.OperatorAuth" ]]; then
        echo "quoted top-level key (${with_scan}): REST lost the operator class" >&2
        exit 1
    fi
}

# The image layout, where yamlscan.awk sits in the install home, so the guard
# asks the same reader the entrypoint does.
quoted_key_tree "${parity_dir}/quoted-key" yes
# The plain release tarball carries no yamlscan.awk; the fallback has to keep
# the same answer for the question it can honestly settle on its own.
quoted_key_tree "${parity_dir}/quoted-key-tarball" no

# ── A value compared to a literal is the decoded value, not the bytes ───
# wait-partition.sh is skipped unless ACTUAL_BACKEND reads hstore.  The JVM
# resolves `backend=h\u0073tore` to hstore, so a reader that hands back the
# on-disk escaping does not see the backend that is actually running, and
# startup continues before the partitions are assigned.
escaped_backend="${test_dir}/backend-escape.properties"
# %s, not the format string: printf resolves \u0073 in a format itself and would
# write the decoded word, which is the very thing this case has to hand the
# reader.  The next assertion is the guard rail against that happening silently.
printf '%s\n' 'backend=h\u0073tore' > "${escaped_backend}"
if [[ "$(tr -d '\n' < "${escaped_backend}")" != 'backend=h\u0073tore' ]]; then
    echo "fixture must hold the escaped bytes on disk, got [$(cat "${escaped_backend}")]" >&2
    exit 1
fi
if [[ "$(get_prop_encoded backend "${escaped_backend}")" == "hstore" ]]; then
    echo "the encoded reader is expected to report the on-disk escaping" >&2
    exit 1
fi
if [[ "$(get_prop_decoded backend "${escaped_backend}")" != "hstore" ]]; then
    echo "decoded read of an escaped backend gave" \
         "[$(get_prop_decoded backend "${escaped_backend}")]" >&2
    exit 1
fi
# The ordinary spelling is unaffected, so this is not decode-instead-of-read.
plain_backend="${test_dir}/backend-plain.properties"
printf 'backend=hstore\n' > "${plain_backend}"
[[ "$(get_prop_decoded backend "${plain_backend}")" == "hstore" ]]
[[ "$(get_prop_encoded backend "${plain_backend}")" == "hstore" ]]
