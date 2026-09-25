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

# CI runs this harness under a backend matrix: server-ci.yml exports BACKEND for
# the rocksdb leg, which is the only leg that reaches these tests.  The
# entrypoint maps BACKEND to HG_SERVER_BACKEND and then overwrites whatever a
# fixture writes into hugegraph.properties, so a case that decides on the
# on-disk backend -- the escaped-hstore assertion below -- would be answered by
# the matrix value rather than by the file it is checking, and would fail in CI
# while passing locally.  Clear the inherited backend/pd environment so the
# harness is hermetic; cases that mean to drive the entrypoint from the
# environment set it on their own invocation (see the hstore mapping test).  The
# production precedence (environment beats file) is left exactly as it is.
unset BACKEND HG_SERVER_BACKEND PD_PEERS HG_SERVER_PD_PEERS

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TEST_HOME=$(mktemp -d "${TMPDIR:-/tmp}/hugegraph-entrypoint-test.XXXXXX")
trap 'rm -rf "${TEST_HOME}"' EXIT

mkdir -p "${TEST_HOME}/bin" "${TEST_HOME}/conf/graphs" "${TEST_HOME}/docker"
cp "${SCRIPT_DIR}/docker-entrypoint.sh" "${TEST_HOME}/docker-entrypoint.sh"
# props.awk is packaged in the release bin/; the image gets it from there, and
# the entrypoint accepts it beside itself so this harness can stage either.
cp "${SCRIPT_DIR}/../src/assembly/static/bin/props.awk" "${TEST_HOME}/props.awk"
cp "${SCRIPT_DIR}/yamlscan.awk" "${TEST_HOME}/yamlscan.awk"
touch "${TEST_HOME}/docker/init_complete"

cat > "${TEST_HOME}/conf/rest-server.properties" <<'EOF'
restserver.url=http://127.0.0.1:8080
# usePD=true
EOF
cat > "${TEST_HOME}/conf/graphs/hugegraph.properties" <<'EOF'
backend=rocksdb
#pd.peers=127.0.0.1:8686
EOF
cat > "${TEST_HOME}/bin/start-hugegraph.sh" <<'EOF'
#!/usr/bin/env bash
# One argument per line, so an assertion can read the exact -t value rather
# than substring-matching a flattened "$*", where -t 1200 contains -t 120.
printf '%s\n' "$@" > ./docker/start-hugegraph-argv
printf 'called\n' >> ./docker/start-hugegraph-calls
exit 0
EOF
cat > "${TEST_HOME}/bin/init-store.sh" <<'EOF'
#!/usr/bin/env bash
printf 'called\n' >> ./docker/init-store-calls
if IFS= read -r password; then
    printf '%s' "${password}" > ./docker/init-store-password
fi
EOF
cat > "${TEST_HOME}/bin/enable-auth.sh" <<'EOF'
#!/usr/bin/env bash
printf 'called\n' >> ./docker/enable-auth-calls
EOF
cat > "${TEST_HOME}/bin/wait-partition.sh" <<'EOF'
#!/usr/bin/env bash
printf 'called\n' >> ./docker/wait-partition-calls
exit 0
EOF
cat > "${TEST_HOME}/bin/wait-storage.sh" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
chmod +x "${TEST_HOME}/bin/"*.sh

(
    cd "${TEST_HOME}"
    HG_SERVER_BACKEND=hstore \
    HG_SERVER_PD_PEERS=pd:8686 \
    HG_SERVER_CLUSTER=hg \
    HG_SERVER_USE_PD=true \
    HG_SERVER_REST_URL=http://server:8080 \
    HG_SERVER_MIN_FREE_MEMORY=0 \
    HG_SERVER_AUTH_TOKEN_SECRET=12345678901234567890123456789012 \
        bash ./docker-entrypoint.sh
)
[[ "$(wc -l < "${TEST_HOME}/docker/init-store-calls")" -eq 1 ]]

grep -qx 'backend=hstore' "${TEST_HOME}/conf/graphs/hugegraph.properties"
grep -qx 'pd.peers=pd:8686' "${TEST_HOME}/conf/graphs/hugegraph.properties"
grep -qx 'usePD=true' "${TEST_HOME}/conf/rest-server.properties"
grep -qx 'pd.peers=pd:8686' "${TEST_HOME}/conf/rest-server.properties"
grep -qx 'cluster=hg' "${TEST_HOME}/conf/rest-server.properties"
grep -qx 'restserver.url=http://server:8080' \
    "${TEST_HOME}/conf/rest-server.properties"
grep -qx 'restserver.min_free_memory=0' \
    "${TEST_HOME}/conf/rest-server.properties"
grep -qx 'auth.token_secret=12345678901234567890123456789012' \
    "${TEST_HOME}/conf/rest-server.properties"
grep -qx 'auth.token_secret=12345678901234567890123456789012' \
    "${TEST_HOME}/conf/graphs/hugegraph.properties"

cp "${TEST_HOME}/conf/rest-server.properties" \
    "${TEST_HOME}/conf/rest-server.properties.before-short-secret"
cp "${TEST_HOME}/conf/graphs/hugegraph.properties" \
    "${TEST_HOME}/conf/graphs/hugegraph.properties.before-short-secret"
if (
    cd "${TEST_HOME}"
    PASSWORD=pa \
    HG_SERVER_AUTH_TOKEN_SECRET=1234567890123456789012345678901 \
        bash ./docker-entrypoint.sh
); then
    echo "short authentication token secret unexpectedly succeeded" >&2
    exit 1
fi
cmp "${TEST_HOME}/conf/rest-server.properties.before-short-secret" \
    "${TEST_HOME}/conf/rest-server.properties"
cmp "${TEST_HOME}/conf/graphs/hugegraph.properties.before-short-secret" \
    "${TEST_HOME}/conf/graphs/hugegraph.properties"
[[ ! -e "${TEST_HOME}/docker/enable-auth-calls" ]]

if (
    cd "${TEST_HOME}"
    PASSWORD=pa \
    HG_SERVER_REQUIRE_AUTH_TOKEN_SECRET=true \
        bash ./docker-entrypoint.sh
); then
    echo "required authentication token secret unexpectedly succeeded" >&2
    exit 1
fi
[[ "$(wc -l < "${TEST_HOME}/docker/init-store-calls")" -eq 1 ]]
[[ ! -e "${TEST_HOME}/docker/enable-auth-calls" ]]

(
    cd "${TEST_HOME}"
    HG_SERVER_REQUIRE_AUTH_TOKEN_SECRET=true \
        bash ./docker-entrypoint.sh
)
[[ "$(wc -l < "${TEST_HOME}/docker/init-store-calls")" -eq 2 ]]
[[ ! -e "${TEST_HOME}/docker/enable-auth-calls" ]]

(
    cd "${TEST_HOME}"
    PASSWORD=pa \
    HG_SERVER_REQUIRE_AUTH_TOKEN_SECRET=true \
    HG_SERVER_AUTH_TOKEN_SECRET=12345678901234567890123456789012 \
        bash ./docker-entrypoint.sh
)
[[ "$(wc -l < "${TEST_HOME}/docker/init-store-calls")" -eq 3 ]]
[[ "$(wc -l < "${TEST_HOME}/docker/enable-auth-calls")" -eq 1 ]]

sed -i '/^auth\.token_secret=/d' "${TEST_HOME}/conf/rest-server.properties"
sed -i '/^auth\.token_secret=/d' "${TEST_HOME}/conf/graphs/hugegraph.properties"
(
    cd "${TEST_HOME}"
    PASSWORD=pa bash ./docker-entrypoint.sh
)
rest_secret=$(sed -n 's/^auth\.token_secret=//p' \
    "${TEST_HOME}/conf/rest-server.properties")
graph_secret=$(sed -n 's/^auth\.token_secret=//p' \
    "${TEST_HOME}/conf/graphs/hugegraph.properties")
[[ ${#rest_secret} -ge 43 ]]
[[ "${rest_secret}" == "${graph_secret}" ]]
grep -qx 'auth.admin_pa=pa' "${TEST_HOME}/conf/rest-server.properties"
(
    cd "${TEST_HOME}"
    PASSWORD=pa bash ./docker-entrypoint.sh
)
reused_secret=$(sed -n 's/^auth\.token_secret=//p' \
    "${TEST_HOME}/conf/rest-server.properties")
[[ "${reused_secret}" == "${rest_secret}" ]]

sed -i '/^auth\.token_secret=/d' \
    "${TEST_HOME}/conf/graphs/hugegraph.properties"
sed -i "s|^auth\\.token_secret=.*|auth.token_secret:  ${rest_secret}|" \
    "${TEST_HOME}/conf/rest-server.properties"
(
    cd "${TEST_HOME}"
    PASSWORD=pa bash ./docker-entrypoint.sh
)
grep -qx "auth.token_secret=${rest_secret}" \
    "${TEST_HOME}/conf/graphs/hugegraph.properties"

sed -i '/^auth\.token_secret=/d' \
    "${TEST_HOME}/conf/rest-server.properties"
sed -i "s|^auth\\.token_secret=.*|auth.token_secret  ${rest_secret}|" \
    "${TEST_HOME}/conf/graphs/hugegraph.properties"
(
    cd "${TEST_HOME}"
    PASSWORD=pa bash ./docker-entrypoint.sh
)
grep -qx "auth.token_secret=${rest_secret}" \
    "${TEST_HOME}/conf/rest-server.properties"

[[ "$(wc -l < "${TEST_HOME}/docker/init-store-calls")" -eq 7 ]]
[[ "$(wc -l < "${TEST_HOME}/docker/enable-auth-calls")" -eq 5 ]]

(
    cd "${TEST_HOME}"
    PASSWORD=pa \
    HG_SERVER_AUTH_TOKEN_SECRET='Strong\Secret 9!0123456789abcdef' \
        bash ./docker-entrypoint.sh
)
complex_secret=$(sed -n 's/^auth\.token_secret=//p' \
    "${TEST_HOME}/conf/rest-server.properties")
(
    cd "${TEST_HOME}"
    PASSWORD=pa bash ./docker-entrypoint.sh
)
reused_complex_secret=$(sed -n 's/^auth\.token_secret=//p' \
    "${TEST_HOME}/conf/rest-server.properties")
[[ "${reused_complex_secret}" == "${complex_secret}" ]]
[[ "${reused_complex_secret}" == \
   'Strong\\Secret\u00209!0123456789abcdef' ]]

(
    cd "${TEST_HOME}"
    PASSWORD=pa \
    HG_SERVER_AUTH_TOKEN_SECRET='SecretEnds 0123456789abcdefABCDE ' \
        bash ./docker-entrypoint.sh
)
trailing_space_secret=$(sed -n 's/^auth\.token_secret=//p' \
    "${TEST_HOME}/conf/rest-server.properties")
(
    cd "${TEST_HOME}"
    PASSWORD=pa bash ./docker-entrypoint.sh
)
reused_trailing_space_secret=$(sed -n 's/^auth\.token_secret=//p' \
    "${TEST_HOME}/conf/rest-server.properties")
[[ "${trailing_space_secret}" == \
   'SecretEnds\u00200123456789abcdefABCDE\u0020' ]]
[[ "${reused_trailing_space_secret}" == "${trailing_space_secret}" ]]
grep -Fqx 'auth.admin_pa=pa' \
    "${TEST_HOME}/conf/rest-server.properties"
# The secret above ends in a space, and commons-configuration trims a physical
# line before it asks whether that line continues: the `\ ` the encoder used to
# write survives the trim as a lone trailing backslash, which pulls the property
# under it into the password -- that is how a file that plainly carried
# auth.admin_pa next to it would reach the server as one long secret.  \u0020
# leaves the trimmer nothing to take.
grep -Fqx 'auth.token_secret=SecretEnds\u00200123456789abcdefABCDE\u0020' \
    "${TEST_HOME}/conf/rest-server.properties" || {
    echo "a trailing space must not be written as a backslash-space" >&2
    sed -n 's/^auth\.token_secret=/written: [&]/p' \
        "${TEST_HOME}/conf/rest-server.properties" >&2
    exit 1
}
# Round trip: the secret comes back with both spaces it started with, and the
# property written under it is still its own property.
props_read() {
    PROPS_MODE=get PROPS_DECODED=1 PROPS_KEY="$1" \
        PROPS_FILE="${TEST_HOME}/conf/rest-server.properties" \
        awk -f "${TEST_HOME}/props.awk" /dev/null
}
[[ "$(props_read auth.token_secret)" == 'SecretEnds 0123456789abcdefABCDE ' ]] || {
    echo "auth.token_secret lost its spaces: [$(props_read auth.token_secret)]" >&2
    exit 1
}
[[ "$(props_read auth.admin_pa)" == "pa" ]] || {
    echo "auth.admin_pa is not its own property any more: [$(props_read auth.admin_pa)]" >&2
    exit 1
}
grep -Fqx 'auth.admin_pa=pa' \
    "${TEST_HOME}/conf/rest-server.properties"

(
    cd "${TEST_HOME}"
    PASSWORD='Strong\Pass 9!' bash ./docker-entrypoint.sh
)
grep -Fqx 'auth.admin_pa=Strong\\Pass\u00209!' \
    "${TEST_HOME}/conf/rest-server.properties"

rm -f "${TEST_HOME}/docker/init_complete"
(
    cd "${TEST_HOME}"
    PASSWORD=-n bash ./docker-entrypoint.sh
)
grep -Fqx -- '-n' "${TEST_HOME}/docker/init-store-password"

# The value start-hugegraph.sh actually received for -t, read from the
# recorded argument vector so that -t 1200 can never satisfy an assertion
# that wants 120.
last_start_timeout() {
    local previous="" argument
    while IFS= read -r argument; do
        if [[ "${previous}" == "-t" ]]; then
            printf '%s\n' "${argument}"
            return 0
        fi
        previous="${argument}"
    done < "${TEST_HOME}/docker/start-hugegraph-argv"
    return 1
}

# Spelled with an explicit exit rather than a bare [[ ]]: bash 3.2, still the
# /bin/bash of macOS, does not apply set -e to a failing [[ ]], so a bare
# assertion reports PASS there while CI catches the regression.
assert_start_timeout() {
    local expected="$1" actual
    if ! actual=$(last_start_timeout); then
        echo "start-hugegraph.sh received no -t argument" >&2
        exit 1
    fi
    if [[ "${actual}" != "${expected}" ]]; then
        echo "expected start-hugegraph.sh -t ${expected}, got -t ${actual}" >&2
        exit 1
    fi
}

# An absent variable keeps the historical default. env -u rather than a bare
# subshell: a child shell inherits an exported HG_SERVER_STARTUP_TIMEOUT_S, so
# without it this case would silently exercise whatever the developer exported.
(
    cd "${TEST_HOME}"
    env -u HG_SERVER_STARTUP_TIMEOUT_S bash ./docker-entrypoint.sh
)
assert_start_timeout 120

(
    cd "${TEST_HOME}"
    HG_SERVER_STARTUP_TIMEOUT_S=450 bash ./docker-entrypoint.sh
)
assert_start_timeout 450

(
    cd "${TEST_HOME}"
    HG_SERVER_STARTUP_TIMEOUT_S=86400 bash ./docker-entrypoint.sh
)
assert_start_timeout 86400

# An empty value is a set value, not an absent one: Compose writes it whenever
# an interpolated host variable is missing. 2m is the shape of a typo, and the
# two large values bracket the point where the deadline arithmetic in
# wait_for_startup would wrap negative and end the wait before its first probe.
for invalid_timeout in "" " " 0 +5 2m 86401 9223372036854775807; do
    start_calls_before_invalid=$(wc -l < "${TEST_HOME}/docker/start-hugegraph-calls")
    init_calls_before_invalid=$(wc -l < "${TEST_HOME}/docker/init-store-calls")
    if (
        cd "${TEST_HOME}"
        HG_SERVER_STARTUP_TIMEOUT_S="${invalid_timeout}" \
            bash ./docker-entrypoint.sh
    ); then
        echo "startup timeout '${invalid_timeout}' unexpectedly succeeded" >&2
        exit 1
    fi
    # The server must not have started, and the guard must have run ahead of
    # init-store, as the comment above it in the entrypoint claims.
    if [[ "$(wc -l < "${TEST_HOME}/docker/start-hugegraph-calls")" -ne \
          "${start_calls_before_invalid}" ]]; then
        echo "startup timeout '${invalid_timeout}' started the server" >&2
        exit 1
    fi
    if [[ "$(wc -l < "${TEST_HOME}/docker/init-store-calls")" -ne \
          "${init_calls_before_invalid}" ]]; then
        echo "startup timeout '${invalid_timeout}' was rejected only after" \
             "init-store ran" >&2
        exit 1
    fi
done

# Still the default once the rejected values are out of the way.
(
    cd "${TEST_HOME}"
    env -u HG_SERVER_STARTUP_TIMEOUT_S bash ./docker-entrypoint.sh
)
assert_start_timeout 120

# A mounted rest-server.properties that already carries auth.authenticator,
# with no matching yaml mapping and no PASSWORD given, used to start without a
# word: the parity check ran only inside the PASSWORD branch, so nothing ever
# compared the two sides and the server came up with REST enforcing and Gremlin
# on AllowAllAuthenticator.  The check now runs on every start, and a refusal
# has to come before anything touches the backend.
printf '%s\n' 'host: 8182' > "${TEST_HOME}/conf/gremlin-server.yaml"
grep -qx 'auth.authenticator=org.apache.hugegraph.auth.StandardAuthenticator' \
    "${TEST_HOME}/conf/rest-server.properties" ||
    printf '%s\n' \
        'auth.authenticator=org.apache.hugegraph.auth.StandardAuthenticator' \
        >> "${TEST_HOME}/conf/rest-server.properties"
rm -f "${TEST_HOME}/docker/init_complete"
before_calls="$(wc -l < "${TEST_HOME}/docker/init-store-calls")"
before_auth="$(wc -l < "${TEST_HOME}/docker/enable-auth-calls")"
status=0
(
    cd "${TEST_HOME}"
    bash ./docker-entrypoint.sh
) || status=$?
if (( status == 0 )); then
    echo "entrypoint must refuse a mounted REST-only authenticator with no PASSWORD" >&2
    exit 1
fi
if [[ "$(wc -l < "${TEST_HOME}/docker/init-store-calls")" != "${before_calls}" ]]; then
    echo "the refusal must happen before init-store runs" >&2
    exit 1
fi
if [[ "$(wc -l < "${TEST_HOME}/docker/enable-auth-calls")" != "${before_auth}" ]]; then
    echo "a refused start must not run enable-auth.sh" >&2
    exit 1
fi

# The same start is accepted once both sides agree, so the check above is a
# parity decision and not a blanket refusal to run without PASSWORD.
printf '%s\n' \
    'authentication: {' \
    '  authenticator: org.apache.hugegraph.auth.StandardAuthenticator,' \
    '  config: {tokens: conf/rest-server.properties}' \
    '}' > "${TEST_HOME}/conf/gremlin-server.yaml"
rm -f "${TEST_HOME}/docker/init_complete"
(
    cd "${TEST_HOME}"
    bash ./docker-entrypoint.sh
)

# ── The stabilization check follows the backend the JVM actually loaded ──
# ACTUAL_BACKEND is compared against a literal, so it has to be the decoded
# value.  A mounted hugegraph.properties may spell the word with a unicode
# escape for the s, which java.util.Properties hands the server as hstore;
# reading the on-disk escaping instead compared something else to hstore,
# skipped wait-partition.sh, and let startup continue before the partitions
# were assigned.  bs is the backslash, taken from its code point rather than
# written here: printf '%c' 92 hands back the digit 9, which would have built a
# fixture holding a different word than the one being decoded.
bs=$(awk 'BEGIN { printf "%c", 92 }')
if [[ "${#bs}" != 1 || "$(printf '%d' "'${bs}")" != 92 ]]; then
    echo "this host did not yield a backslash for code point 92" >&2
    exit 1
fi
touch "${TEST_HOME}/docker/init_complete"
rm -f "${TEST_HOME}/docker/wait-partition-calls"
printf '%s\n' "backend=h${bs}u0073tore" 'pd.peers=pd:8686' \
    > "${TEST_HOME}/conf/graphs/hugegraph.properties"
if [[ "$(head -n 1 "${TEST_HOME}/conf/graphs/hugegraph.properties")" != \
       "backend=h${bs}u0073tore" ]]; then
    echo "the fixture has to hold the escaped bytes, not the decoded word" >&2
    exit 1
fi
(
    cd "${TEST_HOME}"
    bash ./docker-entrypoint.sh
)
if [[ ! -s "${TEST_HOME}/docker/wait-partition-calls" ]]; then
    echo "an escaped hstore backend must still reach wait-partition.sh" >&2
    exit 1
fi
# The other half: this is a read that follows the server, not a switch that
# simply always waits.
rm -f "${TEST_HOME}/docker/wait-partition-calls"
printf '%s\n' 'backend=rocksdb' 'pd.peers=pd:8686' \
    > "${TEST_HOME}/conf/graphs/hugegraph.properties"
(
    cd "${TEST_HOME}"
    bash ./docker-entrypoint.sh
)
if [[ -e "${TEST_HOME}/docker/wait-partition-calls" ]]; then
    echo "wait-partition.sh ran for a rocksdb backend" >&2
    exit 1
fi

echo "PASS: Docker entrypoint configures HStore discovery and authentication"
