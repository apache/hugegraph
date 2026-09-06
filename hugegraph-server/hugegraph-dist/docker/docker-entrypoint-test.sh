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

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TEST_HOME=$(mktemp -d "${TMPDIR:-/tmp}/hugegraph-entrypoint-test.XXXXXX")
trap 'rm -rf "${TEST_HOME}"' EXIT

mkdir -p "${TEST_HOME}/bin" "${TEST_HOME}/conf/graphs" "${TEST_HOME}/docker"
cp "${SCRIPT_DIR}/docker-entrypoint.sh" "${TEST_HOME}/docker-entrypoint.sh"
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
   'Strong\\Secret\ 9!0123456789abcdef' ]]

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
   'SecretEnds\ 0123456789abcdefABCDE\ ' ]]
[[ "${reused_trailing_space_secret}" == "${trailing_space_secret}" ]]
grep -Fqx 'auth.admin_pa=pa' \
    "${TEST_HOME}/conf/rest-server.properties"

(
    cd "${TEST_HOME}"
    PASSWORD='Strong\Pass 9!' bash ./docker-entrypoint.sh
)
grep -Fqx 'auth.admin_pa=Strong\\Pass\ 9!' \
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

echo "PASS: Docker entrypoint configures HStore discovery and authentication"
