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
printf '%s' "${LC_ALL-}" > ./docker/init-store-locale
if IFS= read -r password; then
    printf '%s' "${password}" > ./docker/init-store-password
fi
EOF
cat > "${TEST_HOME}/bin/util.sh" <<'EOF'
# Configuration tests isolate readiness; process lifecycle is tested separately.
record_jvm_startup_diagnostics() { :; }
diagnose_jvm_startup_failure() { :; }
read_property() { sed -n "s/^$2=//p" "$1"; }
wait_for_startup() { return 0; }
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

# CI supplies current HugeConfig and the POM-pinned Commons dependency classpath.
if [[ -n "${HG_CONFIG_TEST_CLASSPATH:-}" ]]; then
    javac -cp "${HG_CONFIG_TEST_CLASSPATH}" -d "${TEST_HOME}/docker" \
        "${SCRIPT_DIR}/../../hugegraph-core/src/main/java/org/apache/hugegraph/config/AuthOptions.java" \
        "${SCRIPT_DIR}/test/AuthConfigReadback.java" \
        "${SCRIPT_DIR}/test/ConfigReadback.java"
else
    echo "SKIP: native auth and service readback (HG_CONFIG_TEST_CLASSPATH is unset)"
fi
assert_service_readback() {
    if [[ -n "${HG_CONFIG_TEST_CLASSPATH:-}" ]]; then
        java -cp "${TEST_HOME}/docker:${HG_CONFIG_TEST_CLASSPATH}" ConfigReadback \
            "${TEST_HOME}/conf/rest-server.properties" "$1"
    fi
}
assert_auth_readback() {
    if [[ -n "${HG_CONFIG_TEST_CLASSPATH:-}" ]]; then
        local secret_base64 admin_base64
        secret_base64=$(printf '%s' "$2" | base64 | tr -d '\n')
        if [[ $# -eq 3 ]]; then
            admin_base64=$(printf '%s' "$3" | base64 | tr -d '\n')
            java -Xmx128m -cp "${TEST_HOME}/docker:${HG_CONFIG_TEST_CLASSPATH}" AuthConfigReadback \
                "$1" "${secret_base64}" "${admin_base64}"
        else
            java -Xmx128m -cp "${TEST_HOME}/docker:${HG_CONFIG_TEST_CLASSPATH}" AuthConfigReadback \
                "$1" "${secret_base64}"
        fi
    fi
}

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

# Identity mappings apply on every start and leave mounted config alone when unset.
identity_conf="${TEST_HOME}/conf/rest-server.properties"
# Earlier secret-escaping fixtures leave escaped trailing whitespace in this file.
# Start identity cases with their own parseable configuration.
printf 'restserver.url=http://127.0.0.1:8080\nserver.service_id=mounted-service\nserver.graphspace=mounted_space\n' \
    > "${identity_conf}"
(cd "${TEST_HOME}"; bash ./docker-entrypoint.sh)
grep -qx 'server.service_id=mounted-service' "${identity_conf}"
grep -qx 'server.graphspace=mounted_space' "${identity_conf}"
(cd "${TEST_HOME}"; HG_SERVER_SERVICE_ID=DEFAULT HG_SERVER_GRAPHSPACE=DEFAULT \
    bash ./docker-entrypoint.sh)
grep -qx 'server.service_id=DEFAULT' "${identity_conf}"
grep -qx 'server.graphspace=DEFAULT' "${identity_conf}"
(cd "${TEST_HOME}"; HG_SERVER_SERVICE_ID=service-a HG_SERVER_GRAPHSPACE=space_a bash ./docker-entrypoint.sh)
grep -qx 'server.service_id=service-a' "${identity_conf}"
grep -qx 'server.graphspace=space_a' "${identity_conf}"
(cd "${TEST_HOME}"; SERVICE_ID=legacy-service GRAPH_SPACE=legacy_space bash ./docker-entrypoint.sh)
grep -qx 'server.service_id=legacy-service' "${identity_conf}"
grep -qx 'server.graphspace=legacy_space' "${identity_conf}"
(cd "${TEST_HOME}"; SERVICE_ID=legacy-service GRAPH_SPACE=legacy_space \
    HG_SERVER_SERVICE_ID=service-b HG_SERVER_GRAPHSPACE=space_b bash ./docker-entrypoint.sh)
grep -qx 'server.service_id=service-b' "${identity_conf}"
grep -qx 'server.graphspace=space_b' "${identity_conf}"
(cd "${TEST_HOME}"; HG_SERVER_SERVICE_ID= HG_SERVER_GRAPHSPACE= bash ./docker-entrypoint.sh)
grep -qx 'server.service_id=service-b' "${identity_conf}"
grep -qx 'server.graphspace=space_b' "${identity_conf}"

# Empty new variables still allow the legacy driver values to apply.
(cd "${TEST_HOME}"; HG_SERVER_SERVICE_ID= HG_SERVER_GRAPHSPACE= \
    SERVICE_ID=legacy-empty GRAPH_SPACE=legacy_empty bash ./docker-entrypoint.sh)
grep -qx 'server.service_id=legacy-empty' "${identity_conf}"
grep -qx 'server.graphspace=legacy_empty' "${identity_conf}"

# Service permits spaces; Unicode escapes round-trip through the Server parser.
(cd "${TEST_HOME}"; HG_SERVER_SERVICE_ID='ops team' bash ./docker-entrypoint.sh)
grep -Fxq 'server.service_id=ops\u0020team' "${identity_conf}"
assert_service_readback 'ops team'
(cd "${TEST_HOME}"; HG_SERVER_SERVICE_ID=' ops team ' bash ./docker-entrypoint.sh)
grep -Fxq 'server.service_id=\u0020ops\u0020team\u0020' "${identity_conf}"
assert_service_readback ' ops team '
(cd "${TEST_HOME}"; HG_SERVER_SERVICE_ID='ops\ team' bash ./docker-entrypoint.sh)
grep -Fxq 'server.service_id=ops\\\u0020team' "${identity_conf}"
assert_service_readback 'ops\ team'

(cd "${TEST_HOME}"; HG_SERVER_SERVICE_ID=' ops\ team ' bash ./docker-entrypoint.sh)
assert_service_readback ' ops\ team '
(cd "${TEST_HOME}"; HG_SERVER_SERVICE_ID='ops\u0020team' bash ./docker-entrypoint.sh)
assert_service_readback 'ops\u0020team'
(cd "${TEST_HOME}"; HG_SERVER_SERVICE_ID='x-$(POD_IP)-$$(POD_IP)-$name-$' bash ./docker-entrypoint.sh)
assert_service_readback 'x-$(POD_IP)-$$(POD_IP)-$name-$'

# Unsupported effective identity input must fail before changing mounted config.
cp "${identity_conf}" "${identity_conf}.before-invalid-identity"
for identity_name in HG_SERVER_SERVICE_ID HG_SERVER_GRAPHSPACE SERVICE_ID GRAPH_SPACE; do
    for identity_value in 'café' '😀' $'ops\tteam' $'ops\nteam'; do
        if (cd "${TEST_HOME}"; env "${identity_name}=${identity_value}" \
            bash ./docker-entrypoint.sh > ./docker/invalid-identity.log 2>&1); then
            echo "unsupported ${identity_name} unexpectedly succeeded" >&2
            exit 1
        fi
        grep -q 'must be printable ASCII' "${TEST_HOME}/docker/invalid-identity.log"
        cmp "${identity_conf}.before-invalid-identity" "${identity_conf}"
    done
done
# Ignored legacy input does not override a supported non-empty new value.
(cd "${TEST_HOME}"; HG_SERVER_SERVICE_ID=analytics SERVICE_ID='café' \
    HG_SERVER_GRAPHSPACE=team_a GRAPH_SPACE='😀' bash ./docker-entrypoint.sh)
grep -qx 'server.service_id=analytics' "${identity_conf}"
grep -qx 'server.graphspace=team_a' "${identity_conf}"

# Apply the existing graph-space naming rule to effective new and legacy input.
graph_space_max=$(printf 'a%.0s' {1..48})
cp "${identity_conf}" "${identity_conf}.before-invalid-graphspace"
for graphspace_env in HG_SERVER_GRAPHSPACE GRAPH_SPACE; do
    for graphspace in TeamA 1team team-a "${graph_space_max}a"; do
        if (cd "${TEST_HOME}"; env "${graphspace_env}=${graphspace}" \
            bash ./docker-entrypoint.sh > ./docker/invalid-graphspace.log 2>&1); then
            echo "invalid ${graphspace_env} unexpectedly succeeded" >&2
            exit 1
        fi
        grep -q 'HG_SERVER_GRAPHSPACE must be DEFAULT' "${TEST_HOME}/docker/invalid-graphspace.log"
        cmp "${identity_conf}.before-invalid-graphspace" "${identity_conf}"
    done
    for graphspace in DEFAULT team_a "${graph_space_max}"; do
        (cd "${TEST_HOME}"; env "${graphspace_env}=${graphspace}" bash ./docker-entrypoint.sh)
        grep -Fxq "server.graphspace=${graphspace}" "${identity_conf}"
    done
    cp "${identity_conf}" "${identity_conf}.before-invalid-graphspace"
done
(cd "${TEST_HOME}"; HG_SERVER_GRAPHSPACE=team_a GRAPH_SPACE=TeamA bash ./docker-entrypoint.sh)
grep -qx 'server.graphspace=team_a' "${identity_conf}"

# Some installed locales make [a-z] include uppercase letters; validation owns its locale.
for identity_locale in sk_SK.ISO8859-2 cs_CZ.ISO8859-2; do
    case "$(LC_ALL="${identity_locale}" locale charmap 2>/dev/null)" in
        ISO8859-2 | ISO-8859-2) ;;
        *) continue ;;
    esac
    cp "${identity_conf}" "${identity_conf}.before-invalid-locale"
    for graphspace_env in HG_SERVER_GRAPHSPACE GRAPH_SPACE; do
        if (cd "${TEST_HOME}"; env LC_ALL="${identity_locale}" "${graphspace_env}=TeamA" \
            bash ./docker-entrypoint.sh > ./docker/invalid-graphspace-locale.log 2>&1); then
            echo "invalid ${graphspace_env} under ${identity_locale} unexpectedly succeeded" >&2
            exit 1
        fi
        grep -q 'HG_SERVER_GRAPHSPACE must be DEFAULT' "${TEST_HOME}/docker/invalid-graphspace-locale.log"
        cmp "${identity_conf}.before-invalid-locale" "${identity_conf}"
    done
done

# Signing keys are opaque strings: both files must load exactly what the caller supplied.
fake_key='FAKE_SIGNING_KEY_0123456789abcdef'
control_soh=$(printf '\001')
control_us=$(printf '\037')
min_key="${fake_key:0:31}${control_soh}"
[[ ${#min_key} -eq 32 ]]
auth_secrets=("${fake_key}" " ${fake_key}" "${fake_key} inner" "${fake_key} "
    "${fake_key}"$'\t' "${fake_key}\\" "${fake_key}café" "${fake_key}中文" "${fake_key}😀"
    "${fake_key}\\u0020😀 café " "${fake_key}"$'\377'
    "${control_soh}${fake_key}" "${fake_key}${control_soh}" "${fake_key}"$'\v' "${fake_key}${control_us}"
    "${min_key}")
fake_admin_password="${control_soh}"' fake\ admin '"${control_us}"
for auth_secret in "${auth_secrets[@]}"; do
    cp "${SCRIPT_DIR}/../src/assembly/static/conf/rest-server.properties" \
        "${TEST_HOME}/conf/rest-server.properties"
    (cd "${TEST_HOME}"; PASSWORD="${fake_admin_password}" HG_SERVER_AUTH_TOKEN_SECRET="${auth_secret}" \
        bash ./docker-entrypoint.sh)
    expected_secret="${auth_secret}"
    if [[ "${auth_secret}" == "${fake_key}"$'\377' ]]; then
        expected_secret="${fake_key}ÿ"
    fi
    assert_auth_readback "${TEST_HOME}/conf/rest-server.properties" "${expected_secret}" "${fake_admin_password}"
    assert_auth_readback "${TEST_HOME}/conf/graphs/hugegraph.properties" "${expected_secret}"
    cp "${TEST_HOME}/conf/rest-server.properties" "${TEST_HOME}/conf/rest-server.properties.before-reuse"
    cp "${TEST_HOME}/conf/graphs/hugegraph.properties" "${TEST_HOME}/conf/graphs/hugegraph.properties.before-reuse"
    (cd "${TEST_HOME}"; PASSWORD="${fake_admin_password}" bash ./docker-entrypoint.sh)
    cmp "${TEST_HOME}/conf/rest-server.properties.before-reuse" "${TEST_HOME}/conf/rest-server.properties"
    cmp "${TEST_HOME}/conf/graphs/hugegraph.properties.before-reuse" "${TEST_HOME}/conf/graphs/hugegraph.properties"
    assert_auth_readback "${TEST_HOME}/conf/rest-server.properties" "${expected_secret}" "${fake_admin_password}"
done

(cd "${TEST_HOME}"; LC_ALL=en_US.UTF-8 PASSWORD='locale probe' \
    HG_SERVER_AUTH_TOKEN_SECRET="${fake_key}" bash ./docker-entrypoint.sh)
[[ "$(cat "${TEST_HOME}/docker/init-store-locale")" == 'en_US.UTF-8' ]]

# Unset discovery variables leave the shipped defaults alone.
if grep -qE '^server\.(urls_to_pd|deploy_in_k8s)' \
    "${TEST_HOME}/conf/rest-server.properties"; then
    echo "unset discovery variables wrote server.* keys" >&2
    exit 1
fi

(
    cd "${TEST_HOME}"
    HG_SERVER_URLS_TO_PD=' http://server-0:8080, http://server-1:8080 ' \
    HG_SERVER_DEPLOY_IN_K8S=' TRUE ' \
        bash ./docker-entrypoint.sh
)
grep -qx 'server.urls_to_pd=http://server-0:8080,http://server-1:8080' \
    "${TEST_HOME}/conf/rest-server.properties"
grep -qx 'server.deploy_in_k8s=true' "${TEST_HOME}/conf/rest-server.properties"

# A restart with new values replaces each key instead of appending another.
(
    cd "${TEST_HOME}"
    HG_SERVER_URLS_TO_PD=http://server-2:8080 \
    HG_SERVER_DEPLOY_IN_K8S=off \
        bash ./docker-entrypoint.sh
)
[[ "$(grep -c '^server\.urls_to_pd' "${TEST_HOME}/conf/rest-server.properties")" -eq 1 ]]
[[ "$(grep -c '^server\.deploy_in_k8s' "${TEST_HOME}/conf/rest-server.properties")" -eq 1 ]]
grep -qx 'server.urls_to_pd=http://server-2:8080' \
    "${TEST_HOME}/conf/rest-server.properties"
grep -qx 'server.deploy_in_k8s=off' "${TEST_HOME}/conf/rest-server.properties"

# HugeConfig rejects 0 and 1 for a boolean, so the entrypoint refuses them
# before writing either conf file, the URL list included. The graph conf holds
# backend=hstore by now, so a different backend makes its cmp able to fail.
cp "${TEST_HOME}/conf/rest-server.properties" \
    "${TEST_HOME}/conf/rest-server.properties.before-k8s-flag"
cp "${TEST_HOME}/conf/graphs/hugegraph.properties" \
    "${TEST_HOME}/conf/graphs/hugegraph.properties.before-k8s-flag"
for invalid_k8s_flag in 1 0 enabled "t rue" "tr ue"; do
    if (
        cd "${TEST_HOME}"
        HG_SERVER_BACKEND=rocksdb \
        HG_SERVER_URLS_TO_PD=http://server-3:8080 \
        HG_SERVER_DEPLOY_IN_K8S="${invalid_k8s_flag}" \
            bash ./docker-entrypoint.sh
    ); then
        echo "HG_SERVER_DEPLOY_IN_K8S '${invalid_k8s_flag}' unexpectedly succeeded" >&2
        exit 1
    fi
    cmp "${TEST_HOME}/conf/rest-server.properties.before-k8s-flag" \
        "${TEST_HOME}/conf/rest-server.properties"
    cmp "${TEST_HOME}/conf/graphs/hugegraph.properties.before-k8s-flag" \
        "${TEST_HOME}/conf/graphs/hugegraph.properties"
done

echo "PASS: Docker entrypoint configures HStore discovery and authentication"
bash "${SCRIPT_DIR}/test/test-docker-entrypoint-process.sh"
