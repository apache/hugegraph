#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -Eeuo pipefail

DOCKER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PASSWORD="ci-compose-password"
SECRET="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
PD_SECRET="ci-compose-pd-secret"
VERSION="ci-version"
RENDER_HUBBLE_IMAGE="example.invalid/hugegraph/hubble:ci"
DATASOURCE="jdbc:h2:file:/hubble/data/hubble;DB_CLOSE_ON_EXIT=FALSE"
CURL_TIMEOUTS=(--connect-timeout 5 --max-time 15)
ACTIVE_PROJECT=""
ACTIVE_FILES=()
RENDER_DIR=""

compose_auth() {
    env HUGEGRAPH_VERSION="${VERSION}" \
        HUBBLE_IMAGE="${RENDER_HUBBLE_IMAGE}" \
        HUGEGRAPH_ADMIN_PASSWORD="${PASSWORD}" \
        HUGEGRAPH_AUTH_TOKEN_SECRET="${SECRET}" \
        HG_PD_AUTH_SECRET_KEY="${PD_SECRET}" \
        docker compose "$@"
}

render() {
    local output="$1"
    shift
    compose_auth "$@" config --format json > "${output}"
}

assert_file_property() {
    local file="$1"
    local property="$2"
    grep -Fqx "${property}" "${file}"
}

assert_common() {
    local rendered="$1"
    local services="$2"
    local volumes="$3"

    jq -e \
       --arg password "${PASSWORD}" \
       --arg secret "${SECRET}" \
       --argjson services "${services}" \
       --argjson volumes "${volumes}" '
        (.services | keys) == $services and
        (.volumes | keys) == $volumes and
        (.networks | keys) == ["hg-net"] and
        all(.networks[]; .external != true) and
        all(.volumes[]; .external != true) and
        all(.services[];
            (.networks | keys) == ["hg-net"] and
            .healthcheck.test[0] == "CMD-SHELL") and
        all(
            [.services | to_entries[] |
             select(.key | startswith("server")) |
             .value.environment][];
            .PASSWORD == $password and
            .HG_SERVER_AUTH_TOKEN_SECRET == $secret)
    ' "${rendered}" >/dev/null
}

assert_hubble() {
    local rendered="$1"
    local config="$2"
    shift 2
    local dependencies
    dependencies="$(printf '%s\n' "$@" | jq -Rsc 'split("\n")[:-1] | sort')"

    jq -e \
       --arg datasource "${DATASOURCE}" \
       --arg config "/docker/conf/hubble/${config}" \
       --argjson dependencies "${dependencies}" '
        .services.hubble.image == "example.invalid/hugegraph/hubble:ci" and
        .services.hubble.pull_policy == "missing" and
        .services.hubble.environment.SPRING_DATASOURCE_URL == $datasource and
        any(.services.hubble.ports[];
            .target == 8088 and .published == "8088" and
            .host_ip == "127.0.0.1") and
        (.services.hubble.depends_on | keys | sort) == $dependencies and
        all(.services.hubble.depends_on[];
            .condition == "service_healthy") and
        any(.services.hubble.volumes[];
            .type == "volume" and .source == "hubble-data" and
            .target == "/hubble/data") and
        any(.services.hubble.volumes[];
            .type == "bind" and (.source | endswith($config)) and
            .target == "/hubble/conf/hugegraph-hubble.properties" and
            .read_only == true) and
        (.services.hubble.healthcheck.test[1] |
            contains("http://localhost:8088/about") and
            contains("\"status\":200") and
            contains("\"name\":\"hugegraph-hubble\""))
    ' "${rendered}" >/dev/null
}

assert_standalone() {
    local rendered="$1"
    assert_common "${rendered}" \
                  '["hubble","server"]' \
                  '["hubble-data","server-data"]'
    assert_hubble "${rendered}" "standalone.properties" server
    jq -e '
        .services.server.image == "hugegraph/hugegraph:ci-version" and
        .services.server.pull_policy == "missing" and
        .services.server.healthcheck.test[1] ==
            "curl -fsS http://localhost:8080/versions >/dev/null" and
        any(.services.server.volumes[];
            .source == "server-data" and
            .target == "/hugegraph-server/rocksdb-data")
    ' "${rendered}" >/dev/null
    assert_file_property "${DOCKER_DIR}/conf/hubble/standalone.properties" \
                         "pd.enabled=false"
    assert_file_property "${DOCKER_DIR}/conf/hubble/standalone.properties" \
                         "server.direct_url=http://server:8080"
}

assert_hstore() {
    local rendered="$1"
    assert_common "${rendered}" \
                  '["hubble","pd","server","store"]' \
                  '["hubble-data","pd-data","store-data"]'
    assert_hubble "${rendered}" "hstore.local.properties" server
    jq -e '
        .services.pd.image == "hugegraph/pd:ci-version" and
        .services.store.image == "hugegraph/store:ci-version" and
        .services.server.image == "hugegraph/server:ci-version" and
        all([.services.pd, .services.store, .services.server][];
            .pull_policy == "missing") and
        .services.server.environment.HG_SERVER_BACKEND == "hstore" and
        .services.server.environment.HG_SERVER_PD_PEERS == "pd:8686" and
        .services.server.environment.HG_SERVER_CLUSTER == "hg" and
        .services.server.environment.HG_SERVER_USE_PD == "true" and
        .services.server.environment.HG_SERVER_REST_URL ==
            "http://server:8080" and
        .services.server.healthcheck.test[1] ==
            "curl -fsS http://server:8080/versions >/dev/null" and
        any(.services.pd.volumes[];
            .source == "pd-data" and
            .target == "/hugegraph-pd/pd_data") and
        any(.services.store.volumes[];
            .source == "store-data" and
            .target == "/hugegraph-store/storage")
    ' "${rendered}" >/dev/null
    assert_file_property "${DOCKER_DIR}/conf/hubble/hstore.local.properties" \
                         "pd.peers=pd:8686"
    assert_file_property "${DOCKER_DIR}/conf/hubble/hstore.local.properties" \
                         "operations.store.allowed_targets=[http://store:8520]"
}

assert_ha() {
    local rendered="$1"
    assert_common "${rendered}" \
        '["hubble","pd0","pd1","pd2","server0","server1","server2","store0","store1","store2"]' \
        '["hg-pd0-data","hg-pd1-data","hg-pd2-data","hg-store0-data","hg-store1-data","hg-store2-data","hubble-data"]'
    assert_hubble "${rendered}" "hstore-ha.local.properties" \
                  server0 server1 server2
    jq -e '
        all([.services.pd0, .services.pd1, .services.pd2][];
            .image == "hugegraph/pd:ci-version" and
            .pull_policy == "missing") and
        all([.services.store0, .services.store1, .services.store2][];
            .image == "hugegraph/store:ci-version" and
            .pull_policy == "missing") and
        all([.services.server0, .services.server1, .services.server2][];
            .image == "hugegraph/server:ci-version" and
            .pull_policy == "missing" and
            .environment.STORE_REST == "store0:8520" and
            .environment.HG_SERVER_BACKEND == "hstore" and
            .environment.HG_SERVER_PD_PEERS ==
                "pd0:8686,pd1:8686,pd2:8686" and
            .environment.HG_SERVER_CLUSTER == "hg" and
            .environment.HG_SERVER_USE_PD == "true" and
            .environment.HG_SERVER_INIT_STORE_ENABLED == "false" and
            .environment.HG_SERVER_REQUIRE_AUTH_TOKEN_SECRET == "true") and
        [.services.server0.environment.HG_SERVER_REST_URL,
         .services.server1.environment.HG_SERVER_REST_URL,
         .services.server2.environment.HG_SERVER_REST_URL] ==
            ["http://server0:8080",
             "http://server1:8080",
             "http://server2:8080"] and
        all([.services.server0, .services.server1, .services.server2][];
            .healthcheck.test[1] ==
                "curl -fsS http://$$(hostname):8080/versions >/dev/null" and
            .healthcheck.interval == "10s" and
            .healthcheck.timeout == "5s" and
            .healthcheck.retries == 30 and
            .healthcheck.start_period == "1m0s")
    ' "${rendered}" >/dev/null
    assert_file_property "${DOCKER_DIR}/conf/hubble/hstore-ha.local.properties" \
                         "pd.peers=pd0:8686,pd1:8686,pd2:8686"
    assert_file_property "${DOCKER_DIR}/conf/hubble/hstore-ha.local.properties" \
        "operations.store.allowed_targets=[http://store0:8520,http://store1:8520,http://store2:8520]"
}

assert_dev_override() {
    local rendered="$1"
    local override="$2"
    assert_common "${rendered}" \
                  '["hubble","pd","server","store"]' \
                  '["hubble-data","pd-data","store-data"]'
    jq -e '
        .services.pd.image == "hugegraph/pd:dev" and
        .services.store.image == "hugegraph/store:dev" and
        .services.server.image == "hugegraph/server:dev" and
        all([.services.pd, .services.store, .services.server][];
            .pull_policy == "build" and .build != null) and
        .services.hubble.image == "example.invalid/hugegraph/hubble:ci" and
        .services.hubble.build == null
    ' "${rendered}" >/dev/null
    jq -e '
        (.services | keys) == ["pd","server","store"] and
        (.networks | keys) == ["default"] and .volumes == null and
        all(.services[];
            .build != null and .image != null and
            .pull_policy == "build" and
            .environment == null and
            (.networks | keys) == ["default"] and
            .volumes == null)
    ' "${override}" >/dev/null
}

cleanup() {
    if [[ -n "${ACTIVE_PROJECT}" ]]; then
        compose_active down -v --remove-orphans >/dev/null 2>&1 || true
    fi
    restore_hubble_configs
    [[ -z "${RENDER_DIR}" ]] || rm -rf "${RENDER_DIR}"
}

# The HStore topologies mount conf/hubble/<name>.local.properties, which is
# generated and untracked. Generate both with the CI secret before any render
# or `up`; a missing file would make Docker create an empty directory at the
# bind path. A developer's own local files are put back afterwards.
HUBBLE_BACKUP_DIR=""
prepare_hubble_configs() {
    local name
    HUBBLE_BACKUP_DIR="$(mktemp -d)"
    for name in hstore hstore-ha; do
        local f="${DOCKER_DIR}/conf/hubble/${name}.local.properties"
        [[ ! -f "${f}" ]] || cp -p "${f}" "${HUBBLE_BACKUP_DIR}/${name}.local.properties"
        "${DOCKER_DIR}/set-hubble-pd-password.sh" "${name}" "${PD_SECRET}" >/dev/null
    done
}
restore_hubble_configs() {
    [[ -n "${HUBBLE_BACKUP_DIR}" ]] || return 0
    local name
    for name in hstore hstore-ha; do
        local f="${DOCKER_DIR}/conf/hubble/${name}.local.properties"
        if [[ -f "${HUBBLE_BACKUP_DIR}/${name}.local.properties" ]]; then
            cp -p "${HUBBLE_BACKUP_DIR}/${name}.local.properties" "${f}"
        else
            rm -f "${f}"
        fi
    done
    rm -rf "${HUBBLE_BACKUP_DIR}"
    HUBBLE_BACKUP_DIR=""
}

# set-hubble-pd-password.sh must survive the characters a sed replacement
# would mangle, keep the rest of the example, produce a world-readable file
# for the read-only mount, and refuse an empty secret or unknown topology.
hubble_password_helper_check() {
    local f="${DOCKER_DIR}/conf/hubble/hstore.local.properties"
    "${DOCKER_DIR}/set-hubble-pd-password.sh" hstore 'a&b#c\d' >/dev/null
    local line mode
    line=$(grep '^operations\.pd\.password=' "${f}")
    mode=$(stat -c '%a' "${f}" 2>/dev/null || stat -f '%Lp' "${f}")
    [[ "${line}" == 'operations.pd.password=a&b#c\\d' ]] || {
        echo "set-hubble-pd-password.sh mangled the secret: ${line}" >&2; exit 1; }
    [[ "${mode}" == "644" ]] || {
        echo "set-hubble-pd-password.sh wrote mode ${mode}, Hubble could not read it" >&2; exit 1; }
    grep -q '^pd.server=pd:8620$' "${f}" || {
        echo "set-hubble-pd-password.sh dropped the example's other properties" >&2; exit 1; }
    ! "${DOCKER_DIR}/set-hubble-pd-password.sh" hstore '' 2>/dev/null || {
        echo "set-hubble-pd-password.sh accepted an empty secret" >&2; exit 1; }
    ! "${DOCKER_DIR}/set-hubble-pd-password.sh" nope 'x' 2>/dev/null || {
        echo "set-hubble-pd-password.sh accepted an unknown topology" >&2; exit 1; }
    # put the CI value back for the render/smoke that follows
    "${DOCKER_DIR}/set-hubble-pd-password.sh" hstore "${PD_SECRET}" >/dev/null
}

run_render() {
    RENDER_DIR="$(mktemp -d)"
    trap cleanup EXIT INT TERM
    prepare_hubble_configs
    render "${RENDER_DIR}/standalone.json" \
           -f "${DOCKER_DIR}/docker-compose.yml"
    render "${RENDER_DIR}/hstore.json" \
           -f "${DOCKER_DIR}/docker-compose-hstore.yml"
    render "${RENDER_DIR}/ha.json" \
           -f "${DOCKER_DIR}/docker-compose-3pd-3store-3server.yml"
    render "${RENDER_DIR}/dev.json" \
           -f "${DOCKER_DIR}/docker-compose-hstore.yml" \
           -f "${DOCKER_DIR}/docker-compose.dev.yml"
    render "${RENDER_DIR}/override.json" \
           -f "${DOCKER_DIR}/docker-compose.dev.yml"

    assert_standalone "${RENDER_DIR}/standalone.json"
    assert_hstore "${RENDER_DIR}/hstore.json"
    assert_ha "${RENDER_DIR}/ha.json"
    assert_dev_override "${RENDER_DIR}/dev.json" \
                        "${RENDER_DIR}/override.json"
    hubble_password_helper_check
    echo "Compose render contracts passed"
}

compose_active() {
    env HUGEGRAPH_VERSION="${HUGEGRAPH_VERSION:-latest}" \
        HUBBLE_IMAGE="${HUBBLE_IMAGE:-hugegraph/hubble:latest}" \
        HUGEGRAPH_ADMIN_PASSWORD="${PASSWORD}" \
        HUGEGRAPH_AUTH_TOKEN_SECRET="${SECRET}" \
        HG_PD_AUTH_SECRET_KEY="${PD_SECRET}" \
        COMPOSE_PROGRESS=plain \
        docker compose -p "${ACTIVE_PROJECT}" "${ACTIVE_FILES[@]}" "$@"
}

diagnose() {
    compose_active ps || true
    compose_active logs --no-color --tail 200 || true
}

http_status() {
    curl "${CURL_TIMEOUTS[@]}" -sS -o /dev/null -w '%{http_code}' "$@"
}

wait_hubble_mode() {
    local expected_pd="$1"
    local expected_auth="$2"
    local response=""
    local _
    for _ in {1..30}; do
        response="$(curl "${CURL_TIMEOUTS[@]}" -fsS \
            http://localhost:8088/api/v1.3/config || true)"
        if jq -e --argjson expected_pd "${expected_pd}" \
                 --argjson expected_auth "${expected_auth}" '
            .status == 200 and
            .data.pd_enabled == $expected_pd and
            .data.auth_enabled == $expected_auth and
            .data.server_capabilities_verified == true
        ' <<<"${response}" >/dev/null 2>&1; then
            return
        fi
        sleep 2
    done
    echo "Hubble authentication detection failed: ${response}" >&2
    return 1
}

check_hubble_login() {
    local response
    response="$(curl "${CURL_TIMEOUTS[@]}" -fsS \
        -H "Content-Type: application/json" \
        --data "{\"user_name\":\"admin\",\"user_password\":\"${PASSWORD}\"}" \
        http://localhost:8088/api/v1.3/auth/login)"
    jq -e '.status == 200 and .data.user_name == "admin"' \
       <<<"${response}" >/dev/null
}

check_hubble_anonymous() {
    curl "${CURL_TIMEOUTS[@]}" -fsS \
        http://localhost:8088/api/v1.3/auth/status |
        jq -e '.status == 200 and .data.level == "ANONYMOUS"' >/dev/null
    curl "${CURL_TIMEOUTS[@]}" -fsS \
        http://localhost:8088/api/v1.3/auth/context |
        jq -e '
            .status == 200 and
            .data.mode == "NON_AUTH" and
            .data.role == "ANONYMOUS"
        ' >/dev/null
}

smoke() {
    local name="$1"
    local expected_pd="$2"
    local expected_auth="$3"
    shift 3
    ACTIVE_PROJECT="hg-ci-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-$$-${name}"
    ACTIVE_FILES=()
    while (($#)); do
        ACTIVE_FILES+=(-f "$1")
        shift
    done

    if ! compose_active up -d --wait --wait-timeout 600; then
        diagnose
        return 1
    fi
    curl "${CURL_TIMEOUTS[@]}" -fsS \
        http://localhost:8080/versions >/dev/null
    if [[ "${expected_auth}" == true ]]; then
        [[ "$(http_status \
            http://localhost:8080/graphspaces/DEFAULT/graphs)" == 401 ]]
        [[ "$(http_status -u "admin:${PASSWORD}" \
            http://localhost:8080/graphspaces/DEFAULT/graphs)" == 200 ]]
    else
        [[ "$(http_status \
            http://localhost:8080/graphspaces/DEFAULT/graphs)" == 200 ]]
    fi
    curl "${CURL_TIMEOUTS[@]}" -fsS http://localhost:8088/about |
        jq -e '.status == 200 and .data.name == "hugegraph-hubble"' >/dev/null
    wait_hubble_mode "${expected_pd}" "${expected_auth}"
    if [[ "${expected_auth}" == true ]]; then
        check_hubble_login
    else
        check_hubble_anonymous
    fi
    compose_active down -v --remove-orphans
    ACTIVE_PROJECT=""
    ACTIVE_FILES=()
    echo "Compose smoke passed: ${name}"
}

run_smoke() {
    trap cleanup EXIT INT TERM
    prepare_hubble_configs
    smoke standalone false true "${DOCKER_DIR}/docker-compose.yml"
    smoke hstore true true "${DOCKER_DIR}/docker-compose-hstore.yml"
}

run_smoke_auth_off() {
    PASSWORD=""
    trap cleanup EXIT INT TERM
    prepare_hubble_configs
    smoke standalone-anon false false "${DOCKER_DIR}/docker-compose.yml"
    smoke hstore-anon true false "${DOCKER_DIR}/docker-compose-hstore.yml"
}

case "${1:-}" in
    render)
        run_render
        ;;
    smoke)
        run_smoke
        ;;
    smoke-auth-off)
        run_smoke_auth_off
        ;;
    all)
        run_render
        run_smoke
        ;;
    *)
        echo "Usage: $0 {render|smoke|smoke-auth-off|all}" >&2
        exit 2
        ;;
esac
