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

function dump_service_diagnostics() {
    local service_dir="$1"
    local service_name="$2"
    local log_dir="${service_dir}/logs"

    echo "::group::${service_name} diagnostics"
    echo "[ci] service dir: ${service_dir}"
    echo "[ci] java processes:"
    ps -ef | grep -E "HugeGraph|hg-|java" | grep -v grep || true
    echo "[ci] listening tcp ports:"
    (ss -ltnp || netstat -ltnp || true) 2>&1

    if [ -d "${log_dir}" ]; then
        find "${log_dir}" -maxdepth 2 -type f | sort | while read -r log_file; do
            echo "--- tail -n 200 ${log_file} ---"
            tail -n 200 "${log_file}" || true
        done
    else
        echo "[ci] log dir not found: ${log_dir}"
    fi
    echo "::endgroup::"
}

function wait_for_tcp_port() {
    local service_name="$1"
    local host="$2"
    local port="$3"
    local pid_file="$4"
    local service_dir="$5"
    local timeout_seconds="${6:-90}"

    echo "[ci] waiting for ${service_name} at ${host}:${port}"
    for second in $(seq 1 "${timeout_seconds}"); do
        if bash -c "echo > /dev/tcp/${host}/${port}" >/dev/null 2>&1; then
            echo "[ci] ${service_name} is listening on ${host}:${port}"
            return 0
        fi

        if [ -f "${pid_file}" ]; then
            local pid
            pid="$(cat "${pid_file}")"
            if [ -n "${pid}" ] && ! kill -0 "${pid}" >/dev/null 2>&1; then
                echo "[ci] ${service_name} process ${pid} exited before readiness"
                dump_service_diagnostics "${service_dir}" "${service_name}"
                return 1
            fi
        fi

        if [ "$((second % 10))" -eq 0 ]; then
            echo "[ci] still waiting for ${service_name} (${second}s)"
        fi
        sleep 1
    done

    echo "[ci] timeout waiting for ${service_name} at ${host}:${port}"
    dump_service_diagnostics "${service_dir}" "${service_name}"
    return 1
}

function http_status_is_accepted() {
    local status="$1"
    local accepted_statuses="$2"

    case ",${accepted_statuses}," in
        *",${status},"*)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

function wait_for_http_status() {
    local service_name="$1"
    local url="$2"
    local pid_file="$3"
    local service_dir="$4"
    local timeout_seconds="${5:-90}"
    local accepted_statuses="${6:-200}"
    local connect_timeout_seconds=2
    local max_request_seconds=5
    local started_at="${SECONDS}"
    local deadline=$((started_at + timeout_seconds))
    local next_log_at=10

    echo "[ci] waiting for ${service_name} HTTP readiness at ${url}"
    echo "[ci] accepted HTTP statuses: ${accepted_statuses}"
    while (( SECONDS < deadline )); do
        local remaining=$((deadline - SECONDS))
        local request_timeout="${max_request_seconds}"
        if (( remaining < request_timeout )); then
            request_timeout="${remaining}"
        fi
        if (( request_timeout < 1 )); then
            break
        fi

        local status
        status="$(curl -s -o /dev/null -w "%{http_code}" \
                  --connect-timeout "${connect_timeout_seconds}" \
                  --max-time "${request_timeout}" \
                  "${url}" 2>/dev/null)" || status="000"
        if http_status_is_accepted "${status}" "${accepted_statuses}"; then
            echo "[ci] ${service_name} is HTTP ready at ${url}" \
                 "(status ${status})"
            return 0
        fi

        if [ -f "${pid_file}" ]; then
            local pid
            pid="$(cat "${pid_file}")"
            if [ -n "${pid}" ] && ! kill -0 "${pid}" >/dev/null 2>&1; then
                echo "[ci] ${service_name} process ${pid} exited before" \
                     "HTTP readiness"
                dump_service_diagnostics "${service_dir}" "${service_name}"
                return 1
            fi
        fi

        local elapsed=$((SECONDS - started_at))
        if (( elapsed >= next_log_at )); then
            echo "[ci] still waiting for ${service_name} HTTP readiness" \
                 "(${elapsed}s, last status ${status})"
            next_log_at=$((next_log_at + 10))
        fi
        if (( SECONDS >= deadline )); then
            break
        fi
        sleep 1
    done

    echo "[ci] timeout waiting for ${service_name} HTTP readiness at ${url}"
    dump_service_diagnostics "${service_dir}" "${service_name}"
    return 1
}

function process_is_running() {
    local pid="$1"
    local state

    if [[ ! "${pid}" =~ ^[0-9]+$ ]]; then
        return 1
    fi

    if ! kill -0 "${pid}" 2>/dev/null; then
        return 1
    fi

    state="$(ps -o stat= -p "${pid}" 2>/dev/null | tr -d '[:space:]')" ||
        state=""
    [[ "${state}" != Z* ]]
}

function wait_for_process_exit() {
    local pid="$1"
    local timeout_seconds="${2:-10}"
    local deadline=$((SECONDS + timeout_seconds))

    while process_is_running "${pid}"; do
        if (( SECONDS >= deadline )); then
            return 1
        fi
        sleep 1
    done
    return 0
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    command="$1"
    shift || true
    case "${command}" in
        dump)
            dump_service_diagnostics "$@"
            exit $?
            ;;
        wait)
            wait_for_tcp_port "$@"
            exit $?
            ;;
        wait-http)
            wait_for_http_status "$@"
            exit $?
            ;;
        *)
            echo "Usage: $0 dump SERVICE_DIR SERVICE_NAME"
            echo "       $0 wait SERVICE_NAME HOST PORT PID_FILE SERVICE_DIR [TIMEOUT_SECONDS]"
            echo "       $0 wait-http SERVICE_NAME URL PID_FILE SERVICE_DIR" \
                 "[TIMEOUT_SECONDS] [ACCEPTED_STATUSES]"
            exit 2
            ;;
    esac
fi
