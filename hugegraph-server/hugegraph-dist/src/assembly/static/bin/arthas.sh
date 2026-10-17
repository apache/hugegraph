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
# Offline attachment to a running Server or Store using the bundled agent.
set -euo pipefail

fail() { echo "Error: $*" >&2; exit 1; }
usage() {
    echo "Usage: $0 [--pid PID] [--telnet-port PORT] [--target-ip ADDRESS] [--command COMMAND]"
    echo "          [--username USER] [--password PASSWORD]"
    echo "Run as the service OS user; defaults to bin/pid and loopback port 3658."
    echo "New agents disable HTTP/MCP; use the existing telnet port for an active agent."
    echo "Exiting this helper stops Arthas, including after --command or quit; the JVM keeps running."
}

BIN=$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TOP=$(cd -P "$BIN/.." && pwd)
pid=""
port=3658
target_ip=127.0.0.1
username=""
password=""
command=""
boot_args=()
while [ "$#" -gt 0 ]; do
    case "$1" in
        --pid|--telnet-port|--target-ip|--command|--username|--password)
            [ "$#" -ge 2 ] || fail "Missing value for $1"
            case "$1" in
                --pid) pid=$2 ;;
                --telnet-port) port=$2 ;;
                --target-ip) target_ip=$2 ;;
                --command) command=$2 ;;
                --username) username=$2 ;;
                --password) password=$2 ;;
            esac
            shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) usage >&2; fail "Unknown option: $1" ;;
    esac
done
# Native batch files contain one command per line.
for credential in "$username" "$password"; do
    [[ "$credential" != *$'\n'* && "$credential" != *$'\r'* ]] || fail "Credentials must be single-line values"
done
if [ -z "$pid" ]; then
    [ -r "$BIN/pid" ] || fail "No bin/pid; start the service or supply --pid PID"
    pid=$(cat "$BIN/pid")
fi
[[ "$pid" =~ ^[1-9][0-9]{0,9}$ ]] || fail "PID must be a positive integer"
[[ "$port" =~ ^[1-9][0-9]{0,4}$ ]] && [ "$port" -le 65535 ] || fail "Invalid telnet port"
[ -t 0 ] || [ -n "$command" ] || fail "Interactive use needs a terminal; use --command COMMAND"
case "$target_ip" in
    [Ll][Oo][Cc][Aa][Ll][Hh][Oo][Ss][Tt]) target_ip=127.0.0.1 ;;
esac
JAVA=${JAVA_HOME:+$JAVA_HOME/bin/}java
ARTHAS="$TOP/lib/arthas"
[ -f "$ARTHAS/arthas-boot.jar" ] || fail "Bundled Arthas files are missing from lib/arthas"
# Keep the listener within this diagnosis session. Verify its PID before stopping
# it, since an occupied port must never stop a different application's agent.
state_dir=$(mktemp -d)
client_pid=""
attaching=true
connect_ip=$target_ip
quote_argument() {
    local value=${1//\'/\'\\\'\'}
    printf "'%s'" "$value"
}
write_batch() {
    local file=$1
    local commands=$2
    {
        if [ -n "$password" ]; then
            printf 'auth --username %s %s\n' "$(quote_argument "${username:-arthas}")" "$(quote_argument "$password")"
        fi
        # Match arthas-client --command's semicolon-separated command syntax.
        printf '%s\n' "${commands//;/$'\n'}"
    } > "$file"
}
redact_auth_echo() {
    local line
    while IFS= read -r line || [ -n "$line" ]; do
        case "$line" in
            *'$ auth --username '*) printf '%s$ auth [credentials omitted]\n' "${line%%'$ auth --username '*}" ;;
            *) printf '%s\n' "$line" ;;
        esac
    done
}
# The native client's -t option does not bound its final response wait.
bounded_client() {
    write_batch "$state_dir/cleanup.commands" "$1"
    "$JAVA" -jar "$ARTHAS/arthas-client.jar" -t 5000 -f "$state_dir/cleanup.commands" "$connect_ip" "$port" &
    local command_pid=$!
    (sleep 10; kill -KILL "$command_pid" 2>/dev/null) >/dev/null 2>&1 &
    local timer_pid=$!
    local status=0
    wait "$command_pid" || status=$?
    kill -KILL "$timer_pid" 2>/dev/null || true
    wait "$timer_pid" 2>/dev/null || true
    return "$status"
}
format_endpoint() {
    case "$connect_ip" in
        *:*) printf '[%s]:%s' "$connect_ip" "$port" ;;
        *) printf '%s:%s' "$connect_ip" "$port" ;;
    esac
}
stop_agent() {
    local session attempt agent_pid="" line endpoint rows=0
    connect_ip=$(cat "$state_dir/connect-ip")
    endpoint=$(format_endpoint)
    if ! session=$(bounded_client session 2>&1); then
        if [[ "$session" == *"Connection refused"* ]]; then
            echo "Arthas listener closed at $endpoint" >&2
            return 0
        fi
        if [[ "$session" == *"not permitted"* ]]; then
            echo "Error: Arthas credentials rejected at $endpoint; agent left running" >&2
        else
            echo "Error: could not verify Arthas cleanup at $endpoint; agent left running" >&2
        fi
        return 1
    fi
    # AuthCommand echoes the username. Trust only one complete session table row.
    while IFS= read -r line; do
        if [[ "$line" =~ ^[[:space:]]*JAVA_PID[[:space:]]+([0-9]+)[[:space:]]*$ ]]; then
            agent_pid=${BASH_REMATCH[1]}
            rows=$((rows + 1))
        fi
    done <<< "$session"
    if [ "$rows" -ne 1 ]; then
        if [ "$rows" -eq 0 ] && [[ "$session" == *"not permitted"* ]]; then
            echo "Error: Arthas credentials rejected at $endpoint; agent left running" >&2
        else
            echo "Error: could not verify an unambiguous Arthas PID at $endpoint; agent left running" >&2
        fi
        return 1
    fi
    if [ "$agent_pid" != "$pid" ]; then
        echo "Error: Arthas at $endpoint belongs to PID $agent_pid, expected $pid; agent left running" >&2
        return 1
    fi
    bounded_client stop >/dev/null 2>&1 || true
    # Command errors can return zero; verify closure after asynchronous shutdown.
    for attempt in 1 2 3; do
        if ! session=$(bounded_client session 2>&1) && [[ "$session" == *"Connection refused"* ]]; then
            echo "Arthas listener closed at $endpoint" >&2
            return 0
        fi
        sleep 0.2
    done
    echo "Error: could not stop Arthas at $endpoint; check session/stop settings before retrying" >&2
    return 1
}
cleanup() {
    local status=$?
    trap '' INT TERM HUP
    trap - EXIT
    if [ -n "$client_pid" ]; then
        # Let an in-flight loadAgent finish before checking/closing its listener.
        if [ "$attaching" = false ]; then
            kill "$client_pid" 2>/dev/null || true
        fi
        # A queued second signal can interrupt wait even after traps are ignored.
        while kill -0 "$client_pid" 2>/dev/null; do
            wait "$client_pid" 2>/dev/null || true
        done
    fi
    if [ -f "$state_dir/validated" ]; then
        stop_agent || { [ "$status" -ne 0 ] || status=1; }
    fi
    rm -f "$state_dir/validated" "$state_dir/connect-ip" "$state_dir/cleanup.commands" "$state_dir/run.commands"
    rmdir "$state_dir"
    exit "$status"
}
trap cleanup EXIT
trap 'trap "" INT TERM HUP; exit 130' INT
trap 'trap "" INT TERM HUP; exit 143' TERM
trap 'trap "" INT TERM HUP; exit 129' HUP
echo "Waiting for PID $pid to answer the attach request..." >&2
(
    # Group signals must not end the client before target-side loadAgent finishes.
    trap '' INT TERM HUP
    exec "$JAVA" --add-modules jdk.attach --class-path "$ARTHAS/arthas-core.jar" \
        "$BIN/ArthasAttach.java" "$pid" "$ARTHAS" "$port" "$TOP" "$state_dir/validated" "$target_ip" \
        "$username" "$password"
) <&0 &
client_pid=$!
wait "$client_pid"
client_pid=""
attaching=false
connect_ip=$(cat "$state_dir/connect-ip")
if ! session=$(bounded_client session 2>&1) && [[ "$session" == *"Connection refused"* ]]; then
    echo "Error: no Arthas listener at $(format_endpoint);" \
         "an existing agent may use another port (API default 8562); retry with --telnet-port" >&2
    rm -f "$state_dir/validated"
    exit 1
fi
if [ -n "$command" ]; then
    if [ -n "$password" ]; then
        write_batch "$state_dir/run.commands" "$command"
        boot_args+=(--batch-file "$state_dir/run.commands")
    else
        boot_args+=(--command "$command")
    fi
fi
[ -z "$username" ] || boot_args+=(--username "$username")
[ -z "$password" ] || boot_args+=(--password "$password")
boot_args+=(--arthas-home "$ARTHAS" --target-ip "$connect_ip" --telnet-port "$port" --http-port -1 "$pid")
if [ -n "$password" ] && [ -n "$command" ]; then
    # The native batch client echoes commands, including its auth command.
    "$JAVA" -jar "$ARTHAS/arthas-boot.jar" "${boot_args[@]}" <&0 > >(redact_auth_echo) &
else
    "$JAVA" -jar "$ARTHAS/arthas-boot.jar" "${boot_args[@]}" <&0 &
fi
client_pid=$!
wait "$client_pid"
client_pid=""
