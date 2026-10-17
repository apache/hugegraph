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

function record_jvm_startup_diagnostics() {
    JVM_STARTUP_TIME=$(date -u +'%Y-%m-%d %H:%M:%S.%N UTC')
    JVM_STARTUP_LOG_OFFSET=$(stat -c %s ./logs/hugegraph-server.log 2>/dev/null || echo 0)
}

function print_jvm_thread_hint() {
    local kind
    kind=$(awk '
        /pthread_create.*\(EPERM\)|pthread_create.*Operation not permitted/ { denied = 1 }
        /[Tt]hread.*Operation not permitted/ { denied = 1 }
        /Cannot create worker GC thread/ { resources = 1 }
        END { if (denied) print "denied"; else if (resources) print "resources" }
    ')
    case "$kind" in
        denied)
            echo 'ERROR: JVM thread creation was denied (EPERM). Check the Docker host seccomp profile and security policy.' >&2
            echo 'To diagnose seccomp restrictions, retry with --security-opt seccomp=unconfined; use a compatible host/runtime profile for normal operation.' >&2
            ;;
        resources)
            echo 'ERROR: JVM could not create a GC worker thread. Possible causes include seccomp denial, insufficient memory, or process/thread limits.' >&2
            echo 'Look for pthread_create failed (EPERM) above in the container log; if present, check the host seccomp profile and security policy.' >&2
            echo 'To diagnose seccomp restrictions, retry with --security-opt seccomp=unconfined; use a compatible host/runtime profile for normal operation.' >&2
            echo 'If the process limit is too low, set --ulimit nproc=65535:65535.' >&2
            ;;
        *) return 1 ;;
    esac
}

function diagnose_jvm_startup_failure() {
    local crash size offset
    while IFS= read -r -d '' crash; do
        if [[ -r "$crash" ]] && print_jvm_thread_hint < "$crash"; then
            return 0
        fi
    done < <(find -H . ./logs -maxdepth 1 -type f -name 'hs_err_pid*.log' \
                 -newermt "$JVM_STARTUP_TIME" -print0 2>/dev/null)
    if [[ -r ./logs/hugegraph-server.log ]]; then
        size=$(stat -c %s ./logs/hugegraph-server.log 2>/dev/null || echo 0)
        offset=$JVM_STARTUP_LOG_OFFSET
        [[ "$size" -ge "$offset" ]] || offset=0
        tail -c +$((offset + 1)) ./logs/hugegraph-server.log | print_jvm_thread_hint || true
    fi
    return 0
}

function command_available() {
    local cmd=$1
    if command -v "$cmd" >/dev/null 2>&1; then
        return 0
    fi
    return 1
}

function configure_riscv64_libatomic() {
    local error_handler="${1:-}"
    if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "riscv64" ]]; then
        return 0
    fi

    if [[ "${LD_PRELOAD:-}" == *"libatomic.so.1"* ]]; then
        return 0
    fi

    local libatomic=""
    if command_available "ldconfig"; then
        libatomic=$(ldconfig -p 2>/dev/null | \
                    awk '/libatomic\.so\.1 .*=>/ && !path {path=$NF}
                         END {if (path) print path}')
    fi

    if [[ -z "$libatomic" ]]; then
        local candidate
        for candidate in /lib/riscv64-linux-gnu/libatomic.so.1 \
                         /usr/lib/riscv64-linux-gnu/libatomic.so.1 \
                         /lib64/lp64d/libatomic.so.1 \
                         /usr/lib64/lp64d/libatomic.so.1 \
                         /lib64/libatomic.so.1 \
                         /usr/lib64/libatomic.so.1; do
            if [[ -r "$candidate" ]]; then
                libatomic="$candidate"
                break
            fi
        done
    fi

    if [[ -z "$libatomic" ]]; then
        local message="RISC-V RocksDB requires libatomic.so.1; install libatomic1"
        if [[ -n "$error_handler" ]]; then
            "$error_handler" "$message"
        else
            echo "$message" >&2
        fi
        return 1
    fi

    export LD_PRELOAD="${LD_PRELOAD:+${LD_PRELOAD}:}${libatomic}"
}

# read a property from .properties file
function read_property() {
    # file path
    local file_name
    local property_name
    file_name=$1
    # replace "." to "\."
    property_name=$(echo "$2" | sed 's/\./\\\./g')
    cat "$file_name" | sed -n -e "s/^[ ]*//g;/^#/d;s/^$property_name=//p" | tail -1
}

function write_property() {
    local file=$1
    local key=$2
    local value=$3

    local os=$(uname)
    case $os in
        # Note: in mac os should use sed -i '' "xxx" to replace string,
        # otherwise prompt 'command c expects \ followed by text'.
        # See http://www.cnblogs.com/greedy-day/p/5952899.html
        Darwin) sed -i '' "s!$key=.*!$key=$value!g" "$file" ;;
        *) sed -i "s!$key=.*!$key=$value!g" "$file" ;;
    esac
}

function parse_yaml() {
    local file=$1
    local version=$2
    local module=$3

    cat "$file" | tr -d '\n {}'| awk -F',+|:' '''{
        pre="";
        for(i=1; i<=NF; ) {
            if(match($i, /version/)) {
                pre=$i;
                i+=1
            } else {
                result[pre"-"$i] = $(i+1);
                i+=2
            }
        }
    } END {for(e in result) {print e": "result[e]}}''' \
    | grep "$version-$module" | awk -F':' '{print $2}' | tr -d ' ' && echo
}

function process_num() {
    num=$(ps -ef | grep "$1" | grep -v grep | wc -l)
    return "$num"
}

function process_id() {
    pid=$(ps -ef | grep "$1" | grep -v grep | awk '{print $2}')
    return "$pid"
}

# Extract a validated TCP port from a configured server URL.
# Echoes the port on success.  Returns 1 when the value carries no usable port
# or is ambiguous, in which case the caller skips the preflight.
function parse_port_from_url() {
    local url="$1"

    # ServerOptions tolerates surrounding whitespace, so strip it first.
    url="${url#"${url%%[![:space:]]*}"}"
    url="${url%"${url##*[![:space:]]}"}"
    [[ -z "$url" ]] && return 1

    # The scheme is optional and case-insensitive.
    local scheme="" rest="$url"
    if [[ "$url" == *"://"* ]]; then
        scheme=$(echo "${url%%://*}" | tr '[:upper:]' '[:lower:]')
        rest="${url#*://}"
    fi

    # The authority ends at the first '/', '?' or '#'.
    local authority="${rest%%[/?#]*}"
    [[ -z "$authority" ]] && return 1

    # Drop any userinfo prefix; its colon would otherwise look like an
    # unbracketed IPv6 separator.
    authority="${authority##*@}"
    [[ -z "$authority" ]] && return 1

    local port=""
    if [[ "$authority" =~ ^\[[^]]*\](:([0-9]+))?$ ]]; then
        # Bracketed IPv6, with or without a port: [::1] or [::1]:8080
        port="${BASH_REMATCH[2]}"
    elif [[ "$authority" == *:*:* ]]; then
        # Unbracketed IPv6 is ambiguous: in "::1:8080" the trailing group may
        # be a port or another hextet.  Refuse to guess.
        # TODO(check_port): no preflight runs at all for this form.  If
        # ServerOptions ever guarantees a normalized bracketed value here, this
        # branch can resolve the port instead of skipping the check.
        echo "WARN: ambiguous IPv6 authority '$authority' in server URL;" \
             "use bracket notation such as [::1]:8080." >&2
        return 1
    elif [[ "$authority" == *:* ]]; then
        port="${authority##*:}"
    fi

    # Fall back to the scheme's default port.
    # TODO(check_port): a scheme-less value with no explicit port (e.g. plain
    # "127.0.0.1") has no derivable port, so it is skipped rather than guessed.
    # Reading the configured default from ServerOptions would close this gap.
    if [[ -z "$port" ]]; then
        case "$scheme" in
            http)  port="80" ;;
            https) port="443" ;;
            *)     return 1 ;;
        esac
    fi

    [[ "$port" =~ ^[0-9]+$ ]] || return 1
    # Normalise leading-zero forms textually; Java reads 08080 as decimal 8080.
    # Arithmetic conversion must not happen before the value is bounded: Bash
    # evaluates in 64-bit and wraps silently, so 18446744073709559616 would
    # otherwise pass the range check as port 8000.
    port="${port#"${port%%[!0]*}"}"
    [[ -z "$port" ]] && return 1
    (( ${#port} <= 5 )) || return 1
    (( port >= 1 && port <= 65535 )) || return 1

    echo "$port"
}

# Echo "busy", "free" or "unknown" for the given TCP port.
#
# The port is compared as text against the listener table, so the argument must
# already be normalised - parse_port_from_url() strips leading zeroes for this
# reason, and "08080" passed directly here would not match a listener on 8080.
#
# Detection is deliberately port-only, matching the conservative behaviour of
# the `lsof -i :PORT` call this replaces.  Reproducing kernel socket semantics
# in Bash - dual-stack IPV6_V6ONLY, wildcard versus specific binds, address
# canonicalisation - produced more wrong answers than it prevented, so we only
# ask "is anything already listening on this port?".
#
# Only LISTEN rows and only the local-address column are inspected, so an
# unrelated outbound connection to the same port number is never mistaken for
# a local listener.  A tool that is missing, fails, or yields no recognisable
# listener row reports "unknown" rather than "free", and the next probe is
# tried; the sole exception is `ss -H` succeeding with no output at all, which
# says positively that the host has no TCP listeners.
#
# TODO(check_port): port-only matching ignores the listener's address, so a
# listener bound to one local address (127.0.0.1:8080) reports the port busy
# even when the server would bind a different one (192.168.1.5:8080).  This is
# deliberate - it is what `lsof -i :PORT` did, and it fails safe - but it can
# refuse a bind that would have succeeded.  If that is reported in practice,
# revisit by comparing the local-address column instead of only its port.
#
# TODO(check_port): only the `ss` branch can tell "no listeners at all" from
# "table unreadable", because -H removes the header and leaves a zero exit with
# empty output as a positive signal.  Both netstat branches print headers that
# the parser drops, so an empty result there stays "unknown" and warns.
function port_listen_state() {
    local port="$1"
    local out
    local state
    local os
    os=$(uname)

    # $4 is the local address for both `ss -ltn` and BSD `netstat -an`.
    # Splitting on the last separator keeps IPv6 hextets (for example
    # [2001:db8::80]:443) from being misread as the port.
    local parser='
        NF >= 4 && (!want_listen || $NF == "LISTEN") {
            addr = $4
            cut = 0
            for (k = length(addr); k > 0; k--) {
                if (substr(addr, k, 1) == sep) { cut = k; break }
            }
            if (cut == 0) next
            rows++
            if (substr(addr, cut + 1) == port) { found = 1; exit }
        }
        END {
            if (found) print "busy"
            else if (rows > 0) print "free"
            else print "unknown"
        }'

    # Each probe answers only when it produced a usable listener table; an
    # inconclusive one falls through to the next rather than ending the search.
    if [[ "$os" == "Darwin" || "$os" == *BSD* ]]; then
        if command_available "netstat" && out=$(netstat -an -p tcp 2>/dev/null) \
           && [[ -n "$out" ]]; then
            state=$(echo "$out" | awk -v port="$port" -v sep="." -v want_listen=1 "$parser")
            case "$state" in
                busy|free) echo "$state"; return 0 ;;
            esac
        fi
    else
        # `ss -H -ltn` already restricts output to listening sockets, and -H
        # drops the header, so a zero exit with no output means "nothing is
        # listening" - the one case where empty is an answer, not a failure.
        if command_available "ss" && out=$(ss -H -ltn 2>/dev/null); then
            if [[ -z "$out" ]]; then
                echo "free"
                return 0
            fi
            state=$(echo "$out" | awk -v port="$port" -v sep=":" -v want_listen=0 "$parser")
            case "$state" in
                busy|free) echo "$state"; return 0 ;;
            esac
        fi
        if command_available "netstat" && out=$(netstat -ltn 2>/dev/null) \
           && [[ -n "$out" ]]; then
            state=$(echo "$out" | awk -v port="$port" -v sep=":" -v want_listen=1 "$parser")
            case "$state" in
                busy|free) echo "$state"; return 0 ;;
            esac
        fi
    fi

    # TODO(check_port): with neither ss nor netstat present there is no probe
    # left, so the preflight is permanently "unknown" and never detects a busy
    # port.  The published server images carry iproute2 for this reason, but a
    # hand-built minimal image can still ship neither.  A dependency-free
    # fallback would need a bounded connect, which was deliberately removed
    # here; adding one back means re-solving the hang this PR fixes.
    echo "unknown"
}

# Best-effort startup preflight.  Exits 1 when the configured port is already
# in use.  The server's own bind stays authoritative, so an inconclusive
# result only warns and lets startup proceed.
function check_port() {
    local url="$1"
    local port
    local state

    port=$(parse_port_from_url "$url") || return 0

    state=$(port_listen_state "$port")
    case "$state" in
        busy)
            echo "The port $port has already been used"
            exit 1
            ;;
        unknown)
            echo "WARN: could not determine whether port $port is free;" \
                 "continuing and letting the server bind decide." >&2
            ;;
    esac

    return 0
}

function crontab_append() {
    local job="$1"
    crontab -l | grep -F "$job" >/dev/null 2>&1
    if [ $? -eq 0 ]; then
        return 1
    fi
    (crontab -l ; echo "$job") | crontab -
}

function crontab_remove() {
    local job="$1"
    # check exist before remove
    crontab -l | grep -F "$job" >/dev/null 2>&1
    if [ $? -eq 1 ]; then
        return 0
    fi

    crontab -l | grep -Fv "$job"  | crontab -

    # Check exist after remove
    crontab -l | grep -F "$job" >/dev/null 2>&1
    if [ $? -eq 0 ]; then
        return 1
    else
        return 0
    fi
}

# wait_for_startup friendly_name host port timeout_s
function wait_for_startup() {
    local pid="$1"
    local server_name="$2"
    local server_url="$3"
    local timeout_s="$4"

    local now_s
    now_s=$(date '+%s')
    local stop_s=$((now_s + timeout_s))

    local status
    local error_file_name="startup_error.txt"

    echo -n "Connecting to $server_name ($server_url)"
    while [ "$now_s" -lt "$stop_s" ]; do
        echo -n .
        process_status "$server_name" "$pid" >/dev/null
        if [ $? -eq 1 ]; then
            echo "Starting $server_name failed"
            if [ -e "$error_file_name" ]; then
                rm "$error_file_name"
            fi
            return 1
        fi

        # Bound each probe by the time left in the overall deadline: without
        # --max-time a single blackholed request blocks past ${timeout_s}s.
        # Refresh the clock after the process check and immediately before the
        # probe.  A stale loop-entry time must not grant curl a new one-second
        # budget when the overall deadline has already been reached.
        now_s=$(date '+%s')
        local remain_s=$((stop_s - now_s))
        [ "$remain_s" -le 0 ] && break
        local connect_s=$((remain_s < 5 ? remain_s : 5))
        status=$(curl -I -sS -k --connect-timeout "$connect_s" --max-time "$remain_s" \
                      -w "%{http_code}" -o /dev/null "$server_url" 2> "$error_file_name")
        if [[ "$status" -eq 200 || "$status" -eq 401 ]]; then
            echo "OK"
            echo "Started [pid $pid]"
            if [ -e "$error_file_name" ]; then
                rm "$error_file_name"
            fi
            return 0
        fi

        # Pause for the retry interval, but never past the deadline: sleeping a
        # fixed 2s and only then re-reading the clock made a 1s timeout take 2s.
        # Nothing left to wait for means the deadline is spent, so stop here
        # rather than spin until the clock ticks past it.
        now_s=$(date '+%s')
        local sleep_s=$((stop_s - now_s))
        [ "$sleep_s" -le 0 ] && break
        [ "$sleep_s" -gt 2 ] && sleep_s=2
        sleep "$sleep_s"
        now_s=$(date '+%s')
    done

    echo ""
    if [ -e "$error_file_name" ]; then
        cat "$error_file_name"
        rm "$error_file_name"
    fi
    echo "The operation timed out(${timeout_s}s) when attempting to connect to $server_url" >&2
    return 1
}

# Return the remaining memory (MiB) under visible cgroup hard limits. Keep
# missing/unlimited controllers empty so ordinary host sizing stays unchanged.
function cgroup_free_memory() {
    local root="${1:-/sys/fs/cgroup}"
    local membership="${2:-/proc/self/cgroup}"
    local mountinfo="${3:-/proc/self/mountinfo}"
    local base="" path="" limit_file="" usage_file=""
    local version=""
    local id controllers member
    if [[ -r "$membership" ]]; then
        while IFS=: read -r id controllers member; do
            if [[ ",$controllers," == *,memory,* ]]; then
                base="$root/memory"
                path="$member"
                limit_file="memory.limit_in_bytes"
                usage_file="memory.usage_in_bytes"
                version="cgroup"
                break
            elif [[ "$id" == 0 && -z "$controllers" ]]; then
                base="$root"
                path="$member"
                limit_file="memory.max"
                usage_file="memory.current"
                version="cgroup2"
            fi
        done < "$membership"
    fi
    [[ -n "$base" ]] || return 0

    # Mount points and exposed roots need not use the conventional hierarchy.
    # The kernel escapes whitespace/backslashes in mountinfo path fields.
    local mount_root="" mountpoint="" mapped_path=""
    local fallback_base="" selected_root="" selected_path=""
    if [[ -r "$mountinfo" ]]; then
        while read -r mount_root mountpoint; do
            printf -v mountpoint '%b' "$mountpoint"
            printf -v mount_root '%b' "$mount_root"
            if [[ -z "$fallback_base" ]]; then
                fallback_base="$mountpoint"
            fi
            if [[ "$mount_root" == / ]]; then
                mapped_path="$path"
            elif [[ "$path" == "$mount_root" ]]; then
                mapped_path=/
            elif [[ "$path" == "$mount_root/"* ]]; then
                mapped_path="${path#"$mount_root"}"
            else
                continue
            fi
            [[ -d "$mountpoint$mapped_path" ]] || continue
            # Prefer the widest visible hierarchy so a delegated bind mount
            # cannot hide a tighter ancestor exposed by another matching mount.
            [[ -z "$selected_root" || ${#mount_root} -lt ${#selected_root} ]] || continue
            base="$mountpoint"
            selected_root="$mount_root"
            selected_path="$mapped_path"
        done < <(awk -v version="$version" '
            {
                split($0, parts, " - ");
                split(parts[2], tail, " ");
                if (tail[1] == version && (version == "cgroup2" ||
                    index("," tail[3] ",", ",memory,") > 0)) {
                    print $4, $5;
                }
            }' "$mountinfo")
        # Only use a namespace root after all membership mappings were tried.
        if [[ -n "$selected_root" ]]; then
            path="$selected_path"
        elif [[ -n "$fallback_base" ]]; then
            base="$fallback_base"
            path=/
        fi
    fi

    # A cgroup namespace can expose its own root while /proc still names a
    # host-relative membership. Fall back to the visible mount root in that
    # case; never follow a membership path out of the controller mount.
    local dir="$base"
    if [[ "$path" == /* && "$path" != *"/../"* && "$path" != */.. &&
          -d "$base$path" ]]; then
        dir="$base$path"
        dir="${dir%/}"
    fi
    local limit usage inactive available smallest=""
    local child child_min
    local cache_key=inactive_file
    local unlimited="" kernel_machine="" page_size=""
    if [[ "$version" == cgroup ]]; then
        cache_key=total_inactive_file
        # v1 exposes PAGE_COUNTER_MAX * PAGE_SIZE instead of "max".
        # LONG_BIT describes userspace, not the kernel. Personality can also
        # mask uname as 32-bit, so infer unlimited only from known 64-bit names.
        kernel_machine=$(uname -m 2>/dev/null) || true
        page_size=$(getconf PAGESIZE 2>/dev/null) || true
        if [[ "$page_size" =~ ^[1-9][0-9]*$ ]]; then
            case "$kernel_machine" in
                x86_64|aarch64|riscv64)
                    unlimited=$((9223372036854775807 / page_size * page_size)) ;;
            esac
        fi
    fi
    while :; do
        limit=""
        usage=""
        [[ ! -r "$dir/$limit_file" ]] || read -r limit < "$dir/$limit_file" || true
        if [[ -n "$unlimited" && "$limit" =~ ^[0-9]+$ ]] &&
                awk -v limit="$limit" -v unlimited="$unlimited" 'BEGIN {exit !(limit >= unlimited)}'; then
            limit=""
        fi
        if [[ "$limit" =~ ^[0-9]+$ ]]; then
            # Unknown usage under a known limit must not fall back to host memory.
            if [[ ! -r "$dir/$usage_file" ]] || ! read -r usage < "$dir/$usage_file" ||
                    [[ ! "$usage" =~ ^[0-9]+$ ]]; then
                echo 0
                return 0
            fi
            # Inactive file cache can be reclaimed on a warm restart. For v1,
            # use the hierarchical total so ancestor budgets include children.
            inactive=0
            if [[ -r "$dir/memory.stat" ]]; then
                inactive=$(awk -v key="$cache_key" '$1 == key {print $2; exit}' "$dir/memory.stat")
            fi
            [[ "$inactive" =~ ^[0-9]+$ ]] || inactive=0
            if [[ "$version" == cgroup2 && "$inactive" != 0 ]]; then
                # A child's protected cache cannot be reclaimed by this limit.
                # The target's own memory.min does not protect against its own reclaim.
                if [[ ! -r "$dir" || ! -x "$dir" ]]; then
                    # Known files can be readable while child enumeration is denied.
                    inactive=0
                else
                    for child in "$dir"/* "$dir"/.[!.]* "$dir"/..?*; do
                        [[ -d "$child" ]] || continue
                        # An unsearchable directory can hide a protected cgroup.
                        if [[ ! -x "$child" ]]; then
                            inactive=0
                            break
                        fi
                        [[ -e "$child/memory.current" && -e "$child/memory.max" ]] || continue
                        child_min=""
                        [[ ! -r "$child/memory.min" ]] || read -r child_min < "$child/memory.min" || true
                        if [[ "$child_min" != 0 ]]; then
                            inactive=0
                            break
                        fi
                    done
                fi
            fi
            # Inconsistent snapshots receive no cache credit. awk also avoids
            # shell integer overflow for v1's unlimited sentinel.
            available=$(awk -v limit="$limit" -v usage="$usage" -v inactive="$inactive" 'BEGIN {
                if (inactive > usage) inactive = 0;
                remaining = limit - (usage - inactive);
                printf "%.0f\n", (remaining > 0 ? int(remaining / 1048576) : 0);
            }')
            if [[ -z "$smallest" ]] || awk -v a="$available" -v b="$smallest" \
                    'BEGIN {exit !(a < b)}'; then
                smallest="$available"
            fi
        fi
        [[ "$dir" != "$base" && "$dir" == "$base/"* ]] || break
        dir="${dir%/*}"
    done
    [[ -z "$smallest" ]] || echo "$smallest"
    return 0
}

function free_memory() {
    local free=""
    local os=$(uname)
    if [ "$os" == "Linux" ]; then
        local mem_free=$(cat /proc/meminfo | grep -w "MemFree" | awk '{print $2}')
        local mem_buffer=$(cat /proc/meminfo | grep -w "Buffers" | awk '{print $2}')
        local mem_cached=$(cat /proc/meminfo | grep -w "Cached" | awk '{print $2}')
        if [[ "$mem_free" == "" || "$mem_buffer" == "" || "$mem_cached" == "" ]]; then
            echo "Failed to get free memory"
            exit 1
        fi
        free=$(expr "$mem_free" + "$mem_buffer" + "$mem_cached")
        free=$(expr "$free" / 1024)
        local cgroup_free=$(cgroup_free_memory)
        if [[ -n "$cgroup_free" ]] && awk -v a="$cgroup_free" -v b="$free" \
                'BEGIN {exit !(a < b)}'; then
            free="$cgroup_free"
        fi
    elif [ "$os" == "Darwin" ]; then
        local pages_free=$(vm_stat | awk '/Pages free/{print $0}' | awk -F'[:.]+' '{print $2}' | tr -d " ")
        local pages_inactive=$(vm_stat | awk '/Pages inactive/{print $0}' | awk -F'[:.]+' '{print $2}' | tr -d " ")
        local pages_available=$(expr "$pages_free" + "$pages_inactive")
        free=$(expr "$pages_available" \* 4096 / 1024 / 1024)
    else
        echo "Unsupported operating system $os"
        exit 1
    fi
    echo "$free"
}

function calc_xmx() {
    local min_mem=$1
    local max_mem=$2
    # Get machine available memory
    local free=$(free_memory)
    local half_free=$((free/2))

    local xmx=$min_mem
    if [[ "$free" -lt "$min_mem" ]]; then
        exit 1
    elif [[ "$half_free" -ge "$max_mem" ]]; then
        xmx=$max_mem
    elif [[ "$half_free" -lt "$min_mem" ]]; then
        xmx=$min_mem
    else
        xmx=$half_free
    fi
    echo $xmx
}

function remove_with_prompt() {
    local path=$1
    local tips=""

    if [ -d "$path" ]; then
        tips="Remove directory '$path' and all sub files [y/n]?"
    elif [ -f "$path" ]; then
        tips="Remove file '$path' [y/n]?"
    else
        return 0
    fi

    read -p "$tips " yn
    case $yn in
        [Yy]* ) rm -rf "$path";;
        * ) ;;
    esac
}

function ensure_path_writable() {
    local path=$1
    local error_handler="${2:-}"
    # Ensure input path exist
    if [ ! -d "${path}" ]; then
        mkdir -p "${path}"
    fi
    # Check for write permission
    if [ ! -w "${path}" ]; then
        local message="No write permission on directory ${path}"
        if [ -n "${error_handler}" ]; then
            "${error_handler}" "${message}"
        else
            echo "${message}"
        fi
        exit 1
    fi
}

function get_ip() {
    local os=$(uname)
    local loopback="127.0.0.1"
    local ip=""
    case $os in
        Linux)
            if command_available "ifconfig"; then
                ip=$(ifconfig | grep 'inet addr:' | grep -v "$loopback" | cut -d: -f2 | awk '{ print $1}')
            elif command_available "ip"; then
                ip=$(ip addr | grep 'state UP' -A2 | tail -n1 | awk '{print $2}' | awk -F"/" '{print $1}')
            else
                ip=$loopback
            fi
            ;;
        FreeBSD|OpenBSD|Darwin)
            if command_available "ifconfig"; then
                ip=$(ifconfig | grep -E 'inet.[0-9]' | grep -v "$loopback" | awk '{ print $2}')
            else
                ip=$loopback
            fi
            ;;
        SunOS)
            if command_available "ifconfig"; then
                ip=$(ifconfig -a | grep inet | grep -v "$loopback" | awk '{ print $2} ')
            else
                ip=$loopback
            fi
            ;;
        *) ip=$loopback;;
    esac
    echo $ip
}

function download() {
    local path=$1
    local download_url=$2

    if [ ! -d "$path" ]; then
        mkdir -p "$path" || {
            echo "Failed to create directory: $path"
            exit 1
        }
    fi

    # Strip query/fragment so the on-disk name matches the server-side artifact.
    local filename tmp
    filename=$(basename "${download_url%%[?#]*}")
    local dest="${path}/${filename}"
    # mktemp, not $$: a PID is shared by concurrent background subshells.
    tmp=$(mktemp -- "${path}/.${filename}.XXXXXX") || {
        echo "Failed to create a temporary file in $path"
        return 1
    }

    if command_available "curl"; then
        # -o must appear before -- so it is parsed as an option, not an extra URL.
        if curl -fL -o "$tmp" -- "${download_url}"; then
            mv -f -- "$tmp" "$dest" || { rm -f -- "$tmp"; return 1; }
        else
            rm -f -- "$tmp"
            return 1
        fi
    elif command_available "wget"; then
        local -a progress_opt=()
        if wget --help 2>&1 | grep -q -- '--show-progress'; then
            progress_opt=(-q --show-progress)
        fi
        if wget ${progress_opt[@]+"${progress_opt[@]}"} -O "$tmp" -- "${download_url}"; then
            mv -f -- "$tmp" "$dest" || { rm -f -- "$tmp"; return 1; }
        else
            rm -f -- "$tmp"
            return 1
        fi
    else
        echo "Required curl or wget but they are unavailable"
        exit 1
    fi
}

function ensure_package_exist() {
    local path=$1
    local dir=$2
    local tar=$3
    local link=$4

    if [ ! -d "${path}/${dir}" ]; then
        if [ ! -f "${path}/${tar}" ]; then
            echo "Downloading the compressed package '${tar}'"
            if ! download "${path}" "${link}"; then
                echo "Failed to download, please ensure the network is available and link is valid"
                exit 1
            fi
            echo "[OK] Finished download"
        fi
        echo "Unzip the compressed package '$tar'"
        if ! tar -zxvf "${path}/${tar}" -C "${path}" >/dev/null 2>&1; then
            echo "Failed to unzip, please check the compressed package"
            exit 1
        fi
        echo "[OK] Finished unzip"
    fi
}

###########################################################################

# TODO(wait_for_shutdown): this loop has the same fixed-2s-then-check-the-clock
# shape wait_for_startup had, so it can also overrun its timeout by roughly one
# sleep interval.  Left alone here to keep this PR to the port preflight; the
# fix is the same remaining-deadline cap used above.
function wait_for_shutdown() {
    local process_name="$1"
    local pid="$2"
    local timeout_s="$3"

    local now_s=$(date '+%s')
    local stop_s=$((now_s + timeout_s))

    echo -n "Killing $process_name(pid $pid)" >&2
    while [ "$now_s" -le $stop_s ]; do
        echo -n .
        process_status "$process_name" "$pid" >/dev/null
        if [ $? -eq 1 ]; then
            echo "OK"
            return 0
        fi
        sleep 2
        now_s=$(date '+%s')
    done
    echo "$process_name shutdown timeout(exceeded $timeout_s seconds)" >&2
    return 1
}

function process_status() {
    local process_name="$1"
    local pid="$2"

    ps -p "$pid"
    if [ $? -eq 0 ]; then
        echo "$process_name is running with pid $pid"
        return 0
    else
        echo "The process $process_name does not exist"
        return 1
    fi
}

function kill_process() {
    local process_name="$1"
    local pid="$2"

    if [ -z "$pid" ]; then
        echo "The process $pid does not exist"
        return 0
    fi

    case "$(uname)" in
        CYGWIN*) taskkill /F /PID "$pid" ;;
        *)       kill "$pid" ;;
    esac
}

function kill_process_and_wait() {
    local process_name="$1"
    local pid="$2"
    local timeout_s="$3"

    kill_process "$process_name" "$pid"
    wait_for_shutdown "$process_name" "$pid" "$timeout_s"
}

function exit_with_usage_help(){
    echo "USAGE: $0 [-d true|false] [-g g1|G1|zgc|ZGC] [-m true|false] [-p true|false]" \
         "[-s true|false] [-j java_options] [-t timeout] [-y true|false]"
    echo "Without -g, JVM/caller collector selection applies. -j applies when JAVA_OPTIONS is empty."
    echo "Default/G1 tuning precedes caller options, so caller tuning values win."
    echo "Do not combine -g with a different collector in JAVA_OPTIONS or -j."
    exit 1
}
