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

# Contract tests for the startup port preflight in bin/util.sh.
#
# The preflight is best effort: the server's own bind is authoritative.  These
# tests pin the three-state contract (busy / free / unknown) rather than the
# internals of any one probe.
#
# TODO(test-check-port): the Linux and BSD detection branches are both driven
# by mocked tool output, so on any one runner only the host's own branch is
# ever exercised against a real kernel.  The single real-listener case covers
# whichever OS the job runs on.  Closing this needs the suite to run on both
# a Linux and a macOS runner, which CI already does for the server job.

set -u

STATIC_DIR="${1:-hugegraph-server/hugegraph-dist/src/assembly/static}"
UTIL_SH="$STATIC_DIR/bin/util.sh"
STORE_UTIL_SH="${2:-}"

if [[ ! -f "$UTIL_SH" ]]; then
    if [[ $# -gt 0 ]]; then
        echo "ERROR: required util.sh not found at $UTIL_SH" >&2
        exit 1
    fi
    # With no argument this script may be discovered outside the repository by
    # an optional test runner.  CI always supplies STATIC_DIR explicitly, so a
    # bad workflow path takes the required failure branch above.
    echo "SKIP: default util.sh not found at $UTIL_SH"
    exit 0
fi

if [[ -n "$STORE_UTIL_SH" && ! -f "$STORE_UTIL_SH" ]]; then
    echo "ERROR: required Store util.sh not found at $STORE_UTIL_SH" >&2
    exit 1
fi

# shellcheck source=/dev/null
source "$UTIL_SH"

# Sections run in subshells so their command overrides stay isolated, which
# means results have to be tallied through a file rather than a variable.
RESULTS=$(mktemp)
# Clean the tally on any exit, not only the normal one.  Section 5 runs outside
# a subshell and has to extend this trap, so it restores this handler rather
# than clearing it.
trap 'rm -f "$RESULTS"' EXIT

pass() {
    echo "  PASS  $1"
    echo "P" >> "$RESULTS"
}

fail() {
    echo "  FAIL  $1"
    echo "        $2"
    echo "F" >> "$RESULTS"
}

expect() {
    # expect <name> <expected> <actual>
    if [[ "$2" == "$3" ]]; then
        pass "$1"
    else
        fail "$1" "expected '$2', got '$3'"
    fi
}

echo ""
echo "check_port contract tests ($UTIL_SH)"

# --------------------------------------------------------------------------
echo ""
echo "1. URL to port"
# "<url>|<expected>", where SKIP means "no usable port, preflight is skipped".
url_cases=(
    'http://127.0.0.1:8080|8080'
    'HTTP://127.0.0.1:8080|8080'
    'http://127.0.0.1|80'
    'https://127.0.0.1|443'
    '127.0.0.1:8080|8080'
    'http://127.0.0.1:8080/path:9090|8080'
    'http://127.0.0.1:8080?probe=x|8080'
    'http://127.0.0.1:8080#frag|8080'
    'http://user:pass@127.0.0.1:8080|8080'
    'http://user@127.0.0.1:8080|8080'
    'http://[::1]:8080|8080'
    'http://user:pass@[::1]:8080|8080'
    '[::1]:8080|8080'
    'https://[::1]|443'
    'http://127.0.0.1:08080|8080'
    '::1:8080|SKIP'
    'http://127.0.0.1:abc|SKIP'
    'http://127.0.0.1:0|SKIP'
    'http://127.0.0.1:70000|SKIP'
    '127.0.0.1|SKIP'
    # Oversized values must be rejected on their digits, not after a 64-bit
    # arithmetic conversion that would wrap them back into range.
    'http://127.0.0.1:18446744073709551617|SKIP'
    'http://127.0.0.1:18446744073709559616|SKIP'
    'http://127.0.0.1:0000000000000008080|8080'
    'http://127.0.0.1:00000|SKIP'
)
for case in "${url_cases[@]}"; do
    url="${case%|*}"
    want="${case##*|}"
    got=$(parse_port_from_url "  $url  " 2>/dev/null) || got="SKIP"
    expect "$url" "$want" "$got"
done

# --------------------------------------------------------------------------
echo ""
echo "2. Linux: ss busy / free / failure, and the netstat fallback"
(
    uname() { echo "Linux"; }
    command_available() { [[ "$1" == "ss" ]]; }
    WORK=$(mktemp -d)
    trap 'rm -rf "$WORK"' EXIT
    SS_ARGS_FILE="$WORK/ss-args"
    SS_OUT=""
    SS_RC=0
    SS_FILTER_LISTEN=0
    ss() {
        echo "$*" > "$SS_ARGS_FILE"
        [[ $# -eq 2 && "$1" == "-H" && "$2" == "-ltn" ]] || return 64
        if [[ "$SS_FILTER_LISTEN" -eq 1 ]]; then
            echo "$SS_OUT" | awk '$1 == "LISTEN"'
        else
            printf '%s' "$SS_OUT"
        fi
        return "$SS_RC"
    }

    SS_OUT='LISTEN 0 4096 0.0.0.0:8080 0.0.0.0:*'
    expect "listener on 8080 is busy" "busy" "$(port_listen_state 8080)"
    expect "ss receives the exact listener-table arguments" \
           "-H -ltn" "$(cat "$SS_ARGS_FILE")"

    # Model the filtering done by `ss -l`: an established connection may use
    # the target as its local ephemeral port, but it is not a listener.  The
    # mock refuses changed argv, so dropping -l makes this unknown, not free.
    SS_FILTER_LISTEN=1
    SS_OUT='ESTAB 0 0 127.0.0.1:8080 127.0.0.1:443'
    expect "Linux non-LISTEN local port is free" "free" "$(port_listen_state 8080)"
    SS_FILTER_LISTEN=0

    SS_OUT='LISTEN 0 4096 0.0.0.0:22 0.0.0.0:*'
    expect "no listener on 8080 is free" "free" "$(port_listen_state 8080)"

    # The port must come from the local-address field, not an IPv6 hextet.
    SS_OUT='LISTEN 0 128 [2001:db8:8080::1]:9090 [::]:*'
    expect "IPv6 hextet 8080 is not the port" "free" "$(port_listen_state 8080)"

    SS_OUT='LISTEN 0 128 [::]:8080 [::]:*'
    expect "IPv6 wildcard listener is busy" "busy" "$(port_listen_state 8080)"

    # A tool that runs but fails proves nothing.
    SS_OUT=''
    SS_RC=1
    expect "ss failure is unknown" "unknown" "$(port_listen_state 8080)"

    # `-H` prints no header, so a zero exit with no output is a positive "this
    # host has no TCP listeners" - the state a fresh container starts in.
    SS_RC=0
    SS_OUT=''
    expect "ss empty output is free" "free" "$(port_listen_state 8080)"

    # Success with an unusable table proves nothing.
    SS_OUT='some diagnostic banner'
    expect "ss unparseable output is unknown" "unknown" "$(port_listen_state 8080)"

    # ...and it must not end the search either: netstat may still have a
    # readable table.  Returning on the first probe left the port "unknown"
    # even when the fallback could have answered.
    command_available() { [[ "$1" == "ss" || "$1" == "netstat" ]]; }
    netstat() {
        echo 'Active Internet connections (only servers)'
        echo 'Proto Recv-Q Send-Q Local Address  Foreign Address  State'
        echo 'tcp        0      0 0.0.0.0:8080   0.0.0.0:*        LISTEN'
    }

    SS_OUT='some diagnostic banner'
    expect "unparseable ss falls back to netstat" "busy" "$(port_listen_state 8080)"
    expect "netstat fallback can also report free" "free" "$(port_listen_state 9999)"

    SS_RC=1
    SS_OUT=''
    expect "failed ss falls back to netstat" "busy" "$(port_listen_state 8080)"

)

# --------------------------------------------------------------------------
echo ""
echo "3. macOS/BSD: LISTEN versus ESTABLISHED"
(
    uname() { echo "Darwin"; }
    command_available() { [[ "$1" == "netstat" ]]; }
    NS_OUT=""
    netstat() { printf '%s' "$NS_OUT"; }

    NS_OUT='Active Internet connections (including servers)
Proto Recv-Q Send-Q  Local Address          Foreign Address        (state)
tcp4       0      0  *.8080                 *.*                    LISTEN'
    expect "BSD LISTEN on 8080 is busy" "busy" "$(port_listen_state 8080)"

    # An outbound connection to :443 is not a local listener on 443.  This is
    # the false positive that blocked startup on macOS.
    NS_OUT='Active Internet connections (including servers)
Proto Recv-Q Send-Q  Local Address          Foreign Address        (state)
tcp4       0      0  192.168.1.3.60320      44.195.16.138.443      ESTABLISHED
tcp4       0      0  *.22                   *.*                    LISTEN'
    expect "BSD ESTABLISHED peer :443 is free" "free" "$(port_listen_state 443)"

    # A socket lingering in TIME_WAIT holds the port in its *local* address but
    # is not a listener, so only the connection state can tell them apart.
    NS_OUT='Proto Recv-Q Send-Q  Local Address          Foreign Address        (state)
tcp4       0      0  127.0.0.1.8080         127.0.0.1.51000        TIME_WAIT
tcp4       0      0  *.22                   *.*                    LISTEN'
    expect "BSD TIME_WAIT on local 8080 is free" "free" "$(port_listen_state 8080)"

    NS_OUT='Proto Recv-Q Send-Q  Local Address          Foreign Address        (state)
tcp6       0      0  ::1.8080               *.*                    LISTEN'
    expect "BSD IPv6 LISTEN is busy" "busy" "$(port_listen_state 8080)"

    NS_OUT='Proto Recv-Q Send-Q  Local Address          Foreign Address        (state)'
    expect "BSD header only is unknown" "unknown" "$(port_listen_state 8080)"

)

# --------------------------------------------------------------------------
echo ""
echo "4. Unknown never blocks startup"
(
    uname() { echo "Linux"; }
    command_available() { return 1; }

    expect "no probe tool is unknown" "unknown" "$(port_listen_state 8080)"

    # check_port must warn and let the server perform the authoritative bind.
    err=$( (check_port "http://127.0.0.1:8080") 2>&1 >/dev/null )
    rc=$?
    expect "unknown exits 0" "0" "$rc"
    if [[ "$err" == *"could not determine"* ]]; then
        pass "unknown warns on stderr"
    else
        fail "unknown warns on stderr" "stderr was: $err"
    fi

)

# --------------------------------------------------------------------------
echo ""
echo "5. Real listener on an ephemeral port"
if command -v python3 >/dev/null 2>&1; then
    PORT_FILE=$(mktemp)
    # Cover the temp file from the moment it exists, then widen the trap to the
    # child as soon as there is a pid; registering only after the fork leaves a
    # window where an interrupt orphans the listener for its full 30s sleep.
    trap 'rm -f "$PORT_FILE" "$RESULTS"' EXIT
    python3 -c '
import socket, sys, time
s = socket.socket()
s.bind(("127.0.0.1", 0))
s.listen(1)
print(s.getsockname()[1])
sys.stdout.flush()
time.sleep(30)
' > "$PORT_FILE" &
    PY_PID=$!
    trap 'kill "$PY_PID" 2>/dev/null; rm -f "$PORT_FILE" "$RESULTS"' EXIT

    BOUND=""
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        BOUND=$(head -1 "$PORT_FILE" 2>/dev/null)
        [[ -n "$BOUND" ]] && break
        # Whole seconds only: fractional sleep is not POSIX, and this suite runs
        # with `set -u` but no `-e`, so a busybox sleep would fail silently and
        # spin all ten iterations instantly.
        sleep 1
    done

    if [[ -z "$BOUND" ]] || ! kill -0 "$PY_PID" 2>/dev/null; then
        fail "listener bound an ephemeral port" "child did not report a port"
    else
        pass "listener bound an ephemeral port ($BOUND)"
        expect "bound port is busy" "busy" "$(port_listen_state "$BOUND")"
        (check_port "http://127.0.0.1:$BOUND" >/dev/null 2>&1)
        expect "check_port exits 1 on the bound port" "1" "$?"

        # A port that was bound and released must not read as busy.
        FREE=$(python3 -c 'import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
p = s.getsockname()[1]
s.close()
print(p)')
        expect "released port is not busy" "0" "$([[ "$(port_listen_state "$FREE")" == "busy" ]] && echo 1 || echo 0)"
    fi

    kill "$PY_PID" 2>/dev/null
    wait "$PY_PID" 2>/dev/null
    rm -f "$PORT_FILE"
    # Restore the script-level handler rather than clearing it, so the tally
    # file stays covered for the remaining sections.
    trap 'rm -f "$RESULTS"' EXIT
else
    echo "  SKIP  real listener test: python3 not available"
fi

# --------------------------------------------------------------------------
echo ""
echo "6. download() rename failure is reported"
(
    WORK=$(mktemp -d)
    trap 'rm -rf "$WORK"' EXIT

    command_available() { [[ "$1" == "curl" ]]; }
    curl() {
        # Write to the -o destination so a temp file exists to rename.
        local out=""
        while [[ $# -gt 0 ]]; do
            [[ "$1" == "-o" ]] && { out="$2"; shift; }
            shift
        done
        echo "payload" > "$out"
        return 0
    }
    # Simulate a rename that fails (read-only target, cross-device, ...).
    mv() { return 1; }

    if download "$WORK" "https://example.com/pkg.tar.gz"; then
        fail "download reports rename failure" "download returned 0 after mv failed"
    else
        pass "download reports rename failure"
    fi

    if [[ -e "$WORK/pkg.tar.gz" ]]; then
        fail "failed rename leaves no destination" "$WORK/pkg.tar.gz exists"
    else
        pass "failed rename leaves no destination"
    fi

    leftover=$(find "$WORK" -name '.pkg.tar.gz.*' 2>/dev/null | wc -l | tr -d ' ')
    expect "failed rename cleans its temp file" "0" "$leftover"

)

# --------------------------------------------------------------------------
echo ""
echo "7. Startup probe is bounded"
(
    WORK=$(mktemp -d)
    cd "$WORK" || exit 1
    trap 'cd /; rm -rf "$WORK"' EXIT

    CLOCK_FILE="$WORK/clock"
    ARGS_FILE="$WORK/curl-args"
    SLEEP_FILE="$WORK/sleep-args"
    echo 100 > "$CLOCK_FILE"
    : > "$ARGS_FILE"
    : > "$SLEEP_FILE"
    curl() { echo "$*" >> "$ARGS_FILE"; echo "000"; return 28; }
    # Keep the contract checks deterministic across whole-second boundaries.
    # The retry pause consumes the remaining simulated second without making
    # this half of the test depend on scheduler timing.
    date() { cat "$CLOCK_FILE"; }
    sleep() {
        echo "$1" >> "$SLEEP_FILE"
        echo 101 > "$CLOCK_FILE"
    }
    process_status() { return 0; }
    ps() { return 0; }

    TIMEOUT_S=1
    if wait_for_startup "$$" "test-server" "http://127.0.0.1:1" "$TIMEOUT_S" \
                        >/dev/null 2>&1; then
        WAIT_STATUS=0
    else
        WAIT_STATUS=$?
    fi

    expect "failed startup probe returns failure" "1" "$WAIT_STATUS"

    # The deterministic half: bounding each curl does not bound the loop, which
    # used to sleep a flat 2s and only then re-read the clock.  A 1s timeout
    # must never ask for more than 1s of sleeping.
    TOTAL_SLEEP_S=$(awk '{ total += $1 } END { print total + 0 }' "$SLEEP_FILE")
    if [[ "$TOTAL_SLEEP_S" -le "$TIMEOUT_S" ]]; then
        pass "retry sleeps stay inside the ${TIMEOUT_S}s deadline"
    else
        fail "retry sleeps stay inside the ${TIMEOUT_S}s deadline" \
             "slept ${TOTAL_SLEEP_S}s in total: $(tr '\n' ' ' < "$SLEEP_FILE")"
    fi

    # With a one-second overall deadline, every positive remaining budget is
    # exactly one second.  Parse each curl call so option presence, values, and
    # the deadline bound are all part of the contract rather than string-grep
    # smoke checks.
    CURL_CALLS=0
    TIMEOUT_VALUES_VALID=1
    TIMEOUT_ERROR=""
    while IFS= read -r curl_args; do
        [[ -z "$curl_args" ]] && continue
        CURL_CALLS=$((CURL_CALLS + 1))
        CONNECT_TIMEOUT=""
        MAX_TIME=""
        set -- $curl_args
        while [[ $# -gt 0 ]]; do
            case "$1" in
                --connect-timeout)
                    shift
                    [[ $# -gt 0 ]] && CONNECT_TIMEOUT="$1"
                    ;;
                --max-time)
                    shift
                    [[ $# -gt 0 ]] && MAX_TIME="$1"
                    ;;
            esac
            [[ $# -gt 0 ]] && shift
        done

        if [[ ! "$CONNECT_TIMEOUT" =~ ^[0-9]+$ ||
              ! "$MAX_TIME" =~ ^[0-9]+$ ||
              "$CONNECT_TIMEOUT" -le 0 || "$CONNECT_TIMEOUT" -gt "$TIMEOUT_S" ||
              "$MAX_TIME" -le 0 || "$MAX_TIME" -gt "$TIMEOUT_S" ]]; then
            TIMEOUT_VALUES_VALID=0
            TIMEOUT_ERROR="invalid timeouts connect='$CONNECT_TIMEOUT' max='$MAX_TIME' in: $curl_args"
            break
        fi
    done < "$ARGS_FILE"

    if [[ "$CURL_CALLS" -gt 0 && "$TIMEOUT_VALUES_VALID" -eq 1 ]]; then
        pass "startup probe timeouts are positive and within the remaining deadline"
    else
        [[ -n "$TIMEOUT_ERROR" ]] || TIMEOUT_ERROR="no curl calls recorded"
        fail "startup probe timeouts are positive and within the remaining deadline" \
             "$TIMEOUT_ERROR"
    fi

    # A probe may consume all of its assigned budget.  Refreshing the clock
    # after curl must then suppress the retry pause rather than sleep beyond
    # the overall deadline.
    EXHAUST_SLEEP_FILE="$WORK/exhausted-sleep-calls"
    echo 100 > "$CLOCK_FILE"
    : > "$EXHAUST_SLEEP_FILE"
    curl() {
        echo 101 > "$CLOCK_FILE"
        echo "000"
        return 28
    }
    sleep() { echo "$1" >> "$EXHAUST_SLEEP_FILE"; }
    if wait_for_startup "$$" "budget-server" "http://127.0.0.1:1" \
                        "$TIMEOUT_S" >/dev/null 2>&1; then
        EXHAUST_STATUS=0
    else
        EXHAUST_STATUS=$?
    fi

    expect "budget-exhausting probe returns failure" "1" "$EXHAUST_STATUS"
    EXHAUST_SLEEP_CALLS=$(wc -l < "$EXHAUST_SLEEP_FILE" | tr -d ' ')
    expect "budget-exhausting probe starts no retry sleep" \
           "0" "$EXHAUST_SLEEP_CALLS"

    # Keep a small real-clock smoke test for the end-to-end deadline.  Crossing
    # an integer-second boundary between the initial and pre-probe clock reads
    # may legitimately produce zero curl calls, so this half checks only the
    # return status and wall-clock bound and does not require an args file.
    date() { command date "$@"; }
    curl() { echo "000"; return 28; }
    sleep() { command sleep "$1"; }
    START_S=$(date '+%s')
    if wait_for_startup "$$" "wall-clock-server" "http://127.0.0.1:1" \
                        "$TIMEOUT_S" >/dev/null 2>&1; then
        WALL_STATUS=0
    else
        WALL_STATUS=$?
    fi
    ELAPSED_S=$(( $(date '+%s') - START_S ))

    expect "real-clock failed startup probe returns failure" "1" "$WALL_STATUS"
    # One second of slack accounts for the granularity of date '+%s'.
    if [[ "$ELAPSED_S" -le $((TIMEOUT_S + 1)) ]]; then
        pass "wait_for_startup returns within the deadline (${ELAPSED_S}s)"
    else
        fail "wait_for_startup returns within the deadline" \
             "took ${ELAPSED_S}s for a ${TIMEOUT_S}s timeout"
    fi

)

# --------------------------------------------------------------------------
echo ""
echo "8. Deadline boundary starts no probe"
(
    WORK=$(mktemp -d)
    cd "$WORK" || exit 1
    trap 'cd /; rm -rf "$WORK"' EXIT

    CLOCK_FILE="$WORK/clock-calls"
    CURL_FILE="$WORK/curl-calls"
    echo 0 > "$CLOCK_FILE"
    : > "$CURL_FILE"

    # The first read establishes [100, 101) as the allowed interval.  The
    # process check consumes that interval, so the mandatory pre-curl refresh
    # observes the deadline exactly.  File-backed state works through the
    # command substitutions used by wait_for_startup, including Bash 3.2.
    date() {
        local calls
        calls=$(cat "$CLOCK_FILE")
        calls=$((calls + 1))
        echo "$calls" > "$CLOCK_FILE"
        if [[ "$calls" -eq 1 ]]; then
            echo 100
        else
            echo 101
        fi
    }
    process_status() { return 0; }
    curl() { echo called >> "$CURL_FILE"; echo "000"; return 28; }
    sleep() { echo called >> "$WORK/sleep-calls"; }

    wait_for_startup "$$" "boundary-server" "http://127.0.0.1:1" 1 \
                     >/dev/null 2>&1

    CLOCK_CALLS=$(cat "$CLOCK_FILE")
    if [[ "$CLOCK_CALLS" -ge 2 ]]; then
        pass "startup refreshes the clock at the probe boundary"
    else
        fail "startup refreshes the clock at the probe boundary" \
             "clock was read only ${CLOCK_CALLS} time(s)"
    fi

    PROBE_CALLS=$(wc -l < "$CURL_FILE" | tr -d ' ')
    expect "no startup probe begins at the deadline" "0" "$PROBE_CALLS"

)

if [[ -n "$STORE_UTIL_SH" ]]; then
    # ----------------------------------------------------------------------
    echo ""
    echo "9. Store verified replacement survives a stale concurrent caller"
    (
        # Keep the Store helper overrides isolated from the server helpers used
        # by every other section in this suite.
        # shellcheck source=/dev/null
        source "$STORE_UTIL_SH"

        WORK=$(mktemp -d)
        DEST="$WORK/lib.so"
        HASHED="$WORK/stale-hashed"
        RELEASE="$WORK/release-stale"
        STALE_DOWNLOAD="$WORK/stale-download"
        STALE_PID=""
        trap '
            touch "$RELEASE"
            if [[ -n "$STALE_PID" ]]; then
                kill "$STALE_PID" 2>/dev/null
                wait "$STALE_PID" 2>/dev/null
            fi
            rm -rf "$WORK"
        ' EXIT

        echo "invalid" > "$DEST"

        wait_for_file() {
            local path="$1"
            local _
            for _ in 1 2 3 4 5 6 7 8 9 10; do
                [[ -e "$path" ]] && return 0
                command sleep 1
            done
            return 1
        }

        # Both callers first observe the invalid destination.  The stale caller
        # then holds that checksum until the installer has atomically published
        # valid bytes.  Reintroducing the old pre-download rm makes the stale
        # caller delete that valid replacement before its own download fails.
        md5sum() {
            local target="${@: -1}"
            if [[ "$target" == "$DEST" ]]; then
                if [[ "$CALLER" == "stale" ]]; then
                    touch "$HASHED"
                    wait_for_file "$RELEASE" || return 1
                fi
                echo "bad  $target"
            else
                echo "good  $target"
            fi
        }

        curl() {
            [[ "$1" == "-fL" && "$2" == "-o" && "$4" == "--" ]] || return 64
            if [[ "$CALLER" == "installer" ]]; then
                echo "valid" > "$3"
                return 0
            fi
            touch "$STALE_DOWNLOAD"
            return 22
        }

        (
            CALLER="stale"
            download_and_verify "https://example.com/lib.so" "$DEST" "good" \
                                >/dev/null 2>&1
        ) &
        STALE_PID=$!

        if ! wait_for_file "$HASHED"; then
            fail "stale caller captured the original checksum" \
                 "caller did not reach the checksum barrier"
            exit 0
        fi
        pass "stale caller captured the original checksum"

        CALLER="installer"
        if download_and_verify "https://example.com/lib.so" "$DEST" "good" \
                               >/dev/null 2>&1; then
            pass "concurrent installer succeeds"
        else
            fail "concurrent installer succeeds" "installer returned nonzero"
        fi

        touch "$RELEASE"
        wait "$STALE_PID" 2>/dev/null
        STALE_WAIT_RC=$?
        STALE_PID=""
        if [[ "$STALE_WAIT_RC" -ne 0 && -e "$STALE_DOWNLOAD" ]]; then
            pass "stale caller reports its failed replacement download"
        else
            CURL_REACHED=$([[ -e "$STALE_DOWNLOAD" ]] && echo yes || echo no)
            fail "stale caller reports its failed replacement download" \
                 "rc=$STALE_WAIT_RC, curl reached=$CURL_REACHED"
        fi

        if [[ -f "$DEST" && "$(cat "$DEST")" == "valid" ]]; then
            pass "failed stale caller preserves the valid replacement"
        else
            fail "failed stale caller preserves the valid replacement" \
                 "destination is missing or no longer contains valid bytes"
        fi

        LEFTOVERS=$(find "$WORK" -name 'lib.so.*' -type f | wc -l | tr -d ' ')
        expect "concurrent replacement cleans private temp files" "0" "$LEFTOVERS"
    )
fi

# --------------------------------------------------------------------------
echo ""
echo "----------------------------------------"
# grep -c prints 0 and exits 1 when there are no matches; the count is what
# matters here, so the exit status is deliberately ignored.
PASSED=$(grep -c '^P' "$RESULTS" 2>/dev/null)
FAILED=$(grep -c '^F' "$RESULTS" 2>/dev/null)
rm -f "$RESULTS"
echo "passed: $PASSED   failed: $FAILED"
if [[ "$FAILED" -gt 0 ]]; then
    exit 1
fi
exit 0
