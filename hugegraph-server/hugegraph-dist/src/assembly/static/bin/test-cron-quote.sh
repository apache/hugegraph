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
# Local regression tests for the cron serialization helpers in util.sh:
#   shell_quote, reject_unsafe_path, cron_quote,
#   crontab_append, crontab_remove.
# No cluster, Java, or real crontab needed: `crontab` is replaced by a stub that
# reads/writes a temp file, so the invoking user's crontab is not touched.
# Run: bash test-cron-quote.sh

set -u

UTIL="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/util.sh"
# shellcheck disable=SC1090
. "$UTIL"

PASS=0
FAIL=0
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

ok()  { echo "PASS: $1"; PASS=$((PASS+1)); }
bad() { echo "FAIL: $1"; FAIL=$((FAIL+1)); }

# ---- stub crontab ---------------------------------------------------------
STUB_DIR="$TMP/stubbin"
CRON_FILE="$TMP/crontab.txt"
mkdir -p "$STUB_DIR"
cat > "$STUB_DIR/crontab" <<EOF
#!/bin/sh
case "\$1" in
    -l) [ -f "$CRON_FILE" ] && cat "$CRON_FILE"; exit 0 ;;
    # Buffer all of stdin first, then replace: the real crontab does not truncate
    # the table while the producer (crontab -l) is still reading it.
    -)  cat > "$CRON_FILE.new" && mv "$CRON_FILE.new" "$CRON_FILE"; exit 0 ;;
esac
exit 1
EOF
chmod +x "$STUB_DIR/crontab"
export PATH="$STUB_DIR:$PATH"
: > "$CRON_FILE"

# Which POSIX shells to execute the serialized values through.
SHELLS="/bin/sh"
command -v dash >/dev/null 2>&1 && SHELLS="$SHELLS $(command -v dash)"
[ -x /bin/bash ] && SHELLS="$SHELLS /bin/bash"

# ---------------------------------------------------------------------------
echo "== A. shell_quote round-trips through each shell =="

# Values that must survive byte-for-byte. Built with $'..' so the odd ones are exact.
VALUES=(
    "/opt/hugegraph"
    "/var/log/Huge Graph"
    "/data/team's-logs"
    "/data/o''neill"
    "'leading-and-trailing'"
    "'"
    "''"
    '/data/a\b'
    '/data/a\'"'"'b'
    '/data/a\\b'
    '/tmp/$(touch PWNED)'
    '/tmp/`touch PWNED`'
    '/tmp/$HOME'
    '/a;touch PWNED;b'
    '/a&b|c>d<e'
    '/a*?[x]~#!(){}b'
    $'/tmp/tab\tsep'
    '/tmp/dq"x'
    "/tmp/-n"
    ""
)

for sh_bin in $SHELLS; do
    n=0
    for v in "${VALUES[@]}"; do
        n=$((n+1))
        q="$(shell_quote "$v")"
        # Execute exactly like cron would: hand the text to `sh -c`.
        got="$(cd "$TMP" && "$sh_bin" -c "V=$q; printf %s \"\$V\"" 2>&1)"
        if [ "$got" = "$v" ]; then
            ok "$(basename "$sh_bin") #$n round-trips: [$v]"
        else
            bad "$(basename "$sh_bin") #$n: want [$v] got [$got]"
        fi
    done
done
[ -e "$TMP/PWNED" ] && bad "a metacharacter value executed a command" \
    || ok "no metacharacter value was executed"

# An apostrophe is closed, escaped and reopened.
expected="'/data/team'\\''s-logs'"
got="$(shell_quote "/data/team's-logs")"
[ "$got" = "$expected" ] && ok "apostrophe is escaped -> $expected" || bad "apostrophe escaping: want $expected got $got"

# Callers must not need extra quotes: output is already one complete word.
case "$(shell_quote "a b")" in "'a b'") ok "output carries its own quotes" ;; *) bad "own quotes" ;; esac

# A trailing newline must be preserved by the quoting itself (no $( ) strip).
q="$(shell_quote $'x\n')"; [ "$q" = $'\'x\n\'' ] && ok "shell_quote keeps a trailing newline" \
    || bad "shell_quote lost trailing newline"
# (command substitution above strips only OUR capture; compare via printf -v to be exact)
printf -v q '%s' "$(shell_quote $'x\n'; printf .)"; q="${q%.}"
[ "$q" = $'\'x\n\'' ] && ok "shell_quote emits the newline inside the quotes" || bad "newline inside quotes"

# Should not emit bash-only ANSI-C quoting (dash cannot parse it).
for v in '/data/a\b' $'/tmp/nl\nx' "/data/team's"; do
    label="${v//$'\n'/<LF>}"
    case "$(shell_quote "$v")" in
        *\$\'*) bad "emitted \$'...' for [$label]" ;;
        *) ok "no \$'...' emitted for [$label]" ;;
    esac
done

# ---------------------------------------------------------------------------
echo "== B. reject_unsafe_path =="

expect_rc() { # desc want_rc value label mode
    local out rc
    out="$(reject_unsafe_path "$3" "$4" "$5" 2>&1 >/dev/null)"; rc=$?
    if [ "$rc" -eq "$2" ]; then ok "$1"; else bad "$1 (rc=$rc want=$2)"; fi
    LAST_ERR="$out"
}

expect_rc "crlf: plain path accepted"            0 "/opt/x" L crlf
expect_rc "crlf: LF rejected"                    1 $'/opt/x\ny' L crlf
expect_rc "crlf: CR rejected"                    1 $'/opt/x\ry' L crlf
expect_rc "crlf: trailing LF rejected"           1 $'/opt/x\n' L crlf
expect_rc "crlf: '%' still accepted (non-cron)"  0 "/opt/100%/x" L crlf
expect_rc "cron: plain path accepted"            0 "/opt/x" L cron
expect_rc "cron: '%' rejected"                   1 "/opt/100%/x" L cron
expect_rc "cron: LF rejected"                    1 $'/opt/x\ny' L cron
expect_rc "cron: apostrophe/space accepted"      0 "/data/team's logs" L cron
expect_rc "cron: empty accepted"                 0 "" L cron

expect_rc "message names the label" 1 "a%b" "CONF_OVERRIDE" cron
case "$LAST_ERR" in *CONF_OVERRIDE*"%"*) ok "error mentions label and %" ;; *) bad "error text: $LAST_ERR" ;; esac
expect_rc "message for CR/LF" 1 $'a\nb' "LOGS_OVERRIDE" crlf
case "$LAST_ERR" in *LOGS_OVERRIDE*CR/LF*) ok "error mentions label and CR/LF" ;; *) bad "error text: $LAST_ERR" ;; esac

# Errors go to stderr, not stdout.
out="$(reject_unsafe_path $'a\nb' L crlf 2>/dev/null)"
[ -z "$out" ] && ok "diagnostics go to stderr only" || bad "diagnostic leaked to stdout"

# CR/LF must be checked on the raw value, before canonicalization: $( ) strips a
# trailing newline, so a check made afterwards would not see it.
raw=$'/tmp/x\n'
stripped="$(printf '%s' "$raw")"
reject_unsafe_path "$stripped" L crlf 2>/dev/null \
    && ok "a trailing LF is already gone after command substitution" \
    || bad "stripped value unexpectedly rejected"
reject_unsafe_path "$raw" L crlf 2>/dev/null \
    && bad "raw trailing-LF value must be rejected" \
    || ok "raw value with trailing LF is rejected"

# ---------------------------------------------------------------------------
echo "== C. cron_quote =="
q="$(cron_quote "/data/team's-logs" L)"; rc=$?
[ $rc -eq 0 ] && [ "$q" = "'/data/team'\\''s-logs'" ] && ok "cron_quote quotes valid input" || bad "cron_quote valid ($q)"
out="$(cron_quote "/a%b" L 2>/dev/null)"; rc=$?
[ $rc -ne 0 ] && [ -z "$out" ] && ok "cron_quote rejects '%' and prints nothing" || bad "cron_quote % (rc=$rc out=$out)"
out="$(cron_quote $'/a\nb' L 2>/dev/null)"; rc=$?
[ $rc -ne 0 ] && [ -z "$out" ] && ok "cron_quote rejects LF and prints nothing" || bad "cron_quote LF"

# ---------------------------------------------------------------------------
echo "== D. serialized job executes through sh with the right environment =="

# Build a job the same way start-monitor.sh does, but point the "monitor" at a
# probe script that records what it received, then run the saved line via sh -c.
for sh_bin in $SHELLS; do
    BASE="$TMP/it's a dir/with space"
    mkdir -p "$BASE/bin"
    PROBE="$BASE/bin/monitor-hugegraph.sh"
    cat > "$PROBE" <<'EOF'
#!/bin/sh
printf 'CONF=[%s]\nLOGS=[%s]\nPID=[%s]\nPLUG=[%s]\nJH=[%s]\n' \
    "$CONF_OVERRIDE" "$LOGS_OVERRIDE" "$PID_FILE_OVERRIDE" "$PLUGINS_OVERRIDE" "$JAVA_HOME"
EOF
    chmod +x "$PROBE"

    c="/data/team's conf"; l='/var/log/a\b'; p='/run/$HOME/pid'; o='/opt/`id`/plugins'; jh="/usr/lib/jvm/jdk 17's"
    job="*/1 * * * * export JAVA_HOME=$(cron_quote "$jh" JH) &&"
    job="$job export CONF_OVERRIDE=$(cron_quote "$c" C) &&"
    job="$job export LOGS_OVERRIDE=$(cron_quote "$l" L) &&"
    job="$job export PID_FILE_OVERRIDE=$(cron_quote "$p" P) &&"
    job="$job export PLUGINS_OVERRIDE=$(cron_quote "$o" O)"
    job="$job && $(cron_quote "$PROBE" M)"

    cmd="${job#\*/1 \* \* \* \* }"   # drop the 5 time fields: what cron hands to sh -c
    res="$("$sh_bin" -c "$cmd" 2>&1)"
    want="CONF=[$c]
LOGS=[$l]
PID=[$p]
PLUG=[$o]
JH=[$jh]"
    if [ "$res" = "$want" ]; then ok "$(basename "$sh_bin"): all six values survive execution"
    else bad "$(basename "$sh_bin"): got:
$res"; fi
done

# ---------------------------------------------------------------------------
echo "== E. crontab_append / crontab_remove (stubbed crontab) =="

printf '%s\n' "# existing entry" "0 3 * * * /usr/bin/backup" > "$CRON_FILE"
BEFORE="$(cat "$CRON_FILE")"

TOP="$TMP/team's top"; mkdir -p "$TOP/bin"
KEY="$(cron_quote "$TOP/bin/monitor-hugegraph.sh" M)"   # what start-monitor.sh saves
JOB="*/1 * * * * export CONF_OVERRIDE=$(cron_quote "/data/team's conf" C) && $KEY"

crontab_append "$JOB"; rc=$?
[ $rc -eq 0 ] && grep -qF -- "$JOB" "$CRON_FILE" && ok "append wrote the entry" || bad "append (rc=$rc)"

crontab_append "$JOB"; rc=$?
[ $rc -ne 0 ] && ok "duplicate append is refused (rc=$rc)" || bad "duplicate append accepted"
[ "$(grep -cF -- "$JOB" "$CRON_FILE")" -eq 1 ] && ok "exactly one copy after duplicate add" || bad "duplicate entry written"

# The unquoted path does not match the serialized line when it contains an apostrophe.
RAW_KEY="$TOP/bin/monitor-hugegraph.sh"
grep -qF -- "$RAW_KEY" "$CRON_FILE" \
    && bad "unquoted key unexpectedly matches the serialized line" \
    || ok "unquoted key does not match an apostrophe install path"

# stop-monitor.sh derives the key with shell_quote; it must match what was saved.
STOP_KEY="$(shell_quote "$TOP/bin/monitor-hugegraph.sh")"
[ "$STOP_KEY" = "$KEY" ] && ok "stop key equals the saved key" || bad "stop key differs: $STOP_KEY vs $KEY"
crontab_remove "$STOP_KEY"; rc=$?
[ $rc -eq 0 ] && ok "remove succeeded for apostrophe install path" || bad "remove rc=$rc"
grep -qF -- "$JOB" "$CRON_FILE" && bad "job still present after remove" || ok "job removed"
[ "$(cat "$CRON_FILE")" = "$BEFORE" ] && ok "unrelated crontab lines untouched" || bad "other entries damaged"

crontab_remove "$STOP_KEY"; rc=$?
[ $rc -eq 0 ] && ok "removing an absent job is a no-op success" || bad "absent remove rc=$rc"

# printf, not echo: backslashes and a leading -n must be written verbatim.
: > "$CRON_FILE"
crontab_append '-n \t \\ literal'; rc=$?
[ "$(cat "$CRON_FILE")" = '-n \t \\ literal' ] && ok "append is literal (backslashes, leading -n)" \
    || bad "append mangled: $(cat "$CRON_FILE")"

# grep -F -- : a job beginning with '-' is a pattern, not an option.
crontab_append '-n \t \\ literal'; rc=$?
[ $rc -ne 0 ] && ok "duplicate detection works for a job starting with '-'" || bad "leading '-' broke grep"

# ---------------------------------------------------------------------------
echo "== F. rejected input never changes the existing crontab =="

printf '%s\n' "# keep me" "5 5 * * * /bin/true" > "$CRON_FILE"
SNAP="$(cat "$CRON_FILE")"

# Reproduce start-monitor.sh's flow.
build_and_append() {
    local q
    q="$(cron_quote "$1" CONF_OVERRIDE)" || return 1
    crontab_append "*/1 * * * * export CONF_OVERRIDE=$q"
}
build_and_append "/opt/100%/conf";  rc=$?
[ $rc -ne 0 ] && [ "$(cat "$CRON_FILE")" = "$SNAP" ] && ok "'%' rejected, crontab unchanged" || bad "'%' flow (rc=$rc)"
build_and_append $'/opt/x\n* * * * * evil'; rc=$?
[ $rc -ne 0 ] && [ "$(cat "$CRON_FILE")" = "$SNAP" ] && ok "newline-injection rejected, crontab unchanged" || bad "LF flow (rc=$rc)"
grep -q evil "$CRON_FILE" && bad "injected job reached crontab" || ok "no injected job in crontab"

# start-monitor.sh itself: wrong order of validation must exit non-zero w/o touching cron.
SM="$(cd "$(dirname "$UTIL")" && pwd)/start-monitor.sh"
if [ -f "$SM" ]; then
    out="$(JAVA_HOME=/usr CONF_OVERRIDE='/opt/100%/conf' bash "$SM" 2>&1)"; rc=$?
    [ $rc -ne 0 ] && [ "$(cat "$CRON_FILE")" = "$SNAP" ] \
        && ok "start-monitor.sh exits non-zero and leaves crontab unchanged on '%'" \
        || bad "start-monitor.sh with '%' (rc=$rc): $out"
fi

# ---------------------------------------------------------------------------
echo
echo "Shells exercised: $SHELLS"
command -v dash >/dev/null 2>&1 || echo "NOTE: dash not installed here; run on Debian/Ubuntu/CI to cover it."
echo "Passed: $PASS  Failed: $FAIL"
[ "$FAIL" -eq 0 ]
