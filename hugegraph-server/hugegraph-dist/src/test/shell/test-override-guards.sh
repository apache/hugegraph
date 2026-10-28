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
# Guard-coverage tests for the -c/-l/-o/-i path overrides across Server, PD and
# Store. Static (grep-based) assertions plus behavioural checks driven with stub
# binaries. No JVM, cluster or network needed.
#
#   bash test-override-guards.sh            # run from a source checkout
#

set -u

BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Walk up to the repository root (the dir holding hugegraph-server/).
ROOT="$BIN"
while [ "$ROOT" != "/" ] && [ ! -d "$ROOT/hugegraph-server" ]; do
    ROOT="$(dirname "$ROOT")"
done

SERVER_BIN="$ROOT/hugegraph-server/hugegraph-dist/src/assembly/static/bin"
PD_BIN="$ROOT/hugegraph-pd/hg-pd-dist/src/assembly/static/bin"
STORE_BIN="$ROOT/hugegraph-store/hg-store-dist/src/assembly/static/bin"

PASS=0; FAIL=0; SKIP=0
ok()   { echo "PASS: $1"; PASS=$((PASS+1)); }
bad()  { echo "FAIL: $1"; FAIL=$((FAIL+1)); }
skip() { echo "SKIP: $1"; SKIP=$((SKIP+1)); }

# has FILE REGEX DESC -- assert the file matches
has() {
    [ -f "$1" ] || { skip "$3 (no $1)"; return; }
    if grep -Eq "$2" "$1"; then ok "$3"; else bad "$3"; fi
}
# hasnt FILE REGEX DESC -- assert the file does NOT match
hasnt() {
    [ -f "$1" ] || { skip "$3 (no $1)"; return; }
    if grep -Eq "$2" "$1"; then bad "$3"; else ok "$3"; fi
}

echo "== 1. raw-OPTARG validation in start scripts =="

# -o must use 'classpath' mode (not plain crlf): the plugins dir is joined into
# a ':'-separated CLASSPATH, so a ':' inside it would split one entry into two.
has "$SERVER_BIN/start-hugegraph.sh" \
    'reject_unsafe_path .*-o plugins dir. classpath' \
    "server start: -o uses classpath mode"
has "$PD_BIN/start-hugegraph-pd.sh" \
    'reject_unsafe_path .*-o plugins dir. classpath' \
    "pd start: -o uses classpath mode"
has "$STORE_BIN/start-hugegraph-store.sh" \
    'reject_unsafe_path .*-o plugins dir. classpath' \
    "store start: -o uses classpath mode"
has "$SERVER_BIN/init-store.sh" \
    'reject_unsafe_path .*-o plugins dir. classpath' \
    "init-store: -o uses classpath mode"

for f in "$SERVER_BIN/start-hugegraph.sh" "$PD_BIN/start-hugegraph-pd.sh" \
         "$STORE_BIN/start-hugegraph-store.sh"; do
    n="$(basename "$f")"
    has "$f" 'reject_unsafe_path .*-c conf dir. crlf'  "$n: -c rejects CR/LF"
    has "$f" 'reject_unsafe_path .*-l logs dir. crlf'  "$n: -l rejects CR/LF"
    has "$f" 'reject_unsafe_path .*-i pid file. crlf'  "$n: -i rejects CR/LF"
done


echo "== 2. canonicalization =="

# -c is READ-ONLY, so it should use nocreate: a typo must fail naming the wrong
# directory instead of silently creating an empty one. -l and -o are
# written into, so they must keep the creating default.
for f in "$SERVER_BIN/start-hugegraph.sh" "$PD_BIN/start-hugegraph-pd.sh" \
         "$STORE_BIN/start-hugegraph-store.sh"; do
    n="$(basename "$f")"
    has "$f" 'CONF_OVERRIDE="\$\(canonicalize_dir "\$CONF_OVERRIDE" nocreate\)"' "$n: -c canonicalized nocreate"
    has "$f" 'LOGS_OVERRIDE="\$\(canonicalize_dir "\$LOGS_OVERRIDE"\)"'    "$n: -l canonicalized (creating)"
    has "$f" 'PLUGINS_OVERRIDE="\$\(canonicalize_dir "\$PLUGINS_OVERRIDE"\)"' "$n: -o canonicalized (creating)"
    has "$f" 'PID_FILE_OVERRIDE="\$\(canonicalize_file "\$PID_FILE_OVERRIDE"\)"' "$n: -i canonicalized"
done
has "$SERVER_BIN/init-store.sh" \
    'CONF_OVERRIDE="\$\(canonicalize_dir "\$CONF_OVERRIDE" nocreate\)"' \
    "init-store: -c canonicalized nocreate"

# Stop scripts only READ the pid file, so they must not create its parent: a
# typo like '-i /var/run/hugegraf/x.pid' would otherwise leave an empty tree.
for f in "$SERVER_BIN/stop-hugegraph.sh" "$PD_BIN/stop-hugegraph-pd.sh" \
         "$STORE_BIN/stop-hugegraph-store.sh"; do
    n="$(basename "$f")"
    has "$f" 'canonicalize_file "\$PID_FILE_OVERRIDE" nocreate' "$n: -i uses nocreate"
    has "$f" 'reject_unsafe_path "\$PID_FILE_OVERRIDE"'         "$n: -i rejects CR/LF"
done

echo "== 3. writability / readability guards =="

for f in "$PD_BIN/start-hugegraph-pd.sh" "$STORE_BIN/start-hugegraph-store.sh"; do
    n="$(basename "$f")"
    has "$f" 'ensure_path_writable "\$LOGS"'                     "$n: -l writability checked"
    has "$f" 'ensure_path_writable "\$PLUGINS"'                  "$n: -o writability checked"
    has "$f" 'ensure_path_writable "\$\(dirname "\$PID_FILE"\)"' "$n: -i parent writability checked"
done
has "$SERVER_BIN/start-hugegraph.sh" 'ensure_path_writable "\$\(dirname "\$PID_FILE"\)"' \
    "server start: -i parent writability checked"
has "$SERVER_BIN/hugegraph-server.sh" 'ensure_path_writable "\$LOGS" report_error' \
    "hugegraph-server: -l writability checked"
has "$SERVER_BIN/monitor-hugegraph.sh" 'ensure_path_writable "\$LOG_DIR"' \
    "monitor: -l writability checked"

# read_property cannot tell "file missing" from "property unset", so every
# consumer must prove the file is readable first.
has "$SERVER_BIN/start-hugegraph.sh" 'ensure_file_readable .*rest-server.properties' \
    "server start: -c config readable"
has "$SERVER_BIN/init-store.sh" 'ensure_file_readable .*rest-server.properties' \
    "init-store: -c config readable"
has "$SERVER_BIN/monitor-hugegraph.sh" 'ensure_file_readable .*rest-server.properties' \
    "monitor: -c config readable"

# gremlin-server.yaml is not covered: it is read by hugegraph-server.sh via
# the fixed $CONF directory, not via a read_property lookup guarded here.
has "$PD_BIN/start-hugegraph-pd.sh" 'ensure_file_readable .*application.yml' \
    "pd start: -c config readable"
has "$STORE_BIN/start-hugegraph-store.sh" 'ensure_file_readable .*application.yml' \
    "store start: -c config readable"

# A relative 'graphs' value resolves against $TOP, not $CONF, so with -c set it
# would silently initialize the packaged graphs instead of the operator's.
has "$SERVER_BIN/start-hugegraph.sh" 'reject_relative_conf_path' \
    "server start: relative conf path rejected"
has "$SERVER_BIN/init-store.sh" 'reject_relative_conf_path' \
    "init-store: relative conf path rejected"

echo "== 4. restart forwarding =="

R="$STORE_BIN/restart-hugegraph-store.sh"
has "$R" 'STOP_ARGS\+=\(-i "\$PID_ARG"\)'   "restart: forwards -i to stop"
has "$R" 'START_ARGS\+=\(-c "\$CONF_ARG"\)' "restart: forwards -c to start"
has "$R" 'START_ARGS\+=\(-l "\$LOGS_ARG"\)' "restart: forwards -l to start"
has "$R" 'START_ARGS\+=\(-o "\$PLUGINS_ARG"\)' "restart: forwards -o to start"
has "$R" 'START_ARGS\+=\(-i "\$PID_ARG"\)'  "restart: forwards -i to start"
has "$R" '"\$\{STOP_ARGS\[@\]\}"'           "restart: STOP_ARGS expanded quoted"
has "$R" '"\$\{START_ARGS\[@\]\}"'          "restart: START_ARGS expanded quoted"
# Without this, a failed stop leaves the old JVM up; start then sees "already
# running" and exits 0, so the restart reports success having done nothing.
has "$R" 'stop-hugegraph-store\.sh "\$\{STOP_ARGS\[@\]\}"( \|\| exit|; then| \&\&)' \
    "restart: aborts when stop fails"
hasnt "$R" 'separate processes so that:$' "restart: comment not truncated"

echo "== 5. behavioural: rejection paths =="

# These run the real start scripts. They must fail during argument validation,
# before any JVM is reached, so no stubs are needed.
CR_PATH="$(printf '/tmp/a\rb')"
run_start() { # run_start SCRIPT ARGS... -> sets RC/OUT
    OUT="$(bash "$@" 2>&1)"; RC=$?
}

for f in "$SERVER_BIN/start-hugegraph.sh" "$PD_BIN/start-hugegraph-pd.sh" \
         "$STORE_BIN/start-hugegraph-store.sh"; do
    [ -f "$f" ] || { skip "$(basename "$f"): rejection tests"; continue; }
    n="$(basename "$f")"

    run_start "$f" -o "/tmp/a:b"
    { [ $RC -ne 0 ] && echo "$OUT" | grep -q "must not contain ':'"; } \
        && ok "$n: rejects ':' in -o" || bad "$n: rejects ':' in -o (rc=$RC)"

    run_start "$f" -l "$CR_PATH"
    { [ $RC -ne 0 ] && echo "$OUT" | grep -q "must not contain CR/LF"; } \
        && ok "$n: rejects CR/LF in -l" || bad "$n: rejects CR/LF in -l (rc=$RC)"

    run_start "$f" -c "$CR_PATH"
    { [ $RC -ne 0 ] && echo "$OUT" | grep -q "must not contain CR/LF"; } \
        && ok "$n: rejects CR/LF in -c" || bad "$n: rejects CR/LF in -c (rc=$RC)"
done

# '%' is only unsafe when the value is persisted into a crontab line.
run_start "$SERVER_BIN/start-hugegraph.sh" -m true -l "/tmp/h%l"
{ [ $RC -ne 0 ] && echo "$OUT" | grep -q "reserved by cron"; } \
    && ok "server start: rejects '%' in -l with -m true" \
    || bad "server start: rejects '%' in -l with -m true (rc=$RC)"

# Store embeds $LOGS in a -Xlog:gc=... value, whose grammar also delimits on
# ':', so it must be rejected before launch (quoting the argv doesn't help).
run_start "$STORE_BIN/start-hugegraph-store.sh" -l "/tmp/hg-review:logs"
{ [ $RC -ne 0 ] && echo "$OUT" | grep -q "must not contain ':'"; } \
    && ok "store start: rejects ':' in -l (GC log -Xlog grammar)" \
    || bad "store start: rejects ':' in -l (GC log -Xlog grammar) (rc=$RC)"

echo "== 6. behavioural: env-var entry point rejects the same as its CLI flag =="

# CONF_OVERRIDE/LOGS_OVERRIDE/PLUGINS_OVERRIDE/PID_FILE_OVERRIDE are also a
# supported entry point as plain environment variables (no-arg restart,
# monitor cron re-exec), bypassing -c/-l/-o/-i entirely. The guard must be
# checked on the effective variable after getopts.
run_start_env() { # run_start_env SCRIPT VAR=VALUE -> sets RC/OUT
    OUT="$(env "$2" bash "$1" 2>&1)"; RC=$?
}

for f in "$SERVER_BIN/start-hugegraph.sh" "$PD_BIN/start-hugegraph-pd.sh" \
         "$STORE_BIN/start-hugegraph-store.sh"; do
    [ -f "$f" ] || { skip "$(basename "$f"): env-var entry point tests"; continue; }
    n="$(basename "$f")"

    run_start_env "$f" "PLUGINS_OVERRIDE=/tmp/a:b"
    { [ $RC -ne 0 ] && echo "$OUT" | grep -q "must not contain ':'"; } \
        && ok "$n: rejects ':' in PLUGINS_OVERRIDE env var" \
        || bad "$n: rejects ':' in PLUGINS_OVERRIDE env var (rc=$RC)"

    run_start_env "$f" "LOGS_OVERRIDE=$CR_PATH"
    { [ $RC -ne 0 ] && echo "$OUT" | grep -q "must not contain CR/LF"; } \
        && ok "$n: rejects CR/LF in LOGS_OVERRIDE env var" \
        || bad "$n: rejects CR/LF in LOGS_OVERRIDE env var (rc=$RC)"

    run_start_env "$f" "CONF_OVERRIDE=$CR_PATH"
    { [ $RC -ne 0 ] && echo "$OUT" | grep -q "must not contain CR/LF"; } \
        && ok "$n: rejects CR/LF in CONF_OVERRIDE env var" \
        || bad "$n: rejects CR/LF in CONF_OVERRIDE env var (rc=$RC)"

    run_start_env "$f" "PID_FILE_OVERRIDE=$CR_PATH"
    { [ $RC -ne 0 ] && echo "$OUT" | grep -q "must not contain CR/LF"; } \
        && ok "$n: rejects CR/LF in PID_FILE_OVERRIDE env var" \
        || bad "$n: rejects CR/LF in PID_FILE_OVERRIDE env var (rc=$RC)"
done

echo "== 7. behavioural: stop must not create the pid parent =="

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
MISSING="$TMP/eg/test/dir/server.pid"
for f in "$SERVER_BIN/stop-hugegraph.sh" "$PD_BIN/stop-hugegraph-pd.sh" \
         "$STORE_BIN/stop-hugegraph-store.sh"; do
    [ -f "$f" ] || { skip "$(basename "$f"): nocreate"; continue; }
    n="$(basename "$f")"
    bash "$f" -i "$MISSING" >/dev/null 2>&1
    [ ! -d "$TMP/eg" ] && ok "$n: does not create the pid parent" \
                       || bad "$n: created $TMP/eg"
    rm -rf "$TMP/eg"
done

echo "== 8. behavioural: -c must not create the conf dir =="

# A typo in -c has to fail naming the wrong directory. Before nocreate, this
# call created the tree and only failed later on a missing file inside it.
MISSING_CONF="$TMP/confOverride/test"
for f in "$SERVER_BIN/start-hugegraph.sh" "$PD_BIN/start-hugegraph-pd.sh" \
         "$STORE_BIN/start-hugegraph-store.sh" "$SERVER_BIN/init-store.sh"; do
    [ -f "$f" ] || { skip "$(basename "$f"): -c nocreate"; continue; }
    n="$(basename "$f")"
    OUT="$(bash "$f" -c "$MISSING_CONF" 2>&1)"; RC=$?
    { [ $RC -ne 0 ] && [ ! -d "$MISSING_CONF" ] \
        && echo "$OUT" | grep -q "no such directory"; } \
        && ok "$n: missing -c dir rejected, not created" \
        || bad "$n: missing -c dir (rc=$RC created=$([ -d "$MISSING_CONF" ] && echo yes || echo no))"
    rm -rf "$TMP/confOverride"
done

echo "== 9. behavioural: restart aborts on a failed stop =="

# Shadow stop/start with stubs so the real exit-code wiring is exercised.
STUB="$TMP/stub"; mkdir -p "$STUB"
cp "$STORE_BIN/restart-hugegraph-store.sh" "$STUB/"
cp "$STORE_BIN/util.sh" "$STUB/"
printf '#!/bin/bash\nexit 1\n'                       > "$STUB/stop-hugegraph-store.sh"
printf '#!/bin/bash\ntouch "%s/started"\nexit 0\n' "$TMP" > "$STUB/start-hugegraph-store.sh"
chmod +x "$STUB"/*.sh

rm -f "$TMP/started"
bash "$STUB/restart-hugegraph-store.sh" -i "$TMP/x.pid" >/dev/null 2>&1
RC=$?
{ [ $RC -ne 0 ] && [ ! -f "$TMP/started" ]; } \
    && ok "restart: stop failure aborts before start" \
    || bad "restart: started=$([ -f "$TMP/started" ] && echo yes || echo no) rc=$RC"

# And a failed start must not be reported as a successful restart.
printf '#!/bin/bash\nexit 0\n' > "$STUB/stop-hugegraph-store.sh"
printf '#!/bin/bash\nexit 3\n' > "$STUB/start-hugegraph-store.sh"
chmod +x "$STUB"/*.sh
bash "$STUB/restart-hugegraph-store.sh" -i "$TMP/x.pid" >/dev/null 2>&1
[ $? -ne 0 ] && ok "restart: start failure propagates" \
             || bad "restart: start failure swallowed"

echo "== 10. behavioural: no-argument restart honors inherited overrides =="

# PdConfigureListener.restart() invokes restart-hugegraph-store.sh with NO
# arguments, relying solely on the CONF_OVERRIDE/LOGS_OVERRIDE/PLUGINS_OVERRIDE/
# PID_FILE_OVERRIDE env vars (exported by start-hugegraph-store.sh) to find the
# external conf/logs/plugins/pid. Prove the preflight checks and the forwarded
# stop/start calls target those external dirs -- distinct from the packaged
# $TOP/conf, $TOP/logs, $TOP/plugins, $BIN/pid -- instead of silently falling
# back to the (possibly read-only/unwritable) packaged ones.

# Isolated under its own fresh parent (not directly under $TMP): earlier
# sections already run restart-hugegraph-store.sh with no overrides from
# stubs whose TOP also resolves to $TMP, which legitimately creates
# $TMP/logs and $TMP/plugins as fallback there. Reusing $TMP directly for the
# negative control below would then false-fail on that unrelated leftover.
SECTION10="$TMP/section10"; mkdir -p "$SECTION10"

EXT="$SECTION10/external"
EXT_CONF="$EXT/conf"
EXT_LOGS="$EXT/logs"
EXT_PLUGINS="$EXT/plugins"
EXT_PID="$EXT/run/store.pid"
mkdir -p "$EXT_CONF" "$EXT_LOGS" "$EXT_PLUGINS" "$(dirname "$EXT_PID")"
echo "app: {}" > "$EXT_CONF/application.yml"

STUB2="$SECTION10/stub2"; mkdir -p "$STUB2"
cp "$STORE_BIN/restart-hugegraph-store.sh" "$STUB2/"
cp "$STORE_BIN/util.sh" "$STUB2/"
# Record the argv each child script was actually invoked with, instead of just
# a boolean "was it started" flag, so the forwarded -c/-l/-o/-i can be checked.
printf '#!/bin/bash\necho "$*" > "%s/stop_args.txt"\nexit 0\n' "$SECTION10" \
    > "$STUB2/stop-hugegraph-store.sh"
printf '#!/bin/bash\necho "$*" > "%s/start_args.txt"\nexit 0\n' "$SECTION10" \
    > "$STUB2/start-hugegraph-store.sh"
chmod +x "$STUB2"/*.sh

RESTART_OUT="$(CONF_OVERRIDE="$EXT_CONF" LOGS_OVERRIDE="$EXT_LOGS" \
    PLUGINS_OVERRIDE="$EXT_PLUGINS" PID_FILE_OVERRIDE="$EXT_PID" \
    bash "$STUB2/restart-hugegraph-store.sh" 2>&1)"
RC=$?

[ $RC -eq 0 ] && ok "no-arg restart: preflight succeeds against external dirs" \
    || bad "no-arg restart: preflight failed (rc=$RC): $RESTART_OUT"

STOP_OUT="$(cat "$SECTION10/stop_args.txt" 2>/dev/null)"
START_OUT="$(cat "$SECTION10/start_args.txt" 2>/dev/null)"

echo "$STOP_OUT" | grep -qF -- "-i $EXT_PID" \
    && ok "no-arg restart: forwards inherited -i to stop" \
    || bad "no-arg restart: stop args: '$STOP_OUT'"

echo "$START_OUT" | grep -qF -- "-c $EXT_CONF" \
    && ok "no-arg restart: forwards inherited -c to start" \
    || bad "no-arg restart: start args: '$START_OUT'"
echo "$START_OUT" | grep -qF -- "-l $EXT_LOGS" \
    && ok "no-arg restart: forwards inherited -l to start" \
    || bad "no-arg restart: start args: '$START_OUT'"
echo "$START_OUT" | grep -qF -- "-o $EXT_PLUGINS" \
    && ok "no-arg restart: forwards inherited -o to start" \
    || bad "no-arg restart: start args: '$START_OUT'"
echo "$START_OUT" | grep -qF -- "-i $EXT_PID" \
    && ok "no-arg restart: forwards inherited -i to start" \
    || bad "no-arg restart: start args: '$START_OUT'"

# Negative control: the packaged fallback dirs ($TOP/conf = $SECTION10/conf,
# $TOP/logs = $SECTION10/logs, $TOP/plugins = $SECTION10/plugins, since TOP
# resolves to STUB2's parent, $SECTION10, which is otherwise untouched) must
# stay unwritten -- otherwise this test could pass merely because
# canonicalize_dir "create"-mode made an empty packaged dir, not because the
# inherited overrides were actually used.
[ ! -e "$SECTION10/conf" ] && [ ! -e "$SECTION10/logs" ] && [ ! -e "$SECTION10/plugins" ] \
    && ok "no-arg restart: packaged fallback dirs untouched" \
    || bad "no-arg restart: packaged fallback dirs were created"

rm -rf "$SECTION10"

# Static regression guard: the _ARG vars must default from the inherited
# *_OVERRIDE env vars before getopts runs, so a no-argument restart doesn't
# silently validate the packaged conf/logs/plugins/pid.
has "$R" 'CONF_ARG="\$\{CONF_OVERRIDE:-\}"' \
    "restart: seeds -c default from inherited CONF_OVERRIDE"
has "$R" 'LOGS_ARG="\$\{LOGS_OVERRIDE:-\}"' \
    "restart: seeds -l default from inherited LOGS_OVERRIDE"
has "$R" 'PLUGINS_ARG="\$\{PLUGINS_OVERRIDE:-\}"' \
    "restart: seeds -o default from inherited PLUGINS_OVERRIDE"
has "$R" 'PID_ARG="\$\{PID_FILE_OVERRIDE:-\}"' \
    "restart: seeds -i default from inherited PID_FILE_OVERRIDE"

echo "== 11. behavioural: exported JAVA_OPTIONS survives into a child env =="

# Extracts the real JAVA_OPTIONS-handling block from each script (by marker,
# not reimplemented), sources it with JAVA_OPTIONS exported, then spawns a
# real child process (env, a separate exec) and asserts the child's
# environment -- not just the parent shell's variable -- still carries the
# original value, alongside the array built from it for the java launch line.
extract_java_options_snippet() { # file -> prints the JAVA_OPTIONS handling block
    awk '
        /JAVA_OPTIONS_ENV="\$\{JAVA_OPTIONS\}"/ { flag=1 }
        flag { print }
        flag && /^esac$/ { exit }
    ' "$1"
}

run_java_options_snippet() { # script util -> sets OUT
    local script="$1" util="$2" snippet
    snippet="$(extract_java_options_snippet "$script")"
    if [ -z "$snippet" ]; then
        OUT="<snippet not found>"
        return 1
    fi
    OUT="$(
        bash -c '
            source "'"$util"'"
            MIN_MEM=64; MAX_MEM=128; GC_OPTION=""; USER_OPTION=""
            LOGS="'"$TMP"'"; OUTPUT="'"$TMP"'/out.log"
            export JAVA_OPTIONS="-Xmx777m -Dexample.marker=yes123"
            '"$snippet"'
            echo "PARENT_JAVA_OPTIONS=$JAVA_OPTIONS"
            echo "ARR_DUMP=${JAVA_OPTIONS_ARR[*]}"
            # A separate "env" process is a real child: it only sees what was
            # actually exported, not just this shells local variables.
            env | grep "^JAVA_OPTIONS=" | sed "s/^/CHILD_ENV_/"
        ' 2>&1
    )"
}

for pair in "$STORE_BIN/start-hugegraph-store.sh:$STORE_BIN/util.sh" \
            "$PD_BIN/start-hugegraph-pd.sh:$PD_BIN/util.sh" \
            "$SERVER_BIN/hugegraph-server.sh:$SERVER_BIN/util.sh"; do
    script="${pair%%:*}"; util="${pair##*:}"
    n="$(basename "$script")"
    [ -f "$script" ] || { skip "$n: JAVA_OPTIONS env preserved (no script)"; continue; }

    run_java_options_snippet "$script" "$util"

    echo "$OUT" | grep -qF 'PARENT_JAVA_OPTIONS=-Xmx777m -Dexample.marker=yes123' \
        && ok "$n: JAVA_OPTIONS scalar unchanged after array build" \
        || bad "$n: JAVA_OPTIONS scalar unchanged (got: $OUT)"

    echo "$OUT" | grep -qF 'CHILD_ENV_JAVA_OPTIONS=-Xmx777m -Dexample.marker=yes123' \
        && ok "$n: child process env still has JAVA_OPTIONS" \
        || bad "$n: child process env has JAVA_OPTIONS (got: $OUT)"

    # The GC-selection block (also part of the extracted snippet) prepends its
    # own default flags, so check the user's options are present, not an exact
    # element count.
    { echo "$OUT" | grep -q 'ARR_DUMP=.*-Xmx777m' && \
      echo "$OUT" | grep -q 'ARR_DUMP=.*-Dexample.marker=yes123'; } \
        && ok "$n: JAVA_OPTIONS still split into the launch array" \
        || bad "$n: JAVA_OPTIONS split into the launch array (got: $OUT)"
done

echo
echo "===================================="
echo "PASS=$PASS  FAIL=$FAIL  SKIP=$SKIP"
echo "===================================="
[ "$FAIL" -eq 0 ]
