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
# ARGV REGRESSION TEST for the real Server / PD / Store JVM launchers.
#
# The helpers (canonicalize_*, cron_quote) can be correct while the
# launcher can still break because the bug lives in the final `exec`: script-built
# options were interpolated into ONE string and then expanded unquoted. For eg:
#
#     -c '/data/external conf'
#
# reached Java as TWO arguments:
#
#     -Dlog4j.configurationFile=/data/external      <- truncated
#     conf/log4j2.xml                               <- read as the MAIN CLASS
#
# which can cause the reported failure like
# "Could not find or load main class conf.log4j2.xml".
#
# This does not inspect the scripts: it RUNS them against a stub `java`
# that dumps its argv one element per line, and asserts on that argv. A bad
# split shows up as an extra standalone element ("conf/log4j2.xml", "logs",
# "plugins") .
#
#   bash test-launcher-argv.sh
#
# No JVM or cluster explictly is needed for this test.

PASS=0; FAIL=0; SKIP=0
ok()   { echo "PASS: $1"; PASS=$((PASS+1)); }
bad()  { echo "FAIL: $1"; FAIL=$((FAIL+1)); }
skip() { echo "SKIP: $1"; SKIP=$((SKIP+1)); }

BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$BIN"
while [ "$ROOT" != "/" ] && [ ! -d "$ROOT/hugegraph-server" ]; do
    ROOT="$(dirname "$ROOT")"
done
SERVER_SRC="$ROOT/hugegraph-server/hugegraph-dist/src/assembly/static/bin"
PD_SRC="$ROOT/hugegraph-pd/hg-pd-dist/src/assembly/static/bin"
STORE_SRC="$ROOT/hugegraph-store/hg-store-dist/src/assembly/static/bin"

# mktemp gives a space-free path on purpose: the point of the review is that the
# split happens even with a NORMAL install path, so only the overrides below
# carry spaces.
TMP="$(mktemp -d)" || { echo "cannot create temp dir"; exit 1; }
trap 'rm -rf "$TMP"' EXIT

DUMP="$TMP/argv.txt"

# ---------------------------------------------------------------------------
# Overrides whose LAST path segment is a distinctive word. If the launcher
# splits on the space, that word shows up as its own argv element, which is
# what the assertions look for.
#   '<...>/external conf'    split -> "conf/log4j2.xml"
#   '<...>/external logs'    split -> "logs"
#   '<...>/external plugins' split -> "plugins"
# ---------------------------------------------------------------------------
CONF_X="$TMP/external conf"
LOGS_X="$TMP/external logs"
PLUGINS_X="$TMP/external plugins"
mkdir -p "$CONF_X" "$LOGS_X" "$PLUGINS_X"

# ---------------------------------------------------------------------------
# Stub binaries
# ---------------------------------------------------------------------------
STUB="$TMP/stub"
mkdir -p "$STUB/bin"

# The launchers probe the version first ("$JAVA" -version / "${JAVA[@]}" -version)
# and parse a line anchored to '^(java|openjdk) version "'. Answer that probe,
# and on the REAL launch dump argv one element per line and exit without
# starting anything.
cat > "$STUB/bin/java" <<'EOF'
#!/bin/bash
for a in "$@"; do
    if [ "$a" = "-version" ]; then
        echo 'openjdk version "17.0.9" 2023-10-17' >&2
        echo 'OpenJDK Runtime Environment (build 17.0.9+9)' >&2
        echo 'OpenJDK 64-Bit Server VM (build 17.0.9+9, mixed mode)' >&2
        exit 0
    fi
done
: > "${ARGV_DUMP:?ARGV_DUMP must be set}"
for a in "$@"; do printf '%s\n' "$a" >> "$ARGV_DUMP"; done
exit 0
EOF

# Store unconditionally tries to fetch libjemalloc before parsing any flag.
# Failing the download fast keeps the test offline; the launcher treats it as
# optional ("Failed to verify or download ... skip it") and continues.
cat > "$STUB/bin/curl" <<'EOF'
#!/bin/bash
exit 1
EOF
cp "$STUB/bin/curl" "$STUB/bin/wget"
chmod +x "$STUB/bin/java" "$STUB/bin/curl" "$STUB/bin/wget"

# ---------------------------------------------------------------------------
# build_install <dest> <src-bin> <boot-jar-glob-name>
# A minimal but structurally real install tree: the launcher scripts are copied
# VERBATIM from source (that is what is under test); only the surrounding jars
# and config files are synthetic.
# ---------------------------------------------------------------------------
build_install() {
    local dest="$1" src="$2" bootjar="$3"
    mkdir -p "$dest/bin" "$dest/lib" "$dest/ext" "$dest/conf" "$dest/plugins"

    cp "$src"/*.sh "$dest/bin/" 2>/dev/null
    # preload-topling.sh and jvm-module.options ship in every component's bin in
    # a real dist, but live only in the Server source tree here.
    cp "$SERVER_SRC/preload-topling.sh" "$dest/bin/" 2>/dev/null
    cp "$SERVER_SRC/jvm-module.options" "$dest/bin/" 2>/dev/null
    chmod +x "$dest/bin"/*.sh 2>/dev/null

    # find -L needs real files; content is irrelevant to argv assembly.
    : > "$dest/lib/placeholder.jar"
    [ -n "$bootjar" ] && : > "$dest/lib/$bootjar"
    return 0
}

# Spring Boot config for PD/Store. 'verify-path'/'license-path' must be
# ABSOLUTE or reject_relative_conf_path refuses to start when -c is set.
write_app_yml() {
    cat > "$1/application.yml" <<EOF
spring:
  application:
    name: test
license:
  verify-path: $CONF_X/verify-license.json
  license-path: $CONF_X/hugegraph.license
EOF
}

# ---------------------------------------------------------------------------
# argv assertions
# ---------------------------------------------------------------------------
argv_count() { # exact-match element count
    [ -f "$DUMP" ] || { echo 0; return; }
    # grep -c already prints "0" on no match (and exits 1), so its status is
    # deliberately ignored: adding a fallback would emit a second "0".
    local n
    n="$(grep -c -x -F -- "$1" "$DUMP" 2>/dev/null)"
    echo "${n:-0}"
}
argv_has()   { [ "$(argv_count "$1")" -ge 1 ]; }
argv_after() { # echo the element following the first exact match of $1
    [ -f "$DUMP" ] || return 1
    awk -v flag="$1" 'found {print; exit} $0 == flag {found=1}' "$DUMP"
}

# One option must be ONE argv element, with the space intact.
assert_single() { # <expected element> <label>
    local n; n="$(argv_count "$1")"
    if [ "$n" -eq 1 ]; then
        ok "$2"
    else
        bad "$2 (found $n elements exactly matching '$1')"
    fi
}

# The split fragments must not appear at all. This is the direct regression
# assertion for the reported "main class conf.log4j2.xml" failure.
assert_no_fragments() { # <label prefix>
    local frag found=0
    for frag in "conf/log4j2.xml" "logs" "plugins" "conf"; do
        if argv_has "$frag"; then
            bad "$1: stray argv element '$frag' -- a path was split on its space"
            found=1
        fi
    done
    [ "$found" -eq 0 ] && ok "$1: no split fragments in argv"
}

dump_argv_on_failure() {
    echo "----- captured argv ($1) -----"
    cat -n "$DUMP" 2>/dev/null || echo "(no argv captured)"
    echo "------------------------------"
}

###############################################################################
echo "== 0. self-check: the detector must catch the ORIGINAL bug =="
###############################################################################
# A test that can only ever pass proves nothing. Replay the pre-fix shape -- the
# options built into ONE string and expanded unquoted -- and require the
# assertions below to FLAG it. If this section ever reports "clean", the stub or
# the assertions have stopped working and every PASS after it is meaningless.
cat > "$TMP/broken-launcher.sh" <<'EOF'
#!/bin/bash
# Deliberately the OLD, broken form: one string, unquoted expansion.
CONF="$1"; LOGS="$2"
JVM_OPTIONS="-Dlog4j.configurationFile=${CONF}/log4j2.xml -DLOG_PATH=${LOGS}"
exec "$JAVA_HOME/bin/java" ${JVM_OPTIONS} -cp "/tmp/cp" org.example.Main
EOF
chmod +x "$TMP/broken-launcher.sh"

rm -f "$DUMP"
JAVA_HOME="$STUB" ARGV_DUMP="$DUMP" PATH="$STUB/bin:$PATH" \
    bash "$TMP/broken-launcher.sh" "$CONF_X" "$LOGS_X" >/dev/null 2>&1

if [ ! -s "$DUMP" ]; then
    bad "self-check: stub java captured nothing -- the harness itself is broken"
else
    # The split must produce exactly the fragments the review reported.
    if argv_has "conf/log4j2.xml" && argv_has "logs"; then
        ok "self-check: split fragments are detectable ('conf/log4j2.xml', 'logs')"
    else
        bad "self-check: broken launcher did NOT produce detectable fragments"
        dump_argv_on_failure "self-check"
    fi
    # And the correct form must be absent, which is what assert_single asserts.
    if [ "$(argv_count "-Dlog4j.configurationFile=$CONF_X/log4j2.xml")" -eq 0 ]; then
        ok "self-check: assert_single would fail on the broken launcher"
    else
        bad "self-check: assert_single cannot distinguish broken from fixed"
    fi
fi

###############################################################################
echo "== 1. Server: hugegraph-server.sh =="
###############################################################################
# hugegraph-server.sh is the Server's JVM launcher: it builds JVM_OPTIONS /
# JAVA_OPTIONS and execs java. start-hugegraph.sh hands it the overrides through
# the exported *_OVERRIDE variables, which is reproduced here.
SRV="$TMP/server"
build_install "$SRV" "$SERVER_SRC" ""

if [ ! -f "$SRV/bin/hugegraph-server.sh" ]; then
    skip "Server: hugegraph-server.sh not found"
else
    rm -f "$DUMP"
    CONF_OVERRIDE="$CONF_X" LOGS_OVERRIDE="$LOGS_X" PLUGINS_OVERRIDE="$PLUGINS_X" \
    JAVA_HOME="$STUB" ARGV_DUMP="$DUMP" PATH="$STUB/bin:$PATH" \
        bash "$SRV/bin/hugegraph-server.sh" \
            "$CONF_X/gremlin-server.yaml" \
            "$CONF_X/rest-server.properties" \
            false "" "" false >/dev/null 2>&1

    if [ ! -s "$DUMP" ]; then
        bad "Server: launcher never reached java (no argv captured)"
    else
        BEFORE=$FAIL
        assert_single "-Dlog4j.configurationFile=$CONF_X/log4j2.xml" \
            "Server: -Dlog4j.configurationFile is one argv element"
        assert_single "-Dlogging.config=$CONF_X/log4j2.xml" \
            "Server: -Dlogging.config is one argv element"
        assert_single "-DLOG_PATH=$LOGS_X" \
            "Server: -DLOG_PATH is one argv element"
        assert_single "-Dbolt.log.path=$LOGS_X" \
            "Server: -Dbolt.log.path is one argv element"
        assert_single "-Dlogging.path=$LOGS_X" \
            "Server: -Dlogging.path is one argv element"
        assert_no_fragments "Server"

        # the classpath and the two config-file arguments should each stay a SINGLE argument.
        CP_VALUE="$(argv_after "-cp")"
        case "$CP_VALUE" in
            -*|"") bad "Server: -cp is not followed by a single classpath element" ;;
            *)     ok  "Server: -cp value is a single argv element" ;;
        esac
        assert_single "$CONF_X/gremlin-server.yaml" \
            "Server: gremlin-server.yaml config arg is one argv element"
        assert_single "$CONF_X/rest-server.properties" \
            "Server: rest-server.properties config arg is one argv element"

        [ "$FAIL" -gt "$BEFORE" ] && dump_argv_on_failure "Server"
    fi
fi

###############################################################################
echo "== 2. Server: user -j options keep the whitespace-splitting contract =="
###############################################################################
# The fix must NOT quote user-supplied options into one argument: '-j' and
# $JAVA_OPTIONS have always been split on whitespace, and callers rely on it.
if [ -f "$SRV/bin/hugegraph-server.sh" ]; then
    rm -f "$DUMP"
    CONF_OVERRIDE="$CONF_X" LOGS_OVERRIDE="$LOGS_X" PLUGINS_OVERRIDE="$PLUGINS_X" \
    JAVA_HOME="$STUB" ARGV_DUMP="$DUMP" PATH="$STUB/bin:$PATH" \
        bash "$SRV/bin/hugegraph-server.sh" \
            "$CONF_X/gremlin-server.yaml" \
            "$CONF_X/rest-server.properties" \
            false "-Dfoo=1 -Dbar=2" "" false >/dev/null 2>&1

    if [ ! -s "$DUMP" ]; then
        skip "Server: -j contract (no argv captured)"
    else
        if argv_has "-Dfoo=1" && argv_has "-Dbar=2" && ! argv_has "-Dfoo=1 -Dbar=2"; then
            ok "Server: -j options still split on whitespace (contract preserved)"
        else
            bad "Server: -j options not split as before"
            dump_argv_on_failure "Server -j"
        fi
    fi
fi

###############################################################################
echo "== 3. PD: start-hugegraph-pd.sh =="
###############################################################################
PD="$TMP/pd"
build_install "$PD" "$PD_SRC" "hg-pd-service-1.0.0.jar"
write_app_yml "$CONF_X"

if [ ! -f "$PD/bin/start-hugegraph-pd.sh" ]; then
    skip "PD: start-hugegraph-pd.sh not found"
else
    rm -f "$DUMP"
    # -d false -> foreground, so the exec is synchronous and no PID/daemon
    # bookkeeping interferes with the capture.
    JAVA_HOME="$STUB" ARGV_DUMP="$DUMP" PATH="$STUB/bin:$PATH" \
        bash "$PD/bin/start-hugegraph-pd.sh" \
            -d false -c "$CONF_X" -l "$LOGS_X" -o "$PLUGINS_X" >/dev/null 2>&1

    if [ ! -s "$DUMP" ]; then
        bad "PD: launcher never reached java (no argv captured)"
        echo "      hint: check $LOGS_X/hugegraph-pd-stdout.log"
    else
        BEFORE=$FAIL
        assert_single "-Dlog4j.configurationFile=$CONF_X/log4j2.xml" \
            "PD: -Dlog4j.configurationFile is one argv element"
        assert_single "-DLOG_PATH=$LOGS_X" \
            "PD: -DLOG_PATH is one argv element"
        assert_single "-Dspring.config.location=$CONF_X/application.yml" \
            "PD: -Dspring.config.location is one argv element"
        assert_no_fragments "PD"
        [ "$FAIL" -gt "$BEFORE" ] && dump_argv_on_failure "PD"
    fi
fi

###############################################################################
echo "== 4. Store: start-hugegraph-store.sh =="
###############################################################################
ST="$TMP/store"
build_install "$ST" "$STORE_SRC" "hg-store-node-1.0.0.jar"

if [ ! -f "$ST/bin/start-hugegraph-store.sh" ]; then
    skip "Store: start-hugegraph-store.sh not found"
else
    rm -f "$DUMP"
    # Store refuses to start below 1024 file descriptors / user processes, which
    # is under the macOS default. Raise the soft limit for this child only.
    JAVA_HOME="$STUB" ARGV_DUMP="$DUMP" PATH="$STUB/bin:$PATH" \
        bash -c 'ulimit -n 2048 2>/dev/null; ulimit -u 2048 2>/dev/null
                 exec bash "$1" -d false -c "$2" -l "$3" -o "$4"' \
            _ "$ST/bin/start-hugegraph-store.sh" "$CONF_X" "$LOGS_X" "$PLUGINS_X" \
            >/dev/null 2>&1
    RC=$?

    if [ ! -s "$DUMP" ] && [ "$RC" -eq 8 ]; then
        skip "Store: ulimit -n/-u below 1024 on this host, launcher refused to start"
    elif [ ! -s "$DUMP" ]; then
        bad "Store: launcher never reached java (no argv captured, rc=$RC)"
        echo "      hint: check $LOGS_X/hugegraph-store-server.log"
    else
        BEFORE=$FAIL
        assert_single "-Dlog4j.configurationFile=$CONF_X/log4j2.xml" \
            "Store: -Dlog4j.configurationFile is one argv element"
        assert_single "-DLOG_PATH=$LOGS_X" \
            "Store: -DLOG_PATH is one argv element"
        assert_single "-Dspring.config.location=$CONF_X/application.yml" \
            "Store: -Dspring.config.location is one argv element"
        # Store also builds a -Xlog:gc option that embeds $LOGS.
        assert_single "-Xlog:gc=info:file=$LOGS_X/gc.log:time,uptime,level,tags:filecount=3,filesize=100m" \
            "Store: -Xlog:gc file path is one argv element"
        assert_no_fragments "Store"
        [ "$FAIL" -gt "$BEFORE" ] && dump_argv_on_failure "Store"
    fi
fi

###############################################################################
echo "== 5. every argv element is well formed =="
###############################################################################
# A split leaves a bare relative fragment behind. Java reads the first
# non-option argument as the main class, so any element that is neither an
# option, an @argfile, nor an absolute path is the signature of the bug.
if [ -s "$DUMP" ]; then
    STRAY=""
    while IFS= read -r a; do
        case "$a" in
            -*|@*|/*)     ;;                              # option / argfile / absolute
            *.*.*)        ;;                              # fully-qualified main class
            true|false)   ;;                              # boolean positional (-s)
            "")           ;;
            *) STRAY="$STRAY[$a] " ;;
        esac
    done < "$DUMP"
    if [ -z "$STRAY" ]; then
        ok "all argv elements are options, argfiles, absolute paths or class names"
    else
        bad "suspicious bare argv elements: $STRAY"
    fi
else
    skip "argv well-formedness (no argv captured)"
fi

echo
echo "===================================="
echo "PASS=$PASS  FAIL=$FAIL  SKIP=$SKIP"
echo "===================================="
[ "$FAIL" -eq 0 ]
