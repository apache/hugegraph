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
# Local unit tests for canonicalize_dir / canonicalize_file in util.sh.
# No cluster, Java, or CM needed. Run: bash test-canonicalize.sh

set -u

UTIL="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/util.sh"

# We only want the helpers; source util.sh in a guarded way. util.sh has no
# top-level side effects for these functions, so a plain source is fine.
# shellcheck disable=SC1090
. "$UTIL"

PASS=0
FAIL=0
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

ok()   { echo "PASS: $1"; PASS=$((PASS+1)); }
bad()  { echo "FAIL: $1"; FAIL=$((FAIL+1)); }

# Run a function in a subshell so its `exit 1` paths don't kill the harness.
run_capture() { # usage: run_capture FUNC ARG ; sets OUT, RC
    OUT="$("$@" 2>/tmp/_cz_err)"; RC=$?
    ERR="$(cat /tmp/_cz_err)"
}

echo "== canonicalize_dir =="

# 1. empty input -> empty output, rc 0
run_capture canonicalize_dir ""
[ $RC -eq 0 ] && [ -z "$OUT" ] && ok "dir: empty input returns empty, rc=0" \
    || bad "dir: empty input (rc=$RC out='$OUT')"

# 2. absolute existing dir -> same absolute path
run_capture canonicalize_dir "$TMP"
[ "$OUT" = "$TMP" ] && ok "dir: absolute existing path echoes itself" \
    || bad "dir: absolute path (out='$OUT' want='$TMP')"

# 3. relative dir -> absolute, and dir is created
( cd "$TMP" && run_capture canonicalize_dir "sub/rel" \
    && [ "$OUT" = "$TMP/sub/rel" ] && [ -d "$TMP/sub/rel" ] ) \
    && ok "dir: relative path canonicalized + created" \
    || bad "dir: relative path (out='$OUT')"

# 4. nested non-existent absolute -> created + echoed
run_capture canonicalize_dir "$TMP/a/b/c"
[ "$OUT" = "$TMP/a/b/c" ] && [ -d "$TMP/a/b/c" ] \
    && ok "dir: nested absolute created" || bad "dir: nested (out='$OUT')"

# 5. unresolvable (parent not writable) -> error + exit 1
RO="$TMP/ro"; mkdir -p "$RO"; chmod 500 "$RO"
run_capture canonicalize_dir "$RO/child"
if [ "$(id -u)" -eq 0 ]; then
    echo "SKIP: dir unwritable test (running as root bypasses perms)"
else
    [ $RC -eq 1 ] && echo "$ERR" | grep -q "cannot resolve path" \
        && ok "dir: unwritable parent -> exit 1 + error msg" \
        || bad "dir: unwritable parent (rc=$RC err='$ERR')"
fi
chmod 700 "$RO"

echo "== canonicalize_file =="

# 6. empty input -> empty, rc 0
run_capture canonicalize_file ""
[ $RC -eq 0 ] && [ -z "$OUT" ] && ok "file: empty input returns empty, rc=0" \
    || bad "file: empty input (rc=$RC out='$OUT')"

# 7. absolute file path -> abs parent + filename, parent created, file NOT created
run_capture canonicalize_file "$TMP/pids/server.pid"
[ "$OUT" = "$TMP/pids/server.pid" ] && [ -d "$TMP/pids" ] && [ ! -e "$TMP/pids/server.pid" ] \
    && ok "file: abs path resolves, parent created, file not created" \
    || bad "file: abs path (out='$OUT')"

# 8. relative file path -> absolute
( cd "$TMP" && run_capture canonicalize_file "relpids/x.pid" \
    && [ "$OUT" = "$TMP/relpids/x.pid" ] && [ -d "$TMP/relpids" ] ) \
    && ok "file: relative path canonicalized" || bad "file: relative (out='$OUT')"

# 9. unresolvable parent -> error + exit 1
# Use a NEVER-created subpath under the read-only dir so mkdir -p must fail.
chmod 500 "$RO"
run_capture canonicalize_file "$RO/fresh_sub/x.pid"
if [ "$(id -u)" -eq 0 ]; then
    echo "SKIP: file unwritable test (running as root)"
else
    [ $RC -eq 1 ] && echo "$ERR" | grep -q "cannot resolve path" \
        && ok "file: unwritable parent -> exit 1 + error msg" \
        || bad "file: unwritable parent (rc=$RC err='$ERR')"
fi
chmod 700 "$RO"

echo "== canonicalize_file nocreate (stop scripts) =="

# 10. existing parent -> absolute path, nothing created
mkdir -p "$TMP/existing"
run_capture canonicalize_file "$TMP/existing/s.pid" nocreate
[ $RC -eq 0 ] && [ "$OUT" = "$TMP/existing/s.pid" ] && [ ! -e "$TMP/existing/s.pid" ] \
    && ok "nocreate: existing parent resolves, file not created" \
    || bad "nocreate: existing parent (rc=$RC out='$OUT')"

# 11. relative path with existing parent -> absolute
( cd "$TMP/existing" && run_capture canonicalize_file "r.pid" nocreate \
    && [ "$OUT" = "$TMP/existing/r.pid" ] ) \
    && ok "nocreate: relative path resolves" || bad "nocreate: relative (out='$OUT')"

# 12. missing parent -> fails with the same error, and the parent is NOT created
run_capture canonicalize_file "$TMP/missing/deeper/s.pid" nocreate
[ $RC -eq 1 ] && echo "$ERR" | grep -q "cannot resolve path" && [ ! -e "$TMP/missing" ] \
    && ok "nocreate: missing parent -> exit 1 + error, nothing created" \
    || bad "nocreate: missing parent (rc=$RC err='$ERR' created=$([ -e "$TMP/missing" ] && echo yes || echo no))"

# 13. default mode still creates the missing parent (start scripts unchanged)
run_capture canonicalize_file "$TMP/startcreates/s.pid"
[ $RC -eq 0 ] && [ -d "$TMP/startcreates" ] \
    && ok "default: missing parent is still created" \
    || bad "default: parent not created (rc=$RC)"

# 14. empty input stays a no-op in nocreate mode
run_capture canonicalize_file "" nocreate
[ $RC -eq 0 ] && [ -z "$OUT" ] && ok "nocreate: empty input returns empty, rc=0" \
    || bad "nocreate: empty input (rc=$RC out='$OUT')"

echo "-----------------------------"
echo "PASS=$PASS FAIL=$FAIL"
[ $FAIL -eq 0 ]
