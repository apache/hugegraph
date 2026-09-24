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
# props.awk — read and rewrite Java ".properties" files with the grammar
# HugeConfig (commons-configuration over JDK Properties) applies, so the
# entrypoint and the server agree on what a mounted file means.  grep/sed
# rewrites do not: they see `\`-escaped keys, `:` separators, continuation
# lines and duplicate definitions differently, which is how a mounted
# config ends up with two definitions of one key.
#
# One invocation, selected with the `PROPS_MODE` environment variable:
#
#   PROPS_MODE=get  PROPS_KEY=K PROPS_FILE=F
#       print the value of K's first logical definition, in the on-disk
#       escaped form; with PROPS_DECODED=1 print it as java.util.Properties
#       would hand it to the server.  Always exits 0.
#   PROPS_MODE=has  PROPS_KEY=K PROPS_FILE=F
#       print nothing; exit 0 when K has any definition at all, empty
#       included, 1 when it has none, 2 on an error
#   PROPS_MODE=set  PROPS_KEY=K PROPS_FILE=F
#       replace K's first definition in place, drop every other
#       definition of K, append one when the file has none.  The new
#       value arrives pre-encoded in PROPS_VALUE_ENCODED (an environment
#       variable, so secrets never appear in `ps` output or in awk's
#       argv), and -v is not used for it so awk cannot mangle its
#       backslash escapes.
#
# Grammar implemented (java.util.Properties line reader + the
# first-definition-wins rule Configuration.getString applies):
#   - physical lines end at \r\n, \n or a bare \r, as in java.util.Properties
#   - '#' / '!' comments and blank lines
#   - '=' / ':' / whitespace separators, where the whitespace Java counts is
#     space, tab and form feed, with whitespace then an optional single '=' or
#     ':' accepted as one separator
#   - continuations: a physical line ending in an odd number of
#     backslashes joins the next line (its leading whitespace stripped)
#   - backslash escapes in keys and values, including \uXXXX
#   - duplicate logical keys resolve to the first definition
#
# Rewrites keep every untouched line byte-for-byte (comments, blank
# lines, unrelated entries), and replace the first definition where it
# stands, so mounted configs stay reviewable in git diffs.
#
# A file that carries commons-configuration's `include` directive is refused in
# every mode, with exit status 2: the directive splices another file into this
# one, so "this key is absent" is a question that cannot be answered from this
# file alone, and writing into it could bury the definition the server reads.
# Rewrites stage through two exclusively created 0600 files (`mktemp`) rather
# than a predictable `<file>.tmp` / `<file>.bak`, which a writer in a mounted
# conf directory could have arranged as a symlink before the entrypoint, running
# as root, opened either of them.

function die(msg) {
    printf "props.awk: %s\n", msg > "/dev/stderr"
    # 2 for an error, so a caller that reads exit status 1 as "the key is not
    # there" (PROPS_MODE=has) cannot mistake an unreadable file for an absent
    # property and append a definition on top of one it failed to read.
    exit 2
}

function hex_digit(c) {
    return index("0123456789abcdef", tolower(c)) - 1
}

# \uXXXX is a UTF-16 code unit in Java.  Values here are effectively
# ISO-8859-1, so codes above 0xFF are kept as their literal escape text
# rather than being mangled through a single-byte sprintf.
function unescape(s,    out, i, n, c, code, j, d, ok) {
    out = ""
    n = length(s)
    for (i = 1; i <= n; i++) {
        c = substr(s, i, 1)
        if (c != "\\") { out = out c; continue }
        if (i == n) break
        i++
        c = substr(s, i, 1)
        if (c == "u" && i + 4 <= n) {
            code = 0
            ok = 1
            for (j = 1; j <= 4; j++) {
                d = hex_digit(substr(s, i + j, 1))
                if (d < 0) { ok = 0; break }
                code = code * 16 + d
            }
            if (ok) {
                i += 4
                if (code <= 255) out = out sprintf("%c", code)
                else out = out substr(s, i - 5, 6)
                continue
            }
        }
        if (c == "t") out = out "\t"
        else if (c == "n") out = out "\n"
        else if (c == "r") out = out "\r"
        else if (c == "f") out = out "\f"
        else out = out c
    }
    return out
}

# A physical line is continued when it ends in an odd number of
# backslashes (an even count escapes itself).
function trailing_backslashes(s,    n, k) {
    n = length(s)
    k = 0
    while (k < n && substr(s, n - k, 1) == "\\") k++
    return k
}

function is_skipped(raw) {
    return raw ~ /^[ \t\f]*([#!]|$)/
}

# Split a logical line into its raw (still-escaped) key and value parts.
# Results land in K_RAW / V_RAW because awk returns one value.
# Java treats form feed as whitespace on both sides of the separator, so
# `auth.authenticator<FF>=...` is one property here too; reading it as part of
# the key name made a valid mounted configuration invisible to the guards.
function split_kv(s,    n, i, c, esc, sep_at, rest) {
    n = length(s)
    esc = 0
    sep_at = 0
    for (i = 1; i <= n; i++) {
        c = substr(s, i, 1)
        if (esc) { esc = 0; continue }
        if (c == "\\") { esc = 1; continue }
        if (c == "=" || c == ":" || c == " " || c == "\t" || c == "\f") { sep_at = i; break }
    }
    if (sep_at == 0) {
        K_RAW = s
        V_RAW = ""
        return
    }
    K_RAW = substr(s, 1, sep_at - 1)
    rest = substr(s, sep_at)
    c = substr(rest, 1, 1)
    if (c == "=" || c == ":") {
        rest = substr(rest, 2)
    } else {
        sub(/^[ \t\f]+/, "", rest)
        c = substr(rest, 1, 1)
        if (c == "=" || c == ":") rest = substr(rest, 2)
    }
    sub(/^[ \t\f]+/, "", rest)
    V_RAW = rest
}

function shquote(s) {
    gsub(/'/, "'\\''", s)
    return "'" s "'"
}

# A private temporary file, created exclusively, beside `file`.
#
# The name has to come from mktemp.  With a fixed `<file>.tmp` anyone able to
# write in a mounted conf directory could leave that name as a symlink to a
# file elsewhere in the container, and neither the shell redirection that
# pre-created it nor awk's own `>` checks for that: both follow it, so the
# entrypoint, running as root by default, would write auth.admin_pa or
# auth.token_secret through the link and into whatever it points at.  An
# exclusive create of an unpredictable name cannot be pre-arranged, and mktemp
# makes the file 0600 whatever the umask says, which is the reason no chmod
# follows it here.
#
# The template is quoted, which is also why no `--` is passed: the argument
# starts at a quote byte, so it can never read as an option.
function make_temp(file, kind,    cmd, path) {
    path = ""
    cmd = "umask 077 && mktemp " shquote(file) "." kind ".XXXXXX"
    if ((cmd | getline path) <= 0 || path == "") {
        close(cmd)
        die("cannot create a private " kind " file beside " file)
    }
    close(cmd)
    return path
}

# java.util.Properties ends a physical line at \r\n, \n or a bare \r, but
# getline splits on \n alone.  A properties file saved with CR-only endings --
# which java.util.Properties writes for a lone `store()` on some platforms, and
# which a mounted config can arrive with -- therefore reached the parser as one
# enormous record: only its first key was ever seen, and rewriting that key
# replaced the whole record and dropped every later entry, including
# auth.authenticator.  So the file is re-scanned for terminators here.
#
# RAW[] keeps the exact bytes of each line and RAWTERM[] its terminator, so a
# rewrite still replays untouched lines byte-for-byte.  A file whose last line
# carries no terminator gets a \n, which is what the replay did before.
function scan_records(s,    i, n, c, start, term, len, cnt) {
    n = length(s)
    cnt = 0
    start = 1
    i = 1
    while (i <= n) {
        c = substr(s, i, 1)
        if (c != "\r" && c != "\n") { i++; continue }
        if (c == "\r" && substr(s, i + 1, 1) == "\n") {
            term = "\r\n"
            len = 2
        } else {
            term = c
            len = 1
        }
        cnt++
        RAW[cnt] = substr(s, start, i - start)
        RAWTERM[cnt] = term
        start = i + len
        i = start
    }
    if (start <= n) {
        cnt++
        RAW[cnt] = substr(s, start)
        RAWTERM[cnt] = ""
    }
    return cnt
}

# Load `file` into per-block arrays: one block per comment/blank line or
# logical entry, spanning exactly the physical lines it occupies.
function props_load(file,    raw, rc, content, nl, stripped, next_raw, start, logical) {
    content = ""
    while ((rc = (getline raw < file)) > 0)
        content = content raw "\n"
    if (rc == -1)
        die("cannot read " file)
    close(file)

    NLINES = scan_records(content)

    NBLOCK = 0
    for (nl = 1; nl <= NLINES; nl++) {
        # RAW[] holds one java.util.Properties physical line with its terminator
        # already removed, so no CR stripping is needed here.
        stripped = RAW[nl]
        if (is_skipped(stripped)) {
            NBLOCK++
            BTYPE[NBLOCK] = "skip"
            BFIRST[NBLOCK] = nl
            BLAST[NBLOCK] = nl
            continue
        }
        start = nl
        logical = stripped
        while (trailing_backslashes(logical) % 2 == 1 && nl < NLINES) {
            logical = substr(logical, 1, length(logical) - 1)
            nl++
            next_raw = RAW[nl]
            sub(/^[ \t\f]+/, "", next_raw)
            logical = logical next_raw
        }
        # java.util.Properties ignores whitespace before the key; strip it
        # so split_kv's separator scan agrees (an indented key used to be
        # read as a key whose name started with a space, and a set then
        # appended a second definition of the real key).
        sub(/^[ \t\f]+/, "", logical)
        split_kv(logical)
        NBLOCK++
        BTYPE[NBLOCK] = "entry"
        BFIRST[NBLOCK] = start
        BLAST[NBLOCK] = nl
        BKEY[NBLOCK] = unescape(K_RAW)
        # `include` is not an ordinary property to the server: commons
        # configuration splices the named file into this one at this point, so
        # auth.authenticator can be defined over there and be invisible from
        # here, and which of the two definitions wins follows the spliced
        # order rather than the order of this file.  Answering that needs the
        # parser the server uses, and answering it wrong is how a mounted
        # config boots with REST open and Gremlin protected.  So a file that
        # uses the directive is refused in every mode, and nothing is written.
        if (BKEY[NBLOCK] == "include")
            die("refusing to read or rewrite " file ": it includes another file (line " start "), which this helper cannot resolve")
        # Values stay in their on-disk escaped form.  get Prop callers feed
        # the result straight back into set, which would corrupt a decoded
        # value by re-writing its backslashes as literals; keys are
        # unescaped because they are matched against plain names.
        BVAL[NBLOCK] = V_RAW
    }
}

function props_set(file, key, enc_val,    tmp, bak, cmd, b, first, ln, msg, nbs, tail) {
    props_load(file)
    # A value whose written form leaves an odd number of backslashes at the end
    # of the physical line turns the line after it into a continuation of that
    # value.  The line has to be judged as the server sees it: commons
    # configuration trims the line before it looks for the continuation, so
    # `abc\ ` -- the spelling encode_prop_value used to give a trailing space --
    # reaches the server as `abc\` and swallows whatever follows it.  Measured
    # against commons-configuration2 (what HugeConfig extends), the same input
    # read back yields no property at all, so a secret written this way never
    # reaches the server that is supposed to authenticate with it.  The
    # entrypoint has to refuse instead of guessing a target.
    tail = enc_val
    sub(/[ \t\f\r]+$/, "", tail)
    nbs = 0
    while (nbs < length(tail) && substr(tail, length(tail) - nbs, 1) == "\\")
        nbs++
    if (nbs % 2 == 1)
        die("refusing to write " key ": trimmed of its trailing blanks the value ends in a backslash, which would swallow the next line")
    first = 0
    for (b = 1; b <= NBLOCK; b++) {
        if (BTYPE[b] == "entry" && BKEY[b] == key) {
            if (first == 0) first = b
            else BDROP[b] = 1
        }
    }
    # Staged rewrite: everything lands in a private temp file first, so a
    # failure before the copy-back leaves the original untouched.  The temp file
    # holds secrets, so it is created 0600 and exclusively (see make_temp): a
    # reused, predictable name is both a disclosure risk under the process umask
    # and a path someone else can have arranged already.
    tmp = make_temp(file, "tmp")
    for (b = 1; b <= NBLOCK; b++) {
        if (BDROP[b]) continue
        if (b == first) {
            printf "%s=%s\n", key, enc_val > tmp
        } else {
            for (ln = BFIRST[b]; ln <= BLAST[b]; ln++) {
                # Replay the line with the terminator it was read with, so a
                # CRLF or CR-only config keeps its endings on lines the
                # rewrite does not touch.
                msg = RAWTERM[ln]
                if (msg == "") msg = "\n"
                printf "%s%s", RAW[ln], msg > tmp
            }
        }
    }
    if (first == 0)
        printf "%s=%s\n", key, enc_val > tmp
    close(tmp)
    # Copy the completed temp file back onto the original instead of
    # renaming it: a rename replaces the inode, which would lose the
    # file's permissions (a 0600 config holding secrets would come back
    # umask-world-readable), turn a symlinked config into a regular file,
    # and fail with EBUSY on a config bind-mounted as a single file — the
    # mounted case this path exists for.  The copy keeps the inode, mode,
    # symlink and mount point.
    #
    # The copy itself is not atomic and the shell's `>` truncates the
    # destination before cat writes a byte, so an ENOSPC or I/O error
    # mid-copy used to leave a truncated config on disk — a truncated
    # rest-server.properties loses `auth.authenticator` and boots the
    # server with authentication off.  Snapshot the original first, into a
    # second exclusively created 0600 file for the same reason as the temp,
    # and put it back when the copy fails.
    bak = make_temp(file, "bak")
    cmd = "cp -- " shquote(file) " " shquote(bak)
    if (system(cmd) != 0)
        die("cannot back up " file " before the copy-back")
    cmd = "cat -- " shquote(tmp) " > " shquote(file)
    if (system(cmd) != 0) {
        # Best effort: the destination is already damaged, so restoring it
        # from the snapshot comes first, and the temp file is kept for an
        # operator who wants to inspect what was being written.
        msg = "cannot copy " tmp " over " file
        cmd = "cat -- " shquote(bak) " > " shquote(file)
        if (system(cmd) == 0) die(msg "; the previous content is restored")
        die(msg "; " file " is damaged, previous content is in " bak)
    }
    if (system("rm -f -- " shquote(tmp) " " shquote(bak)) != 0)
        die("cannot remove " tmp " and " bak " after the copy-back")
}

function props_get(file, key, decoded,    b) {
    props_load(file)
    for (b = 1; b <= NBLOCK; b++) {
        if (BTYPE[b] == "entry" && BKEY[b] == key) {
            if (decoded) print unescape(BVAL[b])
            else print BVAL[b]
            return
        }
    }
    # Absence prints nothing and is NOT an exit status: callers assign from
    # command substitution (`rest=$(get_prop ...)`) under a shell with errexit
    # on, where a nonzero status would abort the entrypoint over a merely
    # missing property.  PROPS_MODE=has is the mode that reports by status.
}

# Exit status only: 0 when the key has any definition at all, including an
# empty one.  Guards that append a default must not treat `auth.authenticator=`
# as absent, because appending a second definition leaves the empty first one
# in force under first-definition-wins.
function props_has(file, key,    b) {
    props_load(file)
    for (b = 1; b <= NBLOCK; b++) {
        if (BTYPE[b] == "entry" && BKEY[b] == key) return 0
    }
    return 1
}

BEGIN {
    mode = ENVIRON["PROPS_MODE"]
    key = ENVIRON["PROPS_KEY"]
    file = ENVIRON["PROPS_FILE"]
    decoded = (ENVIRON["PROPS_DECODED"] == "1")
    if (file == "" || key == "")
        die("PROPS_FILE and PROPS_KEY must be set")
    if (mode == "get") {
        props_get(file, key, decoded)
    } else if (mode == "has") {
        if (props_has(file, key)) exit 1
    } else if (mode == "set") {
        props_set(file, key, ENVIRON["PROPS_VALUE_ENCODED"])
    } else {
        die("PROPS_MODE must be get, has or set")
    }
}
