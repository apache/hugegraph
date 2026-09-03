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
# One invocation, selected with the `mode` environment variable:
#
#   mode=get  key=K file=F
#       print the value of K's first logical definition
#   mode=set  key=K file=F
#       replace K's first definition in place, drop every other
#       definition of K, append one when the file has none.  The new
#       value arrives pre-encoded in PROP_VALUE_ENCODED (an environment
#       variable, so secrets never appear in `ps` output or in awk's
#       argv), and -v is not used for it so awk cannot mangle its
#       backslash escapes.
#
# Grammar implemented (java.util.Properties line reader + the
# first-definition-wins rule Configuration.getString applies):
#   - '#' / '!' comments and blank lines
#   - '=' / ':' / whitespace separators, with whitespace then an optional
#     single '=' or ':' accepted as one separator
#   - continuations: a physical line ending in an odd number of
#     backslashes joins the next line (its leading whitespace stripped)
#   - backslash escapes in keys and values, including \uXXXX
#   - duplicate logical keys resolve to the first definition
#
# Rewrites keep every untouched line byte-for-byte (comments, blank
# lines, unrelated entries), and replace the first definition where it
# stands, so mounted configs stay reviewable in git diffs.

function die(msg) {
    printf "props.awk: %s\n", msg > "/dev/stderr"
    exit 1
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
    return raw ~ /^[ \t]*([#!]|$)/
}

# Split a logical line into its raw (still-escaped) key and value parts.
# Results land in K_RAW / V_RAW because awk returns one value.
function split_kv(s,    n, i, c, esc, sep_at, rest) {
    n = length(s)
    esc = 0
    sep_at = 0
    for (i = 1; i <= n; i++) {
        c = substr(s, i, 1)
        if (esc) { esc = 0; continue }
        if (c == "\\") { esc = 1; continue }
        if (c == "=" || c == ":" || c == " " || c == "\t") { sep_at = i; break }
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
        sub(/^[ \t]+/, "", rest)
        c = substr(rest, 1, 1)
        if (c == "=" || c == ":") rest = substr(rest, 2)
    }
    sub(/^[ \t]+/, "", rest)
    V_RAW = rest
}

# Load `file` into per-block arrays: one block per comment/blank line or
# logical entry, spanning exactly the physical lines it occupies.
function props_load(file,    raw, nl, next_raw, start, logical) {
    NLINES = 0
    while ((getline raw < file) > 0) {
        NLINES++
        RAW[NLINES] = raw
    }
    close(file)

    NBLOCK = 0
    for (nl = 1; nl <= NLINES; nl++) {
        raw = RAW[nl]
        if (is_skipped(raw)) {
            NBLOCK++
            BTYPE[NBLOCK] = "skip"
            BFIRST[NBLOCK] = nl
            BLAST[NBLOCK] = nl
            continue
        }
        start = nl
        logical = raw
        while (trailing_backslashes(logical) % 2 == 1 && nl < NLINES) {
            logical = substr(logical, 1, length(logical) - 1)
            nl++
            next_raw = RAW[nl]
            sub(/^[ \t]+/, "", next_raw)
            logical = logical next_raw
        }
        split_kv(logical)
        NBLOCK++
        BTYPE[NBLOCK] = "entry"
        BFIRST[NBLOCK] = start
        BLAST[NBLOCK] = nl
        BKEY[NBLOCK] = unescape(K_RAW)
        # Values stay in their on-disk escaped form.  get Prop callers feed
        # the result straight back into set, which would corrupt a decoded
        # value by re-writing its backslashes as literals; keys are
        # unescaped because they are matched against plain names.
        BVAL[NBLOCK] = V_RAW
    }
}

function props_set(file, key, enc_val,    b, first, ln) {
    props_load(file)
    first = 0
    for (b = 1; b <= NBLOCK; b++) {
        if (BTYPE[b] == "entry" && BKEY[b] == key) {
            if (first == 0) first = b
            else BDROP[b] = 1
        }
    }
    for (b = 1; b <= NBLOCK; b++) {
        if (BDROP[b]) continue
        if (b == first) {
            printf "%s=%s\n", key, enc_val > file
        } else {
            for (ln = BFIRST[b]; ln <= BLAST[b]; ln++)
                print RAW[ln] > file
        }
    }
    if (first == 0)
        printf "%s=%s\n", key, enc_val > file
    close(file)
}

function props_get(file, key,    b) {
    props_load(file)
    for (b = 1; b <= NBLOCK; b++) {
        if (BTYPE[b] == "entry" && BKEY[b] == key) {
            print BVAL[b]
            return
        }
    }
}

BEGIN {
    mode = ENVIRON["PROPS_MODE"]
    key = ENVIRON["PROPS_KEY"]
    file = ENVIRON["PROPS_FILE"]
    if (file == "" || key == "")
        die("PROPS_FILE and PROPS_KEY must be set")
    if (mode == "get") {
        props_get(file, key)
    } else if (mode == "set") {
        props_set(file, key, ENVIRON["PROPS_VALUE_ENCODED"])
    } else {
        die("PROPS_MODE must be get or set")
    }
}
