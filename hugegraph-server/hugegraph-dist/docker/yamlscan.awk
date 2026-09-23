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
# yamlscan.awk -- does the top-level `authentication` mapping of a Gremlin
# server YAML file name an authenticator?  Prints exactly one of:
#
#   none      there is no top-level authentication mapping
#   nameless  the mapping exists but names no authenticator class
#   named     the mapping names an authenticator class
#
# The entrypoint asks this one question to decide whether
# rest-server.properties and gremlin-server.yaml configure authentication
# together.  Getting it wrong toward "named" is how REST ends up enforcing
# StandardAuthenticator while Gremlin silently falls back to TinkerPop
# AllowAllAuthenticator, so the answer has to follow the same structure
# snakeyaml hands to the server, within the subset of YAML that shipped and
# mounted configs use:
#
#   1. the mapping must start at column 0 -- an `authentication:` nested under
#      some other key belongs to that feature, not to the Gremlin server;
#   2. only a direct child `authenticator` counts -- a class reached through
#      `authentication.config`, or through any other nested mapping, is not
#      the server authenticator, because TinkerPop keeps `config` as its own
#      map;
#   3. `#` outside quotes starts a comment: text behind one is not content,
#      and a comment-only line is neither a child nor the end of the mapping;
#   4. in a flow mapping the key must sit at depth one between the braces, so
#      `{authenticator: X}` names a class while `{config: {authenticator: X}}`
#      does not;
#   5. a direct `authenticator` whose value is empty, `null` or `~` names no
#      class -- the server reads the key, gets nothing and leaves
#      authentication off, which is the nameless case that must be refused.
#
# Quote characters come from sprintf so this file holds no literal apostrophe:
# an awk program written into a single-quoted shell string breaks on one, and
# that has cost this repo twice already.

function apos() { return sprintf("%c", 39) }
function dquo() { return sprintf("%c", 34) }

function ltrim(s) { sub(/^[ \t]+/, "", s); return s }
function rtrim(s) { sub(/[ \t]+$/, "", s); return s }
function trim(s) { return rtrim(ltrim(s)) }

function is_quote(c) { return c == apos() || c == dquo() }

# Remove an unquoted trailing comment together with the whitespace that has to
# precede the `#` for it to be a comment rather than part of a scalar.
function strip_comment(s,    i, n, c, q, prev) {
    q = ""
    prev = ""
    n = length(s)
    for (i = 1; i <= n; i++) {
        c = substr(s, i, 1)
        if (q != "") {
            if (c == q) q = ""
        } else if (is_quote(c)) {
            q = c
        } else if (c == "#" && (prev == "" || prev == " " || prev == "\t")) {
            return rtrim(substr(s, 1, i - 1))
        }
        prev = c
    }
    return s
}

# How many whitespace characters open the line, i.e. its block nesting level.
function indent_of(s,    i, n, c) {
    n = length(s)
    i = 1
    while (i <= n) {
        c = substr(s, i, 1)
        if (c != " " && c != "\t") break
        i++
    }
    return i - 1
}

# One layer of matching quotes off a key or scalar.
function unquote(s,    f) {
    s = trim(s)
    if (length(s) >= 2) {
        f = substr(s, 1, 1)
        if ((f == apos() || f == dquo()) && substr(s, length(s), 1) == f)
            return substr(s, 2, length(s) - 2)
    }
    return s
}

# Split `name: value` at the first colon outside quotes that is followed by end
# of line or a space, which is what makes a colon inside `http://host` part of
# the scalar.  Results go to K_TXT / V_TXT because awk returns one value.
function split_pair(s,    i, n, c, q) {
    q = ""
    n = length(s)
    for (i = 1; i <= n; i++) {
        c = substr(s, i, 1)
        if (q != "") {
            if (c == q) q = ""
            continue
        }
        if (is_quote(c)) { q = c; continue }
        if (c != ":") continue
        if (i == n || substr(s, i + 1, 1) ~ /^[ \t]/) {
            K_TXT = rtrim(substr(s, 1, i - 1))
            V_TXT = ltrim(substr(s, i + 1))
            return 1
        }
    }
    return 0
}

function names_authenticator(k) { return unquote(k) == "authenticator" }

# An authenticator entry only counts when it actually names a class.
function names_class(v) {
    v = trim(v)
    return v != "" && v != "null" && v != "~"
}

# Report and stop.  Output happens in END only, because awk runs END after
# `exit` and a second print there would emit two states on one run.
function finish(r) { RESULT = r; exit }

# Feed one line of a flow collection to the brace scanner.  DEPTH counts open
# collections; keys and values are only read at depth one, which is what makes
# a nested mapping under `config` invisible to it.  FSET records a direct
# authenticator that names a class.  Returns 1 once the outermost collection
# has closed.
function scan_flow(s,    i, n, c, q) {
    n = length(s)
    q = ""
    for (i = 1; i <= n; i++) {
        c = substr(s, i, 1)
        if (q != "") {
            if (FST == "key") CUR = CUR c
            if (c == q) q = ""
            continue
        }
        if (is_quote(c)) {
            q = c
            if (FST == "key") CUR = CUR c
            continue
        }
        if (c == "{" || c == "[") {
            DEPTH++
            CUR = ""
            # Past depth one the whole entry is nested content and is skipped,
            # including an authenticator key inside it.
            FST = (DEPTH == 1 ? "key" : "skip")
            continue
        }
        if (c == "}" || c == "]") {
            if (DEPTH == 1 && FST == "val") commit_val()
            DEPTH--
            CUR = ""
            if (DEPTH == 0) { FST = "key"; return 1 }
            FST = "skip"
            continue
        }
        if (DEPTH != 1) continue
        if (c == ":") {
            if (FST == "key") {
                CUR_KEY = CUR
                CUR_VAL = ""
                FST = "val"
            }
            CUR = ""
            continue
        }
        if (c == ",") {
            if (FST == "val") commit_val()
            FST = "key"
            CUR = ""
            continue
        }
        if (c == " " || c == "\t") {
            # A space ends an unquoted key but never carries a value byte.
            continue
        }
        if (FST == "key") CUR = CUR c
        else if (FST == "val") CUR_VAL = CUR_VAL c
    }
    return 0
}

# Close out the depth-one entry that was being read when a `,` or `}` arrived.
function commit_val(    k) {
    k = CUR_KEY
    if (names_authenticator(k) && names_class(CUR_VAL)) FSET = 1
}

BEGIN {
    DEPTH = 0
    FST = "key"
    CUR = ""
    CUR_KEY = ""
    CUR_VAL = ""
    FSET = 0
    found = 0
    child = -1
    flow = 0
    RESULT = ""
}

{
    line = strip_comment($0)

    if (!found) {
        if (line ~ /^[ \t]/) next
        if (!split_pair(line)) next
        if (unquote(K_TXT) != "authentication") next
        found = 1
        if (substr(V_TXT, 1, 1) == "{") {
            flow = 1
            if (scan_flow(V_TXT)) finish(FSET ? "named" : "nameless")
            next
        }
        # Anything else on the key line -- a scalar, a sequence, nothing -- is
        # not a mapping that names a class.  Reading `authentication: some.Name`
        # as named would accept a config the server cannot use.
        next
    }

    if (flow) {
        if (scan_flow(line)) finish(FSET ? "named" : "nameless")
        next
    }

    if (trim(line) == "") next
    # A column-0 line after the comment was stripped is a sibling key, so the
    # mapping has ended.
    if (indent_of(line) == 0) finish("nameless")

    if (!split_pair(line)) next
    if (child < 0) child = indent_of(line)
    if (indent_of(line) != child) next
    if (names_authenticator(K_TXT) && names_class(V_TXT)) finish("named")
}

END {
    if (RESULT != "") { print RESULT; exit }
    if (!found) print "none"
    else print "nameless"
}
