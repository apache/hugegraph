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
#      authentication off, which is the nameless case that must be refused;
#   6. a mapping is read to its end before it is answered, because the server
#      sees the whole node: a direct `authenticator` defined twice is refused
#      rather than settled by whoever met it first;
#   7. YAML ends a line at CR, LF or CRLF, so a CR that has survived the
#      comment being stripped is line noise, not part of a key or a value.
#      Reading `authentication:\r` as no key at all reported a Gremlin mapping
#      that names a class as absent, which is the direction that leaves REST
#      open while Gremlin authenticates.
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

# An authenticator entry only counts when it actually names a class, and the
# answer has to be what snakeyaml hands the server rather than what the bytes
# look like.  The unsafe direction is `named` for a config that leaves Gremlin
# on AllowAllAuthenticator while REST enforces, so anything this scanner cannot
# resolve to a class is refused instead of guessed at:
#
#   - a plain scalar that resolves to null in any spelling, and YAML resolves
#     null case-insensitively (null, Null, NULL, nUll) as well as to ~, names
#     no class;
#   - a leading `!` makes the tag, not the text, decide the type: !!null is the
#     explicit spelling of empty and every other tag is a type not resolvable
#     here, so neither counts;
#   - a quoted scalar is a string and never null, but `""` and the empty single
#     quoted form are the empty string, and loadAuthenticator("") returns null,
#     which is the same no-authenticator state;
#   - `&label value` is an anchor: the label is not part of the value, so the
#     text after it decides, and `&label` alone anchors an empty node, which is
#     the explicit spelling of null;
#   - `*label` is an alias whose class lives in another node.  This scanner
#     does not resolve nodes, so an alias is refused rather than read as a
#     class name -- `authenticator: &noAuth null` is a valid document whose
#     value is null, and calling it named is the exact mistake this guards.
#   - an unterminated quote is not a scalar at all.
function names_class(v,    first, last, body, rest) {
    v = trim(v)
    if (v == "") return 0
    first = substr(v, 1, 1)
    if (first == "!") return 0
    if (first == "*") return 0
    if (first == "&") {
        rest = trim(substr(v, 2))
        sub(/^[^ \t]*/, "", rest)
        return names_class(trim(rest))
    }
    if (first == apos() || first == dquo()) {
        if (length(v) < 2) return 0
        last = substr(v, length(v), 1)
        if (last != first) return 0
        body = trim(substr(v, 2, length(v) - 2))
        return body != ""
    }
    if (v == "~") return 0
    if (tolower(v) == "null") return 0
    return 1
}

# The answer for the mapping read so far, for both the block and the flow form.
# AUTH_SEEN counts direct `authenticator` children and AUTH_NAMED remembers
# whether the last one named a class.  A key defined twice has no answer this
# scanner can give honestly: snakeyaml either keeps the last value or, with
# unique keys enforced, rejects the document and the server never starts.
# Either way the operator has to be told which line to fix, so the duplicate is
# reported on stderr and the mapping is refused through the nameless state,
# which check_auth_sides stops the boot on and enable-auth.sh will not append
# beside.
function auth_state(    msg) {
    if (AUTH_SEEN > 1) {
        msg = "yamlscan.awk: a mapping with " AUTH_SEEN " direct authenticator entries"
        print msg > "/dev/stderr"
        print "cannot be answered here: the server takes the last one, or rejects the file." > "/dev/stderr"
        print "Remove the duplicate authenticator entry from gremlin-server.yaml." > "/dev/stderr"
        return "nameless"
    }
    if (AUTH_SEEN == 1 && AUTH_NAMED) return "named"
    return "nameless"
}

# Report and stop.  Output happens in END only, because awk runs END after
# `exit` and a second print there would emit two states on one run.
function finish(r) { RESULT = r; exit }

# Feed one line of a flow collection to the brace scanner.  DEPTH counts open
# collections; keys and values are only read at depth one, which is what makes
# a nested mapping under `config` invisible to it.  A direct authenticator seen
# at depth one is recorded for auth_state.  Returns 1 once the outermost
# collection has closed.
function scan_flow(s,    i, n, c, q, esc) {
    n = length(s)
    q = ""
    esc = 0
    for (i = 1; i <= n; i++) {
        c = substr(s, i, 1)
        if (q != "") {
            # Every byte inside the quotes belongs to the scalar, delimiters
            # included; unquote and names_class take the quotes off.  Dropping
            # the value here is what made {authenticator: "org.A"} read as
            # nameless and refuse a valid mounted config.  A backslash escapes
            # the next byte in a double quoted scalar only -- in a single
            # quoted one the way out is a doubled quote, which this loop
            # already gets right because the first one closes and the next
            # reopens, and the pair still counts as content.
            if (FST == "key") CUR = CUR c
            else if (FST == "val") CUR_VAL = CUR_VAL c
            if (esc) esc = 0
            else if (q == dquo() && c == "\\") esc = 1
            else if (c == q) q = ""
            continue
        }
        if (is_quote(c)) {
            q = c
            if (FST == "key") CUR = CUR c
            else if (FST == "val") CUR_VAL = CUR_VAL c
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
        if (c == "\r") continue
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
    if (names_authenticator(k)) {
        AUTH_SEEN++
        AUTH_NAMED = names_class(CUR_VAL)
    }
}

BEGIN {
    DEPTH = 0
    FST = "key"
    CUR = ""
    CUR_KEY = ""
    CUR_VAL = ""
    AUTH_SEEN = 0
    AUTH_NAMED = 0
    found = 0
    child = -1
    flow = 0
    RESULT = ""
}

{
    # A CR here is the terminator of a CRLF line, not content: YAML ends a line
    # at either byte, so `authentication:\r` is the key line and leaving the CR
    # on it made split_pair see no colon followed by end of line, which reported
    # a whole mapping as absent.
    line = strip_comment($0)
    sub(/[ \t\r]+$/, "", line)

    if (!found) {
        if (line ~ /^[ \t]/) next
        if (!split_pair(line)) next
        if (unquote(K_TXT) != "authentication") next
        found = 1
        if (substr(V_TXT, 1, 1) == "{") {
            flow = 1
            if (scan_flow(V_TXT)) finish(auth_state())
            next
        }
        # Anything else on the key line -- a scalar, a sequence, nothing -- is
        # not a mapping that names a class.  Reading `authentication: some.Name`
        # as named would accept a config the server cannot use.
        next
    }

    if (flow) {
        if (scan_flow(line)) finish(auth_state())
        next
    }

    if (trim(line) == "") next
    # A column-0 line after the comment was stripped is a sibling key, so the
    # mapping has ended and what was recorded while reading it is the answer.
    if (indent_of(line) == 0) finish(auth_state())

    if (!split_pair(line)) next
    if (child < 0) child = indent_of(line)
    if (indent_of(line) != child) next
    if (names_authenticator(K_TXT)) {
        AUTH_SEEN++
        AUTH_NAMED = names_class(V_TXT)
    }
}

END {
    if (RESULT == "") {
        if (!found) RESULT = "none"
        else if (flow) RESULT = "nameless"
        else RESULT = auth_state()
    }
    print RESULT
}
