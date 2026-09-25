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
#   1. the mapping must sit at the indentation of the document root -- an
#      `authentication:` nested under some other key belongs to that feature,
#      not to the Gremlin server, while a root mapping written indented below a
#      document marker still counts because the server reads it as the root;
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
#   6b. the file is read to its end, not to the first mapping, because two
#      top-level `authentication` mappings resolve to the last one (or are
#      rejected outright).  Answering from the first reported `named` for a
#      server left on AllowAllAuthenticator by the empty second mapping, so a
#      duplicate root mapping is refused rather than guessed at;
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

# Hex without strtonum, which is not POSIX awk.
function hexval(h,    i, n, c, v) {
    v = 0
    n = length(h)
    for (i = 1; i <= n; i++) {
        c = substr(h, i, 1)
        if (c >= "0" && c <= "9") v = v * 16 + (c - 0)
        else if (c == "a" || c == "A") v = v * 16 + 10
        else if (c == "b" || c == "B") v = v * 16 + 11
        else if (c == "c" || c == "C") v = v * 16 + 12
        else if (c == "d" || c == "D") v = v * 16 + 13
        else if (c == "e" || c == "E") v = v * 16 + 14
        else if (c == "f" || c == "F") v = v * 16 + 15
        else return -1
    }
    return v
}

# Resolve the escapes a double quoted scalar carries, which snakeyaml does
# before the text ever becomes a key.  `"\u0061uthentication"` is the
# authentication key, and `authentic\u0061tion` is the same key spelled out.
# An escape this cannot resolve sets UNRESOLVED instead of being skipped: being
# wrong about a key toward "absent" is what leaves REST open beside an
# authenticating Gremlin, so an unresolved form has to be refused.
function unescape(s,    out, i, n, c, h, k, v) {
    n = length(s)
    out = ""
    i = 1
    while (i <= n) {
        c = substr(s, i, 1)
        if (c != "\\") { out = out c; i++; continue }
        i++
        if (i > n) { UNRESOLVED = 1; return s }
        c = substr(s, i, 1)
        if (c == "u" || c == "U") k = (c == "u" ? 4 : 8)
        else if (c == "x") k = 2
        else k = 0
        if (k > 0) {
            h = substr(s, i + 1, k)
            v = (length(h) == k ? hexval(h) : -1)
            if (v < 0) { UNRESOLVED = 1; return s }
            out = out sprintf("%c", v)
            i = i + k + 1
            continue
        }
        if (c == "0") { out = out sprintf("%c", 0); i++; continue }
        if (c == "a") { out = out sprintf("%c", 7); i++; continue }
        if (c == "e") { out = out sprintf("%c", 27); i++; continue }
        if (c == "N") { out = out sprintf("%c", 133); i++; continue }
        if (c == "L") { out = out sprintf("%c", 8232); i++; continue }
        if (c == "P") { out = out sprintf("%c", 8233); i++; continue }
        if (c == "b") { out = out "\b"; i++; continue }
        if (c == "t") { out = out "\t"; i++; continue }
        if (c == "n") { out = out "\n"; i++; continue }
        if (c == "v") { out = out "\v"; i++; continue }
        if (c == "f") { out = out "\f"; i++; continue }
        if (c == "r") { out = out "\r"; i++; continue }
        if (c == " " || c == dquo() || c == "\\" || c == "/") {
            out = out c
            i++
            continue
        }
        UNRESOLVED = 1
        return s
    }
    return out
}

# One layer of matching quotes off a key or scalar.
function unquote(s,    f, body) {
    s = trim(s)
    if (length(s) >= 2) {
        f = substr(s, 1, 1)
        if ((f == apos() || f == dquo()) && substr(s, length(s), 1) == f) {
            body = substr(s, 2, length(s) - 2)
            return (f == dquo() ? unescape(body) : body)
        }
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

# The answer when the file carries more than one top-level `authentication`
# mapping.  SnakeYAML either takes the last one or, with unique keys enforced,
# rejects the document and the server never starts -- either way the effective
# mapping is not the first the scanner met, so this cannot be answered here.
# Report it on stderr and refuse through the nameless state, exactly like the
# duplicate-authenticator case above, so check_auth_sides stops the boot.
function duplicate_root(    msg) {
    msg = "yamlscan.awk: " AUTH_BLOCKS " top-level authentication mappings"
    print msg > "/dev/stderr"
    print "cannot be answered here: the server takes the last one, or rejects the file." > "/dev/stderr"
    print "Keep a single top-level authentication mapping in gremlin-server.yaml." > "/dev/stderr"
    return "nameless"
}

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

# Refuse a document whose shape this reader cannot resolve, through the same
# nameless state that makes check_auth_sides stop the boot and that
# duplicate_root() uses above.  Guessing `none` here is the unsafe answer: it
# tells the entrypoint that Gremlin configures nothing, so an operator whose
# mounted file does authenticate gets REST started open beside it.
function refuse(what,    msg) {
    msg = "yamlscan.awk: " what
    print msg > "/dev/stderr"
    print "cannot be classified by this reader, so it is refused rather than" > "/dev/stderr"
    print "called unauthenticated. Rewrite gremlin-server.yaml in the plain" > "/dev/stderr"
    print "block form, or fix the spelling above, then restart." > "/dev/stderr"
    return "nameless"
}

# `|` and `>` open a block scalar, whose content is on the following, deeper
# lines rather than on the key line.  Reading the indicator itself as the value
# answered `named` for `authenticator: |` with nothing behind it, and snakeyaml
# hands the server an empty string there, which is no authenticator at all.
function is_block_scalar(v) {
    v = trim(v)
    if (v == "") return 0
    if (substr(v, 1, 1) != "|" && substr(v, 1, 1) != ">") return 0
    return substr(v, 2) ~ /^[0-9]*[-+]?$/
}

BEGIN {
    DEPTH = 0
    FST = "key"
    CUR = ""
    CUR_KEY = ""
    CUR_VAL = ""
    AUTH_SEEN = 0
    AUTH_NAMED = 0
    ROOT_IND = -1
    AUTH_BLOCKS = 0
    in_auth = 0
    child = -1
    flow = 0
    RESULT = ""
    UNRESOLVED = 0
    ROOT_FLOW = 0
    BLOCK = 0
    BLOCK_IND = 0
    BLOCK_TXT = ""
}

# Close out the block scalar whose lines were being collected.
function finish_block() {
    BLOCK = 0
    AUTH_NAMED = names_class(BLOCK_TXT)
    BLOCK_TXT = ""
}

function handle_line(raw,    line, ind) {
    if (BLOCK) {
        # Deeper than the key means the line is still scalar content; anything
        # else ends the scalar and is ordinary content again.
        if (indent_of(raw) > BLOCK_IND) {
            if (BLOCK_TXT != "") BLOCK_TXT = BLOCK_TXT " "
            BLOCK_TXT = BLOCK_TXT trim(strip_comment(raw))
            return
        }
        finish_block()
    }

    # A CR that survived the comment being stripped is line noise, not part of
    # a key or a value.
    line = strip_comment(raw)
    sub(/[ \t]+$/, "", line)
    if (trim(line) == "") return

    ind = indent_of(line)

    # The indentation of the first real content line is the root indentation.
    # A document marker or a stray scalar opens no mapping, so keep looking
    # until a key:value line is met.  Every comparison below is against that
    # indentation rather than column 0, so a root mapping written indented --
    # valid to Settings.read() -- is recognized, while an `authentication:`
    # nested under some other key is still not mistaken for the Gremlin one.
    if (ROOT_IND < 0) {
        # A document written as one flow mapping is a shape this reader does not
        # walk, and its authenticator sits behind a root key rather than at the
        # root indentation.  Answering `none` for it is what left REST open
        # beside a Gremlin that authenticates, so it is refused.
        if (substr(trim(line), 1, 1) == "{") {
            ROOT_FLOW = 1
            return
        }
        if (!split_pair(line)) return
        ROOT_IND = ind
    } else if (ind == ROOT_IND && in_auth && !flow) {
        # A root-level sibling closes the mapping being read -- but not while a
        # flow collection is still open, or the closing brace of a flow mapping
        # spread over several lines was taken for a sibling and the direct
        # authenticator it did name was never committed.
        in_auth = 0
        flow = 0
        child = -1
    }

    # A top-level authentication key opens a mapping.  Count them and read to
    # EOF rather than exiting at the first: two top-level mappings resolve to
    # the last one (or are rejected), and answering from the first reported
    # `named` for a server the empty second mapping left open.
    if (ind == ROOT_IND && split_pair(line) &&
        unquote(K_TXT) == "authentication") {
        AUTH_BLOCKS++
        if (AUTH_BLOCKS > 1) {
            AUTH_SEEN = 0
            AUTH_NAMED = 0
        }
        in_auth = 1
        child = -1
        if (substr(V_TXT, 1, 1) == "{") {
            flow = 1
            if (scan_flow(V_TXT)) {
                in_auth = 0
                flow = 0
            }
        }
        # Anything else on the key line -- a scalar, a sequence, nothing -- is
        # not a mapping that names a class.  Reading `authentication: some.Name`
        # as named would accept a config the server cannot use.
        return
    }

    if (!in_auth) return

    if (flow) {
        if (scan_flow(line)) {
            in_auth = 0
            flow = 0
        }
        return
    }

    # Inside a block mapping: the first child sets the child indentation, and
    # only a direct child at that indentation counts.  A line reaching here is
    # never at the root indentation (the sibling case above consumed those),
    # so `child` is always deeper than the root, as a real child must be.
    if (!split_pair(line)) return
    if (child < 0) child = ind
    if (ind != child) return
    if (names_authenticator(K_TXT)) {
        AUTH_SEEN++
        if (is_block_scalar(V_TXT)) {
            # The class, if this names one at all, is on the deeper lines that
            # follow rather than on the key line.
            BLOCK = 1
            BLOCK_IND = ind
            BLOCK_TXT = ""
            return
        }
        AUTH_NAMED = names_class(V_TXT)
    }
}

{
    # YAML ends a line at CR, LF or CRLF, but awk splits records on LF alone, so
    # a file written with bare CR terminators arrives as one long record whose
    # root `authentication:` key is never met.  Splitting each record on CR
    # gives every spelling its own line; the CR that a Linux reader leaves at
    # the end of a CRLF record simply yields the empty segment that the blank
    # check drops.
    seg_n = split($0, seg, /\r/)
    for (seg_i = 1; seg_i <= seg_n; seg_i++) handle_line(seg[seg_i])
}

END {
    if (BLOCK) finish_block()
    if (ROOT_FLOW) RESULT = refuse("a document written as a root flow mapping")
    else if (UNRESOLVED)
        RESULT = refuse("a quoted key or value carrying an escape that is not resolvable here")
    else if (AUTH_BLOCKS == 0) RESULT = "none"
    else if (AUTH_BLOCKS > 1) RESULT = duplicate_root()
    else RESULT = auth_state()
    print RESULT
}
