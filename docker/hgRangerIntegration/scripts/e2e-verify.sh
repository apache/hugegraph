#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# End-to-end verification of the HugeGraph Ranger plugin against the running
# docker/hgRangerIntegration stack. Automates README Steps 4-9, and checks that
# GET /graphspaces/{space}/graphs hides graphs Ranger denies.
#
# Usage: docker/hgRangerIntegration/scripts/e2e-verify.sh   (stack must be up)
# Safe to re-run: policies are reset at start, test labels get a unique suffix.
# Step 8 stops and restarts the ranger container.

set -u

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
STACK_DIR=$(dirname "$SCRIPT_DIR")
REPO=$(cd "$STACK_DIR/../.." && pwd)
COMPOSE="docker compose -f $STACK_DIR/docker-compose.yml"
HG=${HG_URL:-http://localhost:8080}
RG=${RANGER_URL:-http://localhost:6080}
RA="admin:rangerR0cks!"
G="$HG/graphspaces/DEFAULT/graphs/hugegraph"
AUDIT_LOG=/var/log/ranger/audit/hugegraph_ranger_audit.log
POLL_WAIT=${POLL_WAIT:-35}
RUN=$(date +%s)
GRAPH2=graph2

PASS=0
FAIL=0
FAILED=()

log()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok()   { printf '  \033[32mPASS\033[0m %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  {
    printf '  \033[31mFAIL\033[0m %s\n' "$1"
    [ -n "${2:-}" ] && printf '       %s\n' "$2"
    FAIL=$((FAIL + 1))
    FAILED+=("$1")
}
info() { printf '  .... %s\n' "$*"; }

# check NAME EXPECTED ACTUAL  (EXPECTED is a regex such as '201|400')
check() {
    if [[ "$3" =~ ^($2)$ ]]; then ok "$1 (got $3)"; else bad "$1" "expected $2, got $3"; fi
}

# contains NAME HAYSTACK NEEDLE...
contains() {
    local name=$1 hay=$2
    shift 2
    for n in "$@"; do
        if [[ "$hay" != *"$n"* ]]; then
            bad "$name" "missing '$n' in: ${hay:0:300}"
            return
        fi
    done
    ok "$name"
}

code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }

wait_healthy() {
    for _ in $(seq 1 60); do
        [ "$(docker inspect -f '{{.State.Health.Status}}' "$1" 2>/dev/null)" = healthy ] && return 0
        sleep 5
    done
    return 1
}

# Print the id of the named Ranger policy, or nothing
policy_id() {
    curl -s -u "$RA" "$RG/service/plugins/policies?serviceName=hugegraph&policyName=$1" |
        python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
if isinstance(d, dict):
    d = d.get("policies", [])
if d:
    print(d[0]["id"])'
}

# upsert_policy NAME JSON: create or replace, print the HTTP code
upsert_policy() {
    local id body
    id=$(policy_id "$1")
    if [ -n "$id" ]; then
        body=$(python3 -c 'import json, sys; d = json.loads(sys.argv[1]); d["id"] = int(sys.argv[2]); print(json.dumps(d))' "$2" "$id")
        code -u "$RA" -X PUT "$RG/service/plugins/policies/$id" -H 'Content-Type: application/json' -d "$body"
    else
        code -u "$RA" -X POST "$RG/service/plugins/policies" -H 'Content-Type: application/json' -d "$2"
    fi
}

delete_policy() {
    local id
    id=$(policy_id "$1")
    if [ -n "$id" ]; then
        code -u "$RA" -X DELETE "$RG/service/plugins/policies/$id"
    else
        echo absent
    fi
}

ADMIN_ITEM='{"users":["admin"],"groups":[],"accesses":[{"type":"read","isAllowed":true},{"type":"write","isAllowed":true},{"type":"delete","isAllowed":true},{"type":"execute","isAllowed":true},{"type":"admin","isAllowed":true}],"conditions":[],"delegateAdmin":false}'
ALICE_ITEM='{"users":["alice"],"groups":[],"accesses":[{"type":"admin","isAllowed":true}],"conditions":[],"delegateAdmin":false}'

# res GRAPH LABEL: resources block on every graph space and resource type
res() {
    echo "{\"graphspace\":{\"values\":[\"*\"],\"isExcludes\":false,\"isRecursive\":false},
           \"graph\":{\"values\":[\"$1\"],\"isExcludes\":false,\"isRecursive\":false},
           \"resource-type\":{\"values\":[\"*\"],\"isExcludes\":false,\"isRecursive\":false},
           \"label\":{\"values\":[\"$2\"],\"isExcludes\":false,\"isRecursive\":false}}"
}

# access_policy BOB_ACCESSES: the README's combined hugegraph-access policy
access_policy() {
    cat <<EOF
{"name":"hugegraph-access","service":"hugegraph","isEnabled":true,"isAuditEnabled":true,
 "resources":$(res '*' '*'),
 "policyItems":[{"users":["bob"],"groups":[],"accesses":$1,"conditions":[],"delegateAdmin":false},
                $ALICE_ITEM,$ADMIN_ITEM]}
EOF
}

# deny_bob_read_policy NAME GRAPH LABEL: additive deny, as in README Test 3b
deny_bob_read_policy() {
    cat <<EOF
{"name":"$1","service":"hugegraph","isEnabled":true,"isAuditEnabled":true,"policyPriority":1,
 "resources":$(res "$2" "$3"),
 "policyItems":[$ADMIN_ITEM],
 "denyPolicyItems":[{"users":["bob"],"groups":[],"accesses":[{"type":"read","isAllowed":true}],"conditions":[],"delegateAdmin":false}]}
EOF
}

# audit_has NAME PATTERN...: all patterns on one plugin audit line (retries for flush)
audit_has() {
    local name=$1 lines
    shift
    for _ in $(seq 1 8); do
        lines=$(docker exec hugegraph tail -n 400 "$AUDIT_LOG" 2>/dev/null)
        for p in "$@"; do lines=$(grep -F -- "$p" <<<"$lines"); done
        if [ -n "$lines" ]; then
            ok "$name"
            return
        fi
        sleep 5
    done
    bad "$name" "no audit line matching: $*"
}

graphs_of() {
    curl -s -u "$1" "$HG/graphspaces/DEFAULT/graphs" |
        python3 -c 'import json, sys; print(",".join(sorted(json.load(sys.stdin).get("graphs", []))))' 2>/dev/null
}

refresh() {
    info "waiting ${POLL_WAIT}s for policy refresh"
    sleep "$POLL_WAIT"
}

###############################################################################
log "Preflight"
for c in ranger-db ranger-solr ranger hugegraph; do
    if wait_healthy "$c"; then ok "container $c healthy"; else bad "container $c healthy"; fi
done
if [ "$FAIL" -gt 0 ]; then
    echo "Stack not healthy, aborting."
    exit 1
fi

# Ranger's optional GraalVM script engine for policy conditions is not
# bundled; it logs a WARN with ClassNotFoundException and is not an error
ERRS=$(docker logs hugegraph 2>&1 |
       grep -E 'NoSuchMethodError|ClassNotFoundException|NoClassDefFoundError|LinkageError|ServiceConfigurationError|SLF4J|Unauthenticated' |
       grep -v 'org.graalvm' | head -5)
if [ -z "$ERRS" ]; then
    ok "no class-loading or SLF4J errors in the hugegraph log"
else
    bad "no class-loading or SLF4J errors in the hugegraph log" "$ERRS"
fi
contains "plugin started" "$(docker logs hugegraph 2>&1)" \
    "RangerHugeGraphPlugin started" "RangerHugeGraphAuthenticator initialised"

###############################################################################
log "Step 4 - service definition and service"
SD=$(curl -s -u "$RA" -X POST "$RG/service/plugins/definitions" -H 'Content-Type: application/json' \
     -d @"$REPO/hugegraph-server/hugegraph-ranger-plugin/src/main/resources/ranger-servicedef-hugegraph.json")
if [[ "$SD" == *'"name":"hugegraph"'* || "$SD" == *already* ]]; then
    ok "service def registered"
else
    bad "service def registered" "${SD:0:300}"
fi

SVC=$(curl -s -u "$RA" -X POST "$RG/service/plugins/services" -H 'Content-Type: application/json' -d '{
  "name":"hugegraph","displayName":"HugeGraph","description":"Local HugeGraph test instance",
  "type":"hugegraph","isEnabled":true,
  "configs":{"username":"admin","password":"admin","hugegraph.url":"http://hugegraph:8080"}}')
if [[ "$SVC" == *'"id"'* || "$SVC" == *already* || "$SVC" == *Duplicate* ]]; then
    ok "service instance created"
else
    bad "service instance created" "${SVC:0:300}"
fi

###############################################################################
log "Step 5 - users"
for u in bob alice; do
    check "HugeGraph user $u" '201|200|400' \
        "$(code -u admin:admin -X POST "$HG/graphspaces/DEFAULT/auth/users" -H 'Content-Type: application/json' \
             -d "{\"user_name\":\"$u\",\"user_password\":\"${u}pass\"}")"
    check "Ranger user $u" '200|400' \
        "$(code -u "$RA" -X POST "$RG/service/xusers/secure/users" -H 'Content-Type: application/json' \
             -d "{\"name\":\"$u\",\"firstName\":\"$u\",\"password\":\"${u}Ranger1\",\"userRoleList\":[\"ROLE_USER\"],\"status\":1}")"
done

###############################################################################
log "Step 6 - policies (reset to the README baseline)"
info "deny-software policy: $(delete_policy hugegraph-bob-deny-software)"
info "deny-$GRAPH2 policy: $(delete_policy hugegraph-bob-deny-$GRAPH2)"
check "hugegraph-access policy (bob read)" '200' \
    "$(upsert_policy hugegraph-access "$(access_policy '[{"type":"read","isAllowed":true}]')")"
refresh

check "admin reads schema" '200' "$(code -u admin:admin "$G/schema/propertykeys")"
# The plugin ships audits to Solr asynchronously, so allow time for a flush
ADMIN_AUDIT=
for _ in $(seq 1 12); do
    ADMIN_AUDIT=$(curl -s -u "$RA" "$RG/service/assets/accessAudit?requestUser=admin&sortBy=eventTime&descending=true&pageSize=5")
    [[ "$ADMIN_AUDIT" == *'"accessResult":1'* ]] && break
    sleep 5
done
contains "Ranger Admin audit has admin allowed" "$ADMIN_AUDIT" '"accessResult":1'

###############################################################################
log "Step 7 - smoke tests"
JSON='Content-Type: application/json'
check "setup: vertexlabel person"   '201|400' "$(code -u alice:alicepass -X POST "$G/schema/vertexlabels" -H "$JSON" -d '{"name":"person","id_strategy":"CUSTOMIZE_STRING"}')"
check "setup: vertexlabel software" '201|400' "$(code -u alice:alicepass -X POST "$G/schema/vertexlabels" -H "$JSON" -d '{"name":"software","id_strategy":"CUSTOMIZE_STRING"}')"
check "setup: edgelabel likes"      '201|400' "$(code -u alice:alicepass -X POST "$G/schema/edgelabels" -H "$JSON" -d '{"name":"likes","source_label":"person","target_label":"software"}')"
check "setup: vertex p1"            '201'     "$(code -u alice:alicepass -X POST "$G/graph/vertices" -H "$JSON" -d '{"label":"person","id":"p1","properties":{}}')"
check "setup: vertex s1"            '201'     "$(code -u alice:alicepass -X POST "$G/graph/vertices" -H "$JSON" -d '{"label":"software","id":"s1","properties":{}}')"
check "setup: edge p1-likes->s1"    '201'     "$(code -u alice:alicepass -X POST "$G/graph/edges" -H "$JSON" -d '{"label":"likes","outV":"p1","outVLabel":"person","inV":"s1","inVLabel":"software","properties":{}}')"

contains "Test 1: bob reads vertices" "$(curl -s --compressed -u bob:bobpass "$G/graph/vertices")" '"p1"' '"s1"'
check "Test 2: bob cannot create schema" '403' \
    "$(code -u bob:bobpass -X POST "$G/schema/vertexlabels" -H "$JSON" -d "{\"name\":\"nope_$RUN\",\"id_strategy\":\"DEFAULT\"}")"
contains "Test 3: bob reads edges" "$(curl -s --compressed -u bob:bobpass "$G/graph/edges")" '"likes"'
audit_has "audit: bob read on a real vertex resource" '"reqUser":"bob"' '"access":"read"' 'vertex'

check "Test 3b: add deny policy (bob, label software)" '200' \
    "$(upsert_policy hugegraph-bob-deny-software "$(deny_bob_read_policy hugegraph-bob-deny-software '*' software)")"
refresh
check "Test 3b: bob reads p1 (person)"   '200' "$(code -u bob:bobpass "$G/graph/vertices/%22p1%22")"
check "Test 3b: bob reads s1 (software)" '403' "$(code -u bob:bobpass "$G/graph/vertices/%22s1%22")"
audit_has "audit: bob denied on software" '"reqUser":"bob"' 'software' '"result":0'
check "Test 3b: remove deny policy" '204|200' "$(delete_policy hugegraph-bob-deny-software)"
refresh

check "Test 4: alice creates schema" '201' \
    "$(code -u alice:alicepass -X POST "$G/schema/vertexlabels" -H "$JSON" -d "{\"name\":\"person2_$RUN\",\"id_strategy\":\"DEFAULT\"}")"
contains "Test 4: alice reads vertices" "$(curl -s --compressed -u alice:alicepass "$G/graph/vertices")" '"p1"' '"s1"'
check "Test 5: unknown user rejected" '401' "$(code -u nobody:wrongpass "$G/graph/vertices")"

###############################################################################
log "Step 8 - policy cache while Ranger Admin is down"
$COMPOSE stop ranger >/dev/null 2>&1
contains "bob reads vertices with Ranger Admin down" \
    "$(curl -s --compressed -u bob:bobpass "$G/graph/vertices")" '"p1"'
$COMPOSE start ranger >/dev/null 2>&1
if wait_healthy ranger; then ok "Ranger Admin healthy again"; else bad "Ranger Admin healthy again"; fi

###############################################################################
log "Step 9 - dynamic policy update"
check "bob cannot write before the update" '403' \
    "$(code -u bob:bobpass -X POST "$G/schema/vertexlabels" -H "$JSON" -d "{\"name\":\"testlabel_$RUN\",\"id_strategy\":\"DEFAULT\"}")"
check "grant bob write" '200' \
    "$(upsert_policy hugegraph-access "$(access_policy '[{"type":"read","isAllowed":true},{"type":"write","isAllowed":true}]')")"
refresh
check "bob can write after the update" '201' \
    "$(code -u bob:bobpass -X POST "$G/schema/vertexlabels" -H "$JSON" -d "{\"name\":\"testlabel_$RUN\",\"id_strategy\":\"DEFAULT\"}")"
check "revoke bob write (restore baseline)" '200' \
    "$(upsert_policy hugegraph-access "$(access_policy '[{"type":"read","isAllowed":true}]')")"

###############################################################################
log "Graph listing hides graphs Ranger denies"
# Recreate it on every run. In standalone mode the create API defaults
# gremlin.graph to the unproxied HugeFactory, so pass the auth proxy factory
# explicitly, as conf/graphs/hugegraph.properties does.
info "drop old $GRAPH2: $(code -u admin:admin -X DELETE \
    "$HG/graphspaces/DEFAULT/graphs/$GRAPH2?confirm_message=I%27m%20sure%20to%20drop%20the%20graph")"
check "admin creates $GRAPH2" '201' \
    "$(code -u admin:admin -X POST "$HG/graphspaces/DEFAULT/graphs/$GRAPH2" -H "$JSON" \
         -d "{\"gremlin.graph\":\"org.apache.hugegraph.auth.HugeFactoryAuthProxy\",\"backend\":\"rocksdb\",\"serializer\":\"binary\",\"store\":\"$GRAPH2\",\"rocksdb.data_path\":\"./rocksdb-data/$GRAPH2\",\"rocksdb.wal_path\":\"./rocksdb-data/$GRAPH2\"}")"
check "deny bob read on $GRAPH2" '200' \
    "$(upsert_policy hugegraph-bob-deny-$GRAPH2 "$(deny_bob_read_policy hugegraph-bob-deny-$GRAPH2 $GRAPH2 '*')")"
refresh

ADMIN_GRAPHS=$(graphs_of admin:admin)
ALICE_GRAPHS=$(graphs_of alice:alicepass)
BOB_GRAPHS=$(graphs_of bob:bobpass)
info "admin sees: [$ADMIN_GRAPHS]"
info "alice sees: [$ALICE_GRAPHS]"
info "bob sees:   [$BOB_GRAPHS]"
check "admin lists both graphs" "$GRAPH2,hugegraph" "$ADMIN_GRAPHS"
check "alice lists both graphs" "$GRAPH2,hugegraph" "$ALICE_GRAPHS"
check "bob lists only hugegraph" 'hugegraph' "$BOB_GRAPHS"
check "bob still reads hugegraph vertices" '200' "$(code -u bob:bobpass "$G/graph/vertices")"
# Per-element checks only run on real elements, so read a vertex by id
G2="$HG/graphspaces/DEFAULT/graphs/$GRAPH2"
check "setup: $GRAPH2 vertexlabel thing" '201' \
    "$(code -u admin:admin -X POST "$G2/schema/vertexlabels" -H "$JSON" -d '{"name":"thing","id_strategy":"CUSTOMIZE_STRING"}')"
check "setup: $GRAPH2 vertex t1" '201' \
    "$(code -u admin:admin -X POST "$G2/graph/vertices" -H "$JSON" -d '{"label":"thing","id":"t1","properties":{}}')"
check "admin reads $GRAPH2 vertex t1" '200' "$(code -u admin:admin "$G2/graph/vertices/%22t1%22")"
check "bob denied on $GRAPH2 vertex t1" '403' "$(code -u bob:bobpass "$G2/graph/vertices/%22t1%22")"
audit_has "audit: bob denied on $GRAPH2" '"reqUser":"bob"' "DEFAULT/$GRAPH2/" '"result":0'

###############################################################################
log "Summary"
echo "  passed: $PASS   failed: $FAIL"
for f in "${FAILED[@]+"${FAILED[@]}"}"; do echo "   - $f"; done
echo "  plugin jar: $(docker exec hugegraph sh -c 'ls -l lib/ | grep -i ranger-plugin' | awk '{print $5, $NF}')"
echo "  left in place: users bob/alice, baseline hugegraph-access policy,"
echo "  graph $GRAPH2 and policy hugegraph-bob-deny-$GRAPH2"
[ "$FAIL" -eq 0 ]
