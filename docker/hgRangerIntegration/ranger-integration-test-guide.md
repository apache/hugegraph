# HugeGraph Ranger Plugin — Integration Test Guide

## Prerequisites

- Docker and Docker Compose installed
- Maven 3.5+ and Java 11+
- HugeGraph source cloned, on branch `RangerPlugin`

---

## Step 1 — No manual build needed

`docker-compose.yml` builds the `hugegraph` image straight from source via
`Dockerfile.hugegraph`, which runs:

```bash
mvn package -pl hugegraph-server/hugegraph-dist -am -DskipTests -Dwith-ranger-plugin
```

`-Dwith-ranger-plugin` bundles the shaded `hugegraph-ranger-plugin-1.7.0-plugin.jar`
(the one with Ranger's runtime deps, not the ~37 KB thin jar) directly into the
distribution's `lib/`. There's nothing to build or mount by hand — `docker compose up`
(Step 2 below) does it all. The flag is opt-in on `hugegraph-dist`; a plain
`mvn package` without it omits the plugin entirely, so standalone HugeGraph builds are
unaffected.

If you want to build the distribution standalone (outside Docker) — e.g. to inspect
`lib/` or run HugeGraph directly on the host — use the same command from the repo root:

```bash
mvn clean package -pl hugegraph-server/hugegraph-dist -am -DskipTests -Dwith-ranger-plugin
```

This produces `hugegraph-server/apache-hugegraph-server-1.7.0/` with the plugin already
in `lib/`.

---

## Step 2 — Docker Compose Stack

The `docker-compose.yml` defines the stack with 4 services:

**Services:**
- `ranger-db` — PostgreSQL 14 (Ranger Admin metadata store)
- `ranger-solr` — Solr 8.11 (audit backend for Ranger Admin)
- `ranger` — Apache Ranger 2.4.0 (policy server)
- `hugegraph` — HugeGraph 1.7.0 (graph database with Ranger plugin)

**Key files:**
- `Dockerfile.hugegraph` — builds HugeGraph with Ranger plugin, audit directory
- `ranger-hugegraph-security.xml` — plugin-side Ranger config
- `rest-server.properties` — HugeGraph REST server config
- `solr-init.sh` — creates the `ranger_audits` Solr core on startup

The Ranger Admin password is `rangerR0cks!` — hardcoded in the image's `install.properties`.

---

## Step 3 — Start the Stack

```bash
# Start the full stack
docker compose -f docker/hgRangerIntegration/docker-compose.yml up -d 
```

The Solr and Ranger Admin containers take a few minutes to initialise. Check status:

```bash
docker compose -f docker/hgRangerIntegration/docker-compose.yml ps
# ranger-db    - healthy
# ranger-solr  - healthy (Solr core created, audit backend ready)
# ranger       - healthy (Ranger Admin with Solr audit enabled)
# hugegraph    - healthy (with Ranger plugin)
```

**What happens during startup:**
1. PostgreSQL starts and initializes the Ranger Admin database
2. Solr starts and `solr-init.sh` creates the `ranger_audits` core
3. Ranger Admin starts and configures Solr as the audit backend
4. HugeGraph starts and loads the Ranger plugin

If any container fails, check logs with `docker compose logs <service>`.

---

## Step 4 — Register the HugeGraph Service in Ranger Admin

These steps are one-time setup. Run them after Ranger Admin is healthy.

**Ranger Admin credentials: `admin` / `rangerR0cks!`**

### 4a. Register the service definition

```bash
curl -s -u admin:rangerR0cks! \
  -X POST http://localhost:6080/service/plugins/definitions \
  -H "Content-Type: application/json" \
  -d @hugegraph-server/hugegraph-ranger-plugin/src/main/resources/ranger-servicedef-hugegraph.json
```

Expected: HTTP 200 with a JSON body containing `"name": "hugegraph"`.

If you see `service def with the name[hugegraph] already exists` the definition is already registered — skip to 5b.

### 4b. Clear `implClass` from the service definition

The service definition ships with `implClass` pointing to `RangerHugeGraphService`. Ranger Admin tries to load that class at service-instance creation time; if the plugin jar is not on **Ranger Admin's** classpath it blocks the creation entirely (not just a warning). Clear it before creating the service instance.

**Get the service definition ID:**
```bash
SDEF_ID=$(curl -s -u admin:rangerR0cks! \
  http://localhost:6080/service/plugins/definitions/name/hugegraph \
  | python3 -m json.tool | grep '"id"' | head -1 | grep -o '[0-9]*')
echo "servicedef id: $SDEF_ID"
```

**Patch `implClass` to empty string** (replace `$SDEF_ID` if not using the variable):
```bash
curl -s -u admin:rangerR0cks! \
  -X PUT "http://localhost:6080/service/plugins/definitions/$SDEF_ID" \
  -H "Content-Type: application/json" \
  -d "{
    \"id\": $SDEF_ID,
    \"name\": \"hugegraph\",
    \"displayName\": \"HugeGraph\",
    \"description\": \"Apache Ranger service definition for HugeGraph graph database\",
    \"implClass\": \"\",
    \"label\": \"HugeGraph\",
    \"version\": 1,
    \"isEnabled\": true,
    \"resources\": [
      {\"itemId\":1,\"name\":\"graphspace\",\"type\":\"string\",\"level\":10,\"parent\":\"\",\"mandatory\":true,\"lookupSupported\":true,\"recursiveSupported\":false,\"excludesSupported\":true,\"matcher\":\"org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher\",\"matcherOptions\":{\"wildCard\":\"true\",\"ignoreCase\":\"false\"},\"label\":\"Graph Space\",\"description\":\"HugeGraph graph space\"},
      {\"itemId\":2,\"name\":\"graph\",\"type\":\"string\",\"level\":20,\"parent\":\"graphspace\",\"mandatory\":true,\"lookupSupported\":true,\"recursiveSupported\":false,\"excludesSupported\":true,\"matcher\":\"org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher\",\"matcherOptions\":{\"wildCard\":\"true\",\"ignoreCase\":\"false\"},\"label\":\"Graph\",\"description\":\"HugeGraph graph name\"},
      {\"itemId\":3,\"name\":\"resource-type\",\"type\":\"string\",\"level\":30,\"parent\":\"graph\",\"mandatory\":false,\"lookupSupported\":true,\"recursiveSupported\":false,\"excludesSupported\":true,\"matcher\":\"org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher\",\"matcherOptions\":{\"wildCard\":\"true\",\"ignoreCase\":\"true\"},\"label\":\"Resource Type\",\"description\":\"HugeGraph resource type\"},
      {\"itemId\":4,\"name\":\"label\",\"type\":\"string\",\"level\":40,\"parent\":\"resource-type\",\"mandatory\":false,\"lookupSupported\":false,\"recursiveSupported\":false,\"excludesSupported\":true,\"matcher\":\"org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher\",\"matcherOptions\":{\"wildCard\":\"true\",\"ignoreCase\":\"false\"},\"label\":\"Label\",\"description\":\"Vertex or edge label name\"}
    ],
    \"accessTypes\": [
      {\"itemId\":1,\"name\":\"read\",\"label\":\"Read\",\"impliedGrants\":[]},
      {\"itemId\":2,\"name\":\"write\",\"label\":\"Write\",\"impliedGrants\":[\"read\"]},
      {\"itemId\":3,\"name\":\"delete\",\"label\":\"Delete\",\"impliedGrants\":[\"read\"]},
      {\"itemId\":4,\"name\":\"execute\",\"label\":\"Execute\",\"impliedGrants\":[]},
      {\"itemId\":5,\"name\":\"admin\",\"label\":\"Admin\",\"impliedGrants\":[\"read\",\"write\",\"delete\",\"execute\"]}
    ],
    \"configs\": [
      {\"itemId\":1,\"name\":\"username\",\"type\":\"string\",\"mandatory\":true,\"label\":\"Username\",\"defaultValue\":\"\"},
      {\"itemId\":2,\"name\":\"password\",\"type\":\"password\",\"mandatory\":true,\"label\":\"Password\",\"defaultValue\":\"\"},
      {\"itemId\":3,\"name\":\"hugegraph.url\",\"type\":\"string\",\"mandatory\":true,\"label\":\"HugeGraph REST URL\",\"defaultValue\":\"http://localhost:8080\"}
    ],
    \"enums\": [],
    \"contextEnrichers\": [],
    \"policyConditions\": [],
    \"dataMaskDef\": {},
    \"rowFilterDef\": {}
  }"
```

Expected: HTTP 200 with the updated definition (no `implClass` value).

### 4c. Create a HugeGraph service instance

```bash
curl -s -u admin:rangerR0cks! \
  -X POST http://localhost:6080/service/plugins/services \
  -H "Content-Type: application/json" \
  -d '{
    "name": "hugegraph",
    "displayName": "HugeGraph",
    "description": "Local HugeGraph test instance",
    "type": "hugegraph",
    "isEnabled": true,
    "configs": {
      "username": "admin",
      "password": "admin",
      "hugegraph.url": "http://hugegraph:8080"
    }
  }'
```

Expected: HTTP 200 with a JSON body containing `"id"`. Confirm:
```bash
curl -s -u admin:rangerR0cks! \
  http://localhost:6080/service/plugins/services/name/hugegraph | python3 -m json.tool | grep '"id"'
```

---

## Step 5 — Create Test Users in HugeGraph

```bash
# Create user "bob" with password "bobpass"
curl -u admin:admin \
-X POST http://localhost:8080/graphspaces/DEFAULT/auth/users \
-H "Content-Type: application/json" \
-d '{"user_name": "bob", "user_password": "bobpass"}'

# Create user "alice" with password "alicepass"
curl -u admin:admin \
-X POST http://localhost:8080/graphspaces/DEFAULT/auth/users \
-H "Content-Type: application/json" \
-d '{"user_name": "alice", "user_password": "alicepass"}'
```

---

## Step 6 — Create Ranger Policies

> **Important:** Ranger rejects two separate policies with overlapping resource patterns
> (`graphspace/*`, `graph/*`, etc. match the same space). Bob, Alice, and HugeGraph's
> `admin` user must all be in a **single combined policy** with separate `policyItems`
> entries.

> **Why `resource-type: ["*"]`?** The Ranger plugin checks permissions using
> `ResourceType.ALL` at login time to build the user's role. The policy must cover `"*"`
> (wildcard) so that `"all"` matches. Using `["vertex"]` would not match and the user
> would be denied everything.

> **Why include `admin`?** `HugeGraphAuthProxy` still calls into the Ranger authorizer
> for the `admin` role so that admin's actions land in Ranger's audit log — but it
> discards the verdict, so admin is never actually denied regardless of policy. Without
> a matching policy, admin's actions are audited as `Result: Denied` (harmless, but
> confusing to read). Add `admin` to this policy so Ranger's own verdict is `Allowed`
> and the audit trail reflects that admin genuinely has access, not just that it was
> let through unconditionally.

```bash
curl -s -u admin:rangerR0cks! \
  -X POST http://localhost:6080/service/plugins/policies \
  -H "Content-Type: application/json" \
  -d '{
    "name": "hugegraph-access",
    "service": "hugegraph",
    "isEnabled": true,
    "isAuditEnabled": true,
    "resources": {
      "graphspace":     {"values": ["*"], "isExcludes": false, "isRecursive": false},
      "graph":          {"values": ["*"], "isExcludes": false, "isRecursive": false},
      "resource-type":  {"values": ["*"], "isExcludes": false, "isRecursive": false},
      "label":          {"values": ["*"], "isExcludes": false, "isRecursive": false}
    },
    "policyItems": [
      {
        "users":      ["bob"],
        "groups":     [],
        "accesses":   [{"type": "read", "isAllowed": true}],
        "conditions": [],
        "delegateAdmin": false
      },
      {
        "users":      ["alice"],
        "groups":     [],
        "accesses":   [{"type": "admin", "isAllowed": true}],
        "conditions": [],
        "delegateAdmin": false
      },
      {
        "users":      ["admin"],
        "groups":     [],
        "accesses":   [
          {"type": "read", "isAllowed": true},
          {"type": "write", "isAllowed": true},
          {"type": "delete", "isAllowed": true},
          {"type": "execute", "isAllowed": true},
          {"type": "admin", "isAllowed": true}
        ],
        "conditions": [],
        "delegateAdmin": false
      }
    ]
  }'
```

Expected: HTTP 200 with `"id"` in the response.

Wait ~30 seconds for HugeGraph to pull the new policies from Ranger Admin.

Confirm admin's actions are now audited as allowed, not just let through:

```bash
curl -s -o /dev/null -w "%{http_code}\n" -u admin:admin \
  http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/schema/propertykeys
# Expected: 200

curl -s -u admin:rangerR0cks! \
  "http://localhost:6080/service/assets/accessAudit?requestUser=admin&sortBy=eventTime&descending=true&pageSize=5" \
  | python3 -m json.tool | grep -E '"eventTime"|"accessResult"'
# Expected: accessResult 1 (Allowed) for the most recent entries
```

---

## Step 7 — Run the Smoke Tests

All tests use the REST API (`/graphspaces/DEFAULT/graphs/hugegraph/...`) rather than the
Gremlin endpoint, which has serialization issues in this deployment.

### Setup — create schema and real graph data

The read tests below (Test 1, Test 3) only exercise the per-request Ranger check
(`checkResourceAuthorizer`) if there is actual data to iterate — an empty result set never
reaches the authorizer for a real element. Create schema and a couple of elements as
**alice** (admin) before running the read tests, using fixed (`CUSTOMIZE_STRING`) vertex
IDs so later tests can address them directly.

```bash
# Vertex labels — CUSTOMIZE_STRING lets us assign ids explicitly below
curl -s -o /dev/null -w "%{http_code}\n" -u alice:alicepass \
  -X POST http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/schema/vertexlabels \
  -H "Content-Type: application/json" \
  -d '{"name":"person","id_strategy":"CUSTOMIZE_STRING"}'
# Expected: 201

curl -s -o /dev/null -w "%{http_code}\n" -u alice:alicepass \
  -X POST http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/schema/vertexlabels \
  -H "Content-Type: application/json" \
  -d '{"name":"software","id_strategy":"CUSTOMIZE_STRING"}'
# Expected: 201

# Edge label "likes" from person -> software
curl -s -o /dev/null -w "%{http_code}\n" -u alice:alicepass \
  -X POST http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/schema/edgelabels \
  -H "Content-Type: application/json" \
  -d '{"name":"likes","source_label":"person","target_label":"software"}'
# Expected: 201

# Vertices with fixed ids "p1" (person) and "s1" (software)
curl -s -o /dev/null -w "%{http_code}\n" -u alice:alicepass \
  -X POST http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/graph/vertices \
  -H "Content-Type: application/json" \
  -d '{"label":"person","id":"p1","properties":{}}'
# Expected: 201

curl -s -o /dev/null -w "%{http_code}\n" -u alice:alicepass \
  -X POST http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/graph/vertices \
  -H "Content-Type: application/json" \
  -d '{"label":"software","id":"s1","properties":{}}'
# Expected: 201

# Edge "p1 -likes-> s1"
curl -s -o /dev/null -w "%{http_code}\n" -u alice:alicepass \
  -X POST http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/graph/edges \
  -H "Content-Type: application/json" \
  -d '{"label":"likes","outV":"p1","outVLabel":"person","inV":"s1","inVLabel":"software","properties":{}}'
# Expected: 201
```

### Test 1 — Bob can read vertices ✅

```bash
curl -s --compressed -u bob:bobpass \
  http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/graph/vertices
```

Expected: a `vertices` array containing `p1` (label `person`) and `s1` (label `software`).
Because these are now real elements, the underlying `FilterIterator` in
`HugeGraphAuthProxy.verifyElemPermission` calls `checkResourceAuthorizer()` per element,
consulting Ranger with the real `graphspace/graph/resource-type/label` — see the audit
verification at the end of this step.

### Test 2 — Bob cannot create a schema ❌

```bash
curl -s -o /dev/null -w "%{http_code}\n" -u bob:bobpass \
  -X POST http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/schema/vertexlabels \
  -H "Content-Type: application/json" \
  -d '{"name":"nope","id_strategy":"DEFAULT"}'
```

Expected: `403`

This does **not** exercise the Ranger per-request check — it demonstrates the local
`RolePerm.match()` gate. Bob's HugeGraph role only grants `READ`, so
`HugeGraphAuthProxy.verifyResPermission()` (`HugeGraphAuthProxy.java:1197`) rejects the
`WRITE` attempt before `matched` is ever `true`, and the code at
`HugeGraphAuthProxy.java:1213-1216` (`if (matched && RESOURCE_AUTHORIZER != null && ...)`)
short-circuits — Ranger is never consulted at all for this call. Confirm this by checking
the audit log after this request: there should be **no** new entry for it, since
`RangerHugeGraphPlugin.isAllowed()` was never invoked.

### Test 3 — Bob can also read edges ✅

Bob's policy covers `resource-type: ["*"]` which includes edges, so this succeeds either
way. Because the `likes` edge created in Setup is a real element, this request is also
checked against Ranger with the *real* resource-type/label being accessed (`edge`/`likes`
here), not just the coarse `ResourceType.ALL` grant fetched once at login — see Test 3b
below for a case that actually depends on this distinction, since this test would pass
either way.

```bash
curl -s -o /dev/null -w "%{http_code}\n" -u bob:bobpass \
  http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/graph/edges
```

Expected: `200`, with the `likes` edge from `p1` to `s1` in the `edges` array.

### Test 3b — Fine-grained per-label denial ❌ (regression test for real enforcement)

Keep bob's wildcard `label: ["*"]` read grant in place — it is what lets him pass the
coarse `ResourceType.ALL`/`*` role check done once at login
(`RangerAuthManager.mergeRangerRole`). Ranger policies are additive/allow-list: a policy
that grants read on `label: ["person"]` only adds a grant, it never implies "deny read on
every other label." So **narrowing** bob's grant to `person` only (replacing the wildcard)
does not produce a person/software split — it makes Ranger deny read for *every* label,
including `person`, because bob then has no grant at all that matches the coarse
`ResourceType.ALL` check, and `HugeGraphAuthProxy.verifyResPermission()` rejects him at the
local `RolePerm.match()` gate before the per-element Ranger check is ever consulted.

To get an actual split, add a second, higher-priority policy with an explicit
**`denyPolicyItems`** entry scoped to `label: ["software"]`, while leaving bob's wildcard
allow grant untouched. Ranger evaluates deny policies with priority over allow policies for
a matching resource, so: the coarse per-login check still grants bob `READ` (nothing denies
`resource-type: ALL`/`*`), and the per-element check on the real label then denies
`software` specifically while `person` still matches only the allow.

```bash
# 1. Get the combined policy id (created in Step 6)
POLICY_ID=$(curl -s -u admin:rangerR0cks! \
  "http://localhost:6080/service/plugins/policies?serviceName=hugegraph&policyName=hugegraph-access" \
  | python3 -m json.tool | grep '"id"' | head -1 | grep -o '[0-9]*')
echo "Policy id: $POLICY_ID"

# 2. Add a deny policy for bob's read on label "software" only. Bob's wildcard read
#    grant in hugegraph-access (id=$POLICY_ID) is left untouched — this is additive.
curl -s -u admin:rangerR0cks! \
  -X POST http://localhost:6080/service/plugins/policies \
  -H "Content-Type: application/json" \
  -d '{
    "name": "hugegraph-bob-deny-software",
    "service": "hugegraph",
    "isEnabled": true,
    "isAuditEnabled": true,
    "policyPriority": 1,
    "resources": {
      "graphspace":    {"values": ["*"], "isExcludes": false, "isRecursive": false},
      "graph":         {"values": ["*"], "isExcludes": false, "isRecursive": false},
      "resource-type": {"values": ["*"], "isExcludes": false, "isRecursive": false},
      "label":         {"values": ["software"], "isExcludes": false, "isRecursive": false}
    },
    "policyItems": [
      {
        "users":      ["admin"],
        "groups":     [],
        "accesses":   [
          {"type": "read", "isAllowed": true},
          {"type": "write", "isAllowed": true},
          {"type": "delete", "isAllowed": true},
          {"type": "execute", "isAllowed": true},
          {"type": "admin", "isAllowed": true}
        ],
        "conditions": [],
        "delegateAdmin": false
      }
    ],
    "denyPolicyItems": [
      {
        "users":      ["bob"],
        "groups":     [],
        "accesses":   [{"type": "read", "isAllowed": true}],
        "conditions": [],
        "delegateAdmin": false
      }
    ]
  }'
# Expected: 200 with "id" in the response

# 3. Wait for policy refresh (default poll interval: 30s)
sleep 35

# 4. Bob reading vertex "p1" (label "person") — still allowed. Note the vertex id must be
#    URL-encoded (%22p1%22) — a literal quote in the path 500s with a URISyntaxException
#    rather than exercising the permission check at all.
curl -s -o /dev/null -w "%{http_code}\n" -u bob:bobpass \
  http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/graph/vertices/%22p1%22
# Expected: 200

# 5. Bob reading vertex "s1" (label "software") — now denied
curl -s -o /dev/null -w "%{http_code}\n" -u bob:bobpass \
  http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/graph/vertices/%22s1%22
# Expected: 403
```

Confirm the denial reached the real per-request check (not a local-role rejection, since
bob's base role still grants `READ` generally) by checking the audit log for a `result: 0`
entry with `resource` containing `.../vertex/software` and `reqUser: bob`:

```bash
docker exec hugegraph tail -n 20 /var/log/ranger/audit/hugegraph_ranger_audit.log
```

**Cleanup:** delete the `hugegraph-bob-deny-software` policy before continuing to Test 4,
so later steps that expect bob to read any label are not affected by this deny rule.

```bash
DENY_POLICY_ID=$(curl -s -u admin:rangerR0cks! \
  "http://localhost:6080/service/plugins/policies?serviceName=hugegraph&policyName=hugegraph-bob-deny-software" \
  | python3 -m json.tool | grep '"id"' | head -1 | grep -o '[0-9]*')
curl -s -u admin:rangerR0cks! -X DELETE \
  "http://localhost:6080/service/plugins/policies/$DENY_POLICY_ID" -o /dev/null -w "%{http_code}\n"
# Expected: 204
sleep 35
```

### Test 4 — Alice can do everything ✅

```bash
# Create a new vertex label as alice — "person2", not "person" (that name was
# already taken by the Setup step above; reusing it 400s with ExistedException,
# a schema-validation error unrelated to Ranger)
curl -s -o /dev/null -w "%{http_code}\n" -u alice:alicepass \
  -X POST http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/schema/vertexlabels \
  -H "Content-Type: application/json" \
  -d '{"name":"person2","id_strategy":"DEFAULT"}'
```

Expected: `201`

```bash
# Alice can also read vertices
curl -s --compressed -u alice:alicepass \
  http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/graph/vertices
```

Expected: `{"vertices":[]}`

### Test 5 — Unknown user is rejected ❌

Use a protected endpoint — `/versions` is public and always returns 200.

```bash
curl -s -o /dev/null -w "%{http_code}\n" -u nobody:wrongpass \
  http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/graph/vertices
```

Expected: `401`

### Verify audit entries reflect the real request (do this after Tests 1-4)

After running the smoke tests above, check the audit log and confirm each entry's
`resource`/`access`/`action` fields (see [Audit logs](#audit-logs) below) match the operation
that was actually performed, not a generic placeholder:

```bash
docker exec hugegraph tail -n 20 /var/log/ranger/audit/hugegraph_ranger_audit.log
```

For example, after Test 1 (bob reading vertices), look for an entry with
`"reqUser":"bob"`, `"access":"read"`, `"action":"read"`, and `"resource"` containing
`hugegraph/vertex` (not the stale `*/*/all/*` / null-access-type values). Test 2's denial
(bob's schema create) does **not** produce a corresponding audit entry at all — as noted
in Test 2, that request never reaches `RangerHugeGraphPlugin.isAllowed()`. For a denial
that *does* reach Ranger, see Test 3b, which confirms a `"result":0` entry for the
per-label deny.

---

## Step 8 — Verify Policy Caching

Test that HugeGraph continues to serve Ranger policies even if Ranger Admin goes down (uses local cache).

```bash
# Stop Ranger Admin
docker compose -f docker/ranger-integration/docker-compose.yml stop ranger

# Bob can still read vertices (served from local cache)
curl -s --compressed -u bob:bobpass \
  http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/graph/vertices
```

Because the per-request check now consults `RangerBasePlugin.isAccessAllowed()` directly
against the locally cached policy (not just the once-at-login role fetch), this also confirms
per-request enforcement itself keeps working while Ranger Admin is down — check the audit log
(`docker exec hugegraph tail -n 5 /var/log/ranger/audit/hugegraph_ranger_audit.log`) for a fresh entry
with real `resource`/`access` values even with Ranger Admin stopped.

```bash
# Bring Ranger Admin back
docker compose -f docker/ranger-integration/docker-compose.yml start ranger
```

---

## Step 9 — Test Dynamic Policy Update

Verify that a policy change in Ranger Admin takes effect without restarting HugeGraph.

Because Ranger rejects overlapping resource patterns, all users share one policy
(`hugegraph-access`, id=22 from Step 7). To add write access for bob, update that
policy via `PUT` rather than creating a new one.

```bash
# 1. Confirm bob currently cannot write schema
curl -s -o /dev/null -w "%{http_code}\n" -u bob:bobpass \
  -X POST http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/schema/vertexlabels \
  -H "Content-Type: application/json" \
  -d '{"name":"testlabel","id_strategy":"DEFAULT"}'
# Expected: 403

# 2. Get the policy id
POLICY_ID=$(curl -s -u admin:rangerR0cks! \
  "http://localhost:6080/service/plugins/policies?serviceName=hugegraph&policyName=hugegraph-access" \
  | python3 -m json.tool | grep '"id"' | head -1 | grep -o '[0-9]*')
echo "Policy id: $POLICY_ID"

# 3. Update the policy — add write access for bob alongside existing read
curl -s -u admin:rangerR0cks! \
  -X PUT "http://localhost:6080/service/plugins/policies/$POLICY_ID" \
  -H "Content-Type: application/json" \
  -d "{
    \"id\": $POLICY_ID,
    \"name\": \"hugegraph-access\",
    \"service\": \"hugegraph\",
    \"isEnabled\": true,
    \"isAuditEnabled\": true,
    \"resources\": {
      \"graphspace\":    {\"values\": [\"*\"], \"isExcludes\": false, \"isRecursive\": false},
      \"graph\":         {\"values\": [\"*\"], \"isExcludes\": false, \"isRecursive\": false},
      \"resource-type\": {\"values\": [\"*\"], \"isExcludes\": false, \"isRecursive\": false},
      \"label\":         {\"values\": [\"*\"], \"isExcludes\": false, \"isRecursive\": false}
    },
    \"policyItems\": [
      {
        \"users\": [\"bob\"], \"groups\": [],
        \"accesses\": [{\"type\": \"read\", \"isAllowed\": true}, {\"type\": \"write\", \"isAllowed\": true}],
        \"conditions\": [], \"delegateAdmin\": false
      },
      {
        \"users\": [\"alice\"], \"groups\": [],
        \"accesses\": [{\"type\": \"admin\", \"isAllowed\": true}],
        \"conditions\": [], \"delegateAdmin\": false
      },
      {
        \"users\": [\"admin\"], \"groups\": [],
        \"accesses\": [
          {\"type\": \"read\", \"isAllowed\": true},
          {\"type\": \"write\", \"isAllowed\": true},
          {\"type\": \"delete\", \"isAllowed\": true},
          {\"type\": \"execute\", \"isAllowed\": true},
          {\"type\": \"admin\", \"isAllowed\": true}
        ],
        \"conditions\": [], \"delegateAdmin\": false
      }
    ]
  }"

# 4. Wait for policy refresh (default: 30 seconds)
sleep 35

# 5. Retry — bob can now create schema
curl -s -o /dev/null -w "%{http_code}\n" -u bob:bobpass \
  -X POST http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/schema/vertexlabels \
  -H "Content-Type: application/json" \
  -d '{"name":"testlabel","id_strategy":"DEFAULT"}'
# Expected: 201
```

---

## Observability

### Plugin startup log

```bash
docker logs hugegraph | grep -i ranger
# Should show:
# RangerHugeGraphPlugin started for service 'hugegraph'
# RangerHugeGraphAuthenticator initialised, Ranger service=hugegraph
```

### Policy reload log

```bash
docker logs hugegraph | grep -i "policy"
```

### Check local policy cache

```bash
docker exec hugegraph ls /tmp/ranger/cache/hugegraph/
# hugegraph_hugegraph.json   ← cached policy file
```

### Audit logs

**Ranger Admin audit:** Logged to Solr. View via Ranger Admin UI (Audit tab) or query Solr directly:

```bash
# Query Solr for recent audit events
curl -s 'http://localhost:8983/solr/ranger_audits/select?q=*:*&rows=10' | jq .response.docs[]

# Check Solr collection status
curl -s 'http://localhost:8983/solr/admin/cores?action=STATUS&core=ranger_audits' | jq .status.ranger_audits
```

**HugeGraph plugin audit:** Logged to file. View from the HugeGraph container:

```bash
docker exec hugegraph tail -f /var/log/ranger/audit/hugegraph_ranger_audit.log
```

Each request is now checked against Ranger with the real graphspace/graph/resource-type/label
being accessed (not a hardcoded wildcard), and the audited access type is populated correctly.
For example, bob reading a vertex with label `person` in graph `hugegraph`/graphspace `DEFAULT`
produces an audit entry like:

```json
{"repo":"hugegraph","sess_exprtime":1725106832,"access":"read","reqUser":"bob","cliIP":"172.21.0.1","action":"read","resource":"DEFAULT/hugegraph/vertex/person","result":1,"agent":"curl","policy":22,"role":"ROLE_SEARCHER"}
```

Before the per-request enforcement fix, every entry showed `"access":""`/`"action":null` and a
generic `"resource":"*/*/all/*"` regardless of what was actually accessed. The Ranger Admin
UI's Audit tab shows the same underlying fields under its "Access Type" and "Resource Path"
columns, so use those to confirm real values are now flowing through, e.g.
`Resource Path=DEFAULT/hugegraph/vertex/person`, `Access Type=read`.

**Note on "Resource Type":** unlike Resource Path and Access Type, the audited `resType` /
"Resource Type" column will **always** read `label`, never `vertex`/`edge`/`schema`. This is
not a bug in the plugin — `RangerDefaultAuditHandler` populates `resType` from
`RangerAccessResource.getLeafName()`, which Ranger derives from the service definition's
*deepest configured resource level name* (`ranger-servicedef-hugegraph.json` defines
`graphspace → graph → resource-type → label`, so `label` is always the leaf). HugeGraph's own
notion of resource type (`VERTEX`/`EDGE`/`SCHEMA`/...) is carried instead in the third segment
of the `resource` / "Resource Path" field (`.../vertex/person` above) — that is the field to
check when validating that the real resource type reached Ranger, not "Resource Type".

The `result` field indicates: `1` = allowed, `0` = denied (`RangerDefaultAuditHandler` maps
`RangerAccessResult.getIsAllowed()` to `1`/`0` respectively).

**v1 limitation:** bulk traversal / iterator-based reads (e.g. paging through many vertices or
edges from a single request) are rate-limited before reaching the Ranger authorizer, so they do
not produce one audit event per element — only a bounded sample per (user, graphspace, graph,
resource-type) window. This avoids multiplying audit-event volume (and Ranger policy-engine
calls) by thousands per request. Elements outside the sampled rate fall back to the local
`RolePermission` match only (the coarse permission level from login) rather than a fresh
per-element Ranger check; every element still passes the local check either way, so this is
a sampling trade-off on fine-grained per-element enforcement/audit during iteration, not a
security hole for the single-request checkpoints (schema, single-element read/write, Gremlin
execution) that this fix targets.

---

## Teardown

```bash
docker compose -f docker/ranger-integration/docker-compose.yml down -v   # -v removes volumes (PostgreSQL data)
```

---

## Audit Configuration

Two layers of audit configuration prevent Solr errors:

### 1. Ranger Admin audit (Dockerfile.ranger)

The custom Ranger image (`Dockerfile.ranger`) disables Solr auditing at build time by modifying Ranger Admin's core configuration files:
- Sets `ranger.audit.source.type=db` in `ranger-admin-site.xml`
- Sets `xasecure.audit.destination.solr=false` in `ranger-audit-site.xml`
- Enables `xasecure.audit.destination.db=true` for database audit backend

This prevents the `Error running solr query` error that occurs when Ranger Admin tries to query a non-existent Solr instance.

### 2. HugeGraph plugin audit (ranger-hugegraph-security.xml)

The HugeGraph Ranger plugin is configured to log audit events to a local file:
- `xasecure.audit.is.enabled=true` — enables audit logging
- `xasecure.audit.destination.file=true` — audit to file
- `xasecure.audit.destination.solr=false` — disable Solr
- `xasecure.audit.destination.file.dir=/var/log/ranger/audit` — log directory

**Why this approach:**
- No Solr container needed — simpler deployment for testing.
- Ranger Admin audits to PostgreSQL database; HugeGraph plugin audits to file.
- Both audit trails are preserved without external dependencies.
- If you want Solr integration in the future, modify `Dockerfile.ranger` to use a Solr backend and add a Solr service.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Error running solr query` / `IOException: ranger-solr:8983` in Ranger Admin logs | Ranger Admin trying to audit to non-existent Solr | Rebuild the custom Ranger image: `docker compose -f docker/ranger-integration/docker-compose.yml down -v && docker compose -f docker/ranger-integration/docker-compose.yml up -d --build` — `Dockerfile.ranger` patches the image to disable Solr |
| `ClassNotFoundException: RangerHugeGraphAuthenticator` or `NoClassDefFoundError: org/apache/hadoop/conf/Configuration` | Image built without `-Dwith-ranger-plugin`, or with a stale cached layer | Confirm the jar is in the container: `docker exec hugegraph ls lib/ \| grep ranger`. If missing, rebuild: `docker compose -f docker/hgRangerIntegration/docker-compose.yml build --no-cache hugegraph` |
| All users denied after Ranger Admin restart | Policy cache missing | Check `/tmp/ranger/cache/hugegraph/` exists and is writable |
| Policy changes not taking effect | Poll interval not elapsed | Default is 30 s; reduce `ranger.plugin.hugegraph.policy.pollIntervalMs` for testing |
| `no service found with name[hugegraph]` when creating a policy | Service instance was never created because `implClass` blocked it | Follow step 5b to clear `implClass`, then redo step 5c |
| Ranger Admin returns 401 | Wrong password | Ranger Admin password is `rangerR0cks!` — hardcoded in the image, not overridable via env var |
| HugeGraph container exits immediately | Bad `rest-server.properties` mount | Run `docker logs hugegraph` and check for config parse errors |
| Ranger Admin slow to start | DB initialisation takes time | The `start_period` is 180 s — wait up to 3 min before assuming a failure |
| Audit log file not appearing | Permissions issue or config not applied | Run `docker exec hugegraph ls -la /var/log/ranger/audit/` to check directory exists and is writable. Rebuild container if config is not applied: `docker compose -f docker/ranger-integration/docker-compose.yml up -d --build` |
| Schema creation returns `400` unexpectedly (e.g. Test 4) | Label name already exists (`ExistedException`, HTTP 400) — not a Ranger denial | Use a name not already created earlier in the guide (e.g. `person2` instead of `person`), or check `/schema/vertexlabels` for existing labels first |
