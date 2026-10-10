# Server runtime and query contracts

This guide covers Server configuration, wire formats, query semantics and transaction
ownership. Use it when upgrading applications or changing the
Server runtime. Deployment instructions remain in the [build guide](BUILDING.md)
and [Server module guidance](../hugegraph-server/AGENTS.md).

## Runtime and client configuration

The runtime uses TinkerPop 3.8.1 and Groovy 4.0.25. Build and run every module with Java
17. Applications migrating from TinkerPop 3.5.1 must review the full version interval,
including the configuration changes below.

### Launcher GC options

Use `bin/start-hugegraph.sh -g g1` (or `-g G1`) to select G1 explicitly.
`-g zgc` and `-g ZGC` select ZGC; other values are rejected. Without `-g`, the
launcher preserves the JVM's collector choice or the collector set by the caller in
`JAVA_OPTIONS`. When `JAVA_OPTIONS` is empty, `-j "JVM_OPTIONS"` supplies the caller
options instead.

For the default or G1 path, the launcher supplies `-XX:+ParallelRefProcEnabled`,
`-XX:InitiatingHeapOccupancyPercent=50` and `-XX:G1RSetUpdatingPauseTimePercent=5`
before the caller's options, so caller values override these tuning defaults. For
example, `bin/start-hugegraph.sh -g g1 -j "-XX:InitiatingHeapOccupancyPercent=45"`
uses G1 with an occupancy threshold of 45 when `JAVA_OPTIONS` is empty.

Do not combine explicit `-g` selection with a different collector in `JAVA_OPTIONS`
or `-j`; the JVM rejects conflicting collector selections.

### Serializer configuration

Use the Gremlin configuration files shipped with the upgraded Server. Serializer classes
move from `org.apache.tinkerpop.gremlin.driver.ser` to
`org.apache.tinkerpop.gremlin.util.ser`. The supplied remote configurations include the
corresponding HugeGraph registries.

The Java Driver sample `gremlin-driver-settings.yaml` and Gremlin Console's
`remote.yaml` now default to GraphBinary V1. The serializer entry changes as follows:

```diff
- className: org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV1d0
+ className: org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1
```

Keep the HugeGraph builder and registry in both client configurations:

```yaml
serializer:
  className: org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1
  config:
    serializeResultToString: false
    builder: org.apache.hugegraph.io.HugeGraphTypeSerializerRegistryBuilder
    ioRegistries: [org.apache.hugegraph.io.HugeGraphIoRegistry]
```

GraphBinary preserves graph elements and UUID IDs for Java Driver accessors such as
`Result.getVertex()` and `Result.get(UUID.class)`. Untyped GraphSON V1 decodes these
results as maps and strings.

`remote-objects.yaml` serves the Cypher HTTP JSON path and continues to use
`org.apache.tinkerpop.gremlin.util.ser.GraphSONUntypedMessageSerializerV1` with
`HugeGraphIoRegistry`. It retains its duplicate registry entry as a TinkerPop driver
compatibility workaround.

For the server, keep the GraphBinary builder and registry. The serializer order is
GraphBinary, untyped GraphSON V1/V2/V3, then typed GraphSON V2/V3. The GraphSON entries
keep the HugeGraph registry. The first entries look like this:

```yaml
serializers:
  - className: org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1
    config:
      serializeResultToString: false
      builder: org.apache.hugegraph.io.HugeGraphTypeSerializerRegistryBuilder
      ioRegistries: [org.apache.hugegraph.io.HugeGraphIoRegistry]
  - className: org.apache.tinkerpop.gremlin.util.ser.GraphSONUntypedMessageSerializerV1
    config:
      serializeResultToString: false
      ioRegistries: [org.apache.hugegraph.io.HugeGraphIoRegistry,
                     org.apache.hugegraph.io.HugeGraphSONV1MessageIoRegistry]
```

The full ordering is in `gremlin-server.yaml`. `application/json` selects untyped V1 in
the supplied server configuration variants.

Typed GraphSON is available for the supported simple values and IDs, not as a
replacement for untyped serialization of every HugeGraph schema or graph object. Typed
V1 is not registered: its legacy `@class` deserialization can construct Java objects
before request authentication. V2/V3 use `@type`/`@value`.

GraphBinary uses `HugeGraphTypeSerializerRegistryBuilder` to write HugeGraph IDs as
their underlying values. Schema results use standard maps and Blob values use standard
binary values (`ByteBuffer` in the Java Driver), including when nested in returned
vertices, edges or collections. `Optional` results carry their value or `null` when
empty, including results from `graph.variables().get()`. `File` results retain the map
shape `{"file": "name"}` using the file name, including inside nested results. Untyped
GraphSON V1 keeps the legacy Tree array of `key`/`value` entries and accepts scalar as
well as element keys through the additional `HugeGraphSONV1MessageIoRegistry` in that
serializer's configuration. Keep this registry limited to untyped V1 messages. Standard
graph-file IO and typed GraphSON serializers keep `HugeGraphIoRegistry` alone and use
the TinkerPop Tree format for their negotiated version.

HugeGraph enums such as `DataType.TEXT` and `Directions.OUT` return their names over
GraphBinary, including in maps and lists. `graph.schema()` returns a map with
`propertykeys`, `vertexlabels`, `edgelabels` and `indexlabels` lists, using the same
schema conversion. TinkerPop enums retain their native wire types. Backend `Shard`
values, including `graph.metadata(HugeType.VERTEX, 'splits', size)`, return standard
maps with `start`, `end` and an exact 64-bit `length`, including inside lists and maps.
The default Driver does not need a private Shard serializer.

Local edge ID filters accept both serialized strings and native edge IDs in mixed
collections: `hasId(not(without(ids)))` and `hasId(within(ids))` agree on membership and
count. Numeric and string vertex IDs keep their distinct types.

Groovy interpolated strings (`GString`, including subclasses) return as standard strings
over GraphBinary, including inside lists and maps. For example, `def value = 42;
"value:${value}"` returns `"value:42"`. The untyped GraphSON path retains the
legacy GString bean map with its `values` and `strings` fields.

Cypher extension predicates use the TinkerPop predicate constructor through the
translator extension point. Computed-expression regex filters retain whole-string
matching (`String.matches`), so `"marko"` matches `"mar.*"` and does not match `"ark"`.
The public `cypherRegex`, `cypherIsString`, `cypherIsNode` and `cypherIsRelationship`
factories imported by the bundled Gremlin configuration use that same adapter, so the
Gremlin translation returned by `EXPLAIN` can be replayed.

Cypher results normalize HugeGraph IDs, including values nested in collections, maps and
paths. Cyclic results and nesting deeper than 32 levels are rejected.

The default script engine is Gremlin Groovy. Validate application scripts against the
full TinkerPop 3.5.1 to 3.8.1 upgrade interval before deploying.

## YAML configuration safety

Jackson JSON and YAML modules share the root BOM version. Fabric8 kubeconfig extensions
can contain integer and decimal values; the K8s configuration regression verifies auto-
configuration without contacting a cluster.

The TinkerPop 3.8.1 runtime uses SnakeYAML 2.2, Spring Boot 2.5.15 and Spring Framework
5.3.27. The Boot patch keeps PD and Store's YAML startup loader compatible with
SnakeYAML 2.x. These versions belong to the upgraded runtime; do not apply the SnakeYAML
override to a TinkerPop 3.5.1 deployment. The runtime also manages Jackson YAML 2.15.2
so Fabric8 can read kubeconfig with SnakeYAML 2.2. Store's Log4j SLF4J binding and CLI
logging use 2.18.0; Store Node retains its Boot-managed Log4j core version.

Quota templates and PD-delivered Store configuration accept standard YAML maps, lists
and scalars. Custom Java object tags such as `!!com.example.Type` are rejected before
object construction. Keep these files as configuration data; Java class names used as
ordinary string values remain supported.

## List property compatibility

The public TinkerPop `Graph.Features` contract changes:
`features().vertex().properties().supportsUniformListValues()` and
`features().edge().properties().supportsUniformListValues()` now return `false` instead
of `true`. Clients and compatibility tests that inspect these flags may stop enabling
uniform-list operations. The corresponding graph-variable flag remains `true`.

HugeGraph still supports vertex and edge properties whose schema declares `LIST`
cardinality with `valueList()` and a supported element type, for example
`schema.propertyKey("tags").asText().valueList().create()`. Values must match the
schema's element type. This schema capability does not provide arbitrary Java `List`
values for `SINGLE` properties. The feature-reporting change itself does not remove
stored lists or require rewriting existing list data.

When upgrading, review client feature-detection branches. Applications using HugeGraph
list properties should retain their explicit element type and `LIST` schema, and verify
list writes and reads with that schema. Generic TinkerPop clients should honor the
reported feature flags; do not treat a skipped uniform-list compatibility test as
validation of HugeGraph's schema list path.

## Query semantics

### Predicate and value compatibility

Version-specific handling retains negated predicates and filtering barriers where
pushdown would change their meaning. For UUID IDs, local negated filters such as
`hasId(P.not(P.neq(uuid)))` retain the same matches as ID equality. This also applies
when an ID filter stays local beside a property filter that cannot be pushed down. The
complete `HasStep` remains local; string and numeric ID comparisons keep their existing
semantics. `P.typeOf()` runs as a local filter for system and schema properties,
including inside negated or connective predicates. Its type operands are preserved
without schema value conversion. Missing properties do not match, including when the
type predicate is negated. Type filters do not use backend indexes on their own. The new
TinkerPop step APIs are adapted without discarding property metadata.

Named `GValue` predicates retain their bindings when reused or cloned, and
`updateVariable()` applies when the predicate builds subsequent traversals. Explicit
named `limit()`/`range()` bounds have the same count behavior as literal bounds. Local
String ID comparisons retain their leaf semantics inside `not`, `and` and `or`
predicates. Mixed ID collections retain the typed matches of strings, numbers and UUIDs
when a filter stays local; all-string collections keep standard string-ID comparison
behavior. Fractional numeric IDs are not truncated to integer IDs.

Count filtering is only truncated when every predicate branch has a supported bound that
preserves the filter result. Unsupported negated collection or custom predicate branches
keep the complete count, including inside `and` or `or` conditions.

DATE schema properties accept TinkerPop `OffsetDateTime` values for writes and indexed
equality or range queries. Values normalize by instant to the existing `java.util.Date`
millisecond representation; offsets do not change the stored instant, and sub-
millisecond precision is truncated. Existing DATE data needs no migration.

### Counts and transaction visibility

Counts made inside a transaction include that transaction's uncommitted vertex and edge
changes. Only `count()` supports this fallback; other aggregates with uncommitted
changes are rejected. Queries that combine uncommitted changes with pagination, a limit,
or an offset remain unsupported.

Queries and counts select the current transaction version before applying filters. When
an element is removed and re-added with the same ID, the new element shadows the old
updated record, even if only the old record matches the query's label.

A count query that enters an index query path remains unsupported when that index
transaction has uncommitted changes. It reports `Can't do index query when there are
changes in transaction`. This includes indexed-property conditions and label-only
queries on backends that need a label index. Graph-wide and explicit-ID counts can still
use transaction merging without entering that index path.

Unsupported text predicates remain traversal filters. For ordinary GraphStep and
VertexStep extraction, if any condition in a `HasStep` cannot be converted, the whole
step remains local, including its sibling label, ID and property conditions. Count
optimization preserves that filter step. Existing special handling around `match()` and
connective label filters remains part of traversal optimization.

A self-loop contributes two occurrences to a vertex's `bothE()` traversal and one to
each of `outE()`, `inE()` and the graph-wide `E()` traversal. Counts retain these
multiplicities before and after transaction commit, including edge updates.

Count optimization preserves steps that can filter candidates. Resetting an optimized
count traversal permits it to execute again. Query-step equality does not depend on the
result iterator from a previous execution.

A scan in the middle of a traversal is counted once per incoming traverser, including
its bulk. It is not replaced by a single backend count. For example, with three
vertices, `g.V().V().count()` returns `9L`, while `g.V().count()` returns `3L` and
remains eligible for count optimization.

### Read-only application checks

Run these against an existing graph, replacing the label, keys and values with ones
present in that graph. The queries do not write graph or schema data. Use property keys
with the query indexes required by your backend. The `age` example assumes an integer
property; the path example needs an existing outgoing edge.

```groovy
g.V().hasLabel('person').has('name', 'marko').count()
g.V().hasLabel('person').has('age', P.gt(0)).order().by('age').range(0, 2).values('age').toList()
g.V().has('name', 'marko').limit(1).out().limit(1).path().toList()
g.V().has('name', 'marko').limit(1).id().toList()
```

The first result is a numeric count. The second is a list of at most two numeric values
in ascending order. The third is a list of paths; untyped GraphSON represents each path
with `labels` and `objects`. The fourth is a list of graph IDs encoded as scalar values
by GraphSON or GraphBinary; the scalar type follows the graph's ID type.

For Cypher, send this read-only query to `POST
/graphspaces/{graphspace}/graphs/{graph}/cypher`, substituting existing graph data:

```cypher
MATCH (n:person) WHERE n.name = 'marko' RETURN id(n) AS nodeId
```

The response has rows under `result.data`; each row is a map and `nodeId` is a JSON
scalar, not a HugeGraph internal ID class. The Cypher API test covers this shape, nested
IDs and paths.

## HTTP request errors

The supplied WebSocket/HTTP channelizer releases each HTTP request buffer once,
including when a traversal alias is unknown. Such a request returns one HTTP 400, and a
later valid request on the same keep-alive connection receives its own response. This
avoids TinkerPop 3.8.1's duplicate release and second HTTP 500, which could be mistaken
for the next request's response by a connection pool. The correction applies when the
configured channelizer is TinkerPop's standard `WsAndHttpChannelizer`; HugeGraph selects
its compatibility implementation at startup and retains TinkerPop's WebSocket handling.
Custom channelizer classes keep their configured behavior. Unknown aliases still require
the caller to use a graph or traversal-source name exposed by the server.

## Cypher HTTP contracts

### Request forms

The endpoint is `/graphspaces/{graphspace}/graphs/{graph}/cypher`.

| Method and content type | Request | Behavior |
| --- | --- | --- |
| GET | `?cypher=<URL-encoded statement>` | Query-string form works |
| POST `application/json` | Raw Cypher text | Legacy raw-body form works |
| POST `text/plain` | Any body | Rejected with HTTP 415 |
| POST `application/json` | JSON object below | Separate bindings work |

```json
{
  "cypher": "MATCH (n:person) WHERE n.name = $name RETURN n.name",
  "parameters": {"name": "marko"}
}
```

POST bodies containing JSON arrays, strings, numbers, booleans, or null are rejected
with HTTP 400. Malformed JSON objects and trailing tokens are also request errors and do
not fall back to raw Cypher.

For the JSON-object form, `cypher` must be a nonblank string and `parameters`, when
present, must be an object. Omitting `parameters` means an empty map. Missing bindings
produce an execution error; an explicit null value is accepted. Bindings may be named
`id` and `label`. There is a fixed limit of 16 top-level parameters. This Cypher
processor does not support a `maxParameters` configuration setting. Values containing
quotes and newlines remain bound data rather than changing query text.

A Map counts as one top-level parameter. Group related values into one Map and reference
its fields explicitly; the vertex label and required properties must match the graph's
existing schema:

```json
{
  "cypher": "CREATE (n:person {name:$props.name, age:$props.age, city:$props.city}) RETURN n.name",
  "parameters": {"props": {"name": "new-person", "age": 20, "city": "Beijing"}}
}
```

Do not use the shorthand `CREATE (n:person $props)`: the pinned translator does not
assign the bound Map's properties in that path. The parameter limit counts top-level
bindings, not the entries inside a bound Map or a returned Map. A Map containing 17
entries can be bound through one parameter with explicit property references; 17 top-level
bindings are rejected.

The translator's reserved null-marker string `"  cypher.null"` (two leading spaces) is
rejected in binding values, including nested maps and lists, to avoid silently
converting a user string to null. Actual JSON null remains accepted. The same marker in
query literals or stored properties is an existing translator limitation and is outside
this compatibility target. Check the HTTP status and body `status.code`: execution
failures retain HTTP 200 with `status.code` 400 and null result data.

Traversal iteration failures, including Java errors, attempt rollback on the execution
thread and return a terminal failure response. A rollback exception is attached to the
original failure.

### Compatibility coverage

The API fixture uses a strong schema with a `SECONDARY` index on `city` and a `RANGE`
index on `age`. The regression suite covers:

| Area | Covered behavior |
|---|---|
| Reads | Label scans, equality and range predicates, computed regex whole-string matching, boolean combinations, directed one- and two-hop patterns, empty results. |
| Results | Aliases, scalar and node values, nested maps/lists, relationship IDs, path shape, null and missing-property semantics. |
| Aggregation and pagination | `DISTINCT`, `count`/`sum`/`min`/`max`/`avg`, `ORDER BY`, `SKIP`, and `LIMIT`. |
| Parameters | String, number, boolean, null, empty and missing bindings; quote/newline preservation. |
| Writes | Vertex and edge `CREATE`, property `SET`, edge and vertex `DELETE`, with native REST readback. |
| Failures and routing | Invalid syntax, request shape, binding keys, schema values, authentication, and graph routing. |
| Failed writes | A rejected multi-vertex create leaves no residue in later Cypher and native reads. |

Write regressions check persisted state through native REST reads. Applications are not
required to issue native reads after each successful Cypher write.

### Scope and regression sources

Advanced constructs including `MERGE`, `OPTIONAL MATCH`, `WITH`, `UNWIND`, `UNION`, and
variable-length paths are not established by the compatibility coverage described here.
HStore-specific behavior, Bolt, cross-request transactions, external client versions and
performance also require separate validation. This guide does not claim full openCypher
or Neo4j compatibility.

The pinned translator is `org.opencypher.gremlin:translation:1.0.4`. Its translation
behavior defines the limitations documented above.

Long-term regression sources:

- [`CypherApiTest`](../hugegraph-server/hugegraph-test/src/main/java/org/apache/hugegraph/api/CypherApiTest.java)
  covers request forms, schema-aware reads and writes, result shapes,
  authentication and graph routing, and failed-write isolation.
- [`CypherClientTest`](../hugegraph-server/hugegraph-test/src/main/java/org/apache/hugegraph/api/cypher/CypherClientTest.java)
  covers result normalization and HTTP client behavior.
- [`CypherOpProcessorTest`](../hugegraph-server/hugegraph-test/src/main/java/org/apache/hugegraph/opencypher/CypherOpProcessorTest.java)
  covers binding validation and execution failure handling.

## Transaction and graph ownership

Server releases the current thread's graph transactions when a REST request finishes and
when an authenticated context task exits. Cleanup visits every registered graph even
when one graph fails to close, and reports failures. Cleanup explicitly rolls back
unfinished writes, including when `onClose(COMMIT)` was configured, then releases
backend transactions and resets thread-local transaction behavior and listeners.
Applications must commit successful writes before leaving the request/task boundary.
Closing an OLTP traverser preserves the caller's transaction so that the caller can
still commit or roll it back.

The cleanup also applies when schema caches have become cold. Deleting an auth project
or user must remove its associated access or belong edges while preserving unrelated
relationships. The special OLAP vertex path remains separate; negative schema IDs alone
do not identify edges that can be skipped.

Shared schema and element caches retain their invalidation listeners until the graph
closes. Request cleanup releases backend leases while preserving those graph caches and
their schema identity.

Task database workers release their owned task and graph transactions on the worker
thread. A failure while closing one owner must not skip cleanup of other owners or
server-info resources. Cleanup retains the first exception and attaches later cleanup
failures as suppressed exceptions.

Graph close attempts auth, scheduler and caller-transaction cleanup separately. If a
scheduler remains registered or the graph reports active transactions, the graph remains
open for a later close attempt. Pending local tasks retain their task database transaction
so they can persist completion before the next close attempt. An incomplete scheduler
drain does not queue transaction cleanup behind that scheduler's blocked worker. Stopping
distributed task dispatch is distinct from completing drain: a timeout does not make
the next close return success.

Once these logical checks pass, graph close releases shared cache listeners, invokes
`provider.close()` and destroys graph locks even if an earlier cleanup step reported an error. The error
remains visible to the caller. Logical graph closure does not prove that a failed
backend native-resource release succeeded; callers must inspect the reported failure
rather than infer physical release from graph state.

Regression sources:

- [`TransactionLifecycleTest`](../hugegraph-server/hugegraph-test/src/main/java/org/apache/hugegraph/auth/TransactionLifecycleTest.java)
  exercises request and authenticated-task ownership and rollback.
- [`StandardTaskSchedulerTxTest`](../hugegraph-server/hugegraph-test/src/main/java/org/apache/hugegraph/task/StandardTaskSchedulerTxTest.java)
  exercises task worker transaction cleanup.
- [`HugeFactoryTest`](../hugegraph-server/hugegraph-test/src/main/java/org/apache/hugegraph/HugeFactoryTest.java)
  exercises graph close failure continuation and distributed drain retry.

## Compatibility evidence and deployment checks

Checked-in regression tests define a coverage boundary, not a supported matrix for every
external client, authentication method or backend deployment.

| Path | Regression coverage | Deployment checks still required |
|---|---|---|
| GraphSON | Serializer configuration, MIME selection and wire fixtures. | External client versions, network and authentication combinations. |
| GraphBinary | Shipped Driver configuration, typed results and in-process ID, predicate and element round trips. | External client, network and authentication combinations. |
| Cypher HTTP | Local API harness, result IDs and paths, Basic-auth test client. | Bearer authentication and external REST clients. |

[`GremlinConfigCompatibilityTest`](../hugegraph-server/hugegraph-test/src/main/java/org/apache/hugegraph/unit/config/GremlinConfigCompatibilityTest.java)
contains serializer and configuration regressions. Use the Cypher sources linked above
for API behavior. Keep runtime dependency versions aligned with the root POM and shipped
configuration rather than copying old configuration fragments into a different runtime.

Structure and Process suites exercise TinkerPop behavior on Memory, RocksDB and HStore.
HStore has separate standard-process and feature-test selections.
[`HugeGraphProviderLifecycleTest`](../hugegraph-server/hugegraph-test/src/main/java/org/apache/hugegraph/tinkerpop/HugeGraphProviderLifecycleTest.java)
checks backend cleanup and graph reopening. The test selections and setup are
maintained in the [test POM](../hugegraph-server/hugegraph-test/pom.xml),
[Server CI](../.github/workflows/server-ci.yml) and
[PD/Store CI](../.github/workflows/pd-store-ci.yml).

Inspect executed tests for the backend and protocols used by the deployment. Selected
Structure or Process suites must execute tests; an all-skipped report is not
compatibility evidence. Run the read-only application examples against representative
existing data and validate the serializers, client versions and authentication
combinations actually used by the application.
