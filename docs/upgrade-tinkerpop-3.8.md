# TinkerPop 3.8.1 and Groovy 4 upgrade

This upgrade moves Server and the distributed modules from TinkerPop 3.5.1 to
3.8.1 and uses Groovy 4.0.25. Build and run every module with Java 17.

## Configuration and clients

Use the Gremlin configuration files shipped with the upgraded Server. Serializer
classes move from `org.apache.tinkerpop.gremlin.driver.ser` to
`org.apache.tinkerpop.gremlin.util.ser`. The supplied remote configurations
include the corresponding HugeGraph registries.

The Java Driver sample `gremlin-driver-settings.yaml` and Gremlin Console's
`remote.yaml` now default to GraphBinary V1. The serializer entry changes as
follows:

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

GraphBinary preserves graph elements and UUID IDs for Java Driver accessors such
as `Result.getVertex()` and `Result.get(UUID.class)`. Untyped GraphSON V1 decodes
these results as maps and strings.

`remote-objects.yaml` serves the Cypher HTTP JSON path and continues to use
`org.apache.tinkerpop.gremlin.util.ser.GraphSONUntypedMessageSerializerV1` with
`HugeGraphIoRegistry`. It retains its duplicate registry entry as a TinkerPop
driver compatibility workaround.

For the server, keep the GraphBinary builder and registry. The serializer order
is GraphBinary, untyped GraphSON V1/V2/V3, then typed GraphSON V2/V3. The
GraphSON entries keep the HugeGraph registry. The first entries look like this:

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

The full ordering is in `gremlin-server.yaml`; the config test checks that
`application/json` selects untyped V1 in each server config variant.

Typed GraphSON is available for the supported simple values and IDs, not as a
replacement for untyped serialization of every HugeGraph schema or graph
object. Typed V1 is not registered: its legacy `@class` deserialization can
construct Java objects before request authentication. V2/V3 use `@type`/`@value`.

GraphBinary uses `HugeGraphTypeSerializerRegistryBuilder` to write HugeGraph IDs
as their underlying values. Schema results use standard maps and Blob values use
standard binary values (`ByteBuffer` in the Java Driver), including when nested
in returned vertices, edges or collections. `Optional` results carry their value
or `null` when empty, including results from `graph.variables().get()`. `File`
results retain the map shape `{"file": "name"}` using the file name, including
inside nested results. Untyped GraphSON V1 keeps the legacy Tree array of `key`/`value`
entries and accepts scalar as well as element keys through the additional
`HugeGraphSONV1MessageIoRegistry` in that serializer's configuration. Keep this
registry limited to untyped V1 messages. Standard graph-file IO and typed
GraphSON serializers keep `HugeGraphIoRegistry` alone and use the TinkerPop
Tree format for their negotiated version.

HugeGraph enums such as `DataType.TEXT` and `Directions.OUT` return their names
over GraphBinary, including in maps and lists. `graph.schema()` returns a map
with `propertykeys`, `vertexlabels`, `edgelabels` and `indexlabels` lists, using
the same schema conversion. TinkerPop enums retain their native wire types.
Backend `Shard` values, including `graph.metadata(HugeType.VERTEX, 'splits', size)`,
return standard maps with `start`, `end` and an exact 64-bit `length`, including
inside lists and maps. The default Driver does not need a private Shard serializer.

Local edge ID filters accept both serialized strings and native edge IDs in
mixed collections: `hasId(not(without(ids)))` and `hasId(within(ids))` agree on
membership and count. Numeric and string vertex IDs keep their distinct types.

Groovy interpolated strings (`GString`, including subclasses) return as standard
strings over GraphBinary, including inside lists and maps. For example,
`def value = 42; "r3-value:${value}"` returns `"r3-value:42"`. The untyped GraphSON
path retains the legacy GString bean map with its `values` and `strings` fields.

Cypher extension predicates use the current TinkerPop predicate constructor
through the existing translator extension point. Computed-expression regex
filters retain whole-string matching (`String.matches`), so `"marko"` matches
`"mar.*"` and does not match `"ark"`.
The public `cypherRegex`, `cypherIsString`, `cypherIsNode` and `cypherIsRelationship`
factories imported by the bundled Gremlin configuration use that same adapter,
so the Gremlin translation returned by `EXPLAIN` can be replayed.

Cypher results normalize HugeGraph IDs, including
values nested in collections, maps and paths. Cyclic results and nesting deeper
than 32 levels are rejected.

The default script engine remains Gremlin Groovy. This upgrade does not switch
remote requests to GremlinLang. Validate application scripts against the full
TinkerPop 3.5.1 to 3.8.1 upgrade interval before deploying.

## YAML configuration safety

Jackson JSON and YAML modules share the root BOM version. Fabric8 kubeconfig
extensions can contain integer and decimal values; the K8s configuration
regression verifies auto-configuration without contacting a cluster.

This TinkerPop 3.8.1 upgrade uses SnakeYAML 2.2, Spring Boot 2.5.15 and
Spring Framework 5.3.27. The Boot patch keeps PD and Store's YAML startup
loader compatible with SnakeYAML 2.x. These versions belong to the upgraded
runtime; do not apply the SnakeYAML override to a TinkerPop 3.5.1 deployment.
The runtime also manages Jackson YAML 2.15.2 so Fabric8 can read kubeconfig
with SnakeYAML 2.2. Store's Log4j SLF4J binding and CLI logging use 2.18.0;
Store Node retains its Boot-managed Log4j core version.

Quota templates and PD-delivered Store configuration accept standard YAML
maps, lists and scalars. Custom Java object tags such as `!!com.example.Type`
are rejected before object construction. Keep these files as configuration
data; Java class names used as ordinary string values remain supported.

## List property compatibility

The public TinkerPop `Graph.Features` contract changes:
`features().vertex().properties().supportsUniformListValues()` and
`features().edge().properties().supportsUniformListValues()` now return `false`
instead of `true`. Clients and compatibility tests that inspect these flags may
stop enabling uniform-list operations. The corresponding graph-variable flag
remains `true`.

HugeGraph still supports vertex and edge properties whose schema declares
`LIST` cardinality with `valueList()` and a supported element type, for example
`schema.propertyKey("tags").asText().valueList().create()`. Values must match the
schema's element type. This schema capability does not provide arbitrary Java
`List` values for `SINGLE` properties. The feature-reporting change itself does
not remove stored lists or require rewriting existing list data.

When upgrading, review client feature-detection branches. Applications using
HugeGraph list properties should retain their explicit element type and `LIST`
schema, and verify list writes and reads with that schema. Generic TinkerPop
clients should honor the reported feature flags; do not treat a skipped
uniform-list compatibility test as validation of HugeGraph's schema list path.

## Queries

See [query count and filter behavior](query-semantics.md) for transaction counts
and local predicate filtering. Version-specific handling retains negated
predicates and filtering barriers where pushdown would change their meaning.
For UUID IDs, local negated filters such as `hasId(P.not(P.neq(uuid)))` retain
the same matches as ID equality. This also applies when an ID filter stays
local beside a property filter that cannot be pushed down. The complete
`HasStep` remains local;
string and numeric ID comparisons keep their existing semantics.
`P.typeOf()` runs as a local filter for system and schema properties, including
inside negated or connective predicates. Its type operands are preserved without
schema value conversion. Missing properties do not match, including when the
type predicate is negated. Type filters do not use backend indexes on their own.
The new TinkerPop step APIs are adapted without discarding property metadata.

Named `GValue` predicates retain their bindings when reused or cloned, and
`updateVariable()` applies when the predicate builds subsequent traversals. Explicit named
`limit()`/`range()` bounds have the same count behavior as literal bounds.
Local String ID comparisons retain their leaf semantics inside `not`, `and`
and `or` predicates. Mixed ID collections retain the typed matches of
strings, numbers and UUIDs when a filter stays local; all-string collections
keep standard string-ID comparison behavior. Fractional numeric IDs are not
truncated to integer IDs.

Count filtering is only truncated when every predicate branch has a supported
bound that preserves the filter result. Unsupported negated collection or
custom predicate branches keep the complete count, including inside `and` or
`or` conditions.

DATE schema properties accept TinkerPop `OffsetDateTime` values for writes and
indexed equality or range queries. Values normalize by instant to the existing
`java.util.Date` millisecond representation; offsets do not change the stored
instant, and sub-millisecond precision is truncated. Existing DATE data needs
no migration.

## Read-only upgrade smoke

Run these against an existing graph, replacing the label, keys and values with
ones present in that graph. The queries do not write graph or schema data.
Use property keys with the query indexes required by your backend.
The `age` example assumes an integer property; the path example needs an
existing outgoing edge.

```groovy
g.V().hasLabel('person').has('name', 'marko').count()
g.V().hasLabel('person').has('age', P.gt(0)).order().by('age').range(0, 2).values('age').toList()
g.V().has('name', 'marko').limit(1).out().limit(1).path().toList()
g.V().has('name', 'marko').limit(1).id().toList()
```

The first result is a numeric count. The second is a list of at most two numeric
values in ascending order. The third is a list of paths; untyped GraphSON
represents each path with `labels` and `objects`. The fourth is a list of graph
IDs encoded as scalar values by GraphSON or GraphBinary; the scalar type follows
the graph's ID type.

For Cypher, send this read-only query to
`POST /graphspaces/{graphspace}/graphs/{graph}/cypher`, substituting existing
graph data:

```cypher
MATCH (n:person) WHERE n.name = 'marko' RETURN id(n) AS nodeId
```

The response has rows under `result.data`; each row is a map and `nodeId` is a
JSON scalar, not a HugeGraph internal ID class. The Cypher API test covers this
shape, nested IDs and paths.

## Compatibility verification

The repository's checked-in tests establish the following boundaries:

| Path | Evidence | Not established by these tests |
|---|---|---|
| GraphSON | Config, MIME and serializer fixtures. | External client/version, network and auth matrix. |
| GraphBinary | Shipped Driver config, typed Driver result access, and in-process ID, predicate and element round trips. | External client, network and auth. |
| Cypher HTTP | Local API test checks IDs and paths with Basic auth. | Bearer or external REST client/version matrix. |

Config, GraphSON and GraphBinary evidence comes from
`GremlinConfigCompatibilityTest`. The Cypher evidence comes from
`CypherApiTest`, which uses the local API harness and its Basic-auth test client.

The upgrade CI explicitly selects Structure and Process tests for Memory,
RocksDB and HStore. HStore has separate standard-process and feature-test steps.
Provider lifecycle checks run against the selected backend, including reopening
the graph after cleanup and after closing the provider context. Each selected
Structure or Process suite must report at least one executed test; a report
containing only skipped tests fails the gate.
A green build with those steps skipped is not compatibility evidence. Review
reports from the actual PR revision before deploying. The serializer tests use
the project's TinkerPop 3.8.1 dependency; they do not define a supported external
client matrix for version, GraphSON or GraphBinary, and authentication. Verify
the combinations used by your deployment.
