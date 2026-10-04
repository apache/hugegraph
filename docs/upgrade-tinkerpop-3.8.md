# TinkerPop 3.8.1 and Groovy 4 upgrade

This upgrade moves Server and the distributed modules from TinkerPop 3.5.1 to
3.8.1 and uses Groovy 4.0.25. Build and run every module with Java 17.

## Configuration and clients

Use the Gremlin configuration files shipped with the upgraded Server. Serializer
classes move from `org.apache.tinkerpop.gremlin.driver.ser` to
`org.apache.tinkerpop.gremlin.util.ser`. The supplied remote configurations
include the corresponding HugeGraph registries.

Untyped GraphSON remains first in the serializer list so ordinary JSON requests
keep their existing response shape. Typed GraphSON is available as a fallback
for the supported simple values and IDs; it is not a replacement for untyped
serialization of every HugeGraph internal schema or graph object.

GraphBinary uses `HugeGraphTypeSerializerRegistryBuilder`. Cypher results normalize
HugeGraph IDs to their underlying values, including values nested in collections,
maps and paths. Cyclic results and nesting deeper than 32 levels are rejected.

The default script engine remains Gremlin Groovy. This upgrade does not switch
remote requests to GremlinLang. Validate application scripts against the full
TinkerPop 3.5.1 to 3.8.1 upgrade interval before deploying.

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
`P.typeOf()` runs as a local filter for system and schema properties, including
inside negated or connective predicates. Its type operands are preserved without
schema value conversion. Missing properties do not match, including when the
type predicate is negated. Type filters do not use backend indexes on their own.
The new TinkerPop step APIs are adapted without discarding property metadata.

## Compatibility verification

The upgrade CI explicitly selects Structure and Process tests for Memory,
RocksDB and HStore. HStore has separate standard-process and feature-test steps.
Provider lifecycle checks run against the selected backend, including reopening
the graph after cleanup and after closing the provider context. Each selected
Structure or Process suite must report at least one executed test; a report
containing only skipped tests fails the gate.
A green build with those steps skipped is not compatibility evidence. Review
reports from the actual PR revision before deploying.
