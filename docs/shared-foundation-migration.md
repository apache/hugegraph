# Shared foundations: preparing for the 1.8.0 migration

The planned 1.8.0 consolidation changes Java imports and extension contracts, and requires a coordinated Server, PD and Store upgrade. Use this guide to identify affected code, check historical-data compatibility and prepare your deployment.

Shared implementations move out of Server core so Server and Store can use the same types and codecs. This guide covers the planned 1.8.0 migration.

## What do I need to change?

| If you maintain… | Your next step |
|------------------|----------------|
| A REST client, Loader integration or client DTO | Check actual affected Java calls; REST use alone does not require replacing client DTOs with struct types. |
| Java code importing Server IDs, schema, queries or indexes | Update the imports and affected signatures below, then recompile against matching 1.8.0 artifacts. |
| A `HugeGraph` implementation, serializer or element subclass | Adapt the API/SPI contracts below; old compiled implementations are not binary-compatible. |
| Custom Gremlin scripts or `classImports` | Change the `IdGenerator` class name and verify it in the packaged Gremlin service. |
| Hubble, Toolchain or Computer | Apply the downstream exception/classpath changes and validate the built jars. Use matching downstream artifacts and check their packaged classpaths. |
| An HStore installation | Check OLAP writes, Store-rebuilt indexes and metadata namespaces before a coordinated upgrade. |
| PD or Store code | Use the shared owners without importing Server core; keep service-specific execution and resource lifecycle local. |

## Why share these implementations?

Before this change, Server core and struct carry separate implementations of several IDs, schema types, queries, elements and codecs. A fix to one copy can leave the other with different behavior. Consolidating them gives those fixes one owner and gives Server and Store a common implementation to test.

```text
Before: Core and Struct each maintain shared types and codecs.

After:
Server core --> Struct --> Common
Store core  --> Struct
PD service  -------------> Common
```

*Shared-foundation ownership, rather than the complete Maven dependency tree.* Server still needs PD/Store clients for HStore; Store still needs its network, PD client and storage libraries. Struct retains its shared index/analyzer and codec dependencies, including shaded Kryo where required.

| Capability | Owner after migration | What remains with the caller |
|------------|-----------------------|------------------------------|
| IDs, schema metadata and type codes | struct | Import and use the shared types |
| Queries, base elements, byte/property codecs | struct | Adapt graph-engine and backend behavior |
| Index construction, analyzers and OLAP selection | struct | Supply schema candidates and result sinks; preserve configured analysis and index results |
| Transactions, traversal, tasks and schema mutation | Server core | Operate on shared metadata and elements |
| PD-backed schema access | Store | Manage `SchemaGraph`/`SchemaDriver`, listeners and cache lifecycle |
| JWT signing/verification and auth constants | common | Supply configuration and translate failures into service responses |
| RPC client/server configuration interfaces | common | Keep existing implementations and configuration values |

`HugeGraphSupplier` lets shared code access schema, configuration and the clock without depending on the graph engine. Core and Store implement it.

Core elements wrap shared base elements: property mutation, cloning, removal, expiration and loading state use that same state. `BaseVertex` owns adjacency; the engine caches edge wrappers by identity over the same base edges. Core serializers retain their engine/backend adapters and delegate shared encoding. Core also retains transaction writes, backend capability checks and index-update orchestration around shared index construction and OLAP selection.

## Migrate Java code and extensions

### Start with the imports

For example, code that creates an ID changes its imports while keeping the factory call:

**Before**

```java
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;

Id id = IdGenerator.of("alice");
```

**After**

```java
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;

Id id = IdGenerator.of("alice");
```

| Previous entry | Shared entry or migration |
|----------------|---------------------------|
| `org.apache.hugegraph.backend.id.*` | `org.apache.hugegraph.id.*` |
| `org.apache.hugegraph.schema.*` metadata | `org.apache.hugegraph.struct.schema.*` |
| `org.apache.hugegraph.backend.query.*` | `org.apache.hugegraph.query.*` |
| `org.apache.hugegraph.backend.store.Shard` | `org.apache.hugegraph.backend.Shard` |
| `org.apache.hugegraph.backend.store.BackendEntry.BackendColumn` | `org.apache.hugegraph.backend.BackendColumn` |
| `org.apache.hugegraph.structure.HugeIndex` | `org.apache.hugegraph.structure.Index` |
| Shared bytes/encoding in backend serializers | `org.apache.hugegraph.serializer.*` |
| Core `HugeException` | `org.apache.hugegraph.exception.HugeException` |
| `org.apache.hugegraph.SchemaGraph` / `SchemaDriver` | `org.apache.hugegraph.store.schema.*` |

The table applies to these shared types, not every class in a package. Schema mutation builders and backend-specific serializers remain in core. There is no general compatibility package for removed core classes. Relocated types also change method descriptors that expose IDs, schema, queries and indexes, so update implementations and call sites and recompile every affected integration against matching artifacts. Changing source imports does not make old binaries compatible.

### Check API and SPI implementations

| Integration point | Required change or preserved contract |
|-------------------|---------------------------------------|
| `HugeGraph.sameAs` | Change `sameAs(HugeGraph)` to `sameAs(HugeGraphSupplier)`. External overrides must accept the supplier, as `StandardHugeGraph` and `HugeGraphAuthProxy` now do. There is no old-signature overload. |
| Custom `HugeElement` subclasses | Provide shared `BaseElement` state through `element()` and adapt `BaseProperty` through `wrapProperty`. Removed protected engine state fields and a second adjacency store must not be restored. |
| `GraphSerializer.writeIndex` / `readIndex` | Accept/return `org.apache.hugegraph.structure.Index`; update SPI implementations and callers. No long-lived `HugeIndex` wrapper is retained. Hash-ID generation belongs to shared `Index`. |
| `RpcServiceConfig4Client` / `RpcServiceConfig4Server` | Both now belong to common, with the same `org.apache.hugegraph.rpc` names and signatures. Existing RPC implementations and values need no source migration. Duplicate core/commons-rpc definitions are removed without adding a dependency edge. |
| `TokenGenerator` | Use common's `org.apache.hugegraph.auth.TokenGenerator`; supply the signing secret and translate JWT failures locally. Common's JWT dependencies are optional, so direct JWT consumers must declare their required JWT libraries. |
| Engine vertices and edges | `HugeVertex` and `HugeEdge` remain engine adapters; Store uses base elements. |

### Update downstream exceptions without changing their meaning

Distinct downstream exception contracts need distinct class names on the shared classpath. Use these replacements with the matching Toolchain artifacts:

| Previous downstream entry | Replacement and contract |
|---------------------------|--------------------------|
| Hubble `org.apache.hugegraph.exception.HugeException` | `org.apache.hugegraph.exception.HubbleException`: still a `RuntimeException` with its string constructor |
| Client `org.apache.hugegraph.exception.NotSupportException` | `org.apache.hugegraph.exception.ClientNotSupportException`: still a `ClientException`, with existing message/cause and message/arguments constructors |
| Hubble's copy of `org.apache.hugegraph.license.MachineInfo` | Keep the same import; use the implementation supplied by `hugegraph-common` |

Update imports, constructor calls and handlers for these exceptions. Struct's `HugeException` and `NotSupportException` have different contracts and are not substitutes for those catches. Computer's `HgkvDirImpl` must also use `ClientNotSupportException`.

Rebuild the affected Toolchain and Computer integrations and check their packaged classpaths before deployment. A source scan for duplicate names cannot rule out stale or transitive classes in built jars. Loader's REST boundary and independent client DTOs do not need wholesale conversion to struct.

### Check configuration-only class names

Replace `org.apache.hugegraph.backend.id.IdGenerator` with `org.apache.hugegraph.id.IdGenerator` in Gremlin `classImports` and scripts. The bundled default and Raft examples use the shared class. These strings are not checked by Java compilation, so test them in the packaged Gremlin service.

## Understand data and runtime compatibility

The migration preserves existing type codes, ID/property bytes, ordinary Server index keys, TTL formats, query semantics and configuration defaults. It introduces no general data-rewrite operation. The following cases need separate attention:

| Area | Behavior after migration | Action or limit |
|------|--------------------------|-----------------|
| String ID comparison | Uses Java UTF-16 string order even before an ID loaded from UTF-8 bytes is decoded | Persisted bytes stay the same; supplementary Unicode characters no longer compare differently after lazy decoding. |
| Property values | Preserves schema-driven persisted rows and tagged Store shuffle/aggregation values | Readers choose the mode from the known producer/request plan, not by guessing from bytes. |
| SYSTEM-label index expiry | Preserves Store's stable key and 13-byte value envelope for positive expiry, and core's existing SYSTEM-label bytes | The bounded legacy envelope is read at the serializer boundary; no key TTL suffix or prefix-deletion change. |
| Store-rebuilt long-text indexes | Uses the complete value and hash expected by Server queries | Assess old Store rebuilds; shortened historical rows may require rebuilding from graph data. |
| HStore OLAP physical keys | New writes use `[property ID][vertex ID]` and readers retain a matching legacy-row fallback | Upgrade writers together. Previously overwritten properties cannot be recovered. |
| Schema map decoding | Retains ordered primary/sort keys and resolves endpoint metadata independent of map field order | Matching or legacy endpoint forms work; conflicting or malformed metadata is rejected. |
| Metadata namespace | Store uses `pd.cluster` (default `hg`) across schema, configuration, watches and TTL-cleaner metadata | Match the Server/graph namespace and preserve backend graph names. |
| Historical serialized Java names | Query/Kryo readers retain required legacy names and aliases | Type relocation does not authorize a new wire format. |

### Store-rebuilt indexes

The old Store `IndexBuilder` shortened long text to 20 characters, while Server queries used the complete value and its hash. The shared builder adopts the Server contract. Frozen producer/query fixtures describe that existing mismatch in the [compatibility README](../hugegraph-struct/src/test/resources/compatibility/2f827d6e8/README.md#existing-indexbuilderquery-mismatch).

Old shortened rows remain decodable, but do not automatically match a query using the full value. Check whether your installation used Store-side index rebuilding and rebuild affected indexes from graph data where necessary. The migration neither rebuilds them automatically nor recovers missing historical entries. Ordinary Server-created index keys keep their existing contract.

### HStore OLAP writes

A vertex-only physical key allowed one OLAP property to overwrite another on the same vertex. The new `[property ID][vertex ID]` key lets those properties coexist. This is a behavior repair beyond package relocation; existing row values remain readable without rewriting them.

Readers try the requested property's compound key first, then the legacy vertex-only key if its value contains the matching property ID. Deleting one property removes its compound row and a matching legacy row while preserving other properties. Values already overwritten by the old writer are lost.

Clearing an OLAP property deletes its matching compound and legacy rows while
retaining its schema. Removing the property deletes those rows before the existing
schema/index removal job completes. Both operations retain the shared OLAP table,
other properties and other graphs. Cleanup scans the shared table and commits
bounded batches; if a batch fails, the task fails and the pending session is rolled
back. Deletions already committed to Store nodes can remain, so retrying cleanup
is idempotent; the operation does not promise graph-wide atomicity.

Quiesce writes to the affected OLAP property before clearing or removing it, and
keep those writes stopped until cleanup completes. The scan and bounded delete
batches do not coordinate concurrent writers: a new row can be missed, and a
rewrite of a scanned row can be deleted. An inconsistent compound key/value
blocks cleanup until the affected row is repaired; the error includes its key
in hexadecimal, limited to the first 64 bytes.

Mixed-version writes can produce stale reads: an old writer may update a legacy row while a new reader prefers an earlier compound row. Upgrade all Server and Store writers before resuming writes.

### Match metadata namespaces

In Store `application.yml`, the existing namespace setting is:

```yaml
pd:
  cluster: hg
```

The environment equivalent is `PD_CLUSTER`. Match Server's `cluster` when `usePD=true`, or the graph's `pd.cluster` when `usePD=false`. One Store process cannot reuse its schema driver across conflicting namespaces.

Keep backend graph-name components such as `DEFAULT/hugegraph/g` (`graphspace/store/table`). The REST identity `DEFAULT-hugegraph` is not a metadata key.

## Plan a coordinated upgrade

```text
Prepare --> Adapt Java code --> Upgrade matching services --> Verify

HStore OLAP keys:
Before: vertex ID
After:  [property ID][vertex ID]
```

*Use matching Server, PD and Store artifacts; mixed-version rolling upgrades are outside this migration's supported upgrade contract.*

1. **Prepare integrations.** Update Java APIs/SPIs and Gremlin class names, recompile affected callers and check downstream delivery. Assess Store-rebuilt long-text indexes and OLAP usage.
2. **Back up data and configuration.** Use the existing deployment procedure. Record metadata namespaces and backend graph names so they remain consistent.
3. **Stop writes and upgrade together.** Install the matching Server, PD and Store release artifacts. Upgrade all OLAP writers before allowing writes again.
4. **Verify the built deployment.** Start the packaged services, check Gremlin imports, authentication, schema access and relevant RocksDB/HStore paths. Validate historical data and the built classpath; compilation alone is not sufficient.
5. **Rebuild affected indexes and resume writes.** Where old Store rebuilds produced shortened text keys, rebuild from graph data as needed and verify full-value queries. Check OLAP properties and metadata routing before resuming.

Validate the packaged deployment and affected integrations before resuming writes, using the compatibility checks below.

## Compatibility reference for implementers

### Codecs and schema

Persisted property rows and transient tagged results share the scalar codec. Both must retain their historical samples. Ordinary schema index writers keep the Server key/name-TTL layout. `Index.hasTtl()` follows the engine's existing system-index contract; Store's expiry envelope stays at the serializer boundary. Unsupported index-only element reconstruction remains unsupported.

Shared `Shard` preserves its GraphSON tag and fields, and query decoding retains its legacy fully qualified name. Toolchain's `org.apache.hugegraph.structure.graph.Shard` stays a separate REST DTO. `BackendColumn` retains mutable `name`/`value` byte arrays. Its hash now uses array contents consistently with equality instead of core's array-identity hash. Historical Kryo names require decoder aliases and fixture checks; the fields retain their bytes. Store's `RocksDBSession.BackendColumn` stays a backend adapter.

Schema maps preserve wire field names and ID identity. Two decoder fixes accompany relocation: primary-key and edge sort-key lists retain order and duplicates rather than passing through a set, and redundant edge endpoints are resolved consistently regardless of map field order. Matching `links` and source/target fields are accepted; legacy source/target-only maps remain readable; conflicting or malformed endpoints are rejected. Ordered-schema and endpoint-map tests cover generated vertex/edge IDs and query values, but do not establish all distributed or packaged release gates.

Query JSON still uses Java names for some relation values. Decoder mappings must read historical names, and historical values still require Kryo decoding where applicable. Replacing these Java names with value tags would require a separate wire-format compatibility policy.

### System-vertex classification

`BaseVertex.TypeContext` makes the existing engine/storage distinction explicit:

| Context | `~variables` | `~server` / `~role_data` | `~task` / `~taskresult` |
|---------|--------------|------------------------|------------------------|
| `STORAGE` (standalone storage elements) | Task data | Ordinary vertex data | Task data |
| `ENGINE` (engine wrappers) | Ordinary vertex data | Server data | Task data |

Engine wrappers must select `ENGINE`; standalone storage elements retain `STORAGE`. Selecting the correct context preserves existing table routing and readers for system vertices.

### Review and release checks

Review behavior alongside the package moves. The key entrypoints and invariants are:

| Area | Main entrypoints | What to verify |
|------|------------------|----------------|
| Java API/SPI | `HugeGraphSupplier`, `HugeGraph.sameAs`, `GraphSerializer`, custom `HugeElement` | Caller recompilation and the documented migration |
| Element state | `BaseElement/BaseVertex/BaseEdge`, `HugeElement/HugeVertex/HugeEdge`, offheap properties | One state owner, callback order, clone/adjacency identity and virtual value reads |
| Historical codecs | `BytesBuffer`, `BinaryElementSerializer`, `BinarySerializer`, `LegacyClassNames`, `KryoUtil` | Producer-specific bytes and old enum ordinals, with canonical writers preserved |
| Queries, schema and indexes | `ConditionQuery`, `EdgeLabel.fromMap`, `IndexBuilder`, `Index` | Query behavior, ordered keys, endpoints, full-value hashes and TTL contracts |
| HStore and metadata | `HstoreTables`, `OlapStage`, `SchemaDriver`, `FilterIterator` | OLAP repair, exact owner reads, batch bounds, namespaces and watch closure |
| Delivery | Shared-foundation/inventory guards, packaged jars, Gremlin imports, downstream callers | Resolved/shipped dependencies, startup and paired documentation |

Release evidence must include shared tests and frozen pre-migration byte/JSON fixtures for IDs, schema, properties, indexes, pagination, TTL/OLAP and supported Kryo data. It must also cover RocksDB/HStore execution, Store filtering and schema access, authentication, element state propagation and affected downstream tests.

Check resolved dependencies and built distributions: PD/Store must not depend on Server core, struct must not depend on PD client/core, and packaged jars must not contain duplicate HugeGraph classes. Start the packaged services without a classpath-ordering workaround. Independent review and CI guards must prevent new duplicate owners or forbidden dependencies.

Build/test commands and service prerequisites are in [BUILDING](BUILDING.md), module guidance and CI. Ensure tests actually run; `-DskipTests` is not compatibility evidence.

Deploy matching Server, PD, Store and downstream artifacts. Validate both the Java integrations and the packaged services before adopting the migration.
