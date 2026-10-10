# HugeGraph-Struct

`hugegraph-struct` owns the shared foundations used by Server and Store: IDs, schema metadata, base graph elements, query conditions, type codes and binary encoding. Core builds the graph engine on these implementations instead of maintaining a second copy.

## Responsibilities

| Owner | Responsibility |
|-------|----------------|
| `hugegraph-struct` | Shared models, queries, codecs, index building and analyzers |
| `hugegraph-common` | General utilities, shared JWT verification/signing and RPC configuration interfaces |
| `hugegraph-core` | Graph/TinkerPop adapters, transactions, traversal, backend execution and jobs |
| `hg-store-core` | Store execution and schema metadata access through PD |
| PD service | Placement/metadata service and its own authentication/transport adaptation |

Struct is not a DTO-only module or dependency-free library. It retains the libraries required by shared codecs, collections and analyzers, including TinkerPop interfaces and shaded Kryo compatibility. It must not depend on core or own PD connections, listeners and caches.

`HugeGraphSupplier` lets shared operations obtain schema without a concrete `HugeGraph`. It also supplies naming, configuration and time. Store's `SchemaGraph` and `SchemaDriver` live in Store. The caller owns their PD lifecycle.

Core's `HugeVertex`/`HugeEdge` wrap shared `BaseVertex`/`BaseEdge` state and add engine behavior. `BaseVertex` owns adjacency; core only caches wrapper identities. Shared `Index` replaces `HugeIndex`, including index IDs and values. Index/OLAP selection uses caller-supplied candidates and sinks; core still executes transactions and backend updates. Engine serializers retain backend adaptation and delegate shared encoding. These adapters have different responsibilities from the shared implementation; a second algorithm or state collection is not an adapter.

## Packages and migration

| Package under `org.apache.hugegraph` | Content |
|------------------------------------|---------|
| `struct.schema` | Schema metadata |
| `structure` | Base elements, indexes and shared index builder |
| `id`, `query`, `serializer` | IDs, query conditions and encoding |
| `type`, `backend` | Type codes and shared backend values |
| `analyzer`, `util`, `exception` | Shared analysis, codec helpers and exceptions |

The 1.8.0 Java API migration removes the old core copies. For example, schema metadata moves from `org.apache.hugegraph.schema` to `org.apache.hugegraph.struct.schema`; engine schema mutation builders remain in core. Applications using internal Java APIs must update imports and compile against a consistent set of artifacts.

See the [shared-foundation migration guide](../docs/shared-foundation-migration.md) for ownership, upgrade requirements and compatibility checks. Existing client REST DTOs need not be replaced merely because their names resemble these types.

## Development and validation

Run from the repository root:

```bash
mvn install -pl hugegraph-struct -am -DskipTests
mvn test -pl hugegraph-struct -am
```

The first command builds without tests. Tests exist under `src/test/java`; a successful build alone does not prove compatibility or distributed startup. See [BUILDING](../docs/BUILDING.md) and module CI for service prerequisites.

Change shared behavior once, in its canonical owner, and migrate consumers. Changes to type codes, ID encoding, binary layout, query JSON or classification must be checked against historical fixtures and Server/Store readers. Do not restore deleted copies to resolve an import or dependency problem.

## License

Apache License 2.0; see [LICENSE](../LICENSE).
