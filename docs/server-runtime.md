# Server runtime and query contracts

This guide covers Server configuration, wire formats, query semantics and transaction
ownership. Use it when upgrading applications or changing the
Server runtime. Deployment instructions remain in the [build guide](BUILDING.md)
and [Server module guidance](../hugegraph-server/AGENTS.md).

## Runtime and client configuration

The runtime uses TinkerPop 3.8.1 and Groovy 4.0.25. Build and run every module with Java
17. Applications migrating from TinkerPop 3.5.1 must review the full version interval,
including the configuration changes below.

### Required JVM module options

Server, store initialization, store dumping and Gremlin Console always load the
distribution's `bin/jvm-module.options`. Keep this file with the shipped scripts;
each launcher rejects a missing or unreadable file. Required module permissions
are independent of caller options, so custom `JAVA_OPTIONS` or the Server launcher's
`-j` option do not replace them. Gremlin Console retains its existing
`JAVA_OPTIONS` defaults; store dumping retains its existing command-line interface.
The shared file opens `java.nio` for HBase's reflective `java.nio.Bits.unaligned()`
check during `HBasePlatformDependent` initialization, including on x86 Java 17.
It also opens `java.lang` for SOFA RPC's `ProviderInvoker` initialization, which
reflects on `Throwable.cause`.

PD and Store use their own `bin/jvm-module.options` for the module permission needed
by Hessian serialization on Java 17. Their tests and cluster launchers use the same
component files. CI checks positive-test and startup logs for Java module-access
errors and unsupported JVM options, including errors printed without failing the
operation.

### Gremlin and HStore transport reconnection

The Gremlin security manager permits reads of the gRPC shaded Netty boolean property
`io.grpc.netty.shaded.io.grpc.netty.useCustomAllocator` and the DNS cache setting
`networkaddress.cache.ttl`. gRPC reads these settings when it builds a DNS resolver or
rebuilds a transport, including after a PD or Store restart during a Gremlin request.
Gremlin cannot set or clear these system properties. It also cannot change the separate
JDK security property `networkaddress.cache.ttl` through `java.security.Security.setProperty`.
Trusted code outside Gremlin can still configure these settings. Sensitive system
properties such as `user.home` remain unreadable from Gremlin.

HStore constructs each target's lazy channel and stub pools on the calling thread so
initialization retains the caller's Gremlin security context. The client publishes
the pool after all channels are constructed. If construction throws a runtime exception
or an error, it closes the partial pool and reports the failure; a subsequent request can retry.
Cleanup failures are retained as suppressed exceptions, and an original error is rethrown
unchanged. RPC transport work remains asynchronous. This change does not grant Gremlin permission to create
threads or thread groups.

The shared gRPC framework and provider discovery initialize during normal PD/framework
bootstrap before Server accepts Gremlin requests. This does not initialize the pools
for every future Store target. Unit checks that obtain a stub verify lazy pool
construction; a successful RPC and its readback are separate runtime checks.

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

### Launcher memory budget

The default launcher heap uses half the smaller of host available memory and the
remaining budget under visible cgroup v1/v2 hard limits, with a 512 MiB minimum and a
32 GiB maximum. It refuses startup when the effective budget is below 512 MiB.
If a known hard limit has missing, unreadable or invalid usage, the budget is zero;
the launcher refuses startup instead of using host memory. For cgroup v1, numeric
unlimited values are recognized only when `uname -m` reports `x86_64`, `aarch64` or
`riscv64` and `getconf PAGESIZE` returns a positive integer. The threshold is the
signed 64-bit maximum rounded down to that page size. Recognized unlimited levels
are skipped so readable finite ancestors still apply; without a finite limit,
host sizing remains available. Unknown or masked machine names, or an unavailable
or invalid page size, keep numeric limits on the finite-limit path.

Inactive file cache can contribute to this budget on a warm restart. For a finite
cgroup v2 limit, the launcher checks the immediate cgroup children of that reclaim
target. If any child's `memory.min` is nonzero, missing, invalid or unreadable, it
excludes cache credit at that limit: child cache may be protected from ancestor reclaim.
If the target directory cannot be enumerated, or a child cannot be searched, cache
credit is also excluded because child protection cannot be determined. Ordinary
searchable directories without cgroup controller files are skipped.
Unprotected children retain the existing warm-cache budget, including inherited and
nested hard limits. A cgroup's own `memory.min` does not block cache credit for its
own limit; another ancestor can still constrain it through child protection.
The cgroup v1 cache calculation is unchanged.

This is a startup estimate, not a guarantee against runtime OOM. Native allocations
and concurrent processes also consume memory. Explicit heap options retain their
existing precedence; see [container heap sizing](../docker/README.md#server-heap-sizing).

### Arthas diagnostics

Server and Store bundle Arthas 4.1.5, managed by the root POM. PD does not embed
an Arthas agent. Diagnostic ports bind to `127.0.0.1` by default. Administrators
can explicitly configure another valid address, including wildcard `0.0.0.0` or
`::`; `localhost` is normalized to `127.0.0.1`. Wildcard listeners can accept both
IPv4 and IPv6 depending on the OS/JVM configuration; restrict both in firewall
rules. Server's `PUT /arthas`
requires authentication to be enabled and the caller to be an authenticated administrator.
Without authentication, the Server endpoint returns HTTP 403, including requests
from localhost or through a reverse proxy. Store has no equivalent administrator
authentication, so `GET /v1/arthasstart` always returns HTTP 403.

For local diagnosis of either running service, use its distribution's bundled
helper as the same OS user that owns the JVM:

```bash
bin/arthas.sh
```

The distribution prepares the agent in `lib/arthas` at build time. The helper
requires JDK 17 and uses `bin/pid`, without downloads, runtime extraction, a user
cache, service restarts or configuration edits. A new local agent listens on
`127.0.0.1:3658`, with HTTP, MCP and reverse tunnels disabled. Use `--pid` or
`--telnet-port` for another running instance, and `--command version` for a one-time
command. Interactive mode requires a terminal; for pipes, scripts or `docker exec`
without `-it`, use `--command` instead. Run `bin/arthas.sh --help` for usage.
When the helper exits, including after `--command`, `quit`, or INT/TERM/HUP, it
stops the agent and all its diagnostic sessions; the application JVM keeps running.
SIGKILL or a host failure cannot run this cleanup: stop any surviving agent manually.
For remote diagnosis, log in to the service host over SSH and run the same command,
or explicitly configure a reachable listener with appropriate authentication and
network access controls as described below.
Official Server (including HStore) and Store images include JDK 17, so the same
helper works inside the container, for example
`docker exec -it <store-container> /hugegraph-store/bin/arthas.sh`.
A JRE-only installation needs JDK 17 for this helper.
If the Server API has already activated Arthas, connect to its existing telnet
port instead: `bin/arthas.sh --telnet-port 8562` with the default configuration.
Use `--target-ip` to select another bind address, for example
`bin/arthas.sh --target-ip 0.0.0.0` or
`bin/arthas.sh --target-ip ::1 --telnet-port 8562 --command version`.
The default is `127.0.0.1`; `localhost` maps to `127.0.0.1`. Wildcard addresses
`0.0.0.0` and `::` may accept both IPv4 and IPv6; the helper connects and cleans
up through `127.0.0.1` for either wildcard. Remote clients connect
to the service host's actual reachable address, not the wildcard address.
An already running agent retains its listener and authentication settings;
helper options do not reconfigure it. To switch to the helper's
HTTP-disabled defaults, run `bin/arthas.sh --telnet-port 8562 --command stop`, then
`bin/arthas.sh`; this stops only Arthas, and leaves the service JVM running.
Arthas can execute code inside the JVM. OS attach permissions control initial
activation; HugeGraph administrator authentication protects only the activation
API, not subsequent diagnostic connections. We recommend enabling Arthas native
authentication, especially when binding beyond loopback. However, native defaults
allow connections from `127.0.0.1` to skip authentication even with credentials;
other OS users or containers in the same Pod can still connect this way. To disable
this bypass, set `-Darthas.localConnectionNonAuth=false` on the service JVM and
restart it. Restrict host/Pod and port-forward access to trusted operators, and
limit firewall rules, port mappings and SSH forwarding accordingly. Authentication
does not encrypt telnet
or ordinary HTTP traffic; use a protected network or encrypted transport for
remote access. Avoid concurrent helper sessions because exiting one stops the
shared agent. A disabled-command list is optional and is not access control.

Pass native Arthas credentials when starting or reusing an agent:

```bash
bin/arthas.sh --target-ip 0.0.0.0 \
    --username diagnostics --password '<arthas-password>'
```

Use single-line credentials; printable ASCII avoids the bundled Arthas 4.1.5
telnet client's Unicode limitation. Password-only use defaults to username `arthas`.
Native JVM credentials also apply to helper-started agents: if the target JVM has
`-Darthas.username` or `-Darthas.password`, supply the effective credentials with
`--username` / `--password` for authenticated commands and cleanup, especially
when `-Darthas.localConnectionNonAuth=false` is set. The helper does not clear
native credentials when the corresponding option is omitted.
These credentials are separate from HugeGraph accounts. Batch commands and exit
cleanup use the supplied credentials automatically. For interactive diagnosis,
if prompted, enter `auth --username diagnostics <arthas-password>` in the Arthas
console. Existing agents require their existing credentials; passing new values
does not change them. Password arguments may appear in shell history and process
listings, so handle them on a trusted host. With `--username` alone, native Arthas
generates a password only when none is already configured in the target JVM's
native settings; otherwise it uses that inherited password. Supply `--password` for
authenticated batch commands and automatic cleanup. If cleanup fails, the helper
returns nonzero and the agent may remain running; use the configured or generated credentials with
native `auth`, then `stop`, to close it without stopping the service JVM.

[Arthas native authentication](https://arthas.aliyun.com/en/doc/auth.html) remains
optional and follows upstream defaults: `arthas.localConnectionNonAuth=true`
allows connections from `127.0.0.1` to skip authentication. The helper uses a private
configuration, so editing `lib/arthas/arthas.properties` does not configure it.
If required, the target JVM property `-Darthas.localConnectionNonAuth=false` is
honored after restarting the service JVM. Arthas 4.1.5 generates a password when
binding `0.0.0.0` or `::` without an effective native password and prints it in the target
JVM output and Arthas log; explicitly supplying credentials is more convenient
for remote diagnosis and authenticated cleanup.

Authenticated Server administrators can continue using `PUT /arthas` without an
additional enable switch or restart. Its HTTP/telnet interfaces use the configured
bind address; HugeGraph disables the optional MCP endpoint. The activation response
still contains the same four Arthas configuration keys. For API activation with
native credentials, add `-Darthas.username=diagnostics` and
`-Darthas.password=<arthas-password>` to the Server JVM startup options and restart
Server before activation. These are native JVM properties, not new REST
configuration keys, and also apply to agents started by the local helper as described
above. They may expose secrets in process listings. Without native
credential settings, a specific non-loopback bind address does not require login; wildcard
binds use the generated password described above. API activation alone
leaves the agent running until an operator sends `stop`; connecting through the
helper applies the helper's exit cleanup to that existing agent too.

Before reusing an API-started agent, leave `session` and `stop` out of
`arthas.disabled_commands` (or its legacy alias). `session` verifies the target
PID; `stop` closes Arthas without stopping the service JVM. Existing configurations
remain accepted, but failed PID verification or listener cleanup makes the helper
print an error and exit nonzero; the agent may remain running. To recover from an
agent with `stop` disabled, remove that restriction and restart the service before
activating Arthas again. Newly attached local agents keep both commands available.

No commands are disabled by default. Administrators can optionally set
`arthas.disabled_commands=jad,mc` to restrict selected commands for API activation;
new local agents do not inherit this list.

The existing `arthas.ip`, `arthas.httpPort`, `arthas.telnetPort` and
`arthas.disabledCommands` options configure the Server API-activated agent.
The distribution uses `arthas.telnet_port`, `arthas.http_port` and
`arthas.disabled_commands`. These snake_case keys take precedence over the legacy
camelCase aliases when both are present, including explicit default values.
Conflicting values produce a warning naming both keys and the effective key when
the agent is activated. Remove the unused alias to avoid ambiguity. Store's legacy
configuration fields are retained; the local helper does not read them.

For a synchronous one-time command, POST `{"action":"exec","command":"version"}`
to the agent's `/api` endpoint without a `sessionId`. Check `state=SUCCEEDED`, the
`body.results` version and command status, and `body.jobStatus=TERMINATED`; HTTP 200
alone also occurs for failed commands. Arthas 4.1.5 includes the upstream
[terminal-process interrupt fix](https://github.com/alibaba/arthas/pull/3079) and
[foreground-job cleanup fix](https://github.com/alibaba/arthas/pull/3094), so cleanup
can remove a session whose foreground command has already terminated.
It also includes the [MCP-disabled HTTP handler fix](https://github.com/alibaba/arthas/pull/3127).

Clients that explicitly call `init_session` must call `close_session` when finished.
After closing, `session_info` for that ID reports `state=FAILED` with `session not found`.
[`ArthasApiTest`](../hugegraph-server/hugegraph-test/src/main/java/org/apache/hugegraph/api/ArthasApiTest.java)
covers one-time execution, explicit session execution and removal.

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

### First query after startup

Server initializes the condition-query serializer on its trusted startup thread before
REST and Gremlin accept requests. A first label-filtered Gremlin query must succeed
without a preceding REST graph query, and subsequent REST queries must remain usable.
Gremlin scripts still cannot read restricted system properties such as `java.version`
or disable the security manager.

### Property-query index coverage

For type-compatible values, a global property-index query such as
`g.V().has("name", "alice")` or `g.E().has("weight", 0.5)` requires compatible
indexes on every visible candidate label. A candidate declares all queried non-OLAP
properties; even a label with no records requires coverage. Base edge labels have no
direct records; their indexes can cover sub-labels. An explicit sub-label query
also uses a compatible query-visible parent index when no compatible direct index
exists, while still filtering out sibling sub-labels. Shared OLAP indexes follow the graph read
mode.

| Candidate state | Query behavior |
| --- | --- |
| Every visible candidate has compatible indexes | The query combines the indexed results. Composite and joint index plans may serve different labels in the same query. |
| A visible candidate lacks a compatible index and the values pass the property-type check | The query fails with `NoIndexException`, including when other candidates are hidden or deleting. |
| Every candidate is excluded by visibility | The query returns an empty result, including after those labels' indexes have been removed. |

When query-visible candidates remain, index matching precedes the property-type
check. If at least one index matches but a value fails that check, the query returns
an empty result before complete coverage is checked. If no index matches, it still
raises `NoIndexException`, even for a type-mismatched value.

Normal queries exclude hidden labels and labels in `DELETING` or `UNDELETED`
status. Internal requests that enable `showHidden` or `showDeleting` include the
corresponding labels and still require their indexes. Visibility follows the outer
request when a query is split into subqueries. Each branch checks index state and
complete coverage before opening its backend iterators. If branch construction
fails, the query closes cursors from both the unfinished branch and earlier
branches, preserving the original error. Joint index planning closes unselected
cursors and transfers the selected cursor to the result. This does not provide a
snapshot against concurrent schema changes.

To fix incomplete coverage, specify an indexed label, for example
`g.V().hasLabel("person").has("name", "alice")`, or add indexes compatible with the
query predicates to every candidate label. Explicit-ID lookups and adjacency
filtering do not introduce a global property-index coverage requirement.

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
For the local Standard scheduler, a deleted task disappears from public lookup after its persisted record is
removed, while its actual execution still counts toward drain until native cleanup finishes; late completion saves from
that owner do not recreate the removed record.

Synchronous task cancellation preserves a transaction already open on its caller,
including uncommitted graph writes. Cancellation still runs the normal completion and
status callbacks, but their default cleanup does not close that existing transaction.
The scheduler closes transactions opened by a cancellation callback when it had no
transaction before starting, and a running task retains its worker owner until that
worker's own cleanup finishes. The caller remains responsible for committing or rolling
back its original transaction.

Graph close attempts auth, scheduler and caller-transaction cleanup separately. If a
scheduler remains registered or the graph reports active transactions, the graph remains
open for a later close attempt. Pending local tasks retain their task database transaction
so they can persist completion before the next close attempt. An incomplete scheduler
drain does not queue transaction cleanup behind that scheduler's blocked worker. Stopping
distributed task dispatch is distinct from completing drain: a timeout does not make
the next close return success.

During Server shutdown, the task manager waits for task workers to finish, then retries
closing graphs retained in its existing scheduler registry before stopping the task
database worker. This gives pending tasks time to persist completion and releases the
task database owner's transactions on that owner's thread. Each retained graph gets one
close attempt per shutdown call. A close failure remains visible and keeps the database
executor available for a later explicit retry; it does not start background recovery.
A terminated cron executor already satisfies the scheduler's dispatch barrier. Every
executor wait, including an explicit retry after a timeout, checks actual termination;
shutdown requested alone is insufficient. Store-close notifications finish before the
shared event executor is stopped.

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
  exercises graph close failure continuation, distributed drain retry and Server stop
  with a task that outlives the early graph-close attempts.

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

## Cache RPC graph lifecycle

When cache RPC is enabled and a backend declares shared storage, Server registers both
graph and schema cache notifiers for graphs loaded during startup and graphs created or
loaded later. A new graph's RPC registration must succeed before its local publication.
Graph services use the graph space and graph name together as their service identity.
PD deletion and startup coordinate RPC removal for the same graph instance. Startup does not
register an owner whose RPC removal is pending, even after its placement-event token is consumed,
and checks logical closure under the actual owner's RPC lifecycle monitor before registration.
Any retained provider configuration must be unregistered before startup can export services;
a cleanup failure fails startup instead of re-exporting that owner. Graph clearing, task drains
and full graph close remain outside this RPC publication gate.

Internal graph loading registers RPC services in Server's administrative scope and
restores the caller's identity afterward. Direct registration and cleanup remain
administrative operations.

Successful graph close releases that graph's notifier listeners, providers and consumer
proxies. A failed task drain during ordinary close or non-clearing local removal retains
the owner and its RPC resources. In PD-enabled deployments, RPC cleanup failures during
graph DELETE retain the local owner and bindings for retry; partial cleanup does not imply
a healthy service. This DELETE path first closes task admission and drains local executions,
then cleans up RPC before metadata deletion and backend clearing. A failed drain or RPC
cleanup can be retried with the same DELETE. Logical graph closure fences access before
all resource cleanup completes. Any close failure retains the local owner and bindings;
retrying the same DELETE retries cleanup without repeating backend clearing. A completed clear
is remembered for that exact graph instance even if an external idle transaction delays close;
retry after that transaction exits. A replacement graph with the same name is cleared independently.
A graph closed by a non-clearing removal has no successful-clear certificate. A later clearing DELETE
refuses that closed owner and keeps its binding, rather than reporting success while old backend data remains.
Raft provider, node and an owned listener are closed independently. The first fatal Java Error takes
priority over ordinary close failures; otherwise the first failure is retained. Other failures are
reported as suppressed, with a repeated reference to the primary failure ignored.
A graph using Server's shared Raft listener closes
only its own node. Server releases that listener and its address registration on shutdown;
deleting one graph keeps the listener available to other local graphs. An interrupted node
join retains the graph owner and interrupt status so a later DELETE can finish cleanup.
The context's backend, snapshot and optional read-index executors stop after node join and
drain queued work before close completes. Their wait uses `task.wait_timeout`; timeout or
interruption retains the graph for retry without cancelling queued operations.
RPC cleanup can start before graph close finishes. Clearing PD DELETE unregisters RPC before
metadata removal and backend clear. Factory, binding and quota removal require a successful graph close;
a failed close retains their owner even if its RPC services have already been unregistered.
One failing graph does not prevent other graphs in a PD removal batch from being processed.
Fatal Java Errors retain their original identity and
propagate out of the removal batch. Failures after backend clearing do not undo deletion;
retries do not restore deleted data or placement metadata.

For local deletion without PD, backend clearing precedes RPC cleanup during graph close.
A close failure retains the same local graph, factory identity and configuration for DELETE retry,
even when access has already been fenced by logical closure. Only a successful clear by that
instance's local drop permits retry to skip clearing and finish its retained resource cleanup.
An ordinary close does not provide that certificate. A failure before clearing keeps the open
owner's existing RPC services. A later cleanup failure does not preserve or restore deleted data.

Failed creation or local loading closes the candidate in Server's administrative scope.
If rollback close fails, Server retains the local owner and factory identity for cleanup retry.
Retained failed candidates are excluded from RPC startup even while they are still open.
An old graph's cleanup cannot remove or re-export a replacement graph's services.
A PD graph-add notification blocked by an existing owner is coalesced until that owner is
released, including when independent watches deliver the new ADD before the old REMOVE.
Duplicate ADD notifications alone keep the healthy owner. Server consumes the PD acknowledgement
of its own local graph creation without retaining a replacement ADD unless that exact owner already
has failed peer cleanup; the existing cleanup retry takes priority. A healthy creation acknowledgement
cannot republish the graph during a later reload. After removal, Server rereads the latest configuration;
a missing configuration creates nothing. Reload consumes pending ADDs only after each old owner is
successfully removed, then loads current metadata together instead of replaying ADDs during removal.
After a peer removal fails, a later same-name PD add retries non-clearing cleanup of that exact
owner before loading its replacement. Another cleanup failure keeps the owner for the next add
or normal reload; no background retry is scheduled. A replacement load failure is logged and
remains pending for a later add or normal reload. The completed old-owner DELETE still succeeds.
Events queued for a closed notifier are ignored; an RPC request already sent cannot be recalled.

Cache RPC uses the configured peer addresses; local provider export does not establish
that every remote consumer connection is ready. Notifications are asynchronous, and
connection or delivery failures are logged. This lifecycle handling does not provide
acknowledged delivery or a distributed cache consistency protocol. Validate actual DML
and cache behavior against the deployment's shared backend and topology.

## Deleting a graph with local tasks

Stop application writes before deletion and keep them stopped during retries. The local
scheduler closes task admission and waits for actual executions, cancellation callbacks
and their transaction cleanup before data or metadata is removed. If draining times out,
the graph stays registered with its data and configuration, but task admission remains
closed: new asynchronous tasks are rejected even after the existing owners exit. Keep
application writes stopped and retry deletion after those owners exit. If no destructive
cleanup has begun, restart the Server to resume task admission instead of completing
the deletion. Closing the scheduler alone leaves unstarted persistent work in storage.

This is a local shutdown guarantee. For a graph shared by several Servers, stop writes
and tasks on every Server before deletion; notification delivery is not a cluster-wide
barrier. Do not recreate the same graph name until deletion finishes. Use a restart for
configuration changes rather than hot reload, whose recovery is handled separately.
Deletion is not transactional: a failure after destructive cleanup needs operator
inspection, and retry does not restore removed data. An owner-cleanup failure must not
be treated as a successful drain; inspect it instead of force-removing the scheduler.

## Local loading ownership

Server keeps ordinary import ownership in memory on each graph instance. The first
owner changes NONE to LOADING; releasing one owner leaves LOADING while others are
active, and the last release restores NONE. Manual LOADING, RESTORING and MERGING
cannot be adopted. Original enum setters reject every request while owners are active,
including bare LOADING; their original behavior remains available without active owners.

`GET /graphspaces/{space}/graphs/{graph}/mode/owners` returns
`{"protocol":1,"instance":"<UUID>","epoch":0,"revision":0}`. This response is illustrative;
recovery must use the actual values returned by the latest GET. `PUT` on that path accepts
`{"action":"acquire","instance":"<UUID>","epoch":0,"token":"<import UUID>"}`;
`release` uses the same tuple. Receipts echo protocol/instance/epoch/token and return
`state` (`active` or `released`) and the effective `mode`. Both operations use existing
graph-owner/STATUS WRITE permissions; the authenticated principal owns the token.
Each live graph admits at most 1024 concurrent owners. At that limit, a new owner
receives HTTP 409 without changing state; existing-owner retries, release and
administrator recovery remain available. Releasing an owner frees a slot.
The request principal is also bound for graphs opened without the authentication proxy.
Each new acquire, actual release and successful recovery writes an INFO audit entry
with the graph name, graph instance, previous/resulting mode, epoch and owner counts. Idempotent
retries do not add entries. These audit entries omit tokens and authenticated principal values.

Keep one tuple and identity for the whole import attempt, including uncertain acquire
outcomes. Release after that import's workers stop. Acquire/release retries are
idempotent. An unknown release cancels a late acquire; a released handle cannot be
reused. Terminal records are bounded to 1024 per epoch. Epoch advancement retains
active old handles and permanently retires all other old handles. An initial
acquire prepared before epoch advancement can therefore be rejected even if it
never became active. The client must not automatically refresh that retired handle:
a new token could undo a prior cancellation or administrator recovery. After a
terminal acquire rejection, stop and drain that import attempt before creating a
fresh Loader context to retry.

This protection coordinates Loader processes connected directly to the same stable
Server and graph instance. It does not fence ordinary data requests across load
balancers, node failover, graph replacement or Server restart. There is no automatic
expiry: after kill -9 or permanent disconnection, stop the import and its in-flight
writes before retrying release with the same handle and identity. If that handle is
lost, an administrator can recover without that token:

1. Stop every related import and drain its in-flight writes. Keep ordinary writers
   stopped too; the owner protocol does not fence graph data requests.
2. Read the current instance, epoch and revision from the owners endpoint on the same Server.
3. Send `PUT` to that endpoint with
   `{"action":"recover","instance":"<UUID>","epoch":0,"revision":0,"confirm":"I'm sure all imports have stopped"}`.

Recovery requires the authenticated global administrator role even when graph-space
permission checks are disabled; a proxied graph also enforces administrator permission.
With Server authentication disabled, the existing trusted anonymous administrator semantics apply.
Recovery also requires the exact current instance/epoch/revision and explicit confirmation. It clears owners, advances the epoch and returns NONE;
late acquire/release requests from the retired epoch cannot register an owner again.
The receipt contains the new epoch and `state: recovered`, without tokens. A stale
recovery request returns a conflict and cannot clear an import started in a newer
epoch. Owner registration/release and every successful enum mode write advance the
revision, including a write of the current mode. Either operation after inspection
therefore rejects the stale recovery request even within the same epoch. After an uncertain result, inspect current state rather than blindly
retrying with a newly fetched epoch/revision. This operation does not stop or drain imports itself.

The same recovery procedure can reset manual RESTORING or MERGING after that work
has stopped and drained; restore completion keeps its historical RESTORING mode.
Before ordinary import on an owner-capable Server, explicitly recover or set NONE
only after inspecting the completed work and confirming there are no active imports.
Expected loading-owner and manual-mode conflicts return HTTP 409; invalid requests
return 400 and unauthorized recovery is rejected by the authentication layer.

Loaders retain historical ordinary-import and restore behavior with Servers that do
not provide this API. Concurrent owner protection applies only when this API is
supported; RESTORING keeps its existing enum mode path.

## Graph-space storage reporting and removal

PD refreshes graph-space usage from Store heartbeat statistics, counting the leader
copy of each partition. Usage is approximate and advances on the quota check cycle;
it is not a synchronous count of every write. Changing a storage limit preserves
the latest usage, and a space with no remaining graphs reports zero on the next cycle.
PD stores limits in GB and usage in KB. Usage refreshes update only usage and preserve
concurrent limit changes across the REST, gRPC and quota-check service instances.

Server graph-space responses retain the integer GB `storage_used` field and add
`storage_used_kb` for consumers that need to display usage below one GB. The
`storage_percent` ratio is calculated from that precise usage, without rounding it
to whole percentage points. Older clients can continue reading `storage_used`.
Older Servers, including 1.7, preserve the additional `storage_used_kb` field as a
string when rewriting a space. This Server accepts both its integer and string
forms, allowing metadata to survive a rolling upgrade or rollback and re-upgrade.
Missing, negative or invalid precision values fall back to the original GB usage.

Older PD quota cycles stored some `storage_limit` values in KB. These untagged
records are not automatically converted: the same number could be a legitimate GB
quota. After **all PD replicas** are upgraded, an administrator can normalize each
affected space with one ordinary Server `PUT /graphspaces/{name}` update. First read
the current **Server** space configuration, then submit the unchanged quotas,
including its `storage_limit` in GB and existing compute limits, using the update
format below. Do not copy or divide the raw PD limit to infer the intended quota.
This rewrites the PD limit in GB while preserving usage; it is an explicit
configuration write, not a required automatic migration. Until then PD
`GET /graph-spaces` can report mixed limit units. Storage enforcement reads the
Server configuration and is unaffected by the stale PD reporting field.

Each Server refreshes its cached graph-space configuration on metadata ADD and
UPDATE notifications. This includes quota edits, whose REST path can emit ADD for
an existing space. Notifications carry the space name; the receiver reads current
metadata so delayed or repeated events do not restore earlier limits. Refresh follows
asynchronous metadata delivery, not a synchronous consistency guarantee. Upgrade all
Server replicas to receive these refreshes when requests are routed among them.

After a graph space is deleted, each Server evicts its cached space and service
entries on the metadata removal event. Peers do not repeat destructive backend
cleanup or delete shared metadata when handling that notification. A delayed event
for a name already recreated in metadata refreshes the space from metadata and evicts
its old service cache, so callers see the recreated configuration. Refreshes and
cache misses retry when a local space change completes during the read, including
graph and service counter updates. Older reads cannot replace a newer local quota
or counter, or return a discarded snapshot after local creation or deletion.

New graph spaces, including a newly bootstrapped `DEFAULT` space, use finite
resource defaults: 128 CPU cores, 128 GB memory, 128 compute CPU cores, 128 GB
compute memory, 10240 GB storage, 100 graphs and 100 roles. Existing graph-space
configurations are preserved on restart; this change does not migrate their quotas.
For `POST /graphspaces`, omitted compute limits are stored as `0`, meaning continuously
inherit the space's CPU and memory limits. Zero is neither unlimited nor a zero-sized
allocation. A request for 4 cores / 8 GB without compute fields therefore has effective
compute limits of 4 cores / 8 GB; after updating CPU/memory to 8 cores / 16 GB, compute
also becomes 8 cores / 16 GB. Omitting all resource fields gives effective limits of
128 cores / 128 GB for both. Explicit positive compute values remain independent.
`PUT /graphspaces/{name}` with `action=update` preserves omitted compute fields;
explicit `compute_cpu_limit: 0` or `compute_memory_limit: 0` restores inheritance for
that field. Responses retain the raw zero marker so editing clients can preserve it.
The bootstrapped `DEFAULT` space keeps explicit compute limits of 128 cores / 128 GB;
existing positive compute limits are not converted to inheritance.
Upgrade the Java client together with the Server before sending partial space
updates. The client must preserve the distinction between omitted fields and explicit zeros.
Older clients send zero for omitted compute fields; the new Server interprets an
explicit zero as restoring inheritance and cannot distinguish that from an omitted
SDK property. Until clients are upgraded, use read-modify-write and explicitly send
the current compute limits to preserve independent positive values.
When OLTP and OLAP share a Kubernetes namespace, their quotas are added together;
separate namespaces receive the respective resource limits.

The graph-count limit is enforced: after 100 graphs, creating another graph fails with
`reach graph limit`. Storage enforcement reads Server configuration under
`HUGEGRAPH/<pd.server-cluster>/GRAPHSPACE/CONF/`. Set `pd.server-cluster` in every PD
node's `conf/application.yml` to the Server `cluster` in `rest-server.properties`.
The PD setting defaults to `hg` for compatibility; the Server default is `hg-test`,
so deployments using that default must set `pd.server-cluster: hg-test`. This is a
metadata namespace, not the numeric `pd.cluster_id`. Changing it does not migrate
existing metadata or modify the Server configuration. When usage exceeds the
configured storage limit, PD marks the space's graphs
`ReadOnly`; the next check restores `ReadWrite` once usage is within the limit.
CPU and memory limits configure Kubernetes namespace quotas when `server.use_k8s=true`;
they do not impose process limits in other deployments.

Deleting a graph space also removes its exact PD storage-limit/usage record after
all its graphs have been removed. An empty space that still exists retains its limit
and reports zero usage. Cleanup is idempotent; if PD cleanup fails, the Server keeps
the space configuration and list entry, and reports the failure so deletion can be
retried. By that stage, graphs, graph-space authorization, schema templates and
services have already been cleared. This is not a transaction: retrying does not
restore any of those resources. PD rejects cleanup while the space still has graph
metadata and rechecks current graphs before recreating a missing usage record, so an
older quota collection cannot recreate a deleted space's statistics. This does not
purge records left by deletions before the upgrade. Upgrade PD before Server: older
PD versions do not implement the new `DeleteGraphSpace` RPC. An `UNIMPLEMENTED`
response arrives after the clearing steps above, retains the remaining space
configuration/list entry, and prevents completion until all PD nodes are upgraded.
Do not start space deletion during a Server-first rolling upgrade before PD
supports this RPC. No unsupported-RPC response silently skips statistics cleanup.
This cleanup does not serialize graph
creation with space deletion across Server replicas; avoid creating graphs in a space
while deleting it.

If cleanup is refused, the Server response includes the PD reason, remaining
record count and up to ten PD graph names (each limited to 128 characters).
Inspect PD's authenticated `GET /v1/partitionsAndStats` diagnostic endpoint. The
keys of its `partitions` object retain every full PD graph identifier, including
records with no partitions. This supplies the full names omitted or truncated in
the bounded error message. `GET /v1/graphs` lists only `/g` records and is not a
complete inventory for this check: schema `/m` records can also block cleanup.
Compare all matching identifiers with the Server's graph configuration. For
example, `shop/old/g` and `shop/old/m` belong to graph `old` in space `shop`, with
data and schema backend suffixes respectively. Earlier failed graph drops can
leave a PD record after its Server configuration is gone;
retrying space deletion alone does not remove that record.

Confirm ownership and any data that must be retained before recovery. For a
confirmed obsolete graph, use the normal Server graph APIs to recreate the
missing graph configuration with its original space, graph name and backend,
then drop that graph normally and retry space deletion. A graph owned by another
Server must be removed through that owner instead. The cleanup operation does
not guess ownership or delete remaining graph data automatically.

An administrator can raise limits with `PUT /graphspaces/DEFAULT`, for example:

```json
{"action":"update","update":{"name":"DEFAULT","max_graph_number":200,"max_role_number":100,
"cpu_limit":128,"memory_limit":128,"storage_limit":20480}}
```

Read the current space first and preserve the fields that are not being changed. The
example raises graph count to 200 and storage to 20480 GB. This changes configuration;
it does not remove existing graphs or migrate other spaces.
