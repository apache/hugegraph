# REST Gremlin execution

JSON requests to REST `GET /gremlin` and `POST /gremlin` automatically execute
in-process when `gremlinserver.url` identifies the local Gremlin Server started by
`HugeGraphServer`. Targets on other hosts or ports use HTTP forwarding. There is no
execution-mode configuration option.

```text
local Gremlin target:  REST -------------------------------------> Gremlin workers
remote Gremlin target: REST -> Jersey HTTP client -> Gremlin HTTP -> Gremlin workers
```

Both paths use the configured graph manager, script engines and JSON serializer.
For local targets these are the same instances owned by the co-started Gremlin Server.
The Gremlin listener remains available to HTTP/WebSocket clients, Driver and Console.
Cypher and Gremlin jobs keep their existing execution paths.

## Automatic selection

In `conf/rest-server.properties`, keep the existing Gremlin URL consistent with the
host and port in `conf/gremlin-server.yaml`:

```properties
gremlinserver.url=http://127.0.0.1:8182
```

Every address resolved for the target hostname must belong to the co-started listener.
For a specific bind address, all target addresses must equal that address; another
loopback or local interface address at the same port can identify a different process.
For a wildcard bind, all target addresses must be concrete local addresses in the same
address family. Cross-family wildcard/dual-stack cases conservatively use forwarding.
Wildcard target URLs also use forwarding, since they do not identify a concrete endpoint.

The URL's port must match the co-started listener's port. Omitted HTTP/HTTPS ports are
interpreted as 80/443. Unresolved hostnames, mixed or mismatched target addresses, and
other hosts or ports remain on the forwarding path.
A standalone `RestServer` without a co-started Gremlin Server always forwards.

TLS does not affect the choice: local HTTP and HTTPS targets both use in-process
execution, including when the Gremlin listener enables TLS. This does not change the
TLS settings of either listener. TLS handshakes and client certificates on the Gremlin
listener apply to direct network clients; embedded requests use the REST entry point's
transport and authentication, without an internal TLS connection.

Restart after changing the target URL. There is no automatic forwarding retry after
an embedded execution failure: a query may already have committed even when the caller
receives an error.

Local execution supports the standard HugeGraph `WsAndHttpChannelizer` configuration,
adapted internally to `HugeGraphWsAndHttpChannelizer`. Custom channelizers and custom
Gremlin authorizers retain HTTP forwarding, so their listener-side behavior remains in use.

Embedded execution requires matching REST and Gremlin authentication: either both are anonymous, or
both use `StandardAuthenticator`, with `WsAndHttpBasicAuthHandler` on Gremlin and
`authentication.config.tokens` resolving to the same REST configuration file.
Custom or mismatched authentication, different config files, and config files that cannot
be resolved retain HTTP forwarding. Unsupported local combinations log a warning with the
reason for forwarding. This selection does not add a startup rejection; each listener's
existing configuration validation remains in place. The choice is made before query
execution and does not retry a script through another path after execution begins.

The local path is finalized after the Gremlin listener initializes its serializer registry.
If the registry has no `application/json` text serializer, including when a configured
GraphSON serializer fails to load or configure, the adapter already injected into REST
selects HTTP forwarding and logs a warning. It uses the listener's initialized registry;
no additional serializer is created. JSON REST requests then retain the listener's existing
rejection behavior, while configured WebSocket serializers such as GraphBinary remain
available. This startup choice is made once, before queries are executed; an execution
or serialization failure never triggers a forwarding retry.

## Preserved behavior

- The REST endpoint remains JSON-only. It does not expose typed GraphSON or GraphBinary
  request formats, or introduce new content negotiation.
- GET keeps Jersey URI construction and the existing single-value rule for every query
  parameter. POST uses the TinkerPop 3.8.1 JSON request parser.
- Alias resolution uses the live graph manager. Request bindings override alias bindings.
- Evaluation, iterator materialization, commit and serialization stay on the same Gremlin
  worker, preserving thread-bound graph transactions and authentication context.
- Successful HTTP requests commit before serialization. A serialization failure does not
  imply rollback of an already committed write.
- Existing rollback hooks, evaluation timeout, error mapping and Gremlin metrics remain
  in use. REST input/output histograms remain at the same call sites; output length
  reflects the response entity metadata of the selected transport.
- The serializer instance is obtained from the initialized Channelizer. No second serializer
  registry, graph registry or script engine is created.

Embedded requests authenticate once at the REST entry point. Workers inherit the existing
HugeGraph user context without rechecking credentials. Requests accepted before token expiry
or a permission change retain the existing context semantics; subsequent requests follow the
existing authentication and cache behavior. This feature adds no cross-node revocation or
immutable authorization snapshot guarantee. Audit records use the original REST peer address;
forward audit records continue to use the internal HTTP client's address.

## Timeouts, load and lifecycle

`gremlinserver.max_route` is retained as blocking admission for embedded requests. Pending
callers wait rather than receive a new overload rejection. `gremlinserver.timeout` bounds
waiting for the result after admission, while the Gremlin executor retains its separate
execution timeout. The outer REST server's request timeout is unchanged.

A REST timeout does not cancel or retry the script. The script may continue and commit;
TinkerPop's existing execution timeout and rollback hooks remain responsible for execution.
Admission is released when the REST execution wait ends. This preserves the existing
possibility of work accumulating after client timeouts; it introduces no new hard task limit.

The local adapter is unavailable until Gremlin startup has initialized its serializer registry
and selected embedded execution or forwarding. During normal server shutdown, embedded
execution rejects new submissions before the existing Gremlin stop sequence. An adapter
that selected forwarding retains the existing HTTP lifecycle and closes its client when
REST shuts down. The Gremlin server continues to own worker shutdown and graph closure.
This feature adds no new readiness endpoint, deployment probe, global drain deadline or
stronger termination guarantee.

Protocol limits are checked against the internal request shape. This does not provide an
additional early limit before Jersey reads the outer REST request body. Existing transport
edge behavior is pinned by differential tests, including URI/header failures; it is not a new
listener-wide protection policy.

## Verification

Use Java 17 from the repository root:

```bash
mvn test -pl hugegraph-server/hugegraph-test -am -P unit-test \
  -Dtest=EmbeddedGremlinRestExecutorTest,HttpGremlinRequestHandlerTest,ContextGremlinServerHttpTest,ContextTaskTest,WsAndHttpBasicAuthHandlerTest,GremlinQueryAPITest \
  -Dsurefire.failIfNoSpecifiedTests=false
```

The focused tests compare real forward HTTP responses with embedded execution, exercise
both REST HTTP endpoints, and cover distinct same-port listeners, wildcard/address-family
selection, unsupported-configuration and missing-JSON-serializer forwarding, parsing/encoding,
protocol limits, error mapping,
request identity, context isolation, blocking admission, timeouts, shared serializers,
dynamic aliases and graph transaction side effects on Memory and RocksDB. Response stack traces and newly generated
request IDs require semantic comparison; deterministic responses also use byte comparisons.

Run backend and distribution checks appropriate to the intended deployment before
rolling out automatic local execution. Focused tests do not establish HStore compatibility
or production performance.

Readiness, overload policy, authentication-cache consistency and global shutdown redesign
remain separate tasks from this execution-path change.

For a manual script-only HTTP transport comparison, run the focused tests above, then:

```bash
python3 hugegraph-server/hugegraph-test/scripts/gremlin-rest-benchmark.py \
  --output /tmp/gremlin-rest-benchmark.json
```

Set `JAVA_HOME` to Java 17. The runner uses the tested runtime classpath, starts two
local REST endpoints sharing one Gremlin worker pool, checks every measured response,
and alternates mode order across four rounds. It saves latency samples and environment
metadata. These measurements include client/REST overhead and exclude real backend I/O;
100 warmup requests do not establish steady-state JIT behavior.

For a Linux single-node RocksDB comparison, build this distribution and retain a
pre-automatic-selection distribution whose default path is forwarding. Use an eight-CPU
host with Java 17. The following runner requires a new output directory and binds both
listeners to loopback. It generates its own graph and preserves all evidence under that
directory:

```bash
python3 hugegraph-server/hugegraph-test/scripts/gremlin-rest-rocksdb-benchmark.py \
  --distribution /path/to/apache-hugegraph-server-1.8.0 \
  --forward-distribution /path/to/previous-forward-default-build \
  --java-home /path/to/jdk-17 \
  --output /path/to/new-results-directory
```

It checks GET/POST, bindings, JSON values, aliases, rollback, committed write counts and
restart persistence. The default dataset has 10,000 vertices and 50,000 edges. Four server
starts use the baseline/candidate/candidate/baseline builds in forward/embedded/embedded/forward
order, identical JVM settings and persistent data. The runner records each build's jar hashes.
This compares builds, so other changes between their revisions may affect results. The
script-only runner above explicitly constructs both adapters in one JVM and remains
available for a same-build transport comparison. Each workload has a five-second warmup
and three-second measurements at concurrency 1, 8 and 32. Samples use a closed-loop client on the same host; CPU affinity separates
the client and server, while unrelated host workloads can still affect both. The results
include raw latency samples, response checks, CPU/load snapshots, GC and server logs.
These short measurements do not establish production saturation or steady-state behavior.
