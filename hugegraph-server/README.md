# HugeGraph Server

<!-- TODO: update release image tags and version examples after 1.8.0 is published. -->

HugeGraph Server consists of two layers of functionality: the graph engine layer, and the storage layer.

- Graph Engine Layer:
  - REST Server: Provides a RESTful API for querying graph/schema information, supports the [Gremlin](https://tinkerpop.apache.org/gremlin.html) and [Cypher](https://en.wikipedia.org/wiki/Cypher) query languages, and offers APIs for service monitoring and operations.
  - See [Cypher compatibility notes](../docs/cypher-compatibility.md) for request forms, verified behavior, and
    unverified scope.
  - Graph Engine: Supports both OLTP and OLAP graph computation types, with OLTP implementing the [Apache TinkerPop3](https://tinkerpop.apache.org) framework.
  - Backend Interface: Implements the storage of graph data to the backend.

- Storage Layer:
  - Storage Backend: Includes RocksDB (default, embedded), HStore (distributed), HBase (deprecated and planned for removal in 2.0), and the test-only Memory backend. Users can extend custom backends without modifying the existing source code.

## Backend Evolution and Compatibility

The current mainline does not include implementations for the historical backends. The following timeline distinguishes current support from legacy compatibility guidance:

```text
┌─────────────────────────┐     ┌─────────────────────────┐     ┌─────────────────────────┐
│           1.0           │     │           1.5           │     │           2.x           │
│     Historical era      │     │ Compatibility boundary  │     │     Future roadmap      │
│                         │     │            ↓            │     │                         │
│    MySQL · PostgreSQL   │────▶│    1.7–2.0 mainline     │────▶│    RocksDB · HStore     │
│   Cassandra · ScyllaDB  │     │    RocksDB · HStore     │     │                         │
│       Palo · HBase      │     │    HBase: deprecated    │     │    (HBase: removed)     │
└─────────────────────────┘     └─────────────────────────┘     └─────────────────────────┘
```

Memory remains a test-only backend throughout. Historical backend users must operate and
maintain a compatible release; these implementations are not restored to the current source
tree or distribution packages.

## Readiness endpoint

`GET /readiness` answers `200` while this Server can serve graph traffic and `503` otherwise. It is
unauthenticated and needs no graphspace prefix, so a Kubernetes `httpGet` probe can call it as is. The
JSON body carries `ready`, `storage` (`embedded`, `hstore`, `hbase`, or a comma-separated list), a
`reason`, whether the answer was `cached`, and one `probes` entry per probed backend configuration
(backend, `ready`, `reason`); it carries no addresses and no graph names.

What ready means:

- `embedded`: no graph on a remote storage; ready once the REST layer is up.
- `hstore`: this Server knows a Store list and at least one Store answers a direct status ping
  (`HgStoreState.getScanState`). A Store whose status answers while its raft or partition path is
  broken is not detected.
- `hbase`: every table of the graph (schema store, graph store, system store) exists, is enabled and
  is available.
- A configured graph that failed to load at startup makes the Server not ready whatever its backend
  (`reason` says how many, `failed_graphs` carries the count).

Graphs that share a backend configuration (the same `pd.peers`; for hbase every graph is its own
scope, since the table namespace is derived from the graph name) share one probe; independent
configurations are probed side by side within one time budget and the Server is ready only when all
of them are.

Options in `rest-server.properties`:

| option | default | meaning |
|---|---|---|
| `readiness.timeout` | `1000` | time budget of one probe in ms; a backend that does not answer within it reads as not ready |
| `readiness.cache_ttl` | `2000` | a probe result is reused for this many ms |
| `readiness.max_waiters` | `16` | callers that may wait for the probe in flight; beyond it a caller gets `503` at once |

## Docker

### Standalone Mode

```bash
docker run -itd --name=hugegraph -p 8080:8080 hugegraph/hugegraph:1.7.0
```

> Use release tags (e.g., `1.7.0`) for stable deployments. The `latest` tag is intended for testing or development only.

### Distributed Mode (PD + Store + Server)

For a full distributed deployment, use the compose file in the `docker/` directory at the repository root:

```bash
cd docker
HUGEGRAPH_VERSION=1.7.0 docker compose -f docker-compose-3pd-3store-3server.yml up -d
```

See [docker/README.md](../docker/README.md) for the full setup guide.

## RISC-V Development and Testing

The [RISC-V Server CI](../.github/workflows/riscv64-ci.yml) validates a RocksDB-only Server build and runtime smoke test on 64-bit Linux RISC-V through QEMU. It is a correctness check,
not a performance benchmark. (other backends & non-64-bit Linux RISC-V environments are out of scope)
