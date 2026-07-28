# HugeGraph Server

HugeGraph Server consists of two layers of functionality: the graph engine layer, and the storage layer.

- Graph Engine Layer:
  - REST Server: Provides a RESTful API for querying graph/schema information, supports the [Gremlin](https://tinkerpop.apache.org/gremlin.html) and [Cypher](https://en.wikipedia.org/wiki/Cypher) query languages, and offers APIs for service monitoring and operations.
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
