# HugeGraph Server

HugeGraph Server consists of two layers of functionality: the graph engine layer, and the storage layer.

- Graph Engine Layer:
  - REST Server: Provides a RESTful API for querying graph/schema information, supports the [Gremlin](https://tinkerpop.apache.org/gremlin.html) and [Cypher](https://en.wikipedia.org/wiki/Cypher) query languages, and offers APIs for service monitoring and operations.
  - Graph Engine: Supports both OLTP and OLAP graph computation types, with OLTP implementing the [Apache TinkerPop3](https://tinkerpop.apache.org) framework.
  - Backend Interface: Implements the storage of graph data to the backend.

- Storage Layer:
  - Storage Backend: Supports multiple built-in storage backends (RocksDB/Memory/HStore/HBase/...) and allows users to extend custom backends without modifying the existing source code.

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
