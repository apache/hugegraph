# Project overview

Apache HugeGraph is a graph database with standalone and distributed deployment modes. It provides a property-graph data model, schema and indexes, graph traversals, REST/Gremlin/Cypher access, authentication and operational tooling.

## Deployment modes

- **Standalone:** HugeGraph Server uses embedded RocksDB by default. The HBase backend remains supported but is deprecated. RocksDB is the shortest path for development and smaller deployments.
- **Distributed:** Server handles graph APIs, PD owns placement and metadata coordination, and HStore nodes own partition data and replication.

## Authoritative entry points

- Product and deployment overview: [`../../README.md`](../../README.md)
- Repository documentation map: [`../../docs/README.md`](../../docs/README.md)
- Build and contribution rules: [`../../docs/BUILDING.md`](../../docs/BUILDING.md), [`../../docs/CONTRIBUTING.md`](../../docs/CONTRIBUTING.md)
- Kubernetes deployment: [`../../helm/hugegraph/README.md`](../../helm/hugegraph/README.md)
- Repository agent guidance: [`../../AGENTS.md`](../../AGENTS.md)
- Current versions and module membership: [`../../pom.xml`](../../pom.xml)

Do not copy release versions or detailed feature lists into this memory. Read the owning source when exact current behavior matters.
