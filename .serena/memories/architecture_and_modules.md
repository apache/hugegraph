# Architecture and module ownership

```text
Clients and ecosystem tools
          │
          ▼
HugeGraph Server ─────────────────► RocksDB                 (standalone)
          │
          ├─ metadata / placement lookup ─► PD ── coordinates ─► HStore nodes
          └─ graph data requests ──────────────────────────────► HStore nodes  (distributed)
```

PD supplies metadata, placement and membership information; Server sends graph data requests directly to the selected HStore nodes.

## Top-level ownership

- `hugegraph-server/`: graph engine, schema, tasks, traversers, REST/Gremlin/Cypher APIs and storage adapters.
- `hugegraph-pd/`: placement, metadata, cluster scheduling and PD clients.
- `hugegraph-store/`: partition storage, Store clients and Raft replication.
- `hugegraph-struct/`: shared graph structures, identifiers and serialization used across distributed modules.
- `hugegraph-commons/`: shared utilities and RPC infrastructure.
- `hugegraph-cluster-test/`: cross-component cluster validation.
- `install-dist/`: distribution assembly, dependency inventory and release metadata.
- `helm/hugegraph/` and `docker/`: Kubernetes and container deployment assets.

Read the nearest module `AGENTS.md` before changing an implementation. For current submodule membership and dependency edges, inspect the root and module POMs rather than maintaining a copied count here.
