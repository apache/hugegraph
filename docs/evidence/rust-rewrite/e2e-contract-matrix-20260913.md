# Local E2E contract matrix (2026-09-13)

This matrix is the execution index for the current contract-inventory stage. A
row is covered only when its command, fixture, oracle and result log are linked.

| contract_id | scenario | execution entry | oracle | status |
|---|---|---|---|---|
| API-REST-001 | vertex CRUD and validation | `VertexApiTest`, `api-test,memory`, Java 11 host network | `api-rest-001-independent-oracle-20260913.md` | complete |
| API-GREMLIN-001 | traversal result and ordering | `hugegraph-test` core profile | `api-gremlin-001-independent-oracle-20260913.md` | complete |
| SCHEMA-001 | property/index uniqueness | core test profile | invariant oracle | planned |
| TXN-001 | commit/rollback visibility | core test profile | state-transition oracle | planned |
| RAFT-001 | replicated write under restart | `tools/raft-linearizability` harness | independent linearizability oracle | partial |
| SNAP-001 | snapshot export/import and corruption rejection | `tools/rust-rewrite-cluster/verify-snapshot-corruption.py` (negative component) | checksum/state oracle | partial |

Coverage is currently 2/6 complete (33%); SNAP-001 now has a deterministic
corruption-rejection component, but no real snapshot round-trip. RAFT-001 is partial because the
independent single-register oracle exists but real three-node history capture is
still absent. No Go decision is implied by this inventory.

Required evidence per row: pinned command/profile, fixture digest and seed,
stdout/stderr log, normalized result, oracle report, and (for mutation rows) a
report showing the injected defect was detected.
