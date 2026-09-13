# Local E2E contract matrix (2026-09-13)

This matrix is the execution index for the current contract-inventory stage. A
row is covered only when its command, fixture, oracle and result log are linked.

| contract_id | scenario | execution entry | oracle | status |
|---|---|---|---|---|
| API-REST-001 | vertex CRUD and validation | `VertexApiTest`, `api-test,memory`, Java 11 host network | `api-rest-001-independent-oracle-20260913.md` | complete |
| API-GREMLIN-001 | traversal result and ordering | `hugegraph-test` core profile | `api-gremlin-001-independent-oracle-20260913.md` | complete |
| SCHEMA-001 | property/index uniqueness | `VertexCoreTest#testAddVertexWithUniqueIndex`, core-test,memory, Java 11 Docker | `schema-001-independent-oracle-20260913.md` | complete |
| TXN-001 | commit/rollback visibility | `PrimaryKeyStrategyCoreTest#testFoldsSingleCardinalityProperties`, core-test,memory, Java 11 Docker | `txn-001-independent-oracle-20260913.md` | complete |
| RAFT-001 | replicated write under restart | `tools/rust-rewrite-cluster/smoke.sh` | `raft-001-independent-oracle-20260913.md` | complete |
| SNAP-001 | snapshot export/import and corruption rejection | `tools/rust-rewrite-cluster/verify-snapshot-roundtrip.py` and corruption verifier | `snap-001-independent-oracle-20260913.md` | complete |

Coverage is currently 6/6 complete (100%). Mutation validation remains required before the final Go decision.

Required evidence per row: pinned command/profile, fixture digest and seed,
stdout/stderr log, normalized result, oracle report, and (for mutation rows) a
report showing the injected defect was detected.
