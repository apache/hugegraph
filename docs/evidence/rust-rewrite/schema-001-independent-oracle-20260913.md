# SCHEMA-001 independent invariant oracle

Oracle source: `VertexCoreTest#testAddVertexWithUniqueIndex`.

Invariant: a schema unique index accepts the first vertex value and rejects a
second vertex carrying the same indexed property, preserving uniqueness.
The Java 11 execution log records one test with zero failures.

Evidence: `schema-001-unique-index-java11-20260913.log`.
