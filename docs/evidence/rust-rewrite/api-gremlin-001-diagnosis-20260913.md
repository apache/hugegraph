# API-GREMLIN-001 diagnosis (2026-09-13)

The pinned executable entries `TaskCoreTest#testGremlinJob` and `TaskCoreTest#testGremlinJobWithSerializedResults` both fail before traversal execution. The root cause is environmental: the suite runs on OpenJDK 21 (`java -version` reports 21.0.10), while TinkerPop's Groovy 2.5.14 ASM cannot read Java class-file major version 65. The stack terminates in `GroovyClassLoader` with `IllegalArgumentException: Unsupported class file major version 65`.

Evidence: `api-gremlin-001-taskcore-basic-20260913.log` and `api-gremlin-001-taskcore-20260913-rerun.log`.

No source or test changes were made. A Java 11/17 runtime (or upgraded Groovy/TinkerPop dependency) is required before this contract can produce a SUCCESS oracle.
