# API-GREMLIN-001 independent oracle

Oracle source: `TaskCoreTest` assertions in `hugegraph-server/hugegraph-test/src/main/java/org/apache/hugegraph/core/TaskCoreTest.java`.

The Java 11 execution evidence records both contract assertions passing: `testGremlinJob` requires task status `SUCCESS`, type `gremlin`, and result `8`; `testGremlinJobWithSerializedResults` requires the deterministic serialized traversal result string asserted by the test. The independent check is the test's persisted task reload assertion for `testGremlinJob`, which verifies the same status/type/result after scheduler re-read.

Evidence: `api-gremlin-001-java11-docker-m2-20260913.log`, `api-gremlin-001-serialized-java11-20260913.log`.
