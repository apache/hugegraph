# 本地测试历史

每次执行测试前检查本文件。若代码、配置、数据集或测试本身没有变化，不重复执行已通过命令。

| command | commit | result | evidence |
|---|---|---|---|
| `bash -n tools/run-codex-loop.sh` | `c2463296d` | PASS | `continuation-script-fix-20260913T112316Z.log` |
| `python3 tools/raft-linearizability/test_capture_history.py` | `c2463296d` | PASS | `continuation-script-fix-20260913T112316Z.log` |
| `python3 tools/raft-linearizability/test_checker.py` | `c2463296d` | PASS | `continuation-script-fix-20260913T112316Z.log` |
| `mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,memory -DskipITs` | `working-tree` | FAIL (RAT: 4 unapproved files) | `api-gremlin-001-20260913.log` |
| `mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,memory -Drat.skip=true -DskipITs` | `working-tree` | INTERRUPTED (suite exceeded 2 min; partial traversal/core output) | `api-gremlin-001-20260913-rerun.log` |
| `mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,memory -Drat.skip=true -DskipITs -DfailIfNoTests=false -Dtest=TaskCoreTest#testGremlinJobWithSerializedResults` | `working-tree` | FAIL (TaskCoreTest: expected SUCCESS, got FAILED) | `api-gremlin-001-taskcore-20260913-rerun.log` |
| `mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,memory -Drat.skip=true -DskipITs -DfailIfNoTests=false -Dtest=TaskCoreTest#testGremlinJob` | `working-tree` | FAIL (test failure; see surefire output) | `api-gremlin-001-taskcore-basic-20260913.log` |
| `mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,memory -Drat.skip=true -DskipITs -DfailIfNoTests=false -Dtest=TaskCoreTest#testGremlinJob` | `working-tree` | FAIL (environment: Groovy 2.5 ASM rejects Java 21 class file major 65; `Unsupported class file major version 65`) | `api-gremlin-001-taskcore-basic-20260913.log` |
