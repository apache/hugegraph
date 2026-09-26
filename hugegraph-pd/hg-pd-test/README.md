# PD module tests

Run common and core suites separately so native and Raft singletons do not carry
state between suite processes:

```bash
mvn test -pl hugegraph-pd/hg-pd-test -am -P pd-common-test
mvn test -pl hugegraph-pd/hg-pd-test -am -P pd-core-test
```

The core fixture uses `tmp/pd-core-data` relative to the test module by default.
Override it with `-Dpd.test.data_path=/absolute/task-owned/path` when isolating a
validation run. The fixture deletes that directory before initialization; never
point it at an existing service's data. Client and REST suites also require the
PD service setup described in the repository CI workflow.
