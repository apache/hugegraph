# ToplingDB Documentation

ToplingDB is an optional RocksDB-compatible runtime for HugeGraph. Select it
when a HugeGraph process owns a local RocksDB database:

| Deployment | Process that selects ToplingDB |
|---|---|
| Standalone | HugeGraph Server |
| Distributed HStore | PD and Store |
| HStore-backed Server | None; it is a remote client |

Start with one of these paths:

- [Quickstart](toplingdb-quickstart.md) explains how to build a Topling
  distribution, select the provider, keep its data separate, start the
  component, and return to standard RocksDB safely.
- [Developer guide](toplingdb-development.md) covers the source layout,
  distribution contract, tests, CI, Docker status, and native runtime work.
- [Issue illustrations](images/issues/v1/README.md) explains the scope and
  core issues tracked by [Summary #240](https://github.com/hugegraph/hugegraph/issues/240).

Use the reference guides when you need more detail:

- [Architecture and configuration](toplingdb.md)
- [HStore integration](toplingdb-hstore-integration.md)
- [Operations](toplingdb-operations.md)
- [Troubleshooting](toplingdb-troubleshooting.md)
- [Security](toplingdb-security.md)

ToplingDB selection happens only at process startup. Changing
`rocksdb.provider` does not migrate or convert an existing data directory.
Standard RocksDB and ToplingDB must use separate data directories unless you
restore a verified, compatible snapshot into an empty directory.
