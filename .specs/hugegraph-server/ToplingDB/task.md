# Tasks of ToplingDB Easy Migrate

## Runtime Integration

- [x] Keep Server, PD, and Store on the existing RocksDB Java API.
- [x] Prepare the optional ToplingDB JAR, native library, and Web resources
  before service startup.
- [x] Select ToplingDB explicitly from component configuration.
- [x] Export a readable component-specific
  `TOPLINGDB_EASY_MIGRATE_CONF` before the JVM starts.
- [x] Let the native Easy Migrate hook apply options and track DB/CF lifecycle.
- [x] Keep `DBOptions.default` as the global fallback and retain the dedicated
  `DBOptions.log` profile.

## Component Integration

- [x] Use `conf/toplingdb.yaml` for standalone Server.
- [x] Use `conf/rocksdb_pd.yaml` for PD.
- [x] Use `conf/rocksdb_store.yaml` for Store.
- [x] Keep HStore-backed Server as a remote client without local RocksDB
  ownership.

## Lifecycle and Testing

- [x] Use normal RocksDB Java close paths during graceful shutdown.
- [x] Rely on native `MaybeForgetCF` and `MaybeForgetDB`; do not add Java-side
  repository cleanup.
- [x] Verify JAR/native mapping without constructing a plugin repository.
- [x] Run every ToplingDB DB/CF functional test with Easy Migrate configuration
  enabled.
- [ ] Keep general Store shutdown timeout and unrelated thread-exit fixes in
  their existing workstream.
