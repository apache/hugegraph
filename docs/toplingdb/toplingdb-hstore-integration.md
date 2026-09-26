# ToplingDB and HStore Integration

## Deployment Model

ToplingDB and HStore solve different problems:

- ToplingDB is an optional RocksDB-compatible native runtime.
- HStore is HugeGraph's distributed storage backend.

In an HStore deployment, HugeGraph Server sends storage requests to Store. It
does not open a local RocksDB database.

```text
HugeGraph Server
  HStore client
      |
      v
PD cluster ---------------- Store cluster
local RocksDB metadata      local RocksDB data
optional Topling runtime    optional Topling runtime
```

ToplingDB therefore applies independently to PD and Store. Each process keeps
its existing RocksDB Java calls and loads its own native runtime and Easy
Migrate configuration.

## Runtime Setup

PD startup sets:

```bash
TOPLINGDB_EASY_MIGRATE_CONF="$PD_HOME/conf/rocksdb_pd.yaml"
```

Store startup sets:

```bash
TOPLINGDB_EASY_MIGRATE_CONF="$STORE_HOME/conf/rocksdb_store.yaml"
```

Both distributions carry the same `preload-topling.sh` helper and load only
their component-local Topling JAR and native library. PD and Store do not
discover or depend on a neighboring Server distribution. This is shared source
tooling, not a shared Java provider or shared DB owner.

## Configuration

Set the provider for each component that should use ToplingDB:

```yaml
rocksdb:
  provider: topling
```

PD and Store use distinct Easy Migrate YAML files and distinct HTTP ports.
Their `DBOptions.default` mappings remain the global fallbacks, while
`DBOptions.log` remains the specialized log-database profile.

The Server's graph configuration does not enable a local ToplingDB instance
when the backend is HStore.

## Open and Close Behavior

PD and Store continue to use normal RocksDB Java APIs. The preloaded native hook
applies options and tracks DB/CF lifecycle. Neither component reflectively
creates a repository or calls an alternate `openDB`.

Shutdown is also unchanged:

```text
SIGTERM
  -> PD/Store graceful shutdown
  -> Java closes CF handles and RocksDB
  -> native MaybeForgetCF and MaybeForgetDB
  -> process exits
```

Store's existing shutdown timeout is a general process boundary. A timeout does
not justify calling `SidePluginRepo.closeAllDB()` and this integration does not
change the broader Store thread-exit behavior.

## Validation

Validate PD and Store separately:

1. Confirm the component provider is `topling`.
2. Confirm the startup log reports the expected
   `TOPLINGDB_EASY_MIGRATE_CONF`.
3. Confirm only the prepared component native library is mapped.
4. Run DB/CF create, reopen, drop, and recreate operations with Easy Migrate
   enabled.
5. Stop the component with SIGTERM and verify normal process exit.

An ABI-only check may inspect the JAR, Topling API marker, and native mapping
without opening a database. It is not a storage-functionality test.
