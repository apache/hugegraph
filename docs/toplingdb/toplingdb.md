# ToplingDB Support and Configuration

ToplingDB is an optional RocksDB-compatible runtime for HugeGraph. The
integration uses ToplingDB Easy Migrate: HugeGraph keeps its existing RocksDB
Java API, while a native hook applies ToplingDB configuration and tracks the
DB/column-family lifecycle.

New users should start with the [ToplingDB quickstart](toplingdb-quickstart.md).
Contributors should use the
[ToplingDB developer guide](toplingdb-development.md).

## Architecture

```text
component configuration
  -> component-local prepare-topling.sh prepares the runtime
  -> start script selects rocksdb or topling
  -> preload-topling.sh exports TOPLINGDB_EASY_MIGRATE_CONF
  -> JVM uses the existing RocksDB Java API
  -> native Easy Migrate hook applies options and tracks DB/CF close
```

There is no HugeGraph Java provider SPI, reflected `SidePluginRepo`, or
separate ToplingDB service process.

## Component Scope

| Component | Local RocksDB owner | Easy Migrate YAML |
|---|---:|---|
| Standalone HugeGraph Server | Yes | `conf/toplingdb.yaml` |
| HugeGraph PD | Yes | `conf/rocksdb_pd.yaml` |
| HugeGraph Store | Yes | `conf/rocksdb_store.yaml` |
| HugeGraph Server using HStore | No | Not applicable |

An HStore-backed Server is a remote client. Configure ToplingDB on PD and Store,
not on that Server process.

## Enable ToplingDB

ToplingDB is opt-in. Use the Topling distribution or image for the component,
set the provider in its configuration, then prepare the bundled runtime before
starting the service.

For Server:

```properties
rocksdb.provider=topling
```

For PD or Store, set the corresponding `rocksdb.provider` value in its
application YAML.

Each Server, PD, and Store distribution carries the same runtime helpers. A
Topling distribution additionally carries its component-local JAR under
`lib/topling/`. Prepare that runtime without a source checkout:

```bash
bin/prepare-topling.sh
```

The standard distribution carries the helpers but no Topling JAR or native
library. Selecting `topling` from a standard distribution therefore fails
explicitly instead of falling back to RocksDB.

The normal start script sources its own `bin/preload-topling.sh`. For ToplingDB
it validates the prepared files and exports:

```bash
TOPLINGDB_EASY_MIGRATE_CONF=/absolute/path/to/component/config.yaml
LD_LIBRARY_PATH=/path/to/component/library:...
LD_PRELOAD=.../librocksdbjni-linux64.so
```

A missing JAR, native library, or readable configuration is a startup error.
HugeGraph does not silently change providers.

## Easy Migrate Configuration

The component YAML defines ToplingDB options and optional HTTP observability.
The provided configurations use:

- `DBOptions.default` as the global fallback;
- `DBOptions.log` as a dedicated profile for the log database.

Keep both mappings. The log profile is intentional and must not be folded into
the fallback.

The embedded HTTP endpoint has no authentication. Keep it bound to a trusted
interface and restrict network access.

## DB and Column-Family Lifecycle

HugeGraph continues to call `RocksDB.open()`, column-family APIs, handle
`close()`, and `RocksDB.close()`. Easy Migrate observes these calls in native
code, applies the matching configuration, retains live DB/CF state, and releases
it through `MaybeForgetCF` and `MaybeForgetDB` on normal close.

Do not create or close a `SidePluginRepo` from HugeGraph Java code.

## Shutdown

Use the normal component stop script or send SIGTERM:

```text
SIGTERM
  -> component graceful shutdown
  -> normal CF/DB close
  -> native MaybeForgetCF / MaybeForgetDB
  -> JVM exit
```

Store has an existing bounded shutdown wait. If it times out, diagnose the
ordinary Store thread/lifecycle issue. Do not call a Topling-specific
`closeAllDB()` workaround.

## Compatibility and Rollback

ToplingDB-specific options may produce WAL, SST, or metadata that standard
RocksDB cannot safely reopen. Switching only `rocksdb.provider` or replacing a
JAR is not a rollback.

Before enabling ToplingDB, create a complete RocksDB-consistent checkpoint or
backup. To roll back, stop writers and restore the full pre-migration snapshot
with the matching standard RocksDB runtime.

## Related Guides

- [Documentation index](README.md)
- [Quickstart](toplingdb-quickstart.md)
- [Developer guide](toplingdb-development.md)
- [ToplingDB and HStore integration](toplingdb-hstore-integration.md)
- [Operations guide](toplingdb-operations.md)
- [Troubleshooting](toplingdb-troubleshooting.md)
- [Security guide](toplingdb-security.md)
