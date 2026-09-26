# Requirements of ToplingDB Easy Migrate

## Runtime Compatibility

HugeGraph must support standard RocksDB by default and an explicitly selected
ToplingDB runtime without changing the RocksDB Java API used by Server, PD, or
Store.

Acceptance criteria:

- ToplingDB is selected only by component configuration.
- Missing or unreadable Easy Migrate configuration fails ToplingDB startup.
- Standard RocksDB does not require an Easy Migrate configuration.
- No HugeGraph Java code creates or manages a `SidePluginRepo`.

## External Configuration

The startup layer must set `TOPLINGDB_EASY_MIGRATE_CONF` to the configuration
for the component that owns RocksDB. The native hook must apply the YAML
options to normal RocksDB open and column-family operations.

`DBOptions.default` remains the fallback for unmatched databases.
`DBOptions.log` remains a separate specialized mapping.

## Component Ownership

Standalone Server, PD, and Store may own local RocksDB instances. A Server
using the HStore backend is a remote client and must not be described as
opening a local RocksDB instance.

## Graceful Shutdown

SIGTERM must enter the existing HugeGraph, PD, or Store graceful shutdown,
which closes column families and databases through the normal RocksDB API.
Easy Migrate then releases native tracking through `MaybeForgetCF` and
`MaybeForgetDB`.

No operation or test may require `SidePluginRepo.closeAllDB()`. The existing
bounded Store shutdown timeout remains a general lifecycle boundary, not a
ToplingDB-specific cleanup mechanism.

## Verification

- Standard RocksDB DB/CF tests run without Easy Migrate.
- ToplingDB ABI diagnostics may inspect only JAR/native availability and
  mappings.
- Every ToplingDB DB/CF functional test runs with a readable
  `TOPLINGDB_EASY_MIGRATE_CONF`.
