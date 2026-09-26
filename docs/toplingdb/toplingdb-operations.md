# ToplingDB Operations Guide

## Preflight

Before startup:

1. Confirm the component configuration selects `topling`.
2. Run the installation step for that component.
3. Confirm its Easy Migrate YAML and prepared native library are readable.
4. Check that the configured HTTP port is available and restricted to a
   trusted interface.
5. Create and verify a complete pre-migration RocksDB checkpoint or backup.

The expected configuration files are:

| Component | File |
|---|---|
| Standalone Server | `conf/toplingdb.yaml` |
| PD | `conf/rocksdb_pd.yaml` |
| Store | `conf/rocksdb_store.yaml` |

## Monitoring

Monitor read/write latency, compaction backlog, cache usage, WAL growth, disk
space, native memory, and process health. The sample configurations keep the
Topling HTTP endpoint disabled by default (`auto_start_http: false`). If it is
needed, enable it explicitly; it can show native state but has no
authentication and must not be exposed directly to untrusted networks.

Use the component logs to confirm the selected configuration path and native
library. Do not infer ToplingDB activation from the JAR name alone.

## Tuning

Change one YAML setting group at a time and benchmark with a representative
workload. Preserve:

- `DBOptions.default` as the global fallback;
- the dedicated `DBOptions.log` profile;
- component-specific HTTP ports;
- a memory budget that includes JVM heap, block cache, memtables, native
  allocations, and background jobs.

## Graceful Stop and Restart

Use the normal stop script or SIGTERM. Do not stop a separate “ToplingDB
process”; none exists.

```text
SIGTERM
  -> HugeGraph/PD/Store graceful shutdown
  -> normal CF/DB close
  -> native MaybeForgetCF / MaybeForgetDB
  -> JVM exit
```

Do not invoke `SidePluginRepo.closeAllDB()`. Store's stop script already has a
bounded wait. If it reports a timeout, collect thread dumps and component logs
and investigate the ordinary Store shutdown path before any restart.

## Upgrade and Rollback

Drain traffic and stop the component cleanly before changing the runtime or
YAML. Validate JAR/native compatibility and the YAML schema in staging first.

A provider switch is not a data rollback. If the upgraded Topling runtime has
written data and rollback is required, stop all writers and restore the full
pre-upgrade snapshot into an empty data directory with the matching previous
runtime.
