# HStore GraphID checks before upgrading from 1.7

Some 1.7 batch PUT/MERGE paths wrote business keys with the reserved GraphID
`0xFFFE` when no mapping existed. Current Store allocates a real GraphID for
these operations. The allocation can also happen during Raft replay, before a
client sends a request. Existing `0xFFFE` rows then become invisible to reads
through the new mapping. Replacing the Store JAR with 1.7 does not undo a mapping
that has already been persisted.

This is an HStore upgrade issue. A 1.7 database using only valid allocated
GraphIDs does not need its keys converted for this issue.

## Check each Store before the first 1.8 startup

Keep a complete, matching backup of the 1.7 PD, every Store (data **and** Raft
paths), Server configuration, and the original input where available. Quiesce
all writers, take snapshots with 1.7, and stop the Stores before copying their
files. A stopped process alone does not prove that WAL-disabled writes were
flushed; retain the Raft logs together with the data and snapshots.

From the 1.8 Store distribution, using that Store's effective local configuration:

```bash
bin/hugegraph-store-check.sh --check
```

An explicit configuration directory and multiple data roots are supported:

```bash
bin/hugegraph-store-check.sh --check \
  --config /srv/hugegraph-store/conf \
  --data-path /srv/store-data-a,/srv/store-data-b
```

Run the command with Java 17, selected through `JAVA_HOME` or `java` on `PATH`.
It uses the RocksDB dependencies already in the Store distribution, needs no
running PD, Store or Server, and creates no database. It reads `application.yml`
followed by `application-pd.yml` when present. It does not start Spring or resolve
its environment, JSON or profile overrides. If the deployment uses those
overrides, pass the actual data roots with `--data-path` and point `--config` at
local configuration files containing the resolved effective RocksDB settings.
For example, a Docker deployment's `HG_STORE_DATA_PATH` must be supplied as
`--data-path`; the command does not read the entrypoint's `SPRING_APPLICATION_JSON`.
Relative paths are resolved from the Store distribution, as with its startup scripts.
The checker uses the same verified runtime selector as the Store launcher. For a
Topling deployment, prepare its runtime first, set
`TOPLINGDB_ROCKSDB_PROVIDER=topling`, and retain `rocksdb.provider=topling` in the
effective configuration. Its selected JNI JAR takes precedence over the bundled
standard RocksDB classes; missing or unverified runtime assets fail the check.
Do not point the command at a running Store: it refuses locked active databases,
and a separate read-only database cannot observe another process's memtables.

If a complete checkpoint-derived database has `CURRENT` but no `LOCK`, first stop
the Store and disable automatic restart, preserve a complete backup, and confirm
no other process can open the data directory. The offline check intentionally
refuses this situation without writing to it. An operator can create an empty
`LOCK` file in that verified database directory and repeat the check while the
Store remains stopped. Do not use this procedure to repair a missing `CURRENT`
or any other incomplete database directory.

| Result | Meaning | Next step |
| --- | --- | --- |
| Exit 0 with inspected data | No reserved-prefix rows or invalid GraphID mappings were found in the persisted databases and latest snapshots inspected | Keep the backups and follow the normal release upgrade procedure; this check does not certify the entire upgrade |
| Exit 0 with zero inspected databases and snapshots | The supplied roots may be empty or incorrect | Verify the data paths and repeat the check; no business databases or snapshots were inspected |
| Exit 2 with `Legacy GraphID 0xFFFE` | The reported database/column family contains incompatible historical rows | Use the recovery path below before starting 1.8 on this data |
| Exit 2 with an invalid mapping, lock, configuration or I/O error | Inspection could not establish a clean persisted state | Resolve the reported problem and run the check again; do not treat this as a clean result |

A launcher or JVM failure can return another nonzero status. Treat every nonzero
status as an unsuccessful check.

Run the check on **every Store**. Users do not need to supply numeric GraphIDs or
partition IDs. The check visits configured data roots, retained DB versions and
the latest local snapshot for each partition. It also considers stored locations
that are not remapped from the configured roots. It seeks the reserved prefix
once per column family and reads GraphID metadata; it does not scan every
business row. Retained versions are inspected without invoking the normal
session opener, which can delete older versions. If a metadata-recorded partition
has no matching database under either the configured roots or its stored location,
the offline command refuses to certify the inspection. The location may be stale
after migration; verify the Store paths, backups and partition ownership before
proceeding. Normal startup retains its existing stale-location cleanup behavior.

Store startup performs the same GraphID scans before the Store engine,
RocksDB factory, Raft groups and Store registration initialize. Unlike the offline
command, it does not reject a missing metadata-recorded partition location because
normal startup removes stale locations for partitions that have migrated away. On failure the
process exits unsuccessfully instead of leaving a partially initialized Store
advertising healthy partitions. The earlier configuration listener can still
contact PD or synchronize local configuration. The guarantee here concerns the
Store databases and registration, not all configuration activity. For a checkpoint
without a RocksDB `LOCK` file, startup creates an empty `LOCK` before inspecting it;
the offline command remains read-only and refuses a database with no lock file.

Incoming snapshots are checked before opening or replacing the local partition
database. A rejected snapshot does not advance the state machine's committed
index. Local snapshots are also checked at startup: a `should_not_load` marker
only skips local installation and does not stop a checkpoint reaching another
replica.

### Incomplete database directories

`Incomplete RocksDB directory: <path>` means the reported directory has no
regular `CURRENT` file. This is distinct from finding a reserved GraphID row.
For example, interruption while the normal opener removes an old `db/00000`
can leave that directory incomplete beside a valid `db/00000_42`. The read-only
checker cannot certify the leftover directory and does not delete or skip it.

An existing `hgstore-metadata` path must also have a regular `CURRENT` file.
This database records partition locations, so an incomplete metadata directory
or a dangling link cannot be treated as a new Store. Preserve it and recover a
complete matching backup; do not move it aside to make the check pass. A new
Store with no metadata path is still allowed.

Keep the Store stopped and preserve a complete backup before changing any
directory. Check the exact reported path against the partition's stored location,
the previous Store logs and all sibling versions. A larger numeric suffix alone
does not establish that another directory is the correct live database.

If an operator has independently confirmed that the reported path is only an
obsolete, partly deleted version and has verified the selected database, move
that one leftover directory to a backed-up quarantine location outside **every**
configured Store data and Raft root. Keep its files; renaming it within a data
root is not a recovery check. Run the offline command again on every Store with
the effective configuration and data roots before attempting startup.

Do not use that procedure for a selected/latest database, a current snapshot or
a directory whose role is uncertain. Do not fabricate a `CURRENT` file. Preserve
the evidence and recover a complete matching backup or follow the independently
verified logical recovery route below. This manual procedure does not make the
checker automatically distinguish harmless cleanup remnants from damaged data.

## Choose the recovery source first

Do not infer that every `0xFFFE` row belongs to the graph named in the Server
configuration. Different graphs, including the automatically created
`DEFAULT/~sys_graph/g`, could share the same physical namespace. System variables
can occupy vertex and index tables too. Reassigning the entire prefix to one
graph can silently assign data to the wrong owner.

| Historical state | Supported action |
| --- | --- |
| Valid GraphID mappings and no incompatible rows | No GraphID-specific data conversion |
| A trusted 1.7 source still returns all expected application data | Validate a logical export and restore it into a **new, empty** 1.8 cluster |
| New and old prefixes coexist, including after replay, writes or truncate | Preserve the affected cluster. Recover a matching trusted pre-upgrade source or original input first; do not union the prefixes automatically |
| Several graphs shared the prefix | Validate each graph's ownership and complete application data against independent input/backups before exporting. Refuse an automatic reassignment when ownership is uncertain |
| A physical key was overwritten or deleted | No mapping or prefix conversion can recover the lost value without another copy |

An empty new prefix, or equal values at currently colliding keys, does not prove
that combining prefixes is safe: an earlier delete or truncate can make a merge
resurrect data the application already removed. A disk inspection cannot prove
historical ownership or the absence of those operations.

If 1.8 has already opened the data, do not use JAR rollback as a recovery
procedure. Bring up a **copy** of the matching 1.7 backup with its original
configuration in an isolated recovery environment. Verify the source through
fresh requests, not a Server cache retained across the upgrade. Keep the
original backup immutable.

## Logical migration with existing HugeGraph Tools

For a validated, readable application graph, the existing Tools `backup` and
`restore` commands avoid editing RocksDB keys, allocator metadata, Raft logs or
snapshots. Restore uses the normal Server/Store API and Raft replication. The
target cluster creates its own mappings, replicas and snapshots. Use independent
PD and Store data paths and endpoints for the empty target; never attach an empty
target to the source PD or reuse the source Store identity.

The example below is for a graph in the **DEFAULT GraphSpace**, without special
state outside the export scope described below. Use a HugeGraph Tools
release compatible with both source and target.
Keep application writers stopped until export and validation finish. Keep target
application traffic disabled until all checks pass.

Use new, empty output and log directories for **each attempt**. Reusing a backup
directory can overwrite an earlier export and retain stale shard files from it.
`restore --clean` deletes the local backup; it is deliberately absent below.

```bash
# Run from the HugeGraph Tools distribution. Set these for the isolated clusters.
SOURCE_URL=http://source-server:8080
TARGET_URL=http://target-server:8080
GRAPH=hugegraph
BACKUP=/srv/upgrade/backup-attempt-1
BACKUP_LOG=/srv/upgrade/backup-log-attempt-1
RESTORE_LOG=/srv/upgrade/restore-log-attempt-1
VERIFY_BACKUP=/srv/upgrade/verify-attempt-1
VERIFY_LOG=/srv/upgrade/verify-log-attempt-1

bin/hugegraph --url "$SOURCE_URL" --graph "$GRAPH" backup \
  --directory "$BACKUP" --log "$BACKUP_LOG" --huge-types all \
  --format json --compress false --thread-num 1

# Verify the source export and failure logs before continuing (see below).
bin/hugegraph --url "$TARGET_URL" --graph "$GRAPH" graph-mode-set \
  --graph-mode RESTORING

bin/hugegraph --url "$TARGET_URL" --graph "$GRAPH" restore \
  --directory "$BACKUP" --log "$RESTORE_LOG" --huge-types all --thread-num 1

bin/hugegraph --url "$TARGET_URL" --graph "$GRAPH" backup \
  --directory "$VERIFY_BACKUP" --log "$VERIFY_LOG" --huge-types all \
  --format json --compress false --thread-num 1

# Only after complete comparison, query checks and restart verification:
bin/hugegraph --url "$TARGET_URL" --graph "$GRAPH" graph-mode-set \
  --graph-mode NONE
```

Adjust the worker count for the deployment after establishing correctness.
The example uses a single worker to make a small validation run
straightforward; it is not a throughput recommendation.

### Validate completeness before cutover

A zero exit status is necessary but insufficient. The tested Tools version can
log a failed backup shard and still exit successfully. Check its separate
vertices/edges `failed_shards`, `timeout_shards` and `limit_exceed_shards` files;
any nonempty such file means the export is incomplete. Preserve the logs and
start a new attempt in new directories after resolving the error.

Compare the source export with independent expected data, then compare source
and target exports. Include all four schema types and their IDs; vertex IDs,
labels and properties; edge IDs, labels, endpoints, direction and properties;
record counts and ID uniqueness. Ignore transport-only schema controls and
normalize unordered sets, but preserve ordered list properties. `RESTORING`
retains schema IDs; the tool can recompute primary-key vertex IDs from the label
ID and key properties, so verify IDs explicitly.

Run application queries that exercise indexes, adjacency, filters and graph
isolation. Repeat the read checks after a cold target restart and verify a new
write. On replicated deployments, verify failover and snapshot installation on a
recovering replica before accepting the target. Counts alone are not an adequate
migration oracle.

If restore fails or is interrupted, leave target application traffic disabled.
For this upgrade, use another empty target or recreate only the disposable target
from its initial empty state and rerun the complete restore from the retained,
verified export. Do not merge an uncertain partial restore into the source. A
failed command can leave the target in `RESTORING`; inspect its mode explicitly.
Keep source backups through the acceptance period. Once applications write to the
target, returning to the source requires reconciling those writes; it is no
longer a file or JAR rollback.

### Export scope and release limits

Tools exports property keys, vertex labels, edge labels, index labels, vertices
and edges. This is not a full Store/PD/system-state backup. Authentication data
has separate Tools commands. Graph variables, system KV, task state, OLAP state
and operational configuration need a separate inventory and recovery procedure.
Do not restore the shared historical system namespace by assigning it to the
user graph.

The inspected Tools CLI addresses DEFAULT GraphSpace and has no `--graphspace`
option. Other GraphSpaces require an explicitly GraphSpace-aware API/client
export and a separately verified restore. Do not substitute a graphspace-prefixed
graph name in these commands. TTL expiration timestamps are another boundary:
restoring a TTL label without `ttl_start_time` can restart its lifetime. A plain
logical export must not be presented as preserving those original timestamps.

Deployments needing these features must validate their additional recovery path
before upgrading. Shared-prefix ownership that cannot be established and lost
rows without backups remain recovery blockers. The preflight intentionally has
no override that silently assigns ambiguous data.

## Background and compatibility

The legacy allocation issue is documented in
[apache/hugegraph#3224](https://github.com/apache/hugegraph/issues/3224)
and [#3095](https://github.com/apache/hugegraph/issues/3095).

The issue author's [reference data](https://github.com/SebastianGruza/hugegraph-validation)
is available with the original report.

This fix adds no second read/write format, GraphID allocator rule, Raft command
or recurring scan to normal requests. It does not establish general rolling
upgrade/rollback compatibility for the release; see
[apache/hugegraph#3223](https://github.com/apache/hugegraph/issues/3223).
