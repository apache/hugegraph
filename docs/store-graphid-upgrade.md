# Store GraphID upgrade

Some 1.7 Store batch writes use the reserved graph prefix `FFFE` when the graph
has no persisted ID mapping. A newer Store allocates a real graph ID but does not
move those rows. Reads can then miss old data, even if the allocation was caused
by Raft replay rather than a client write. This does not affect every 1.7 database.

For affected data, use a complete, trusted 1.7 source and the existing
HugeGraph-Tools migration command to populate a fresh 1.8 cluster. This leaves the
source intact and writes target data through the normal GraphID and Raft paths.
It requires a write freeze and temporary target capacity, but adds no legacy
read/write mode or physical-key conversion to normal operation.

## What the preflight checks

A Store build containing this change checks local partition DBs before registering
the Store, starting heartbeats or starting Raft. It inspects all retained
valid DB versions under the configured data directories, using the effective
Store column-family options. Each graph-data column family is checked by an
ordered seek to `FFFE`; metadata-only column families are excluded.

Affected rows or a database that cannot be safely inspected cause startup to
fail. The error identifies the startup stage, partition and path, and the column
family when a legacy row is found. The check does not create, prune or rewrite
partition DBs. Nonempty partition DB directories without `CURRENT` are rejected,
as are incomplete selected versions that would hide an older valid version.
This startup does not register the Store as `Up`;
a previous registration can remain visible in PD until its heartbeat expires.

The shared check also protects missing-ID allocation, cold partition opens,
Raft initialization and staged snapshot activation. Cached primary sessions are
checked directly so unflushed writes made with WAL disabled are visible. A
GraphID preflight failure during later local recovery requests a nonzero process
exit instead of leaving a partially restored Store running.

A rejected snapshot retains the active DB and committed index, the incoming
snapshot and its staged copy. The preflight preserves inspected data; application
configuration loading can already have contacted PD, and later bootstrap paths
can have changed Store/Raft location metadata. The persisted ID format is
unchanged, and the check never infers row ownership or
performs a migration.

### Incomplete database directories

`Nonempty database path <path> has no CURRENT` identifies a directory that cannot
be safely opened. It does not by itself establish that reserved GraphID rows
exist. For example, interruption while the normal opener removes an older
`db/00000_1` may leave it incomplete beside a valid `db/00000_2`. The read-only
preflight preserves and rejects that leftover rather than deleting or skipping it.

Keep the Store stopped and preserve its complete matching data, metadata and
Raft backup. Check the exact reported path against the partition's stored
location, previous Store logs and all sibling versions. A larger numeric suffix
alone does not establish that another directory is the correct live database.

Only after independently confirming that the reported directory is an obsolete,
partly deleted version and verifying the selected database, move that one
leftover to a backed-up quarantine outside **every** configured Store data and
Raft root. Keep its files. Renaming it inside a data root is not a recovery check.
Retry the guarded startup only after that check, with writes still stopped.

Do not apply this procedure to a selected/latest database, a current snapshot or
an uncertain directory. Do not fabricate a `CURRENT` file. Preserve the evidence
and recover a complete matching backup or use the verified logical recovery
workflow below. This manual recovery step does not make the guard automatically
distinguish harmless cleanup remnants from damaged data or certify the upgrade.

## Upgrade workflow

Keep an upgrade backup of the stopped cluster's data, metadata and Raft state.
If the new Store starts without a GraphID refusal, this particular problem does
not require a migration. Complete the usual upgrade checks before resuming writes.

If startup reports `FFFE`, stop the upgrade and preserve the original DB versions,
WAL, metadata and snapshots. Restore an isolated 1.7 source from the pre-upgrade
backup when needed. Use the source only after confirming that its visible data is
complete and that the affected rows belong to the intended graph. A current
single-graph configuration alone does not establish historical ownership.

For a graph in the `DEFAULT` graph space, migrate to an empty graph in a fresh
1.8 target. Use a HugeGraph-Tools build that stops migration when exporting any
data page fails, before target restore begins:

```bash
bin/hugegraph --url http://old-1.7:8080 --graph hugegraph \
  migrate --target-url http://new-1.8:8080 --target-graph hugegraph \
  --graph-mode RESTORING --directory /safe-backup/hugegraph \
  --huge-types all --keep-local-data
```

Keep application writes stopped throughout export, restore and validation;
the export does not provide a snapshot across all requests.
`RESTORING` preserves schema IDs on the empty target; `--keep-local-data` retains
the logical backup. The current Tools command selects the default graph space;
do not assume this example migrates another graph space.

Do not switch traffic if export or restore reports an error. Preserve the backup
and retry against a fresh empty target; a failed restore can leave partial data
and the target in `RESTORING` mode. A successful command or matching counts alone
is insufficient: compare vertex and edge IDs, endpoints,
properties, schema and index queries against the known source baseline. Repeat
the application queries after a target snapshot and restart, and verify follower
rebuilds for a replicated deployment. Switch traffic only after these checks pass.
Keep the source and backup until the target is accepted; writes accepted by the
target are not included in the old backup.

## Cases requiring recovery first

If an earlier upgrade already allocated a new ID, the visible source can be
incomplete. Switching its mapping back to `FFFE` can hide newer rows instead.
Use a complete pre-upgrade backup or reconcile the old and new histories before
exporting. Replacing the JAR alone does not undo a persisted mapping.

Different graphs may have shared `FFFE`. Their physical keys do not identify the
original graph, and overwritten or deleted values cannot be reconstructed without
another source. Do not automatically assign all such rows to one graph, merge
old and new prefixes, or edit each replica's RocksDB independently. These cases
need evidence-based recovery before a migration can claim completeness.

This guard provides detection and refusal. It does not certify an entire cluster
upgrade or make affected data safe for an in-place rolling upgrade.
