# Offline GraphID conversion: design and experimental kernel

Status: **experimental; not a complete cluster upgrade tool; do not activate its output**.
Related: apache/hugegraph#3224 and hugegraph/hugegraph#325. The latter is a
startup/snapshot guard, not the conversion implementation.

## Goal

Permit a stopped HStore cluster to upgrade without exporting all vertices and
edges through Server APIs and rebuilding them through a logical restore. A
physical rewrite still reads and writes data; it is not an O(1) metadata change.
Do not add a permanent legacy read path or guess ownership of shared FFFE keys.

## Alternatives and decision

| Candidate | Benefits | Failure boundary | Decision |
| --- | --- | --- | --- |
| Change only graph-to-ID metadata | Smallest change and no data rewrite | Existing keys retain FFFE; changing the mapping hides them. Assigning FFFE permanently or aliasing multiple owners reintroduces legacy semantics | Reject as the upgrade route |
| Rewrite keys in the existing DB | Avoids a second complete DB copy | A crash between batches leaves mixed prefixes. Requires a durable migration journal and a startup barrier throughout the transition | Keep as a comparison candidate, not the first implementation |
| Rewrite an isolated copy, validate, then switch while stopped | Original state remains available; bounded batches need no in-place rollback protocol | Needs additional disk, ownership proof, and a coordinated Raft/snapshot cutover | Preferred baseline; kernel implemented here |

Only immutable SST files may be hard-linked on the same filesystem. CURRENT,
MANIFEST, OPTIONS and WAL files are copied. The destination `LOCK` is created as
a fresh file: reopening the source `LOCK` while its process-wide POSIX lock is
held can release that lock on some systems. Cross-filesystem staging falls back
to copying SSTs. No file in the original database is opened for writing by the
kernel. Native tests validate the source lock throughout copying, rather than
assuming read-only open alone is sufficient.

## Implemented kernel

`GraphIdMigrationPlan` validates explicit nonoverlapping column-family/logical-key
prefix rules. It deterministically allocates legal target IDs, excluding existing
physical prefixes, graph mappings and allocator slots. A rule is an externally
established ownership assertion, **not proof produced by this algorithm**.

`GraphIdRewriter.plan()` reads a stopped RocksDB under its existing lock,
checks all FFFE records against the rules, and binds the plan to a framed SHA-256
digest of all logical records. `convert()` reacquires the lock, revalidates source
state and allocation before creating output, and writes only to a new staging
DB. It rewrites the first two key bytes; the remaining key and value bytes are
unchanged. Mappings and allocator slots are written in a final batch. The source
is neither deleted nor renamed.

The output is closed and reopened, every expected record is compared, additional
records are detected by total count, metadata/slot values are checked, remaining
FFFE records are rejected, and RocksDB checksums are verified. Interrupted outputs
retain `INCOMPLETE`; retry means making another fresh output from the unchanged
source, not resuming an unqualified partial database. Even a successful result is
labelled `DB_VERIFIED_NOT_CLUSTER_READY`.

The persisted slot layout intentionally preserves GraphIdManager.genCIDSlotKey's
use of `Long.SIZE` bytes including padding. Consolidating that codec with the
existing helper and adding a cross-implementation regression is a merge gate.

This kernel is not wired into startup, snapshot installation, a released CLI,
or automatic directory activation. It cannot yet establish that Raft will not
replay or install older state. Its explicit output label is not a durable
cluster-ready protocol and must not be treated as one.

## Ownership is a separate correctness gate

A single configured user graph does not imply a single historical owner. System
graphs, variables and tasks must be inventoried. Negative/hidden label IDs,
successful deserialization of a sample, or absence of another current mapping do
not by themselves prove ownership. Two graphs may have had identical encodings;
physical overwrites cannot be recovered by a prefix rewrite.

For the actual 1.7 fixture, first inventory all graph namespaces and schema,
including hidden state. Compare complete candidate decoding/encoding and index
references, and ground a partitioning rule in independent input/provenance.
Every affected record must have exactly one supported owner. A fully matching
rule set is necessary but not sufficient: a wrong human-supplied rule can still
be wrong. Do not promote these synthetic test prefixes into a production
heuristic. Prefix rules are intentionally a restricted interface; an arbitrary
schema-aware classifier may require a different ownership representation.

Mixed allocated/legacy prefixes are rejected, including an existing valid mapping
for a migrating graph. A union is unsafe without deletion/truncate history;
equal current values or an empty new prefix do not establish that history.

## Cluster cutover contract (not implemented yet)

1. Stop writers, schema changes, TTL cleanup and background mutation sources.
   Take matched PD, every Store, configuration, data and Raft backups. On the old
   version, finish replay and take successful snapshots at a verified committed
   boundary for each partition. A stopped process alone does not flush WAL-disabled
   writes. Do not discard Raft logs to make a dirty state appear clean.
2. Obtain a common ownership and target-ID plan per partition across all its
   replicas. IDs need not be globally equal across different partitions. Verify
   logical state and committed index/term before converting any candidate.
3. Prefer a **snapshot-aligned/no-unapplied-tail** first release. Verify the log
   boundary mechanically; a tail requires old-version recovery and a fresh
   snapshot, or a separately implemented replay-aware path. An operator saying
   'snapshot completed' is not enough.
4. Convert both the selected DB and the current snapshot in staging. Rebuild
   snapshot file lists/checksums using JRaft's APIs and HugeGraph's checksum
   convention. Preserve index, term, membership and unrelated local metadata.
   Old snapshots must not remain eligible for later local or remote installation.
   Never hand-edit only the leader or replace files underneath running Raft.
5. Validate all prepared replicas against the common plan. Keep all services
   stopped until every required partition/replica is prepared. Directory renames
   are local operations, not an atomic distributed transaction. The activation
   wrapper must fail closed on a partial cutover and preserve matched old roots.
6. Start 1.8, verify reads and new writes, cold restart, leader changes and a real
   remote snapshot to a lagging replica. Rollback before traffic uses the complete
   matched old state; rollback after new writes requires reconciling those writes.

These gates must precede a public upgrade CLI. This PR does not implement them
by deleting logs, resetting indexes, suppressing checks, or allowing FFFE aliases.

## Reproducible evidence

`.github/workflows/graphid-rekey-experiment.yml` compiles the exact head's kernel
with Java 17, RocksDB JNI 8.10.2 and protobuf 3.22.3. It runs the dependency-free
planner probe and the native synthetic DB probe. It retains dependency hashes,
exact commit, logs and small DB fixtures in a seven-day artifact.

The native probe is designed to cover five data column families, two explicitly
known owners, existing legal data, opaque value preservation, ID/slot publication,
close/reopen verification, deterministic fresh-output retry, injected failure
after a durable batch, stale plans, mixed prefixes and a separate-process native
lock. It additionally checks that cold-copying does not release the held source
lock and that interruption at an exact final batch cannot publish a success
certificate. A mapping-only counterexample is also exercised against an actual DB.

These probes are not official 1.7 Server data, actual TTL expiration, Raft replay,
snapshot installation, a throughput benchmark, or a three-Store upgrade test.
Test presence is not test execution: use the actual workflow outcome and retained
logs. The local environment used for the initial planner probe had Java 21 with
`javac --release 17`, no Maven/Docker/RocksDB JNI and unavailable external DNS.

## Remaining acceptance gates

- Run the fixed issue fixture from SebastianGruza/hugegraph-validation at
  52c52778263e4ba81686a86de8eac2cf4b0df7ae with the official 1.7 release, not only
  handcrafted keys. Validate its 16 queries, complete schema/data/index identity,
  hidden state and proven owner separation.
- Implement and fault-test the cluster contract above on one Store and three
  Stores. Cover no-tail/tail boundaries, partial conversion, interrupted activation,
  old/current snapshots, remote snapshot checksums and mixed-version refusal.
- Compare in-place versus isolated conversion on the same representative dataset;
  report elapsed time, actual bytes read/written, peak disk and downtime. The
  baseline choice here is based on failure isolation, not a completed cost benchmark.
- Add a user-facing plan/convert/verify/activate entry point only after qualification;
  preserve original TTL timestamps and state, rather than re-creating graph objects.
- Full reactor build, project formatting, codec compatibility and independent review
  remain required in addition to the isolated native probe.
