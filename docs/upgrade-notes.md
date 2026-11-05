# Storage and scan upgrade notes

These notes describe development storage changes. A published image can lag the
source revision; check that the deployed build contains the corresponding fixes.

## Required shard scan bounds

`traversers/vertices/scan` and `traversers/edges/scan` now reject a missing
`start` or `end` with HTTP 400 on **all backends**. Previously RocksDB could
throw an NPE and return 500, while HStore and in-memory implementations could
interpret omitted values as zero and run an incomplete scan. Both parameters
are now mandatory even when that older behavior appeared to succeed.

For an export, obtain all ranges from the matching `vertices/shards` or
`edges/shards` endpoint. Keep each range's exact boundary strings and paginate
it independently with an explicit initial `page=`. Only RocksDB and HBase
interpret empty bounds as an open range. Do not use `start=&end=` for a
whole-graph HStore or in-memory export. See [REST usage](rest-api-usage.md#export-a-shard-with-bounded-pages).

## Topling native profiles

The new default `conf/toplingdb.yaml` supports the tested community JNI without
CSPP plugins. It changes native formats and tuning from the previous default:

| Setting | Previous CSPP profile | New community profile |
| --- | --- | --- |
| Memtable/table factories | CSPP with DispatcherTable and CSPPMemTabTable readers | SkipList and BlockBasedTable |
| Block cache | 8 GiB | 64 MiB |
| Write buffer / maximum buffers | 128 MiB / 6 | 16 MiB / 3 |
| Maximum background jobs | 8 | 2 |

Other prior compaction tuning is also absent from the smaller community profile.
This is a configuration and performance change, not a transparent optimization
or an existing-data format conversion. With the tested community JNI, native
EasyMigrate options override corresponding Java options when opening a database.
Inspect effective OPTIONS and cache capacity when using a different JNI build.

For an existing Topling installation, stop the component and retain its previous
JNI, configuration and a recoverable data backup. The Server, PD and Store
distributions also ship `conf/toplingdb-legacy-cspp.yaml`, preserving the old
profile unchanged. Select that profile, or the deployment's previous tuned
profile, through an absolute `TOPLINGDB_EASY_MIGRATE_CONF` path during preparation
and startup. Keep the native library that provides the required CSPP readers.
The community JNI used for validation lacks those readers and cannot validate
old CSPP SST data; such an upgrade needs a matching JNI and old-format dataset
before deployment. Do not replace its options with the new community profile.

The prepare script verifies JNI capabilities with a disposable database and
imports the selected profile before publication. This import checks plugin
availability; it does not read existing business SSTs or certify their
compatibility. Relative profile paths are normalized before a launcher changes
its working directory; absolute service configuration is recommended.
