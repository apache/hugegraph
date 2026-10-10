# Switch RocksDB to ToplingDB

HugeGraph defaults to standard RocksDB. Server, PD and Store can explicitly select a prepared ToplingDB runtime using their normal distributions.
Prepare and configure each component separately; this guide covers the shipped launch scripts, not custom IDE or embedded launchers.

| Component | Business provider setting | Start command |
| --- | --- | --- |
| Server with the RocksDB backend | `rocksdb.provider=topling` in each graph properties file | `bin/start-hugegraph.sh` |
| PD | `rocksdb.provider: topling` in `conf/application.yml` | `bin/start-hugegraph-pd.sh` |
| Store | `rocksdb.provider: topling` in `conf/application-pd.yml` | `bin/start-hugegraph-store.sh` |

For a distributed graph, Server uses `backend=hstore`; PD and Store own its persistent storage. A standalone Server keeps `backend=rocksdb`.
The launcher selection and the business provider must agree. A mismatch fails before opening that component's database.

## Quick start: standalone Server

![Switch RocksDB to ToplingDB](images/switch-rocksdb-to-toplingdb.png)

Use a stopped Server distribution on Linux x86_64 with a Java 17 JDK. If your distribution lacks these scripts, [build it from this source](#build-and-prepare) first.
Prepare the runtime once, select it, then initialize and start the Server.
The graph APIs and normal start/stop commands stay the same; Topling's native options are configured in `conf/toplingdb.yaml`.
Use new, empty, service-writable data and WAL directories outside the distribution for this example.

**1. Prepare the runtime.** Supply your ToplingDB JNI JAR and its trusted SHA-256:

```bash
export TOPLING_JNI_JAR=/absolute/path/to/rocksdbjni-topling.jar
export TOPLING_JNI_SHA256='<trusted-64-character-sha256>'
bash bin/prepare-topling.sh
```

**2. Select Topling.** Edit `conf/graphs/hugegraph.properties` and every other graph loaded from the configured graphs directory:

```properties
backend=rocksdb
rocksdb.provider=topling
rocksdb.data_path=/srv/hugegraph/topling/server/data
rocksdb.wal_path=/srv/hugegraph/topling/server/wal
```

Create those external directories with permissions for the service account. The paths above are examples; choose persistent locations for your deployment.

**3. Initialize and start.** From the same shell, select the matching JNI runtime and initialize the new store once:

```bash
export TOPLINGDB_ROCKSDB_PROVIDER=topling
bash bin/init-store.sh
bash bin/start-hugegraph.sh
```

Keep `TOPLINGDB_ROCKSDB_PROVIDER=topling` in the service environment for subsequent starts; initialization is only needed for a new store.
For PD and Store, prepare each component separately, then use its YAML provider setting and start command from the table above.
A Server connected to distributed storage keeps `backend=hstore`; PD and Store select their own storage runtime.

## Build and prepare

Build the normal distributions from the repository root using Java 17 and Maven 3.6.3 or later:

```bash
mvn clean package -Dmaven.test.skip=true -Dmaven.javadoc.skip=true
```

Obtain a trusted ToplingDB JNI JAR and its SHA-256 through your approved artifact channel. The runtime is not downloaded by the launcher. It must implement the `TOPLINGDB_EASY_MIGRATE_CONF` configuration entry point.
Preparation requires a Java 17 JDK, Linux x86_64, `sha256sum`, `unzip`, `od`, `ldd` and GNU `mv`. The currently exercised JNI requires glibc 2.38 or newer and libaio.
Native dependency requirements depend on the selected JAR; preparation rejects unresolved dependencies.

In each stopped, unpacked component directory:

```bash
export TOPLING_JNI_JAR=/absolute/path/to/rocksdbjni-topling.jar
export TOPLING_JNI_SHA256='<trusted-64-character-sha256>'
bash bin/prepare-topling.sh
```

The script verifies the copied JAR's hash, the Topling Java marker, one Linux x86_64 JNI entry, the ELF architecture and native dependencies.
Before installation, it opens and closes a disposable database with the selected JNI and checks that a probe configuration changes the persisted write-buffer size.
A marker or matching hash alone is insufficient: incompatible builds, including the older `frocksdbjni-8.10.2-topling-1.0.jar` without EasyMigrate support, are rejected.
The probe validates the configuration entry point, not existing-data compatibility or every setting in your production profile.
Its checksum receipt binds the probe to the installed JAR and native library. Launchers reject missing or stale receipts; prepare a fresh distribution when upgrading from an older preparation script.
It installs `topling/rocksdbjni.jar`, its matching native library and optional web resources outside the standard `lib` directory.
On Ubuntu with libaio's t64 name, the compatibility symlink stays inside that component's runtime directory.
The published runtime directories use 0755 and regular files use 0644, so a different service account can read the prepared JNI.
That account also needs traversal permission on the component's parent directories; preparation does not change those parents.

Preparation refuses an existing `topling` directory. To replace a runtime, stop the service and prepare a fresh distribution; it never changes database files.

## Select and start

Set the provider in the component's business configuration. For PD and Store, edit the existing `rocksdb` mapping, retaining its other options:

```yaml
rocksdb:
  provider: topling
```

For a standalone Server, edit every selected graph configuration:

```properties
backend=rocksdb
rocksdb.provider=topling
```

Use separate, initially empty data directories for validation. This integration does not convert or certify an existing database for a different provider.

For persistent use, create service-writable directories outside source checkouts, build outputs and unpacked distributions, and configure all applicable paths:

| Component | Configuration file | Setting | Example external path |
| --- | --- | --- | --- |
| Server data | Graph properties | `rocksdb.data_path` | `/srv/hugegraph/topling/server/data` |
| Server WAL | Graph properties | `rocksdb.wal_path` | `/srv/hugegraph/topling/server/wal` |
| PD metadata | `conf/application.yml` | `pd.data-path` | `/srv/hugegraph/topling/pd` |
| Store data | `conf/application.yml` | `app.data-path` | `/srv/hugegraph/topling/store/data` |
| Store Raft | `conf/application.yml` | `app.raft-path` | `/srv/hugegraph/topling/store/raft` |

Set both Server data and WAL paths. Retaining the default distribution-relative WAL path can lose that directory when rebuilding or replacing the distribution.
Preserve the external data, WAL and Raft roots across runtime updates.
Start each component with explicit runtime selection:

```bash
export TOPLINGDB_ROCKSDB_PROVIDER=topling
# Run the start command for this component from the table above.
```

The component uses its own `conf/toplingdb.yaml`. An explicit `TOPLINGDB_EASY_MIGRATE_CONF=/absolute/path/config.yaml` overrides that native configuration.
The shipped profile disables the native HTTP server and keeps `memtable_as_log_index=false`, which is required by the Java write path.
Tune the native profile for your machine before production use; it does not replace the component's business configuration.

Server's `init-store.sh` and `dump-store.sh` use the same explicit runtime selection. PD and Store put the prepared JNI JAR before their Spring Boot dependencies.
Standard launch remains the default and does not require Topling files or preparation tools.
Launch rejects inherited recognizable RocksDB JNI preloads in default/standard mode and competing JNI in Topling mode.
Unrelated allocator or tracing preloads remain intact; switching from a previously selected Topling runtime removes only that runtime. Store adds verified jemalloc without replacing or duplicating caller preloads; the selected JNI stays first.
Do not preload a second RocksDB JNI alongside the selected runtime.

## Validate and stop

Before adopting a JAR, validate each component's real writes, reads, normal stop and restart using a dedicated database directory.
Confirm both the Java classes and the loaded JNI library come from the prepared runtime; a standard-JNI unit test or a successful startup alone is insufficient.
For distributed storage, validate PD and Store together, then perform a Server `hstore` graph operation.

At request/task completion, explicitly committed writes are preserved; unfinished writes are rolled back and cached backend leases are released.
Shared schema and element caches retain their invalidation listeners until the graph closes.

Stop incoming work and use the component's normal stop script. PD drains its scheduled metadata work, joins Raft and closes the metadata database and native options.
Do not use a forced kill as evidence of normal resource cleanup.

To return to standard RocksDB, stop the component, unset `TOPLINGDB_ROCKSDB_PROVIDER` and set its business provider back to `rocksdb`.
Use the corresponding standard-provider data directory; do not test a provider switch by reusing the other provider's database.

Dedicated Topling distributions, Docker/Compose packaging and broader lifecycle/recovery changes are separate follow-ups.
