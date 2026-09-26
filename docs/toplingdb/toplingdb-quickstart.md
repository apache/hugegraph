# ToplingDB Quickstart

This guide covers a source-built ToplingDB distribution on Linux x86_64. On
macOS, run the published Linux image through Docker Desktop or OrbStack
container mode. Native macOS execution (including Intel) is outside this
ToplingDB support matrix. Use a standard HugeGraph distribution when you want
standard RocksDB.

## Choose the Correct Component

| What you run | Topling package | Provider configuration |
|---|---|---|
| Standalone Server | `apache-hugegraph-server-<version>-topling` | `conf/graphs/hugegraph.properties` |
| PD | `apache-hugegraph-pd-<version>-topling` | `conf/application.yml` |
| Store | `apache-hugegraph-store-<version>-topling` | `conf/application-pd.yml` |
| HStore Server | Standard HStore Server | No local Topling setting |

PD and Store select their providers independently. An HStore-backed Server does
not load a Topling JAR or native library.

Set standalone data with `rocksdb.data_path` in
`conf/graphs/hugegraph.properties`, PD metadata with `pd.data-path` in
`conf/application.yml`, and Store data with `app.data-path` in
`conf/application-pd.yml`.

## Prerequisites

Topling distributions and images currently target Linux x86_64. The published
images use the `linux/amd64` platform, so macOS container mode may need
`--platform linux/amd64`; the native packages required by the bundled library
are installed in the images. The preparation script reports any unresolved
dependency and stops.

## Docker Images

The `topling` image tag names one compatible deployment set:

| Image | Role |
|---|---|
| `hugegraph/hugegraph:topling` | Standalone Server with local ToplingDB |
| `hugegraph/pd:topling` | PD metadata on ToplingDB |
| `hugegraph/store:topling` | Store data on ToplingDB |
| `hugegraph/server:topling` | HStore Server without a local Topling runtime |

Run the standalone image with its provider-specific data volume:

```bash
docker volume create hugegraph-topling-data
docker run -d \
  --pull always \
  --platform linux/amd64 \
  --name hugegraph-topling \
  -p 8080:8080 \
  -v hugegraph-topling-data:/hugegraph-server/topling-data \
  hugegraph/hugegraph:topling

curl --fail http://127.0.0.1:8080/versions
```

The same standalone image can be run with the generic repository Compose file.
Inject the provider and volume parameters so the standard volume cannot be
reused:

```bash
export HUGEGRAPH_SERVER_IMAGE='hugegraph/hugegraph:topling'
export HUGEGRAPH_SERVER_PULL_POLICY='always'
export HG_SERVER_ROCKSDB_PROVIDER='topling'
export HG_SERVER_DATA_PATH='/hugegraph-server/topling-data'
export HG_SERVER_ENFORCE_PROVIDER_MARKER='true'
export HUGEGRAPH_SERVER_VOLUME='server-topling-data'
docker compose -f docker/docker-compose.yml \
  up -d --wait
```

Run a source-checkout 1+1+1 HStore stack with Topling PD and Store images.
The generic Compose files accept the same parameters for local builds or
published images; the HStore Server itself remains standard:

```bash
export HUGEGRAPH_ADMIN_PASSWORD='replace-with-a-strong-password'
export HG_PD_AUTH_SECRET_KEY='replace-with-a-separate-strong-pd-secret'
export HUGEGRAPH_PD_IMAGE='hugegraph/pd:topling'
export HUGEGRAPH_PD_PULL_POLICY='always'
export HUGEGRAPH_PD_VOLUME='pd-topling-data'
export HG_PD_ROCKSDB_PROVIDER='topling'
export HG_PD_DATA_PATH='/hugegraph-pd/topling-pd-data'
export HG_PD_ENFORCE_PROVIDER_MARKER='true'
export HUGEGRAPH_STORE_IMAGE='hugegraph/store:topling'
export HUGEGRAPH_STORE_PULL_POLICY='always'
export HUGEGRAPH_STORE_VOLUME='store-topling-data'
export HG_STORE_ROCKSDB_PROVIDER='topling'
export HG_STORE_DATA_PATH='/hugegraph-store/topling-storage'
export HG_STORE_ENFORCE_PROVIDER_MARKER='true'
export HUGEGRAPH_SERVER_IMAGE='hugegraph/server:topling'
export HUGEGRAPH_SERVER_PULL_POLICY='always'

docker compose \
  -f docker/docker-compose-hstore.yml \
  up -d --wait pd store server
```

For source-built PD and Store images, use `local/hugegraph-{pd,store}:topling`,
set both pull policies to `never`, add `HUGEGRAPH_{PD,STORE}_BUILD_TARGET=topling`,
and append `-f docker/docker-compose.dev.yml`. The generic files mount the
separate `pd-topling-data` and `store-topling-data` volumes. The HStore Server
does not load a local Topling library.

For a published 3+3+3 stack, select all three deployment images explicitly:

```bash
export HUGEGRAPH_ADMIN_PASSWORD='replace-with-a-strong-password'
export HG_PD_AUTH_SECRET_KEY='replace-with-a-separate-strong-pd-secret'
export HUGEGRAPH_PD_IMAGE=hugegraph/pd:topling
export HUGEGRAPH_PD_PULL_POLICY=always
export HG_PD_ROCKSDB_PROVIDER=topling
export HG_PD_DATA_PATH=/hugegraph-pd/topling-pd-data
export HG_PD_ENFORCE_PROVIDER_MARKER=true
export HUGEGRAPH_PD0_VOLUME=hg-pd0-topling-data
export HUGEGRAPH_PD1_VOLUME=hg-pd1-topling-data
export HUGEGRAPH_PD2_VOLUME=hg-pd2-topling-data
export HUGEGRAPH_STORE_IMAGE=hugegraph/store:topling
export HUGEGRAPH_STORE_PULL_POLICY=always
export HG_STORE_ROCKSDB_PROVIDER=topling
export HG_STORE_DATA_PATH=/hugegraph-store/topling-storage
export HG_STORE_ENFORCE_PROVIDER_MARKER=true
export HUGEGRAPH_STORE0_VOLUME=hg-store0-topling-data
export HUGEGRAPH_STORE1_VOLUME=hg-store1-topling-data
export HUGEGRAPH_STORE2_VOLUME=hg-store2-topling-data
export HUGEGRAPH_SERVER_IMAGE=hugegraph/server:topling
export HUGEGRAPH_SERVER_PULL_POLICY=always

docker compose -f docker/docker-compose-3pd-3store-3server.yml \
  up -d --wait
```

Each PD and Store instance receives a distinct Topling volume. For local Bake
images, use the same variables with the tags produced by `docker/bake.hcl`.
No Topling-specific Compose file is required or maintained.

The `topling` tag is mutable. The commands above force a pull so a cached image
cannot silently stand in for the current deployment set. Pin the registry
digest when an exact build must be reproduced, and verify the image source,
revision, and runtime labels during acceptance.

## Build Distributions from Source

Build on Linux x86_64 with Java 11+, Maven 3.5+, `rsync`, `unzip`, and `tar`.

```bash
git clone https://github.com/hugegraph/hugegraph.git
cd hugegraph
git switch toplingdb

VERSION=$(mvn help:evaluate \
  -Dexpression=project.version -q -DforceStdout)

mvn clean package \
  -pl hugegraph-server/hugegraph-dist,hugegraph-pd/hg-pd-dist,hugegraph-store/hg-store-dist \
  -am -Dmaven.test.skip=true -Dmaven.javadoc.skip=true -ntp
```

The build creates standard distributions first. Generate only the Topling
distributions you need:

```bash
install-dist/scripts/build-topling-distribution.sh server "$VERSION"
install-dist/scripts/build-topling-distribution.sh pd "$VERSION"
install-dist/scripts/build-topling-distribution.sh store "$VERSION"
```

Each command creates a directory and a matching `.tar.gz` file beside the
standard distribution. The Topling package contains:

```text
bin/prepare-topling.sh
bin/preload-topling.sh
lib/topling/rocksdbjni*.jar
lib/topling/runtime.properties
library/librocksdbjni-linux64.so
```

The generator selects `provider=topling` and prepares the native runtime. Run
`bin/prepare-topling.sh` again after replacing the bundled Topling JAR.

## Provider Data Markers

Docker entrypoints validate `.hugegraph-rocksdb-provider` before starting a
JVM. The marker records the component and provider. A mismatched marker always
stops startup. Topling also rejects an unmarked, non-empty data directory.
The configured data root must already exist as a real directory; mount or
create it before startup. Symlinked path components are rejected.

Standard RocksDB accepts an existing unmarked directory for backward
compatibility. New deployments should still use the image defaults:

| Component | Standard data root | Topling data root |
|---|---|---|
| Server | `/hugegraph-server/rocksdb-data` | `/hugegraph-server/topling-data` |
| PD | `/hugegraph-pd/pd_data` | `/hugegraph-pd/topling-pd-data` |
| Store | `/hugegraph-store/storage` | `/hugegraph-store/topling-storage` |

The marker prevents accidental reuse. It does not convert data between
providers.

## Standalone Server

Enter the generated Server distribution:

```bash
cd "hugegraph-server/apache-hugegraph-server-$VERSION-topling"
```

Confirm the provider and assign a data directory that no standard RocksDB
process uses:

```properties
# conf/graphs/hugegraph.properties
backend=rocksdb
rocksdb.provider=topling
rocksdb.data_path=/srv/hugegraph/topling/server
```

Initialize and start the Server:

```bash
bin/init-store.sh
bin/start-hugegraph.sh
curl --fail http://127.0.0.1:8080/versions
```

The startup output must include the selected Easy Migrate configuration:

```text
[preload-topling] TOPLINGDB_EASY_MIGRATE_CONF=.../conf/toplingdb.yaml
```

Stop with the normal HugeGraph script:

```bash
bin/stop-hugegraph.sh
```

Restart the same Topling distribution and read previously written data before
you accept persistence for that deployment.

## Distributed HStore

Build one Topling distribution for PD and one for Store. Keep the HStore Server
on its normal HStore distribution.

In every PD distribution, enable ToplingDB and use a provider-specific metadata
directory:

```yaml
# conf/application.yml
rocksdb:
  provider: topling
  option-path: ./conf/rocksdb_pd.yaml

pd:
  data-path: /srv/hugegraph/topling/pd
```

In every Store distribution, enable ToplingDB:

```yaml
# conf/application-pd.yml
rocksdb:
  provider: topling
```

Set a provider-specific Store data directory in the Store service
configuration:

```yaml
# conf/application-pd.yml
app:
  data-path: /srv/hugegraph/topling/store
```

Configure the normal PD, Store, and HStore network addresses as described in
the [distributed deployment guide](../../hugegraph-store/docs/deployment-guide.md).
Start PD before Store, then start the HStore-backed Server:

```bash
# Run from the corresponding distribution directory
bin/start-hugegraph-pd.sh
bin/start-hugegraph-store.sh
bin/start-hugegraph.sh
```

Check each PD and Store log for its own Easy Migrate path. PD must use
`conf/rocksdb_pd.yaml`; Store must use `conf/rocksdb_store.yaml`. The HStore
Server must not report a local Topling runtime.

## Topling HTTP Monitor

The sample Easy Migrate files bind their HTTP monitors to loopback, but set
`auto_start_http: false`, so the monitor is disabled by default:

| Component | Configuration | Default address |
|---|---|---|
| Server | `conf/toplingdb.yaml` | `127.0.0.1:2011` |
| PD | `conf/rocksdb_pd.yaml` | `127.0.0.1:2012` |
| Store | `conf/rocksdb_store.yaml` | `127.0.0.1:2013` |

The endpoint has no authentication. Keep the loopback binding. Enable it only
when you need it:

```yaml
http:
  auto_start_http: true
```

Leave `auto_start_http: false` to keep the monitor disabled.

## Return to Standard RocksDB

A provider change is not a data conversion. Use this procedure:

1. Stop all writers and stop the component cleanly.
2. Keep the Topling data directory unchanged.
3. Restore a full pre-Topling snapshot into a new, empty standard RocksDB data
   directory.
4. Start the standard distribution with `provider=rocksdb`.
5. Validate schema, reads, writes, restart, and persistence before serving
   traffic.

Do not point standard RocksDB at a directory that ToplingDB has modified.

## Startup Failures

Startup stops when the provider is invalid or the component-local JAR, native
library, Easy Migrate file, or system dependency is missing. Follow the exact
error and see the [troubleshooting guide](toplingdb-troubleshooting.md).
