# ToplingDB Developer Guide

This guide describes the Topling integration maintained in this repository.
Read the [quickstart](toplingdb-quickstart.md) first if you only need to run it.

The runtime support target is native Linux x86_64 and Linux containers on
macOS (Docker Desktop or OrbStack). Native macOS execution, including Intel,
is deliberately outside the ToplingDB matrix; do not treat that CI job as a
Topling image or container failure.

## Scope and Design Priorities

The integration focuses on Topling-specific adaptation, bug fixes, feature
compatibility, usability, and maintainable engine selection. Keep HugeGraph
on the standard RocksDB API with Easy Migrate owning native integration;
do not add Java-side SidePluginRepo management or a parallel storage API.
Reuse shared launch helpers and deployment topologies instead of maintaining
separate copies per provider.

Track provider-independent Server, PD, Store, schema/cache, client reconnect,
and deployment defects in their own issues or PRs. Mark them `ignore` in the
Topling task: this excludes them from its backlog and completion denominator,
not from product ownership, and does not mean they passed. Preserve reproduction
and dependency links. Such defects may limit an individual experiment; record
that limit and continue independent Topling checks rather than making all
pre-existing platform gaps prerequisites for this integration.

Regressions introduced by this PR remain in scope even in shared code.
Unattributed native or WAL failures need a minimal diagnosis before exclusion.
Provider selection, native ownership, runtime packaging, data-directory safety,
and standard RocksDB compatibility remain integration requirements.

Review engine switching for explicit configuration, actionable startup errors,
component-local JNI isolation, consistent defaults and versions, and recoverable
configuration changes. Do not imply live hot switching or automatic conversion
of an existing data directory. Use focused local tests and source/image/JNI-bound
Linux acceptance; performance experiments belong on the Linux test host.

Track native CF lifecycle in [#212](https://github.com/hugegraph/hugegraph/issues/212),
release-grade JNI delivery in [#213](https://github.com/hugegraph/hugegraph/issues/213),
and current coordination in [#240](https://github.com/hugegraph/hugegraph/issues/240).
Issue [#214](https://github.com/hugegraph/hugegraph/issues/214) retains historical
integration and publication milestones.
Keep feature-merge criteria distinct from official-release requirements.
Current configuration-source and multi-graph isolation gaps are tracked in
[#250](https://github.com/hugegraph/hugegraph/issues/250),
[#251](https://github.com/hugegraph/hugegraph/issues/251), and
[#253](https://github.com/hugegraph/hugegraph/issues/253); adapter regression
coverage and diagnostic classification are tracked in
[#254](https://github.com/hugegraph/hugegraph/issues/254) and
[#255](https://github.com/hugegraph/hugegraph/issues/255).

## Runtime Contract

HugeGraph keeps the RocksDB Java API. Topling Easy Migrate supplies the native
runtime and configuration at process startup.

```text
component config
  -> preload-topling.sh validates rocksdb or topling
  -> component-local JAR joins CLASSPATH
  -> component-local native library joins LD_PRELOAD
  -> Easy Migrate YAML is exported
  -> existing RocksDB Java calls open and close DB/CF handles
```

The supported providers are exactly `rocksdb` and `topling`. Duplicate
configuration values must agree. Missing Topling files or dependencies stop
startup. PD and Store never search a neighboring Server distribution.

## Source Layout

| Area | Path |
|---|---|
| Shared helper source | `hugegraph-server/hugegraph-dist/src/assembly/static/bin/common-topling.sh` |
| Package-local preparation | `hugegraph-server/hugegraph-dist/src/assembly/static/bin/prepare-topling.sh` |
| Startup selection | `hugegraph-server/hugegraph-dist/src/assembly/static/bin/preload-topling.sh` |
| Checked-in Topling JAR | `hugegraph-server/hugegraph-dist/src/assembly/static/lib/topling/` |
| Standalone Easy Migrate YAML | `hugegraph-server/hugegraph-dist/src/assembly/static/conf/toplingdb.yaml` |
| PD Easy Migrate YAML | `hugegraph-pd/hg-pd-dist/src/assembly/static/conf/rocksdb_pd.yaml` |
| Store Easy Migrate YAML | `hugegraph-store/hg-store-dist/src/assembly/static/conf/rocksdb_store.yaml` |
| Topling distribution generator | `install-dist/scripts/build-topling-distribution.sh` |
| Provider data marker | `hugegraph-server/hugegraph-dist/src/assembly/static/bin/verify-rocksdb-provider.sh` |
| Docker build graph | `docker/bake.hcl` |
| Focused shell tests | `hugegraph-server/hugegraph-dist/src/assembly/travis/test-topling-*.sh` |

The PD and Store assembly descriptors copy the canonical helper scripts into
their own `bin/` directories. Change the canonical Server copy, then verify all
three assembled copies are identical.

## Build Standard and Topling Distributions

Use Linux x86_64. The distribution generator also requires `rsync`, `unzip`,
`tar`, and `sha256sum`.

```bash
VERSION=$(mvn help:evaluate \
  -Dexpression=project.version -q -DforceStdout)

mvn clean package \
  -pl hugegraph-server/hugegraph-dist,hugegraph-pd/hg-pd-dist,hugegraph-store/hg-store-dist \
  -am -Dmaven.test.skip=true -Dmaven.javadoc.skip=true -ntp

for component in server pd store; do
  install-dist/scripts/build-topling-distribution.sh \
    "$component" "$VERSION"
done
```

The standard distributions contain the selection helpers but no
`lib/topling/rocksdbjni*.jar` or prepared Topling native library. Selecting
Topling from a standard distribution must fail.

When a Topling distribution is switched back to `rocksdb`, the Server and
`init-store` launchers exclude the canonical `lib/topling/` subtree (including
symlink aliases) from their classpaths; the optional Topling JAR is only added
after the startup selector accepts `topling`.

The Topling distributions add only build-time runtime files. The generator
excludes PID files, logs, data directories, and prepared runtime state from the
standard distribution before creating the Topling tarball. It writes
`lib/topling/runtime.properties` with the component, project version, JAR name,
and JAR SHA-256.

That SHA-256 identifies the local input. It is not an official release
signature or a full source-to-binary provenance record. Issue
[#213](https://github.com/hugegraph/hugegraph/issues/213) tracks immutable
coordinates, toolchain and CPU baseline, licensing, SBOM, signatures, and
consumer verification.

## Provider Configuration

Server reads `rocksdb.provider` from graph `.properties` files:

```properties
rocksdb.provider=topling
```

PD and Store read the provider under the root `rocksdb` YAML section:

```yaml
rocksdb:
  provider: topling
```

Keep only one effective value for a component. The loader rejects unknown or
conflicting values. The default is standard RocksDB when no provider is set.

Every Topling functional test must use a readable Easy Migrate YAML. Removing
`TOPLINGDB_EASY_MIGRATE_CONF` reduces a test to Java ABI coverage and does not
exercise the Topling runtime.

## Focused Tests

Run the platform-independent selection and packaging tests first:

```bash
TRAVIS_DIR=hugegraph-server/hugegraph-dist/src/assembly/travis

"$TRAVIS_DIR/test-topling-runtime-selection.sh"
"$TRAVIS_DIR/test-topling-docker-entrypoints.sh"
"$TRAVIS_DIR/test-topling-runtime-packaging.sh" \
  "hugegraph-server/apache-hugegraph-server-$VERSION" \
  "hugegraph-pd/apache-hugegraph-pd-$VERSION" \
  "hugegraph-store/apache-hugegraph-store-$VERSION"
```

On Linux x86_64, validate each generated Topling distribution:

```bash
"$TRAVIS_DIR/test-topling-distribution.sh" \
  server \
  "hugegraph-server/apache-hugegraph-server-$VERSION" \
  "hugegraph-server/apache-hugegraph-server-$VERSION-topling"

"$TRAVIS_DIR/test-rocksdb-runtime.sh" \
  topling \
  "hugegraph-server/apache-hugegraph-server-$VERSION-topling"
```

Repeat the distribution and runtime checks for PD and Store. The CI jobs
`server-rocksdb-runtime` and `distributed-rocksdb-runtime` build a matrix across
standard RocksDB and ToplingDB. Standard runtime checks and both distribution
contracts are required. The synthetic Topling column-family lifecycle check is
reported as a non-blocking diagnostic for issue #212; image-backed service
lifecycle tests remain required. The jobs also contaminate the standard build
with known runtime-state fixtures and verify that the Topling directory and
tarball remain clean.

Run repository checks before pushing:

```bash
mvn editorconfig:check -ntp
mvn clean compile -Dmaven.javadoc.skip=true -ntp
git diff --check
```

Use ShellCheck on each changed shell script. Existing warnings in untouched
lines do not justify unrelated cleanup in a Topling change.

## Docker Images

Server, PD, and Store Dockerfiles have explicit `standard` and `topling`
targets. Build the complete Topling deployment set with Bake:

```bash
RUNTIME_VARIANT=topling \
IMAGE_TAG=topling \
docker buildx bake --file docker/bake.hcl
```

The Topling variant is Linux x86_64 only. It builds
`hugegraph/hugegraph`, `hugegraph/pd`, and `hugegraph/store` from their
`topling` targets. `hugegraph/server` remains the standard HStore Server and
receives the same tag as the compatible deployment set.

An HStore Server image remains free of a local Topling JAR and native library.
PD and Store own their local runtime.

Published candidates carry `org.opencontainers.image.source`,
`org.opencontainers.image.revision`, and
`org.apache.hugegraph.rocksdb-runtime` labels. Verify the exact source commit
before accepting a mutable deployment tag:

```bash
docker image inspect hugegraph/hugegraph:topling \
  --format '{{json .Config.Labels}}'
```

Run the generic standalone, minimal HStore, or 3+3+3 Compose file with
provider parameters injected through the environment. Local development can
use `local/hugegraph-{pd,store}:topling` with
`HUGEGRAPH_{PD,STORE}_BUILD_TARGET=topling`; published candidates use
`hugegraph/{pd,store}:topling` and `HUGEGRAPH_{PD,STORE}_PULL_POLICY=always`.
Set `HUGEGRAPH_SERVER_IMAGE=hugegraph/server:topling` and
`HUGEGRAPH_SERVER_PULL_POLICY=always` only when the matching HStore image is
intended. Keep the provider-specific volume names and data roots explicit.
The repository deliberately maintains one Compose topology per deployment
shape rather than separate Topling files.

Every local RocksDB owner uses a provider-specific data root. The entrypoint
validates `.hugegraph-rocksdb-provider` before it mutates configuration or
starts Java. Topling rejects unmarked non-empty data. Standard RocksDB accepts
legacy unmarked data, but rejects a Topling marker. The helper rejects symlinked
path components and serializes marker initialization on a pinned directory
inode.

## Native Runtime Changes

When replacing `rocksdbjni*.jar`:

1. Keep exactly one Topling JAR in the checked-in source directory.
2. Run `bin/prepare-topling.sh` in every generated component distribution.
3. Check `ldd library/librocksdbjni-linux64.so` for unresolved dependencies.
4. Run real DB and column-family create, close, reopen, restart, and persistence
   tests with the component Easy Migrate YAML enabled.
5. Record the JAR SHA-256 and the exact source and toolchain evidence available.

Issue [#212](https://github.com/hugegraph/hugegraph/issues/212) tracks native
DB/column-family lifecycle behavior. Real Server and 1+1+1 HStore tests must
cover CRUD, clean HugeGraph transaction close, container recreation, and
persistence. The current native runtime can still report a SidePluginRepo
bookkeeping warning after HugeGraph closes successfully. Keep that upstream
evidence separate from the provider-selection and data-isolation gates. ABI
loading or `ldd` output cannot establish service correctness.

## Review Boundaries

A complete Topling change must keep these properties:

- standard artifacts have no Topling JAR or native library;
- Server, PD, and Store load only their component-local runtime;
- an HStore Server does not load Topling locally;
- standard and Topling data directories stay separate;
- invalid or incomplete Topling selection fails before service startup;
- shutdown uses the normal component lifecycle;
- documentation names the exact configuration file and verification command.

Do not claim hot switching, automatic data migration, safe reuse of a
Topling-modified directory by standard RocksDB, or release provenance that the
artifact metadata does not prove.
