# Standalone Server with local RocksDB

Standalone Server needs no PD or Store. The standard distribution ships
`backend=rocksdb` and `serializer=binary`; `pd.peers` is commented out. The
HStore image deliberately replaces this graph configuration. Select the image
or distribution before diagnosing a wait for distributed storage.

| Deployment | Artifact | Default backend |
| --- | --- | --- |
| Single Server container | `hugegraph/hugegraph:<tag>` | Local RocksDB |
| Binary distribution | `apache-hugegraph-server-<version>` | Local RocksDB |
| Distributed Server container | `hugegraph/server:<tag>` | HStore; requires PD and Store |
| Current Helm chart | `helm/hugegraph` | HStore only |

The chart's `values-single.yaml` means one PD, one Store and one Server. It does
not mean a standalone RocksDB Server. Vermeer and SeaTunnel each have their own
standalone topology setting, independent of the Server storage backend.

## One Server container

Pin a tag built from the revision you want to run; `latest` is mutable. A
published image may not yet contain the corresponding source fixes.

```bash
docker volume create hugegraph-rocksdb
docker run -d --name hugegraph-standalone \
  -p 127.0.0.1:8080:8080 \
  -e PASSWORD='replace-with-your-password' \
  -v hugegraph-rocksdb:/hugegraph-server/rocksdb-data \
  hugegraph/hugegraph:latest
docker logs -f hugegraph-standalone
```

The image initializes authentication from `PASSWORD` on the first startup.
Changing this variable does not rotate an existing account's password. The
named data volume retains its contents when the container is removed. The
default data path is `/hugegraph-server/rocksdb-data`; mounting a volume at
`/hugegraph-server/data` does not persist the default RocksDB database.

If you need to reuse the HStore Server image for a new, empty standalone
installation, set both `HG_SERVER_BACKEND=rocksdb` and
`HG_SERVER_USE_PD=false`. Its template already uses `serializer=binary`.
The entrypoint then configures local storage before the storage-wait step;
non-HStore backends skip PD/Store polling even if the template retains
`pd.peers`. Backend selection accepts spaces around `=` as well as the ordinary
colon/whitespace property separators, without requiring a backend environment
override. Backend names are case-insensitive.
Only recognized local backends bypass the wait; an unread or unknown
backend cannot silently take that path.
Images containing this fix pre-create `rocksdb-data` owned by
10001:10001 so a fresh named volume inherits writable ownership.
No graph configuration bind mount is needed. Older HStore images need a
corrected writable graph configuration and data-directory ownership instead.
Do not switch an existing
HStore installation's backend or reuse its initialization marker this way.

Custom configuration bind mounts must be writable by UID/GID 10001. Mount a
whole writable configuration directory rather than a single properties file:
the entrypoint edits files with atomic rename. Prefer a named data volume to
changing host-directory permissions recursively. See the
[Docker guide](../docker/README.md) for authentication keys, volume ownership
and a Server plus Hubble Compose quickstart.

## Binary distribution

Use Java 17. Server 1.8 bytecode does not run on Java 11. Run these commands
inside a fresh unpacked Server distribution:

```bash
java -version
# Confirm backend=rocksdb, serializer=binary and no active pd.peers in this file.
cat conf/graphs/hugegraph.properties
bin/init-store.sh
bin/start-hugegraph.sh
```

To enable authentication on a fresh distribution instead, set
`auth.admin_pa` in `conf/rest-server.properties`, run `bin/enable-auth.sh`,
and provide the same password on stdin when running `bin/init-store.sh`.
The initializer does not read the container entrypoint's `PASSWORD` variable.
Then start Server normally. Configuration files containing passwords must be
readable only by the service account and its administrators.

## Verify the graph

```bash
curl -fsS http://localhost:8080/versions
curl -fsS -u 'admin:replace-with-your-password' \
  http://localhost:8080/graphspaces/DEFAULT/graphs
curl -fsS -u 'admin:replace-with-your-password' \
  -H 'Content-Type: application/json' \
  -d '{"gremlin":"g.V().count()","aliases":{"g":"__g_DEFAULT-hugegraph"}}' \
  http://localhost:8080/gremlin
```

Omit `-u` if authentication is disabled. Server 1.8 still uses the `DEFAULT`
GraphSpace in standalone mode; it does not register the legacy
`__g_hugegraph` traversal source. GraphSpace management itself requires PD.
See [REST requests](rest-api-usage.md) for aliases, shard pagination and tasks.

## Optional Topling runtime

Keep `backend=rocksdb` and select the native provider separately, following
[Storage provider selection](storage-lifecycle.md#storage-provider-selection-and-toplingdb).
The standard image includes a Java 17 JDK and `unzip` for preparing a compatible
JNI supplied by the operator; it does not bundle or download optional JNI.
The JNI's glibc, libaio and CPU requirements must match the image or host.
Do not assume that a mutable image tag proves which native library is loaded.
