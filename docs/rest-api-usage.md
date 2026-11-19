# REST paths and Gremlin requests

The REST listener uses paths such as `/versions`, `/graphspaces` and `/gremlin`.
`GET /apis` lists resource paths and methods. Each `url` starts with `/`
and includes the application path configured in `restserver.url`: a root
installation lists `/gremlin`, while `http://localhost:8080/hg` lists
`/hg/gremlin`. Call the listed path on the Server origin, without prepending
`/apis`. A gateway's additional external prefix may still need to be retained.

## Select a graph for synchronous Gremlin

GraphSpace names and graph names are case-sensitive. In a GraphSpace-enabled
Server, the graph binding is `<graphspace>-<graph>`, and its traversal-source
binding is `__g_<graphspace>-<graph>`. There is no implicit global `g`: a Server
can host multiple graphs. Supply aliases for the names used by the query:

```bash
curl -X POST 'http://localhost:8080/gremlin' \
  -H 'Content-Type: application/json' \
  -d '{"gremlin":"g.V().count()","aliases":{"graph":"DEFAULT-hugegraph","g":"__g_DEFAULT-hugegraph"}}'
```

For an authenticated Server, also supply its configured HTTP authentication
credentials, for example `curl -u "$HG_USER:$HG_PASSWORD"`. Request `bindings`
are query variables; they do not create graph bindings. An alias target must be
an existing graph or traversal-source binding, rather than a Gremlin expression
such as `graph.traversal()`.

Server 1.8 uses these GraphSpace-scoped bindings in standalone RocksDB mode too:
the default graph is `DEFAULT-hugegraph`, and its traversal source is
`__g_DEFAULT-hugegraph`. Disabling PD does not select legacy aliases. Only older
Servers without GraphSpace bindings use `<graph>` and `__g_<graph>`. Select the
Java client's GraphSpace to match the Server, and verify the configured names
before treating a `Could not rebind` response as a graph-registration failure.

## Export a shard with bounded pages

Both `GET .../traversers/vertices/scan` and `GET .../traversers/edges/scan`
require `start` and `end`. Missing either parameter returns HTTP 400 with its
name on every backend. This also changes HStore and in-memory requests that
previously interpreted missing bounds as zero; callers must now supply both.
See the [upgrade notes](upgrade-notes.md#required-shard-scan-bounds).

Empty strings represent open bounds in RocksDB and HBase. They do **not**
represent a whole-graph range in HStore or the in-memory backend. The following
example is for **RocksDB**:

```bash
curl -G 'http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/traversers/vertices/scan' \
  --data-urlencode 'start=' --data-urlencode 'end=' \
  --data-urlencode 'page=' --data-urlencode 'page_limit=1000'
```

Pass `page=` explicitly for the first page, then URL-encode the returned `page`
token on subsequent requests, keeping the same bounds. Stop when the returned
token is null or empty. Edge scans use the same pagination contract.

For HStore and in-memory exports, first request the corresponding
`.../vertices/shards?split_size=1048576` or `.../edges/shards?split_size=1048576`.
Iterate **every** returned shard, using its exact `start` and `end` and a fresh
`page=` for each shard. Preserve the backend's boundary encoding; do not reuse
vertex bounds for edges. An empty HStore range can stop after the partition
containing hash zero, and an empty in-memory end can produce no results.
A terminal page token finishes only that shard, not the entire graph.

Omitting `page` retains the existing unpaged scan behavior: `page_limit` does
not bound that response and the result has no continuation token. Exporters
must pass the empty initial page to avoid reading an entire shard at once.

## Run long queries as tasks

Synchronous execution has a configured timeout (`evaluationTimeout` in
`gremlin-server.yaml`, 30000 milliseconds by default). Large label counts may
scan substantial data even when a whole-graph count is fast. Use an asynchronous
job when a query can exceed this limit:

```bash
curl -X POST 'http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/jobs/gremlin' \
  -H 'Content-Type: application/json' \
  -d '{"gremlin":"g.V().hasLabel(\"person\").count()"}'
```

The response contains `task_id`. Poll
`GET /graphspaces/DEFAULT/graphs/hugegraph/tasks/{task_id}` until the task reports
a terminal status, then inspect `task_result`. A running task has not completed
its count. The job binds `graph` and `g` for the graph selected in its path.
Use the same authentication as for the other Server APIs.

## Create another graph

In PD mode, list graph spaces with `GET /graphspaces`. This listing is not
supported in standalone mode; skip it and use `GET /graphs` to list configured
graphs. Create a graph using
`POST /graphspaces/DEFAULT/graphs/{graph}` with the graph configuration expected
by the backend. `POST /graphs` is not the graph-creation contract. Creating a
graph requires the corresponding Server permissions and backend configuration.
