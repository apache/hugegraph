# Store monitoring metrics

The Store REST service offers a lightweight Raft summary for dashboards:

```http
GET /metrics/raft/summary
Accept: application/json
```

```json
{"groups": 32, "enabled_groups": 32}
```

`groups` counts local partition Raft groups. `enabled_groups` counts groups
whose JRaft metrics collection is enabled; it is not a count of healthy groups,
leaders, or available replicas. An empty node returns both counters as zero.
A group whose metrics object is not yet available still counts in `groups`
and does not count in `enabled_groups`.

Use this endpoint when only the counters are needed. Its response contains two
integers regardless of how many groups, histograms, and timer samples the node
holds. It does not include group names or metric registries. It uses the same
Store REST access controls as the other `/metrics` endpoints.

`GET /metrics/raft` retains the complete per-group JRaft metrics response for
manual diagnostics. Its payload can exceed dashboard collection limits as
partition count and registered metrics grow. Clients supporting older Store
versions can fall back to this detailed endpoint only when the summary endpoint
returns HTTP 404 or 405. Transport, authorization, or server errors should remain
visible instead of causing a second full metrics request.
