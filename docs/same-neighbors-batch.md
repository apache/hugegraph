# Pairwise same neighbors

`POST /graphspaces/{graphspace}/graphs/{graph}/traversers/sameneighborsbatch`
computes the common neighbors of each vertex pair independently.
It uses the graph and graphspace permission checks of other traverser APIs.

```json
{
  "vertex_list": [["vertex-a", "vertex-b"], ["vertex-c", "vertex-d"]],
  "direction": "BOTH",
  "label": "knows",
  "max_degree": 10000,
  "limit": 100
}
```

`vertex_list` must be present, non-null, and non-empty. Each entry must contain exactly two
non-null vertex IDs, using the numeric/string ID representation. Numeric IDs must
use JSON integer syntax, between -9223372036854775808 and 9223372036854775807.
Repeated pairs remain repeated results.
All pairs are validated before traversal. Missing vertices and unsupported ID
values are rejected through the existing argument error handling.

`direction` defaults to `BOTH`; allowed values are `OUT`, `IN`, and `BOTH`.
`label` is optional and filters edges for each pair. `max_degree` defaults to
10000 and must be positive or `-1` (unlimited). `limit` defaults to 10000000,
applies separately to each pair, and must be positive up to 2147483647 or `-1`.
The API does not introduce a separate batch-size parameter; callers should bound
the number of pairs and use finite degree/limit values to bound traversal work.

The response contains `same_neighbors`, an array of arrays in input-pair order,
and `measure`, containing traversal iteration counts and elapsed time.
A pair with no common neighbors contributes an empty array. Neighbor order within
a pair is unspecified. Iteration counts cover all pairs once.

The existing `/traversers/sameneighbors` endpoints remain unchanged: GET computes
the intersection for two vertices, while POST computes one intersection across
all vertices in its flat `vertex_list`. Their response shapes are unchanged.
