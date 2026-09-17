# Negative-label queries and local filtering

HugeGraph keeps property filters local when pushing them into an index could
discard vertices or edges needed by a later label predicate. For example,
`hasLabel(P.neq("author"))` includes other labels even if they do not have the same
property indexes. This also applies to unsafe label predicates across barriers,
ranges and child traversals where the optimizer cannot prove a narrower scope.

## Result completeness changes the no-index behavior

Consider a defined property `unindexedProp` with no property index:

```groovy
g.V().has("unindexedProp", "x")
g.V().has("unindexedProp", "x").hasLabel(P.neq("author"))
```

The first query uses the indexed-property query path and raises
`NoIndexException`. The second keeps the property predicate local and can scan
vertices, returning matching non-author vertices. It does **not** use the
missing-index exception as a fast-fail guard. This is intentional: selecting
only labels with a matching index could silently omit valid results.

This fallback can turn a selective index lookup into a full candidate scan,
increasing latency and backend work even when very few results match. Adding an
index to one label alone does not guarantee that this conservative fallback will
use it. When possible, specify a known positive label with a suitable index, or
start from explicit element IDs. Explicit-ID lookups and adjacent-element
traversals can filter their own candidates locally; they do not necessarily scan
the whole graph.

## Limits and paging

Existing query capacity checks still apply where the execution path enforces
them. The default capacity is 800,000 records; a candidate scan can reach this
limit before finding all matching results and raise `LimitExceedException`.
This is not a universal work bound: the test-only Memory backend does not enforce
scan capacity, some count paths disable capacity checks, and a final `limit()`
bounds returned matches rather than all candidates examined.

With `has("~page", cursor)`, the backend page is bounded before local filtering.
A page may contain fewer matches than requested, or no matches at all, while
still returning a continuation cursor. Continue until the cursor is exhausted;
do not stop solely because the filtered page is empty. Backends without paging
support cannot use this mechanism.

## SEARCH predicates

In this fallback, `Text.contains()` predicates in the filter chain directly
following the source step use the graph's SEARCH analyzer and exact term matcher,
including explicit `(word)` and `(word1|word2)` expressions. This chain can
include `barrier()`, but stops at steps such as `range()`, `limit()` or `order()`.
A `Text.contains()` placed after those steps keeps plain substring semantics.
For example, this query looks for the literal substring `(alpha)`, not the
SEARCH term `alpha`:

```groovy
g.V().hasLabel(P.neq("author")).limit(10).has("body", Text.contains("(alpha)"))
```

The adapted filters retain the original predicate tree for traversal inspection
and cloning. The runtime matcher is rebuilt against the element's graph after
serialization or rebinding; graph and analyzer objects are not serialized with
the filter.
