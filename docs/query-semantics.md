# Query count and filter behavior

Counts made inside a transaction include that transaction's uncommitted vertex
and edge changes. Only `count()` supports this fallback; other aggregates with
uncommitted changes are rejected. Queries that combine uncommitted changes with
pagination, a limit, or an offset remain unsupported.

Unsupported text predicates remain traversal filters. HugeGraph can still use
label and supported indexed conditions to select candidates, then evaluates the
text predicate locally. A missing required index is still reported as an error.

Count optimization preserves steps that can filter candidates. Resetting an
optimized count traversal permits it to execute again. Query-step equality does
not depend on the result iterator from a previous execution.

A scan in the middle of a traversal is counted once per incoming traverser,
including its bulk. It is not replaced by a single backend count. For example,
with three vertices, `g.V().V().count()` returns `9L`, while `g.V().count()`
returns `3L` and remains eligible for count optimization.
