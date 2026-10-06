# Query count and filter behavior

Counts made inside a transaction include that transaction's uncommitted vertex
and edge changes. Only `count()` supports this fallback; other aggregates with
uncommitted changes are rejected. Queries that combine uncommitted changes with
pagination, a limit, or an offset remain unsupported.

Queries and counts select the current transaction version before applying filters.
When an element is removed and re-added with the same ID, the new element shadows
the old updated record, even if only the old record matches the query's label.

A count query that enters an index query path remains unsupported when that index
transaction has uncommitted changes. It reports `Can't do index query when there
are changes in transaction`. This includes indexed-property conditions and
label-only queries on backends that need a label index. Graph-wide and explicit-ID
counts can still use transaction merging without entering that index path.

Unsupported text predicates remain traversal filters. For ordinary GraphStep and
VertexStep extraction, if any condition in a `HasStep` cannot be converted, the
whole step remains local, including its sibling label, ID and property conditions.
Count optimization preserves that filter step. Existing special handling around
`match()` and connective label filters is inherited from master.

New generalized selective pushdown, predicate-specific local ID/SEARCH matching and
candidate-index coverage are outside this upgrade series. They are coordinated
through [PR #2994](https://github.com/apache/hugegraph/pull/2994),
[issue #3201](https://github.com/apache/hugegraph/issues/3201) and
[PR #3243](https://github.com/apache/hugegraph/pull/3243).

A self-loop contributes two occurrences to a vertex's `bothE()` traversal and
one to each of `outE()`, `inE()` and the graph-wide `E()` traversal. Counts retain
these multiplicities before and after transaction commit, including edge updates.

Count optimization preserves steps that can filter candidates. Resetting an
optimized count traversal permits it to execute again. Query-step equality does
not depend on the result iterator from a previous execution.

A scan in the middle of a traversal is counted once per incoming traverser,
including its bulk. It is not replaced by a single backend count. For example,
with three vertices, `g.V().V().count()` returns `9L`, while `g.V().count()`
returns `3L` and remains eligible for count optimization.
