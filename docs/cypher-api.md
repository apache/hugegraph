# Cypher API response compatibility

The Cypher endpoint is
`/graphspaces/{graphspace}/graphs/{graph}/cypher`. Submit one statement per
request. GET accepts the `cypher` query argument and does not accept a parameter
map; use POST for parameterized queries. POST with `Content-Type: application/json`
accepts a JSON object containing a nonblank string `cypher` and an optional
object `parameters`, for example:

```json
{
  "cypher": "MATCH (n:person) WHERE n.name = $name RETURN n.name",
  "parameters": {"name": "marko"}
}
```

Legacy raw Cypher bodies sent as `application/json` remain accepted. JSON arrays,
scalars, malformed JSON, missing/non-string/blank `cypher`, and a `parameters`
value of null, an array or a scalar are rejected by HTTP request validation.
Omitting `parameters` supplies an empty map. Additional request fields and extra
parameter names are allowed; every parameter referenced by the parsed query must
be present. Individual parameter values may explicitly be null.
Requests are limited to 16 top-level parameter names. This limit is fixed; see
[Cypher HTTP contracts](server-runtime.md#cypher-http-contracts) for the binding
limits and validation rules.

The Server checks missing bindings before executing the traversal, so GET, raw
POST and JSON POST with an absent referenced name fail with `Missing Cypher
parameter: name`. This uses the execution-failure envelope below, classified as
`HugeGraph.Cypher.ExecutionError` with an empty hint. The upstream translator's
standalone unbound-to-null behavior is not the Server request contract. Do not
send `?parameters=...` to bind a GET query. Invalid binding values, including the
reserved translator null marker, also fail before traversal execution; explicit
JSON null values are accepted and normalized by the Server.

## Authentication

Cypher follows the Server authentication mode. When authentication is disabled,
GET and POST requests do not require an `Authorization` header, and the Server
uses an anonymous Gremlin connection. Both the request's `Authorization` header
and credentials in `conf/remote-objects.yaml` are ignored. Mixed modes with REST
authentication disabled and Gremlin authentication enabled are unsupported.
When authentication is enabled, requests still need
valid Basic or Bearer credentials and retain the caller's graph permissions.
Use `bin/enable-auth.sh` to configure authentication consistently for both REST
and Gremlin; enabling only the REST authenticator does not protect the Gremlin
endpoint used to execute Cypher.

The Gremlin host and port in `conf/remote-objects.yaml` must point to the same
Server's Gremlin endpoint, including when using nondefault ports.

The API test runner exercises anonymous GET and POST before enabling
authentication, then runs the authenticated suite, including missing-header
rejection and a read-only user's write and cross-graph rejection cases.

## Response envelope

Responses retain the Gremlin-compatible top-level fields `requestId`, `status`
and `result`. There is no top-level `errors` field. Existing Java clients can
continue to deserialize both successful and failed queries without an upgrade.

A successful query has `status.code = 200`, an empty `status.attributes` map,
and its rows in `result.data`, for example:

```json
{
  "requestId": "example-request",
  "status": {"message": "", "code": 200, "attributes": {}},
  "result": {"data": [{"value": 1}], "meta": {}}
}
```

When query submission or execution fails inside the Cypher client, the existing
HTTP 200 / `status.code = 400` convention and `status.message` are preserved.
Authentication, request validation and other HTTP-layer errors remain separate
and need not use this envelope. Consumers must inspect `status.code` instead of
relying only on the HTTP status.

Structured error metadata lives at `status.attributes.errors`, an array with
one entry for the failed query. Each entry contains a stable string `code`, the
translator/execution `message`, and a `hint` (empty when no specific guidance is
available). Message wording is not a stable contract. Example:

```json
{
  "requestId": "example-request",
  "status": {
    "message": "Expected exactly one statement per query but got: 2",
    "code": 400,
    "attributes": {
      "errors": [{
        "code": "HugeGraph.Cypher.SyntaxError",
        "message": "Expected exactly one statement per query but got: 2",
        "hint": "Submit exactly one Cypher statement per request"
      }]
    }
  },
  "result": {"data": null, "meta": {}}
}
```

Clients that only consume `status.code`, `status.message` and `result` can ignore
the attributes. A failure with no mapped metadata may have empty attributes.

## Error codes and hints

| Code | Recognized failure | Hint |
| --- | --- | --- |
| `HugeGraph.Cypher.SyntaxError` | Parser invalid input, unknown function or semantic error with a `(line N, column M (offset: K))` suffix | Check the query syntax and function names supported by the built-in Cypher translator |
| `HugeGraph.Cypher.SyntaxError` | More or fewer than one statement | Submit exactly one Cypher statement per request |
| `HugeGraph.Cypher.SyntaxError` | `Variable` followed by a backtick-quoted name and `not defined` | Declare the variable in a MATCH, UNWIND or WITH clause before referencing it |
| `HugeGraph.Cypher.ExecutionError` | Undefined vertex label, edge label, property key or index label | Create the schema element via the schema API before running the query |
| `HugeGraph.Cypher.UnsupportedFeature` | Translator messages beginning `Unsupported clause:`, `Unsupported match pattern:`, `Unsupported create pattern:`, `Unsupported property type:`, `Multiple labels are not supported:` or `Custom functions and predicates are not supported on target implementation:` | This construct is not covered by the built-in translator, see the Cypher compatibility guide for supported syntax |
| `HugeGraph.Cypher.ExecutionError` | Other failures, including unrecognized translation failures | Empty string |

`ExecutionError` is also the fallback for unrecognized translation failures; it
does not mean traversal execution has started. For example, `reduce(...)` or
semantic rejections without a recognized position suffix may return this code
with an empty hint while preserving the original failure message. Classification
recognizes only the translation-1.0.4 forms above and does not cover every
translator failure; runtime errors such as `Unsupported predicate` and
`CONTAINS query ... is not supported` also fall back.
There is no `HugeGraph.Cypher.MissingParameter` contract.

## Temporal functions

The built-in translator has no temporal functions of its own. The Server folds
the `date()`, `datetime()` and `localdatetime()` constructors into native date
values before translation, so these calls work wherever a bound value is
accepted, for example `MATCH (n) WHERE n.birthday = date('2000-01-01') RETURN n`.
A constructor folds when it has no argument (the current date or date-time), a
string literal argument, or a parameter bound to a string; `date()` uses the
`yyyy-MM-dd` form and the other two use `yyyy-MM-ddTHH:mm:ss[.SSS]`. Arguments
that are not foldable, other temporal forms such as map or duration arguments,
and the remaining temporal functions (`time()`, `localtime()`, `duration()`)
keep failing translation with `Unknown function` as before.
Constructor names are case-insensitive like the rest of Cypher, and the bare
constructors of one statement read the clock once. EXPLAIN prints a folded
value as an explicit `new Date(...)` constructor instead of a stringified date.

## Regression checks

Run the mapper, real-parser and envelope tests from the repository root:

```sh
mvn test -pl hugegraph-server/hugegraph-test -am -P unit-test \
  -Dtest=CypherErrorTest,CypherClientTest,CypherOpProcessorTest \
  -Dsurefire.failIfNoSpecifiedTests=false
```

`CypherApiTest` also exercises malformed syntax, multiple statements, undefined
variables and missing schema through a running test server. Its `testParameters`
and `testMissingParameterHasFallbackError` cover bound JSON requests and missing
bindings. `CypherClientTest` checks outgoing query/binding transport, and
`CypherOpProcessorTest` covers request binding validation. The mapper's
translator-only unbound-parameter test bypasses that validation. Follow the normal
Server API-test setup using disposable test data.

The standalone `CypherClientCompatibilityTest` uses the actual Java Client
`Response` and `RestResult` classes. It is outside the Server reactor's source
roots to avoid a Server-to-Client dependency cycle. Supply an existing Client
jar (checked with locally available 1.7.0 and development 1.8.0 artifacts) and run from the repository root:

```sh
mvn -f hugegraph-server/hugegraph-api/pom.xml dependency:build-classpath \
  -Dmdep.outputFile="$PWD/target/cypher-classpath.txt"
CYPHER_CLIENT_JAR="$HOME/.m2/repository/org/apache/hugegraph/hugegraph-client/1.7.0/hugegraph-client-1.7.0.jar"
CYPHER_CP="$CYPHER_CLIENT_JAR:$(cat target/cypher-classpath.txt)"
mkdir -p target/cypher-compatibility
javac -encoding UTF-8 -proc:none -cp "$CYPHER_CP" \
  -d target/cypher-compatibility \
  hugegraph-server/hugegraph-api/src/main/java/org/apache/hugegraph/api/cypher/CypherModel.java \
  hugegraph-server/hugegraph-api/src/main/java/org/apache/hugegraph/api/cypher/CypherErrorMapper.java \
  hugegraph-server/hugegraph-test/src/compatibility/java/CypherClientCompatibilityTest.java
java -cp "target/cypher-compatibility:$CYPHER_CP" \
  org.junit.runner.JUnitCore CypherClientCompatibilityTest
```

These checks cover legacy envelopes, current success/failure deserialization,
status/message preservation, and a negative control proving that a top-level
`errors` field is rejected. They do not certify a live Hubble deployment or
matching-release-candidate acceptance.
