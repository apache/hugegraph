# Cypher API response compatibility

The Cypher endpoint is
`/graphspaces/{graphspace}/graphs/{graph}/cypher`. GET accepts the `cypher`
query argument; POST accepts a raw Cypher statement with
`Content-Type: application/json`. Submit one statement per request.
The Apache mainline endpoint does not accept a parameter map. In the default
translator, unbound `$name` expressions become Cypher null; they do not raise a
missing-parameter error. Do not send `?parameters=...` to bind them.

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
| `HugeGraph.Cypher.SyntaxError` | Parser invalid input or unknown function | Check the query syntax and function names supported by the built-in Cypher translator |
| `HugeGraph.Cypher.SyntaxError` | More or fewer than one statement | Submit exactly one Cypher statement per request |
| `HugeGraph.Cypher.SyntaxError` | `Variable` followed by a backtick-quoted name and `not defined` | Declare the variable in a MATCH, UNWIND or WITH clause before referencing it |
| `HugeGraph.Cypher.ExecutionError` | Undefined vertex label, edge label, property key or index label | Create the schema element via the schema API before running the query |
| `HugeGraph.Cypher.UnsupportedFeature` | Translator reports `not supported` or `unsupported` | This construct is not covered by the built-in translator, see the Cypher compatibility guide for supported syntax |
| `HugeGraph.Cypher.ExecutionError` | Other failures | Empty string |

The Gremlin transport exposes parser failures as messages, so classification
recognizes the translation-1.0.4 message forms above. Other failures fall back to
`ExecutionError`; these codes are not a promise to identify every possible
translator failure. There is no `HugeGraph.Cypher.MissingParameter` contract.

This contract repairs the unreleased proposal in
[Server #3241](https://github.com/apache/hugegraph/pull/3241). The paired website
work is [Doc #499](https://github.com/apache/hugegraph-doc/pull/499); its author
must incorporate this response contract before coordinated publication.
The optional [bound-query fork #238](https://github.com/hugegraph/hugegraph/pull/238)
is independent and is not implemented here.

## Regression checks

Run the mapper, real-parser and envelope tests from the repository root:

```sh
mvn test -pl hugegraph-server/hugegraph-test -am -P unit-test \
  -Dtest=CypherErrorTest -Dsurefire.failIfNoSpecifiedTests=false
```

`CypherApiTest` also exercises malformed syntax, multiple statements, undefined
variables and missing schema through a running test server. Follow the normal
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
