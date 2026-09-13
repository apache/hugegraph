# API-REST-001 independent schema/error Oracle

Oracle source: `VertexApiTest` and `BaseApiTest.assertResponseStatus`.

The contract requires vertex creation to return HTTP 201, retrieval and listing
to return HTTP 200, and deletion to return HTTP 204. The fixture uses the
`person` label with `name` and `age` fields. The Java 11 host-network execution
reported four tests with zero failures, covering these status and response
contracts.

Evidence: `api-rest-001-vertex-java11-hostnet-20260913.log`.
