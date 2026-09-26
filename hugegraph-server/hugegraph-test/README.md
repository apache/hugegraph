# Server integration tests

Run API tests against a dedicated test graph service with the matching backend
profile, for example `mvn test -pl hugegraph-server/hugegraph-test -am -P api-test,hstore`.
The suite creates and removes schemas, data, users and graphspaces. Its default
graph is `DEFAULT/hugegraph`; use an isolated service and data volumes.

`BaseApiTest` accepts the following optional environment variables so the same
suite can run through a port-forward against a Helm-managed test deployment:

| Variable | Default |
| --- | --- |
| `HUGEGRAPH_TEST_SERVER_URL` | `http://127.0.0.1:8080` |
| `HUGEGRAPH_TEST_ADMIN_USERNAME` | `admin` |
| `HUGEGRAPH_TEST_ADMIN_PASSWORD` | Existing local test fixture password |

Supply credentials through the test process environment. Do not place them in
Maven command-line arguments or save them to test evidence. The overrides affect
the administrator client; per-test users still use their own fixture credentials.
Existing local and CI invocations keep their defaults when variables are absent.
