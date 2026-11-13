# Key file locations

Use these paths as search anchors; follow the nearest README or `AGENTS.md` for local rules.

| Work area | Start here |
|---|---|
| Repository modules and versions | `pom.xml` |
| Build, contribution and documentation map | `docs/README.md`, `docs/BUILDING.md`, `docs/CONTRIBUTING.md` |
| Graph engine, schema, traversal and tasks | `hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/` |
| REST, Gremlin, Cypher and authentication APIs | `hugegraph-server/hugegraph-api/src/main/java/org/apache/hugegraph/api/` |
| Server distribution and runtime configuration | `hugegraph-server/hugegraph-dist/` |
| Server tests | `hugegraph-server/hugegraph-test/` |
| PD implementation and protocol | `hugegraph-pd/`, especially `hg-pd-core/`, `hg-pd-service/` and `hg-pd-grpc/` |
| Store implementation and protocol | `hugegraph-store/`, especially `hg-store-core/`, `hg-store-node/` and `hg-store-grpc/` |
| Shared graph structures | `hugegraph-struct/` |
| CI workflows and policy | `.github/workflows/`, `.github/scripts/`, `docs/ci.md` |
| Dependency and release metadata | `install-dist/release-docs/`, `install-dist/scripts/dependency/` |
| Docker and Compose | `docker/README.md`, `docker/` |
| Kubernetes and Helm | `helm/hugegraph/README.md`, `helm/hugegraph/docs/README.md`, `helm/hugegraph/values.yaml` |

Search the repository before assuming a path or configuration name is still current. This memory intentionally omits workflow filenames and submodule counts that change frequently.
