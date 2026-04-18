# Key File and Directory Locations

## Build & Config
- `pom.xml` — Root multi-module POM
- `.editorconfig` — Code style rules
- `style/checkstyle.xml` — Checkstyle enforcement (hugegraph-style.xml was removed)
- `.licenserc.yaml` — License checker config

## Server (hugegraph-server)
- Core engine: `hugegraph-core/src/main/java/org/apache/hugegraph/` → `backend/`, `schema/`, `traversal/`, `task/`
- GraphSpace: `hugegraph-core/.../space/` → `GraphSpace.java`, `SchemaTemplate.java`, `Service.java`, `register/`
- REST APIs: `hugegraph-api/src/main/java/org/apache/hugegraph/api/` → `graph/`, `schema/`, `gremlin/`, `cypher/`, `auth/`, `space/` (GraphSpaceAPI), `metrics/`, `arthas/`
- OpenCypher: `hugegraph-api/.../opencypher/`
- Backend interface: `hugegraph-core/.../backend/store/BackendStore.java`
- Distribution: `hugegraph-dist/src/assembly/static/` → `bin/`, `conf/`, `lib/`, `logs/`
- Tests: `hugegraph-test/src/main/java/.../` → `unit/`, `core/`, `api/`, `tinkerpop/`

## Docker
- `docker/docker-compose.yml` — Single-node (bridge network, pd+store+server)
- `docker/docker-compose-3pd-3store-3server.yml` — 3-node cluster
- `docker/docker-compose.dev.yml` — Dev mode

## PD Module
- Proto: `hugegraph-pd/hg-pd-grpc/src/main/proto/`
- Dist: `hugegraph-pd/hg-pd-dist/src/assembly/static/`

## Store Module
- Proto: `hugegraph-store/hg-store-grpc/src/main/proto/`
- Dist: `hugegraph-store/hg-store-dist/src/assembly/static/`

## CI Workflows (.github/workflows/)
- `server-ci.yml` — Server tests (matrix: memory/rocksdb/hbase × Java 11)
- `pd-store-ci.yml` — PD, Store & HStore tests
- `commons-ci.yml` — Commons tests
- `cluster-test-ci.yml` — Cluster integration
- `licence-checker.yml` — License headers
- `rerun-ci.yml` — Auto-rerun for flaky workflows
- `auto-pr-review.yml` — Auto-comment on new PRs
- `check-dependencies.yml` — Dependency checks
- `codeql-analysis.yml` — CodeQL security scanning
- `stale.yml` — Stale issue/PR cleanup

## Docs
- `README.md`, `BUILDING.md`, `CONTRIBUTING.md`, `AGENTS.md`, `CLAUDE.md`
