<h1 align="center">
  <img width="720" alt="Apache HugeGraph" src="https://github.com/apache/hugegraph/assets/38098239/e02ffaed-4562-486b-ba8f-e68d02bb0ea6" />
</h1>

<h3 align="center">A scalable graph database for connected data and real-time graph workloads</h3>

<div align="center">

[![License](https://img.shields.io/badge/license-Apache%202-0E78BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Server CI](https://github.com/apache/hugegraph/actions/workflows/server-ci.yml/badge.svg)](https://github.com/apache/hugegraph/actions/workflows/server-ci.yml)
[![PD and Store CI](https://github.com/apache/hugegraph/actions/workflows/pd-store-ci.yml/badge.svg)](https://github.com/apache/hugegraph/actions/workflows/pd-store-ci.yml)
[![Downloads](https://img.shields.io/github/downloads/apache/hugegraph/total.svg)](https://github.com/apache/hugegraph/releases)
[![Artifact Hub](https://img.shields.io/endpoint?url=https%3A%2F%2Fartifacthub.io%2Fbadge%2Frepository%2Fhugegraph-helm)](https://artifacthub.io/packages/helm/hugegraph-helm/hugegraph-helm)

</div>

**Start here:** [Quick start](#quick-start) · [Deployment](#deployment) · [Documentation](#documentation) · [Development](#development) · [Ecosystem](#ecosystem)

## Apache HugeGraph

Apache HugeGraph is a graph database for storing, querying and operating connected data at scale. It combines schema management, indexes, graph traversals, pluggable storage and standalone or distributed deployment in one system.

Applications can use REST APIs, Gremlin, Cypher or client SDKs. Gremlin compatibility is provided through Apache TinkerPop; it is one access path alongside HugeGraph's native APIs and services.

## Core capabilities

| Area | Capability |
|---|---|
| Data model | Property graph with explicit vertex labels, edge labels, property keys and indexes |
| Query and APIs | REST, Gremlin, Cypher, traverser APIs and language-specific clients |
| Storage | Embedded RocksDB for standalone deployments and HStore for distributed clusters |
| Operations | Authentication, metrics, tasks, backup and restore tooling, Docker and Kubernetes deployment |
| Scale | Single-node development through horizontally scalable Server, PD and Store clusters |
| Ecosystem | Data loading, visualization, graph computation, GraphRAG and AI integrations |

## Quick start

Start a standalone server for local evaluation, bind it to the loopback interface and wait for the version endpoint before continuing:

```bash
docker run -d --name hugegraph \
  -p 127.0.0.1:8080:8080 hugegraph/hugegraph

for attempt in $(seq 1 60); do
  if curl -fs http://127.0.0.1:8080/versions; then
    break
  fi
  if [ "${attempt}" -eq 60 ]; then
    printf 'Server did not become ready; inspect the logs before continuing.\n' >&2
    docker logs hugegraph
    break
  fi
  sleep 2
done
```

After the version request succeeds, run a small graph query. If the loop reports a timeout, resolve the startup failure before continuing:

```bash
curl -X POST http://127.0.0.1:8080/gremlin \
  -H 'Content-Type: application/json' \
  -d '{"gremlin":"g.V().limit(5)"}'
```

The loopback bind keeps this evaluation endpoint off ordinary remote interfaces while authentication is not enabled. Users of older Docker or unusual host-network setups should also restrict host firewall access explicitly. Pin a released image for repeatable deployments and enable authentication before exposing the service outside a trusted network. See the [server deployment guide](https://hugegraph.apache.org/docs/quickstart/hugegraph-server/) for packages, configuration and production preparation.

## Deployment

| Goal | Recommended entry point |
|---|---|
| Local development | [Standalone Docker](docker/README.md) |
| Multi-process or distributed test environment | [Docker Compose examples](docker/README.md) |
| Kubernetes deployment | [HugeGraph Helm chart](helm/hugegraph/README.md) |
| Binary installation | [Download and deployment guide](https://hugegraph.apache.org/docs/quickstart/hugegraph-server/) |
| Build or customize the source | [Building from source](docs/BUILDING.md) |

A standalone deployment runs Server with embedded RocksDB. In a distributed deployment, Server gets metadata and placement from PD but sends graph data requests directly to HStore nodes:

```text
Clients and tools
       │
       ▼
HugeGraph Server ─────────────────► RocksDB                 (standalone)
       │
       ├─ metadata / placement lookup ─► PD ── coordinates ─► HStore nodes
       └─ graph data requests ──────────────────────────────► HStore nodes  (distributed)
```

## Repository modules

| Module | Responsibility |
|---|---|
| [`hugegraph-server`](hugegraph-server/) | Graph engine, schema, REST/Gremlin/Cypher APIs and backend adapters |
| [`hugegraph-pd`](hugegraph-pd/) | Placement, metadata and distributed-cluster coordination |
| [`hugegraph-store`](hugegraph-store/) | Partition storage and Raft-based replication |
| [`hugegraph-struct`](hugegraph-struct/) | Shared graph structures, identifiers and serialization |
| [`hugegraph-commons`](hugegraph-commons/) | Shared utilities and RPC infrastructure |
| [`helm/hugegraph`](helm/hugegraph/) | Kubernetes chart for Server, PD, Store and optional ecosystem services |

## Documentation

Use the shortest authoritative source for the task:

- [Repository documentation index](docs/README.md): build, contribution, API, runtime, storage and internal design documents.
- [Official website documentation](https://hugegraph.apache.org/docs/): user installation, configuration and feature guides.
- [Helm documentation index](helm/hugegraph/docs/README.md): Kubernetes installation, operations and optional components.
- [Docker guide](docker/README.md): local and distributed Compose environments.
- [`AGENTS.md`](AGENTS.md): repository rules and code-navigation hints for contributors and coding agents.

The repository index explains which location owns each kind of information. Detailed documents should be linked from an index rather than left as isolated files.

## Development

The source build uses Java 17 and Maven 3.6.3 or later. Read versions from the root [`pom.xml`](pom.xml) instead of copying them into secondary documents.

```bash
mvn clean install -DskipTests
mvn clean install -pl hugegraph-server -am -DskipTests
```

Before contributing, read [BUILDING.md](docs/BUILDING.md), [CONTRIBUTING.md](docs/CONTRIBUTING.md) and the relevant module `AGENTS.md`. Run the tests required by the changed modules; Server changes commonly require both Server and PD/Store validation.

## Ecosystem

| Project | Purpose |
|---|---|
| [HugeGraph Toolchain](https://github.com/apache/hugegraph-toolchain) | Java client, Loader, Hubble, CLI tools and Spark connector |
| [HugeGraph Computer](https://github.com/apache/hugegraph-computer) | Distributed and in-memory graph computation, including Vermeer |
| [HugeGraph AI](https://github.com/apache/hugegraph-ai) | Python client, GraphRAG, knowledge-graph extraction, MCP and graph ML |
| [HugeGraph Documentation](https://github.com/apache/hugegraph-doc) | Website and long-form user documentation |

## Community and license

- [Contribution guide](https://hugegraph.apache.org/docs/contribution-guidelines/)
- [Issue tracker](https://github.com/apache/hugegraph/issues)
- [Developer mailing list](mailto:dev@hugegraph.apache.org)

Apache HugeGraph is licensed under the [Apache License 2.0](LICENSE).
