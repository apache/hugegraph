# HugeGraph Helm chart

This chart deploys a distributed HugeGraph cluster on Kubernetes. The default topology is 3 PD, 3 Store and 3 Server replicas with Hubble, authentication and persistent storage enabled. SeaTunnel and Vermeer are optional and disabled by default.

**Requirements:** Helm 3.10 or later, Kubernetes 1.23 or later, and a default StorageClass or prepared persistent volumes.

## Documentation map

| Goal | Document |
|---|---|
| Install the chart and choose common values | This README |
| Understand every detailed chart document | [Documentation index](docs/README.md) |
| Operate, scale, upgrade or troubleshoot a cluster | [Operations](docs/operations.md) |
| Install from OCI, mirror images or work offline | [Distribution](docs/distribution.md) |
| Deploy the RAG service | [RAG](docs/rag.md) |
| Enable data import and export | [SeaTunnel](docs/seatunnel.md) |
| Enable graph computation | [Vermeer](docs/vermeer.md) |
| Start from reviewed value files | [Examples](examples/README.md) |
| Inspect every supported value | [`values.yaml`](values.yaml) |

The root [repository documentation index](../../docs/README.md) links this chart to the wider Server, Docker and development documentation.

## Install a published chart

```bash
kubectl config current-context
kubectl get storageclass
helm install hugegraph oci://ghcr.io/hugegraph/hugegraph-helm --devel \
  --namespace hugegraph --create-namespace --wait --timeout 15m
```

`--devel` is convenient for evaluation because it can select development chart versions. For production or reproducible environments, use an explicit published version and pin image tags or digests after reviewing them.

## Install from source

Run from the repository root:

```bash
helm install hugegraph ./helm/hugegraph \
  --namespace hugegraph --create-namespace --wait --timeout 15m
```

Check the deployed services and read the initial administrator password:

```bash
kubectl -n hugegraph get pods,svc
kubectl -n hugegraph get secret hugegraph-admin \
  -o jsonpath='{.data.password}' | base64 --decode
```

Use the assigned Hubble service address, or forward it locally:

```bash
kubectl -n hugegraph port-forward svc/hugegraph-hubble 8088:8088
```

Then open `http://127.0.0.1:8088` and sign in as `admin`.

## Choose a topology

| Configuration | Purpose |
|---|---|
| Default `values.yaml` | 3 PD, 3 Store, 3 Server and Hubble |
| `values-single.yaml` | Small 1 PD, 1 Store, 1 Server environment for evaluation |
| `values-cluster.yaml` | Cluster scheduling, anti-affinity and resource starting points |
| `values-strict.yaml` | Private service defaults and NetworkPolicy-oriented deployment |

Topology presets do not automatically enable SeaTunnel or Vermeer. Enable optional components explicitly and choose their own standalone or cluster mode.

## Common settings

| Goal | Value |
|---|---|
| Disable Hubble | `hubble.enabled=false` |
| Disable Server authentication for a trusted local environment | `server.auth.enabled=false` |
| Disable persistent storage for a disposable new installation | `persistence.enabled=false` |
| Select a StorageClass | `pd.storage.storageClassName`, `store.storage.storageClassName`, `hubble.persistence.storageClassName` |
| Use an internal image registry | `global.imageRegistry` |
| Enable SeaTunnel | `seatunnel.enabled=true` |
| Enable Vermeer | `vermeer.enabled=true` |
| Change Server, PD or Store replicas | `server.replicas`, `pd.replicas`, `store.replicas` |
| Choose public or private service entry | Component `service.type` or chart `mode` |

Use `--set` for small overrides and a reviewed YAML file for repeatable installations. [`values.yaml`](values.yaml) is the authoritative option reference; examples show complete combinations without duplicating the full option table here.

## Optional components

### SeaTunnel

SeaTunnel provides import, export and graph-to-graph migration. Enable it with `seatunnel.enabled=true`, then follow the [SeaTunnel guide](docs/seatunnel.md) for standalone or cluster mode, credentials, example jobs and operational limits.

### Vermeer

Vermeer provides graph computation and Hubble integration. Enable it with `vermeer.enabled=true`, then follow the [Vermeer guide](docs/vermeer.md) for topology, authentication, storage and recovery.

### RAG

The chart can deploy HugeGraph AI's RAG service. Follow the [RAG guide](docs/rag.md) for model credentials, network exposure, persistence and compatibility requirements.

## Upgrade and verify

Review the active values before upgrading:

```bash
helm get values hugegraph -n hugegraph --all
helm upgrade hugegraph ./helm/hugegraph -n hugegraph --reuse-values \
  --wait --timeout 15m
helm test hugegraph -n hugegraph --logs --timeout 5m
```

`--reuse-values` preserves existing settings; `--reset-values` adopts new defaults and therefore requires a complete reviewed values file. Always verify Pod readiness, a fresh Hubble login and at least one business query after the upgrade. Templates and linting cannot prove storage, networking or application behavior in a live cluster.

## Operational boundaries

- PD and Store membership changes are coordinated operations. Changing a replica count in values does not by itself complete every safe join, transfer or retirement step; use the [operations guide](docs/operations.md).
- Increasing Store replicas does not retroactively change existing data from replication factor 1 to replication factor 3. Replica policy and node count are separate concerns.
- Persistent PD and Store data must keep stable identities and metadata settings. Do not switch an initialized deployment between temporary and persistent storage.
- Generated Secrets provide initial credentials. Changing a Secret alone does not necessarily rotate an account already stored by an application.
- Pin compatible chart and component image versions for production. A chart cannot reliably infer behavior that an older image does not implement.

## More resources

- [Official Helm deployment guide](https://hugegraph.apache.org/docs/quickstart/hugegraph/hugegraph-helm/)
- [Official Kubernetes operations guide](https://hugegraph.apache.org/docs/quickstart/hugegraph/hugegraph-helm-operations/)
- [Artifact Hub package](https://artifacthub.io/packages/helm/hugegraph-helm/hugegraph-helm)
