# HugeGraph HStore Helm Chart

[Apache HugeGraph](https://hugegraph.apache.org/) - an open source, distributed graph database.

## Documentation

This chart deploys a distributed HugeGraph cluster - PD, Store, and Server - on
Kubernetes. For HugeGraph itself see <https://hugegraph.apache.org/docs/>.

Note that this chart requires Helm 3. `--reset-then-reuse-values`, referenced
under Upgrading, requires Helm 3.14 or later.

## Prerequisites Details

* Kubernetes 1.23+ (the chart renders `autoscaling/v2` and `policy/v1`)
* PV support on the underlying infrastructure: a default StorageClass, or an
  explicit `storageClassName` for PD and Store
* Sufficient memory for nine JVM processes in the default topology.
  Insufficient memory causes OOM kills that surface as silent Raft failures
  rather than as clear errors.

## Chart Details

| Component | Workload | Purpose |
|---|---|---|
| PD | StatefulSet + PVC | Placement driver; Raft group tracking Stores and partitions |
| Store | StatefulSet + PVC | Graph data storage (HStore) |
| Server | Deployment | Gremlin and REST query layer |

A distributed HugeGraph cluster has a startup contract that this chart encodes
so operators do not have to:

- **Server does not run `init-store`.** The chart injects
  `HG_SERVER_INIT_STORE_ENABLED=false`, and the image's `init-store` exits when
  `init_store.enabled=false`, after which Server registers with PD normally.
  This matters because nothing serializes Server replicas: without the gate,
  every replica would initialize the same backend concurrently. The chart
  creates no init Job and does not set `HG_SERVER_SKIP_INIT`. Standalone
  behavior is unchanged, because the option defaults to `true` when unset.
- **Store waits for PD quorum** in an init container before starting, so Store
  never registers against an incomplete PD Raft group.
- **The Server startup probe allows at least 450 seconds.** The image may spend
  300 seconds waiting for storage and a further 120 seconds in the start
  command. A lower configured `failureThreshold` is raised to this floor rather
  than being rejected.
- **The image entrypoint keeps ownership of `PASSWORD` handling and
  `auth.admin_pa`.** When authentication or a custom port or REST tuning is
  configured, the chart's wrapper only ensures `usePD=true` and `pd.peers` are
  present, then hands off to the image entrypoint.
- **Resource names reserve their suffix and StatefulSet ordinal before
  truncation,** so a long release name cannot produce colliding or over-long
  Pod and Service names, and PD/Store identities stay fixed when replicas
  change.

## Installing the Chart

```bash
helm install hugegraph ./helm/hugegraph --namespace hugegraph --create-namespace
```

This deploys 3 PD + 3 Store + 3 Server, preserves the image's automatic JVM
sizing, and sets no resource requests or limits. Set resources before
production use.

This first chart is version `0.1.0`. While the contribution is a draft, its
component image tags and `appVersion` track `latest` with pull policy `Always`.
Before stable publication, pin all three component tags and `appVersion` to the
next HugeGraph release and switch the component pull policies to
`IfNotPresent`.

Verify the release:

```bash
helm test hugegraph --namespace hugegraph
```

### Values Presets

| File | Purpose |
|---|---|
| `values.yaml` | Default 3+3+3 topology |
| `values-single.yaml` | Single-node 1+1+1 example |
| `values-cluster.yaml` | Production 3+3+3 starting point with JVM/resources, PD/Store PDBs, and required anti-affinity |

`values-cluster.yaml` is a production starting point, not a capacity
guarantee. Recalculate capacity for the graph size, traffic, failure budget,
node topology, and storage class before production use.

## Upgrading the Chart

```bash
helm upgrade hugegraph ./helm/hugegraph --namespace hugegraph --reuse-values
```

Every optional field stays optional, so a release created by an earlier
revision continues to render under `--reuse-values`. Note that `--reuse-values`
keeps the old release's values as the complete base, so a release created
before a field existed does **not** pick up its new default — including the
hardened `securityContext`, ServiceAccounts, and `terminationGracePeriodSeconds`.
Pod-level token mounting is the one exception: it is disabled unconditionally.
Use `-f` with your own values, or `--reset-then-reuse-values`, to adopt them.

PD and Store resource names reserve room for their StatefulSet ordinal before truncation, so
identities stay fixed across replica changes and scaling never renames a
PersistentVolumeClaim.

## Uninstalling the Chart

```bash
helm uninstall hugegraph --namespace hugegraph
```

Helm does not remove PersistentVolumeClaims created by StatefulSets. Delete
them explicitly, and only when the data is no longer needed.

## Configuration

The following table lists the configurable parameters of the chart and their
default values.

### Global

| Parameter | Description | Default |
|---|---|---|
| `nameOverride` | Override the chart name in generated resource names | `""` |
| `fullnameOverride` | Override the full generated resource name | `""` |
| `imagePullSecrets` | Secrets used to pull the PD, Store, and Server images | `[]` |

### PD

| Parameter | Description | Default |
|---|---|---|
| `pd.replicas` | PD StatefulSet replicas. Maximum `99` | `3` |
| `pd.image.repository` | PD image repository | `hugegraph/pd` |
| `pd.image.tag` | PD image tag. Tracks the development image until the next release is pinned | `latest` |
| `pd.image.pullPolicy` | PD image pull policy | `Always` |
| `pd.javaOpts` | Empty preserves the image's automatic JVM sizing | `""` |
| `pd.ports.grpc` | PD gRPC port | `8686` |
| `pd.ports.rest` | PD REST port, also used by probes | `8620` |
| `pd.ports.raft` | PD Raft port | `8610` |
| `pd.dataPath` | PD data directory inside the container | `/hugegraph-pd/pd_data` |
| `pd.storage.size` | PD PersistentVolumeClaim size | `10Gi` |
| `pd.storage.storageClassName` | Empty uses the cluster default StorageClass | `""` |
| `pd.resources` | PD container resources. Set these for production | `{}` |
| `pd.podSecurityContext` | Pod-level securityContext, rendered only when set | `{}` |
| `pd.securityContext` | Container-level securityContext. Hardened by default; `runAsNonRoot` is not set because the published images run as root | `allowPrivilegeEscalation: false`, `capabilities.drop: [ALL]`, `seccompProfile: RuntimeDefault` |
| `pd.antiAffinity` | One of `required`, `preferred`, `disabled` | `required` |
| `pd.nodeSelector` | Node selector for pd Pods | `{}` |
| `pd.tolerations` | Tolerations for pd Pods | `[]` |
| `pd.affinity` | Raw affinity; overrides `pd.antiAffinity` when set | `{}` |
| `pd.topologySpreadConstraints` | Topology spread constraints for pd Pods | `[]` |
| `pd.priorityClassName` | PriorityClass for pd Pods | `""` |
| `pd.podAnnotations` | Extra annotations on pd Pods | `{}` |
| `pd.podLabels` | Extra labels on pd Pods | `{}` |
| `pd.extraEnv` | Extra environment variables for the PD container | `[]` |
| `pd.terminationGracePeriodSeconds` | Shutdown grace period | `300` |
| `pd.serviceAccount.create` | Create a ServiceAccount for pd | `true` |
| `pd.serviceAccount.name` | Use an existing ServiceAccount instead | `""` |
| `pd.serviceAccount.annotations` | Annotations on the created ServiceAccount | `{}` |
| `pd.serviceAccount.automountServiceAccountToken` | Mount an API token. The chart makes no API calls | `false` |
| `pd.pdb.enabled` | Create a PodDisruptionBudget for PD | `true` |
| `pd.pdb.minAvailable` | Must be strictly less than `pd.replicas`. No PDB is rendered when `pd.replicas` is 1 | `2` |
| `pd.probes.*.periodSeconds` | Probe interval | see `values.yaml` |
| `pd.probes.*.failureThreshold` | Probe failure threshold | see `values.yaml` |
| `pd.probes.*.timeoutSeconds` | Probe timeout. Defaults to `5` on readiness/liveness; Kubernetes would otherwise apply `1` | `5` |
| `pd.probes.*.initialDelaySeconds` | Optional probe start delay | unset |
| `pd.probes.*.successThreshold` | Optional probe success threshold | unset |

### Store

| Parameter | Description | Default |
|---|---|---|
| `store.replicas` | Store StatefulSet replicas. Maximum `99` | `3` |
| `store.image.repository` | Store image repository | `hugegraph/store` |
| `store.image.tag` | Store image tag. Tracks the development image until the next release is pinned | `latest` |
| `store.image.pullPolicy` | Store image pull policy | `Always` |
| `store.javaOpts` | Empty preserves the image's automatic JVM sizing | `""` |
| `store.ports.grpc` | Store gRPC port | `8500` |
| `store.ports.raft` | Store Raft port | `8510` |
| `store.ports.rest` | Store REST port | `8520` |
| `store.dataPath` | Store data directory | `/hugegraph-store/storage` |
| `store.storage.size` | Store PersistentVolumeClaim size | `50Gi` |
| `store.storage.storageClassName` | Empty uses the cluster default StorageClass | `""` |
| `store.resources` | Store container resources. Set these for production | `{}` |
| `store.podSecurityContext` | Pod-level securityContext, rendered only when set | `{}` |
| `store.securityContext` | Container-level securityContext; also applied to the PD-quorum init container. Hardened by default; `runAsNonRoot` is not set because the published images run as root | `allowPrivilegeEscalation: false`, `capabilities.drop: [ALL]`, `seccompProfile: RuntimeDefault` |
| `store.waitTimeoutSeconds` | Bound on the PD-quorum wait before the init container fails | `900` |
| `store.antiAffinity` | One of `required`, `preferred`, `disabled` | `required` |
| `store.nodeSelector` | Node selector for store Pods | `{}` |
| `store.tolerations` | Tolerations for store Pods | `[]` |
| `store.affinity` | Raw affinity; overrides `store.antiAffinity` when set | `{}` |
| `store.topologySpreadConstraints` | Topology spread constraints for store Pods | `[]` |
| `store.priorityClassName` | PriorityClass for store Pods | `""` |
| `store.podAnnotations` | Extra annotations on store Pods | `{}` |
| `store.podLabels` | Extra labels on store Pods | `{}` |
| `store.extraEnv` | Extra environment variables for the Store container | `[]` |
| `store.terminationGracePeriodSeconds` | Shutdown grace period | `300` |
| `store.serviceAccount.create` | Create a ServiceAccount for store | `true` |
| `store.serviceAccount.name` | Use an existing ServiceAccount instead | `""` |
| `store.serviceAccount.annotations` | Annotations on the created ServiceAccount | `{}` |
| `store.serviceAccount.automountServiceAccountToken` | Mount an API token. The chart makes no API calls | `false` |
| `store.pdb.enabled` | Create a PodDisruptionBudget for Store | `true` |
| `store.pdb.minAvailable` | Must be strictly less than `store.replicas`. No PDB is rendered when `store.replicas` is 1 | `2` |
| `store.waitImage` | Image for the PD-quorum init container | `curlimages/curl:8.5.0` |
| `store.waitResources` | Resources for the init container | `{}` |
| `store.probes.*` | Same probe keys as PD | see `values.yaml` |

### Server

| Parameter | Description | Default |
|---|---|---|
| `server.replicas` | Server Deployment replicas. Ignored when `server.hpa.enabled` | `3` |
| `server.image.repository` | Server image repository | `hugegraph/server` |
| `server.image.tag` | Server image tag. Tracks the development image until the next release is pinned | `latest` |
| `server.image.pullPolicy` | Server image pull policy | `Always` |
| `server.javaOpts` | Empty preserves the image's automatic JVM sizing | `""` |
| `server.port` | Server REST port, container port, and Service port | `8080` |
| `server.backend` | Storage backend | `hstore` |
| `server.resources` | Server resources. `requests.cpu` is required when HPA is enabled | `{}` |
| `server.podSecurityContext` | Pod-level securityContext, rendered only when set | `{}` |
| `server.securityContext` | Container-level securityContext. Hardened by default; `runAsNonRoot` is not set because the published images run as root | `allowPrivilegeEscalation: false`, `capabilities.drop: [ALL]`, `seccompProfile: RuntimeDefault` |
| `server.pdb.enabled` | Create a PodDisruptionBudget for Server. Off by default: Server holds no quorum | `false` |
| `server.pdb.minAvailable` | Must be strictly less than `server.replicas` | `2` |
| `server.antiAffinity` | One of `required`, `preferred`, `disabled`. Defaults to `preferred` rather than `required` because HPA may scale Server past the node count; set `required` when replicas always stay below it | `preferred` |
| `server.nodeSelector` | Node selector for server Pods | `{}` |
| `server.tolerations` | Tolerations for server Pods | `[]` |
| `server.affinity` | Raw affinity; overrides `server.antiAffinity` when set | `{}` |
| `server.topologySpreadConstraints` | Topology spread constraints for server Pods | `[]` |
| `server.priorityClassName` | PriorityClass for server Pods | `""` |
| `server.podAnnotations` | Extra annotations on server Pods | `{}` |
| `server.podLabels` | Extra labels on server Pods | `{}` |
| `server.extraEnv` | Extra environment variables for the Server container | `[]` |
| `server.terminationGracePeriodSeconds` | Shutdown grace period | `60` |
| `server.serviceAccount.create` | Create a ServiceAccount for server | `true` |
| `server.serviceAccount.name` | Use an existing ServiceAccount instead | `""` |
| `server.serviceAccount.annotations` | Annotations on the created ServiceAccount | `{}` |
| `server.serviceAccount.automountServiceAccountToken` | Mount an API token. The chart makes no API calls | `false` |
| `server.waitImage` | Image for the Helm test hook | `curlimages/curl:8.5.0` |
| `server.restServer.minFreeMemory` | Empty preserves the image default | `""` |
| `server.restServer.batchMaxWriteThreads` | Empty preserves the image default | `""` |
| `server.initStoreEnabled` | Must remain `false` for distributed HStore | `false` |
| `server.auth.enabled` | Enable admin authentication | `false` |
| `server.auth.existingSecret` | Required when auth is enabled; must contain key `password` | `""` |
| `server.ingress.enabled` | Create an Ingress for the Server Service | `false` |
| `server.ingress.className` | IngressClass name | `""` |
| `server.ingress.annotations` | Ingress annotations (cert-manager, nginx, ALB) | `{}` |
| `server.service.type` | Server Service type | `ClusterIP` |
| `server.service.annotations` | Server Service annotations | `{}` |
| `server.ingress.hosts` | Ingress hosts and paths | see `values.yaml` |
| `server.ingress.tls` | Ingress TLS configuration | `[]` |
| `server.hpa.enabled` | Create a HorizontalPodAutoscaler | `false` |
| `server.hpa.minReplicas` | HPA minimum replicas | `3` |
| `server.hpa.maxReplicas` | HPA maximum replicas | `10` |
| `server.hpa.targetCPUUtilizationPercentage` | HPA CPU utilization target | `70` |
| `server.probes.startup.failureThreshold` | Raised automatically so the budget is at least 450s | `90` |
| `server.probes.startup.periodSeconds` | Startup probe interval | `5` |
| `server.probes.*` | Same optional probe keys as PD | see `values.yaml` |

When `server.hpa.enabled` is `true` the Deployment omits `spec.replicas`, so a
Helm upgrade does not overwrite the autoscaler's live replica count. Enabling
utilization-based HPA requires a strictly positive
`server.resources.requests.cpu`.

Specify each parameter with `--set`, or supply a YAML file with `-f`:

```bash
helm install hugegraph ./helm/hugegraph --set server.replicas=5
```

`values.schema.json` and template helpers reject invalid input at render time,
before anything reaches the cluster:

- Unknown keys and wrong types are rejected.
- `server.initStoreEnabled` must remain `false` for a distributed deployment.
- With authentication enabled, `server.auth.existingSecret` must name a Secret
  containing a `password` key. With authentication disabled it must be empty,
  so a configured but inactive Secret reference cannot be overlooked. A missing
  Secret fails when Kubernetes configures the container; an empty `password`
  fails in the Server startup wrapper.
- `server.hpa.minReplicas` must not exceed `maxReplicas`, and enabling
  utilization-based HPA requires a strictly positive
  `server.resources.requests.cpu`.
- `pdb.minAvailable` must be less than the matching `replicas`, so a
  PodDisruptionBudget cannot permanently block node drains.
- `pd.replicas` and `store.replicas` are capped at 99.

## Deep Dive

### Connecting to the Cluster

```bash
kubectl port-forward -n hugegraph svc/hugegraph-server 8080:8080
curl http://127.0.0.1:8080/versions
curl http://127.0.0.1:8080/graphs
```

### Cluster Health

| Component | Port | Purpose |
|------|-------------|---------|
| PD | `8686` | gRPC (Store and Server clients) |
| PD | `8620` | REST / health probes |
| PD | `8610` | Raft |
| Store | `8500` | gRPC |
| Store | `8510` | Raft |
| Store | `8520` | REST / health probes |
| Server | `8080` | Gremlin and REST API |

All ports are configurable through `values.yaml`. Changing `server.port` updates
the listener, container port, and Service together.

---

### Scaling

PD and Store reserve the maximum StatefulSet ordinal in their resource names,
so scaling never renames a PersistentVolumeClaim or shifts a Pod identity.
Both are capped at 99 replicas.

Server scales through `server.replicas`, or by enabling `server.hpa`. With HPA
enabled the Deployment omits `spec.replicas`, so a Helm upgrade does not
overwrite the autoscaler's live replica count.

## Troubleshooting

### Store Pods Stuck in `Init:0/1`

The Store init container waits for PD to reach Raft quorum. Check PD first:

```bash
kubectl get pods -l app.kubernetes.io/component=pd
kubectl logs <store-pod> -c wait-for-pd
```

The wait is bounded by `store.waitTimeoutSeconds` (default 900). On timeout the
init container exits with a message naming the peers it polled, so the failure
appears in `kubectl describe pod` instead of hanging silently.

### PersistentVolumeClaims Stay `Pending`

No default StorageClass, or the provisioner is unhealthy:

```bash
kubectl get sc
kubectl get pvc -l app.kubernetes.io/instance=<release>
kubectl -n <provisioner-namespace> get pods
```

### Server Ready but Queries Fail

The Server readiness probe uses `/versions`, which can report ready before the
graph is fully able to serve index-backed queries. Confirm the graph is live:

```bash
kubectl exec <server-pod> -c server -- curl -s localhost:8080/graphs
```

### Pods OOM Killed or Restarting

The default `values.yaml` sets **no** resource requests or limits and preserves
the image's automatic JVM sizing. Set resources explicitly before production
use; see `values-cluster.yaml`.

```bash
kubectl get pods -o wide
kubectl describe pod <pod> | grep -A5 "Last State"
```

### Release Name Too Long

Helm itself rejects release names longer than 53 characters, before this chart
renders anything:

```
invalid release name ... the length must not be longer than 53
```

Within that limit the chart is safe: resource names reserve their suffix and
StatefulSet ordinal before truncation, so every generated Service and Pod name
stays inside the 63-character DNS label limit, and PD/Store identities do not
shift when replicas change. Use `fullnameOverride` to shorten generated names
independently of the release name.

---

## Limitations

- No TLS, backups, Operator, multi-cluster support, automatic leader transfer,
  or a complete monitoring stack.
- The published images run as root, so `runAsNonRoot` and
  `readOnlyRootFilesystem` are not chart defaults. The container
  `securityContext` does default to `allowPrivilegeEscalation: false`,
  `capabilities.drop: [ALL]`, and `seccompProfile: RuntimeDefault`, which are
  valid for a root image; `podSecurityContext` and `securityContext` are fully
  configurable per component.
- `values-cluster.yaml` is a starting point, not a capacity guarantee.
