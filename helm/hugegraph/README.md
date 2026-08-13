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
- **The wrapper writes `auth.admin_pa` from the auth Secret.** With
  `init_store.enabled=false` the admin credential is created on the PD startup
  path from `auth.admin_pa`, not from the Docker `PASSWORD` stdin path. When
  authentication is enabled, the chart's wrapper therefore writes
  `auth.admin_pa` from the mounted Secret alongside `usePD=true` and
  `pd.peers`, then hands off to the image entrypoint. Two caveats:
  `auth.admin_pa` applies only when the admin is first created, so changing the
  Secret does not rotate an existing cluster's password, and the value lands in
  `rest-server.properties` inside the container (file mode 600). Because the
  Java properties parser reinterprets them, the Secret value must not contain
  newlines, carriage returns, or backslashes; the wrapper refuses to start if
  it does.
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

The default anti-affinity for `pd`, `store`, and `server` is `preferred`
(Server always was; Hubble has no anti-affinity knob because it is
single-replica by design), so the chart schedules even on clusters with
fewer nodes than replicas. Production should pin `pd.antiAffinity` and
`store.antiAffinity` to `required`, as `values-cluster.yaml` does, so one
node failure cannot take out the PD quorum or co-locate shard replicas; see
Scheduling below.

A fresh install seeds PD with a partition shard count of 3 when
`store.replicas` is at least 3, and 1 otherwise, instead of the image
default of 1. The seed applies at first bootstrap only; see Partition
Sharding below.

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
| `values.yaml` | Default 3+3+3 topology with preferred anti-affinity, so it schedules on clusters of any node count |
| `values-single.yaml` | Single-node 1+1+1 example |
| `values-cluster.yaml` | Production 3+3+3 starting point with JVM/resources, PD/Store PDBs, and required anti-affinity for PD and Store; Hubble stays opt-in because the preset does not enable authentication |

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
before a field existed does **not** pick up its new default, including the
hardened `securityContext`, ServiceAccounts, and `terminationGracePeriodSeconds`.
Use `-f` with your own values, or `--reset-then-reuse-values`, to adopt them.
That rule covers values-sourced defaults only; the asymmetry is that
template-derived settings **are** applied even under `--reuse-values`,
because they are computed at render time from whatever values are in effect.
Pod-level token mounting (disabled unconditionally) and the derived
`-Dpartition.default-shard-count` in the PD `JAVA_OPTS` are the current
cases. On an already-initialized cluster the seeded shard count is inert
either way; see Partition Sharding.

Upgrading an existing release to this chart version rolls the PD StatefulSet
once: PD Pods now always carry a `JAVA_OPTS` environment variable with the
chart-derived partition properties, where previous versions set the variable
only when `pd.javaOpts` was non-empty.

The `pd.antiAffinity` and `store.antiAffinity` defaults changed from
`required` to `preferred` in this version. `--reuse-values` keeps the old
effective value, but installs that relied on the old `required` default
while supplying their own values files must now pin `antiAffinity: required`
explicitly.

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
| `pd.javaOpts` | Extra JVM flags, rendered after the chart-derived `-D` properties below so an explicit duplicate here wins. The image's automatic heap sizing is preserved unless heap flags are set | `""` |
| `pd.partition.defaultShardCount` | Shard replicas per partition, seeded into PD's persisted config at first bootstrap only; inert on an initialized cluster (see Partition Sharding). Empty derives 3 when `store.replicas` is at least 3, else 1. An explicit value must be odd and must not exceed `store.replicas` | `""` |
| `pd.partition.storeMaxShardCount` | Maximum shards per Store, seeded at first bootstrap only. Also fixes the initial partition count, `store.replicas x storeMaxShardCount / shardCount` (see Partition Sharding). Empty preserves the image default of `12` | `""` |
| `pd.ports.grpc` | PD gRPC port | `8686` |
| `pd.ports.rest` | PD REST port, also used by probes | `8620` |
| `pd.ports.raft` | PD Raft port | `8610` |
| `pd.dataPath` | PD data directory inside the container | `/hugegraph-pd/pd_data` |
| `pd.storage.size` | PD PersistentVolumeClaim size | `10Gi` |
| `pd.storage.storageClassName` | Empty uses the cluster default StorageClass | `""` |
| `pd.resources` | PD container resources. Set these for production | `{}` |
| `pd.podSecurityContext` | Pod-level securityContext, rendered only when set | `{}` |
| `pd.securityContext` | Container-level securityContext. Hardened by default; `runAsNonRoot` is not set because the published images run as root | `allowPrivilegeEscalation: false`, `capabilities.drop: [ALL]`, `seccompProfile: RuntimeDefault` |
| `pd.antiAffinity` | One of `required`, `preferred`, `disabled`. `preferred` schedules on clusters with fewer nodes than replicas; production should use `required` so one node failure cannot take out the PD quorum | `preferred` |
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
| `store.antiAffinity` | One of `required`, `preferred`, `disabled`. `preferred` schedules on clusters with fewer nodes than replicas; production should use `required` so one node failure cannot co-locate shard replicas | `preferred` |
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
| `server.pdb.minAvailable` | Must be less than `server.hpa.minReplicas` when HPA is enabled, otherwise less than `server.replicas` | `2` |
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
| `server.testResources` | Resources for the Helm test hook container | `{}` |
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

### Hubble (optional UI)

Set `hubble.enabled=true` to deploy [HugeGraph Hubble](https://hugegraph.apache.org/docs/quickstart/toolchain/hugegraph-hubble/),
the web UI for graph management, schema browsing, Gremlin queries, and the
cluster operations view. `hubble.mode` selects the wiring. In the default
`pd` mode the chart points `pd.peers` at the PD gRPC peers, `pd.server` at
the PD client Service REST port, and the Store metrics allow-list at the
Store REST endpoints, so the cluster view works without manual wiring; the
Server is additionally configured to register its client Service URL with PD
(see below). In `direct` mode Hubble only receives `server.direct_url`
pointing at the Server client Service; there is no PD discovery and no
operations view. Everything else in `hugegraph-hubble.properties` keeps the
image default.

Enabling `pd`-mode Hubble switches the Server into PD meta mode (`usePD`,
`server.urls_to_pd`, `server.deploy_in_k8s`) so that PD can hand Hubble a
resolvable Server address; auth-enabled installs already run in this mode.
On an existing release this change rolls the Server Deployment once. The
Store allow-list is computed from `store.replicas` at render time, so scale
Store with `helm upgrade`, not `kubectl scale`, or the list goes stale until
the next upgrade.

Hubble is one replica by design: it keeps UI connection metadata, including
any graph credentials entered in the UI, in an embedded per-instance H2
database. Enable `hubble.persistence` to keep that metadata across Pod
replacement; the chart then redirects the H2 location into the mounted
volume through Spring's environment binding. The Deployment uses the
`Recreate` strategy so two Hubble instances never attach the same database.
The PVC is kept on `helm uninstall` (delete it explicitly to discard the
stored metadata), `size` and `storageClassName` apply at install time only,
and a non-root `podSecurityContext` needs a matching `fsGroup` so H2 can
write the volume.

**Enable `server.auth` when using Hubble.** Current Hubble images gate the
UI behind a login that authenticates against the cluster; with server
authentication disabled the login cannot complete (the server rejects
`/auth/login` with "Unconfigured authenticator"), so Hubble is only useful
on an auth-enabled deployment, where the admin credential from
`server.auth.existingSecret` logs in. The chart therefore refuses to render
`hubble.enabled=true` without `server.auth` unless
`hubble.allowWithoutServerAuth=true` explicitly overrides it for images
whose login does not need cluster authentication.

**Hubble serves plain HTTP.** Reach it with `kubectl port-forward` or behind
an HTTPS-terminating Ingress; never expose the port directly to an untrusted
network. An Ingress without `tls` is rejected at render time unless
`hubble.ingress.allowPlainHttp=true` explicitly accepts plain HTTP for a
trusted network.

| Parameter | Description | Default |
|---|---|---|
| `hubble.enabled` | Deploy the Hubble UI. Requires `server.auth` (see below) | `false` |
| `hubble.mode` | `pd` discovers the cluster through PD and enables the operations view; `direct` talks to the Server client Service only | `pd` |
| `hubble.allowWithoutServerAuth` | Renders Hubble without `server.auth`, for future images whose login does not require cluster authentication | `false` |
| `hubble.image.repository` | Hubble image repository | `hugegraph/hubble` |
| `hubble.image.tag` | Hubble image tag. Tracks the development image until the next release is pinned | `latest` |
| `hubble.image.pullPolicy` | Hubble image pull policy | `Always` |
| `hubble.port` | Hubble HTTP port, container port, and Service port | `8088` |
| `hubble.persistence.enabled` | Persist UI connection metadata in a PVC | `false` |
| `hubble.persistence.size` | PVC size | `1Gi` |
| `hubble.persistence.storageClassName` | Empty uses the cluster default StorageClass | `""` |
| `hubble.resources` | Hubble resources | `{}` |
| `hubble.podSecurityContext` | Pod-level securityContext, rendered only when set | `{}` |
| `hubble.securityContext` | Container-level securityContext, hardened like the other components | see `values.yaml` |
| `hubble.service.type` | Hubble Service type | `ClusterIP` |
| `hubble.service.annotations` | Hubble Service annotations | `{}` |
| `hubble.service.nodePort` | Requires a `NodePort` or `LoadBalancer` Service type | unset |
| `hubble.ingress.*` | Same Ingress keys as `server.ingress.*`, plus `allowPlainHttp`, which applies to the Hubble Ingress only | `enabled: false` |
| `hubble.serviceAccount.*` | Same ServiceAccount keys as the other components | `create: true` |
| `hubble.nodeSelector` / `tolerations` / `affinity` / `topologySpreadConstraints` | Scheduling controls | unset |
| `hubble.priorityClassName` | PriorityClass for the hubble Pod | `""` |
| `hubble.podAnnotations` / `hubble.podLabels` | Extra Pod metadata | `{}` |
| `hubble.extraEnv` | Extra environment variables for the hubble container | `[]` |
| `hubble.terminationGracePeriodSeconds` | Shutdown grace period | `30` |
| `hubble.probes.*` | Same probe keys as PD; startup and readiness check `/actuator/health`, liveness is a TCP check | see `values.yaml` |

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
- `pd.pdb.minAvailable` must also be at least the PD Raft majority,
  `floor(replicas/2)+1`, so the budget cannot permit evictions that drop PD
  below quorum. With 2 PD replicas no valid budget exists (the majority is
  the whole membership); disable the PD PDB or use an odd replica count.
- `extraEnv` must not set chart-managed variable names (for example
  `HG_SERVER_INIT_STORE_ENABLED` or the PD/Store identity and topology
  variables): entries render after the chart-owned variables and the last
  duplicate wins, so an override would silently bypass a validated contract.
  `JAVA_OPTS` and `JAVA_OPTIONS` are reserved for the same reason: the
  component start scripts skip automatic heap sizing and drop the chart's
  `JAVA_OPTS` flags entirely when `JAVA_OPTIONS` arrives preset.
- `pd.replicas` and `store.replicas` are capped at 99.
- `pd.partition.defaultShardCount` and `pd.partition.storeMaxShardCount`
  must each be empty or a positive integer. An explicit shard count must
  also be odd (PD's config API rejects even values, and PD clamps 2 to 1)
  and must not exceed `store.replicas`, past which PD would silently clamp
  it to the live store count.
- `hubble.port` must be a valid port, `hubble.persistence.size` must be
  non-empty, and `hubble.service.nodePort` requires a `NodePort` or
  `LoadBalancer` Service type.
- `hubble.image.tag` must be non-empty (the chart `appVersion` tracks the
  Server release, not Hubble), and a Hubble Ingress without `tls` is rejected
  unless `hubble.ingress.allowPlainHttp=true`.
- `hubble.enabled` without `server.auth.enabled` is rejected unless
  `hubble.allowWithoutServerAuth=true`, and setting
  `server.ingress.allowPlainHttp` is rejected because the plain-HTTP opt-in
  applies to the Hubble Ingress only.

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

### Scheduling

Every component (`pd`, `store`, `server`, `hubble`) exposes the full set of
scheduling controls: `nodeSelector`, `tolerations`, `affinity`,
`topologySpreadConstraints`, and `priorityClassName`. For example, pinning
Store to labeled nodes is just:

```yaml
store:
  nodeSelector:
    hugegraph/role: storage
```

`antiAffinity` (`required` | `preferred` | `disabled`) renders a hostname
pod-anti-affinity preset for `pd`, `store`, and `server`; Hubble has no
`antiAffinity` key because it is single-replica by design. Setting a raw
`affinity` replaces the preset entirely. All three default to `preferred`
(Server always did; the pd and store defaults changed from `required`), so
the chart schedules on clusters with fewer nodes than replicas (including
single-node development clusters). The trade: `preferred` lets the
scheduler co-locate replicas under node pressure, so a single node failure
can then take more than one PD or Store replica with it. Production
clusters with enough nodes should pin `pd.antiAffinity` and
`store.antiAffinity` to `required`, as `values-cluster.yaml` does.

### Partition Sharding

A fresh install seeds PD's persisted configuration with a partition shard
count of 3 when `store.replicas` is at least 3, and 1 otherwise. Without
this the PD image's `conf/application.yml` would pin
`partition.default-shard-count` to 1, leaving chart-deployed clusters
without store-level HA. The derivation never produces 2 because PD clamps a
shard count of 2 to 1: two shards cannot elect a leader.

The chart renders the setting as `-Dpartition.default-shard-count` in the PD
container's `JAVA_OPTS`; system properties outrank the shipped config file,
and the PD start script appends `JAVA_OPTS` after its automatically computed
heap flags, so the image's JVM auto-sizing is unaffected.

**The seed applies at first bootstrap only.** PD persists the shard count
into its own metadata the first time it starts with empty storage, and from
then on the stored value is authoritative: every PD leader change re-reads
it from storage, overwriting whatever the `-D` flag says. Changing
`pd.partition.defaultShardCount` later, or scaling `store.replicas` across
the derivation boundary, therefore has **no** effect on an initialized
cluster. Nor is the value frozen at partition creation: PD reconciles
existing shard groups toward the stored value whenever a partition patrol
runs. To change the shard count of a running cluster, use PD's own config
API (which accepts only odd values not exceeding the live store count) and
then trigger `GET /v1/task/patrolPartitions`; expect shard-group
reallocation when the counts differ.

The shard count also fixes the initial partition count:
`store.replicas x storeMaxShardCount / shardCount`, computed once at
bootstrap. With the image's `store-max-shard-count` default of 12, the
derived shard count moves a default 3-store install from 36 partitions
(shard count 1) to 12 (shard count 3). Set
`pd.partition.storeMaxShardCount` higher to compensate when more partitions
are wanted; it is likewise seeded at first bootstrap only.

An explicit `pd.partition.defaultShardCount` must be odd and at most
`store.replicas`. The chart rejects other values at render time: PD would
silently clamp a value above the live store count, clamp 2 to 1, and reject
even values at its config API, so an accepted render would not mean an
honored setting.

### Disaster Recovery

What PD automates on current builds is narrow. A scheduled patrol runs on a
hardcoded 60-second cadence and only marks Stores that stopped sending
heartbeats as `Offline`; it does not touch partitions. There is **no
automatic re-replication**: re-placing the replicas of a lost Store,
reconciling shard groups against the stored shard count, and processing
tombstoned Stores all run only when a partition patrol is triggered
explicitly. PD's configuration binds `pd.patrol-interval` and
`store.max-down-time` keys, but no code path on current builds reads
either, which is why this chart does not expose them.

Recovery and rebalancing are operator-triggered. PD exposes REST triggers,
reachable through the PD client Service:

```bash
kubectl port-forward -n hugegraph svc/hugegraph-pd-client 8620:8620
curl http://127.0.0.1:8620/v1/task/patrolPartitions   # reconcile shard groups, process tombstoned Stores
curl http://127.0.0.1:8620/v1/task/balanceLeaders     # spread Raft leaders
curl http://127.0.0.1:8620/v1/task/balancePartitions  # spread partition data
```

Run `patrolPartitions` after replacing a Store that is not coming back,
`balancePartitions` once the cluster is stable again, and `balanceLeaders`
after restarts that skewed leader placement.

Periodic balancing and shard-sync progress metrics do not exist upstream
yet and are out of scope for this chart. Periodic leader balancing is
tracked in
[apache/hugegraph#3135](https://github.com/apache/hugegraph/issues/3135);
disaster-recovery metrics are tracked in
[apache/hugegraph#3136](https://github.com/apache/hugegraph/issues/3136).

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

### Queries Fail with "Could not rebind" Right After Creating a Graph

**Update (2026-08-12):** [#3138](https://github.com/apache/hugegraph/pull/3138)
merged on `master` and closes Phase 1 of
[#3137](https://github.com/apache/hugegraph/issues/3137). The Server that
handles `CreateGraph` now waits for its own Gremlin binding before returning
HTTP 200, so create-then-query on the **same** Server (or sticky routing to
that Pod) is reliable.

Other Server replicas still converge independently through a PD metadata
watch plus a local graph open. Until they finish, a Gremlin query routed
through the load-balanced Service to a not-yet-converged replica can still
fail with a 400 error such as `Could not rebind [g]`. This is upstream
behavior, not a chart setting. Mitigations for multi-replica load-balanced
deployments:

- Retry with backoff in the client; the window normally closes in seconds.
- Use sticky routing (or `kubectl port-forward` to one Pod) for
  create-then-verify flows.
- Poll `/graphs` on each replica until the new graph appears everywhere
  before opening query traffic.

Cluster-wide readiness and PD-owned graph creation remain tracked in
[#3137](https://github.com/apache/hugegraph/issues/3137) (Phase 2:
[#3139](https://github.com/apache/hugegraph/pull/3139); Phase 3: PD
orchestration).

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

```text
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
  or a complete monitoring stack. Store recovery is manual on current builds:
  re-replication after Store loss, leader balancing, and partition
  rebalancing run only when triggered (see Disaster Recovery); periodic
  balancing and shard-sync metrics are upstream feature work.
- After [#3138](https://github.com/apache/hugegraph/pull/3138), the creating
  Server is consistent at HTTP 200; other replicas may still lag for a short
  window on load-balanced installs (see Troubleshooting: "Could not rebind";
  [#3137](https://github.com/apache/hugegraph/issues/3137) stays open for
  cluster-wide and PD-owned creation).
- The published images run as root, so `runAsNonRoot` and
  `readOnlyRootFilesystem` are not chart defaults. The container
  `securityContext` does default to `allowPrivilegeEscalation: false`,
  `capabilities.drop: [ALL]`, and `seccompProfile: RuntimeDefault`, which are
  valid for a root image; `podSecurityContext` and `securityContext` are fully
  configurable per component.
- `values-cluster.yaml` is a starting point, not a capacity guarantee.
- The auth Secret sets the admin password only at first creation via
  `auth.admin_pa`; the chart cannot rotate an existing cluster's admin
  password.
- Hubble is single-replica, serves plain HTTP, requires `server.auth` to be
  enabled for its login to complete, and keeps UI connection metadata,
  including any graph credentials entered in the UI, in an embedded H2
  database that is lost on Pod replacement unless `hubble.persistence` is
  enabled.
