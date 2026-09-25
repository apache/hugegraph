# HugeGraph HStore Helm Chart

[Apache HugeGraph](https://hugegraph.apache.org/) - an open source, distributed graph database.

## Documentation

This chart deploys a distributed HugeGraph cluster - PD, Store, and Server - on
Kubernetes. For HugeGraph itself see <https://hugegraph.apache.org/docs/>.

Two docs-site pages accompany this README:
[deploying with Helm](https://hugegraph.apache.org/docs/quickstart/hugegraph/hugegraph-helm/)
and
[operating on Kubernetes](https://hugegraph.apache.org/docs/quickstart/hugegraph/hugegraph-helm-operations/).
The operations page carries the walkthroughs this README links to below.

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
| Hubble | Deployment + optional PVC | Web UI, off by default; enable with `hubble.enabled` |

A distributed HugeGraph cluster has a startup contract that this chart encodes
so operators do not have to:

- **Server does not run `init-store`.** The chart injects
  `HG_SERVER_INIT_STORE_ENABLED=false`; nothing serializes Server replicas,
  so without the gate every replica would initialize the same backend
  concurrently. The chart creates no init Job and does not set
  `HG_SERVER_SKIP_INIT`.
- **Every Server uses PD for graph metadata.** The startup wrapper always writes
  `usePD=true` and the chart-derived `pd.peers` into
  `rest-server.properties`, so all Server replicas share the graph catalog
  through PD. It also registers each Server Pod IP with PD for in-cluster
  discovery (`server.advertiseUrl` replaces that with one shared URL). This is
  required for distributed HStore and does not make a local RocksDB backend
  shared across replicas.
- **Store waits for PD** in an init container before starting: a majority of
  the PD peers must answer `store.waitPath`. The default `/v1/ready` stays 503
  until a raft leader exists, so that majority is a quorum and not merely a set
  of live listeners.
- **One PD REST secret, three readers.** PD checks the Basic-auth password of
  every management call against `auth.secret-key` and refuses to start
  without one. The chart keeps that value in a release-pd-auth Secret
  (or `pd.auth.existingSecret`) and hands it to PD as `HG_PD_AUTH_SECRET_KEY`,
  to the Server storage wait as `PD_AUTH_PASSWORD`, and to Hubble as
  `operations.pd.password`; a `checksum/pd-auth` annotation rolls all three
  when the Secret changes.
- **The Server startup probe allows at least 450 seconds, and the image
  gets the same budget.** The chart sets `HG_SERVER_STARTUP_TIMEOUT_S` to
  the startup probe's budget (`failureThreshold` * `periodSeconds`, 450 s
  by default), so the image's 120-second default cannot self-kill a Server
  that is still starting; a lower probe budget is raised to the floor, and
  raising the probe raises the timeout. The variable is chart-managed;
  change the probe, not `server.extraEnv`.
- **The wrapper writes `auth.admin_pa` from the auth Secret.** With
  `init_store.enabled=false` the admin credential is created on the PD startup
  path from `auth.admin_pa`, which applies only when the admin is first
  created: changing the Secret later does not rotate a live cluster's
  password, and only makes `helm test` disagree with the live credential. To
  rotate, change the password through the Server API
  (`PUT /graphspaces/DEFAULT/auth/users/admin` with `{"user_password": "..."}`),
  set the Secret to the same value, and
  `kubectl -n <namespace> rollout restart deployment/<fullname>-server` so
  every replica's auth cache drops the old password at once (the caches
  otherwise expire per replica over minutes). The Secret value lands in
  `rest-server.properties` (mode 600) and must not contain newlines, carriage
  returns, or backslashes; the wrapper refuses to start if it does. The
  rotation caveats are also on the
  [deployment page](https://hugegraph.apache.org/docs/quickstart/hugegraph/hugegraph-helm/#4-authentication-and-secrets).

## Installing the Chart

Before installing, confirm `kubectl` points at the intended cluster and that
it can provision volumes. The default topology needs 3 PD and 3 Store PVCs,
and PVCs stuck `Pending` for want of a StorageClass are the most common
first-run failure:

```bash
kubectl config current-context
kubectl get nodes
kubectl get storageclass
```

```bash
helm install hugegraph ./helm/hugegraph --namespace hugegraph \
    --create-namespace --wait --timeout 15m
```

This deploys 3 PD + 3 Store + 3 Server, preserves the image's automatic JVM
sizing, and sets no resource requests or limits. Set resources before
production use.

The command examples in this document assume the release is named
`hugegraph`. With a different release name, substitute the release-prefixed
resource names (`kubectl get svc,secret -n <namespace>` lists them):
workloads and Services are named `<fullname>-*` (the release name itself
when it already contains `hugegraph`, `<release>-hugegraph` otherwise, or
`fullnameOverride`), while the kept Secrets always use the release name:
`<release>-admin`, `<release>-auth-token`, and `<release>-pd-auth`.

**Authentication is enabled by default.** The chart creates a kept Secret
named `<release>-admin` (for example `hugegraph-admin`) with a random
password unless `server.auth.admin.existingSecret` points at a pre-created Secret.
To manage the credential yourself, create the Secret before installing and set
`server.auth.admin.existingSecret`. It always takes priority, and the chart does
not overwrite or manage that Secret:

```bash
kubectl -n hugegraph create secret generic my-hugegraph-admin \
  --from-literal=password='CHANGE_ME'
```

Then add `--set-string server.auth.admin.existingSecret=my-hugegraph-admin` to
the install command. The Secret must contain a `password` key with no newlines,
carriage returns, backslashes, or surrounding whitespace (a properties read
trims padding, so a padded Secret creates the account under a different
password than it holds; the schema rejects padding on inline values but
cannot see a bring-your-own Secret). The JWT signing key uses the same shape
under `server.auth.token` (`value`, `existingSecret`, `autoGenerate`), and
its value must be at least 32 bytes.
Read the password and exercise the API:

```bash
PASSWORD="$(kubectl get secret -n hugegraph hugegraph-admin \
  -o jsonpath='{.data.password}' | base64 --decode)"
kubectl port-forward -n hugegraph svc/hugegraph-server 8080:8080
curl --user "admin:${PASSWORD}" http://127.0.0.1:8080/versions
```

**Hubble is not installed by default.** Enable the optional UI after install:

```bash
helm upgrade hugegraph ./helm/hugegraph --namespace hugegraph \
    --reuse-values --set hubble.enabled=true
```

`--reuse-values` keeps the release's existing overrides (presets, images,
resources, Secrets); without it the upgrade rebuilds the release from chart
defaults. Auth is already on, so that single flag is enough. Login uses the same admin
credential from the chart-managed (or BYO) Secret.

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

The component image tags and `appVersion` track `latest` until the next
HugeGraph release tag is published. For production, pin the image tags (or
digests) and switch the component pull policies to `IfNotPresent`.

Verify the release:

```bash
helm test hugegraph --namespace hugegraph
```

### Values Presets

| File | Purpose |
|---|---|
| `values.yaml` | Default 3+3+3 topology with preferred anti-affinity, authentication on, and Hubble off |
| `values-single.yaml` | Single-node 1+1+1 example with authentication on |
| `values-cluster.yaml` | Production 3+3+3 starting point with JVM/resources, PD/Store PDBs, required anti-affinity for PD and Store, and NetworkPolicy on; authentication on, Hubble still opt-in |

`values-cluster.yaml` is a production starting point, not a capacity
guarantee. Recalculate capacity for the graph size, traffic, failure budget,
node topology, and storage class before production use.

### Local Kubernetes (Kind / minikube)

Only needed when there is no cluster yet, or to test locally built images.
Build the three images, load them into the cluster, and override their tags
and pull policies. The override is required, not optional: this chart
defaults `pullPolicy: Always`, so without `Never` the kubelet tries to pull
your local tag from Docker Hub and fails even though the image is loaded.

```bash
kind create cluster --name hg

docker build -f hugegraph-pd/Dockerfile -t hugegraph/pd:local .
docker build -f hugegraph-store/Dockerfile -t hugegraph/store:local .
docker build -f hugegraph-server/Dockerfile-hstore -t hugegraph/server:local .

kind load docker-image hugegraph/pd:local hugegraph/store:local \
    hugegraph/server:local --name hg
# minikube: minikube image load <the same three images>

helm upgrade --install hugegraph ./helm/hugegraph \
    --namespace hugegraph --create-namespace \
    -f helm/hugegraph/values-single.yaml \
    --set pd.image.tag=local --set pd.image.pullPolicy=Never \
    --set store.image.tag=local --set store.image.pullPolicy=Never \
    --set server.image.tag=local --set server.image.pullPolicy=Never
```

Server uses `Dockerfile-hstore` so the image's default backend is HStore.
Skipping the load step fails the Pods with `ErrImageNeverPull`; do not retag
Docker Hub images as `local`.

## Upgrading the Chart

```bash
helm upgrade hugegraph ./helm/hugegraph --namespace hugegraph --reuse-values
```

Any upgrade that changes a Pod template rolls that workload once.

A release created before the exposure gates existed can hit them on its
next upgrade, `--reuse-values` included: a non-ClusterIP `pd.service.type`
now needs `pd.service.allowInsecureExposure=true`, and a TLS-less Server
Ingress needs `server.ingress.allowPlainHttp=true`. The render error names
the value to set.

PD and Store storage sizes live in the StatefulSet `volumeClaimTemplates`,
which Kubernetes forbids changing, so an upgrade with a new size is
rejected in full. To grow storage on a StorageClass that supports volume
expansion: patch each PVC's `spec.resources.requests.storage`, wait for
the resize to finish, recreate the StatefulSet object without touching
Pods (`kubectl delete statefulset <name> --cascade=orphan`), then upgrade
with the matching value.

Two cases are worth knowing about in advance:

- **PD** restarts one pod at a time whenever its Pod template changes, which
  includes adopting the `-Draft.ip-whitelist.enabled=false` setting described
  under Limitations. For a maintenance-window upgrade, set
  `pd.updateStrategy.type=OnDelete` and restart the pods yourself.
- **Store** rolling updates advance on `/v1/health`, which reports the
  listener, not shard recovery: the controller can replace the next Store
  while the previous one is still rejoining its shard groups. For a
  production image roll, set `store.updateStrategy.type=OnDelete` and delete
  Store Pods one at a time, checking between deletions.

  `Up` in PD is not that check: PD marks a Store `Up` at registration,
  before anything is restored, and a stopped Store stays `Up` in every
  shard group until its keep-alive entry expires (300 s on current images).
  Start with the Pod (`kubectl -n <namespace> wait --for=condition=Ready
  pod/<fullname>-store-<ordinal> --timeout=10m`), then check shard
  membership and leadership per group, read from the PD leader:

  ```bash
  # PD leader, then its shard groups (see Disaster Recovery for the port-forward)
  curl -s -u "hg:${PD_SECRET}" http://127.0.0.1:8620/v1/shardGroups | jq '
    .shardGroups[] | {id: (.id // 0),
                      shards: [.shards[] | {storeId, role}],
                      leaders: [.shards[] | select(.role=="Leader")] | length}'
  ```

  Delete the next Store only when the replaced Pod is `Ready`, its Store id
  shows a fresh `lastHeartBeat` in `/v1/stores`, and every group reports its
  full shard count with exactly one `Leader`. Leave a margin after the
  membership check, keep `store.pdb.minAvailable` at `replicas - 1` so an
  accidental second eviction is refused, and treat a group that is short a
  shard or has no leader as a stop. What the membership record does not
  prove, and the closer per-group check on the Store's own REST port, are on
  the
  [operations page](https://hugegraph.apache.org/docs/quickstart/hugegraph/hugegraph-helm-operations/#6-rolling-store-images-safely).
- **Server** rolls once on the first `helm upgrade` after a fresh install,
  when the `checksum/auth` annotation first observes the install-created
  Secrets. Template-only pipelines (`helm template`, GitOps renderers) never
  see live Secrets, so there the annotation is a constant and Secret rotation
  does not roll pods.
- **PD and Hubble** roll once on the first `helm upgrade` after a fresh
  install as well, when the `checksum/pd-auth` annotation first observes the
  install-created PD REST Secret; Store is untouched. A PD roll is a raft
  rolling restart, one pod at a time. Rotating the PD REST Secret later
  rolls PD, Server and Hubble together, which keeps their copies in step.
- **A Server that starts while PD is rolling can come up without its Gremlin
  binding** and then passes readiness and serves REST while every Gremlin
  request on it fails with `Could not rebind [graph]`, for the life of the
  Pod. After an upgrade that rolls PD and Server together, check Gremlin on
  each Server Pod and delete any Pod that fails; the replacement binds
  normally once PD is stable (see Troubleshooting).
- **Dropping an inline credential back to the chart-managed Secret rolls PD,
  Server and Hubble once more, with no credential change.** The rollout
  checksum takes the inline value's digest while `pd.auth.value` or
  `server.auth.token.value` is set, and the Secret's `resourceVersion` when it
  is not, so removing the inline value changes the annotation although the
  credential is unchanged (the chart never hashes Secret data). Expect one
  extra roll on that upgrade.

Every optional field stays optional, so a release created by an earlier
revision continues to render under `--reuse-values`. That flag keeps the old
values as the complete base, so such a release does **not** pick up new
values defaults (the hardened `securityContext`, ServiceAccounts,
`terminationGracePeriodSeconds`); use `-f` with your own values, or
`--reset-then-reuse-values`, to adopt them. Template-derived settings
**are** applied either way, because they are computed at render time from
whatever values are in effect.

Upgrading an existing release to this chart version rolls the PD StatefulSet
once: PD Pods now always carry a `JAVA_OPTS` environment variable with the
chart-derived partition properties, where previous versions set the variable
only when `pd.javaOpts` was non-empty.

The `pd.antiAffinity` and `store.antiAffinity` defaults changed from
`required` to `preferred` in this version. `--reuse-values` keeps the old
effective value, but installs that relied on the old `required` default
while supplying their own values files must now pin `antiAffinity: required`
explicitly.

## Uninstalling the Chart

```bash
helm uninstall hugegraph --namespace hugegraph
```

Helm does not remove PersistentVolumeClaims created by StatefulSets. Delete
them explicitly, and only when the data is no longer needed.

The chart-managed authentication Secret is kept on uninstall and reused by a
later install of the same release name. Do not delete it unless you intend to
manage the password separately. `helm template` and client-side dry runs cannot
read an existing Secret, so the password they generate is only a render-time
placeholder; a live install or upgrade reuses the existing Secret when Helm has
permission to read it.

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
| `pd.image.tag` | PD image tag; pin it (or a digest) for production | `latest` |
| `pd.image.digest` | Optional immutable digest such as `sha256:...`; when set it takes priority over the tag | `""` |
| `pd.image.pullPolicy` | PD image pull policy | `Always` |
| `pd.javaOpts` | Extra JVM flags, rendered after the chart-derived `-D` properties below so an explicit duplicate here wins. The image's automatic heap sizing is preserved unless heap flags are set | `""` |
| `pd.raftIpWhitelistEnabled` | Enable PD's raft peer IP whitelist. Off in-cluster because PD resolves peers once at boot; requires a PD image carrying the upstream switch | `false` |
| `pd.raftRpcTimeoutMs` | Raft RPC timeout (`-Draft.rpc-timeout`). Bounds the wait on a vanished leader, so it bounds leader elections: the image default of 10000 was measured leaderless for about a minute, 3000 elects in seconds. Empty preserves the image default | `3000` |
| `pd.partition.defaultShardCount` | Shard replicas per partition, seeded into PD's persisted config at first bootstrap only; inert on an initialized cluster (see Partition Sharding). Empty derives 3 when `store.replicas` is at least 3, else 1. An explicit value must be odd and must not exceed `store.replicas` | `""` |
| `pd.partition.storeMaxShardCount` | Maximum shards per Store, seeded at first bootstrap only. Also fixes the initial partition count, `store.replicas x storeMaxShardCount / shardCount` (see Partition Sharding). Empty preserves the image default of `12` | `""` |
| `pd.ports.grpc` | PD gRPC port | `8686` |
| `pd.ports.rest` | PD REST port, also used by probes | `8620` |
| `pd.ports.raft` | PD Raft port | `8610` |
| `pd.dataPath` | PD data directory inside the container | `/hugegraph-pd/pd_data` |
| `pd.storage.size` | PD PersistentVolumeClaim size. Applies at install; see Upgrading for the resize procedure | `10Gi` |
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
| `pd.podLabels` | Extra labels on pd Pods. The `app.kubernetes.io/name`, `instance` and `component` keys are chart-managed and rejected | `{}` |
| `pd.extraEnv` | Extra environment variables for the PD container | `[]` |
| `pd.terminationGracePeriodSeconds` | Shutdown grace period | `300` |
| `pd.serviceAccount.create` | Create a ServiceAccount for pd | `true` |
| `pd.serviceAccount.name` | Use an existing ServiceAccount instead | `""` |
| `pd.serviceAccount.annotations` | Annotations on the created ServiceAccount | `{}` |
| `pd.serviceAccount.automountServiceAccountToken` | Mount an API token. The chart makes no API calls | `false` |
| `pd.pdb.enabled` | Create a PodDisruptionBudget for PD | `true` |
| `pd.pdb.minAvailable` | Must be strictly less than `pd.replicas`. No PDB is rendered when `pd.replicas` is 1 | `2` |
| `pd.readinessPath` | Path the PD readinessProbe hits. `/v1/ready` is quorum-aware and returns 503 without a raft leader | `/v1/ready` |
| `pd.livenessPath` | Path the PD startup and liveness probes hit. Empty derives it from `pd.replicas`: `/v1/health` above one replica, `/v1/ready` at one | `""` |
| `pd.auth.value` | Plaintext PD REST secret (`auth.secret-key`). Prefer `existingSecret` in shared clusters. Printable ASCII, no backslashes, no leading or trailing space (a properties read trims it) | `""` |
| `pd.auth.existingSecret` | Pre-created Secret holding the PD REST secret under `pd.auth.key`. Wins over `value` and `autoGenerate`; the chart does not manage it. Its value must meet the same constraint as `pd.auth.value`: printable ASCII, no backslashes, no leading or trailing space | `""` |
| `pd.auth.key` | Key inside the PD REST Secret | `secret-key` |
| `pd.auth.autoGenerate` | Create and keep a random release-pd-auth Secret when `value` and `existingSecret` are empty | `true` |
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
| `store.image.tag` | Store image tag; pin it (or a digest) for production | `latest` |
| `store.image.digest` | Optional immutable digest such as `sha256:...`; when set it takes priority over the tag | `""` |
| `store.image.pullPolicy` | Store image pull policy | `Always` |
| `store.javaOpts` | Empty preserves the image's automatic JVM sizing | `""` |
| `store.ports.grpc` | Store gRPC port | `8500` |
| `store.ports.raft` | Store Raft port | `8510` |
| `store.ports.rest` | Store REST port | `8520` |
| `store.dataPath` | Store data directory | `/hugegraph-store/storage` |
| `store.storage.size` | Store PersistentVolumeClaim size. Applies at install; see Upgrading for the resize procedure | `50Gi` |
| `store.storage.storageClassName` | Empty uses the cluster default StorageClass | `""` |
| `store.resources` | Store container resources. Set these for production | `{}` |
| `store.podSecurityContext` | Pod-level securityContext, rendered only when set | `{}` |
| `store.securityContext` | Container-level securityContext; also applied to the PD wait init container. Hardened by default; `runAsNonRoot` is not set because the published images run as root | `allowPrivilegeEscalation: false`, `capabilities.drop: [ALL]`, `seccompProfile: RuntimeDefault` |
| `store.waitPath` | Path the init container polls on each PD peer; a majority must answer 2xx. `/v1/ready` counts quorum members, not merely live listeners | `/v1/ready` |
| `store.waitTimeoutSeconds` | Bound on the PD wait before the init container fails | `900` |
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
| `store.pdb.minAvailable` | Must be strictly less than `store.replicas` and at least `store.replicas - 1`, so voluntary evictions cannot remove two copies of one shard at once. No PDB is rendered when `store.replicas` is 1 | `2` |
| `store.waitImage` | Image for the PD wait init container | `curlimages/curl:8.5.0` |
| `store.waitResources` | Resources for the init container | `{}` |
| `store.probes.*` | Same probe keys as PD | see `values.yaml` |

### Server

| Parameter | Description | Default |
|---|---|---|
| `server.replicas` | Server Deployment replicas. Ignored when `server.hpa.enabled` | `3` |
| `server.image.repository` | Server image repository | `hugegraph/server` |
| `server.image.tag` | Server image tag; pin it (or a digest) for production | `latest` |
| `server.image.digest` | Optional immutable digest such as `sha256:...`; when set it takes priority over the tag | `""` |
| `server.image.pullPolicy` | Server image pull policy | `Always` |
| `server.javaOpts` | Empty preserves the image's automatic JVM sizing | `""` |
| `server.port` | Server REST port, container port, and Service port | `8080` |
| `server.readinessPath` | Path the Server readinessProbe hits. Set `/readiness` once the Server image serves it (apache/hugegraph#3212); it answers 503 while the Server cannot serve graph traffic. Startup and liveness stay on `/versions` | `/versions` |
| `server.backend` | Storage backend | `hstore` |
| `server.resources` | Server resources. `requests.cpu` is required when HPA is enabled | `{}` |
| `server.podSecurityContext` | Pod-level securityContext, rendered only when set | `{}` |
| `server.securityContext` | Container-level securityContext. Hardened by default; `runAsNonRoot` is not set because the published images run as root | `allowPrivilegeEscalation: false`, `capabilities.drop: [ALL]`, `seccompProfile: RuntimeDefault` |
| `server.pdb.enabled` | Create a PodDisruptionBudget for Server. Off by default: Server holds no quorum. `values-cluster.yaml` enables it so a node drain cannot evict every Server at once | `false` |
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
| `server.testResources` | Resources for the Helm test hook container | requests `25m`/`32Mi`, limits `250m`/`64Mi` |
| `server.initStoreEnabled` | Must remain `false` for distributed HStore | `false` |
| `server.auth.enabled` | Enable admin authentication | `true` |
| `server.auth.admin.password` | Optional inline admin password; prefer a Secret in shared clusters | `""` |
| `server.auth.admin.existingSecret` | Pre-created Secret name (key defaults to `password`); takes priority | `""` |
| `server.auth.admin.key` | Key inside the admin password Secret | `password` |
| `server.auth.admin.autoGenerate` | Create and keep a random release-admin Secret when password and existingSecret are empty | `true` |
| `server.auth.token.value` | Optional inline JWT signing key, minimum 32 bytes; prefer a Secret in shared clusters | `""` |
| `server.auth.token.existingSecret` | Pre-created Secret for the JWT signing key (`auth.token_secret`) | `""` |
| `server.auth.token.key` | Key inside the JWT signing Secret | `token_secret` |
| `server.auth.token.autoGenerate` | Create and keep a random release-auth-token Secret when value and existingSecret are empty | `true` |
| `server.ingress.enabled` | Create an Ingress for the Server Service | `false` |
| `server.ingress.className` | IngressClass name | `""` |
| `server.ingress.annotations` | Ingress annotations (cert-manager, nginx, ALB) | `{}` |
| `server.advertiseUrl` | Absolute Server URL registered with PD (`server.urls_to_pd`). Empty registers each Server Pod IP for in-cluster discovery | `""` |
| `server.service.type` | Server Service type | `ClusterIP` |
| `server.service.annotations` | Server Service annotations | `{}` |
| `server.ingress.hosts` | Ingress hosts and paths | see `values.yaml` |
| `server.ingress.tls` | Ingress TLS configuration. Empty is refused unless `allowPlainHttp` opts in: the Server carries Basic-auth credentials and JWTs | `[]` |
| `server.ingress.allowPlainHttp` | Explicit opt-in to a TLS-less Server Ingress on a trusted network | unset |
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


### Reaching Hubble (pick one path)

Most people should stop at **1**. Use **2** only if Hubble must run outside
the cluster. Use **3** only if that outside Hubble must discover Server through
PD. Store Operations metrics from outside the cluster are out of scope here.

#### 1. In-cluster Hubble (recommended)

Set `hubble.enabled=true` (off by default so API-only clusters stay lean);
the chart wires PD/Server for you. Open the UI with one port-forward, then
open `http://127.0.0.1:8088` and log in with the chart admin password:

```bash
kubectl -n <namespace> port-forward svc/<fullname>-hubble 8088:8088
```

For a shared environment, expose Hubble with `hubble.service.type`
NodePort/LoadBalancer or `hubble.ingress` instead of port-forward.

#### 2 and 3. Outside Hubble (direct Server URL, or PD discovery)

A non-ClusterIP PD Service requires `pd.service.allowInsecureExposure=true`
(PD gRPC has no authentication; restrict who can reach it first), and a set
`server.advertiseUrl` registers that one URL with PD for every discovery
client, an in-cluster Hubble included. The walkthrough for both paths is on
the
[operations page](https://hugegraph.apache.org/docs/quickstart/hugegraph/hugegraph-helm-operations/#9-running-hubble-outside-the-cluster).

| Parameter | Description | Default |
|---|---|---|
| `server.advertiseUrl` | Absolute Server URL registered with PD for discovery clients. Empty registers each Server Pod IP for in-cluster discovery | `""` |
| `pd.service.type` | PD client Service type (`ClusterIP`, `NodePort`, `LoadBalancer`). A non-ClusterIP type requires `pd.service.allowInsecureExposure` | `ClusterIP` |
| `pd.service.allowInsecureExposure` | Acknowledgement that a non-ClusterIP PD Service exposes the unauthenticated gRPC port; restrict reachability by other means first | `false` |
| `pd.service.annotations` | Annotations on the PD client Service | `{}` |
| `pd.service.restNodePort` | Optional fixed NodePort for PD REST; requires NodePort/LoadBalancer | unset |
| `pd.service.grpcNodePort` | Optional fixed NodePort for PD gRPC; requires NodePort/LoadBalancer | unset |

### Hubble (optional UI)

How to open Hubble (in-cluster vs outside) is under
[Reaching Hubble](#reaching-hubble-pick-one-path) above. This section covers
chart wiring and parameters.

`hubble.enabled=true` deploys [HugeGraph Hubble](https://hugegraph.apache.org/docs/quickstart/toolchain/hugegraph-hubble/),
the web UI; login uses the chart admin credential. `hubble.mode` selects
the wiring. The default `pd` mode points Hubble at the PD gRPC peers, the
PD client Service REST port, and the Store REST endpoints, so the cluster
operations view works without manual wiring (the Server's own PD
registration supplies a resolvable Server address); `direct` mode hands
Hubble only `server.direct_url` on the Server client Service, with no PD
discovery and no operations view. Everything else in
`hugegraph-hubble.properties` keeps the image default. The Store metrics
allow-list is computed from `store.replicas` at render time, so scale Store
with `helm upgrade`, not `kubectl scale`, or the list goes stale until the
next upgrade.

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

**Current Hubble images still require `server.auth`**: the UI login
authenticates against the cluster, so the chart refuses to render
`hubble.enabled=true` with `server.auth.enabled=false` unless
`hubble.allowWithoutServerAuth=true` overrides it for images whose login
does not need cluster authentication.

**Hubble serves plain HTTP.** Reach it through a port-forward or an
HTTPS-terminating Ingress, never directly from an untrusted network; an
Ingress without `tls` is rejected unless `hubble.ingress.allowPlainHttp=true`
opts in.

| Parameter | Description | Default |
|---|---|---|
| `hubble.enabled` | Deploy the Hubble UI. Requires `server.auth` (see below) | `false` |
| `hubble.mode` | `pd` discovers the cluster through PD and enables the operations view; `direct` talks to the Server client Service only | `pd` |
| `hubble.allowWithoutServerAuth` | Renders Hubble without `server.auth`, for future images whose login does not require cluster authentication | `false` |
| `hubble.image.repository` | Hubble image repository | `hugegraph/hubble` |
| `hubble.image.tag` | Hubble image tag; pin it (or a digest) for production | `latest` |
| `hubble.image.digest` | Optional immutable digest such as `sha256:...`; when set it takes priority over the tag | `""` |
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
| `hubble.ingress.*` | Same Ingress keys as `server.ingress.*`, including `allowPlainHttp` | `enabled: false` |
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
- With authentication enabled, either `server.auth.admin.existingSecret` must
  name a Secret containing the configured key (default `password`), or
  `server.auth.admin.password` must be set, or `server.auth.admin.autoGenerate`
  must be true. The same shape applies to `server.auth.token` (`existingSecret`
  / `value` / `autoGenerate`). With authentication disabled,
  `admin.existingSecret`, `admin.password`, `token.existingSecret`, and
  `token.value` must be empty, so a configured but inactive Secret reference
  cannot be overlooked. A missing Secret fails when Kubernetes configures the
  container; an empty `password` fails in the Server startup wrapper.
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
- `hubble.image` needs a tag or a digest (the chart `appVersion` tracks the
  Server release, not Hubble), and an Ingress without `tls` is rejected for
  Server and Hubble alike unless the matching `ingress.allowPlainHttp=true`
  opts in.
- `hubble.enabled` without `server.auth.enabled` is rejected unless
  `hubble.allowWithoutServerAuth=true`, and
  `securityContext.readOnlyRootFilesystem=true` is rejected for Server and
  Hubble alike because each one's wrapper rewrites its properties file
  inside the image at startup and the chart mounts no writable volume there.
- `store.pdb.minAvailable` must be at least `store.replicas - 1`, so
  voluntary evictions cannot remove two copies of one shard at once.
- `podLabels` may not override the chart-managed `app.kubernetes.io/name`,
  `instance` or `component` keys on any workload.
- `podAnnotations` may not set keys under the chart-owned `checksum/`
  prefix on any workload: user annotations render after the chart's own
  and the last duplicate key wins, so a fixed value would pin the checksum
  and stop Secret or config rotation from rolling the pods.
- `updateStrategy.rollingUpdate` options are rejected together with
  `updateStrategy.type: OnDelete` for PD and Store: Kubernetes refuses such
  a StatefulSet at apply time, which would otherwise surface mid-upgrade.
- A non-ClusterIP `pd.service.type` requires
  `pd.service.allowInsecureExposure=true`.
- With `networkPolicy.enabled`, exposing PD, Server or Hubble (a NodePort
  or LoadBalancer Service, a Server or Hubble Ingress, or a set
  `server.advertiseUrl`) requires a non-empty
  `networkPolicy.<component>.extraIngress` naming who may connect.
- An upgrade may not shrink `pd.replicas` or `store.replicas` below the
  live StatefulSet; see Scaling for the manual procedure.

### NetworkPolicy

`networkPolicy.enabled` renders one NetworkPolicy per component (PD, Store,
Server, and Hubble when enabled). Each one isolates its own Pods in both
directions and lists the traffic they need, so the policies take effect in any
apply order. It is off in `values.yaml`, because the base values cannot know
who your clients are, and on in `values-cluster.yaml`.

It only works when the cluster's network plugin enforces NetworkPolicy (kind
v0.25 or later, k3s, Calico, Cilium). Other plugins accept the objects and
enforce nothing. To check, run a Pod without chart labels in another
namespace and `curl` the PD client Service on the REST port: it must time out.

With it on, the release admits only its own traffic plus DNS; anything
else, the Ingress controller and NodePort or LoadBalancer clients included,
must be listed in `networkPolicy.<component>.extraIngress`, and exposing
PD, Server or Hubble (or setting `server.advertiseUrl`) with an empty
`extraIngress` fails the render instead of opening the port. The
admitted-traffic matrix, the egress notes, and worked `extraIngress`
examples with the per-plugin NodePort client addresses are on the
[operations page](https://hugegraph.apache.org/docs/quickstart/hugegraph/hugegraph-helm-operations/#5-networkpolicy).

| Parameter | Description | Default |
|---|---|---|
| `networkPolicy.enabled` | Render the policies | `false` |
| `networkPolicy.<pd\|store\|server\|hubble>.extraIngress` | Extra NetworkPolicy ingress rules, appended as written | `[]` |
| `networkPolicy.hubble.extraEgress` | Extra egress rules for Hubble's optional outside endpoints (`es.urls`, `prometheus.url`) | `[]` |

## Deep Dive

### Cluster Health

Component ports are in the configuration tables above; a stalled component
is ended by its liveness probe within about a minute. The connection
commands and the health walkthrough are on the
[operations page](https://hugegraph.apache.org/docs/quickstart/hugegraph/hugegraph-helm-operations/#2-ports-and-health).

### Scheduling

Every component exposes the full set of scheduling controls, and
`antiAffinity` renders the hostname anti-affinity preset described under
Installing. Examples and the preferred-versus-required trade are on the
[operations page](https://hugegraph.apache.org/docs/quickstart/hugegraph/hugegraph-helm-operations/#3-scheduling).

### Partition Sharding

**The shard-count seed applies at first bootstrap only**: changing
`pd.partition.defaultShardCount`, or scaling `store.replicas` across the
derivation boundary, has no effect on an initialized cluster. Change a
running cluster through PD's own config API (odd values only, at most the
live store count), then trigger `GET /v1/task/patrolPartitions`. The
derivation, the initial partition count, and the constraints are on the
[operations page](https://hugegraph.apache.org/docs/quickstart/hugegraph/hugegraph-helm-operations/#4-partition-sharding).

### Disaster Recovery

Recovery is operator-triggered on current builds: PD's own patrol only
marks silent Stores `Offline`, and there is **no automatic
re-replication**. The task endpoints execute locally on the PD that
receives them, and a follower answers with an empty success while doing
nothing, so identify the leader first and port-forward that Pod (the
forward runs in the foreground; use a second terminal for the curls). The
credential is required; PD answers 401 without it:

```bash
kubectl port-forward -n hugegraph svc/hugegraph-pd-client 8620:8620
PD_SECRET="$(kubectl -n hugegraph get secret hugegraph-pd-auth \
  -o jsonpath='{.data.secret-key}' | base64 --decode)"
# Read .data.pdLeader.raftUrl; its host names the leader Pod.
curl -su "hg:${PD_SECRET}" http://127.0.0.1:8620/v1/members
# Stop the Service forward, then forward the leader Pod instead.
kubectl port-forward -n hugegraph pod/<leader-pod> 8620:8620
# Reconcile shard groups and process tombstoned Stores.
curl -u "hg:${PD_SECRET}" http://127.0.0.1:8620/v1/task/patrolPartitions
# Spread Raft leaders, then partition data.
curl -u "hg:${PD_SECRET}" http://127.0.0.1:8620/v1/task/balanceLeaders
curl -u "hg:${PD_SECRET}" http://127.0.0.1:8620/v1/task/balancePartitions
```

Read `/v1/members` again after the tasks: if leadership moved mid-sequence,
the later tasks ran on a follower and did nothing. Wait at least 180 s
before rerunning `balanceLeaders` after a `balancePartitions` call; the
refusal shapes, when to run which task, and telling a real run from a
no-op or a follower answer are on the
[operations page](https://hugegraph.apache.org/docs/quickstart/hugegraph/hugegraph-helm-operations/#7-disaster-recovery).

**Replace a Store Pod, keep its PVC**: the Store id lives in the data path,
and the Pod comes back under the same id. A Store rebuilt with an empty PVC
registers under a **new Store ID** at the unchanged address, and recovers
in place only on images carrying
[apache/hugegraph#3234](https://github.com/apache/hugegraph/pull/3234)
(merged 2026-09-24); on every earlier image, including all published
release images, it does not. On post-#3234 images, retire the old ID on
the PD leader:

```bash
# The old ID is the row at the replaced Pod's address that is not the
# newly registered one.
curl -su "hg:${PD_SECRET}" http://127.0.0.1:8620/v1/stores
curl -u "hg:${PD_SECRET}" -X POST -H 'Content-Type: application/json' \
  -d '{"storeState":"Tombstone"}' http://127.0.0.1:8620/v1/store/<oldId>
curl -u "hg:${PD_SECRET}" http://127.0.0.1:8620/v1/task/patrolPartitions
# Verify: every group at full shard count with one leader, the old ID in
# no group, and the replaced Store answering 200 on :8520/v1/partition/<id>.
curl -u "hg:${PD_SECRET}" -X DELETE http://127.0.0.1:8620/v1/store/<oldId>
```

On earlier images the same retirement runs and does not repair the groups,
and nothing in the health surface shows the loss: treat a genuinely lost
volume there as a degraded cluster and expect to rebuild rather than to
recover in place. Both measurements and the full walkthrough are on the
[operations page](https://hugegraph.apache.org/docs/quickstart/hugegraph/hugegraph-helm-operations/#7-disaster-recovery).

Periodic leader balancing is tracked in
[apache/hugegraph#3135](https://github.com/apache/hugegraph/issues/3135);
disaster-recovery metrics are tracked in
[apache/hugegraph#3136](https://github.com/apache/hugegraph/issues/3136).

---

### Scaling

Server scales through `server.replicas`, or by enabling `server.hpa` (the
Deployment then omits `spec.replicas`, so a Helm upgrade does not overwrite
the autoscaler). PD and Store are capped at 99 replicas. Stage a rollout
with `kubectl scale statefulset <fullname>-store --replicas=0` and scale
back up when ready; the Servers wait, not-ready, until Stores register, and
the next `helm upgrade` restores the values topology.

**Changing PD or Store replicas on a live release is not a values change.**
Raft and shard membership are persisted, and Pods alone do not reconfigure
them; the chart rejects both directions for PD and a shrink for Store by
reading the live StatefulSet (a client-side `--dry-run` does not show the
guard). Treat a PD replica change as unsupported and install the PD count
you intend to keep. The PD membership background and the Store
drain-then-scale procedure (Tombstone the leaving ids, wait for the groups,
then scale) are on the
[operations page](https://hugegraph.apache.org/docs/quickstart/hugegraph/hugegraph-helm-operations/#8-scaling).
Deleting the PVCs of removed ordinals is separate and permanent; do it only
after the groups no longer list the retired ids.

## Troubleshooting

### Store Pods Stuck in `Init:0/1`

The Store init container waits for a majority of PD peers to answer
`store.waitPath`. Check PD first:

```bash
kubectl -n <namespace> get pods -l app.kubernetes.io/component=pd
kubectl -n <namespace> logs <store-pod> -c wait-for-pd
```

The wait is bounded by `store.waitTimeoutSeconds` (default 900). On timeout the
init container exits with a message naming the peers it polled, so the failure
appears in `kubectl describe pod` instead of hanging silently.

### PersistentVolumeClaims Stay `Pending`

No default StorageClass, or the provisioner is unhealthy:

```bash
kubectl get sc
kubectl -n <namespace> get pvc -l app.kubernetes.io/instance=<release>
kubectl -n <provisioner-namespace> get pods
```

### Server Ready but Queries Fail

The Server readiness probe uses `/versions`, which can report ready before the
graph is fully able to serve index-backed queries. Confirm the graph is live
(the server image does not ship `curl`, so probe through a port-forward):

```bash
kubectl port-forward -n hugegraph svc/hugegraph-server 8080:8080
curl -s --user "admin:${PASSWORD}" http://127.0.0.1:8080/graphs
```

### Queries Fail with "Could not rebind" Right After Creating a Graph

Two causes. Right after `CreateGraph`, the creating Server is consistent at
HTTP 200 ([#3138](https://github.com/apache/hugegraph/pull/3138)), but the
other replicas converge independently for a short window, and a Gremlin
query routed to a not-yet-converged replica fails with a 400 such as
`Could not rebind [g]`: retry with backoff, use sticky routing for
create-then-verify flows, or poll `/graphs` on each replica before opening
query traffic (cluster-wide readiness is tracked in
[#3137](https://github.com/apache/hugegraph/issues/3137)).

The second cause does not close on its own: a Server Pod that started while
PD was rolling serves REST and passes readiness while every Gremlin call on
it fails, for the life of the Pod. Its `hugegraph-server.log` names it:

```
Graph [DEFAULT-hugegraph] configured at [...] could not be instantiated and
will not be available in Gremlin Server
```

Check Gremlin on each Server Pod after any upgrade that rolled PD: a
port-forward to the Pod, then `POST /gremlin` with the admin credential read
into `PASSWORD` as in Installing the Chart (authentication is on by default, so
the call answers 401 without it):

```bash
kubectl port-forward -n hugegraph pod/<server-pod> 8080:8080
curl -s --compressed -u "admin:${PASSWORD}" -H 'Content-Type: application/json' \
  -X POST http://127.0.0.1:8080/gremlin \
  -d '{"gremlin":"graph.traversal().V().limit(1).count()","aliases":{"graph":"DEFAULT-hugegraph"}}'
```

A healthy Pod answers with `result.data`; delete a Pod that answers
`Could not rebind`, and its replacement binds normally once PD is stable.
The measurements behind both causes are on the
[operations page](https://hugegraph.apache.org/docs/quickstart/hugegraph/hugegraph-helm-operations/#10-when-gremlin-fails-with-could-not-rebind).

### Pods OOM Killed or Restarting

The default `values.yaml` sets **no** resource requests or limits and preserves
the image's automatic JVM sizing. Set resources explicitly before production
use; see `values-cluster.yaml`.

```bash
kubectl get pods -o wide
kubectl -n <namespace> describe pod <pod> | grep -A5 "Last State"
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

- On images predating
  [apache/hugegraph#3234](https://github.com/apache/hugegraph/pull/3234)
  (merged 2026-09-24, in no release yet), a Store cannot be recovered in
  place after its volume is lost: the replacement keeps the Pod's DNS raft
  address, PD's reallocation adds a peer the raft group already has, the
  group keeps the old Store id, and no health surface reports it. Images
  built from `master` at or after `dbb6663a` repair this through the
  documented retirement; see Disaster Recovery for both measurements.
- A Server Pod that starts while PD is rolling can lose its Gremlin binding
  for the life of the Pod while passing readiness and serving REST; delete
  that Pod. See Troubleshooting, "Could not rebind".
- The default values set no container resources, so every pod is
  BestEffort and each JVM sizes its heap against total node memory. Fine on
  a single node; on a multi-node cluster the heaps oversubscribe the nodes
  and pods abort. Use `values-cluster.yaml`, or set your own `resources`,
  for any multi-node deployment.
- The Store's memory ceiling is not its heap: the image pins
  `rocksdb.total_memory_size` at 32 GB of native caches outside the JVM,
  and the chart cannot lower it (the entrypoint rebuilds its Spring config
  and no file is mounted). The cluster preset requests 5Gi and limits at
  8Gi per Store for this reason; a 4Gi limit was OOM-killed after about
  1 GB of data. Scale both numbers with the data size; the full breakdown
  is in the `values-cluster.yaml` comment.
- PD's raft IP whitelist resolves peer hostnames once at startup, which
  under Kubernetes can block peers whose pod IPs were unpublished at that
  moment or change later. The chart disables the whitelist in-cluster via
  the upstream `raft.ip-whitelist.enabled` switch; enable
  `networkPolicy.enabled` (on in `values-cluster.yaml`) so only PD Pods
  reach the raft port. `pd.raftIpWhitelistEnabled=true` restores the image
  default and its one-shot resolution races at the operator's own risk.
- PD's `/v1/health` answers 200 as soon as the REST listener is up and
  cannot see a lost quorum. With more than one PD the chart uses it for
  startup and liveness on purpose (a follower that merely lost its leader
  is not restarted) and puts readiness and the Store wait on `/v1/ready`.
  A single PD derives startup and liveness to `/v1/ready` instead: one
  that steps down for good, as after a failed raft snapshot on a full disk
  ([apache/hugegraph#3222](https://github.com/apache/hugegraph/issues/3222)),
  would answer `/v1/health` forever; `pd.livenessPath` overrides.
- Server discovery is a lease: a replaced Server can stay in PD's list for
  up to 45 seconds after it stops, so Hubble's cluster view may briefly
  show a stale address. Application traffic is unaffected, because it
  reaches Servers through the Service, which drops the Pod immediately.
- No TLS, backups, Operator, multi-cluster support, automatic leader
  transfer, or a complete monitoring stack. Store recovery is manual on
  current builds (see Disaster Recovery); periodic balancing and shard-sync
  metrics are upstream feature work.
- After [#3138](https://github.com/apache/hugegraph/pull/3138), the creating
  Server is consistent at HTTP 200; other replicas may still lag for a short
  window (see Troubleshooting: "Could not rebind";
  [#3137](https://github.com/apache/hugegraph/issues/3137) stays open).
- The published images run as root, so `runAsNonRoot` and
  `readOnlyRootFilesystem` are not chart defaults; the container
  `securityContext` is still hardened (`allowPrivilegeEscalation: false`,
  `capabilities.drop: [ALL]`, `seccompProfile: RuntimeDefault`) and fully
  configurable per component.
- `values-cluster.yaml` is a starting point, not a capacity guarantee.
- Authentication is on by default. The auth Secret sets the admin password
  only at first creation via `auth.admin_pa`; the chart cannot rotate an
  existing cluster's admin password.
- Every Server replica must share one JWT signing key. The chart injects
  `HG_SERVER_AUTH_TOKEN_SECRET` from `server.auth.token`
  (chart-managed by default) so Hubble login stays stable behind a
  multi-replica Service.
- Hubble is single-replica, serves plain HTTP, and requires `server.auth`
  for its login to complete; the persistence and H2 constraints are in the
  Hubble section above.
