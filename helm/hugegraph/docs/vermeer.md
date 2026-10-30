# Deploy Vermeer with HugeGraph and Hubble

Vermeer is optional and disabled by default, including with `values-single.yaml`
and `values-cluster.yaml`. Enabling it deploys the native Master/Worker engine,
configures Hubble automatically, and provides its authenticated native UI/API.
For normal use, enable Vermeer and sign in to Hubble with the existing HugeGraph
administrator account. Native administration is optional.
It needs no Computer Operator, CRD, etcd or external coordination service.

## Choose a topology

| Setting | Result |
|---|---|
| `values-single.yaml` | HugeGraph: 1 PD + 1 Store + 1 Server, plus Hubble |
| `values-cluster.yaml` | HugeGraph: 3 PD + 3 Store + 3 Server, plus Hubble |
| `vermeer.mode: standalone` | One Pod containing separate Master and Worker containers |
| `vermeer.mode: cluster` | One Master Pod plus `vermeer.worker.replicas` Worker Pods; default 3 |
| `mode: easy` / `strict` | HTTP entry, network policies and generated-password defaults; independent of both topologies |

Both Vermeer modes run the same two native process roles. Standalone co-locates
them for a smaller deployment; cluster mode distributes the Workers. A cluster
still has one Master: adding Workers does not provide Master high availability.
Hubble's native loader requires HStore, PD discovery and `hubble.mode=pd`; the
HugeGraph single-node preset meets these requirements. A RocksDB-only Server
does not provide the same direct Store-loading path.

Server and Store also need the same PD metadata prefix. The shared
`metadataCluster` defaults to the existing Server namespace `hg-test` and wires
both components consistently. Retain the initialized value on upgrade; see
[namespace adoption](operations.md#pd-metadata-namespace) for older custom images.

## Install and enable

Commands run from the HugeGraph repository root. Choose one installation:

```bash
# Small HugeGraph topology, plus one Pod with Master and Worker containers
helm install hugegraph ./helm/hugegraph -n hugegraph --create-namespace \
  -f helm/hugegraph/values-single.yaml \
  --set vermeer.enabled=true --set vermeer.mode=standalone \
  --wait --timeout 20m
```

```bash
# Multi-node HugeGraph topology, plus one Master and three Worker Pods
helm install hugegraph ./helm/hugegraph -n hugegraph --create-namespace \
  -f helm/hugegraph/values-cluster.yaml \
  --set vermeer.enabled=true --set vermeer.mode=cluster \
  --set vermeer.worker.replicas=3 --wait --timeout 20m
```

The two choices are independent: a small HugeGraph deployment can also use
multiple Vermeer Workers. To enable Vermeer on an existing release, preserve its
complete reviewed values and use `helm upgrade` with the same `vermeer` settings.
Neither installation creates a sample graph, loads data or runs an algorithm.

For a release created before Vermeer was added, `--reuse-values` cannot import
the new component defaults. Export its effective values to a private file, review
them, then pass the file without `--reuse-values`:

```bash
umask 077
helm get values hugegraph -n hugegraph --all -o yaml > existing-values.yaml
helm upgrade hugegraph ./helm/hugegraph -n hugegraph \
  -f existing-values.yaml --set vermeer.enabled=true \
  --wait --timeout 20m
```

Resource names honor `fullnameOverride`. With the default release name, the
Master StatefulSet and HTTP Service are `hugegraph-vermeer-master`; its Pod is
`hugegraph-vermeer-master-0`. Cluster Workers use StatefulSet
`hugegraph-vermeer-worker`. Standalone has no separate Worker StatefulSet.

## Use Hubble

Keep Hubble enabled in its default PD mode and sign in using the existing
HugeGraph administrator account. The chart sets `vermeer.url` to the internal
Master Service and provides a service-token file. No address or token entry is
required in Hubble. Select a graph and open batch analysis:

1. Load or reload the selected graph into Vermeer.
2. Choose one of Hubble's nine supported algorithms and submit it.
3. Inspect asynchronous task status, cancel an unfinished task when needed,
   and open its paginated vertex results.
4. When needed, unload the compute copy through Hubble's existing administrator
   API. The current page provides Load/Reload controls, not a separate Unload button.

For example, an authenticated Hubble session can send
`DELETE /api/v1.3/vermeer/DEFAULT-vermeer_demo` with this JSON body:

```json
{"graphspace":"DEFAULT","graph":"vermeer_demo"}
```

This removes the Vermeer copy and retains the HugeGraph source graph. Refresh the
analysis page to see its unloaded state before loading it again.

Results remain queryable through the native task API while their cache exists;
they are not written into HugeGraph vertex properties. A successful historical
task does not guarantee its in-memory results survive a restart. Direct Store
scans bypass Server label permissions, so Hubble restricts this flow to HugeGraph
super administrators. Ordinary graph readers retain their existing Server task
view; the Vermeer service token does not grant them this administrator access.

Setting `hubble.enabled=false` keeps Vermeer available through its native entry.
`hubble.mode=direct` cannot be combined with an enabled native Vermeer integration.

### Cluster health in Hubble

The Hubble **System & Operations → Cluster Overview** keeps Server, PD and Store
as the main topology. Vermeer appears as a compact compute-cluster summary;
select it to switch to the Master-above-Worker view, then use **Return to main
cluster** to restore the primary topology. This view describes the deployment,
not the graph selected elsewhere in Hubble. Both Vermeer deployment modes use
the same view, including standalone's one Master and one Worker.

With `vermeer.enabled=true`, the chart also writes these Hubble properties:

| Property | Chart-managed value |
|---|---|
| `operations.vermeer.enabled` | `true` |
| `operations.vermeer.address` | The release's internal Master HTTP Service origin |
| `operations.vermeer.token_file` | `/var/run/hugegraph/vermeer/token` |

Operations and batch analysis reuse the same generated service token. Hubble
mounts that token read-only; its main container never mounts the signing factor.
No extra password, externally exposed Worker port or Kubernetes API permission
is needed. The existing network policies allow Hubble to reach only the Master
HTTP entry. Disabling Vermeer explicitly clears both integrations' endpoint and
token-file properties and sets `operations.vermeer.enabled=false`, including
when the image contains old defaults.

Cluster health requires a paired Hubble and Vermeer build that supports the
authenticated `GET /api/v1/admin/cluster` snapshot (`schema_version: 1`). The
published Vermeer pin documented below predates that endpoint; retain its
immutable digest until a reviewed replacement has been published, then select
the paired images using `hubble.image` and `vermeer.image` (clear or replace the
old `vermeer.image.digest` when selecting another tag). Do not infer capability
from a tag alone. An older image that returns 404 is shown as unsupported, not
healthy. A failed or unauthorized snapshot is reported as unavailable and does
not turn the main graph cluster into a database outage.

Worker counts are **ready / registered**, not ready / desired Helm replicas.
Registration alone does not establish readiness: the Master checks its reverse
connection and all three task streams. Process CPU is expressed in cores and
memory as RSS bytes; these are not Pod utilization percentages. Missing resource
measurements do not imply zero usage or make an otherwise ready Worker offline.
This deployment-wide snapshot is separate from the space-aware allocation
check at `/api/v1/workers/availability`.

## Optional native administration

Master HTTP defaults to NodePort in easy mode and ClusterIP in strict mode.
For optional native administration in easy mode:

```bash
kubectl -n hugegraph get svc hugegraph-vermeer-master \
  -o jsonpath='{.spec.ports[?(@.name=="http")].nodePort}{"\n"}'
kubectl get nodes -o wide
# Open http://<reachable-node-ip>:<nodePort>/
```

For a ClusterIP entry or when node access is unavailable, forward Master HTTP:

```bash
kubectl -n hugegraph port-forward svc/hugegraph-vermeer-master 6688:6688
```

Open <http://127.0.0.1:6688/> when forwarding. Both entries use the password read by:

```bash
kubectl -n hugegraph get secret hugegraph-vermeer-admin \
  -o jsonpath='{.data.password}' | base64 --decode
```

Paste it into the **Admin credential** input; no username is required. New installations generate a
6-character random alphanumeric password in `easy` or 16 characters in `strict`.
Existing credentials are reused across online upgrades and mode changes. Successful login creates a versioned session cookie valid for
24 hours. This password is separate from the HugeGraph/Hubble login. Hubble's
service authentication is configured automatically and needs no manual token.

Only Master reads the native administrator password. Set
`vermeer.auth.admin.password` through controlled values, or reference an external
Secret using `vermeer.auth.admin.existingSecret/key` (default key `password`).
With neither source set, `vermeer.auth.admin.autoGenerate=true` creates the kept
`<release>-vermeer-admin` Secret. External Secret takes priority over the inline
password and auto-generation. Read the actual Secret/key when overriding them.
Changing an external administrator Secret requires restarting Master; this
password rotation does not change Hubble's internal service token.

Authentication stays enabled. Service overrides and existing values follow the
[shared entry rules](../README.md#modes-and-entry-points).

Only NodePort Master HTTP admits outside callers, including in strict mode.
ClusterIP HTTP, RPC and Worker HTTP remain internal; strict additionally controls
egress. Same-release PD/Store access and Server Schema reads are allowed by
default, without extra ingress/egress rules. These policies need a supporting CNI.
Additional native ingress uses explicit internal Pod/namespace selectors under
`networkPolicy.vermeer.extraIngress`; unrestricted and IP-based ingress rules
are rejected. External data destinations can use `extraEgress` when needed.

## Deployment credentials and GitOps

Both engines retain generated administrator credentials during online upgrades.
For GitOps, provide stable fixed or external credentials through the matching
component settings. Vermeer's human password uses `vermeer.auth.admin`; its
internal service authentication is managed separately.

<details>
<summary>Advanced: service identities, versioned tokens and GitOps</summary>

Hubble service authentication uses a separate signing factor in the kept
`<release>-vermeer-auth` Secret, key `token-factor` by default. Online Helm
upgrades reuse it. Configure `vermeer.auth.existingSecret/key` for an external
Secret, or a controlled `vermeer.auth.value` for reproducible offline rendering;
external Secret takes priority over the fixed value and auto-generation.
Signing factors, including those in external Secrets, require at least 16
characters; prefer a randomly generated value of 32 characters or more.
Never place a real factor in a public values file.

Master reads the factor from its mounted file. Hubble's init container reads the
same factor and writes a long-lived native administrator service token to a
private temporary volume; the Hubble application mounts the token, not the factor.
The bundled `/go/bin/vermeer-token` accepts only `-factor-file` and rejects the
inline `-factor` option. That legacy interface remains only in the source tool
`tools/token/token_generator.go`; its older tokens are rejected by file-backed
deployments.
Worker group `$` provides the common pool, so newly created graph spaces do not
require manual Worker allocation.

The chart also wires both process roles to the release's PD peers and shared
PD credential file. Worker Schema reads default to the existing deployment
Server administrator Secret; no extra account is created. This deployment
identity is separate from the user's Hubble session. To use an existing account
with narrower Schema access, configure:

```yaml
vermeer:
  hugegraphCredentials:
    existingSecret: vermeer-schema-reader
    key: credentials.json
```

That Secret key must contain JSON `{"username":"schema-reader","password":"..."}`.
The account and required Schema-read permissions must already exist for every
graph space and graph it will load. The chart does not reconcile future account
memberships. A configured missing or invalid credential file fails the load.
These credentials do not configure the separate native HugeGraph result writer.

For a custom non-root Hubble image, set an explicit numeric `runAsUser` in
`hubble.securityContext` or `hubble.podSecurityContext`. The token init container
uses that same UID so Hubble can read its private `0600` file. Setting
`runAsNonRoot=true` without a positive UID is rejected before installation.

For GitOps, pin all credential sources before rendering; offline `helm template`
cannot reuse live generated Secrets through `lookup`. Preserve the existing
factor when adopting GitOps. Rotate it deliberately through its source values
or secret manager, then restart Master and Hubble as directed by
Helm NOTES. Workers do not mount this factor and need no factor-related restart.
An external Secret change alone does not roll Pods or regenerate Hubble's token. Server password rotation also needs the existing account/Secret
synchronization procedure in [operations](operations.md#credentials).

Upgrade Master and the token helper together when adopting versioned file-mode
tokens. Keep the factor Secret, recreate Hubble's token init container, and log
in to the native UI again; legacy SHA1 tokens and cookies are not accepted by
file mode.

For trusted internal automation that specifically needs a raw service token,
the native helper can generate one inside Master. This is not required for
Hubble or the native UI password login:

```bash
kubectl -n hugegraph exec hugegraph-vermeer-master-0 -c master -- \
  /go/bin/vermeer-token -factor-file /var/run/vermeer/auth/token-factor \
  -user admin -space '$DEFAULT' -client helm -i -quiet
```

Keep this long-lived token private. Use `Authorization: Bearer <token>` only for
its intended internal API integration.

</details>

<details>
<summary>Advanced: image compatibility and publishing</summary>

### Image compatibility

The Chart pins the officially published `hugegraph/vermeer:helm-30b67d93e7d0`
image to OCI index digest
`sha256:f9b69ef134069b432de1c9964374d7fdbfa53245d1fe7dcbce1e75b82b69dc9e`.
The index provides Linux `amd64` and `arm64` images built from
[source commit 30b67d93e7d0](https://github.com/hugegraph/hugegraph-computer/commit/30b67d93e7d097374341cf46ee6a0703421512b1).
No manual image override is needed for ordinary enablement.

Publishing uses the
[publish latest Vermeer workflow](https://github.com/hugegraph/actions/blob/master/.github/workflows/publish_latest_vermeer_image.yml).
Custom builds must include `/go/bin/vermeer-token`, versioned HMAC-SHA256 file
credentials, native administrator password login and space-aware Worker
availability. Hubble needs its native Vermeer adapter and nine algorithm forms.
A different tag alone does not establish these capabilities.

`vermeer.image.digest` takes priority over the tag; a repository override also
retains that digest. To select another reviewed tag, clear it explicitly:

```bash
helm upgrade hugegraph ./helm/hugegraph -n hugegraph -f my-values.yaml \
  --set-string vermeer.image.tag='<reviewed-tag>' \
  --set-string vermeer.image.digest= --wait --timeout 20m
```

Retain the release's reviewed credentials, storage and metadata namespace in
`my-values.yaml`. A custom immutable build can instead set its own verified
`vermeer.image.digest`. Images inherit `global.imageRegistry` and
`imagePullSecrets`; registry mirrors must preserve the selected index and
required architectures. Master, Worker and token initialization reuse the
Vermeer image, with no runtime package downloads.

</details>

## Storage and recovery

Master and Worker use separate writable data directories, including when they
share one Pod. Each role mounts its own `/go/bin/vermeer_data` directory;
logs use `/go/bin/vermeer_data/vermeer.log`. Never share a Pebble directory
between process roles. Each process also works from that directory, so relative
local output files under `./data/hubble` use its corresponding data volume.
Keeping those files does not recreate the in-memory result-query cache.

| Setting | Default |
|---|---|
| `vermeer.master.storage.enabled` / `worker.storage.enabled` | Inherit `persistence.enabled` |
| `vermeer.master.storage.size` | `10Gi` |
| `vermeer.worker.storage.size` | `20Gi` per Worker |
| `vermeer.master.storage.storageClassName` / `worker.storage.storageClassName` | Cluster default when empty |

Standalone adds two claims: the Master StatefulSet claim and the kept
`<fullname>-vermeer-standalone-worker-data` claim. Cluster adds one Master claim
and one Worker claim per replica. Thus the default HugeGraph installation needs
9 volumes with standalone Vermeer or 11 with its default three-Worker cluster;
the small HugeGraph preset with standalone Vermeer needs 5. The seven-volume
local-PV example covers only the base installation; prepare additional matching
volumes before enabling Vermeer there.

Claims are retained when disabling/uninstalling the engine or scaling Workers
down. Explicitly disabled storage uses `emptyDir`; PVC failures never fall back
to temporary data. Persistent Master metadata, Worker files and task history do
not guarantee restoration of in-memory graphs, results or running tasks. After a
restart, inspect state and reload graphs before resubmitting work as needed.

Before changing topology or Worker count, stop submissions, finish/cancel tasks,
and unload loaded compute graphs. Back up required metadata and retained files.
Switching standalone/cluster does not migrate Worker-local data between the kept
standalone claim and cluster Worker claims. Recheck Worker availability and reload
graphs after the change. Do not promise uninterrupted mode switching, Master
failover or automatic job recovery.

## Verify and troubleshoot

Run `helm test hugegraph -n hugegraph --logs` for the chart's enabled checks, then
validate the actual Hubble flow with a dedicated graph and known expected
algorithm results. Pod Ready and `/healthcheck` alone do not establish usable
Worker allocation. Space-aware `/api/v1/workers/availability` checks whether the
service token's space can obtain Workers.

For queued loads, verify Worker registration, common-pool membership, internal
gRPC reachability, PD discovery, Store endpoints and Server Schema credentials.
If Store reports `no such vertex label` or `no such edge label`, verify that its
`PD_CLUSTER` matches the Server's `HG_SERVER_CLUSTER` and recorded metadata prefix.
For Hubble configuration errors, check matching image capabilities, PD mode,
the native Master origin and the generated token file. For lost result pages
after restart, rerun the graph load/computation; a retained successful task record
does not recreate its result cache. Configure `vermeer.master.resources` and
`worker.resources` for the real graph size and algorithm workspace; disk space
does not replace sufficient Worker memory.

<details>
<summary>Advanced: Worker peer connection timeout</summary>

`grpc_dial_timeout` defaults to `5s`; `VERMEER_GRPC_DIAL_TIMEOUT` overrides it.
The value must be a positive Go duration such as `500ms` or `10s`. It bounds
blocking Worker peer dialing so an unreachable bootstrap peer fails within a
limited time and Kubernetes can retry. It does not limit established streams
or algorithm execution. If troubleshooting needs another budget, set the
environment override on the affected role through `vermeer.master.extraEnv`
or `vermeer.worker.extraEnv`.

</details>
