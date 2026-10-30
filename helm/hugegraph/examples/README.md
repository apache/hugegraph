# HugeGraph deployment examples

These files are examples, not automatically installed resources. Replace registry,
node names, paths, release/namespace and credentials for your cluster before use.
Commands below are deployment instructions; authoring them does not deploy anything.

## Storage

The default topology needs **seven volumes**: 3 × 10 GiB for PD, 3 × 50 GiB for
Store, and 1 × 1 GiB for Hubble. With a default dynamic StorageClass, ordinary
`helm install` requests them automatically. For a named dynamic StorageClass:

```bash
helm install hugegraph ./helm/hugegraph -n hugegraph --create-namespace \
  --set pd.storage.storageClassName=my-csi \
  --set store.storage.storageClassName=my-csi \
  --set hubble.persistence.storageClassName=my-csi
```

SeaTunnel and Vermeer are disabled in both HugeGraph presets. Enabled Vermeer
adds two volumes in standalone mode, or one Master volume plus one per Worker
in cluster mode. The default three-Worker cluster therefore adds four volumes.
Prepare these separately when using the seven-volume local example below.
See the [Vermeer storage guide](../docs/vermeer.md#storage-and-recovery).

An empty chart `storageClassName` inherits the cluster default; `"-"` renders
an explicit empty Kubernetes StorageClass and disables default-class assignment.
Neither creates a provisioner. PVCs remain Pending without matching PVs.

### Static local volumes

[local-pvs.yaml](local-pvs.yaml) contains a `kubernetes.io/no-provisioner`
StorageClass with `WaitForFirstConsumer` and all seven prebound PVs. It assumes
release/namespace `hugegraph` and hostname labels `worker-1`, `worker-2`, `worker-3`.
Change both `claimRef` and nodes if those names differ. StatefulSet claim names
are `<volume-name>-<fullname>-<component>-<ordinal>`; Hubble uses
`<fullname>-hubble-data`.

On each indicated host, create these directories on disks with sufficient free
space and make them writable by the UID/GID of the chosen images. PD, Store
and Server images built from this version run as `10001:10001`, and the chart's
PD and Store `fsGroup` default makes their directories writable; Hubble runs as
the user of its own image. Existing unrelated data must not be reused.

| Node | Directories | Required capacities |
|---|---|---|
| worker-1 | `/var/lib/hugegraph/pd-0`, `/var/lib/hugegraph/store-0`, `/var/lib/hugegraph/hubble` | 10 GiB + 50 GiB + 1 GiB |
| worker-2 | `/var/lib/hugegraph/pd-1`, `/var/lib/hugegraph/store-1` | 10 GiB + 50 GiB |
| worker-3 | `/var/lib/hugegraph/pd-2`, `/var/lib/hugegraph/store-2` | 10 GiB + 50 GiB |

PV capacity is a binding declaration, not a directory quota; arrange filesystem
capacity separately. Check labels with `kubectl get nodes -L kubernetes.io/hostname`,
then apply the reviewed resources:

```bash
kubectl create namespace hugegraph
kubectl apply -f helm/hugegraph/examples/local-pvs.yaml
helm install hugegraph ./helm/hugegraph -n hugegraph \
  -f helm/hugegraph/examples/values-local-storage.yaml --wait --timeout 15m
```

`WaitForFirstConsumer` may leave a PVC Pending until a Pod can be scheduled;
local PV node affinity and workload affinity/nodeSelector must allow the same
node. Local volumes survive Pod replacement on that node, but cannot move their
files to another node on failure. `Retain` keeps data on claim deletion;
recovery/reuse requires operator inspection.

For a fresh temporary installation, `--set persistence.enabled=false` uses
`emptyDir`. Component `pd.storage.enabled`, `store.storage.enabled` and
`hubble.persistence.enabled` override the global choice. A failed PVC never
falls back to temporary storage. Changing an initialized cluster's storage mode
requires a separate data migration/recovery procedure.
Vermeer Master/Worker storage flags also inherit that global choice; SeaTunnel
checkpoints remain temporary local files rather than PVC-backed recovery storage.

## Internal images

[values-internal-registry.yaml](values-internal-registry.yaml) mirrors the default
four images: `hugegraph/pd:latest`, `hugegraph/store:latest`,
`hugegraph/server:latest`, and `hugegraph/hubble:latest`. Tags follow the chart's
current version policy; pin your own published tags/digests.
Enabled SeaTunnel additionally needs its configured `apache/seatunnel` image;
enabled Vermeer needs a compatible `hugegraph/vermeer` build, reused for both
process roles and token initialization. Mirror every enabled image and explicit
helper override at the verified reference. See [image compatibility](../docs/vermeer.md#image-compatibility)
before selecting Hubble/Vermeer builds.

`global.imageRegistry` prefixes repositories without an explicit registry.
A repository such as `other-registry.example.com/team/pd` overrides that prefix.
Registry credentials remain `imagePullSecrets`.

When Vermeer is enabled, mirror its additional image
`hugegraph/vermeer:latest` or the compatible tag or digest you selected. It is
not part of the four-image default installation above. See
[image compatibility](../docs/vermeer.md#image-compatibility) for the required
capabilities and migration from the former pinned default.

The PD wait init container reuses the Store image and Helm test reuses the
Server image. Both already include `curl`; the chart selects a shell command
rather than the component entrypoint. No extra `curlimages/curl` image or runtime
package installation is needed by default. `store.waitImage` and `server.waitImage`
remain explicit overrides for custom images. Store wait overrides need
`/bin/sh`, `curl`, `tr`, `date` and `sleep`; Server test overrides additionally
need `base64`, `getent`, `awk`, `sort`, `grep` and `tail`. Reused component images
also inherit the component pull policy. Older values may retain a curl image
under `--reuse-values`; clear those two fields to restore inheritance.

```bash
helm install hugegraph ./helm/hugegraph -n hugegraph --create-namespace \
  -f helm/hugegraph/examples/values-internal-registry.yaml --wait --timeout 15m
```

## GitOps credentials

Use [values-gitops-fixed.yaml](values-gitops-fixed.yaml) with encrypted values,
or [values-gitops-existing-secrets.yaml](values-gitops-existing-secrets.yaml) with
a secret controller. Replace sample credentials before use. For the latter,
provision these three Secret data keys before the workloads start:

| Secret | Key | Purpose |
|---|---|---|
| `hugegraph-pd-credentials` | `secret-key` | Shared PD REST Basic-auth password |
| `hugegraph-admin-credentials` | `password` | Initial Server admin password |
| `hugegraph-token-credentials` | `secret` | Shared JWT signing key, at least 32 bytes |

The admin password must be printable ASCII without spaces, colons or backslashes.
PD credentials must be printable ASCII without surrounding whitespace or
backslashes. `existingSecret` takes priority over fixed values and is not managed
by the chart. Ordinary Helm installations generate/reuse credentials; repeated
offline renders cannot look up live generated Secrets and otherwise generate new
random values. Changing the admin Secret alone does not change an already-created
account; follow the [account password and Secret synchronization steps](../docs/operations.md#credentials).

For native administrator credentials, enabled Vermeer accepts
`vermeer.auth.admin.password` or `vermeer.auth.admin.existingSecret/key`;
new installations otherwise generate a kept random alphanumeric password: 6 characters in `easy`, 16 in `strict`. Advanced
GitOps setups also preserve the automatically managed internal service identity
through `vermeer.auth.value` or `vermeer.auth.existingSecret/key`. Optional external
Schema credentials use `vermeer.hugegraphCredentials.existingSecret/key`; the
referenced key contains a username/password JSON object. See
[Vermeer deployment credentials](../docs/vermeer.md#deployment-credentials-and-gitops)
for permissions, generated service tokens and rotation.

Before moving an existing release to a declarative GitOps renderer, export and
pin its effective values; offline rendering cannot apply Helm's `--reuse-values`.
Keep the existing `server.service.type`, `hubble.service.type`, `hubble.enabled`
and storage flags explicitly, as well as fixed/external credentials. Selecting
`mode: strict` keeps inherited Services internal, but also enables NetworkPolicy;
set `networkPolicy.enabled` explicitly to preserve the installed behavior.
A legacy Hubble with no mounted data volume still has metadata in its container
filesystem: keep `hubble.persistence.enabled: false` until a reviewed migration
moves that metadata to a PVC. Export or back up any records you need before
upgrading: keeping the temporary mode does not retain the old container's H2
files when its Pod is replaced. Do not use a default reset as a migration.

## Entry points and UI

[values-http-ingress.yaml](values-http-ingress.yaml) uses an already installed
nginx controller for hostless HTTP Server Ingress. Both modes allow HTTP;
certificates, hostnames and acknowledgement fields are optional.
NodePort needs reachable node IPs and firewall permissions; query the actual port
with `kubectl -n hugegraph get svc`, then use `http://<node-ip>:<port>`.
ClusterIP stays inside the cluster; use a gateway or port-forward for external
access. LoadBalancer needs your infrastructure's controller.

[values-api-only.yaml](values-api-only.yaml) disables Hubble. Keep auth for an
authenticated API; disable `server.auth.enabled` as well for an unauthenticated
API. Current Hubble login still needs Server auth. Strict mode defaults to internal
Services and NetworkPolicy, but explicit parameters override defaults. With
NetworkPolicy enabled, add ingress rules for actual base-service clients.
SeaTunnel/Vermeer HTTP defaults to NodePort in easy mode and ClusterIP in strict
mode; login stays required and peer channels stay internal. Entry commands and
overrides are in the [SeaTunnel](../docs/seatunnel.md) and
[Vermeer](../docs/vermeer.md#optional-native-administration) guides.
