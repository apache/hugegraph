# Helm operations

Start with [installation and common settings](../README.md).
[values.yaml](../values.yaml) documents every available parameter; this page
covers settings that need coordination or diagnosis.

Commands below use the local chart from the repository root. For a published
Chart, replace `./helm/hugegraph` with
`oci://ghcr.io/hugegraph/hugegraph-helm --version <published-version>`.
Keep the installed version for configuration-only changes; choose and review a
new version for upgrades. See [version selection](distribution.md#choose-a-version-and-registry).

## Credentials

Normal Helm installations generate/reuse three kept Secrets:

| Default Secret | Data key | Readers |
|---|---|---|
| `<release>-admin` | `password` | Server, Helm test |
| `<release>-auth-token` | `token_secret` | All Server replicas |
| `<release>-pd-auth` | `secret-key` | PD, Server storage wait, Hubble |

`existingSecret` takes priority over an inline value, which takes priority over
auto-generation. Existing Secrets are not managed by the chart. Override their
key names through the corresponding `key` fields.

The admin password must be printable ASCII without spaces, colons or
backslashes. The PD REST secret must be printable ASCII without surrounding
whitespace or backslashes. JWT signing keys must be at least 32 bytes.
These constraints match the current image's authentication/property encoding.

The admin Secret supplies the **initial** password when the account is created.
To rotate an existing account, first call the Server API with the old credential:

```bash
curl --user 'admin:<old-password>' \
  -X PUT -H 'Content-Type: application/json' \
  -d '{"user_password":"<new-password>"}' \
  "http://<server-address>/graphspaces/DEFAULT/auth/users/admin"
```

Do not replace password placeholders with real credentials in recorded shell
commands: they can enter history or briefly appear in process arguments.
For interactive calls, `curl --user admin` (or `--user hg` for PD) prompts for
the password. Variable-based examples avoid literal passwords in shell history.

<details>
<summary>Optional credential files for shared terminals or repeated requests</summary>

Create a curl config and password-update JSON with an editor or secret manager,
using owner-only permissions (`600`). The config contains
`user = "admin:<old-password>"`; the JSON contains the `user_password` body above.
Then use `--config /path/to/server.curl` and
`--data-binary @/path/to/password-update.json` instead of inline credentials.
For PD commands, use a separate config containing `user = "hg:<pd-rest-password>"`
and replace `--user "hg:${PD_SECRET}"` with `--config /path/to/pd.curl`.
Do not log or commit these files; remove them when no longer needed.

</details>

Update the Secret and its source values to the same new password, then restart
all Server replicas to clear cached credentials:

```bash
kubectl -n hugegraph rollout restart deployment/hugegraph-server
```

Use your secret manager rather than storing real passwords in public files.
GitOps needs fixed encrypted values or external Secrets, because offline
renderers cannot reuse a live generated Secret through `lookup`.
[Both complete examples](../examples/README.md#gitops-credentials) include all
three credential sources. Changing PD credentials rolls PD, Server and Hubble;
verify Gremlin and UI login after that coordinated rollout.

### PD REST authentication

PD REST uses HTTP Basic auth with a service username: `hg`, `store`, `hubble`
or `vermeer`. Server's `admin` account is not a PD REST account and receives 401.
All four service names use the shared PD REST password, not the admin password.
For the default release/Secret, query a reachable PD REST address (port 8620):

```bash
PD_SECRET=$(kubectl -n hugegraph get secret hugegraph-pd-auth \
  -o jsonpath='{.data.secret-key}' | base64 --decode)
curl --user "hg:${PD_SECRET}" "http://<pd-rest-address>:8620/v1/stores"
```

With `pd.auth.existingSecret` or a custom `pd.auth.key`, read the actual Secret
and key instead. See the [PD REST API reference](../../../hugegraph-pd/docs/api-reference.md#rest-api)
for probe exceptions and API details. Management tasks below must target the PD leader.

## Storage and upgrades

Inspect saved values before upgrading. `--reuse-values` preserves old effective
settings, including disabled Hubble, ClusterIP and explicit helper images.
Adopt new defaults only with reviewed values and `--reset-values`.

PD, Store and Server images built from this version run as UID/GID `10001`. PD
and Store need `podSecurityContext.fsGroup: 10001` to write PVCs created by older
root images; the chart applies it unless `fsGroup` is set, including under
`--reuse-values` (see [Upgrade and verify](../README.md#upgrade-and-verify)). With
`fsGroupChangePolicy: OnRootMismatch` the kubelet fixes ownership only when the
volume root does not match, so switch to the `10001` images in the same rollout
that first applies `fsGroup`. If the release ran a root image after `fsGroup`
was already set, files that image created can stay unwritable: roll out the
first `10001` image with `fsGroupChangePolicy: Always` (update or clear a reused
`image.digest`, which wins over `image.tag`), then set it back to
`OnRootMismatch` explicitly, because `--reuse-values` keeps it (see
[Upgrade and verify](../README.md#upgrade-and-verify)), or change the owner once
with `chown -R 10001:10001`. Volumes
whose CSI driver ignores `fsGroup` (for example many NFS drivers) and
`hostPath` volumes need that `chown` too.
Restricted Pod Security also needs `securityContext.runAsNonRoot: true`, which
is safe only with images from this version.

PD/Store PVC size and StorageClass live in StatefulSet claim templates and are
not routine mutable settings. For capacity growth, the StorageClass must support
expansion: resize each PVC, wait for completion, then coordinate an orphaned
StatefulSet recreation with matching claim-template values. Preserve Pod/claim
names and persisted data; do not recreate claims as a resize workaround.

PD/Store storage-mode changes are rejected after initialization. Hubble uses
an embedded H2 database with connection metadata and saved graph credentials;
a PVC and `Recreate` deployment strategy preserve it across Pod replacement.
Disabling Hubble or uninstalling does not delete kept claims/Secrets automatically.
Changing Hubble storage requires a separate reviewed metadata migration.

An older Hubble Deployment may have no `data` volume: its H2 records live in
its container filesystem. The new persistent default is a storage change too,
so a live Helm upgrade rejects it rather than silently creating an empty database.
Keep `hubble.persistence.enabled=false` with `--reuse-values` or an explicit
GitOps value to preserve the previous storage mode. Move any records you need
to retain through a reviewed metadata migration before enabling persistence;
retaining the temporary mode does not make those records survive Pod replacement.
Export or back up required metadata before any rollout of that old Pod, including
an upgrade that retains `hubble.persistence.enabled=false`.
Offline `helm template` and GitOps rendering cannot inspect the existing Deployment
through `lookup`, so this live guard cannot enforce the migration requirement there.
Export and explicitly pin the existing storage flags before a GitOps sync; a
successful offline render does not establish a safe migration.

Local PVs bind to nodes and cannot move their files automatically on node loss.
See [all seven local PVs](../examples/README.md#storage) for directory, capacity,
claim binding and scheduling requirements. `Retain` preserves files on claim
deletion; reclamation/recovery needs explicit inspection.

## Startup and probes

Store's init container waits for a majority of PD peers on `store.waitPath`
(default `/v1/ready`) for up to `store.waitTimeoutSeconds` (default 900).
PD readiness requires a Raft leader. Images with the fatal-health fix return
`503` from `/v1/health` for terminal Raft errors or shutdown, while elections
and temporary quorum loss remain healthy. Older images only check the listener.
Store's own readiness does not prove every shard has recovered.

Server waits for storage for `server.storageWaitTimeoutSeconds` (default 300),
passed as `WAIT_STORAGE_TIMEOUT_S`. An unspecified startup threshold is derived
from that wait plus a 150-second startup allowance. Explicit thresholds are
preserved and must leave at least 120 seconds beyond the storage wait.
The probe budget is `initialDelaySeconds + (failureThreshold - 1) * periodSeconds`;
without an initial delay, the first probe can run immediately. Contradictory budgets fail rendering with
the relevant parameter names; `HG_SERVER_STARTUP_TIMEOUT_S` is chart-managed.
Legacy reused values retain their earlier probe-budget compatibility behavior.

`store.waitImage` and `server.waitImage` default empty and reuse the Store/Server
images, respectively. Those images already include `/bin/sh` and `curl`.
Explicit helper overrides remain supported for custom images. To clear an older
override without resetting other release settings:

```bash
helm upgrade hugegraph ./helm/hugegraph -n hugegraph --reuse-values \
  --set-string store.waitImage= --set-string server.waitImage=
```

## Topology and scheduling

The startup wrapper sets `usePD=true` and the chart PD peers so all Servers
share graph metadata. Distributed Servers keep `initStoreEnabled=false` and do
not concurrently run `init-store`. Each Server registers its Pod address with
PD unless `server.advertiseUrl` specifies one shared discovery address.

At first bootstrap, the chart seeds three shard replicas with at least three
Stores, or one otherwise. PD persists those settings. Chart changes after
initialization do not rewrite the partition metadata; use the PD configuration
API and coordinate partition patrol for existing groups.

PD Raft addresses, Store Raft addresses and StatefulSet names are persisted
identities. Do not change those hostnames/ports as an ordinary upgrade.
PD membership changes and Store shrink require explicit member/shard operations;
the chart rejects unsupported changes against initialized live StatefulSets.
Fresh installs do not perform the upgrade-only StatefulSet query.

For Store shrink, first confirm the remaining Stores can hold the configured
shard replica count and data. Retire the leaving Store IDs through PD and run
partition patrol on the PD leader. Wait until no shard group references a retired ID and
all remaining groups have their full membership and one leader. Only then
scale the live StatefulSet and upgrade with the matching `store.replicas`;
PVC removal is a separate step after retirement. Keep the installed PD count
unless its persisted voting configuration is reconfigured outside Helm; a
fresh release with data migration is the alternative. The
[PD management API](../../../hugegraph-pd/docs/api-reference.md) documents the
Store state and task endpoints.

Default preferred anti-affinity permits fewer nodes than replicas. The cluster
preset requires separate nodes for PD/Store; explicit affinity overrides presets.
Local PV node affinity must agree with scheduling rules. PDBs protect voluntary
eviction, not controller-driven updates. PD needs a majority available; Store
needs enough shard copies to remain available.

Server replicas can be changed routinely; CPU-utilization HPA requires positive
CPU requests. Store growth still needs volume and shard planning. Use chart
updates for Store scale changes so Hubble's endpoint list is updated too.

## Rolling Store updates and recovery

Use `store.updateStrategy.type=OnDelete` for a controlled rollout. Replace one
Store Pod at a time. Before replacing another, confirm the previous Pod is
Ready, its Store heartbeat is fresh, and every shard group reports full
membership and exactly one leader from the PD leader's view.
PD `Up` status alone does not prove shard recovery; stop on missing members or
leaders. The [website operations guide](https://hugegraph.apache.org/docs/quickstart/hugegraph/hugegraph-helm-operations/)
explains the rollout and recovery constraints.
A single Store replacement can still cause request errors or timeouts while Raft
leadership and client routes converge. Observe application errors as well as
shard recovery before continuing; do not blindly retry writes.

With PD and Store images containing #3229's registration changes, `Up` follows
local engine restore and graceful stops report `Offline`. Use PD's
`heartbeatAgeMillis` to reject stale `Up` records after an abrupt stop.
Temporary shutdown reports preserve the existing keepalive window without renewing
it; quick restarts do not trip the initial-store-count gate, while expiry still
applies if the Store does not return. Stream reconnects do not repeat local restore.
While recovery runs, Offline registrations renew the returned Store's lease.
The dedicated shutdown RPC falls back to normal expiry on older PDs without
using the administrative Offline path, so a PD rollback does not remove that lease.
These signals do not prove Raft catch-up.
<!-- TODO: replace the manual barrier only when Store also exposes per-group
     Raft catch-up readiness; local restore and heartbeat freshness are insufficient. -->
The shutdown report succeeds only if PD accepts a privileged internal credential:
set `HG_PD_AUTH_SECRET_KEY` in Store to match PD's `auth.secret-key`. Without it,
or if the RPC fails, shutdown continues and the existing lease expires normally.
Restoring Offline Stores retain their liveness lease but cannot receive new
placements until `Up`; recovery retries back off to the heartbeat interval.


### PD patrol and leader balance

Partition patrol repairs shard membership; it does not balance shard leaders.
After a Store replacement, leader counts can remain uneven because leader
balancing is not scheduled automatically. Use the existing manual task when
all Stores have fresh heartbeats and every group has full, healthy membership.

Find the PD leader through authenticated `/v1/members` (`data.pdLeader.restUrl`).
Forward that Pod in a separate terminal, not a randomly selected Service peer:

```bash
kubectl -n hugegraph port-forward pod/<pd-leader-pod> 8620:8620
```

In the terminal holding `PD_SECRET` from the credentials section, check that
`/v1/ready` reports `isLeader: true`, then inspect the current group and Store state:

```bash
PD_REST=http://127.0.0.1:8620
curl "${PD_REST}/v1/ready"
curl --user "hg:${PD_SECRET}" "${PD_REST}/v1/partitions"
curl --user "hg:${PD_SECRET}" "${PD_REST}/v1/stores"
```

After an intentional member retirement or shard-count change, patrol on that
leader if needed; it is not a leader-balancing command:

```bash
curl --user "hg:${PD_SECRET}" "${PD_REST}/v1/task/patrolPartitions"
```

Wait for membership recovery and split/move tasks to finish before requesting
leader balance separately:

```bash
curl --user "hg:${PD_SECRET}" "${PD_REST}/v1/task/balanceLeaders"
```

A busy task returns body `status: 1001`; inspect the result and wait for current
maintenance to finish rather than looping patrol/balance calls. A follower can
return an empty success, and leader transfers are asynchronous: HTTP 200 or a
returned transfer plan does not prove completion. Recheck `/v1/partitions` and
`/v1/stores` for actual leaders, full membership and fresh heartbeats before
continuing maintenance.

Preserve volumes and recorded member identities during recovery. Deleting a
RocksDB lock file or assigning an empty volume does not recover shard data.
The chart does not provide backup or automatic disk-loss recovery; keep backups
outside the data PVs and validate recovery separately.

## Networking and Hubble

Easy mode leaves NetworkPolicy off; strict enables it. Explicit values win.
Policies require a network plugin that enforces them. Generated rules allow
component communication and DNS; admit external clients/controllers with native
`extraIngress`, and external addresses with appropriate `extraEgress`.
Selecting strict mode and then overriding a Service to NodePort/LoadBalancer,
setting `externalIPs`, or enabling Ingress does not automatically admit outside traffic. NOTES names
exposed components with no external ingress rule and points to the matching
`networkPolicy.<component>.extraIngress` setting; template rendering remains valid.
For Hubble PD discovery via an external `server.advertiseUrl`, also configure
Hubble's `extraEgress` for that destination.
Empty external rule lists are valid and can block clients; `{}` is a valid
allow-all rule. Consider NodePort source NAT when choosing source ranges.

Client Services accept native Kubernetes traffic and load-balancer fields under
`pd.service`, `server.service` and `hubble.service`. LoadBalancer-specific fields
need that Service type and a compatible controller. Hostless HTTP Ingress is
supported; install an Ingress controller separately. TLS is optional.

In-chart Hubble is wired to internal PD/Server endpoints. `hubble.mode=direct`
uses only the Server Service and has no PD operations view. External Hubble
needs reachable Server addresses; PD discovery additionally needs reachable PD
gRPC and a suitable `server.advertiseUrl`. That override affects every discovery
client, including in-cluster Hubble. Current Hubble login requires Server auth;
use `hubble.enabled=false` for an unauthenticated API-only deployment.

API response compression follows `Accept-Encoding`. Clients that support gzip
can request it explicitly or use `curl --compressed`; requests with `identity`
or without a compression header receive plain responses. Quality factors `q=0.`
and `q=1.` are equivalent to `q=0` and `q=1`. Explicit gzip exclusion overrides
`*`; a higher `identity` quality selects a plain response.
Older images can still force gzip; use a Server image built from current source.
Updating chart values alone does not change an existing image's behavior.

## Business query assertions

The default `helm test` checks the `server.testGraphAlias` binding (`DEFAULT-hugegraph` by default)
and requires `CREATED` indexes
associated with the vertex/edge labels visible on each Pod. It reads those
indexes by ID, using the same local cache path as queries; it does not refresh
the full index cache. This does not detect missing/stale label associations,
orphaned indexes or incorrect index data, and does not prove range-query results.
Run it after index creation/rebuild jobs finish, with no schema changes during
the test. Like indexed queries, the check rejects every other status, including
`CREATING`, `REBUILDING` and `INVALID`. On HStore, reading an unfinished index can
cache its temporary status on Pods other than the job runner. Later attempts
reuse that cache; the retry window does not repair it. Indexed queries on those
Pods may keep failing after the job succeeds; replace affected Server Pods only
after the jobs finish. This check does not fix cross-Server schema cache consistency.

For a graph with existing test data, provide a read-only Gremlin script that
asserts the expected result. For example, with 40 existing `person` vertices
(ages 0–39), save this as `assertions.groovy`, adapting names/results to your data:

```groovy
def n = graph.traversal().V().hasLabel('person')
             .has('age', org.apache.tinkerpop.gremlin.process.traversal.P.gte(20))
             .count().next()
assert n == 20 : "Expected 20 adults, got ${n}"
```

```bash
helm upgrade hugegraph ./helm/hugegraph -n hugegraph --reuse-values \
  --set-file server.testGremlin=assertions.groovy
helm test hugegraph -n hugegraph --logs --timeout 5m
```

In each attempt, the hook checks binding/index state on every Ready Server Pod,
then runs optional business assertions on Pods where that check succeeds. A failing
request or assertion reports its Pod IP, check stage and response, and fails
the hook if no complete attempt succeeds before its deadline. Use
`kubectl -n hugegraph get pods -o wide` to map IPs to Pod names. Returning a value
or `false` alone is not a failure: use `assert` or throw an exception. Without a
script, the hook explicitly reports business assertions as skipped. The hook
creates no schema/data by default. Custom scripts use the hook's Server credential
(`admin` when authentication is enabled); read-only execution is not enforced,
so supply trusted, read-only assertions. The hook does not guarantee the first request
after startup succeeds; its retries allow transient elections to settle.

## Troubleshooting

| Symptom | First checks |
|---|---|
| PVC Pending | StorageClass/provisioner, matching PVs, PVC and Pod events; initial `WaitForFirstConsumer` Pending can be normal |
| Mount failure | Host directory, node affinity and image UID/GID permissions |
| ImagePullBackOff | Pod events, registry reachability, image tags/digests and `imagePullSecrets` |
| `HG_PD_AUTH_SECRET_KEY` missing | PD Pod Secret reference and key; chart injects the shared PD REST password automatically |
| Server storage wait timeout | Ready PD majority, Store registration, network policies and `server.storageWaitTimeoutSeconds` |
| NodePort/Ingress unreachable | Assigned port, reachable node/controller address, firewall and NetworkPolicy rules |
| REST works but Gremlin reports `Could not rebind` | Run `helm test`. Server images built from source that includes apache/hugegraph#3351 (the fix for #3228) rebind Gremlin when REST loads the graph; if it persists, check the Server log for `could not be bound in Gremlin Server` or `can't be rebound in Gremlin Server`, then replace the failing Server Pod after PD stabilizes and retry |

```bash
kubectl -n hugegraph get pods,pvc,svc
kubectl -n hugegraph describe pod <pod-name>
kubectl -n hugegraph logs <pod-name>
helm test hugegraph -n hugegraph --logs --timeout 5m
```

Never print credential values in diagnostic logs. Ready and `/versions` do not
prove graph binding or fresh UI login; verify graph queries and a new login too.
