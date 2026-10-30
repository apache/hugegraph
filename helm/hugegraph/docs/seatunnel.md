# Deploy HugeGraph with SeaTunnel

This chart adapts the [SeaTunnel 3.0.0 native hybrid Kubernetes deployment](https://github.com/apache/seatunnel/blob/v3.0.0/docs/en/getting-started/kubernetes/hybrid-cluster-mode.md)
for HugeGraph resource names, DNS, images, configuration and replica counts.
It does not introduce a subchart, operator, CRD or derived SeaTunnel image.

SeaTunnel is disabled by default. When enabled, a long-running hybrid member both
schedules and executes jobs. Cluster mode scales the same StatefulSet to multiple
members; it is not a `local-mode` Pod or Job that exits after one submission.

| Setting | Meaning |
|---|---|
| `values-single.yaml` | HugeGraph 1 PD + 1 Store + 1 Server using HStore |
| `values-cluster.yaml` | HugeGraph's own multi-replica topology |
| `seatunnel.mode: standalone` | One long-running hybrid member |
| `seatunnel.mode: cluster`, `replicas: 2` | Two hybrid members; at least two are required |

Choose HugeGraph and SeaTunnel topologies independently. Top-level `mode: easy/strict`
controls HTTP entry and full NetworkPolicy defaults. Both SeaTunnel modes provide
the same built-in UI and API.

## Install, access and switch modes

From the HugeGraph repository root, choose one installation:

```bash
# HugeGraph 1 PD + 1 Store + 1 Server, plus one long-running SeaTunnel member
helm install hugegraph ./helm/hugegraph -n hugegraph --create-namespace \
  -f helm/hugegraph/values-single.yaml \
  -f helm/hugegraph/examples/values-seatunnel-standalone.yaml \
  --wait --timeout 30m

# The same HugeGraph topology, plus two SeaTunnel hybrid members
helm install hugegraph ./helm/hugegraph -n hugegraph --create-namespace \
  -f helm/hugegraph/values-single.yaml \
  -f helm/hugegraph/examples/values-seatunnel-cluster.yaml \
  --wait --timeout 30m
```

The default image is `apache/seatunnel:3.0.0`. Native HTTP 8080 defaults to
NodePort in easy mode and ClusterIP in strict mode. For easy mode:

```bash
kubectl -n hugegraph get svc hugegraph-seatunnel \
  -o jsonpath='{.spec.ports[?(@.name=="http")].nodePort}{"\n"}'
kubectl get nodes -o wide
# Open http://<reachable-node-ip>:<nodePort>/#/overview
```

For a ClusterIP entry or when node access is unavailable, forward the Service:

```bash
kubectl -n hugegraph port-forward svc/hugegraph-seatunnel 8081:8080
```

Open <http://127.0.0.1:8081/#/overview> when forwarding, with the separate SeaTunnel credentials
described below. The UI shows cluster members, jobs, DAGs and logs. Submit jobs
through the CLI or authenticated REST API. Helm starts the engine and prepares
examples; it does not create graphs or run jobs.

For Service overrides and existing values, see the [shared entry rules](../README.md#modes-and-entry-points).

Only NodePort HTTP admits outside callers, including an explicit NodePort in
strict mode. ClusterIP HTTP and Hazelcast member/CLI 5801 stay restricted to
required release callers in both modes, including when
`networkPolicy.enabled=false`. Strict additionally restricts egress.
Additional internal ingress uses explicit Pod/namespace selectors in
`networkPolicy.seatunnel.extraIngress`; unrestricted and IP-based rules are
rejected. External source/sink destinations can use `extraEgress` when needed.

Actual isolation requires a NetworkPolicy-capable CNI. Pod selectors are not
native Hazelcast authentication: use trusted namespace RBAC/admission controls
for creating or relabeling Pods and for `kubectl exec`. Do not expose 5801 outside
that administrative boundary. The chart does not manage a separate SeaTunnel Ingress.

For release `hugegraph`, resources are the `hugegraph-seatunnel` StatefulSet and
HTTP Service, `hugegraph-seatunnel-headless` discovery Service, and
`hugegraph-seatunnel-configs` ConfigMap. Names honor `fullnameOverride`.
Members use Headless Service DNS rather than the Kubernetes API, so discovery
requires no Kubernetes API token or RBAC permission.

Stop all jobs before switching modes. Retain the complete installation values
and upgrade with:

```bash
helm upgrade hugegraph ./helm/hugegraph -n hugegraph -f my-values.yaml \
  --set seatunnel.enabled=true --set seatunnel.mode=cluster \
  --set seatunnel.replicas=2 --wait --timeout 30m
```

Set `seatunnel.mode=standalone` to return to one member. The StatefulSet stays the
same, but replica count and engine configuration change. Uninterrupted switching,
automatic job recovery and resubmission are not guaranteed. Setting
`seatunnel.enabled=false` removes the engine workloads and Services without
deleting data already written to HugeGraph. The retained auth Secret can be
reused after re-enabling or reinstalling.

## SeaTunnel entry authentication

The UI, static resources and REST submission endpoints use native HTTP Basic
authentication. There is no added proxy or login service. `seatunnel.auth.username`
defaults to `admin`. An empty password creates a random alphanumeric password: 6 characters in `easy`, 16 in `strict`, stored in
`<SeaTunnel-resource-name>-auth` for a new installation. Online upgrades reuse
the existing password, including on mode changes and for longer credentials from older releases.
Authentication stays enabled in both easy and strict mode.

Use a controlled values file to set or rotate `seatunnel.auth.username/password`.
Values cannot contain leading/trailing whitespace or `${...}` placeholders, and
usernames cannot contain a colon. Internal spaces and quotes are accepted.
Pin `seatunnel.auth.password` for offline templates or GitOps rendering; automatic
password reuse requires an online Helm lookup.

The default release uses Secret `hugegraph-seatunnel-auth`. Open the native
UI as `admin` with the password read by:

```bash
kubectl -n hugegraph get secret hugegraph-seatunnel-auth \
  -o jsonpath='{.data.password}' | base64 --decode
```

<details>
<summary>Optional: call authenticated REST</summary>

With port forwarding active, read the credentials into local variables and call REST:

```bash
ST_USERNAME="$(kubectl -n hugegraph get secret hugegraph-seatunnel-auth \
  -o jsonpath='{.data.username}' | base64 --decode)"
ST_PASSWORD="$(kubectl -n hugegraph get secret hugegraph-seatunnel-auth \
  -o jsonpath='{.data.password}' | base64 --decode)"

curl -fsS --user "$ST_USERNAME:$ST_PASSWORD" http://127.0.0.1:8081/overview
```

</details>

Use the same credentials for the native browser Basic login and REST requests
through the selected HTTP entry. REST submissions
follow the [official REST API](https://github.com/apache/seatunnel/blob/v3.0.0/docs/en/engines/zeta/rest-api-and-web-ui.md).

The Secret contains `username`, `password` and the final `seatunnel.yaml`.
The engine configuration is mounted directly from the Secret, not embedded in
a ConfigMap, Java arguments or logs. Rotate through controlled values and
`helm upgrade`, which updates the Secret and rolls the engine Pods. Editing only
`data.password` does not update credentials inside the engine configuration.
`HG_USERNAME/HG_PASSWORD` are separate HugeGraph connector credentials and cannot
replace entry authentication. Disabling `server.auth.enabled` does not disable
SeaTunnel authentication.

HTTP Basic does not authenticate the Hazelcast member/CLI protocol on 5801.
The examples below execute the CLI inside an engine Pod under Kubernetes RBAC;
manual clients use authenticated REST through the HTTP entry.
This is a single administrator entry, not per-user/per-job authorization or
HugeGraph RBAC/SSO. Keep the member/CLI channel inside the cluster.

## Images, credentials and native configuration

The official `apache/seatunnel:3.0.0` image used by this chart includes the
HugeGraph, FakeSource and Console connectors and the built-in UI. Verify plugin
availability when changing the version or image. Configure
`seatunnel.image.repository/tag/pullPolicy`; it inherits `global.imageRegistry`
and `imagePullSecrets`. Helm does not download plugins at startup. The upstream
image contains many connectors, so allow sufficient pull time or pre-pull/mirror
it into an internal registry.

The chart provides these defaults for same-release HugeGraph example jobs:

| Variable | Default source |
|---|---|
| `HG_HOST` | Same-release HugeGraph Server Service DNS |
| `HG_PORT` | `server.port` |
| `HG_PROTOCOL` | `http` for the internal Server connection |
| `HG_USERNAME` | `admin` |
| `HG_PASSWORD` | Actual Server administrator Secret name/key, or empty when auth is disabled |
| `HG_GRAPH_SPACE` | `server.graphSpace` |
| `HG_GRAPH_NAME` | `graph_a` |

`HG_PASSWORD` uses a native `secretKeyRef`, including
`server.auth.admin.existingSecret` and custom keys. It is not copied into example
job files. The chart does not create an extra HugeGraph account. Follow the
Server account synchronization procedure after rotating its credential Secret,
then restart relevant engine Pods to refresh their environment.

A matching name in `seatunnel.env` overrides an automatic value without creating
duplicate entries. For example, select another graph and an existing job account:

```yaml
seatunnel:
  env:
    - name: HG_GRAPH_NAME
      value: import_demo
    - name: HG_USERNAME
      value: seatunnel-job
    - name: HG_PASSWORD
      valueFrom:
        secretKeyRef:
          name: hugegraph-job-credentials
          key: password
```

The account must already exist and have the required data/Schema permissions.
These defaults serve only the local Source/Sink examples. Cross-environment jobs
require the explicit `HG_SOURCE_*` and `HG_TARGET_*` values below; the chart does
not discover remote graphs. Connector credentials do not replace entry auth.

Tune the JVM with `seatunnel.javaOpts` and container resources with
`seatunnel.resources`. Defaults retain G1 and OOM heap dumps, with a 1 GiB maximum
heap and 256 MiB metaspace cap to leave room for native memory under the 2 GiB limit.
Heap dumps use writable `/tmp/seatunnel.hprof` and are ephemeral. `javaOpts` and
`config.jvm_options` each replace the complete image JVM options file; keep the
required GC and diagnostic flags when overriding either. `seatunnel.podSecurityContext` and
`seatunnel.securityContext` follow the other chart components. The container
starts with privilege escalation disabled, all capabilities dropped and
`RuntimeDefault` seccomp. The upstream image runs as root: configure a compatible
UID and writable paths when enforcing `runAsNonRoot` or a restricted Pod Security
namespace; these defaults alone do not establish compatibility with that policy.
Use `seatunnel.nodeSelector`, `tolerations`, `affinity` and `priorityClassName` for
node placement and scheduling.

Override complete native configuration files through `seatunnel.config`, for example:

```yaml
seatunnel:
  config:
    jvm_client_options: |
      -Xms128m
      -Xmx256m
```

The ConfigMap supplies discovery, JVM and example job files. The authenticated
`seatunnel.yaml` is mounted from its separate Secret; other files use the image
defaults. Final HTTP Basic settings remain managed by `seatunnel.auth` even when
overriding engine configuration. Retain member port 5801, HTTP 8080 and
`enable-http: true` so Services, probes and UI remain usable. Checkpoints use an
ephemeral local directory; this integration does not guarantee durable recovery.
Full NetworkPolicy isolation allows peer traffic, DNS and the same-release
Server API by default. See the [Chart README](../README.md) for HugeGraph access,
credentials and storage.

## Same-release examples: import and read

The chart mounts [hugegraph-sink.conf](../examples/seatunnel/hugegraph-sink.conf)
and [hugegraph-source.conf](../examples/seatunnel/hugegraph-source.conf) as
`/opt/seatunnel/config/hugegraph-sink.conf` and
`/opt/seatunnel/config/hugegraph-source.conf`. The Sink writes ten fixed vertices
with label `seatunnel_demo_person`; the Source reads that label into Console.
Both use the automatic `HG_*` environment values. Override a file under the same
name in `seatunnel.config` to customize a job.

Create `graph_a` through Hubble or the HugeGraph API first, or create the graph
selected by explicit `HG_GRAPH_SPACE/HG_GRAPH_NAME` values. The default release,
namespace and GraphSpace are used below. Local `HG_USERNAME/HG_PASSWORD` must
come from your credential manager; Pod environment values do not reach your shell.

```bash
# Keep Server port forwarding active in a separate terminal
kubectl -n hugegraph port-forward svc/hugegraph-server 8082:8080
```

```bash
export HG_URL=http://127.0.0.1:8082
curl -fsS -u "$HG_USERNAME:$HG_PASSWORD" -H 'Content-Type: application/json' \
  -X POST "$HG_URL/graphspaces/DEFAULT/graphs/graph_a" -d '{}'
```

Do not recreate an existing graph. Submit the mounted Sink, then Source:

```bash
kubectl -n hugegraph exec sts/hugegraph-seatunnel -- \
  /opt/seatunnel/bin/seatunnel.sh --master cluster \
  --config /opt/seatunnel/config/hugegraph-sink.conf

kubectl -n hugegraph exec sts/hugegraph-seatunnel -- \
  /opt/seatunnel/bin/seatunnel.sh --master cluster \
  --config /opt/seatunnel/config/hugegraph-source.conf
```

`--master cluster` submits to the long-running engine, including standalone mode.
On an initially empty graph, expect a finished Sink with ten rows read/written,
and a Source with ten rows read into the executing member's log. Inspect UI
status/DAG, all member logs and the actual graph data:

```bash
kubectl -n hugegraph logs -l app.kubernetes.io/instance=hugegraph,app.kubernetes.io/component=seatunnel \
  --all-containers --prefix --tail=200

curl -fsS -u "$HG_USERNAME:$HG_PASSWORD" -H 'Content-Type: application/json' \
  "$HG_URL/gremlin" -d '{"gremlin":"graph.traversal().V().hasLabel(\"seatunnel_demo_person\").count()","aliases":{"graph":"DEFAULT-graph_a"}}'
```

The count query should return `result.data: [10]`. A cluster job can execute on
any member, so Console output is not limited to `hugegraph-seatunnel-0`. Neither
example runs automatically during Helm installation or upgrade.

## Cross-environment copy: Schema, vertices and edges

Use an existing source graph A and an empty graph B in another environment.
Copy **Schema, then vertices, then edges**. Sink auto-schema mapping cannot
replace copying the complete source Schema, indexes and IDs.

Create `graph_b` through the native API, then run
[copy-schema.py](../examples/seatunnel/clone/copy-schema.py). It copies property
keys, vertex labels, edge labels and indexes with source Schema IDs while the
target is in RESTORING mode. It does not copy graph data. Use compatible HugeGraph
versions and accounts with administrative permissions; target Schema must be empty.

Only NORMAL edge labels are supported. The script rejects PARENT, SUB or unknown
edge types before contacting the target; inherited Schema needs a separate
migration procedure.

```bash
# Both URLs include GraphSpace and graph; credentials come from your local environment
export HG_SOURCE_GRAPH_URL=https://source.example.com/graphspaces/DEFAULT/graphs/graph_a
export HG_TARGET_GRAPH_URL=https://target.example.com/graphspaces/DEFAULT/graphs/graph_b
python3 helm/hugegraph/examples/seatunnel/clone/copy-schema.py \
  --source "$HG_SOURCE_GRAPH_URL" --target "$HG_TARGET_GRAPH_URL"
```

The script reads `HG_SOURCE_USERNAME/HG_SOURCE_PASSWORD` and
`HG_TARGET_USERNAME/HG_TARGET_PASSWORD`. Configure corresponding credentials in
the engine Pods through Secrets, plus `HG_SOURCE_HOST/PORT/PROTOCOL` and
`HG_TARGET_HOST/PORT/PROTOCOL`. Addresses must be reachable from the engine Pods,
not merely local port-forward addresses.

The four example jobs read `graph_a` and write `graph_b`, copying both vertex
labels before either edge label:

```bash
for clone_job in person company works_at knows; do
  kubectl -n hugegraph exec -i sts/hugegraph-seatunnel -- \
    sh -c "cat > /tmp/$clone_job.conf" \
    < "helm/hugegraph/examples/seatunnel/clone/$clone_job.conf"
  kubectl -n hugegraph exec sts/hugegraph-seatunnel -- \
    /opt/seatunnel/bin/seatunnel.sh --master cluster --config "/tmp/$clone_job.conf" \
    || break
done
```

Mappings use `ERROR_WHEN_SCHEMA_NOT_EXIST` and the already copied Schema. Edges
require their endpoint vertices first. See [person](../examples/seatunnel/clone/person.conf),
[company](../examples/seatunnel/clone/company.conf), [works_at](../examples/seatunnel/clone/works_at.conf)
and [knows](../examples/seatunnel/clone/knows.conf). The official Sink requires
explicit `mappings` or `schema_config`; adapt labels, properties and ID strategies
to your graph. These examples do not enumerate or migrate every graph automatically.

The mappings support PRIMARY_KEY and CUSTOMIZE_NUMBER vertex IDs. AUTOMATIC
vertices receive new IDs, so these mappings cannot preserve their edge endpoints
losslessly. Check source ID strategies before migration.

Every Sink mapping sets `timeZone = UTC`, assuming UTC-serialized DATE strings
from the source Server. Set each mapping to the source Server's actual time zone
when different. Source `time_zone` cannot repair timezone-free REST date strings;
include original timestamps in acceptance checks for DATE properties.

Compare Schema types, cardinalities, labels, primary keys, endpoint definitions,
indexes and Schema IDs, plus every vertex ID/property and edge ID/endpoint/property.
Counts or job status alone are insufficient. See the official
[HugeGraph Source](https://github.com/apache/seatunnel/blob/v3.0.0/docs/en/connectors/source/HugeGraph.md)
and [Sink](https://github.com/apache/seatunnel/blob/v3.0.0/docs/en/connectors/sink/HugeGraph.md) options.

## Limits

Check the Workers and member counts on every hybrid Pod against the configured
replicas; Pod Ready alone does not establish a shared engine cluster. DNS
discovery's `service-port: 5801` must match the listening member port.

- PRIMARY_KEY and CUSTOMIZE_NUMBER examples do not provide lossless AUTOMATIC-ID
  copies; these require an explicit mapping of generated IDs and edge endpoints.
- The upstream 3.0.0 UI Duration display has a known timezone/completed-job issue.
  Use CLI/REST status and start/end timestamps to assess actual completion.
- Stop jobs before mode changes. Uninterrupted switching, production HA and
  persistent recovery are not guaranteed. Temporary checkpoints are not a durable
  recovery design, and actual NetworkPolicy enforcement depends on the CNI.
