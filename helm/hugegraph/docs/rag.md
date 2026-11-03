# Optional HugeGraph RAG deployment

The canonical HugeGraph chart can deploy the `hugegraph/rag` application from
[hugegraph-ai](https://github.com/apache/hugegraph-ai) beside PD, Store, Server
and Hubble. RAG is opt-in and does not change the existing graph topology.

![RAG Helm integration](https://raw.githubusercontent.com/apache/hugegraph/master/helm/hugegraph/docs/images/rag-helm-architecture.svg)

## Minimal same-release configuration

For the normal case, the only RAG value that must be set is:

```yaml
rag:
  enabled: true
```

The chart derives the complete connection to the Server in the same release:

- `GRAPH_URL` uses the release-scoped Server Service and `server.port`;
- `GRAPH_NAME` defaults to `hugegraph`;
- `GRAPH_SPACE` follows `server.graphSpace`;
- `GRAPH_USER` defaults to `admin`; and
- `GRAPH_PWD` comes from the existing Server administrator Secret when Server
  authentication is enabled.

Do not repeat a Server Pod IP, REST port, graph name, graph space or administrator
Secret for this topology. A custom `rag.port` is also applied consistently to the
RAG process, container, Service and probes.

The supplied overlay keeps the ordinary settings in one reviewed file:

```bash
# One PD + Store + Server, plus one RAG Pod
helm upgrade --install hugegraph ./helm/hugegraph \
  -n hugegraph --create-namespace \
  -f helm/hugegraph/values-single.yaml \
  -f helm/hugegraph/values-rag.yaml

# 3 + 3 + 3 HugeGraph cluster, plus one RAG Pod
helm upgrade --install hugegraph ./helm/hugegraph \
  -n hugegraph --create-namespace \
  -f helm/hugegraph/values-cluster.yaml \
  -f helm/hugegraph/values-rag.yaml
```

When `rag.persistence.enabled` is unset, RAG inherits the top-level
`persistence.enabled` value. The chart default is `true`, so a bare
`--set rag.enabled=true` creates one retained PVC. Set the RAG-specific value only
when this component must differ from the rest of the release.

Model, embedding and prompt settings can be entered through the RAG interface
and retained on the PVC. Kubernetes Secrets remain preferable for declarative
production credentials.

## Automatic HugeGraph wiring and credentials

With `rag.hugegraph.autoConfigure=true` and an empty `rag.hugegraph.url`, the
same-release connection above is generated automatically. A matching entry in
`rag.env` replaces its generated counterpart.

The automatic `admin` identity is convenient for evaluation but has full Server
privileges. Production deployments should create a dedicated least-privilege
HugeGraph account and override only its identity and Secret while retaining the
automatically derived address:

```yaml
rag:
  enabled: true
  hugegraph:
    username: rag-service
    passwordSecret:
      name: rag-hugegraph-credentials
      key: password
```

The chart-managed Server Secret is covered by a pod-template checksum. An
explicit `rag.hugegraph.passwordSecret` is covered by its live resource version
during an online `helm upgrade`. In both cases, the restarted Pod uses the new
Secret even when an older value exists in the retained `.env` file.

Offline renderers and GitOps controllers cannot use Helm `lookup` to observe a
live Secret's resource version. Rotating an explicit
`rag.hugegraph.passwordSecret` while applying otherwise identical rendered
manifests therefore needs an explicit rollout trigger after the Secret has been
updated. Either restart the Deployment:

```bash
RELEASE=hugegraph
kubectl -n hugegraph rollout restart deployment/${RELEASE}-hugegraph-rag
```

or change a reviewed Pod annotation in the same GitOps change:

```yaml
rag:
  podAnnotations:
    hugegraph.apache.org/credential-revision: "2026-11-04-1"
```

When `rag.env` explicitly supplies `GRAPH_URL`, the chart does not attach the
same-release administrator password to that endpoint. Supply an explicit
`passwordSecret` or `GRAPH_PWD` if the selected endpoint requires authentication.
Set `rag.hugegraph.autoConfigure=false` when the complete graph connection is
owned outside the chart.

## Model-provider Secrets

Keep provider credentials out of values files. Explicit `env` entries are easy
to audit:

```bash
kubectl -n hugegraph create secret generic rag-model \
  --from-literal=OPENAI_API_KEY='replace-me' \
  --from-literal=OPENAI_EMBEDDING_API_KEY='replace-me'
```

```yaml
rag:
  enabled: true
  env:
    - name: OPENAI_API_KEY
      valueFrom:
        secretKeyRef:
          name: rag-model
          key: OPENAI_API_KEY
    - name: OPENAI_EMBEDDING_API_KEY
      valueFrom:
        secretKeyRef:
          name: rag-model
          key: OPENAI_EMBEDDING_API_KEY
```

`envFrom` is also supported without an extra key list:

```yaml
rag:
  enabled: true
  envFrom:
    - secretRef:
        name: rag-model
```

Kubernetes gives an explicit `env` entry precedence over the same key imported
through `envFrom`. Because automatic same-release wiring injects `GRAPH_PWD` as
an explicit entry, an `envFrom` Secret cannot replace that particular key. Use
`rag.hugegraph.passwordSecret` (preferred), or declare a `rag.env` entry named
`GRAPH_PWD` with its own `secretKeyRef`; either choice suppresses the automatic
password entry. `envFrom` remains appropriate for provider and other keys that
the chart does not generate.

At every start, any key present in the current Pod environment—including keys
loaded by `envFrom`—takes precedence over the same key in the retained `.env`.
Changing a provider Secret alone does not restart a Pod whose environment was
already populated, so restart RAG after the Secret changes:

```bash
kubectl -n hugegraph rollout restart deployment/hugegraph-hugegraph-rag
```

The exact Deployment name follows the Helm release name. The application may
write credentials to `.env`; treat the RAG PVC as sensitive, restrict access,
use storage encryption where appropriate, and include it in the relevant backup
policy.

## Persistent and externally editable configuration

When persistence is enabled, the claim is mounted at the native HugeGraph AI
resource directory:

```text
/home/work/hugegraph-llm/src/hugegraph_llm/resources
```

The important writable locations are:

```text
.env                         model and graph runtime settings
/demo/config_prompt.yaml     prompts and graph-schema examples
/...                         indexes and other generated resources
```

The chart sets:

```text
HUGEGRAPH_LLM_ENV_PATH=<mountPath>/.env
HUGEGRAPH_LLM_PROMPT_CONFIG_PATH=<mountPath>/demo/config_prompt.yaml
```

A restart or image replacement reuses these files. The init container copies
only files missing from the claim and uses an ordinary recursive copy so it does
not attempt privileged ownership or timestamp changes on the CSI volume root.
Existing operator or UI edits are never overwritten. Consequently, an image
upgrade does not replace an existing prompt or configuration file with a newer
packaged default; compare and update selected files manually when adopting those
defaults.

### Configuration precedence on restart

HugeGraph AI loads an existing `.env` before Pydantic reads settings. To prevent
an old file from masking a rotated Secret or changed Helm value, the container
performs one narrow reconciliation before starting the application:

1. Read the configured `.env` file.
2. Remove only keys that are already present in the current Pod environment.
3. Leave every other `.env` key unchanged.
4. Start the native RAG process.

The result is a simple precedence rule:

```text
current Kubernetes env / envFrom  >  retained .env  >  image defaults
```

This covers the automatically generated graph connection, explicit `rag.env`,
and arbitrary Secret or ConfigMap keys supplied through `envFrom` without a
second key registry. UI and operator settings that are not declared in the Pod
environment remain persistent and externally editable. The application may
write the current values back during the run; the same reconciliation happens
before the next start.

A custom `HUGEGRAPH_LLM_ENV_PATH` in `rag.env` follows the same rule. Make that
path writable and mount persistent storage there when it must survive Pod
replacement.

### Read and update files from outside the Pod

Locate the Pod through stable labels rather than assuming its generated name:

```bash
NS=hugegraph
RELEASE=hugegraph
MOUNT=/home/work/hugegraph-llm/src/hugegraph_llm/resources
POD=$(kubectl -n "$NS" get pod \
  -l "app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=rag" \
  -o jsonpath='{.items[0].metadata.name}')
```

Export, edit locally, then replace the files atomically:

```bash
kubectl -n "$NS" exec "$POD" -- cat "$MOUNT/.env" > rag.env
kubectl -n "$NS" exec "$POD" -- cat "$MOUNT/demo/config_prompt.yaml" \
  > config_prompt.yaml

# Edit the local files. Do not commit rag.env.
kubectl -n "$NS" exec -i "$POD" -- sh -c \
  'umask 007; tmp="$1.tmp"; cat > "$tmp" && mv "$tmp" "$1"' sh \
  "$MOUNT/.env" < rag.env
kubectl -n "$NS" exec -i "$POD" -- sh -c \
  'tmp="$1.tmp"; cat > "$tmp" && mv "$tmp" "$1"' sh \
  "$MOUNT/demo/config_prompt.yaml" < config_prompt.yaml

kubectl -n "$NS" rollout restart deployment/${RELEASE}-hugegraph-rag
```

Back up a file before replacing it. Do not print `.env` in shared logs.

For GitOps, provision and seed a PVC separately, then use it directly:

```yaml
rag:
  enabled: true
  persistence:
    existingClaim: managed-rag-resources
```

## Persistence lifecycle and single-replica contract

A chart-generated RAG PVC has `helm.sh/resource-policy: keep`. Disabling RAG or
uninstalling the release removes the workload but retains the claim and its data.
Inspect the generated name and delete it only after the data is no longer needed:

```bash
kubectl -n hugegraph get pvc
kubectl -n hugegraph delete pvc hugegraph-hugegraph-rag-data
```

The current HugeGraph RAG application keeps mutable settings in one process and
writes the resource tree without cross-Pod coordination. An RWX volume would
make files mountable by multiple Pods, but would not synchronize in-memory
settings or serialize writes. The chart therefore requires `rag.replicas=1` and
uses `Recreate`, so an update never leaves two active RAG Pods.

Disable all chart persistence for disposable environments:

```yaml
persistence:
  enabled: false
rag:
  enabled: true
```

Or override only RAG:

```yaml
rag:
  enabled: true
  persistence:
    enabled: false
```

Without persistence, `.env` is created under `/tmp` and in-container edits are
lost when the Pod is replaced.

## External HugeGraph deployment

Set an explicit URL and credential source for another Server:

```yaml
rag:
  enabled: true
  hugegraph:
    url: http://hugegraph-server.graphs.svc:8080
    graphName: hugegraph
    graphSpace: DEFAULT
    username: rag-service
    passwordSecret:
      name: external-hugegraph-credentials
      key: password
```

A custom Secret defaults to key `password` when `key` is omitted, although an
explicit key is clearer.

## Security contexts and OpenShift

The templates default to non-root UID/GID/fsGroup `1000`, dropped Linux
capabilities, disabled privilege escalation and RuntimeDefault seccomp. The main
RAG container intentionally keeps `resources: {}` because model, embedding and
index workloads need installation-specific sizing; only the small seeding init
container has generic defaults in `values-rag.yaml`.

Platforms such as OpenShift restricted SCC assign the numeric identity at
admission time. Keep `runAsNonRoot: true`, but use `auto` to omit the fixed IDs:

```yaml
rag:
  podSecurityContext:
    runAsUser: auto
    runAsGroup: auto
    fsGroup: auto
  securityContext:
    runAsUser: auto
    runAsGroup: auto
```

When `fsGroup` is omitted, the chart also omits `fsGroupChangePolicy`. Confirm
that the platform makes the selected PVC writable for the assigned identity.

## Exposure and NetworkPolicy

The RAG Service defaults to `ClusterIP` in both `easy` and `strict` modes. This
is deliberately stricter than browser-oriented Server and Hubble defaults: the
current application login does not protect every HTTP route.

NodePort, LoadBalancer and Ingress are explicit choices. Put externally exposed
RAG behind an authenticated reverse proxy or another reviewed network boundary,
and apply model-provider spend limits.

In `mode=strict`, NetworkPolicy permits DNS and the same-release Server. Add the
Ingress controller and required model-provider destinations through
`rag.networkPolicy.extraIngress` and `rag.networkPolicy.extraEgress`. Policy
objects are effective only when the cluster CNI implements NetworkPolicy.
