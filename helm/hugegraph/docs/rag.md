# Optional HugeGraph RAG deployment

The main HugeGraph chart owns the Kubernetes topology for the whole system.
`rag.enabled=true` adds the `hugegraph/rag` application from
[hugegraph-ai](https://github.com/apache/hugegraph-ai) without changing the
PD, Store, Server or Hubble topology. RAG remains disabled by default, and the
Docker image and Docker Compose workflows in hugegraph-ai remain independent.

![RAG Helm integration](images/rag-helm-architecture.svg)

## Minimal configuration

For a RAG Pod connected to the HugeGraph Server in the **same Helm release**, the
chart deliberately keeps the required values small:

1. Enable RAG with `rag.enabled=true` or the supplied `values-rag.yaml` overlay.
2. Provide the selected model provider credentials and model settings, normally
   through a Kubernetes Secret or the RAG interface.
3. Set `rag.persistence.storageClassName` or `existingClaim` only when the
   cluster has no suitable default StorageClass.

Do **not** repeat the Server Pod IP, REST port, graph name, graph space or Server
administrator Secret for the normal same-release deployment. The chart derives
them from the Server Service and existing Server values. RAG uses a single
persistent replica by default in both topology presets so the ordinary
`ReadWriteOnce` claim is sufficient.

```bash
# Single PD + Store + Server, plus one persistent RAG Pod
helm upgrade --install hugegraph ./helm/hugegraph \
  -n hugegraph --create-namespace \
  -f helm/hugegraph/values-single.yaml \
  -f helm/hugegraph/values-rag.yaml

# 3 + 3 + 3 HugeGraph cluster, plus one persistent RAG Pod
helm upgrade --install hugegraph ./helm/hugegraph \
  -n hugegraph --create-namespace \
  -f helm/hugegraph/values-cluster.yaml \
  -f helm/hugegraph/values-rag.yaml
```

A bare `--set rag.enabled=true` has the same safe non-root, internal-Service and
persistent-storage defaults. The overlay keeps the commonly reviewed knobs in
one file, but no second copy of the Server connection is required.

## Automatic HugeGraph wiring

With `rag.hugegraph.autoConfigure=true` and an empty `rag.hugegraph.url`, the
chart injects:

- `GRAPH_URL` as the same-release Server client Service in the application's
  native `host:port` form;
- `GRAPH_NAME=hugegraph`, unless explicitly overridden;
- `GRAPH_SPACE` from `server.graphSpace`;
- `GRAPH_USER=admin`, unless explicitly overridden; and
- `GRAPH_PWD` from the same Server administrator Secret when Server auth is
  enabled.

The chart also passes `rag.port` to the native RAG process. The process,
container port, Service and probes therefore use one resolved port value.

Any entry in `rag.env` with the same name replaces its automatic counterpart.
When `rag.env` explicitly supplies `GRAPH_URL`, the chart does **not** attach the
same-release administrator password to that endpoint. Supply an explicit
`rag.hugegraph.passwordSecret` or `GRAPH_PWD` for the selected endpoint. Set
`rag.hugegraph.autoConfigure=false` to manage the complete application
environment yourself.

The automatic user is convenient for a local or evaluation installation, but it
inherits administrator privileges. Production deployments should create a
dedicated least-privilege HugeGraph user and Secret, then override only the user
and password source while keeping the release-derived Server address:

```yaml
rag:
  enabled: true
  hugegraph:
    autoConfigure: true
    username: rag-service
    passwordSecret:
      name: rag-hugegraph-credentials
      key: password
```

When the chart-managed Server administrator Secret changes during a Helm
upgrade, the RAG pod template checksum changes and RAG rolls out with the new
credential. For an external Secret, update the Secret and run `helm upgrade` so
its resource version is re-evaluated; changing the Secret alone does not restart
an existing Pod whose environment was already populated.

## Model provider credentials

Keep provider credentials out of values files. Create a Secret and expose it
with `envFrom`:

```bash
kubectl -n hugegraph create secret generic rag-model \
  --from-literal=OPENAI_API_KEY='replace-me' \
  --from-literal=OPENAI_EMBEDDING_API_KEY='replace-me'
```

```yaml
rag:
  enabled: true
  envFrom:
    - secretRef:
        name: rag-model
```

The application supports other providers through the same native `env` and
`envFrom` mechanisms. The chart does not copy provider credentials into a
ConfigMap or generate a second credential system. The application can persist
selected model and graph settings in its `.env` file, so treat the RAG PVC as
sensitive data as well: restrict access, include it in the appropriate backup
policy, and use storage encryption where required.

## Persistent and externally editable configuration

Persistence defaults to enabled after RAG is enabled. One claim stores the
application's writable resource tree at:

```text
/home/work/hugegraph-llm/src/hugegraph_llm/resources
```

The chart points the application's writable configuration to locations on that
same claim:

```text
.env                         model and graph runtime settings
/demo/config_prompt.yaml     editable prompts and graph schema examples
```

The effective environment variables are therefore:

```text
HUGEGRAPH_LLM_ENV_PATH=<mountPath>/.env
HUGEGRAPH_LLM_PROMPT_CONFIG_PATH=<mountPath>/demo/config_prompt.yaml
```

A restart or image replacement reuses these files. The init container copies
only files that are missing from the claim (`cp -n`); it never overwrites a file
already changed through the UI or by an operator. Consequently, an image upgrade
also does not replace existing prompt/config files with newer packaged defaults.
Back up the claim and update selected files manually when adopting those new
defaults.

The files can be inspected or replaced without rebuilding the image. The
following example locates the RAG Pod by labels instead of assuming a generated
name:

```bash
NS=hugegraph
RELEASE=hugegraph
MOUNT=/home/work/hugegraph-llm/src/hugegraph_llm/resources
POD=$(kubectl -n "$NS" get pod \
  -l "app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=rag" \
  -o jsonpath='{.items[0].metadata.name}')

kubectl -n "$NS" exec "$POD" -- ls -la "$MOUNT"
kubectl -n "$NS" exec -it "$POD" -- sh
```

For GitOps or an externally managed configuration lifecycle, provision a PVC,
seed or mount it with the desired files, and point the chart at it:

```yaml
rag:
  enabled: true
  persistence:
    enabled: true
    existingClaim: managed-rag-config
```

Overriding `HUGEGRAPH_LLM_ENV_PATH`, `HUGEGRAPH_LLM_PROMPT_CONFIG_PATH` or
`rag.persistence.mountPath` is supported, but an override outside the claim is
intentionally ephemeral unless another persistent volume is mounted there.

## Persistence lifecycle and replicas

A chart-generated RAG PVC has `helm.sh/resource-policy: keep`. Disabling RAG or
uninstalling the release removes the RAG workload but retains that PVC and its
data. Delete it manually only after the data is no longer required:

```bash
kubectl -n hugegraph delete pvc hugegraph-rag-data
```

The exact generated name follows the Helm release name; inspect it before
deleting with `kubectl -n hugegraph get pvc`.

One generated `ReadWriteOnce` claim supports one RAG replica. To run multiple
replicas, use a StorageClass and claim that support `ReadWriteMany`, either with
an existing shared claim or by setting `rag.persistence.accessModes` accordingly.
A bound PVC cannot be converted from RWO to RWX in place by `helm upgrade`.
Create a new shared claim and migrate the files before setting
`rag.persistence.existingClaim`, or back up and recreate the generated claim.

```yaml
rag:
  replicas: 2
  persistence:
    enabled: true
    existingClaim: shared-rag-resources
```

Persistence can be disabled explicitly for disposable tests:

```yaml
rag:
  enabled: true
  persistence:
    enabled: false
```

In that mode the generated `.env` and changes inside the container filesystem
are lost when the Pod is replaced.

## Use another HugeGraph deployment

Set an explicit URL and credential Secret for an external Server. A custom
Secret defaults to the key `password` when `key` is omitted; specifying the key
is clearer and recommended.

```yaml
rag:
  enabled: true
  hugegraph:
    autoConfigure: true
    url: http://hugegraph-server.graphs.svc:8080
    graphName: hugegraph
    graphSpace: DEFAULT
    username: rag-service
    passwordSecret:
      name: external-hugegraph-credentials
      key: password
```

## Exposure and authentication

The RAG Service defaults to `ClusterIP` in both `easy` and `strict` modes. This
is intentionally stricter than the browser-oriented Server and Hubble defaults:
the current RAG application login does not provide unified authentication for
every HTTP API route.

An explicit `rag.service.type` always wins. Ingress remains disabled until
`rag.ingress.enabled=true`. Treat NodePort, LoadBalancer and Ingress as reviewed
external-exposure choices. Put the application behind an authenticated reverse
proxy or another trusted network boundary and apply model-provider spend limits.
The application's native login can still be enabled for its own UI and guarded
write paths, but it is not a substitute for an authenticated ingress protecting
all routes.

In `mode=strict`, a NetworkPolicy isolates the Pod. Strict mode permits DNS and
the same-release Server automatically, but does not silently open access to
public model APIs or an Ingress controller. Add the required peers to
`rag.networkPolicy.extraIngress` and provider destinations to
`rag.networkPolicy.extraEgress` using native Kubernetes NetworkPolicy rules.

The templates enforce non-root UID/GID/fsGroup `1000`, dropped Linux
capabilities, disabled privilege escalation and RuntimeDefault seccomp even for
a bare enable. The small default requests/limits in `values-rag.yaml` apply only
to the resource-seeding init container. The main RAG container deliberately
keeps `resources: {}` because model, embedding and index workloads require
installation-specific sizing.
