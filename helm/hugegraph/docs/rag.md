# Optional HugeGraph RAG deployment

The main HugeGraph chart owns the Kubernetes topology for the whole system.
`rag.enabled=true` adds the `hugegraph/rag` application from
[hugegraph-ai](https://github.com/apache/hugegraph-ai) without changing the
PD, Store, Server or Hubble topology. The component remains disabled by
default, and the Docker image and Docker Compose workflows in hugegraph-ai
remain independent.

![RAG Helm integration](images/rag-helm-architecture.svg)

## Enable it

The same RAG template works with both presets because it connects through the
release-scoped Server Service rather than enumerating Server Pods. The optional
`values-rag.yaml` overlay is the complete, reviewable switch; `rag.enabled=false`
removes every RAG object again.

```bash
# Single PD + Store + Server, plus one RAG Pod
helm upgrade --install hugegraph ./helm/hugegraph \
  -n hugegraph --create-namespace \
  -f helm/hugegraph/values-single.yaml \
  -f helm/hugegraph/values-rag.yaml

# 3 + 3 + 3 HugeGraph cluster, plus the preset's RAG replica count
helm upgrade --install hugegraph ./helm/hugegraph \
  -n hugegraph --create-namespace \
  -f helm/hugegraph/values-cluster.yaml \
  -f helm/hugegraph/values-rag.yaml
```

With `rag.hugegraph.autoConfigure=true` and an empty `rag.hugegraph.url`, the
chart injects:

- `GRAPH_URL` as the same-release Server client Service in the application's native `host:port` form;
- `GRAPH_NAME`, `GRAPH_SPACE` and `GRAPH_USER` from chart values;
- `GRAPH_PWD` from the same Server administrator Secret when Server auth is
  enabled; and
- a writable `HUGEGRAPH_LLM_ENV_PATH` under `/tmp`.

Any entry in `rag.env` with the same name replaces its automatic counterpart.
Set `rag.hugegraph.autoConfigure=false` to manage the complete application
environment yourself.

## Model provider credentials

Keep provider credentials out of values files. Create a Secret and expose it
with `envFrom`:

```bash
kubectl -n hugegraph create secret generic rag-model \
  --from-literal=OPENAI_API_KEY='replace-me' \
  --from-literal=OPENAI_EMBEDDING_API_KEY='replace-me'

cat > /tmp/rag-values.yaml <<'YAML'
rag:
  enabled: true
  envFrom:
    - secretRef:
        name: rag-model
YAML

helm upgrade --install hugegraph ./helm/hugegraph \
  -n hugegraph --create-namespace -f /tmp/rag-values.yaml
```

The application supports other model providers through the same native
`env`/`envFrom` mechanism. The chart does not copy credentials into a
ConfigMap or a generated Secret.

## Use another HugeGraph deployment

Set an explicit URL and credential Secret. An external URL disables automatic
reuse of this release's Server password.

```yaml
rag:
  enabled: true
  hugegraph:
    autoConfigure: true
    url: http://hugegraph-server.graphs.svc:8080
    graphName: hugegraph
    graphSpace: DEFAULT
    username: admin
    passwordSecret:
      name: external-hugegraph-admin
      key: password
```

## Exposure and strict mode

In `mode=easy`, the RAG Service inherits `NodePort`. In `mode=strict`, it
inherits `ClusterIP` and a NetworkPolicy isolates the Pod. Strict mode permits
DNS and the same-release Server automatically, but does not silently open
access to public model APIs or an Ingress controller. Add the required peers
to `rag.networkPolicy.extraIngress` and provider destinations to
`rag.networkPolicy.extraEgress` using native Kubernetes NetworkPolicy rules.

An explicit `rag.service.type` always wins over the mode default. Ingress is
independent and remains disabled until `rag.ingress.enabled=true`. Before exposing
the demo UI outside a trusted network, review the application login settings and
model-provider spend controls; the chart does not invent a second RAG credential
system.

## Persistence and replicas

Persistence is off by default, so the image's packaged prompt and resource
files are not hidden by an empty volume. When enabled, an init container seeds
the claim from the selected image before the main container mounts it at
`rag.persistence.mountPath`. A generated claim is retained on uninstall.

One generated `ReadWriteOnce` claim is valid only for one RAG replica. For
multiple replicas, either keep persistence disabled, provide an existing
shared claim, or use a StorageClass that supports `ReadWriteMany` and include
that mode in `rag.persistence.accessModes`. Model and graph credentials remain
Kubernetes Secrets regardless of persistence.
