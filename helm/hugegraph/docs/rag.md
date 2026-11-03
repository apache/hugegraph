# Optional HugeGraph RAG deployment

The chart deploys one RAG instance that connects to the Server Service. This is
independent of whether HugeGraph uses one PD/Store/Server or a distributed cluster.
RAG remains a single process with a persistent resource directory.

> This refactor requires [HugeGraph AI PR #96](https://github.com/hugegraph/hugegraph-ai/pull/96) in the
> `latest`/1.8 image. Publish that image before using this chart revision. The chart
> no longer patches application configuration at startup.

## Start and open RAG

```bash
helm upgrade --install hugegraph ./helm/hugegraph \
  -n hugegraph --create-namespace \
  -f helm/hugegraph/values-single.yaml \
  -f helm/hugegraph/values-rag.yaml
kubectl -n hugegraph port-forward svc/hugegraph-hugegraph-rag 8001:8001
```

Open http://127.0.0.1:8001/. A different release name changes the Service name;
Helm prints the matching command. For distributed HugeGraph, use
`values-cluster.yaml` instead of `values-single.yaml`.

The only required RAG value is `rag.enabled: true`. The chart derives the full
Server HTTP URL, graph space, admin identity and administrator Secret from the
release. Graph name defaults to `hugegraph`; override `rag.hugegraph.graphName`
for another graph. Model services and credentials are needed for inference.
The UI being reachable does not establish that Server or model services are ready.

RAG uses ClusterIP by default. Put external exposure behind an authenticated
reverse proxy: the application's login does not protect every HTTP route.

## Configuration ownership

The application reads configuration with this precedence:

```text
process environment / Secret > mounted .env > application defaults
```

Within one source, a field-specific provider setting overrides its generic key.
For example, `OPENAI_CHAT_API_KEY` overrides `OPENAI_API_KEY` in the same source,
but a generic process key overrides stale field-specific values in `.env`.
Startup does not modify `.env` or rewrite an existing prompt file. UI saves write
only changed settings, and reject changes to environment-managed fields. Injected
Secrets and derived defaults are not copied into `.env`.

Change deployment configuration outside the container, then restart RAG:

```bash
kubectl -n hugegraph rollout restart deployment/hugegraph-hugegraph-rag
```

There is no automatic hot reload. Secret changes require a restart too. An online
Helm upgrade observes an explicit graph password Secret's resource version;
offline/GitOps rendering requires an explicit rollout trigger or changed annotation.

## Mount configuration without entering the container

Manage a local `rag.env` and `config_prompt.yaml` and create a Secret/ConfigMap:

```bash
kubectl -n hugegraph create secret generic rag-settings \
  --from-env-file=rag.env --dry-run=client -o yaml | kubectl apply -f -
kubectl -n hugegraph create configmap rag-prompts \
  --from-file=config_prompt.yaml --dry-run=client -o yaml | kubectl apply -f -
```

Keep credentials out of Git. Use this overlay after `values-rag.yaml`:

```yaml
rag:
  envFrom:
    - secretRef:
        name: rag-settings
  env:
    - name: HUGEGRAPH_LLM_PROMPT_CONFIG_PATH
      value: /etc/rag/config_prompt.yaml
  extraVolumes:
    - name: prompts
      configMap:
        name: rag-prompts
  extraVolumeMounts:
    - name: prompts
      mountPath: /etc/rag
      readOnly: true
```

The prompt file is operator-owned; edit its source and apply the ConfigMap rather
than saving it through the UI. The application can also read a mounted `.env`:
mount its Secret at a separate path and set `HUGEGRAPH_LLM_ENV_PATH` to that file.
A read-only `.env` is supported at startup; UI/API saves require a writable file.
Keep configuration mounts separate from the writable resource directory.

Automatic graph settings are explicit environment entries and take precedence over
`envFrom`. To replace the automatic password use `rag.hugegraph.passwordSecret`
or an explicit `rag.env` entry named `GRAPH_PWD` with a `secretKeyRef`.
Set `rag.hugegraph.autoConfigure: false` to own the entire graph connection outside
the chart. With automatic wiring enabled, an endpoint without configured
credentials receives an explicit empty password so old file credentials cannot
leak to a different Server.

## Persistent resources

A StorageClass is required by default. RAG inherits `persistence.enabled` unless
`rag.persistence.enabled` is explicitly set. Its PVC is mounted at
`/var/lib/hugegraph-rag`; the application uses this directory for `.env`,
`demo/config_prompt.yaml`, indexes and generated resources. Missing bundled
examples are read directly from the image; startup does not copy resources or
generate configuration files. Explicit saves and index generation need a writable
resource directory.
Changing `rag.persistence.mountPath` moves the complete resource root.

Explicit `HUGEGRAPH_LLM_ENV_PATH` and `HUGEGRAPH_LLM_PROMPT_CONFIG_PATH` overrides
can point to separate configuration mounts. For an existing externally managed
volume, set `rag.persistence.existingClaim`. Mount that volume in an administrative
job or manage its backing storage to edit files without a shell in the RAG Pod.
UI saves to writable configuration files survive container replacement.

RAG requires exactly one replica and uses Recreate. The chart retains its PVC
when RAG is disabled or the release is uninstalled; remove the claim explicitly
only when its data is no longer needed. UI-entered credentials may be stored on
this volume, so treat it as sensitive. Image upgrades do not overwrite user files.

For disposable deployments, set `rag.persistence.enabled: false`. File changes
are then lost on container replacement.

## External Server and deployment options

```yaml
rag:
  hugegraph:
    url: http://hugegraph-server.graphs.svc:8080
    graphName: hugegraph
    graphSpace: DEFAULT
    username: rag-service
    passwordSecret:
      name: rag-hugegraph-credentials
      key: password
```

For production use, provision a dedicated least-privilege account instead of the
automatically connected administrator account. Keep full `http://` or `https://`
URLs for explicitly configured endpoints.

RAG defaults to non-root UID/GID/fsGroup 1000 with dropped capabilities and no
ServiceAccount token. On platforms assigning identities, use `auto` for
`rag.podSecurityContext.runAsUser`, `runAsGroup`, `fsGroup` and their container
counterparts, and ensure the volume is writable. Size `rag.resources` for the
selected models, indexing workload and dataset.

In strict mode, NetworkPolicy permits DNS and the same-release Server. Configure
model-provider destinations and the ingress controller through
`rag.networkPolicy.extraEgress` and `extraIngress`. Enforcement requires a
NetworkPolicy-capable CNI.
