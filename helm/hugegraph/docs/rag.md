# Optional HugeGraph RAG deployment

The chart deploys one RAG instance that connects to the Server Service. This is
independent of whether HugeGraph uses one PD/Store/Server or a distributed cluster.
RAG remains a single process with a persistent resource directory.

> Use a HugeGraph RAG image built from Apache HugeGraph AI 1.8.0 or later.
> Earlier images do not implement the external-configuration contract required by
> this chart. The chart mounts resources and supplies bootstrap defaults; it does
> not patch application configuration at startup.

## Start and open RAG

Run these commands from the repository root. Before installing, provide a
Kubernetes cluster with enough CPU and memory, working persistent-volume
provisioning, and access to a compatible HugeGraph RAG 1.8.0-or-later image.
The cluster preset runs three PD, three Store and three Server replicas and
requires scheduling them across nodes; RAG itself still runs one replica. By
default the RAG resource PVC requests 10Gi, separately from PD and Store storage.

For a distributed HugeGraph cluster with RAG:

```bash
helm upgrade --install hugegraph ./helm/hugegraph \
  -n hugegraph --create-namespace \
  -f helm/hugegraph/values-cluster.yaml \
  -f helm/hugegraph/values-rag.yaml \
  --wait --timeout 15m
kubectl -n hugegraph port-forward svc/hugegraph-rag 8001:8001
```

For a single-node HugeGraph deployment with RAG:

```bash
helm upgrade --install hugegraph ./helm/hugegraph \
  -n hugegraph --create-namespace \
  -f helm/hugegraph/values-single.yaml \
  -f helm/hugegraph/values-rag.yaml \
  --wait --timeout 15m
kubectl -n hugegraph port-forward svc/hugegraph-rag 8001:8001
```

Open http://127.0.0.1:8001/. A different release name changes the Service name;
Helm prints the matching command.

The only required RAG value is `rag.enabled: true`. The chart derives the full
Server HTTP URL, graph space, admin identity and administrator Secret from the
release. Graph name defaults to `hugegraph`; override `rag.hugegraph.graphName`
for another graph. Model services and credentials are needed for inference.
The UI being reachable does not establish that Server or model services are ready.

### Configure models in the web UI

No model key needs to be placed in Helm values before installation. After opening
the RAG page, use the configuration section to select an OpenAI-compatible or
Ollama Chat provider and enter its address, model and, where required, API key.
Choose **Save configuration**, then **Test saved connection**. Configure an
Embedding provider in the same way before workflows that need embeddings or
vector indexes. Reranker and external vector-engine settings are optional and
depend on the chosen workflow. Extraction and Text2GQL use Chat by default.

When RAG and Server belong to this release, the chart supplies the Server
connection and administrator Secret automatically. Use the HugeGraph server
configuration section only when connecting to a different Server or graph.
Model endpoints must be reachable from the RAG Pod. Values saved in the web UI
persist on the RAG PVC; alternatively, operators can supply bootstrap defaults
through mounted files and Kubernetes Secrets as described below.

RAG uses ClusterIP by default. Put external exposure behind an authenticated
reverse proxy: the application's login does not protect every HTTP route.

## Configuration ownership

The web UI/API manages model connections. Deployment values are bootstrap
fallbacks; saved connection fields take precedence across restarts/upgrades:

```text
saved settings.yaml > process environment / Secret > mounted .env > application defaults
```

Deployment sources can be combined: for example, put the model URL and name in
a mounted `.env` and the key in a Secret. Within one source, a field-specific
setting wins over its generic alias. Helm only mounts files and injects defaults;
the application resolves their priority and saves its own settings.

**Save configuration** validates and saves without contacting the model service.
**Test saved connection** is separate; a failed test leaves the saved connection
intact. Saved keys never appear in input fields. Blank/omitted key input keeps the
existing key; use **Clear API key** to remove it. For a connection with an existing
key, changing the model service URL requires a new key or explicit clear; changing
the model at the same URL keeps the key. OpenAI-compatible services and Ollama are
supported. Model requests use the service's default temperature.
Information extraction and Text2GQL dynamically use Chat until independently
configured; **Use Chat connection** restores inheritance. Embedding/reranker stay
separate. Login/tokens and Thin API write opt-in remain deployment controls.

Saved application connections live in `settings.yaml` on the writable RAG PVC.
Changing Helm defaults does not replace saved connection fields. When a key is
injected from a Secret as an environment variable and the user saves without
replacing it, the application keeps a reference to that variable rather than
copying the value.
Edit the file offline and restart, or delete the relevant connection group to
return to deployment defaults.
Do not edit the file while an application save is in progress. Startup does not
modify `.env`, saved settings or prompt files. Normal requests do not persist
request prompts. Only explicit saves/index generation require writable storage.

Deployment file/Secret changes require a restart:

```bash
kubectl -n hugegraph rollout restart deployment/hugegraph-rag
```

There is no automatic file hot reload. An online Helm upgrade observes an explicit
graph password Secret's resource version; offline/GitOps rendering requires an
explicit rollout trigger or changed annotation. Saved graph connection fields keep
their values; a password still referencing a Secret follows its updated value after
the restart. Update a user-saved password through its configuration entry.

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
A read-only `.env` remains supported while model UI/API saves go to the writable
`settings.yaml` on the resource PVC. Keep bootstrap mounts separate from that
writable directory; a read-only prompt itself still cannot be saved through the UI.

Automatic graph settings are explicit environment entries and take precedence over
`envFrom`. To replace the automatic password use `rag.hugegraph.passwordSecret`
or an explicit `rag.env` entry named `GRAPH_PWD` with a `secretKeyRef`.
Set `rag.hugegraph.autoConfigure: false` to own the entire graph connection outside
the chart. With automatic wiring enabled, an endpoint without configured
credentials receives an explicit empty password so old file credentials cannot
leak to a different Server. With `autoConfigure: false`, file credentials remain
valid for their configured endpoint. The application clears a file password when
a process-level `GRAPH_URL` switches to a different endpoint without a process
password; explicitly supply the new endpoint's `GRAPH_PWD` through a Secret (or an
empty value). When editing the mounted file itself, update its URL and credentials
together.

## Persistent resources

A StorageClass is required by default. RAG inherits `persistence.enabled` unless
`rag.persistence.enabled` is explicitly set. Its PVC is mounted at
`/var/lib/hugegraph-rag`; the application uses this directory for `settings.yaml`, optional bootstrap `.env`,
`demo/config_prompt.yaml`, indexes and generated resources. Missing bundled
examples are read directly from the image; startup does not copy resources or
generate configuration files. Explicit saves and index generation need a writable
resource directory.
Changing `rag.persistence.mountPath` moves the complete resource root.
`rag.persistence.size` must be a positive Kubernetes storage quantity, such as
`"10Gi"`, `"500M"` or `"1e9"` (quote numeric-looking quantities). Access modes must be a non-empty list of
`ReadWriteOnce`, `ReadOnlyMany`, `ReadWriteMany` or `ReadWriteOncePod`; support
for a mode still depends on the cluster and storage driver. Decimal kilo uses
lowercase `k` (such as `"100k"`); binary kilo uses `Ki` (`"100Ki"`). Bare uppercase
`K` is not a valid Kubernetes suffix.

Explicit `HUGEGRAPH_LLM_SETTINGS_PATH`, `HUGEGRAPH_LLM_ENV_PATH` and `HUGEGRAPH_LLM_PROMPT_CONFIG_PATH` overrides
can point to separate configuration mounts. For an existing externally managed
volume, set `rag.persistence.existingClaim` to a string (quote numeric names such
as `"123"`). Mount that volume in an administrative
job or manage its backing storage to edit files without a shell in the RAG Pod.
UI saves to writable configuration files survive container replacement.

RAG requires exactly one replica and uses Recreate. The chart retains its PVC
when RAG is disabled or the release is uninstalled; remove the claim explicitly
only when its data is no longer needed. UI-entered credentials may be stored on
this volume, so treat it as sensitive. Image upgrades do not overwrite user files.

For disposable deployments, set `rag.persistence.enabled: false`; file changes
are then lost on container replacement. Changing or disabling the PVC does not
automatically copy existing settings, prompts or indexes to the new storage.
Back up any files you need before switching.

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

RAG probe paths must start with `/`; `terminationGracePeriodSeconds` must be a
non-negative integer. Partial probe overrides keep the remaining defaults.

Application settings contain user-saved credentials. New files use mode 0600;
existing permissions are preserved. Restrict PVC/backup access. Prefer a writable
directory/PVC: ordinary saves use atomic replacement; a writable single-file bind
mount uses an in-place fallback on `EBUSY`, which cannot guarantee atomicity if
interrupted. This does not affect read-only bootstrap Secret/ConfigMap mounts.

See the [application configuration guide](https://github.com/apache/hugegraph-ai/blob/main/hugegraph-llm/config.md)
for API examples, complete saved connection groups and restore-default behavior.