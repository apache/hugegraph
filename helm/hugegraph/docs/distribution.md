# Chart distribution and offline installation

The OCI chart is named `hugegraph-helm`, independently of the HugeGraph service
version. Its source remains in `helm/hugegraph`. The application name, default
resource names and `nameOverride` / `fullnameOverride` behavior remain unchanged.
Use Helm 3.10+ to render and install this Chart, including OCI support.

This distribution targets HugeGraph **1.8.0 and later**. Development Charts may
use `latest` component images; production deployments need a stable Chart and
published fixed component images.

## Choose a version and registry

The same packaged chart is published to both registries:

| Registry | Chart reference | Typical use |
| --- | --- | --- |
| GHCR | `oci://ghcr.io/hugegraph/hugegraph-helm` | Public CI |
| Docker Hub | `oci://registry-1.docker.io/hugegraph/hugegraph-helm` | Alternative public entry point |

GHCR is recommended for public CI. To use Docker Hub, replace `CHART` with its
reference from the table:

```bash
CHART=oci://ghcr.io/hugegraph/hugegraph-helm
helm install hugegraph "$CHART" --devel \
  -n hugegraph --create-namespace --wait --timeout 15m
```

> [!TIP]
> `--devel` selects the highest Chart version, including development and stable
> versions, without limiting the version series. For production or reproducible
> installs, replace it with `--version <published-version>` to pin your choice.

[Artifact Hub](https://artifacthub.io/packages/helm/hugegraph-helm/hugegraph-helm)
indexes versions and installation instructions. The Chart archive itself comes
from GHCR or Docker Hub.

For a single-node trial, use this instead of the default installation. Install
the downloaded Chart so its bundled preset and templates use the same version:

```bash
CHART=oci://ghcr.io/hugegraph/hugegraph-helm
helm pull "$CHART" --devel --untar
helm install hugegraph ./hugegraph-helm -n hugegraph \
  -f ./hugegraph-helm/values-single.yaml \
  --create-namespace --wait --timeout 10m
```

Development packages append a workflow run number to the source chart version,
for example `0.4.0-dev.123`. They may use `latest` service images. Helm OCI tags
come from the chart's SemVer version; `helm-dev` and `latest` are not Chart versions.

Formal packages use the exact `Chart.version` maintained in source, for example
`0.4.0`. They are published only after the corresponding HugeGraph service release
exists and the chart selects published fixed component images. A source tag
such as `helm-1.8.0` therefore denotes an already released service, not an
advance preview. Chart fixes may be released independently from a reviewed master
commit after the service release; chart and service version numbers need not match.

The manual workflow in [hugegraph/actions](https://github.com/hugegraph/actions)
resolves its selected source ref to a fixed commit, validates and packages the
chart, and installs that exact `.tgz` for its Kubernetes checks before publishing.
Turning off `publish` runs validation without uploading to a registry.

## Install and mirror a chart offline

On an internet-connected machine, select the latest available version and
record it before downloading. Transfer the `.tgz` and container images into
your network. The archive contains no container images.

```bash
CHART=oci://ghcr.io/hugegraph/hugegraph-helm
CHART_VERSION=$(helm show chart "$CHART" --devel | sed -n 's/^version: //p')
helm pull "$CHART" --version "$CHART_VERSION"
```

Set your internal container registry in `private-images.yaml`. Preserve the
component paths, such as `hugegraph/pd`, when mirroring the images:

```yaml
# private-images.yaml
global:
  imageRegistry: registry.internal.example.com
```

Choose one installation method below and replace the example addresses with
your own. Each method installs the same Chart version and uses the private images.

<details open>
<summary>Install the archive directly</summary>

```bash
helm install hugegraph "./hugegraph-helm-${CHART_VERSION}.tgz" \
  -n hugegraph --create-namespace -f private-images.yaml --wait --timeout 15m
```

</details>

<details>
<summary>Internal OCI registry</summary>

```bash
helm registry login registry.internal.example.com
helm push "./hugegraph-helm-${CHART_VERSION}.tgz" \
  oci://registry.internal.example.com/charts
helm install hugegraph oci://registry.internal.example.com/charts/hugegraph-helm \
  --version "$CHART_VERSION" -n hugegraph --create-namespace \
  -f private-images.yaml --wait --timeout 15m
```

`helm push` takes the parent registry path; Helm adds the Chart name and version.

</details>

<details>
<summary>Internal HTTP repository</summary>

Upload the same `.tgz` using the repository's upload mechanism and update
`index.yaml`. For a static HTTP repository, `helm repo index` creates the index.
Native `helm push` only supports OCI.

```bash
helm repo add hugegraph-internal https://charts.internal.example.com
helm repo update hugegraph-internal
helm install hugegraph hugegraph-internal/hugegraph-helm \
  --version "$CHART_VERSION" -n hugegraph --create-namespace \
  -f private-images.yaml --wait --timeout 15m
```

</details>

Mirror the configured `hugegraph/pd`, `hugegraph/store`, `hugegraph/server` and,
when enabled, `hugegraph/hubble` images at their selected tags or digests. The
default Store wait container and Helm test hook reuse component images. Explicit
`store.waitImage` / `server.waitImage` overrides must also be mirrored. Use
`global.imageRegistry`, per-component repositories and `imagePullSecrets` as
shown in the [private registry example](../examples/values-internal-registry.yaml).
The mirror must preserve the selected image references and support the cluster's
CPU architecture.

SeaTunnel and Vermeer are disabled by default in every HugeGraph preset. When
enabling either, also transfer its configured `apache/seatunnel` or
`hugegraph/vermeer` image. Vermeer Master, Worker and token initialization use
the same Vermeer image, which defaults to `hugegraph/vermeer:latest`. Mirror the
selected image and required platforms; pin a compatible tag or digest for a
reproducible deployment. See [image compatibility](vermeer.md#image-compatibility)
for required capabilities and migration from the former pinned default. Hubble
needs its native Vermeer adapter when selecting a custom build.
