#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

{{/* Resolve inherited defaults once. Never use default/or for explicit booleans. */}}
{{- define "hugegraph.resolveValues" -}}
{{- if not (get .Values "__hugegraphResolved") -}}
{{- $_ := set .Values "__hugegraphResolved" true -}}
{{/* Old official Server images use hg-test when no cluster is configured. */}}
{{- if not (hasKey .Values "metadataCluster") -}}
{{- $_ := set .Values "metadataCluster" "hg-test" -}}
{{- end -}}
{{- $legacy := not (hasKey .Values "mode") -}}
{{- if not (hasKey .Values "hubble") -}}
{{- $_ := set .Values "hubble" (dict "enabled" false "service" dict "persistence" dict) -}}
{{- end -}}
{{- if not (hasKey .Values "seatunnel") -}}
{{- $_ := set .Values "seatunnel" (dict "enabled" false "service" dict) -}}
{{- end -}}
{{- if not (hasKey .Values "vermeer") -}}
{{- $_ := set .Values "vermeer" (dict "enabled" false "service" dict) -}}
{{- end -}}
{{/* --reuse-values can combine an old release with only vermeer.enabled=true.
Fail before dereferencing missing blocks; keep values.yaml the sole defaults source. */}}
{{- if .Values.vermeer.enabled -}}
{{- range $key := list "mode" "image" "auth" "hugegraphCredentials" "master" "worker" "probes" "terminationGracePeriodSeconds" "podLabels" "podAnnotations" -}}
{{- if not (hasKey $.Values.vermeer $key) -}}
{{- fail (printf "vermeer is missing new chart defaults (%s): legacy --reuse-values does not import newly added settings. Export and review existing effective values with helm get values --all -o yaml, then upgrade with -f existing-values.yaml --set vermeer.enabled=true without --reuse-values so Helm merges the new defaults" $key) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if and .Values.vermeer.enabled (not (hasKey .Values.vermeer.auth "admin")) -}}
{{- fail "vermeer.auth.admin is missing new chart defaults: export and review existing effective values, then upgrade with -f existing-values.yaml without --reuse-values so Helm imports the admin login defaults" -}}
{{- end -}}
{{- $strict := eq (.Values.mode | default "easy") "strict" -}}
{{- $serviceType := ternary "ClusterIP" "NodePort" (or $strict $legacy) -}}
{{- range $comp := list "server" "hubble" "seatunnel" "vermeer" -}}
{{- $component := get $.Values $comp -}}
{{- $svc := get $component "service" | default dict -}}
{{- $_ := set $component "service" $svc -}}
{{- $typeInherited := or (not (hasKey $svc "type")) (eq (get $svc "type" | toString) "<nil>") -}}
{{- $_ := set $component "__serviceTypeInherited" $typeInherited -}}
{{- if $typeInherited -}}{{- $_ := set $svc "type" $serviceType -}}{{- end -}}
{{- end -}}
{{- $np := .Values.networkPolicy | default dict -}}
{{- $_ := set .Values "networkPolicy" $np -}}
{{- if or (not (hasKey $np "enabled")) (eq (get $np "enabled" | toString) "<nil>") -}}{{- $_ := set $np "enabled" $strict -}}{{- end -}}
{{- $persistent := true -}}
{{- if hasKey (.Values.persistence | default dict) "enabled" -}}{{- $persistent = .Values.persistence.enabled -}}{{- end -}}
{{- range $comp := list "pd" "store" "hubble" -}}
{{- $component := get $.Values $comp -}}
{{- $key := ternary "persistence" "storage" (eq $comp "hubble") -}}
{{- $storage := get $component $key | default dict -}}
{{- $_ := set $component $key $storage -}}
{{- if or (not (hasKey $storage "enabled")) (eq (get $storage "enabled" | toString) "<nil>") -}}{{- $_ := set $storage "enabled" $persistent -}}{{- end -}}
{{- end -}}
{{/* The runtime is an image variant. Keep business/JNI configuration in the image. */}}
{{- $variant := get (.Values.global | default dict) "runtimeVariant" | default "standard" -}}
{{- if not (has $variant (list "standard" "topling")) -}}
{{- fail "global.runtimeVariant must be standard or topling" -}}
{{- end -}}
{{- if eq $variant "topling" -}}
{{- range $comp := list "pd" "store" "server" -}}
{{- $component := get $.Values $comp -}}
{{- $image := $component.image -}}
{{- $tag := $image.tag | default $.Chart.AppVersion -}}
{{- if not (hasSuffix "-topling" $tag) -}}{{- $_ := set $image "tag" (printf "%s-topling" $tag) -}}{{- end -}}
{{- $selector := get $component "nodeSelector" | default dict -}}
{{- range $key, $required := dict "kubernetes.io/arch" "amd64" "kubernetes.io/os" "linux" -}}
{{- if and (hasKey $selector $key) (ne (get $selector $key) $required) -}}
{{- fail (printf "%s.nodeSelector.%s must be %s for global.runtimeVariant=topling" $comp $key $required) -}}
{{- end -}}
{{- $_ := set $selector $key $required -}}
{{- end -}}
{{- $_ := set $component "nodeSelector" $selector -}}
{{- end -}}
{{- end -}}
{{- if .Values.vermeer.enabled -}}
{{- range $role := list "master" "worker" -}}
{{- $storage := get (get $.Values.vermeer $role) "storage" -}}
{{- if or (not (hasKey $storage "enabled")) (eq (get $storage "enabled" | toString) "<nil>") -}}
{{- $_ := set $storage "enabled" $persistent -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $registry := trimSuffix "/" (get (.Values.global | default dict) "imageRegistry" | default "") -}}
{{- if contains "://" $registry -}}{{- fail "global.imageRegistry must be a registry host/path without http:// or https://" -}}{{- end -}}
{{- if $registry -}}
{{- range $comp := list "pd" "store" "server" "hubble" "seatunnel" "vermeer" -}}
{{- $image := get (get $.Values $comp) "image" | default dict -}}
{{- if $image -}}
{{- $_ := set $image "repository" (include "hugegraph.registryImage" (dict "reference" $image.repository "registry" $registry)) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- range $comp := list "store" "server" -}}
{{- $values := get $.Values $comp -}}
{{- $override := get $values "waitImage" | default "" -}}
{{- $componentImage := include "hugegraph.image" (dict "image" $values.image "appVersion" $.Chart.AppVersion) -}}
{{- if $override -}}
{{- $_ := set $values "waitImage" (include "hugegraph.registryImage" (dict "reference" $override "registry" $registry)) -}}
{{- else -}}
{{- $_ := set $values "waitImage" $componentImage -}}
{{- end -}}
{{- $_ := set $values "__waitImageInherited" (eq $values.waitImage $componentImage) -}}
{{- end -}}
{{- $server := .Values.server -}}
{{- if not (hasKey $server "storageWaitTimeoutSeconds") -}}{{- $_ := set $server "storageWaitTimeoutSeconds" 300 -}}{{- end -}}
{{- $startup := $server.probes.startup -}}
{{- if $legacy -}}
{{- $period := int $startup.periodSeconds -}}
{{- $budget := add (int $server.storageWaitTimeoutSeconds) 150 -}}
{{- $_ := set $startup "failureThreshold" (max (int $startup.failureThreshold) (add (div (add (sub $budget 1) $period) $period) 1)) -}}
{{- end -}}
{{- if or (not (hasKey $startup "failureThreshold")) (eq (get $startup "failureThreshold" | toString) "<nil>") -}}
{{- $period := int $startup.periodSeconds -}}
{{- $budget := add (int $server.storageWaitTimeoutSeconds) 150 -}}
{{- $_ := set $startup "failureThreshold" (add (div (add (sub $budget 1) $period) $period) 1) -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/* A qualified component reference overrides the global registry. */}}
{{- define "hugegraph.registryImage" -}}
{{- $reference := .reference -}}
{{- $first := first (splitList "/" $reference) -}}
{{- $qualified := and (contains "/" $reference) (or (contains "." $first) (contains ":" $first) (eq $first "localhost")) -}}
{{- if and .registry (not $qualified) -}}{{- printf "%s/%s" .registry $reference -}}{{- else -}}{{- $reference -}}{{- end -}}
{{- end }}

{{/*
Keep the application identity stable when the distribution chart is renamed.
The OCI artifact is hugegraph-helm; Kubernetes resources still use hugegraph.
*/}}
{{- define "hugegraph.name" -}}
{{- default "hugegraph" .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "hugegraph.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default "hugegraph" .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "hugegraph.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "hugegraph.labels" -}}
helm.sh/chart: {{ include "hugegraph.chart" . }}
{{ include "hugegraph.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "hugegraph.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hugegraph.name" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{- end }}

{{- define "hugegraph.pd.name" -}}
{{- printf "%s-pd" (include "hugegraph.fullname" . | trunc 57 | trimSuffix "-") }}
{{- end }}

{{- define "hugegraph.pd.clientName" -}}
{{- printf "%s-pd-client" (include "hugegraph.fullname" . | trunc 53 | trimSuffix "-") }}
{{- end }}

{{- define "hugegraph.store.name" -}}
{{- printf "%s-store" (include "hugegraph.fullname" . | trunc 54 | trimSuffix "-") }}
{{- end }}

{{- define "hugegraph.server.name" -}}
{{- printf "%s-server" (include "hugegraph.fullname" . | trunc 56 | trimSuffix "-") }}
{{- end }}

{{- define "hugegraph.server.headlessName" -}}
{{- printf "%s-server-headless" (include "hugegraph.fullname" . | trunc 47 | trimSuffix "-") }}
{{- end }}

{{- define "hugegraph.hubble.name" -}}
{{- printf "%s-hubble" (include "hugegraph.fullname" . | trunc 56 | trimSuffix "-") }}
{{- end }}

{{- define "hugegraph.hubble.dataName" -}}
{{- printf "%s-hubble-data" (include "hugegraph.fullname" . | trunc 51 | trimSuffix "-") }}
{{- end }}

{{/*
PD and Store pod securityContext. The images run as UID/GID 10001, so their data
volumes need fsGroup 10001. Apply it here as well as in values.yaml: --reuse-values
keeps a pre-existing release's empty value. An explicit fsGroup is kept as set.
*/}}
{{- define "hugegraph.dataPodSecurityContext" -}}
{{- $context := deepCopy (. | default dict) -}}
{{- if not (hasKey $context "fsGroup") -}}
{{- $_ := set $context "fsGroup" 10001 -}}
{{- if not (hasKey $context "fsGroupChangePolicy") -}}
{{- $_ := set $context "fsGroupChangePolicy" "OnRootMismatch" -}}
{{- end -}}
{{- end -}}
{{- toYaml $context -}}
{{- end }}

{{- define "hugegraph.test.name" -}}
{{- printf "%s-test-connection" (include "hugegraph.fullname" . | trunc 47 | trimSuffix "-") }}
{{- end }}

{{/*
Resolve the Server authentication Secret. A user-provided Secret always wins;
otherwise use a stable chart-managed name so the generated Secret can survive
uninstall and be reused by a later install of the same release.
*/}}
{{- define "hugegraph.server.authSecretName" -}}
{{- $auth := get .Values.server "auth" | default dict -}}
{{- $admin := get $auth "admin" | default dict -}}
{{- $existingSecret := get $admin "existingSecret" | default "" -}}
{{- if $existingSecret -}}
{{- $existingSecret -}}
{{- else -}}
{{- printf "%s-admin" (.Release.Name | trunc 55 | trimSuffix "-") -}}
{{- end -}}
{{- end }}

{{- define "hugegraph.server.authSecretKey" -}}
{{- $auth := get .Values.server "auth" | default dict -}}
{{- $admin := get $auth "admin" | default dict -}}
{{- get $admin "key" | default "password" -}}
{{- end }}

{{/*
Return the chart-managed admin password (base64). Inline admin.password wins
on first write; otherwise lookup keeps upgrades from rotating a generated
credential. Never used for an external existingSecret.
*/}}
{{- define "hugegraph.server.authSecretPassword" -}}
{{- $auth := get .Values.server "auth" | default dict -}}
{{- $admin := get $auth "admin" | default dict -}}
{{- $password := get $admin "password" | default "" -}}
{{- if $password -}}
{{- $password | b64enc -}}
{{- else -}}
{{- $key := include "hugegraph.server.authSecretKey" . -}}
{{- $secret := lookup "v1" "Secret" .Release.Namespace (include "hugegraph.server.authSecretName" .) -}}
{{- if and $secret (hasKey $secret "data") (hasKey (get $secret "data") $key) -}}
{{- get (get $secret "data") $key -}}
{{- else -}}
{{- include "hugegraph.generatedCredential" (dict "root" . "name" "server-admin") | b64enc -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
One random credential per render, by name. randAlphaNum returns a different
value on every call, and the Secret template and the rollout checksum both
need the value the install is about to write, so the first call generates it
and later calls read it back from a scratch map kept on .Values for the
duration of this render (it is not part of the stored release values; the
next render finds the Secret through lookup instead).
*/}}
{{- define "hugegraph.generatedCredential" -}}
{{- if not (hasKey .root.Values "__generatedCredentials") -}}{{- $_ := set .root.Values "__generatedCredentials" dict -}}{{- end -}}
{{- $memo := get .root.Values "__generatedCredentials" -}}
{{- if not (hasKey $memo .name) -}}{{- $_ := set $memo .name (randAlphaNum (int (get . "length" | default 32))) -}}{{- end -}}
{{- get $memo .name -}}
{{- end }}

{{/*
Resolve the JWT signing Secret. User-provided token.existingSecret wins;
otherwise use a stable chart-managed name so every Server replica shares one key.
*/}}
{{- define "hugegraph.server.authTokenSecretName" -}}
{{- $auth := get .Values.server "auth" | default dict -}}
{{- $token := get $auth "token" | default dict -}}
{{- $existing := get $token "existingSecret" | default "" -}}
{{- if $existing -}}
{{- $existing -}}
{{- else -}}
{{- printf "%s-auth-token" (.Release.Name | trunc 51 | trimSuffix "-") -}}
{{- end -}}
{{- end }}

{{- define "hugegraph.server.authTokenSecretKey" -}}
{{- $auth := get .Values.server "auth" | default dict -}}
{{- $token := get $auth "token" | default dict -}}
{{- get $token "key" | default "token_secret" -}}
{{- end }}

{{/*
Return the chart-managed JWT signing secret (base64). Inline token.value wins
on first write; otherwise lookup keeps multi-replica pods and upgrades on the
same signing key.
*/}}
{{- define "hugegraph.server.authTokenSecretValue" -}}
{{- $auth := get .Values.server "auth" | default dict -}}
{{- $token := get $auth "token" | default dict -}}
{{- $value := get $token "value" | default "" -}}
{{- if $value -}}
{{- $value | b64enc -}}
{{- else -}}
{{- $name := include "hugegraph.server.authTokenSecretName" . -}}
{{- $key := include "hugegraph.server.authTokenSecretKey" . -}}
{{- $secret := lookup "v1" "Secret" .Release.Namespace $name -}}
{{- if and $secret (hasKey $secret "data") (hasKey (get $secret "data") $key) -}}
{{- get (get $secret "data") $key -}}
{{- else -}}
{{- include "hugegraph.generatedCredential" (dict "root" . "name" "server-token") | b64enc -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
Resolve the PD REST auth Secret. User-provided pd.auth.existingSecret wins;
otherwise a stable chart-managed name shared by PD, the Server storage wait
and Hubble.
*/}}
{{- define "hugegraph.pd.authSecretName" -}}
{{- $auth := get .Values.pd "auth" | default dict -}}
{{- $existing := get $auth "existingSecret" | default "" -}}
{{- if $existing -}}
{{- $existing -}}
{{- else -}}
{{- printf "%s-pd-auth" (.Release.Name | trunc 55 | trimSuffix "-") -}}
{{- end -}}
{{- end }}

{{- define "hugegraph.pd.authSecretKey" -}}
{{- $auth := get .Values.pd "auth" | default dict -}}
{{- get $auth "key" | default "secret-key" -}}
{{- end }}

{{/*
Return the chart-managed PD REST secret (base64). Inline pd.auth.value wins
on first write; otherwise lookup keeps every PD, Server and Hubble Pod, and
every upgrade, on the same secret.
*/}}
{{- define "hugegraph.pd.authSecretValue" -}}
{{- $auth := get .Values.pd "auth" | default dict -}}
{{- $value := get $auth "value" | default "" -}}
{{- if $value -}}
{{- $value | b64enc -}}
{{- else -}}
{{- $name := include "hugegraph.pd.authSecretName" . -}}
{{- $key := include "hugegraph.pd.authSecretKey" . -}}
{{- $secret := lookup "v1" "Secret" .Release.Namespace $name -}}
{{- if and $secret (hasKey $secret "data") (hasKey (get $secret "data") $key) -}}
{{- get (get $secret "data") $key -}}
{{- else -}}
{{- include "hugegraph.generatedCredential" (dict "root" . "name" "pd-auth") | b64enc -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
Checksum for the PD, Server and Hubble pod templates so rotating the PD REST
Secret rolls the Pods that read it. It hashes the Secret name, its key, and
one revision input chosen by where the credential comes from.

An active inline value is known at render time, so its digest is the
revision: it changes exactly once, on the upgrade that rotates it. Mixing the
live resourceVersion into that case would roll the Pods a second time on the
next no-change upgrade, once the rotated Secret had been applied and its
resourceVersion moved.

A chart-generated Secret contributes the digest of its value. At install
that is the value the Secret template is about to write (generated once per
render, see hugegraph.generatedCredential); on every later render it is the
live Secret's data, so the first no-change upgrade after an install renders
the same checksum and rolls nothing. The value is a 32-character random, so
its digest in a Pod annotation discloses nothing usable. The alternative, the
live resourceVersion, differs between install (no Secret yet) and the first
upgrade, which rolled PD and Server together on a no-change upgrade and let
a Server start against a PD member mid-restart (apache/hugegraph#3228).

An operator-supplied existingSecret keeps the live resourceVersion as its
revision: its value may be weak enough for a digest to be worth attacking,
and that Secret exists before the install, so install and upgrade already
agree. Template-only renders (helm template, GitOps renderers) see no live
Secret and generate a fresh value on every render; they are unsupported
with chart-generated credentials, see README, Template-only pipelines.
*/}}
{{- define "hugegraph.pd.authChecksum" -}}
{{- $parts := list (include "hugegraph.pd.authSecretName" .) (include "hugegraph.pd.authSecretKey" .) -}}
{{- $pdAuthCfg := get .Values.pd "auth" | default dict -}}
{{- $inline := get $pdAuthCfg "value" | default "" -}}
{{- $existing := get $pdAuthCfg "existingSecret" | default "" -}}
{{- if $existing -}}
{{- $secret := lookup "v1" "Secret" .Release.Namespace $existing -}}
{{- if $secret -}}{{- $parts = append $parts (dig "metadata" "resourceVersion" "" $secret) -}}{{- end -}}
{{- else if $inline -}}
{{- $parts = append $parts (sha256sum $inline) -}}
{{- else -}}
{{- $parts = append $parts (include "hugegraph.pd.authSecretValue" . | b64dec | sha256sum) -}}
{{- end -}}
{{- join "|" $parts | sha256sum -}}
{{- end }}

{{/*
Path the PD startup and liveness probes hit.

/v1/health stays 200 during elections and quorum loss, and images with the
apache/hugegraph#3222 fix answer 503 on fatal raft errors or shutdown. Losing
leadership alone must not restart followers and turn an election into a
rolling outage. Readiness carries the leader-availability signal instead.

A single PD has no election to lose. There, a PD that steps down and cannot
recover, as after a failed snapshot on a full disk
(apache/hugegraph#3222), keeps answering /v1/health forever on older images.
For compatibility with those images, the default moves to /v1/ready when
pd.replicas is 1.

The startup probe follows this path as well. Kubernetes suppresses liveness
until the startup probe succeeds, so leaving startup on /v1/health would give
a single PD only the liveness budget (60 s by default) to reach raft
readiness after a restart, and a slow log replay would crash-loop instead of
booting. Following the same path puts that window inside the startup budget
(300 s by default) instead.

Setting pd.livenessPath overrides the choice in both places. Drop the
single-PD readiness derivation once the minimum supported image includes
the /v1/health fatal raft error fix.
*/}}
{{- define "hugegraph.pd.livenessPath" -}}
{{- $explicit := get .Values.pd "livenessPath" | default "" -}}
{{- if $explicit -}}
{{- $explicit -}}
{{- else if eq (int .Values.pd.replicas) 1 -}}
/v1/ready
{{- else -}}
/v1/health
{{- end -}}
{{- end }}

{{/*
PD Raft peers list: pod-0.svc.ns.svc:8610,...
Uses short headless DNS (cluster.local optional) resolvable inside the namespace.
*/}}
{{- define "hugegraph.pd.raftPeersList" -}}
{{- $peers := list -}}
{{- $replicas := int .Values.pd.replicas -}}
{{- $name := include "hugegraph.pd.name" . -}}
{{- $ns := .Release.Namespace -}}
{{- $port := int .Values.pd.ports.raft -}}
{{- range $i := until $replicas -}}
  {{- $peers = append $peers (printf "%s-%d.%s.%s.svc:%d" $name $i $name $ns $port) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end }}

{{/*
PD gRPC peers for Store/Server.
*/}}
{{- define "hugegraph.pd.grpcPeersList" -}}
{{- $peers := list -}}
{{- $replicas := int .Values.pd.replicas -}}
{{- $name := include "hugegraph.pd.name" . -}}
{{- $ns := .Release.Namespace -}}
{{- $port := int .Values.pd.ports.grpc -}}
{{- range $i := until $replicas -}}
  {{- $peers = append $peers (printf "%s-%d.%s.%s.svc:%d" $name $i $name $ns $port) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end }}

{{/*
PD REST endpoints for Server storage-readiness checks.
*/}}
{{- define "hugegraph.pd.restPeersList" -}}
{{- $peers := list -}}
{{- $replicas := int .Values.pd.replicas -}}
{{- $name := include "hugegraph.pd.name" . -}}
{{- $ns := .Release.Namespace -}}
{{- $port := int .Values.pd.ports.rest -}}
{{- range $i := until $replicas -}}
  {{- $peers = append $peers (printf "%s-%d.%s.%s.svc:%d" $name $i $name $ns $port) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end }}

{{/*
Checksum for the Server pod template so rotating the referenced auth Secrets
rolls Server pods. The admin and token credentials pick their revision input
independently, by source, for the reasons given on hugegraph.pd.authChecksum:
an active inline value and a chart-generated Secret contribute the digest of
their value (so a rotation rolls Server exactly once, and the first no-change
upgrade after an install rolls nothing); an operator-supplied existingSecret
contributes its live metadata.resourceVersion, and its out-of-band rotation
applies on the next `helm upgrade`. Template-only renders are unsupported
with chart-generated credentials, see README, Template-only pipelines.
*/}}
{{- define "hugegraph.server.authChecksum" -}}
{{- $parts := list (include "hugegraph.server.authSecretName" .) (include "hugegraph.server.authSecretKey" .) (include "hugegraph.server.authTokenSecretName" .) (include "hugegraph.server.authTokenSecretKey" .) -}}
{{- $srvAuth := get .Values.server "auth" | default dict -}}
{{- $adminCfg := get $srvAuth "admin" | default dict -}}
{{- $inlineAdmin := get $adminCfg "password" | default "" -}}
{{- $existingAdmin := get $adminCfg "existingSecret" | default "" -}}
{{- $tokenCfg := get $srvAuth "token" | default dict -}}
{{- $inlineToken := get $tokenCfg "value" | default "" -}}
{{- $existingToken := get $tokenCfg "existingSecret" | default "" -}}
{{- if $existingAdmin -}}
{{- $admin := lookup "v1" "Secret" .Release.Namespace $existingAdmin -}}
{{- if $admin -}}{{- $parts = append $parts (dig "metadata" "resourceVersion" "" $admin) -}}{{- end -}}
{{- else if $inlineAdmin -}}
{{- $parts = append $parts (sha256sum $inlineAdmin) -}}
{{- else -}}
{{- $parts = append $parts (include "hugegraph.server.authSecretPassword" . | b64dec | sha256sum) -}}
{{- end -}}
{{- if $existingToken -}}
{{- $token := lookup "v1" "Secret" .Release.Namespace $existingToken -}}
{{- if $token -}}{{- $parts = append $parts (dig "metadata" "resourceVersion" "" $token) -}}{{- end -}}
{{- else if $inlineToken -}}
{{- $parts = append $parts (sha256sum $inlineToken) -}}
{{- else -}}
{{- $parts = append $parts (include "hugegraph.server.authTokenSecretValue" . | b64dec | sha256sum) -}}
{{- end -}}
{{- join "|" $parts | sha256sum -}}
{{- end }}

{{/*
Initial store list for PD bootstrap: store-0.svc.ns.svc:8500,...
*/}}
{{- define "hugegraph.store.initialStoreList" -}}
{{- $peers := list -}}
{{- $replicas := int .Values.store.replicas -}}
{{- $name := include "hugegraph.store.name" . -}}
{{- $ns := .Release.Namespace -}}
{{- $port := int .Values.store.ports.grpc -}}
{{- range $i := until $replicas -}}
  {{- $peers = append $peers (printf "%s-%d.%s.%s.svc:%d" $name $i $name $ns $port) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end }}

{{/*
First store REST endpoint for STORE_REST / wait-partition.
*/}}
{{- define "hugegraph.store.restPrimary" -}}
{{- $name := include "hugegraph.store.name" . -}}
{{- $ns := .Release.Namespace -}}
{{- printf "%s-0.%s.%s.svc:%d" $name $name $ns (int .Values.store.ports.rest) -}}
{{- end }}

{{/*
Server REST URL reached through the client Service.
*/}}
{{- define "hugegraph.server.clientUrl" -}}
{{- printf "http://%s.%s.svc:%d" (include "hugegraph.server.name" .) .Release.Namespace (int .Values.server.port) -}}
{{- end }}

{{/*
URL registered with PD (server.urls_to_pd / HG_SERVER_URLS_TO_PD).
server.advertiseUrl wins when set so outside PD-mode Hubble receives a reachable address; otherwise each Server Pod announces its own Pod IP so PD discovery preserves the replica list for in-cluster clients.
*/}}
{{- define "hugegraph.server.urlsToPd" -}}
{{- $advertise := trim (default "" .Values.server.advertiseUrl) -}}
{{- if $advertise -}}
{{- $advertise -}}
{{- else -}}
{{- printf "http://$(POD_IP):%d" (int .Values.server.port) -}}
{{- end -}}
{{- end }}

{{/*
PD REST endpoint reached through the client Service, for Hubble's pd.server.
*/}}
{{- define "hugegraph.pd.restClientEndpoint" -}}
{{- printf "%s.%s.svc:%d" (include "hugegraph.pd.clientName" .) .Release.Namespace (int .Values.pd.ports.rest) -}}
{{- end }}

{{/*
Store REST origins in Hubble's bracketed allow-list form:
[http://store-0.svc.ns.svc:8520,...]
*/}}
{{- define "hugegraph.store.restOriginsList" -}}
{{- $origins := list -}}
{{- $replicas := int .Values.store.replicas -}}
{{- $name := include "hugegraph.store.name" . -}}
{{- $ns := .Release.Namespace -}}
{{- $port := int .Values.store.ports.rest -}}
{{- range $i := until $replicas -}}
  {{- $origins = append $origins (printf "http://%s-%d.%s.%s.svc:%d" $name $i $name $ns $port) -}}
{{- end -}}
{{- printf "[%s]" (join "," $origins) -}}
{{- end }}

{{/*
Quorum size: floor(replicas/2)+1
*/}}
{{- define "hugegraph.pd.quorum" -}}
{{- add (div (int .Values.pd.replicas) 2) 1 -}}
{{- end }}

{{/*
Render JAVA_OPTS only when explicitly configured. An empty value preserves the
image entrypoint's existing automatic JVM sizing behavior.
*/}}
{{- define "hugegraph.javaOptsEnv" -}}
{{- $javaOpts := default "" . -}}
{{- if ne (trim $javaOpts) "" -}}
- name: JAVA_OPTS
  value: {{ $javaOpts | quote }}
{{- end -}}
{{- end }}

{{/*
String form of a possibly-absent scalar value, preserving zero. sprig's
`default` treats 0 as unset, which would let a zero slip past the named
validation below, so absence is detected explicitly instead. Numbers from a
values file arrive as float64, whose toString switches to scientific
notation at 1e6 or higher (1000000 becomes "1e+06"), so integral float64
values are formatted without an exponent. Non-integral floats keep their
raw form on purpose: the schema already rejects them, and the raw form
fails the named validation instead of being silently rounded.
*/}}
{{- define "hugegraph.optionalScalar" -}}
{{- if not (kindIs "invalid" .) -}}
{{- if and (kindIs "float64" .) (eq (floor .) .) -}}{{- printf "%.0f" . -}}{{- else -}}{{- trim (toString .) -}}{{- end -}}
{{- end -}}
{{- end }}

{{/*
Effective PD JAVA_OPTS: chart-derived -D system properties, then pd.javaOpts.

The -D route is the grounded override mechanism: the PD image's
docker-entrypoint.sh forwards JAVA_OPTS via `-j` into
bin/start-hugegraph-pd.sh, which places it on the java command line ahead of
-Dspring.config.location, and Spring system properties outrank the shipped
conf/application.yml (which pins partition.default-shard-count to 1).

The shard count is always derived, so PD Pods always carry a JAVA_OPTS
variable, which shadows the PD image's `ENV JAVA_OPTS` default
(-XX:MaxRAMPercentage=50, -XX:+UseContainerSupport, -XshowSettings:vm).
That is acceptable: the start script always computes explicit -Xms/-Xmx
heap flags when the separate JAVA_OPTIONS variable is unset, which makes
MaxRAMPercentage moot, and only the -XshowSettings:vm startup diagnostics
are lost.

JVM auto-sizing is preserved, verified against the PD dist start script:
`-j` lands in USER_OPTION, while the automatic heap sizing branch is gated on
the separate JAVA_OPTIONS variable and appends USER_OPTION after the computed
-Xms/-Xmx flags. A JAVA_OPTS holding only -D flags therefore still gets
automatic heap sizing, and heap flags in pd.javaOpts land later on the
command line, so they win. The derived -D flags come first for the same
reason: an explicit duplicate in pd.javaOpts overrides them.

Both -D properties seed PD's persisted config at first bootstrap only:
ConfigService.loadConfig persists them when no stored config exists, and
every leader change re-reads the stored values (updatePDConfig), so on an
initialized cluster the flags are inert and the authoritative values live
in PD metadata, changeable only through PD's own config API. PD reconciles
existing shard groups toward the stored value when a partition patrol is
triggered (TaskScheduleService reallocShards). The empty-value derivation
is 3 when store.replicas is at least 3, else 1. PD requires an odd
replica count and rejects allocations with insufficient eligible Stores. store-max-shard-count is rendered only when set,
keeping the image default. All lookups tolerate absent keys so releases
stored before these values existed keep rendering under --reuse-values.

raft.ip-whitelist.enabled is always rendered, default false: PD resolves its
raft peer allowlist once at boot, which under Kubernetes blocks peers whose
pod IPs were unpublished at that moment or change later, so the switch is
off in-cluster per the upstream design and k8s auth owns that layer. Images
without the property ignore the flag. Set pd.raftIpWhitelistEnabled=true to
restore the image default.

raft.rpc-timeout is a plain runtime property, applied on every start rather
than seeded at bootstrap; empty preserves the image default. It bounds raft
requests between PDs, and on images that wait for a raft ReadIndex before
they read an id counter, that wait. Images that have raft.rpc-connect-timeout
use that option for the Ping that opens a connection to a peer (default 1000,
set through pd.javaOpts); it does not bound requests on a connection that is
already open. On older images raft.rpc-timeout is the timeout of that Ping as
well, which is what a leader election waited on.
*/}}
{{- define "hugegraph.pd.effectiveJavaOpts" -}}
{{- $pd := .Values.pd -}}
{{- $partition := get $pd "partition" | default dict -}}
{{- $flags := list -}}
{{- $shardCount := include "hugegraph.optionalScalar" (get $partition "defaultShardCount") -}}
{{- if eq $shardCount "" -}}
{{- $shardCount = ternary "3" "1" (ge (int .Values.store.replicas) 3) -}}
{{- end -}}
{{- $flags = append $flags (printf "-Dpartition.default-shard-count=%s" $shardCount) -}}
{{- $maxShard := include "hugegraph.optionalScalar" (get $partition "storeMaxShardCount") -}}
{{- if ne $maxShard "" -}}
{{- $flags = append $flags (printf "-Dpartition.store-max-shard-count=%s" $maxShard) -}}
{{- end -}}
{{- $ipWhitelist := ternary "true" "false" (eq (get $pd "raftIpWhitelistEnabled" | toString) "true") -}}
{{- $flags = append $flags (printf "-Draft.ip-whitelist.enabled=%s" $ipWhitelist) -}}
{{- $rpcTimeout := include "hugegraph.optionalScalar" (get $pd "raftRpcTimeoutMs") -}}
{{- if ne $rpcTimeout "" -}}
{{- $flags = append $flags (printf "-Draft.rpc-timeout=%s" $rpcTimeout) -}}
{{- end -}}
{{- $userOpts := trim (get $pd "javaOpts" | default "") -}}
{{- if ne $userOpts "" -}}
{{- $flags = append $flags $userOpts -}}
{{- end -}}
{{- join " " $flags -}}
{{- end }}

{{/*
Unspecified probes reserve storageWaitTimeoutSeconds plus 150 seconds.
An explicitly configured threshold is preserved and checked by validateValues.
Kubelet may probe immediately: the budget is (failureThreshold - 1) * periodSeconds.
*/}}
{{- define "hugegraph.server.startupFailureThreshold" -}}
{{- .Values.server.probes.startup.failureThreshold -}}
{{- end }}

{{/*
Seconds the chart gives the Server image to finish starting, passed as
HG_SERVER_STARTUP_TIMEOUT_S. The image defaults that to 120 seconds, which is
shorter than the storage wait alone, so a Server still coming up kills itself
before Kubernetes has given up on it. The value therefore tracks the startup
probe's guaranteed alive time, (failureThreshold - 1) * periodSeconds,
because the first probe can fail immediately, minus the configured storage
wait the entrypoint runs before the start command, so the start command and
kubelet give up together instead of the image outliving the probe. Floored
at the image's own 120-second default, and capped at the entrypoint's 86400
maximum rather than rendered into a Pod that refuses to start.
*/}}
{{- define "hugegraph.server.startupProbeBudget" -}}
{{- $startup := .Values.server.probes.startup -}}
{{- add (int ($startup.initialDelaySeconds | default 0)) (mul (sub (int $startup.failureThreshold) 1) (int $startup.periodSeconds)) -}}
{{- end }}

{{- define "hugegraph.server.startupTimeoutSeconds" -}}
{{- $budget := include "hugegraph.server.startupProbeBudget" . | int -}}
{{- min 86400 (max 120 (sub $budget (int .Values.server.storageWaitTimeoutSeconds))) -}}
{{- end }}

{{/*
Optional probe tunables, emitted only when explicitly set. Kubernetes defaults
timeoutSeconds to 1 second, which a garbage-collection pause can exceed on a
loaded Server; operators need a supported way to raise it without forking the
chart. Only explicitly configured fields are rendered.
*/}}
{{- define "hugegraph.probeTuning" -}}
{{- if hasKey . "timeoutSeconds" }}
timeoutSeconds: {{ .timeoutSeconds }}
{{- end }}
{{- if hasKey . "initialDelaySeconds" }}
initialDelaySeconds: {{ .initialDelaySeconds }}
{{- end }}
{{- if hasKey . "successThreshold" }}
successThreshold: {{ .successThreshold }}
{{- end }}
{{- end }}

{{/*
Resolve the ServiceAccount name for a component: an explicit name wins,
otherwise the generated one when create is true, otherwise "default".
*/}}
{{- define "hugegraph.serviceAccountName" -}}
{{- $sa := get .component "serviceAccount" | default dict -}}
{{- if get $sa "name" -}}
{{- get $sa "name" -}}
{{- else if (get $sa "create" | default false) -}}
{{- .name -}}
{{- else -}}
default
{{- end -}}
{{- end }}

{{/*
The minimum Server replica count that a PDB must remain valid against.
*/}}
{{- define "hugegraph.server.replicaFloor" -}}
{{- if .Values.server.hpa.enabled -}}
{{- .Values.server.hpa.minReplicas -}}
{{- else -}}
{{- .Values.server.replicas -}}
{{- end -}}
{{- end }}

{{/*
Cross-field validation that JSON Schema draft-07 cannot express.
*/}}
{{- define "hugegraph.validateValues" -}}
{{- include "hugegraph.resolveValues" . -}}
{{- include "hugegraph.nativeServices.validate" . -}}
{{- include "hugegraph.metadataCluster.validate" . -}}
{{- include "hugegraph.vermeer.validate" . -}}
{{- if (.Values.seatunnel | default dict).enabled -}}
{{- if not (has .Values.seatunnel.mode (list "standalone" "cluster")) -}}
{{- fail "seatunnel.mode must be standalone or cluster" -}}
{{- end -}}
{{- if and (eq .Values.seatunnel.mode "cluster") (lt (int .Values.seatunnel.replicas) 2) -}}
{{- fail "seatunnel.replicas must be at least 2 in cluster mode" -}}
{{- end -}}
{{- end -}}

{{- include "hugegraph.resolveValues" . -}}
{{- if (.Values.seatunnel | default dict).enabled -}}
{{- $svc := .Values.seatunnel.service -}}
{{- range $key := list "username" "password" -}}
{{- $value := get ($.Values.seatunnel.auth | default dict) $key | default "" -}}
{{- if or (ne $value (trim $value)) (contains "${" $value) -}}
{{- fail (printf "seatunnel.auth.%s cannot contain ${...} placeholders or leading/trailing whitespace: native Engine YAML expands or trims them" $key) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $startupBudget := include "hugegraph.server.startupProbeBudget" . | int -}}
{{- if lt $startupBudget (add (int .Values.server.storageWaitTimeoutSeconds) 120) -}}
{{- fail "server.probes.startup.failureThreshold and periodSeconds must leave at least server.storageWaitTimeoutSeconds + 120 seconds to wait for storage and start Server; increase the probe budget or reduce server.storageWaitTimeoutSeconds" -}}
{{- end -}}
{{- range $comp := list "pd" "store" "server" "hubble" -}}
{{- $compLabels := get (get $.Values $comp | default dict) "podLabels" | default dict -}}
{{- range $reserved := list "app.kubernetes.io/name" "app.kubernetes.io/instance" "app.kubernetes.io/component" -}}
{{- if hasKey $compLabels $reserved -}}
{{- fail (printf "%s.podLabels must not set %s: the chart manages it and the workload selectors, Services and PDBs match on it" $comp $reserved) -}}
{{- end -}}
{{- end -}}
{{/* User pod annotations render after the chart's own, and the Kubernetes
     decoder keeps the last duplicate key, so a fixed checksum/* value would
     replace the rendered checksum and pin it: rotating a Secret or changing
     config would no longer roll the pods. */}}
{{- $compAnnotations := get (get $.Values $comp | default dict) "podAnnotations" | default dict -}}
{{- range $key, $_ := $compAnnotations -}}
{{- if hasPrefix "checksum/" $key -}}
{{- fail (printf "%s.podAnnotations must not set %s: the chart owns the checksum/ annotation prefix, which triggers pod rollouts when resolved Secrets or config change" $comp $key) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if and .Values.server.hpa.enabled (gt (int .Values.server.hpa.minReplicas) (int .Values.server.hpa.maxReplicas)) -}}
{{- fail "server.hpa.minReplicas must be less than or equal to server.hpa.maxReplicas" -}}
{{- end -}}
{{- if .Values.server.hpa.enabled -}}
{{- $serverResources := .Values.server.resources | default dict -}}
{{- $serverRequests := get $serverResources "requests" | default dict -}}
{{- if not (hasKey $serverRequests "cpu") -}}
{{- fail "server.resources.requests.cpu is required when server.hpa.enabled=true" -}}
{{- end -}}
{{- $cpuRequest := trim (toString (get $serverRequests "cpu")) -}}
{{- if or (eq $cpuRequest "") (hasPrefix "-" $cpuRequest) (regexMatch "^[+]?((0+([.]0*)?)|([.]0+))(([KMGTPE]i)|[numkMGTPE]|[eE][+-]?[0-9]+)?$" $cpuRequest) -}}
{{- fail "server.resources.requests.cpu must be strictly positive when server.hpa.enabled=true" -}}
{{- end -}}
{{- end -}}
{{/*
Raft and shard membership are persisted; deleting Pods does not reconfigure
them, so an in-place replica shrink permanently loses PD quorum or Store
shard majorities. The guard reads the live StatefulSet, so it fires only on
a real upgrade against a cluster; template-only renders have no live object
and skip it. An operator who has completed the documented manual scale-down
procedure has already scaled the live StatefulSet, so desired equals live
and the upgrade passes.
*/}}
{{- if .Release.IsUpgrade -}}
{{- range $comp := list "pd" "store" -}}
{{- $stsName := "" -}}
{{- if eq $comp "pd" -}}{{- $stsName = include "hugegraph.pd.name" $ -}}{{- else -}}{{- $stsName = include "hugegraph.store.name" $ -}}{{- end -}}
{{- $live := lookup "apps/v1" "StatefulSet" $.Release.Namespace $stsName -}}
{{- if $live -}}
{{- $wasPersistent := false -}}
{{- range $claim := get (get $live "spec") "volumeClaimTemplates" | default list -}}
{{- if eq (dig "metadata" "name" "" $claim) (printf "%s-data" $comp) -}}{{- $wasPersistent = true -}}{{- end -}}
{{- end -}}
{{- if ne $wasPersistent (get (get (get $.Values $comp) "storage") "enabled") -}}
{{- fail (printf "%s.storage.enabled cannot change on an initialized release: StatefulSet volumeClaimTemplates are immutable and changing storage can discard data. Use a fresh release and migrate data" $comp) -}}
{{- end -}}

{{- $liveReplicas := int (dig "spec" "replicas" 0 $live) -}}
{{- $desired := int (get (get $.Values $comp) "replicas") -}}
{{- if and (gt $liveReplicas 0) (lt $desired $liveReplicas) -}}
{{- $procedure := ternary "Reconfigure the persisted PD voting membership before scaling the live StatefulSet" "Retire leaving Store IDs and migrate their shards before scaling the live StatefulSet" (eq $comp "pd") -}}
{{- fail (printf "%s.replicas cannot shrink from %d to %d through a helm upgrade: persisted membership is not changed by removing Pods. %s; see docs/operations.md (Topology and scheduling). The upgrade passes once live replicas match the value" $comp $liveReplicas $desired $procedure) -}}
{{- end -}}
{{/*
PD raft membership is the persisted voting configuration, and the peer list
the chart renders reaches it only as NodeOptions.setInitialConf, which jraft
applies when bootstrapping a node that has no configuration of its own. On an
initialized group, adding Pods adds non-voting strangers: the extra PD starts,
the peer list changes, and the voting configuration does not. PD exposes the
change through RaftEngine.changePeerList, reachable from the PD client API but
from no REST route, so the chart cannot perform it and does not pretend to.
Growing Store is ordinary scale-out and stays allowed.
*/}}
{{- if and (eq $comp "pd") (gt $liveReplicas 0) (gt $desired $liveReplicas) -}}
{{- fail (printf "pd.replicas cannot grow from %d to %d through a helm upgrade: the rendered peer list reaches raft only as the initial configuration, so new Pods would start without joining the voting configuration. Change the persisted membership through PD first, then scale the live StatefulSet, then upgrade with the matching value; see docs/operations.md (Topology and scheduling) for the supported lifecycle. A fresh install at any replica count is unaffected" $liveReplicas $desired) -}}
{{- end -}}
{{/*
Raft identity. Every PD and Store names itself to its peers as
<pod>.<statefulset>.<namespace>.svc:<raft port>, and that address is what the
persisted membership records (PD's voting configuration, each Store shard
group's peer list). The port reaches raft only through the bootstrap
configuration, and the StatefulSet name is the DNS name, so a Pod restarted
on a changed port is a stranger to the group it belongs to: it cannot rejoin,
and once enough Pods have been replaced the group has no quorum. The live
StatefulSet carries the port it was initialized with, so a change is refused
there; a fresh install chooses freely.
*/}}
{{- $desiredRaft := int (get (get (get $.Values $comp) "ports" | default dict) "raft") -}}
{{- range $container := dig "spec" "template" "spec" "containers" list $live -}}
{{- if eq (get $container "name" | default "") $comp -}}
{{- range $port := get $container "ports" | default list -}}
{{- if and (eq (get $port "name" | default "") "raft") (ne (int (get $port "containerPort")) $desiredRaft) -}}
{{- fail (printf "%s.ports.raft cannot change from %d to %d on an initialized release: the raft address <pod>.%s.<namespace>.svc:<port> is the identity the persisted raft membership records, and a Pod restarted on the new port cannot rejoin its group%s. Keep the value; changing the port needs a fresh install, or a migration of the persisted membership before the upgrade" $comp (int (get $port "containerPort")) $desiredRaft $stsName (ternary " (a single PD loses service outright)" "" (eq $comp "pd"))) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{/*
nameOverride also feeds the app.kubernetes.io/name selector label. A
StatefulSet selector is immutable, so an override that keeps the name (one
contained in the release name) but changes that label is refused by the API
server mid-upgrade; refuse it here with the reason instead.
*/}}
{{- $liveName := dig "spec" "selector" "matchLabels" "app.kubernetes.io/name" "" $live -}}
{{- if and $liveName (ne $liveName (include "hugegraph.name" $)) -}}
{{- fail (printf "nameOverride changes the app.kubernetes.io/name selector label of the %s StatefulSet %s from %s to %s, which Kubernetes forbids on an existing StatefulSet. Restore the previous override; a rename needs a fresh install and a data migration" $comp $stsName $liveName (include "hugegraph.name" $)) -}}
{{- end -}}
{{- end -}}
{{/*
The same identity is carried by the StatefulSet name, which nameOverride and
fullnameOverride change. The renamed StatefulSet does not exist yet, so it
cannot be looked up by name; the release's existing StatefulSets are found by
the instance and component labels instead, which do not depend on the name.
*/}}
{{- $released := lookup "apps/v1" "StatefulSet" $.Release.Namespace "" -}}
{{- range $sts := get $released "items" | default list -}}
{{- $labels := dig "metadata" "labels" dict $sts -}}
{{- if and (eq (get $labels "app.kubernetes.io/instance" | default "") $.Release.Name) (eq (get $labels "app.kubernetes.io/component" | default "") $comp) (ne (dig "metadata" "name" "" $sts) $stsName) -}}
{{- fail (printf "release %s already owns the %s StatefulSet %s, but the values now name it %s (nameOverride or fullnameOverride changed): the StatefulSet name is the raft address of every Pod and the prefix of every PersistentVolumeClaim, so the rename would start empty Pods under new identities and leave the data volumes detached. Restore the previous override; a rename needs a fresh install and a data migration" $.Release.Name $comp (dig "metadata" "name" "" $sts) $stsName) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $hubbleLive := lookup "apps/v1" "Deployment" .Release.Namespace (include "hugegraph.hubble.name" .) -}}
{{- if and $hubbleLive .Values.hubble.enabled -}}
{{- $wasPersistent := false -}}
{{- $hasDataVolume := false -}}
{{- range $volume := dig "spec" "template" "spec" "volumes" list $hubbleLive -}}
{{- if eq ($volume.name | default "") "data" -}}{{- $hasDataVolume = true -}}{{- end -}}
{{- if and (eq ($volume.name | default "") "data") (hasKey $volume "persistentVolumeClaim") -}}{{- $wasPersistent = true -}}{{- end -}}
{{- end -}}
{{- if ne $wasPersistent .Values.hubble.persistence.enabled -}}
{{- if not $hasDataVolume -}}
{{- fail "hubble.persistence.enabled cannot change on an initialized release: legacy Hubble has no data volume and its UI records are in the container filesystem. Keep hubble.persistence.enabled=false with --reuse-values or an explicit GitOps value; enabling persistence requires a reviewed metadata migration first" -}}
{{- end -}}
{{- fail "hubble.persistence.enabled cannot change on an initialized release: changing storage can discard UI records. Migrate the data into a fresh deployment first" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{/*
Only validate minAvailable where a PDB is actually rendered. The pd/store PDB
templates require replicas > 1, so a single-replica release never creates one
and must not be failed for a value that has no effect.
*/}}
{{- if and .Values.pd.pdb.enabled (gt (int .Values.pd.replicas) 1) (ge (int .Values.pd.pdb.minAvailable) (int .Values.pd.replicas)) -}}
{{- fail "pd.pdb.minAvailable must be less than pd.replicas, otherwise the PDB permanently blocks voluntary disruptions such as node drains" -}}
{{- end -}}
{{- if and .Values.pd.pdb.enabled (gt (int .Values.pd.replicas) 1) (lt (int .Values.pd.pdb.minAvailable) (include "hugegraph.pd.quorum" . | int)) -}}
{{- fail "pd.pdb.minAvailable must be at least the PD Raft majority, floor(replicas/2)+1, otherwise the budget permits voluntary disruptions that drop PD below quorum. Note a PDB only limits voluntary disruption such as drains and evictions; it cannot protect quorum from node failure" -}}
{{- end -}}
{{- if and .Values.store.pdb.enabled (gt (int .Values.store.replicas) 1) (ge (int .Values.store.pdb.minAvailable) (int .Values.store.replicas)) -}}
{{- fail "store.pdb.minAvailable must be less than store.replicas, otherwise the PDB permanently blocks voluntary disruptions such as node drains" -}}
{{- end -}}
{{- if and .Values.store.pdb.enabled (gt (int .Values.store.replicas) 1) (lt (int .Values.store.pdb.minAvailable) (sub (int .Values.store.replicas) 1)) -}}
{{- fail "store.pdb.minAvailable must be at least store.replicas - 1: each shard keeps its copies on a subset of the Stores, so permitting more than one concurrent voluntary eviction can remove a shard majority regardless of the Store count" -}}
{{- end -}}
{{/*
PD -D system properties must be empty or a positive integer. The schema
enforces the types; these checks add a named failure for zero, negative, and
nonsense values, and check an explicit shard count against PD's real
constraints: PD accepts only positive odd shard counts and rejects
placement when there are fewer eligible Stores than requested replicas. All lookups tolerate absent
keys for releases stored before the values existed.
*/}}
{{- $pdPartition := get .Values.pd "partition" | default dict -}}
{{- $pdProps := dict
      "pd.partition.defaultShardCount" (include "hugegraph.optionalScalar" (get $pdPartition "defaultShardCount"))
      "pd.partition.storeMaxShardCount" (include "hugegraph.optionalScalar" (get $pdPartition "storeMaxShardCount")) -}}
{{- range $label, $raw := $pdProps -}}
{{- if and (ne $raw "") (or (not (regexMatch "^[0-9]+$" $raw)) (eq (int $raw) 0)) -}}
{{- fail (printf "%s must be empty or a positive integer" $label) -}}
{{- end -}}
{{- end -}}
{{- $explicitShards := include "hugegraph.optionalScalar" (get $pdPartition "defaultShardCount") -}}
{{- if regexMatch "^[1-9][0-9]*$" $explicitShards -}}
{{- if eq (mod (int $explicitShards) 2) 0 -}}
{{- fail "pd.partition.defaultShardCount must be odd: PD rejects even shard counts because they cannot provide an odd voting majority" -}}
{{- end -}}
{{- if gt (int $explicitShards) (int .Values.store.replicas) -}}
{{- fail "pd.partition.defaultShardCount is greater than store.replicas: PD rejects placement without enough eligible Stores. Raise store.replicas or lower the shard count" -}}
{{- end -}}
{{- end -}}
{{- $svc := get .Values.server "service" | default dict -}}
{{- if and (get $svc "nodePort") (not (has (get $svc "type" | default "ClusterIP") (list "NodePort" "LoadBalancer"))) -}}
{{- fail "server.service.nodePort requires server.service.type to be NodePort or LoadBalancer" -}}
{{- end -}}
{{- $advertiseUrl := trim (default "" .Values.server.advertiseUrl) -}}
{{- if and $advertiseUrl (not (or (hasPrefix "http://" $advertiseUrl) (hasPrefix "https://" $advertiseUrl))) -}}
{{- fail "server.advertiseUrl must be an absolute http:// or https:// URL when set" -}}
{{- end -}}
{{- $pdSvc := get .Values.pd "service" | default dict -}}
{{- $pdSvcType := get $pdSvc "type" | default "ClusterIP" -}}
{{- if and (or (get $pdSvc "restNodePort") (get $pdSvc "grpcNodePort")) (not (has $pdSvcType (list "NodePort" "LoadBalancer"))) -}}
{{- fail "pd.service.restNodePort and pd.service.grpcNodePort require pd.service.type to be NodePort or LoadBalancer" -}}
{{- end -}}
{{- $serverPdb := get .Values.server "pdb" | default dict -}}
{{- $serverReplicaFloor := include "hugegraph.server.replicaFloor" . | int -}}
{{- if and (get $serverPdb "enabled" | default false) (gt $serverReplicaFloor 1) (ge (int (get $serverPdb "minAvailable" | default 1)) $serverReplicaFloor) -}}
{{- fail "server.pdb.minAvailable must be less than the active Server replica floor (server.hpa.minReplicas when HPA is enabled, otherwise server.replicas), otherwise the PDB permanently blocks voluntary disruptions such as node drains" -}}
{{- end -}}
{{- $serverIngress := get .Values.server "ingress" | default dict -}}
{{- if get (get .Values.server "securityContext" | default dict) "readOnlyRootFilesystem" | default false -}}
{{- fail "server.securityContext.readOnlyRootFilesystem=true breaks Server: its wrapper rewrites conf/rest-server.properties inside the image at startup and the chart mounts no writable volume there" -}}
{{- end -}}
{{/*
extraEnv entries render after the chart-owned variables and Kubernetes lets
the last duplicate win, so a duplicate name would silently override a
validated contract (for example re-enabling init-store across Server
replicas). Reserved names are rejected instead. JAVA_OPTIONS is reserved
for pd, store, and server because each component's start script skips its
automatic heap sizing and drops the chart's JAVA_OPTS flags entirely when
JAVA_OPTIONS arrives preset in the environment (verified in
start-hugegraph-pd.sh, start-hugegraph-store.sh, and hugegraph-server.sh).
*/}}
{{- $reservedEnv := dict
      "pd" (list "HG_PD_GRPC_HOST" "HG_PD_GRPC_PORT" "HG_PD_REST_PORT" "HG_PD_RAFT_ADDRESS" "HG_PD_RAFT_PEERS_LIST" "HG_PD_INITIAL_STORE_LIST" "HG_PD_INITIAL_STORE_COUNT" "HG_PD_DATA_PATH" "HG_PD_AUTH_SECRET_KEY" "JAVA_OPTS" "JAVA_OPTIONS")
      "store" (list "PD_CLUSTER" "HG_STORE_PD_ADDRESS" "HG_STORE_GRPC_HOST" "HG_STORE_GRPC_PORT" "HG_STORE_REST_PORT" "HG_STORE_RAFT_ADDRESS" "HG_STORE_DATA_PATH" "JAVA_OPTS" "JAVA_OPTIONS")
      "server" (list "HG_SERVER_CLUSTER" "POD_IP" "HG_SERVER_SERVICE_ID" "HG_SERVER_GRAPHSPACE" "SERVICE_ID" "GRAPH_SPACE" "HG_SERVER_BACKEND" "HG_SERVER_PD_PEERS" "HG_SERVER_PD_REST_ENDPOINT" "STORE_REST" "HG_SERVER_INIT_STORE_ENABLED" "HG_SERVER_URLS_TO_PD" "HG_SERVER_DEPLOY_IN_K8S" "HG_SERVER_STARTUP_TIMEOUT_S" "WAIT_STORAGE_TIMEOUT_S" "PD_AUTH_PASSWORD" "PASSWORD" "HG_SERVER_AUTH_TOKEN_SECRET" "JAVA_OPTS" "JAVA_OPTIONS")
      "hubble" (list "HG_HUBBLE_PD_PEERS" "HG_HUBBLE_PD_SERVER" "HG_HUBBLE_PD_PASSWORD" "HG_HUBBLE_STORE_TARGETS" "HG_HUBBLE_SERVER_URL" "HG_HUBBLE_VERMEER_URL" "HG_HUBBLE_VERMEER_TOKEN_FILE" "SPRING_DATASOURCE_URL") -}}
{{- range $component, $reserved := $reservedEnv -}}
{{- $componentValues := get $.Values $component | default dict -}}
{{- range $entry := get $componentValues "extraEnv" | default list -}}
{{- if has (get $entry "name") $reserved -}}
{{- fail (printf "%s.extraEnv must not set the chart-managed variable %s" $component (get $entry "name")) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $hubble := get .Values "hubble" | default dict -}}
{{- if get $hubble "enabled" | default false -}}
{{- if and (not .Values.server.auth.enabled) (not (get $hubble "allowWithoutServerAuth" | default false)) -}}
{{- fail "hubble.enabled requires server.auth: current Hubble images authenticate against the cluster and cannot complete their login on an auth-less deployment. Enable server.auth, or set hubble.allowWithoutServerAuth=true for images that support it" -}}
{{- end -}}
{{- $hubbleSvc := get $hubble "service" | default dict -}}
{{- if and (get $hubbleSvc "nodePort") (not (has (get $hubbleSvc "type" | default "ClusterIP") (list "NodePort" "LoadBalancer"))) -}}
{{- fail "hubble.service.nodePort requires hubble.service.type to be NodePort or LoadBalancer" -}}
{{- end -}}
{{- $hubbleImage := get $hubble "image" | default dict -}}
{{- if and (eq (trim (get $hubbleImage "tag" | default "")) "") (eq (trim (get $hubbleImage "digest" | default "")) "") -}}
{{- fail "hubble.image needs a tag or a digest: the chart appVersion tracks the Server release, not Hubble, so there is no meaningful fallback" -}}
{{- end -}}
{{- if get (get $hubble "securityContext" | default dict) "readOnlyRootFilesystem" | default false -}}
{{- fail "hubble.securityContext.readOnlyRootFilesystem=true breaks Hubble: its wrapper writes conf/hugegraph-hubble.properties inside the image at startup and the chart mounts no writable volume there" -}}
{{- end -}}
{{- $hubbleIngress := get $hubble "ingress" | default dict -}}
{{- end -}}
{{- $pdAuth := get .Values.pd "auth" | default dict -}}
{{/* A values set with no pd.auth block at all (a release stored before the
     field existed, replayed by --reuse-values) gets the chart default,
     autoGenerate; only an explicit autoGenerate=false with nothing else set
     is an error. */}}
{{- $pdAutoGen := ternary (get $pdAuth "autoGenerate") true (hasKey $pdAuth "autoGenerate") -}}
{{- if and (not (get $pdAuth "existingSecret" | default "")) (not (get $pdAuth "value" | default "")) (not $pdAutoGen) -}}
{{- fail "pd.auth requires existingSecret, value, or autoGenerate=true: PD refuses to start without a REST secret" -}}
{{- end -}}
{{- $auth := get .Values.server "auth" | default dict -}}
{{- $admin := get $auth "admin" | default dict -}}
{{- $token := get $auth "token" | default dict -}}
{{- if get $auth "enabled" | default false -}}
{{- if and (not (get $admin "existingSecret" | default "")) (not (get $admin "password" | default "")) (not (get $admin "autoGenerate" | default false)) -}}
{{- fail "server.auth.admin requires existingSecret, password, or autoGenerate=true when auth is enabled" -}}
{{- end -}}
{{- if and (not (get $token "existingSecret" | default "")) (not (get $token "value" | default "")) (not (get $token "autoGenerate" | default false)) -}}
{{- fail "server.auth.token requires existingSecret, value, or autoGenerate=true when auth is enabled" -}}
{{- end -}}
{{- end -}}

{{- end }}

{{/*
podAntiAffinity snippet for a component label key.
mode: required | preferred | disabled
*/}}
{{/*
NetworkPolicy building blocks: a same-release peer by component, a TCP port
list, and DNS egress by port only, so it works wherever the cluster runs its
resolver (CoreDNS in any namespace, NodeLocal DNSCache).
*/}}
{{- define "hugegraph.netpol.peer" -}}
- podSelector:
    matchLabels:
      {{- include "hugegraph.selectorLabels" .root | nindent 6 }}
      app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "hugegraph.netpol.ports" -}}
{{- $rules := list -}}
{{- range . -}}
{{- $rules = append $rules (printf "- protocol: TCP\n  port: %d" (int .)) -}}
{{- end -}}
{{- join "\n" $rules -}}
{{- end }}

{{/* Native SeaTunnel resources belong to this HugeGraph release. */}}
{{- define "hugegraph.seatunnel.name" -}}
{{- printf "%s-seatunnel" (include "hugegraph.fullname" . | trunc 44 | trimSuffix "-") -}}
{{- end }}

{{- define "hugegraph.netpol.seatunnelLabels" -}}
{{ include "hugegraph.selectorLabels" . }}
app.kubernetes.io/component: seatunnel
{{- end }}

{{- define "hugegraph.netpol.seatunnelPeer" -}}
- podSelector:
    matchLabels:
      {{- include "hugegraph.netpol.seatunnelLabels" . | nindent 6 }}
{{- end }}

{{- define "hugegraph.netpol.dns" -}}
- ports:
    - protocol: UDP
      port: 53
    - protocol: TCP
      port: 53
{{- end }}

{{- define "hugegraph.antiAffinity" -}}
{{- $mode := .mode -}}
{{- $component := .component -}}
{{- $labels := .labels -}}
{{- if eq $mode "required" }}
affinity:
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchLabels:
            {{- toYaml $labels | nindent 12 }}
            app.kubernetes.io/component: {{ $component }}
        topologyKey: kubernetes.io/hostname
{{- else if eq $mode "preferred" }}
affinity:
  podAntiAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        podAffinityTerm:
          labelSelector:
            matchLabels:
              {{- toYaml $labels | nindent 14 }}
              app.kubernetes.io/component: {{ $component }}
          topologyKey: kubernetes.io/hostname
{{- end }}
{{- end }}

{{/*
Render a container image reference. An explicit image.digest pins immutably and wins
over tag; otherwise fall back to tag, then to the chart appVersion. Takes a dict of
(image, appVersion).
*/}}
{{- define "hugegraph.image" -}}
{{- $img := .image -}}
{{- $digest := trim (get $img "digest" | default "") -}}
{{- if ne $digest "" -}}
{{- printf "%s@%s" $img.repository $digest -}}
{{- else -}}
{{- printf "%s:%s" $img.repository (default .appVersion $img.tag) -}}
{{- end -}}
{{- end }}

{{/* Native Vermeer identities stay stable across standalone/cluster mode changes. */}}
{{- define "hugegraph.vermeer.masterName" -}}
{{- printf "%s-vermeer-master" (include "hugegraph.fullname" . | trunc 39 | trimSuffix "-") -}}
{{- end }}
{{- define "hugegraph.vermeer.workerName" -}}
{{- printf "%s-vermeer-worker" (include "hugegraph.fullname" . | trunc 44 | trimSuffix "-") -}}
{{- end }}
{{- define "hugegraph.vermeer.configName" -}}
{{- printf "%s-vermeer-config" (include "hugegraph.fullname" . | trunc 48 | trimSuffix "-") -}}
{{- end }}
{{- define "hugegraph.vermeer.standaloneWorkerDataName" -}}
{{- printf "%s-vermeer-standalone-worker-data" (include "hugegraph.fullname" . | trunc 31 | trimSuffix "-") -}}
{{- end }}
{{/* Stable signing factor and rollout checksum; external Secrets contribute their revision. */}}
{{- define "hugegraph.vermeer.authSecretName" -}}
{{- $auth := get .Values.vermeer "auth" | default dict -}}
{{- $existing := get $auth "existingSecret" | default "" -}}
{{- if $existing -}}
{{- $existing -}}
{{- else -}}
{{- printf "%s-vermeer-auth" (.Release.Name | trunc 50 | trimSuffix "-") -}}
{{- end -}}
{{- end }}

{{- define "hugegraph.vermeer.authSecretKey" -}}
{{- $auth := get .Values.vermeer "auth" | default dict -}}
{{- get $auth "key" | default "token-factor" -}}
{{- end }}

{{- define "hugegraph.vermeer.authSecretValue" -}}
{{- $auth := get .Values.vermeer "auth" | default dict -}}
{{- $value := get $auth "value" | default "" -}}
{{- if $value -}}
{{- $value | b64enc -}}
{{- else -}}
{{- $name := include "hugegraph.vermeer.authSecretName" . -}}
{{- $key := include "hugegraph.vermeer.authSecretKey" . -}}
{{- $secret := lookup "v1" "Secret" .Release.Namespace $name -}}
{{- if and $secret (hasKey $secret "data") (hasKey (get $secret "data") $key) -}}
{{- get (get $secret "data") $key -}}
{{- else -}}
{{- include "hugegraph.generatedCredential" (dict "root" . "name" "vermeer-auth") | b64enc -}}
{{- end -}}
{{- end -}}
{{- end }}

{{- define "hugegraph.vermeer.authChecksum" -}}
{{- $parts := list (include "hugegraph.vermeer.authSecretName" .) (include "hugegraph.vermeer.authSecretKey" .) -}}
{{- $authCfg := get .Values.vermeer "auth" | default dict -}}
{{- $inline := get $authCfg "value" | default "" -}}
{{- $existing := get $authCfg "existingSecret" | default "" -}}
{{- if $existing -}}
{{- $secret := lookup "v1" "Secret" .Release.Namespace $existing -}}
{{- if $secret -}}{{- $parts = append $parts (dig "metadata" "resourceVersion" "" $secret) -}}{{- end -}}
{{- else if $inline -}}
{{- $parts = append $parts (sha256sum $inline) -}}
{{- else -}}
{{- $parts = append $parts (include "hugegraph.vermeer.authSecretValue" . | b64dec | sha256sum) -}}
{{- end -}}
{{- join "|" $parts | sha256sum -}}
{{- end }}


{{- define "hugegraph.vermeer.validate" -}}
{{- if (.Values.vermeer | default dict).enabled -}}
{{- $v := .Values.vermeer -}}
{{- $admin := $v.auth.admin -}}
{{- if and (not $admin.existingSecret) (not $admin.password) (not $admin.autoGenerate) -}}
{{- fail "vermeer.auth.admin requires existingSecret, password, or autoGenerate=true" -}}
{{- end -}}
{{- if and $admin.password (or (eq (trim $admin.password) "") (contains "\n" $admin.password) (contains "\r" $admin.password)) -}}
{{- fail "vermeer.auth.admin.password must be one nonblank UTF-8 line; whitespace is preserved" -}}
{{- end -}}
{{- if and .Values.hubble.enabled (ne (.Values.hubble.mode | default "pd") "pd") -}}
{{- fail "vermeer.enabled with Hubble requires hubble.mode=pd for graph-space discovery; disable Hubble to use Vermeer independently" -}}
{{- end -}}
{{- if .Values.hubble.enabled -}}
{{- $hubbleContainer := .Values.hubble.securityContext | default dict -}}
{{- $hubblePod := .Values.hubble.podSecurityContext | default dict -}}
{{- $nonRoot := get $hubblePod "runAsNonRoot" | default false -}}
{{- if hasKey $hubbleContainer "runAsNonRoot" -}}{{- $nonRoot = get $hubbleContainer "runAsNonRoot" -}}{{- end -}}
{{- if and $nonRoot (le (int (include "hugegraph.hubble.vermeerUid" .)) 0) -}}
{{- fail "Vermeer token files require an explicit positive hubble.securityContext.runAsUser or hubble.podSecurityContext.runAsUser when runAsNonRoot=true" -}}
{{- end -}}
{{- end -}}
{{- if not (has $v.mode (list "standalone" "cluster")) -}}
{{- fail "vermeer.mode must be standalone or cluster" -}}
{{- end -}}
{{- if ne .Values.server.backend "hstore" -}}
{{- fail "vermeer.enabled requires server.backend=hstore" -}}
{{- end -}}
{{- if and (not $v.auth.existingSecret) (not $v.auth.value) (not $v.auth.autoGenerate) -}}
{{- fail "vermeer.auth requires existingSecret, value, or autoGenerate=true" -}}
{{- end -}}
{{- if and (not $v.image.tag) (not $v.image.digest) -}}
{{- fail "vermeer.image needs a tag or digest matching the file-credential and token-helper interface" -}}
{{- end -}}
{{- $ports := list (int $v.master.httpPort) (int $v.master.grpcPort) (int $v.worker.httpPort) (int $v.worker.grpcPort) -}}
{{- if ne (len (uniq $ports)) 4 -}}
{{- fail "vermeer master and worker HTTP/gRPC ports must be distinct" -}}
{{- end -}}
{{- range $reserved := list "app.kubernetes.io/name" "app.kubernetes.io/instance" "app.kubernetes.io/component" -}}
{{- if hasKey $v.podLabels $reserved -}}
{{- fail (printf "vermeer.podLabels must not set %s" $reserved) -}}
{{- end -}}
{{- end -}}
{{- range $key, $_ := $v.podAnnotations -}}
{{- if hasPrefix "checksum/" $key -}}{{- fail "vermeer.podAnnotations must not set checksum/ annotations" -}}{{- end -}}
{{- end -}}
{{- range $role := list "master" "worker" -}}
{{- range $entry := (get $v $role).extraEnv -}}
{{- if has $entry.name (list "POD_IP" "VERMEER_AUTH_TOKEN_FACTOR_FILE" "VERMEER_ADMIN_PASSWORD_FILE" "VERMEER_PD_PASSWORD_FILE" "VERMEER_PD_PEERS" "VERMEER_HUGEGRAPH_USERNAME" "VERMEER_HUGEGRAPH_PASSWORD_FILE" "VERMEER_HUGEGRAPH_CREDENTIALS_FILE") -}}
{{- fail (printf "vermeer.%s.extraEnv must not set chart-managed variable %s" $role $entry.name) -}}
{{- end -}}
{{- end -}}
{{- if $.Release.IsUpgrade -}}
{{- $name := include "hugegraph.vermeer.masterName" $ -}}
{{- if eq $role "worker" -}}{{- $name = include "hugegraph.vermeer.workerName" $ -}}{{- end -}}
{{- $live := lookup "apps/v1" "StatefulSet" $.Release.Namespace $name -}}
{{- if $live -}}
{{- $wasPersistent := false -}}
{{- range $claim := dig "spec" "volumeClaimTemplates" list $live -}}
{{- if eq (dig "metadata" "name" "" $claim) (printf "%s-data" $role) -}}{{- $wasPersistent = true -}}{{- end -}}
{{- end -}}
{{- if ne $wasPersistent (get (get $v $role) "storage").enabled -}}
{{- fail (printf "vermeer.%s.storage.enabled cannot change on an initialized release; migrate retained data into a fresh workload" $role) -}}
{{- end -}}
{{- if eq $role "master" -}}
{{- range $volume := dig "spec" "template" "spec" "volumes" list $live -}}
{{- if and (eq $v.mode "standalone") (eq (get $volume "name") "worker-data") -}}
{{- $workerPersistent := hasKey $volume "persistentVolumeClaim" -}}
{{- if ne $workerPersistent $v.worker.storage.enabled -}}
{{- fail "vermeer.worker.storage.enabled cannot change on an initialized standalone release; migrate retained data first" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/* Server owns the initialized namespace; Store must use that same PD key prefix. */}}
{{- define "hugegraph.metadataCluster.validate" -}}
{{- if regexMatch "(^|[[:space:]\"'])-Dpd[.]cluster(=|[[:space:]\"']|$)" (.Values.store.javaOpts | default "") -}}
{{- fail "store.javaOpts must not set -Dpd.cluster; use shared metadataCluster so Server and Store read the same PD metadata namespace" -}}
{{- end -}}
{{- if .Release.IsUpgrade -}}
{{- $live := lookup "apps/v1" "Deployment" .Release.Namespace (include "hugegraph.server.name" .) -}}
{{- if $live -}}
{{- $desired := .Values.metadataCluster -}}
{{- $record := dig "metadata" "annotations" "hugegraph.apache.org/metadata-cluster" "" $live -}}
{{- if and $record (ne $record $desired) -}}
{{- fail "metadataCluster cannot change from the recorded Server metadata namespace: changing PD keys does not migrate existing metadata; retain the initialized value" -}}
{{- end -}}
{{- $observed := dict "explicit" false "unknown" false -}}
{{- range $container := dig "spec" "template" "spec" "containers" list $live -}}
{{- if eq (get $container "name") "server" -}}
{{- range $entry := get $container "env" | default list -}}
{{- if eq (get $entry "name") "HG_SERVER_CLUSTER" -}}
{{- $_ := set $observed "explicit" true -}}
{{- if hasKey $entry "valueFrom" -}}
{{- $_ := set $observed "unknown" true -}}
{{- else if ne (get $entry "value" | default "") $desired -}}
{{- fail "metadataCluster cannot change from the existing Server HG_SERVER_CLUSTER value: changing PD keys does not migrate existing metadata; retain the initialized value" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if and (not $record) (or (get $observed "unknown") (and (not (get $observed "explicit")) (ne $desired "hg-test"))) -}}
{{- fail "metadataCluster adoption cannot verify the legacy Server namespace. First inspect the existing Server logs/configuration and record that same verified value in its hugegraph.apache.org/metadata-cluster Deployment annotation; then retain it as metadataCluster. This records existing metadata and does not migrate PD keys" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/* Native HTTP entries share mode defaults; internal RPC Services stay private. */}}
{{- define "hugegraph.nativeServices.validate" -}}
{{- range $component := list "seatunnel" "vermeer" -}}
{{- $values := get $.Values $component | default dict -}}
{{- if get $values "enabled" -}}
{{- $svc := get $values "service" | default dict -}}
{{- if not (has (get $svc "type") (list "ClusterIP" "NodePort")) -}}
{{- fail (printf "%s.service.type must be ClusterIP or NodePort" $component) -}}
{{- end -}}
{{- if and (get $svc "nodePort") (ne (get $svc "type") "NodePort") -}}
{{- fail (printf "%s.service.nodePort requires %s.service.type=NodePort" $component $component) -}}
{{- end -}}
{{- range $field := list "externalIPs" "externalTrafficPolicy" "loadBalancerClass" "loadBalancerIP" "loadBalancerSourceRanges" "allocateLoadBalancerNodePorts" -}}
{{- if hasKey $svc $field -}}
{{- fail (printf "%s.service.%s is not supported; native HTTP entries support ClusterIP or NodePort" $component $field) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/* Native UI password is distinct from the internal HMAC signing factor. */}}
{{- define "hugegraph.vermeer.adminSecretName" -}}
{{- $admin := .Values.vermeer.auth.admin -}}
{{- if $admin.existingSecret -}}{{- $admin.existingSecret -}}
{{- else -}}{{- printf "%s-vermeer-admin" (.Release.Name | trunc 49 | trimSuffix "-") -}}{{- end -}}
{{- end }}
{{- define "hugegraph.vermeer.adminSecretKey" -}}
{{- .Values.vermeer.auth.admin.key | default "password" -}}
{{- end }}
{{- define "hugegraph.vermeer.adminSecretValue" -}}
{{- $admin := .Values.vermeer.auth.admin -}}
{{- $name := include "hugegraph.vermeer.adminSecretName" . -}}
{{- $key := include "hugegraph.vermeer.adminSecretKey" . -}}
{{- if $admin.existingSecret -}}
{{- $secret := lookup "v1" "Secret" .Release.Namespace $name -}}
{{- if and $secret (hasKey $secret "data") (hasKey (get $secret "data") $key) -}}{{- get (get $secret "data") $key -}}{{- end -}}
{{- else if $admin.password -}}
{{- $admin.password | b64enc -}}
{{- else -}}
{{- $secret := lookup "v1" "Secret" .Release.Namespace $name -}}
{{- if and $secret (hasKey $secret "data") (hasKey (get $secret "data") $key) -}}
{{- get (get $secret "data") $key -}}
{{- else -}}
{{- $length := ternary 16 6 (eq (.Values.mode | default "easy") "strict") -}}
{{- include "hugegraph.generatedCredential" (dict "root" . "name" "vermeer-admin" "length" $length) | b64enc -}}
{{- end -}}
{{- end -}}
{{- end }}
{{- define "hugegraph.vermeer.adminChecksum" -}}
{{- $admin := .Values.vermeer.auth.admin -}}
{{- $parts := list (include "hugegraph.vermeer.adminSecretName" .) (include "hugegraph.vermeer.adminSecretKey" .) -}}
{{- if $admin.existingSecret -}}
{{- $secret := lookup "v1" "Secret" .Release.Namespace $admin.existingSecret -}}
{{- if $secret -}}{{- $parts = append $parts (dig "metadata" "resourceVersion" "" $secret) -}}{{- end -}}
{{- else if $admin.password -}}
{{- $parts = append $parts (sha256sum $admin.password) -}}
{{- else -}}
{{- $parts = append $parts (include "hugegraph.vermeer.adminSecretValue" . | b64dec | sha256sum) -}}
{{- end -}}
{{- join "|" $parts | sha256sum -}}
{{- end }}
