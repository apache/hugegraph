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

{{/*
Expand the name of the chart.
*/}}
{{- define "hugegraph.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "hugegraph.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
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
app.kubernetes.io/name: {{ include "hugegraph.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
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

{{- define "hugegraph.hubble.name" -}}
{{- printf "%s-hubble" (include "hugegraph.fullname" . | trunc 56 | trimSuffix "-") }}
{{- end }}

{{- define "hugegraph.hubble.dataName" -}}
{{- printf "%s-hubble-data" (include "hugegraph.fullname" . | trunc 51 | trimSuffix "-") }}
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
{{- randAlphaNum 32 | b64enc -}}
{{- end -}}
{{- end -}}
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
{{- randAlphaNum 32 | b64enc -}}
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
{{- randAlphaNum 32 | b64enc -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
Checksum for the PD, Server and Hubble pod templates so rotating the PD REST
Secret rolls the Pods that read it. Never hashes Secret data: it hashes the
Secret name, its key, and one revision input chosen by where the credential
comes from.

The revision input is per credential source, because the two sources move at
different times. An active inline value is known at render time, so its
digest is the revision and it changes exactly once, on the upgrade that
rotates it. Mixing the live resourceVersion into that case would roll the
Pods a second time on the next no-change upgrade, once the rotated Secret had
been applied and its resourceVersion moved. An external or chart-generated
Secret has no render-time value to hash, so the live resourceVersion is the
only signal that it changed; there the lookup is kept and template-only
renders emit a constant.
*/}}
{{- define "hugegraph.pd.authChecksum" -}}
{{- $parts := list (include "hugegraph.pd.authSecretName" .) (include "hugegraph.pd.authSecretKey" .) -}}
{{- $pdAuthCfg := get .Values.pd "auth" | default dict -}}
{{- $inline := get $pdAuthCfg "value" | default "" -}}
{{- if and $inline (not (get $pdAuthCfg "existingSecret" | default "")) -}}
{{- $parts = append $parts (sha256sum $inline) -}}
{{- else -}}
{{- $secret := lookup "v1" "Secret" .Release.Namespace (include "hugegraph.pd.authSecretName" .) -}}
{{- if $secret -}}{{- $parts = append $parts (dig "metadata" "resourceVersion" "" $secret) -}}{{- end -}}
{{- end -}}
{{- join "|" $parts | sha256sum -}}
{{- end }}

{{/*
Path the PD startup and liveness probes hit.

/v1/health answers 200 as soon as the REST listener is up and never consults
raft, which is what a multi-PD deployment wants: losing leadership is normal
during an election, and restarting a follower for it would turn one election
into a rolling outage. Readiness carries the raft-aware signal instead.

A single PD has no election to lose. There, a PD that steps down and cannot
recover, as after a failed snapshot on a full disk
(apache/hugegraph#3222), keeps answering /v1/health forever and liveness
never restarts it, so the default moves to /v1/ready when pd.replicas is 1.

The startup probe follows this path as well. Kubernetes suppresses liveness
until the startup probe succeeds, so leaving startup on /v1/health would give
a single PD only the liveness budget (60 s by default) to reach raft
readiness after a restart, and a slow log replay would crash-loop instead of
booting. Following the same path puts that window inside the startup budget
(300 s by default) instead.

Setting pd.livenessPath overrides the choice in both places. If PD starts
answering 503 from /v1/health in this state, this value stops being needed.
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
rolls Server pods. Hashes Secret names and keys, never Secret data, so the
annotation carries no credential-derived material.

The admin and token credentials pick their revision input independently, by
source, for the reason given on hugegraph.pd.authChecksum: an active inline
value contributes its digest and nothing else, so a rotation rolls Server
once rather than again on the next no-change upgrade; an external or
chart-generated Secret contributes its live metadata.resourceVersion.

The lookup half is best-effort: plain `helm template` (and template-only
GitOps renderers) see no live Secrets and emit a constant; the first upgrade
after a fresh install rolls Server once as the checksum picks up the Secrets
created by that install; out-of-band rotation of an existingSecret applies on
the next `helm upgrade`.
*/}}
{{- define "hugegraph.server.authChecksum" -}}
{{- $parts := list (include "hugegraph.server.authSecretName" .) (include "hugegraph.server.authSecretKey" .) (include "hugegraph.server.authTokenSecretName" .) (include "hugegraph.server.authTokenSecretKey" .) -}}
{{- $srvAuth := get .Values.server "auth" | default dict -}}
{{- $adminCfg := get $srvAuth "admin" | default dict -}}
{{- $inlineAdmin := get $adminCfg "password" | default "" -}}
{{- if and $inlineAdmin (not (get $adminCfg "existingSecret" | default "")) -}}{{- $parts = append $parts (sha256sum $inlineAdmin) -}}{{- end -}}
{{- $tokenCfg := get $srvAuth "token" | default dict -}}
{{- $inlineToken := get $tokenCfg "value" | default "" -}}
{{- if and $inlineToken (not (get $tokenCfg "existingSecret" | default "")) -}}{{- $parts = append $parts (sha256sum $inlineToken) -}}{{- end -}}
{{- if not (and $inlineAdmin (not (get $adminCfg "existingSecret" | default ""))) -}}
{{- $admin := lookup "v1" "Secret" .Release.Namespace (include "hugegraph.server.authSecretName" .) -}}
{{- if $admin -}}{{- $parts = append $parts (dig "metadata" "resourceVersion" "" $admin) -}}{{- end -}}
{{- end -}}
{{- if not (and $inlineToken (not (get $tokenCfg "existingSecret" | default ""))) -}}
{{- $token := lookup "v1" "Secret" .Release.Namespace (include "hugegraph.server.authTokenSecretName" .) -}}
{{- if $token -}}{{- $parts = append $parts (dig "metadata" "resourceVersion" "" $token) -}}{{- end -}}
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
is 3 when store.replicas is at least 3, else 1, because PD clamps a shard
count of 2 to 1 (two shards cannot elect a leader) and its config API
accepts only odd values. store-max-shard-count is rendered only when set,
keeping the image default. All lookups tolerate absent keys so releases
stored before these values existed keep rendering under --reuse-values.

raft.ip-whitelist.enabled is always rendered, default false: PD resolves its
raft peer allowlist once at boot, which under Kubernetes blocks peers whose
pod IPs were unpublished at that moment or change later, so the switch is
off in-cluster per the upstream design and k8s auth owns that layer. Images
without the property ignore the flag. Set pd.raftIpWhitelistEnabled=true to
restore the image default.

raft.rpc-timeout is a plain runtime property, applied on every start rather
than seeded at bootstrap. It bounds how long the surviving PDs wait on a
peer that stopped answering without closing its sockets, which is what a
leader election waits on; empty preserves the image default.
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
Keep the startup probe alive for the 300-second storage wait, the Server's
120-second start timeout, and 30 seconds of process overhead. Older stored
values remain accepted, but their rendered threshold is raised to this floor.
*/}}
{{- define "hugegraph.server.startupFailureThreshold" -}}
{{- $period := int .Values.server.probes.startup.periodSeconds -}}
{{- $configured := int .Values.server.probes.startup.failureThreshold -}}
{{- $minimum := div (add 449 $period) $period -}}
{{- max $configured $minimum -}}
{{- end }}

{{/*
Seconds the chart gives the Server image to finish starting, passed as
HG_SERVER_STARTUP_TIMEOUT_S. The image defaults that to 120 seconds, which is
shorter than the storage wait alone, so a Server still coming up kills itself
before Kubernetes has given up on it. The value therefore tracks the startup
probe: the effective budget above (floored at 450 seconds) minus the
300-second storage wait the entrypoint runs first, so the start command and
kubelet give up together instead of the image outliving the probe. Floored
at the image's own 120-second default, and capped at the entrypoint's 86400
maximum rather than rendered into a Pod that refuses to start.
*/}}
{{- define "hugegraph.server.startupTimeoutSeconds" -}}
{{- $period := int .Values.server.probes.startup.periodSeconds -}}
{{- $threshold := include "hugegraph.server.startupFailureThreshold" . | int -}}
{{- min 86400 (max 120 (sub (mul $threshold $period) 300)) -}}
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
{{- range $comp := list "pd" "store" -}}
{{- $stsName := "" -}}
{{- if eq $comp "pd" -}}{{- $stsName = include "hugegraph.pd.name" $ -}}{{- else -}}{{- $stsName = include "hugegraph.store.name" $ -}}{{- end -}}
{{- $live := lookup "apps/v1" "StatefulSet" $.Release.Namespace $stsName -}}
{{- if $live -}}
{{- $liveReplicas := int (dig "spec" "replicas" 0 $live) -}}
{{- $desired := int (get (get $.Values $comp) "replicas") -}}
{{- if and (gt $liveReplicas 0) (lt $desired $liveReplicas) -}}
{{- fail (printf "%s.replicas cannot shrink from %d to %d through a helm upgrade: raft and shard membership are persisted, and removing Pods does not reconfigure them. Follow the manual scale-down procedure in the README (Scaling), which ends by scaling the live StatefulSet; the upgrade passes once the live replicas match the value" $comp $liveReplicas $desired) -}}
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
{{- fail (printf "pd.replicas cannot grow from %d to %d through a helm upgrade: the rendered peer list reaches raft only as the initial configuration, so new Pods would start without joining the voting configuration. Change the persisted membership through PD first, then scale the live StatefulSet, then upgrade with the matching value; the README (Scaling) has the procedure and its limits. A fresh install at any replica count is unaffected" $liveReplicas $desired) -}}
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
constraints: PD's config API accepts only odd shard counts, PD clamps a
count of 2 to 1 (two shards cannot elect a leader), and PD clamps the
effective count to the number of live stores. All lookups tolerate absent
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
{{- fail "pd.partition.defaultShardCount must be odd: PD's config API rejects even shard counts, and PD clamps a bootstrap value of 2 to 1 because two shards cannot elect a leader" -}}
{{- end -}}
{{- if gt (int $explicitShards) (int .Values.store.replicas) -}}
{{- fail "pd.partition.defaultShardCount is greater than store.replicas: PD would clamp the effective shard count to the number of live stores, so the extra replicas would silently never be placed. Raise store.replicas or lower the shard count" -}}
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
{{- if and (ne $pdSvcType "ClusterIP") (not (get $pdSvc "allowInsecureExposure" | default false)) -}}
{{- fail "pd.service.type NodePort or LoadBalancer exposes PD's unauthenticated gRPC port outside the cluster, raft membership RPCs included; keep ClusterIP, or set pd.service.allowInsecureExposure=true once reachability is restricted by other means (NetworkPolicy, load balancer allowlist, firewall)" -}}
{{- end -}}
{{- $serverPdb := get .Values.server "pdb" | default dict -}}
{{- $serverReplicaFloor := include "hugegraph.server.replicaFloor" . | int -}}
{{- if and (get $serverPdb "enabled" | default false) (gt $serverReplicaFloor 1) (ge (int (get $serverPdb "minAvailable" | default 1)) $serverReplicaFloor) -}}
{{- fail "server.pdb.minAvailable must be less than the active Server replica floor (server.hpa.minReplicas when HPA is enabled, otherwise server.replicas), otherwise the PDB permanently blocks voluntary disruptions such as node drains" -}}
{{- end -}}
{{- $serverIngress := get .Values.server "ingress" | default dict -}}
{{- if and (get $serverIngress "enabled" | default false) (empty (get $serverIngress "tls")) (not (get $serverIngress "allowPlainHttp" | default false)) -}}
{{- fail "server.ingress.enabled without tls publishes Basic-auth credentials and JWTs over plain HTTP; configure server.ingress.tls, or set server.ingress.allowPlainHttp=true to accept that on a trusted network" -}}
{{- end -}}
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
      "store" (list "HG_STORE_PD_ADDRESS" "HG_STORE_GRPC_HOST" "HG_STORE_GRPC_PORT" "HG_STORE_REST_PORT" "HG_STORE_RAFT_ADDRESS" "HG_STORE_DATA_PATH" "JAVA_OPTS" "JAVA_OPTIONS")
      "server" (list "POD_IP" "HG_SERVER_BACKEND" "HG_SERVER_PD_PEERS" "HG_SERVER_PD_REST_ENDPOINT" "STORE_REST" "HG_SERVER_INIT_STORE_ENABLED" "HG_SERVER_URLS_TO_PD" "HG_SERVER_STARTUP_TIMEOUT_S" "PD_AUTH_PASSWORD" "PASSWORD" "HG_SERVER_AUTH_TOKEN_SECRET" "JAVA_OPTS" "JAVA_OPTIONS")
      "hubble" (list "HG_HUBBLE_PD_PEERS" "HG_HUBBLE_PD_SERVER" "HG_HUBBLE_PD_PASSWORD" "HG_HUBBLE_STORE_TARGETS" "HG_HUBBLE_SERVER_URL" "SPRING_DATASOURCE_URL") -}}
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
{{- if and (get $hubbleIngress "enabled" | default false) (empty (get $hubbleIngress "tls")) (not (get $hubbleIngress "allowPlainHttp" | default false)) -}}
{{- fail "hubble.ingress.enabled without tls publishes the plain-HTTP, unauthenticated Hubble UI; configure hubble.ingress.tls, or set hubble.ingress.allowPlainHttp=true to accept that on a trusted network" -}}
{{- end -}}
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
{{/* NetworkPolicy exposure check runs last, so an exposure the other checks
     refuse (allowInsecureExposure, Ingress TLS) is reported first. */}}
{{- $networkPolicy := get .Values "networkPolicy" | default dict -}}
{{- if get $networkPolicy "enabled" -}}
{{- $exposed := dict
      "pd" (ne $pdSvcType "ClusterIP")
      "server" (or (ne (get $svc "type" | default "ClusterIP") "ClusterIP") (get $serverIngress "enabled" | default false) (ne $advertiseUrl ""))
      "hubble" (and (get $hubble "enabled" | default false) (or (ne (get (get $hubble "service" | default dict) "type" | default "ClusterIP") "ClusterIP") (get (get $hubble "ingress" | default dict) "enabled" | default false))) -}}
{{- range $comp := list "pd" "server" "hubble" -}}
{{- if and (get $exposed $comp) (empty (get (get $networkPolicy $comp | default dict) "extraIngress")) -}}
{{- fail (printf "networkPolicy.enabled admits nothing from outside the release, so the %s exposure (NodePort/LoadBalancer Service, Ingress%s) is unreachable; list its callers in networkPolicy.%s.extraIngress, for example the Ingress controller's namespace or a client CIDR" $comp (ternary ", server.advertiseUrl" "" (eq $comp "server")) $comp) -}}
{{- end -}}
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
