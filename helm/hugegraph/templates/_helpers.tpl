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
Server REST URL reached through the client Service. Announced to PD via
server.urls_to_pd so PD-discovered clients (Hubble) get a resolvable address
instead of the in-pod 0.0.0.0 default.
*/}}
{{- define "hugegraph.server.clientUrl" -}}
{{- printf "http://%s.%s.svc:%d" (include "hugegraph.server.name" .) .Release.Namespace (int .Values.server.port) -}}
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
{{- $networkPolicy := get .Values "networkPolicy" | default dict -}}
{{- if (get $networkPolicy "enabled" | default false) -}}
{{- fail "networkPolicy.enabled=true is unsupported because this chart does not implement NetworkPolicy resources" -}}
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
Only validate minAvailable where a PDB is actually rendered. The pd/store PDB
templates require replicas > 1, so a single-replica release never creates one
and must not be failed for a value that has no effect.
*/}}
{{- if and .Values.pd.pdb.enabled (gt (int .Values.pd.replicas) 1) (ge (int .Values.pd.pdb.minAvailable) (int .Values.pd.replicas)) -}}
{{- fail "pd.pdb.minAvailable must be less than pd.replicas, otherwise the PDB permanently blocks voluntary disruptions such as node drains" -}}
{{- end -}}
{{- if and .Values.pd.pdb.enabled (gt (int .Values.pd.replicas) 1) (lt (int .Values.pd.pdb.minAvailable) (include "hugegraph.pd.quorum" . | int)) -}}
{{- fail "pd.pdb.minAvailable must be at least the PD Raft majority, floor(replicas/2)+1, otherwise the budget permits voluntary evictions that drop PD below quorum" -}}
{{- end -}}
{{- if and .Values.store.pdb.enabled (gt (int .Values.store.replicas) 1) (ge (int .Values.store.pdb.minAvailable) (int .Values.store.replicas)) -}}
{{- fail "store.pdb.minAvailable must be less than store.replicas, otherwise the PDB permanently blocks voluntary disruptions such as node drains" -}}
{{- end -}}
{{- $svc := get .Values.server "service" | default dict -}}
{{- if and (get $svc "nodePort") (not (has (get $svc "type" | default "ClusterIP") (list "NodePort" "LoadBalancer"))) -}}
{{- fail "server.service.nodePort requires server.service.type to be NodePort or LoadBalancer" -}}
{{- end -}}
{{- $serverPdb := get .Values.server "pdb" | default dict -}}
{{- $serverReplicaFloor := include "hugegraph.server.replicaFloor" . | int -}}
{{- if and (get $serverPdb "enabled" | default false) (gt $serverReplicaFloor 1) (ge (int (get $serverPdb "minAvailable" | default 1)) $serverReplicaFloor) -}}
{{- fail "server.pdb.minAvailable must be less than the active Server replica floor (server.hpa.minReplicas when HPA is enabled, otherwise server.replicas), otherwise the PDB permanently blocks voluntary disruptions such as node drains" -}}
{{- end -}}
{{- $serverIngress := get .Values.server "ingress" | default dict -}}
{{- if hasKey $serverIngress "allowPlainHttp" -}}
{{- fail "server.ingress.allowPlainHttp has no effect; the plain-HTTP opt-in applies to hubble.ingress only" -}}
{{- end -}}
{{/*
extraEnv entries render after the chart-owned variables and Kubernetes lets
the last duplicate win, so a duplicate name would silently override a
validated contract (for example re-enabling init-store across Server
replicas). Reserved names are rejected instead.
*/}}
{{- $reservedEnv := dict
      "pd" (list "HG_PD_GRPC_HOST" "HG_PD_GRPC_PORT" "HG_PD_REST_PORT" "HG_PD_RAFT_ADDRESS" "HG_PD_RAFT_PEERS_LIST" "HG_PD_INITIAL_STORE_LIST" "HG_PD_INITIAL_STORE_COUNT" "HG_PD_DATA_PATH" "JAVA_OPTS")
      "store" (list "HG_STORE_PD_ADDRESS" "HG_STORE_GRPC_HOST" "HG_STORE_GRPC_PORT" "HG_STORE_REST_PORT" "HG_STORE_RAFT_ADDRESS" "HG_STORE_DATA_PATH" "JAVA_OPTS")
      "server" (list "HG_SERVER_BACKEND" "HG_SERVER_PD_PEERS" "HG_SERVER_PD_REST_ENDPOINT" "STORE_REST" "HG_SERVER_INIT_STORE_ENABLED" "HG_SERVER_URLS_TO_PD" "PASSWORD" "JAVA_OPTS")
      "hubble" (list "HG_HUBBLE_PD_PEERS" "HG_HUBBLE_PD_SERVER" "HG_HUBBLE_STORE_TARGETS" "HG_HUBBLE_SERVER_URL" "SPRING_DATASOURCE_URL") -}}
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
{{- if eq (trim (get $hubbleImage "tag" | default "")) "" -}}
{{- fail "hubble.image.tag must not be empty: the chart appVersion tracks the Server release, not Hubble, so there is no meaningful fallback" -}}
{{- end -}}
{{- $hubbleIngress := get $hubble "ingress" | default dict -}}
{{- if and (get $hubbleIngress "enabled" | default false) (empty (get $hubbleIngress "tls")) (not (get $hubbleIngress "allowPlainHttp" | default false)) -}}
{{- fail "hubble.ingress.enabled without tls publishes the plain-HTTP, unauthenticated Hubble UI; configure hubble.ingress.tls, or set hubble.ingress.allowPlainHttp=true to accept that on a trusted network" -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
podAntiAffinity snippet for a component label key.
mode: required | preferred | disabled
*/}}
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
