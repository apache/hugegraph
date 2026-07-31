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
