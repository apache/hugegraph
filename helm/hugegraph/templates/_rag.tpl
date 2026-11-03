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

{{- define "hugegraph.rag.name" -}}
{{- printf "%s-rag" (include "hugegraph.fullname" . | trunc 59 | trimSuffix "-") -}}
{{- end }}

{{- define "hugegraph.rag.dataName" -}}
{{- printf "%s-data" (include "hugegraph.rag.name" . | trunc 58 | trimSuffix "-") -}}
{{- end }}

{{- define "hugegraph.rag.replicas" -}}
{{- $rag := get .Values "rag" | default dict -}}
{{- if hasKey $rag "replicas" -}}{{- get $rag "replicas" -}}{{- else -}}1{{- end -}}
{{- end }}

{{- define "hugegraph.rag.port" -}}
{{- $rag := get .Values "rag" | default dict -}}
{{- if hasKey $rag "port" -}}{{- get $rag "port" -}}{{- else -}}8001{{- end -}}
{{- end }}

{{- define "hugegraph.rag.image" -}}
{{- $rag := get .Values "rag" | default dict -}}
{{- $image := deepCopy (get $rag "image" | default dict) -}}
{{- $registry := trimSuffix "/" (get (.Values.global | default dict) "imageRegistry" | default "") -}}
{{- $repository := get $image "repository" | default "hugegraph/rag" -}}
{{- $_ := set $image "repository" (include "hugegraph.registryImage" (dict "reference" $repository "registry" $registry)) -}}
{{- include "hugegraph.image" (dict "image" $image "appVersion" .Chart.AppVersion) -}}
{{- end }}

{{- define "hugegraph.rag.serviceType" -}}
{{- $rag := get .Values "rag" | default dict -}}
{{- $service := get $rag "service" | default dict -}}
{{- if and (hasKey $service "type") (ne (get $service "type" | toString) "<nil>") (ne (get $service "type" | toString) "") -}}
{{- get $service "type" -}}
{{- else -}}
ClusterIP
{{- end -}}
{{- end }}

{{- define "hugegraph.rag.graphUrl" -}}
{{- $rag := get .Values "rag" | default dict -}}
{{- $graph := get $rag "hugegraph" | default dict -}}
{{- $url := trim (get $graph "url" | default "") -}}
{{- if $url -}}
{{- $url -}}
{{- else -}}
{{- include "hugegraph.server.clientUrl" . | trimPrefix "http://" | trimPrefix "https://" -}}
{{- end -}}
{{- end }}

{{- define "hugegraph.rag.graphSpace" -}}
{{- $rag := get .Values "rag" | default dict -}}
{{- $graph := get $rag "hugegraph" | default dict -}}
{{- get $graph "graphSpace" | default .Values.server.graphSpace -}}
{{- end }}

{{- define "hugegraph.rag.passwordSecretName" -}}
{{- $rag := get .Values "rag" | default dict -}}
{{- $graph := get $rag "hugegraph" | default dict -}}
{{- $password := get $graph "passwordSecret" | default dict -}}
{{- $explicit := get $password "name" | default "" -}}
{{- if $explicit -}}
{{- $explicit -}}
{{- else if and (not (trim (get $graph "url" | default ""))) (get (.Values.server.auth | default dict) "enabled" | default false) -}}
{{- include "hugegraph.server.authSecretName" . -}}
{{- end -}}
{{- end }}

{{- define "hugegraph.rag.passwordSecretKey" -}}
{{- $rag := get .Values "rag" | default dict -}}
{{- $graph := get $rag "hugegraph" | default dict -}}
{{- $password := get $graph "passwordSecret" | default dict -}}
{{- $explicit := get $password "key" | default "" -}}
{{- if $explicit -}}
{{- $explicit -}}
{{- else if and (not (trim (get $graph "url" | default ""))) (get (.Values.server.auth | default dict) "enabled" | default false) -}}
{{- include "hugegraph.server.authSecretKey" . -}}
{{- else -}}
password
{{- end -}}
{{- end }}

{{- define "hugegraph.rag.claimName" -}}
{{- $rag := get .Values "rag" | default dict -}}
{{- $persistence := get $rag "persistence" | default dict -}}
{{- get $persistence "existingClaim" | default (include "hugegraph.rag.dataName" .) -}}
{{- end }}

{{- define "hugegraph.rag.validate" -}}
{{- $rag := get .Values "rag" | default dict -}}
{{- if get $rag "enabled" | default false -}}
{{- $replicas := include "hugegraph.rag.replicas" . | int -}}
{{- if or (lt $replicas 1) (gt $replicas 100) -}}
{{- fail "rag.replicas must be between 1 and 100" -}}
{{- end -}}
{{- $port := include "hugegraph.rag.port" . | int -}}
{{- if or (lt $port 1) (gt $port 65535) -}}
{{- fail "rag.port must be between 1 and 65535" -}}
{{- end -}}
{{- $service := get $rag "service" | default dict -}}
{{- $serviceType := include "hugegraph.rag.serviceType" . -}}
{{- if not (has $serviceType (list "ClusterIP" "NodePort" "LoadBalancer")) -}}
{{- fail "rag.service.type must be ClusterIP, NodePort or LoadBalancer" -}}
{{- end -}}
{{- $nodePort := int (get $service "nodePort" | default 0) -}}
{{- if and (ne $nodePort 0) (or (lt $nodePort 1) (gt $nodePort 65535)) -}}
{{- fail "rag.service.nodePort must be 0/unset or between 1 and 65535" -}}
{{- end -}}
{{- if and (ne $nodePort 0) (eq $serviceType "ClusterIP") -}}
{{- fail "rag.service.nodePort requires service.type NodePort or LoadBalancer" -}}
{{- end -}}
{{- $persistence := get $rag "persistence" | default dict -}}
{{- if get $persistence "enabled" | default false -}}
{{- $accessModes := get $persistence "accessModes" | default (list "ReadWriteOnce") -}}
{{- if and (gt $replicas 1) (not (get $persistence "existingClaim" | default "")) (not (has "ReadWriteMany" $accessModes)) -}}
{{- fail "rag.persistence.accessModes must include ReadWriteMany when the chart creates one claim for multiple RAG replicas; otherwise use an existing shared claim or keep rag.replicas=1" -}}
{{- end -}}
{{- end -}}
{{- $podLabels := get $rag "podLabels" | default dict -}}
{{- range $reserved := list "app.kubernetes.io/name" "app.kubernetes.io/instance" "app.kubernetes.io/component" -}}
{{- if hasKey $podLabels $reserved -}}
{{- fail (printf "rag.podLabels must not set %s because the chart owns workload selectors" $reserved) -}}
{{- end -}}
{{- end -}}
{{- range $key, $_ := get $rag "podAnnotations" | default dict -}}
{{- if hasPrefix "checksum/" $key -}}
{{- fail (printf "rag.podAnnotations must not set %s because the chart owns checksum annotations" $key) -}}
{{- end -}}
{{- end -}}
{{- range $index, $entry := get $rag "env" | default list -}}
{{- if not (get $entry "name" | default "") -}}
{{- fail (printf "rag.env[%d].name must not be empty" $index) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end }}
