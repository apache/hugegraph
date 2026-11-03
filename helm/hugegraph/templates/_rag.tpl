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
{{- $explicit := trim (get $password "name" | default "") -}}
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
{{- $explicitName := trim (get $password "name" | default "") -}}
{{- $explicitKey := trim (get $password "key" | default "") -}}
{{- if $explicitKey -}}
{{- $explicitKey -}}
{{- else if $explicitName -}}
{{- if eq $explicitName (include "hugegraph.server.authSecretName" .) -}}
{{- include "hugegraph.server.authSecretKey" . -}}
{{- else -}}
password
{{- end -}}
{{- else if and (not (trim (get $graph "url" | default ""))) (get (.Values.server.auth | default dict) "enabled" | default false) -}}
{{- include "hugegraph.server.authSecretKey" . -}}
{{- else -}}
password
{{- end -}}
{{- end }}

{{- define "hugegraph.rag.persistenceEnabled" -}}
{{- $rag := get .Values "rag" | default dict -}}
{{- $persistence := get $rag "persistence" | default dict -}}
{{- if hasKey $persistence "enabled" -}}{{- get $persistence "enabled" -}}{{- else -}}true{{- end -}}
{{- end }}

{{- define "hugegraph.rag.mountPath" -}}
{{- $rag := get .Values "rag" | default dict -}}
{{- $persistence := get $rag "persistence" | default dict -}}
{{- get $persistence "mountPath" | default "/home/work/hugegraph-llm/src/hugegraph_llm/resources" -}}
{{- end }}

{{- define "hugegraph.rag.envPath" -}}
{{- if eq (include "hugegraph.rag.persistenceEnabled" .) "true" -}}
{{- printf "%s/.env" (include "hugegraph.rag.mountPath" . | trimSuffix "/") -}}
{{- else -}}
/tmp/hugegraph-llm.env
{{- end -}}
{{- end }}

{{- define "hugegraph.rag.promptConfigPath" -}}
{{- if eq (include "hugegraph.rag.persistenceEnabled" .) "true" -}}
{{- printf "%s/demo/config_prompt.yaml" (include "hugegraph.rag.mountPath" . | trimSuffix "/") -}}
{{- else -}}
/home/work/hugegraph-llm/src/hugegraph_llm/resources/demo/config_prompt.yaml
{{- end -}}
{{- end }}

{{- define "hugegraph.rag.claimName" -}}
{{- $rag := get .Values "rag" | default dict -}}
{{- $persistence := get $rag "persistence" | default dict -}}
{{- get $persistence "existingClaim" | default (include "hugegraph.rag.dataName" .) -}}
{{- end }}

{{- define "hugegraph.rag.podSecurityContext" -}}
{{- $rag := get .Values "rag" | default dict -}}
{{- $defaults := dict
      "runAsNonRoot" true
      "runAsUser" 1000
      "runAsGroup" 1000
      "fsGroup" 1000
      "fsGroupChangePolicy" "OnRootMismatch"
      "seccompProfile" (dict "type" "RuntimeDefault") -}}
{{- $context := mergeOverwrite (deepCopy $defaults) (deepCopy (get $rag "podSecurityContext" | default dict)) -}}
{{- range $field := list "runAsUser" "runAsGroup" "fsGroup" -}}
{{- if hasKey $context $field -}}{{- $_ := set $context $field (int (get $context $field)) -}}{{- end -}}
{{- end -}}
{{- toYaml $context -}}
{{- end }}

{{- define "hugegraph.rag.securityContext" -}}
{{- $rag := get .Values "rag" | default dict -}}
{{- $defaults := dict
      "runAsNonRoot" true
      "runAsUser" 1000
      "runAsGroup" 1000
      "allowPrivilegeEscalation" false
      "capabilities" (dict "drop" (list "ALL"))
      "seccompProfile" (dict "type" "RuntimeDefault") -}}
{{- $context := mergeOverwrite (deepCopy $defaults) (deepCopy (get $rag "securityContext" | default dict)) -}}
{{- range $field := list "runAsUser" "runAsGroup" -}}
{{- if hasKey $context $field -}}{{- $_ := set $context $field (int (get $context $field)) -}}{{- end -}}
{{- end -}}
{{- toYaml $context -}}
{{- end }}

{{- define "hugegraph.rag.validate" -}}
{{- $rag := get .Values "rag" | default dict -}}
{{- if get $rag "enabled" | default false -}}
{{- $rawReplicas := "1" -}}
{{- if hasKey $rag "replicas" -}}{{- $rawReplicas = get $rag "replicas" | toString -}}{{- end -}}
{{- if not (regexMatch "^[0-9]+$" $rawReplicas) -}}
{{- fail "rag.replicas must be an integer between 1 and 100" -}}
{{- end -}}
{{- $replicas := int $rawReplicas -}}
{{- if or (lt $replicas 1) (gt $replicas 100) -}}
{{- fail "rag.replicas must be between 1 and 100" -}}
{{- end -}}
{{- $rawPort := "8001" -}}
{{- if hasKey $rag "port" -}}{{- $rawPort = get $rag "port" | toString -}}{{- end -}}
{{- if not (regexMatch "^[0-9]+$" $rawPort) -}}
{{- fail "rag.port must be an integer between 1 and 65535" -}}
{{- end -}}
{{- $port := int $rawPort -}}
{{- if or (lt $port 1) (gt $port 65535) -}}
{{- fail "rag.port must be between 1 and 65535" -}}
{{- end -}}
{{- $service := get $rag "service" | default dict -}}
{{- $serviceType := include "hugegraph.rag.serviceType" . -}}
{{- if not (has $serviceType (list "ClusterIP" "NodePort" "LoadBalancer")) -}}
{{- fail "rag.service.type must be ClusterIP, NodePort or LoadBalancer" -}}
{{- end -}}
{{- $rawNodePort := get $service "nodePort" | default 0 | toString -}}
{{- if and (ne $rawNodePort "0") (not (regexMatch "^[0-9]+$" $rawNodePort)) -}}
{{- fail "rag.service.nodePort must be an integer" -}}
{{- end -}}
{{- $nodePort := int $rawNodePort -}}
{{- if and (ne $nodePort 0) (or (lt $nodePort 1) (gt $nodePort 65535)) -}}
{{- fail "rag.service.nodePort must be 0/unset or between 1 and 65535" -}}
{{- end -}}
{{- if and (ne $nodePort 0) (eq $serviceType "ClusterIP") -}}
{{- fail "rag.service.nodePort requires service.type NodePort or LoadBalancer" -}}
{{- end -}}
{{- $graph := get $rag "hugegraph" | default dict -}}
{{- $password := get $graph "passwordSecret" | default dict -}}
{{- if and (trim (get $password "key" | default "")) (not (trim (get $password "name" | default ""))) -}}
{{- fail "rag.hugegraph.passwordSecret.key requires passwordSecret.name" -}}
{{- end -}}
{{- $persistence := get $rag "persistence" | default dict -}}
{{- if eq (include "hugegraph.rag.persistenceEnabled" .) "true" -}}
{{- $mountPath := include "hugegraph.rag.mountPath" . -}}
{{- if or (not $mountPath) (not (hasPrefix "/" $mountPath)) -}}
{{- fail "rag.persistence.mountPath must be an absolute path" -}}
{{- end -}}
{{- $accessModes := get $persistence "accessModes" | default (list "ReadWriteOnce") -}}
{{- if and (gt $replicas 1) (not (get $persistence "existingClaim" | default "")) (not (has "ReadWriteMany" $accessModes)) -}}
{{- fail "rag.persistence.accessModes must include ReadWriteMany when the chart creates one claim for multiple RAG replicas; otherwise use an existing shared claim or keep rag.replicas=1" -}}
{{- end -}}
{{- end -}}
{{- $podContext := get $rag "podSecurityContext" | default dict -}}
{{- $containerContext := get $rag "securityContext" | default dict -}}
{{- range $scope, $context := dict "rag.podSecurityContext" $podContext "rag.securityContext" $containerContext -}}
{{- if and (hasKey $context "runAsNonRoot") (not (get $context "runAsNonRoot")) -}}
{{- fail (printf "%s.runAsNonRoot must stay true for the HugeGraph RAG image" $scope) -}}
{{- end -}}
{{- range $field := list "runAsUser" "runAsGroup" -}}
{{- if hasKey $context $field -}}
{{- $raw := get $context $field | toString -}}
{{- if not (regexMatch "^[1-9][0-9]*$" $raw) -}}
{{- fail (printf "%s.%s must be a positive integer" $scope $field) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if hasKey $podContext "fsGroup" -}}
{{- $rawFsGroup := get $podContext "fsGroup" | toString -}}
{{- if not (regexMatch "^[1-9][0-9]*$" $rawFsGroup) -}}
{{- fail "rag.podSecurityContext.fsGroup must be a positive integer" -}}
{{- end -}}
{{- end -}}
{{- if and (hasKey $containerContext "allowPrivilegeEscalation") (get $containerContext "allowPrivilegeEscalation") -}}
{{- fail "rag.securityContext.allowPrivilegeEscalation must stay false" -}}
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
{{- $seenEnv := dict -}}
{{- range $index, $entry := get $rag "env" | default list -}}
{{- $name := get $entry "name" | default "" | toString -}}
{{- if not $name -}}
{{- fail (printf "rag.env[%d].name must not be empty" $index) -}}
{{- end -}}
{{- if hasKey $seenEnv $name -}}
{{- fail (printf "rag.env contains duplicate name %s" $name) -}}
{{- end -}}
{{- $_ := set $seenEnv $name true -}}
{{- end -}}
{{- end -}}
{{- end }}
