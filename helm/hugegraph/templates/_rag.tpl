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
{{- include "hugegraph.server.clientUrl" . -}}
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
{{- if and (hasKey $persistence "enabled") (ne (get $persistence "enabled" | toString) "<nil>") -}}
{{- $enabled := get $persistence "enabled" -}}
{{- if not (kindIs "bool" $enabled) -}}
{{- fail "rag.persistence.enabled must be a boolean or null" -}}
{{- end -}}
{{- $enabled -}}
{{- else -}}
{{- $global := get .Values "persistence" | default dict -}}
{{- if and (hasKey $global "enabled") (ne (get $global "enabled" | toString) "<nil>") -}}
{{- get $global "enabled" -}}
{{- else -}}true{{- end -}}
{{- end -}}
{{- end }}

{{- define "hugegraph.rag.mountPath" -}}
{{- $rag := get .Values "rag" | default dict -}}
{{- $persistence := get $rag "persistence" | default dict -}}
{{- get $persistence "mountPath" | default "/var/lib/hugegraph-rag" -}}
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
