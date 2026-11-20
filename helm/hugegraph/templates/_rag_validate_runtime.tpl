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

{{- define "hugegraph.rag.validate.runtime" -}}
{{- $rag := get .Values "rag" | default dict -}}
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
{{- if and (ne $rawNodePort "0") (eq $nodePort 0) -}}
{{- fail "rag.service.nodePort must fit an integer and be 0/unset or between 1 and 65535" -}}
{{- end -}}
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
{{- $existingClaim := trim (get $persistence "existingClaim" | default "" | toString) -}}
{{- if not $existingClaim -}}
{{- $accessModes := get $persistence "accessModes" | default (list "ReadWriteOnce") -}}
{{- if and (has "ReadWriteOncePod" $accessModes) (ne (len $accessModes) 1) -}}
{{- fail "rag.persistence.accessModes must contain only ReadWriteOncePod when that mode is selected" -}}
{{- end -}}
{{- end -}}
{{- range $mount := get $rag "extraVolumeMounts" | default (list) -}}
{{- if eq (get $mount "mountPath" | default "" | toString) $mountPath -}}
{{- fail "rag.extraVolumeMounts must not use rag.persistence.mountPath when persistence is enabled" -}}
{{- end -}}
{{- end -}}
{{- range $volume := get $rag "extraVolumes" | default (list) -}}
{{- if eq (get $volume "name" | default "" | toString) "data" -}}
{{- fail "rag.extraVolumes must not use the reserved volume name data when persistence is enabled" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end }}
