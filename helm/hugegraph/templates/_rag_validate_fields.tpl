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

{{- define "hugegraph.rag.validate.fields" -}}
{{- $rag := get .Values "rag" | default dict -}}
{{- $allowedRagKeys := list "enabled" "replicas" "image" "port" "hugegraph" "resources" "podSecurityContext" "securityContext" "nodeSelector" "tolerations" "affinity" "topologySpreadConstraints" "priorityClassName" "podAnnotations" "podLabels" "env" "envFrom" "terminationGracePeriodSeconds" "serviceAccount" "service" "ingress" "persistence" "extraVolumes" "extraVolumeMounts" "probes" "networkPolicy" -}}
{{- range $key, $_ := $rag -}}
{{- if not (has $key $allowedRagKeys) -}}
{{- fail (printf "rag contains unsupported field %s" $key) -}}
{{- end -}}
{{- end -}}
{{- $image := get $rag "image" | default dict -}}
{{- range $key, $_ := $image -}}
{{- if not (has $key (list "repository" "tag" "digest" "pullPolicy")) -}}
{{- fail (printf "rag.image contains unsupported field %s" $key) -}}
{{- end -}}
{{- end -}}
{{- $graphKeys := list "autoConfigure" "url" "graphName" "graphSpace" "username" "passwordSecret" -}}
{{- $graphForKeys := get $rag "hugegraph" | default dict -}}
{{- range $key, $_ := $graphForKeys -}}
{{- if not (has $key $graphKeys) -}}
{{- fail (printf "rag.hugegraph contains unsupported field %s" $key) -}}
{{- end -}}
{{- end -}}
{{- $passwordForKeys := get $graphForKeys "passwordSecret" | default dict -}}
{{- range $key, $_ := $passwordForKeys -}}
{{- if not (has $key (list "name" "key")) -}}
{{- fail (printf "rag.hugegraph.passwordSecret contains unsupported field %s" $key) -}}
{{- end -}}
{{- end -}}
{{- $serviceForKeys := get $rag "service" | default dict -}}
{{- range $key, $_ := $serviceForKeys -}}
{{- if not (has $key (list "type" "nodePort" "annotations")) -}}
{{- fail (printf "rag.service contains unsupported field %s" $key) -}}
{{- end -}}
{{- end -}}
{{- $serviceAccountForKeys := get $rag "serviceAccount" | default dict -}}
{{- range $key, $_ := $serviceAccountForKeys -}}
{{- if not (has $key (list "create" "name" "annotations" "automountServiceAccountToken")) -}}
{{- fail (printf "rag.serviceAccount contains unsupported field %s" $key) -}}
{{- end -}}
{{- end -}}
{{- $persistenceForKeys := get $rag "persistence" | default dict -}}
{{- range $key, $_ := $persistenceForKeys -}}
{{- if not (has $key (list "enabled" "existingClaim" "size" "storageClassName" "accessModes" "mountPath")) -}}
{{- fail (printf "rag.persistence contains unsupported field %s" $key) -}}
{{- end -}}
{{- end -}}
{{- $ingressForKeys := get $rag "ingress" | default dict -}}
{{- range $key, $_ := $ingressForKeys -}}
{{- if not (has $key (list "enabled" "className" "annotations" "hosts" "tls")) -}}
{{- fail (printf "rag.ingress contains unsupported field %s" $key) -}}
{{- end -}}
{{- end -}}
{{- $networkPolicyForKeys := get $rag "networkPolicy" | default dict -}}
{{- range $key, $_ := $networkPolicyForKeys -}}
{{- if not (has $key (list "extraIngress" "extraEgress")) -}}
{{- fail (printf "rag.networkPolicy contains unsupported field %s" $key) -}}
{{- end -}}
{{- end -}}
{{- $probesForKeys := get $rag "probes" | default dict -}}
{{- range $key, $probe := $probesForKeys -}}
{{- if not (has $key (list "startup" "readiness" "liveness")) -}}
{{- fail (printf "rag.probes contains unsupported field %s" $key) -}}
{{- end -}}
{{- range $probeKey, $_ := $probe -}}
{{- if not (has $probeKey (list "path" "failureThreshold" "periodSeconds" "timeoutSeconds" "initialDelaySeconds" "successThreshold")) -}}
{{- fail (printf "rag.probes.%s contains unsupported field %s" $key $probeKey) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end }}
