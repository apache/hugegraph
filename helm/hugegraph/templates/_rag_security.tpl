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

{{- define "hugegraph.rag.podSecurityContext" -}}
{{- $rag := get .Values "rag" | default dict -}}
{{- $defaults := dict
      "runAsNonRoot" true
      "runAsUser" 1000
      "runAsGroup" 1000
      "fsGroup" 1000
      "fsGroupChangePolicy" "OnRootMismatch"
      "seccompProfile" (dict "type" "RuntimeDefault") -}}
{{- $overrides := deepCopy (get $rag "podSecurityContext" | default dict) -}}
{{- $context := mergeOverwrite (deepCopy $defaults) $overrides -}}
{{- range $field := list "runAsUser" "runAsGroup" "fsGroup" -}}
{{- if hasKey $overrides $field -}}
{{- $raw := get $overrides $field | toString -}}
{{- if or (eq $raw "<nil>") (eq (lower $raw) "auto") -}}
{{- $_ := unset $context $field -}}
{{- else -}}
{{- $_ := set $context $field (int (get $overrides $field)) -}}
{{- end -}}
{{- else if hasKey $context $field -}}
{{- $_ := set $context $field (int (get $context $field)) -}}
{{- end -}}
{{- end -}}
{{- if not (hasKey $context "fsGroup") -}}{{- $_ := unset $context "fsGroupChangePolicy" -}}{{- end -}}
{{- toYaml $context -}}
{{- end }}

{{- define "hugegraph.rag.securityContext" -}}
{{- $rag := get .Values "rag" | default dict -}}
{{- $defaults := dict
      "runAsNonRoot" true
      "runAsUser" 1000
      "runAsGroup" 1000
      "privileged" false
      "allowPrivilegeEscalation" false
      "capabilities" (dict "drop" (list "ALL"))
      "seccompProfile" (dict "type" "RuntimeDefault") -}}
{{- $overrides := deepCopy (get $rag "securityContext" | default dict) -}}
{{- $context := mergeOverwrite (deepCopy $defaults) $overrides -}}
{{- range $field := list "runAsUser" "runAsGroup" -}}
{{- if hasKey $overrides $field -}}
{{- $raw := get $overrides $field | toString -}}
{{- if or (eq $raw "<nil>") (eq (lower $raw) "auto") -}}
{{- $_ := unset $context $field -}}
{{- else -}}
{{- $_ := set $context $field (int (get $overrides $field)) -}}
{{- end -}}
{{- else if hasKey $context $field -}}
{{- $_ := set $context $field (int (get $context $field)) -}}
{{- end -}}
{{- end -}}
{{- toYaml $context -}}
{{- end }}
