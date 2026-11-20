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

{{- define "hugegraph.rag.validate.security" -}}
{{- $rag := get .Values "rag" | default dict -}}
{{- $podContext := get $rag "podSecurityContext" | default dict -}}
{{- $containerContext := get $rag "securityContext" | default dict -}}
{{- range $scope, $context := dict "rag.podSecurityContext" $podContext "rag.securityContext" $containerContext -}}
{{- if hasKey $context "runAsNonRoot" -}}
{{- if not (kindIs "bool" (get $context "runAsNonRoot")) -}}
{{- fail (printf "%s.runAsNonRoot must be a boolean" $scope) -}}
{{- end -}}
{{- if not (get $context "runAsNonRoot") -}}
{{- fail (printf "%s.runAsNonRoot must stay true for the HugeGraph RAG image" $scope) -}}
{{- end -}}
{{- end -}}
{{- range $field := list "runAsUser" "runAsGroup" -}}
{{- if hasKey $context $field -}}
{{- $raw := get $context $field | toString -}}
{{- if and (ne $raw "<nil>") (ne (lower $raw) "auto") (not (regexMatch "^[1-9][0-9]*$" $raw)) -}}
{{- fail (printf "%s.%s must be a positive integer, null or auto" $scope $field) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if hasKey $context "seccompProfile" -}}
{{- $seccomp := get $context "seccompProfile" -}}
{{- if not (kindIs "map" $seccomp) -}}
{{- fail (printf "%s.seccompProfile must be an object" $scope) -}}
{{- end -}}
{{- if ne (get $seccomp "type" | default "" | toString) "RuntimeDefault" -}}
{{- fail (printf "%s.seccompProfile.type must stay RuntimeDefault" $scope) -}}
{{- end -}}
{{- if hasKey $seccomp "localhostProfile" -}}
{{- fail (printf "%s.seccompProfile.localhostProfile must be unset when type is RuntimeDefault" $scope) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if hasKey $podContext "fsGroup" -}}
{{- $rawFsGroup := get $podContext "fsGroup" | toString -}}
{{- if and (ne $rawFsGroup "<nil>") (ne (lower $rawFsGroup) "auto") (not (regexMatch "^[1-9][0-9]*$" $rawFsGroup)) -}}
{{- fail "rag.podSecurityContext.fsGroup must be a positive integer, null or auto" -}}
{{- end -}}
{{- end -}}
{{- range $field := list "privileged" "allowPrivilegeEscalation" -}}
{{- if hasKey $containerContext $field -}}
{{- if not (kindIs "bool" (get $containerContext $field)) -}}
{{- fail (printf "rag.securityContext.%s must be a boolean" $field) -}}
{{- end -}}
{{- if get $containerContext $field -}}
{{- fail (printf "rag.securityContext.%s must stay false" $field) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if hasKey $containerContext "capabilities" -}}
{{- $capabilities := get $containerContext "capabilities" -}}
{{- if not (kindIs "map" $capabilities) -}}
{{- fail "rag.securityContext.capabilities must be an object" -}}
{{- end -}}
{{- if hasKey $capabilities "add" -}}
{{- $added := get $capabilities "add" -}}
{{- if not (kindIs "slice" $added) -}}
{{- fail "rag.securityContext.capabilities.add must be an array" -}}
{{- end -}}
{{- if gt (len $added) 0 -}}
{{- fail "rag.securityContext.capabilities.add must stay empty" -}}
{{- end -}}
{{- end -}}
{{- if hasKey $capabilities "drop" -}}
{{- $dropped := get $capabilities "drop" -}}
{{- if not (kindIs "slice" $dropped) -}}
{{- fail "rag.securityContext.capabilities.drop must be an array" -}}
{{- end -}}
{{- if not (has "ALL" $dropped) -}}
{{- fail "rag.securityContext.capabilities.drop must include ALL" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $serviceAccount := get $rag "serviceAccount" | default dict -}}
{{- if hasKey $serviceAccount "automountServiceAccountToken" -}}
{{- if not (kindIs "bool" (get $serviceAccount "automountServiceAccountToken")) -}}
{{- fail "rag.serviceAccount.automountServiceAccountToken must be a boolean" -}}
{{- end -}}
{{- if get $serviceAccount "automountServiceAccountToken" -}}
{{- fail "rag.serviceAccount.automountServiceAccountToken must stay false" -}}
{{- end -}}
{{- end -}}
{{- end }}
