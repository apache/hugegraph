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

{{- define "hugegraph.rag.validate.metadata" -}}
{{- $rag := get .Values "rag" | default dict -}}
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
{{- if not (regexMatch `^[._a-zA-Z][-._a-zA-Z0-9]*$` $name) -}}
{{- fail (printf "rag.env[%d].name must match [._a-zA-Z][-._a-zA-Z0-9]* for Kubernetes 1.23 compatibility" $index) -}}
{{- end -}}
{{- if hasKey $seenEnv $name -}}
{{- fail (printf "rag.env contains duplicate name %s" $name) -}}
{{- end -}}
{{- $_ := set $seenEnv $name true -}}
{{- end -}}
{{- end }}
