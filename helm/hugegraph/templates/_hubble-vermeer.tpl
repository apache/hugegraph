{{/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/}}

{{/* Match the reader's UID: the token helper deliberately creates mode 0600. */}}
{{- define "hugegraph.hubble.vermeerUid" -}}
{{- $container := .Values.hubble.securityContext | default dict -}}
{{- $pod := .Values.hubble.podSecurityContext | default dict -}}
{{- if hasKey $container "runAsUser" -}}
{{- $container.runAsUser -}}
{{- else if hasKey $pod "runAsUser" -}}
{{- $pod.runAsUser -}}
{{- else -}}0{{- end -}}
{{- end -}}

{{- define "hugegraph.hubble.vermeerGid" -}}
{{- $container := .Values.hubble.securityContext | default dict -}}
{{- $pod := .Values.hubble.podSecurityContext | default dict -}}
{{- if hasKey $container "runAsGroup" -}}
{{- $container.runAsGroup -}}
{{- else if hasKey $pod "runAsGroup" -}}
{{- $pod.runAsGroup -}}
{{- else -}}0{{- end -}}
{{- end -}}

{{/* Signing material exists only in this completed init container. */}}
{{- define "hugegraph.vermeer.tokenInit" -}}
{{- $root := .root -}}
- name: vermeer-token
  image: {{ include "hugegraph.image" (dict "image" $root.Values.vermeer.image "appVersion" $root.Chart.AppVersion) | quote }}
  imagePullPolicy: {{ $root.Values.vermeer.image.pullPolicy }}
  command: ["/go/bin/vermeer-token"]
  args:
    - -factor-file
    - /var/run/hugegraph/vermeer-factor/factor
    - -output-file
    - /var/run/hugegraph/vermeer/token
    - -quiet
    - -user
    - admin
    - -space
    - "$DEFAULT"
    - -client
    - helm
    - -i
  securityContext:
    allowPrivilegeEscalation: false
    capabilities:
      drop: ["ALL"]
    readOnlyRootFilesystem: true
    runAsUser: {{ .uid }}
    runAsGroup: {{ .gid }}
    runAsNonRoot: {{ ne (int .uid) 0 }}
    seccompProfile:
      type: RuntimeDefault
  {{- with .resources }}
  resources:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  volumeMounts:
    - name: vermeer-factor
      mountPath: /var/run/hugegraph/vermeer-factor
      readOnly: true
    - name: vermeer-token
      mountPath: /var/run/hugegraph/vermeer
{{- end -}}

{{- define "hugegraph.vermeer.tokenVolumes" -}}
- name: vermeer-factor
  secret:
    secretName: {{ include "hugegraph.vermeer.authSecretName" . | quote }}
    defaultMode: 0444
    items:
      - key: {{ include "hugegraph.vermeer.authSecretKey" . | quote }}
        path: factor
- name: vermeer-token
  emptyDir:
    medium: Memory
{{- end -}}
