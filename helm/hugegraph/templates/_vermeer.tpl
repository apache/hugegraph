{{/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/}}

{{- define "hugegraph.vermeer.container" -}}
{{- $root := .root -}}
{{- $role := .role -}}
{{- $v := $root.Values.vermeer -}}
{{- $roleValues := get $v $role -}}
- name: {{ $role }}
  image: {{ include "hugegraph.image" (dict "image" $v.image "appVersion" $root.Chart.AppVersion) | quote }}
  imagePullPolicy: {{ $v.image.pullPolicy }}
  workingDir: /go/bin/vermeer_data
  command: ["/go/bin/app"]
  args:
    - -env=/var/run/vermeer/config/{{ $role }}.ini
    - -log_prefix=/go/bin/vermeer_data/vermeer.log
    {{- if eq $role "worker" }}
    - {{ printf "-grpc_peer=%s:%v" (ternary "127.0.0.1" "$(POD_IP)" (eq $v.mode "standalone")) $v.worker.grpcPort | quote }}
    {{- end }}
  {{- with $v.securityContext }}
  securityContext:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  env:
    - name: POD_IP
      valueFrom:
        fieldRef:
          fieldPath: status.podIP
    - name: VERMEER_PD_PEERS
      value: {{ splitList "," (include "hugegraph.pd.grpcPeersList" $root) | toJson | quote }}
    - name: VERMEER_PD_PASSWORD_FILE
      value: /var/run/vermeer/pd/password
    {{- if eq $role "master" }}
    - name: VERMEER_AUTH_TOKEN_FACTOR_FILE
      value: /var/run/vermeer/auth/token-factor
    - name: VERMEER_ADMIN_PASSWORD_FILE
      value: /var/run/vermeer/admin/password
    {{- else if $v.hugegraphCredentials.existingSecret }}
    - name: VERMEER_HUGEGRAPH_CREDENTIALS_FILE
      value: /var/run/vermeer/hugegraph/credentials.json
    {{- else if $root.Values.server.auth.enabled }}
    - name: VERMEER_HUGEGRAPH_USERNAME
      value: admin
    - name: VERMEER_HUGEGRAPH_PASSWORD_FILE
      value: /var/run/vermeer/hugegraph/password
    {{- end }}
    {{- with $roleValues.extraEnv }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  ports:
    - name: {{ $role }}-http
      containerPort: {{ $roleValues.httpPort }}
    - name: {{ $role }}-grpc
      containerPort: {{ $roleValues.grpcPort }}
  volumeMounts:
    - name: {{ $role }}-data
      mountPath: /go/bin/vermeer_data
    - name: vermeer-config
      mountPath: /var/run/vermeer/config
      readOnly: true
    - name: pd-auth
      mountPath: /var/run/vermeer/pd
      readOnly: true
    {{- if eq $role "master" }}
    - name: vermeer-auth
      mountPath: /var/run/vermeer/auth
      readOnly: true
    - name: vermeer-admin
      mountPath: /var/run/vermeer/admin
      readOnly: true
    {{- else if or $v.hugegraphCredentials.existingSecret $root.Values.server.auth.enabled }}
    - name: hugegraph-auth
      mountPath: /var/run/vermeer/hugegraph
      readOnly: true
    {{- end }}
  {{- range $probe := list "startup" "readiness" "liveness" }}
  {{ $probe }}Probe:
    {{- if eq $role "master" }}
    httpGet:
      path: /healthcheck
      port: master-http
    {{- else }}
    tcpSocket:
      port: worker-http
    {{- end }}
    {{- toYaml (get $v.probes $probe) | nindent 4 }}
  {{- end }}
  {{- with $roleValues.resources }}
  resources:
    {{- toYaml . | nindent 4 }}
  {{- end }}
{{- end }}

{{- define "hugegraph.vermeer.volumes" -}}
{{- $root := .root -}}
{{- $worker := .worker -}}
{{- $master := .master -}}
- name: vermeer-config
  configMap:
    name: {{ include "hugegraph.vermeer.configName" $root }}
- name: pd-auth
  secret:
    secretName: {{ include "hugegraph.pd.authSecretName" $root }}
    items:
      - key: {{ include "hugegraph.pd.authSecretKey" $root | quote }}
        path: password
{{- if $master }}
- name: vermeer-admin
  secret:
    secretName: {{ include "hugegraph.vermeer.adminSecretName" $root }}
    items:
      - key: {{ include "hugegraph.vermeer.adminSecretKey" $root | quote }}
        path: password
- name: vermeer-auth
  secret:
    secretName: {{ include "hugegraph.vermeer.authSecretName" $root }}
    items:
      - key: {{ include "hugegraph.vermeer.authSecretKey" $root | quote }}
        path: token-factor
{{- end }}
{{- if and $worker (or $root.Values.vermeer.hugegraphCredentials.existingSecret $root.Values.server.auth.enabled) }}
- name: hugegraph-auth
  secret:
    {{- if $root.Values.vermeer.hugegraphCredentials.existingSecret }}
    secretName: {{ $root.Values.vermeer.hugegraphCredentials.existingSecret | quote }}
    items:
      - key: {{ $root.Values.vermeer.hugegraphCredentials.key | quote }}
        path: credentials.json
    {{- else }}
    secretName: {{ include "hugegraph.server.authSecretName" $root }}
    items:
      - key: {{ include "hugegraph.server.authSecretKey" $root | quote }}
        path: password
    {{- end }}
{{- end }}
{{- end }}

{{- define "hugegraph.vermeer.claim" -}}
- metadata:
    name: {{ .role }}-data
  spec:
    accessModes: ["ReadWriteOnce"]
    {{- if eq .storage.storageClassName "-" }}
    storageClassName: ""
    {{- else if .storage.storageClassName }}
    storageClassName: {{ .storage.storageClassName | quote }}
    {{- end }}
    resources:
      requests:
        storage: {{ .storage.size }}
{{- end }}
