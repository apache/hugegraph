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

{{/*
Retain only initialization counts, not a second membership view. Explicit pins may reproduce history offline but cannot change
an online record. A legacy release is imported from its actual PD environment,
never from StatefulSet replicas that a controlled operation may already change.
*/}}
{{- define "hugegraph.bootstrap.name" -}}
{{- $name := include "hugegraph.fullname" . -}}
{{- if gt (len $name) 53 -}}
{{- printf "%s-%s-bootstrap" ($name | trunc 44 | trimSuffix "-") ($name | sha256sum | trunc 8) -}}
{{- else -}}
{{- printf "%s-bootstrap" $name -}}
{{- end -}}
{{- end }}

{{- define "hugegraph.bootstrap.resolve" -}}
{{- $config := get .Values "bootstrap" | default dict -}}
{{- $enabled := get $config "enabled" | default false -}}
{{- if not $enabled -}}
{{- $resolved := dict "pdReplicas" (int .Values.pd.replicas) "storeReplicas" (int .Values.store.replicas) "initialStoreCount" (int .Values.store.replicas) -}}
{{- $_ := set .Values "__hugegraphBootstrap" $resolved -}}
{{- if .Release.IsUpgrade -}}
{{- $live := lookup "apps/v1" "StatefulSet" .Release.Namespace (include "hugegraph.pd.name" .) -}}
{{- if and $live (eq (dig "metadata" "annotations" "hugegraph.apache.org/bootstrap-retained" "" $live) "true") -}}
{{- fail "bootstrap.enabled cannot be disabled while the live PD StatefulSet records retained bootstrap topology" -}}
{{- end -}}
{{- end -}}
{{- else -}}
{{- $record := lookup "v1" "ConfigMap" .Release.Namespace (include "hugegraph.bootstrap.name" .) -}}
{{- $history := dict -}}
{{- $legacyEnv := dict -}}
{{- if $record -}}
{{- if not .Release.IsUpgrade -}}
{{- fail "bootstrap history already exists on an install; use a reviewed recovery procedure rather than treating kept data as a fresh cluster" -}}
{{- end -}}
{{- $owner := dig "metadata" "labels" "app.kubernetes.io/instance" "" $record -}}
{{- if not $owner -}}
{{- fail "bootstrap ConfigMap is missing the app.kubernetes.io/instance owner label; inspect the retained initialization topology before continuing" -}}
{{- end -}}
{{- if ne $owner .Release.Name -}}
{{- fail "bootstrap ConfigMap belongs to another release; inspect the retained initialization topology" -}}
{{- end -}}
{{- $data := get $record "data" | default dict -}}
{{- if ne (get $data "schemaVersion" | default "") "1" -}}
{{- fail "bootstrap ConfigMap has an unsupported schemaVersion; inspect it before continuing" -}}
{{- end -}}
{{- range $key := list "pdReplicas" "storeReplicas" "initialStoreCount" -}}
{{- $_ := set $history $key (get $data $key | default "") -}}
{{- end -}}
{{- else -}}
{{- $live := lookup "apps/v1" "StatefulSet" .Release.Namespace (include "hugegraph.pd.name" .) -}}
{{- if $live -}}
{{- if not .Release.IsUpgrade -}}
{{- fail "bootstrap import requires an upgrade of the existing release; inspect live PD and retained data before recovery" -}}
{{- end -}}
{{- $owner := dig "metadata" "labels" "app.kubernetes.io/instance" "" $live -}}
{{- if not $owner -}}
{{- fail "bootstrap import requires the existing PD StatefulSet to carry the app.kubernetes.io/instance owner label" -}}
{{- end -}}
{{- if ne $owner .Release.Name -}}
{{- fail "bootstrap import found a PD StatefulSet owned by another release" -}}
{{- end -}}
{{- $env := dict -}}
{{- range $container := dig "spec" "template" "spec" "containers" list $live -}}
{{- if eq (get $container "name" | default "") "pd" -}}
{{- range $item := get $container "env" | default list -}}
{{- $_ := set $env (get $item "name") (get $item "value" | default "") -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $pdPeers := get $env "HG_PD_RAFT_PEERS_LIST" | default "" -}}
{{- $stores := get $env "HG_PD_INITIAL_STORE_LIST" | default "" -}}
{{- $count := get $env "HG_PD_INITIAL_STORE_COUNT" | default "" -}}
{{- if or (not $pdPeers) (not $stores) (not $count) -}}
{{- fail "bootstrap import requires the existing PD peer list, initial Store list and initial Store count; inspect the live environment instead of guessing from replicas" -}}
{{- end -}}
{{- $_ := set $history "pdReplicas" (len (splitList "," $pdPeers) | toString) -}}
{{- $_ := set $history "storeReplicas" (len (splitList "," $stores) | toString) -}}
{{- $_ := set $history "initialStoreCount" $count -}}
{{- $legacyEnv = $env -}}
{{- else if .Release.IsUpgrade -}}
{{- range $key := list "pdReplicas" "storeReplicas" "initialStoreCount" -}}
{{- if eq (include "hugegraph.optionalScalar" (get $config $key)) "" -}}
{{- fail "bootstrap.pdReplicas, bootstrap.storeReplicas and bootstrap.initialStoreCount must all be pinned for an upgrade without a live bootstrap record" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if not (empty $history) -}}
{{- range $key := list "pdReplicas" "storeReplicas" "initialStoreCount" -}}
{{- if not (regexMatch "^[1-9][0-9]?$" (get $history $key | toString)) -}}
{{- fail (printf "bootstrap record is missing a valid %s between 1 and 99; inspect it before continuing" $key) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $resolved := dict -}}
{{- $defaults := dict "pdReplicas" .Values.pd.replicas "storeReplicas" .Values.store.replicas "initialStoreCount" .Values.store.replicas -}}
{{- range $key := list "pdReplicas" "storeReplicas" "initialStoreCount" -}}
{{- $pin := include "hugegraph.optionalScalar" (get $config $key) -}}
{{- $old := get $history $key | default "" | toString -}}
{{- if and (ne $old "") (ne $pin "") (ne $pin $old) -}}
{{- fail (printf "bootstrap.%s cannot change from %s to %s on an initialized release" $key $old $pin) -}}
{{- end -}}
{{- $value := ternary $pin $old (ne $pin "") -}}
{{- if eq $value "" -}}{{- $value = get $defaults $key | toString -}}{{- end -}}
{{- if not (regexMatch "^[1-9][0-9]?$" $value) -}}
{{- fail (printf "bootstrap.%s must be an integer between 1 and 99" $key) -}}
{{- end -}}
{{- $_ := set $resolved $key (int $value) -}}
{{- end -}}
{{- if gt (int $resolved.initialStoreCount) (int $resolved.storeReplicas) -}}
{{- fail "bootstrap.initialStoreCount cannot exceed bootstrap.storeReplicas: the initial allowlist could not satisfy startup" -}}
{{- end -}}
{{- if and (not .Release.IsUpgrade) (empty $history) -}}
{{- if ne (int $resolved.pdReplicas) (int .Values.pd.replicas) -}}
{{- fail "bootstrap.pdReplicas must equal pd.replicas on a fresh install; use --is-upgrade with pinned history for offline upgrades" -}}
{{- end -}}
{{- if ne (int $resolved.storeReplicas) (int .Values.store.replicas) -}}
{{- fail "bootstrap.storeReplicas must equal store.replicas on a fresh install; use --is-upgrade with pinned history for offline upgrades" -}}
{{- end -}}
{{- end -}}
{{- $_ := set .Values "__hugegraphBootstrap" $resolved -}}
{{- if not (empty $legacyEnv) -}}
{{- if or (ne (get $legacyEnv "HG_PD_RAFT_PEERS_LIST") (include "hugegraph.pd.raftPeersList" .)) (ne (get $legacyEnv "HG_PD_INITIAL_STORE_LIST") (include "hugegraph.store.initialStoreList" .)) -}}
{{- fail "bootstrap only supports the chart's contiguous ordinal endpoint lists; inspect customized live endpoints before migration" -}}
{{- end -}}
{{- $seed := include "hugegraph.pd.grpcPeersList" . -}}
{{- range $component := list "store" "server" -}}
{{- $kind := ternary "StatefulSet" "Deployment" (eq $component "store") -}}
{{- $name := ternary (include "hugegraph.store.name" $) (include "hugegraph.server.name" $) (eq $component "store") -}}
{{- $consumer := lookup "apps/v1" $kind $.Release.Namespace $name -}}
{{- $key := ternary "HG_STORE_PD_ADDRESS" "HG_SERVER_PD_PEERS" (eq $component "store") -}}
{{- if $consumer -}}
{{- $found := false -}}
{{- range $container := dig "spec" "template" "spec" "containers" list $consumer -}}
{{- if eq (get $container "name" | default "") $component -}}
{{- range $item := get $container "env" | default list -}}
{{- if eq (get $item "name" | default "") $key -}}
{{- $found = true -}}
{{- if ne (get $item "value" | default "") $seed -}}
{{- fail (printf "bootstrap import found %s seeds that differ from the PD initial peers; reconcile the existing templates before migration" $component) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if not $found -}}
{{- fail (printf "bootstrap import requires the existing %s template to expose %s as a literal value" $component $key) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end }}
