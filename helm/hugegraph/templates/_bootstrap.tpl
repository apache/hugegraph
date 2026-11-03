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

{{/* Initialization history is immutable. Existing data is adopted only at the same topology. */}}
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
{{- $record := lookup "v1" "ConfigMap" .Release.Namespace (include "hugegraph.bootstrap.name" .) -}}
{{- $live := lookup "apps/v1" "StatefulSet" .Release.Namespace (include "hugegraph.pd.name" .) -}}
{{- $history := dict -}}
{{- if not $live -}}
{{/* A retained volume cannot be treated as a fresh cluster after its StatefulSet disappears. */}}
{{- $pdPrefix := printf "pd-data-%s-" (include "hugegraph.pd.name" .) -}}
{{- $storePrefix := printf "store-data-%s-" (include "hugegraph.store.name" .) -}}
{{- $claims := lookup "v1" "PersistentVolumeClaim" .Release.Namespace "" -}}
{{- range $claim := get $claims "items" | default list -}}
{{- $name := dig "metadata" "name" "" $claim -}}
{{- if or (hasPrefix $pdPrefix $name) (hasPrefix $storePrefix $name) -}}
{{- fail "retained data PVCs exist without a live PD StatefulSet; use a reviewed recovery procedure" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if $record -}}
{{- if not .Release.IsUpgrade -}}
{{- fail "bootstrap history already exists on an install; use a reviewed recovery procedure" -}}
{{- end -}}
{{- $owner := dig "metadata" "labels" "app.kubernetes.io/instance" "" $record -}}
{{- if not $owner -}}
{{- fail "bootstrap ConfigMap is missing the app.kubernetes.io/instance owner label" -}}
{{- end -}}
{{- if ne $owner .Release.Name -}}
{{- fail "bootstrap ConfigMap belongs to another release" -}}
{{- end -}}
{{- $data := get $record "data" | default dict -}}
{{- if ne (get $data "schemaVersion" | default "") "1" -}}
{{- fail "bootstrap ConfigMap has an unsupported schemaVersion" -}}
{{- end -}}
{{- $history = $data -}}
{{- else if $live -}}
{{- if not .Release.IsUpgrade -}}
{{- fail "bootstrap adoption requires an upgrade of the existing release" -}}
{{- end -}}
{{- if eq (dig "metadata" "annotations" "hugegraph.apache.org/bootstrap-retained" "" $live) "true" -}}
{{- fail "bootstrap record is missing from an initialized release; restore the original record before upgrading" -}}
{{- end -}}
{{- else -}}
{{- if .Release.IsUpgrade -}}
{{- range $key := list "pdReplicas" "storeReplicas" "initialStoreCount" -}}
{{- if eq (include "hugegraph.optionalScalar" (get $config $key)) "" -}}
{{- fail "bootstrap.pdReplicas, bootstrap.storeReplicas and bootstrap.initialStoreCount must all be pinned for an upgrade without a live bootstrap record" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $resolved := dict -}}
{{- $defaults := dict "pdReplicas" .Values.pd.replicas "storeReplicas" .Values.store.replicas "initialStoreCount" .Values.store.replicas -}}
{{- range $key := list "pdReplicas" "storeReplicas" "initialStoreCount" -}}
{{- $pin := include "hugegraph.optionalScalar" (get $config $key) -}}
{{- $old := "" -}}
{{- if $record -}}
{{- $old = get $history $key | default "" | toString -}}
{{- if not (regexMatch "^[1-9][0-9]?$" $old) -}}
{{- fail (printf "bootstrap record is missing a valid %s between 1 and 99" $key) -}}
{{- end -}}
{{- end -}}
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
{{- if and $record (not $live) (or (ne (int $resolved.pdReplicas) (int .Values.pd.replicas)) (ne (int $resolved.storeReplicas) (int .Values.store.replicas))) -}}
{{- fail "bootstrap retry without a live PD must preserve the recorded PD and Store counts; use a reviewed recovery procedure" -}}
{{- end -}}
{{- if gt (int $resolved.initialStoreCount) (int $resolved.storeReplicas) -}}
{{- fail "bootstrap.initialStoreCount cannot exceed bootstrap.storeReplicas" -}}
{{- end -}}
{{- if and (not $record) (or $live (not .Release.IsUpgrade)) -}}
{{- if ne (int $resolved.pdReplicas) (int .Values.pd.replicas) -}}
{{- fail "bootstrap.pdReplicas must equal pd.replicas without a retained record; changed-topology adoption is unsupported" -}}
{{- end -}}
{{- if ne (int $resolved.storeReplicas) (int .Values.store.replicas) -}}
{{- fail "bootstrap.storeReplicas must equal store.replicas without a retained record; changed-topology adoption is unsupported" -}}
{{- end -}}
{{- end -}}
{{- $_ := set .Values "__hugegraphBootstrap" $resolved -}}
{{- if and $live (not $record) -}}
{{- $rf := include "hugegraph.optionalScalar" (get (.Values.pd.partition | default dict) "defaultShardCount") -}}
{{- if eq $rf "" -}}
{{- fail "same-topology adoption requires explicit pd.partition.defaultShardCount matching the read-only database preflight" -}}
{{- end -}}
{{- $pdEnv := dict "HG_PD_RAFT_PEERS_LIST" (include "hugegraph.pd.raftPeersList" .) "HG_PD_INITIAL_STORE_LIST" (include "hugegraph.store.initialStoreList" .) "HG_PD_INITIAL_STORE_COUNT" ($resolved.initialStoreCount | toString) "HG_PD_DATA_PATH" .Values.pd.dataPath -}}
{{- include "hugegraph.bootstrap.checkExisting" (dict "root" . "component" "pd" "live" $live "expectedEnv" $pdEnv) -}}
{{- $store := lookup "apps/v1" "StatefulSet" .Release.Namespace (include "hugegraph.store.name" .) -}}
{{- if not $store -}}
{{- fail "same-topology adoption requires the existing Store StatefulSet" -}}
{{- end -}}
{{- $storeEnv := dict "HG_STORE_PD_ADDRESS" (include "hugegraph.pd.grpcPeersList" .) "HG_STORE_DATA_PATH" .Values.store.dataPath -}}
{{- include "hugegraph.bootstrap.checkExisting" (dict "root" . "component" "store" "live" $store "expectedEnv" $storeEnv) -}}
{{- end -}}
{{- end }}

{{/* Only Kubernetes-visible invariants are checked here; actual voters/RF/PV binding need preflight. */}}
{{- define "hugegraph.bootstrap.checkExisting" -}}
{{- $root := .root -}}
{{- $component := .component -}}
{{- $live := .live -}}
{{- $desired := get $root.Values $component -}}
{{- if ne (dig "metadata" "labels" "app.kubernetes.io/instance" "" $live) $root.Release.Name -}}
{{- fail (printf "same-topology adoption requires the existing %s StatefulSet owner to match the release" $component) -}}
{{- end -}}
{{- $name := include (printf "hugegraph.%s.name" $component) $root -}}
{{- if or (ne (int (dig "spec" "replicas" 0 $live)) (int $desired.replicas)) (ne (dig "spec" "serviceName" "" $live) $name) -}}
{{- fail (printf "same-topology adoption requires unchanged %s replicas and StatefulSet DNS" $component) -}}
{{- end -}}
{{- $claims := dig "spec" "volumeClaimTemplates" list $live -}}
{{- if $desired.storage.enabled -}}
{{- if or (ne (len $claims) 1) (ne (dig "metadata" "name" "" (first $claims)) (printf "%s-data" $component)) -}}
{{- fail (printf "same-topology adoption requires unchanged %s PVC template identity" $component) -}}
{{- end -}}
{{- else if $claims -}}
{{- fail (printf "same-topology adoption cannot discard existing %s data volumes" $component) -}}
{{- end -}}
{{- $env := dict -}}
{{- range $container := dig "spec" "template" "spec" "containers" list $live -}}
{{- if eq (get $container "name" | default "") $component -}}
{{- range $item := get $container "env" | default list -}}
{{- $_ := set $env (get $item "name") (get $item "value" | default "") -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- range $key, $expected := .expectedEnv -}}
{{- if ne (get $env $key | default "") $expected -}}
{{- fail (printf "same-topology adoption requires unchanged literal %s; run preflight and retain the original configuration" $key) -}}
{{- end -}}
{{- end -}}
{{- if eq $component "pd" -}}
{{- $rf := include "hugegraph.optionalScalar" (get ($root.Values.pd.partition | default dict) "defaultShardCount") -}}
{{- if not (regexMatch (printf "(^|[[:space:]])-Dpartition.default-shard-count=%s([[:space:]]|$)" $rf) (get $env "JAVA_OPTS" | default "")) -}}
{{- fail "same-topology adoption requires unchanged explicit RF in the existing PD JAVA_OPTS; verify persisted RF with preflight" -}}
{{- end -}}
{{- end -}}
{{- end }}
