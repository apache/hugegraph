#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Validation regression coverage for the optional RAG component."""

from .support import RAG_MOUNT, pod


class RagValidationMixin:
    def test_invalid_contracts_fail_before_render(self):
        invalid = (
            ({"enabled": "false"}, "enabled"),
            ({"hugegraph": {"autoConfigure": "false"}}, "autoConfigure"),
            ({"env": [{"name": "-BAD", "value": "x"}]}, "rag.env"),
            ({"service": {"type": "ExternalName"}}, "service.type"),
            ({"service": {"type": "ClusterIP", "nodePort": 31001}}, "nodePort"),
            ({"service": {"type": "NodePort", "nodePort": "3100x"}}, "nodePort"),
            ({"service": {"type": "NodePort", "nodePort": "9" * 100}}, "nodePort"),
            ({"port": 0}, "rag.port"),
            ({"port": 8001.5}, "rag.port"),
            ({"replicas": 0}, "rag.replicas"),
            ({"replicas": 1.5}, "rag.replicas"),
            ({"replicas": 2}, "rag.replicas"),
            ({"podSecurityContext": {"runAsUser": 0}}, "runAsUser"),
            ({"podSecurityContext": {"fsGroup": 0}}, "fsGroup"),
            ({"podSecurityContext": {"seccompProfile": {"type": "Unconfined"}}}, "seccompProfile"),
            ({"podSecurityContext": {"seccompProfile": {
                "type": "RuntimeDefault", "localhostProfile": "profiles/rag.json",
            }}}, "localhostProfile"),
            ({"securityContext": {"privileged": True}}, "privileged"),
            ({"securityContext": {"allowPrivilegeEscalation": True}}, "allowPrivilegeEscalation"),
            ({"securityContext": {"seccompProfile": {"type": "Unconfined"}}}, "seccompProfile"),
            ({"securityContext": {"seccompProfile": {
                "type": "RuntimeDefault", "localhostProfile": "profiles/rag.json",
            }}}, "localhostProfile"),
            ({"securityContext": {"capabilities": {"add": ["SYS_ADMIN"]}}}, "capabilities.add"),
            ({"securityContext": {"capabilities": {"drop": ["NET_RAW"]}}}, "capabilities.drop"),
            ({"serviceAccount": {"automountServiceAccountToken": True}}, "automountServiceAccountToken"),
            ({"hugegraph": {"passwordSecret": {"key": "password"}}}, "passwordSecret.name"),
            ({"persistence": {"enabled": 1}}, "persistence.enabled"),
            ({"persistence": {"mountPath": "relative/path"}}, "mountPath"),
            ({"extraVolumeMounts": [{"name": "other", "mountPath": RAG_MOUNT}]},
             "extraVolumeMounts"),
            ({"extraVolumes": [{"name": "data", "emptyDir": {}}]},
             "reserved volume name data"),
            ({"env": [{"name": "BAD=NAME", "value": "x"}]}, "Kubernetes 1.23"),
            ({"env": [{"name": "BAD NAME", "value": "x"}]}, "Kubernetes 1.23"),
            ({"env": [{"name": "BAD\tNAME", "value": "x"}]}, "Kubernetes 1.23"),
            ({"env": [{"name": "GRAPH_URL", "value": "a"}, {"name": "GRAPH_URL", "value": "b"}]}, "duplicate"),
            ({"persistance": {"enabled": False}}, "persistance"),
        )
        for values, expected in invalid:
            with self.subTest(values=values):
                result = self.render(values)
                self.assertNotEqual(0, result.returncode)
                self.assertIn(expected.replace("/", "."), result.stderr.replace("/", "."))

    def test_reserved_data_volume_and_mount_path_are_available_when_persistence_is_off(self):
        documents = self.documents({
            "persistence": {"enabled": False},
            "extraVolumes": [{"name": "data", "emptyDir": {}}],
            "extraVolumeMounts": [{"name": "data", "mountPath": RAG_MOUNT}],
        })
        workload = self.deployment(documents)
        self.assertIn({"name": "data", "emptyDir": {}}, pod(workload)["spec"]["volumes"])
        self.assertIn({"name": "data", "mountPath": RAG_MOUNT},
                      pod(workload)["spec"]["containers"][0]["volumeMounts"])

    def test_platform_assigned_non_root_ids_can_be_requested_with_auto(self):
        documents = self.documents({
            "podSecurityContext": {
                "runAsUser": "auto",
                "runAsGroup": "auto",
                "fsGroup": "auto",
            },
            "securityContext": {
                "runAsUser": "auto",
                "runAsGroup": "auto",
            },
        })
        workload = self.deployment(documents)
        pod_context = pod(workload)["spec"]["securityContext"]
        self.assertTrue(pod_context["runAsNonRoot"])
        self.assertNotIn("runAsUser", pod_context)
        self.assertNotIn("runAsGroup", pod_context)
        self.assertNotIn("fsGroup", pod_context)
        self.assertNotIn("fsGroupChangePolicy", pod_context)
        for container in [
            *pod(workload)["spec"].get("initContainers", []),
            *pod(workload)["spec"]["containers"],
        ]:
            context = container["securityContext"]
            self.assertTrue(context["runAsNonRoot"])
            self.assertNotIn("runAsUser", context)
            self.assertNotIn("runAsGroup", context)
