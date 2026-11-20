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

from .support import RAG_MOUNT, component_resources, pod


class RagValidationMixin:
    def test_boolean_resource_switches_control_their_own_resources(self):
        for service_account, ingress in ((False, False), (False, True), (True, False), (True, True)):
            with self.subTest(serviceAccount=service_account, ingress=ingress):
                documents = self.documents({
                    "serviceAccount": {"create": service_account},
                    "ingress": {"enabled": ingress},
                })
                expected = {"ServiceAccount": int(service_account), "Ingress": int(ingress)}
                for kind, count in expected.items():
                    self.assertEqual(count, len(component_resources(documents, kind=kind)))

    def test_invalid_contracts_fail_before_render(self):
        invalid = (
            ({"enabled": "false"}, "enabled"),
            ({"serviceAccount": {"create": "false"}}, "serviceAccount.create"),
            ({"ingress": {"enabled": "false"}}, "ingress.enabled"),
            ({"hugegraph": {"autoConfigure": "false"}}, "autoConfigure"),
            ({"env": [{"name": "-BAD", "value": "x"}]}, "rag.env"),
            ({"service": {"type": "ExternalName"}}, "service.type"),
            ({"service": {"type": "ClusterIP", "nodePort": 31001}}, "nodePort"),
            ({"service": {"type": "NodePort", "nodePort": "3100x"}}, "nodePort"),
            ({"service": {"type": "NodePort", "nodePort": "9" * 100}}, "nodePort"),
            ({"probes": {"liveness": {"periodSeconds": -5}}}, "liveness.periodSeconds"),
            ({"probes": {"startup": {"failureThreshold": 0}}}, "startup.failureThreshold"),
            ({"probes": {"readiness": {"timeoutSeconds": 0}}}, "readiness.timeoutSeconds"),
            ({"probes": {"startup": {"initialDelaySeconds": -1}}}, "startup.initialDelaySeconds"),
            ({"probes": {"readiness": {"periodSeconds": "5"}}}, "readiness.periodSeconds"),
            ({"probes": {"startup": {"successThreshold": 2}}}, "startup.successThreshold"),
            ({"probes": {"liveness": {"successThreshold": 2}}}, "liveness.successThreshold"),
            ({"terminationGracePeriodSeconds": -10}, "terminationGracePeriodSeconds"),
            ({"probes": {"readiness": {"path": "health"}}}, "readiness.path"),
            ({"probes": {"startup": {"path": ""}}}, "startup.path"),
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
            ({"persistence": {"size": "abc"}}, "persistence.size"),
            ({"persistence": {"size": "100K"}}, "persistence.size"),
            ({"persistence": {"existingClaim": 123}}, "existingClaim"),
            ({"persistence": {"existingClaim": True}}, "existingClaim"),
            ({"persistence": {"storageClassName": 123}}, "storageClassName"),
            ({"persistence": {"size": "-1Gi"}}, "persistence.size"),
            ({"persistence": {"size": "0Gi"}}, "persistence.size"),
            ({"persistence": {"size": "1e"}}, "persistence.size"),
            ({"persistence": {"accessModes": ["ReadWriteSome"]}}, "accessModes"),
            ({"persistence": {"accessModes": "ReadWriteOnce"}}, "accessModes"),
            ({"persistence": {
                "accessModes": ["ReadWriteOncePod", "ReadWriteOnce"],
            }}, "ReadWriteOncePod"),
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

    def test_storage_class_name_values_render_as_strings(self):
        for storage_class, expected in (("", None), ("-", ""), ("123", "123"), ("fast", "fast")):
            with self.subTest(storageClassName=storage_class):
                documents = self.documents({"persistence": {"storageClassName": storage_class}})
                claim, = component_resources(documents, kind="PersistentVolumeClaim")
                if expected is None:
                    self.assertNotIn("storageClassName", claim["spec"])
                else:
                    self.assertEqual(expected, claim["spec"]["storageClassName"])

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

    def test_readiness_allows_multiple_successes(self):
        workload = self.deployment(self.documents({
            "probes": {"readiness": {"successThreshold": 2}},
        }))
        container = pod(workload)["spec"]["containers"][0]
        self.assertEqual(2, container["readinessProbe"]["successThreshold"])

    def test_partial_probe_options_keep_defaults(self):
        workload = self.deployment(self.documents({
            "probes": {"readiness": {"path": "/health", "initialDelaySeconds": 0, "periodSeconds": 1}},
            "terminationGracePeriodSeconds": 0,
        }, use_profile=False))
        self.assertEqual(0, pod(workload)["spec"]["terminationGracePeriodSeconds"])
        probe = pod(workload)["spec"]["containers"][0]["readinessProbe"]
        self.assertEqual("/health", probe["httpGet"]["path"])
        self.assertEqual(0, probe["initialDelaySeconds"])
        self.assertEqual(1, probe["periodSeconds"])
        self.assertGreaterEqual(probe["failureThreshold"], 1)

    def test_pvc_quantities_and_access_modes_preserve_supported_values(self):
        for size, access_modes in (
            ("10Gi", ["ReadWriteOnce"]),
            ("1.5Gi", ["ReadWriteMany"]),
            ("500M", ["ReadOnlyMany"]),
            ("100k", ["ReadWriteOnce"]),
            ("100Ki", ["ReadWriteOnce"]),
            ("1e9", ["ReadWriteOncePod"]),
            ("1000", ["ReadWriteOnce"]),
            (".5Gi", ["ReadWriteOnce", "ReadOnlyMany"]),
        ):
            with self.subTest(size=size, access_modes=access_modes):
                documents = self.documents({"persistence": {"size": size, "accessModes": access_modes}})
                claim, = component_resources(documents, kind="PersistentVolumeClaim")
                self.assertEqual(size, claim["spec"]["resources"]["requests"]["storage"])
                self.assertEqual(access_modes, claim["spec"]["accessModes"])

    def test_existing_claim_names_remain_strings(self):
        for name in ("123", "external-rag-data"):
            with self.subTest(name=name):
                documents = self.documents({"persistence": {"existingClaim": name}})
                self.assertEqual([], component_resources(documents, kind="PersistentVolumeClaim"))
                volumes = pod(self.deployment(documents))["spec"]["volumes"]
                data, = [volume for volume in volumes if volume["name"] == "data"]
                self.assertEqual(name, data["persistentVolumeClaim"]["claimName"])
