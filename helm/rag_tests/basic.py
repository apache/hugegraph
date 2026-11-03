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

"""Basic rendered contracts for the optional HugeGraph RAG component."""

from .support import RAG_MOUNT, RAG_PERSISTENT_ENV, component_resources, env_by_name, pod


class RagBasicMixin:
    def test_disabled_adds_no_resources(self):
        documents = self.documents()
        self.assertEqual([], component_resources(documents))
        self.assertFalse(any("-rag" in document["metadata"]["name"] for document in documents))

    def test_bare_enable_has_safe_persistent_defaults(self):
        documents = self.documents({}, use_profile=False)
        deployment = self.deployment(documents)
        container = self.container(documents)
        service, = component_resources(documents, kind="Service")
        pvc, = component_resources(documents, kind="PersistentVolumeClaim")

        self.assertEqual("hugegraph/rag:latest", container["image"])
        self.assertEqual(1, deployment["spec"]["replicas"])
        self.assertEqual("Recreate", deployment["spec"]["strategy"]["type"])
        self.assertEqual(8001, container["ports"][0]["containerPort"])
        self.assertEqual(["python", "-m", "hugegraph_llm.demo.rag_demo.app"], container["command"])
        self.assertEqual(["--host", "0.0.0.0", "--port", "8001"], container["args"])
        self.assertEqual("ClusterIP", service["spec"]["type"])
        self.assertEqual("10Gi", pvc["spec"]["resources"]["requests"]["storage"])

        pod_security = pod(deployment)["spec"]["securityContext"]
        self.assertTrue(pod_security["runAsNonRoot"])
        self.assertEqual(1000, pod_security["runAsUser"])
        self.assertEqual(1000, pod_security["runAsGroup"])
        self.assertEqual(1000, pod_security["fsGroup"])
        self.assertEqual("RuntimeDefault", pod_security["seccompProfile"]["type"])
        self.assertFalse(container["securityContext"]["privileged"])
        self.assertFalse(container["securityContext"]["allowPrivilegeEscalation"])
        self.assertEqual(["ALL"], container["securityContext"]["capabilities"]["drop"])
        self.assertFalse(pod(deployment)["spec"]["automountServiceAccountToken"])

        environment = env_by_name(container)
        self.assertEqual(RAG_PERSISTENT_ENV, environment["HUGEGRAPH_LLM_ENV_PATH"]["value"])
        self.assertEqual(
            f"{RAG_MOUNT}/demo/config_prompt.yaml",
            environment["HUGEGRAPH_LLM_PROMPT_CONFIG_PATH"]["value"],
        )

    def test_default_profile_uses_internal_server_contract(self):
        documents = self.documents({})
        deployment = self.deployment(documents)
        container = self.container(documents)
        service, = component_resources(documents, kind="Service")
        environment = env_by_name(container)

        self.assertEqual("http://fixture-hugegraph-server.graphs.svc:8080", environment["GRAPH_URL"]["value"])
        self.assertEqual("hugegraph", environment["GRAPH_NAME"]["value"])
        self.assertEqual("DEFAULT", environment["GRAPH_SPACE"]["value"])
        self.assertEqual("admin", environment["GRAPH_USER"]["value"])
        self.assertEqual("fixture-admin", environment["GRAPH_PWD"]["valueFrom"]["secretKeyRef"]["name"])
        self.assertEqual("password", environment["GRAPH_PWD"]["valueFrom"]["secretKeyRef"]["key"])
        self.assertIn("checksum/hugegraph-auth", pod(deployment)["metadata"]["annotations"])
        self.assertEqual("ClusterIP", service["spec"]["type"])

    def test_custom_port_reaches_process_pod_service_and_probes(self):
        documents = self.documents({"port": 9001})
        container = self.container(documents)
        service, = component_resources(documents, kind="Service")
        self.assertEqual("9001", container["args"][-1])
        self.assertEqual(9001, container["ports"][0]["containerPort"])
        self.assertEqual(9001, service["spec"]["ports"][0]["port"])
        for name in ("startupProbe", "readinessProbe", "livenessProbe"):
            self.assertEqual("http", container[name]["httpGet"]["port"])
