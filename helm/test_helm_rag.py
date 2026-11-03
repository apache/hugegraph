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

"""Regression coverage for the optional HugeGraph RAG Helm component."""

from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import yaml


CHART = Path(__file__).resolve().parent / "hugegraph"
HELM_TEMPLATE_TIMEOUT_SECONDS = 120


def component_resources(documents, component="rag", kind=None):
    return [
        document
        for document in documents
        if (kind is None or document["kind"] == kind)
        and document["metadata"].get("labels", {}).get("app.kubernetes.io/component") == component
    ]


def pod(workload):
    return workload["spec"]["template"]


def env_by_name(container):
    return {entry["name"]: entry for entry in container.get("env", [])}


class RagSourceContractTest(unittest.TestCase):
    def test_defaults_are_opt_in_and_match_the_image_contract(self):
        values = yaml.safe_load((CHART / "values-rag.yaml").read_text(encoding="utf-8"))
        rag = values["rag"]
        self.assertTrue(rag["enabled"])
        self.assertEqual("hugegraph/rag", rag["image"]["repository"])
        self.assertEqual(8001, rag["port"])
        self.assertEqual(8001, rag["service"].get("port", rag["port"]))
        self.assertEqual(
            "/home/work/hugegraph-llm/src/hugegraph_llm/resources",
            rag["persistence"]["mountPath"],
        )

    def test_single_and_cluster_presets_do_not_enable_rag_implicitly(self):
        single = yaml.safe_load((CHART / "values-single.yaml").read_text(encoding="utf-8"))
        cluster = yaml.safe_load((CHART / "values-cluster.yaml").read_text(encoding="utf-8"))
        self.assertEqual({"enabled": False, "replicas": 1}, single["rag"])
        self.assertEqual({"enabled": False, "replicas": 2}, cluster["rag"])

    def test_templates_use_named_port_and_release_server_service(self):
        deployment = (CHART / "templates" / "rag-deployment.yaml").read_text(encoding="utf-8")
        service = (CHART / "templates" / "rag-service.yaml").read_text(encoding="utf-8")
        helpers = (CHART / "templates" / "_rag.tpl").read_text(encoding="utf-8")
        self.assertIn('containerPort: {{ include "hugegraph.rag.port" . }}', deployment)
        self.assertIn("targetPort: http", service)
        self.assertIn('include "hugegraph.server.clientUrl"', helpers)
        self.assertIn('trimPrefix "http://"', helpers)


class RagChartTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if shutil.which("helm") is None:
            raise unittest.SkipTest("Helm is required for rendered chart regression tests")

    def render(self, rag=None, preset=None, mode=None, **values):
        if rag is not None:
            values["rag"] = {"enabled": True, **rag}
        if mode is not None:
            values["mode"] = mode
        with tempfile.TemporaryDirectory(prefix="rag-values-") as directory:
            configuration = Path(directory) / "values.yaml"
            configuration.write_text(yaml.safe_dump(values), encoding="utf-8")
            command = ["helm", "template", "fixture", str(CHART), "--namespace", "graphs"]
            if preset is not None:
                command.extend(["-f", str(CHART / preset)])
            if rag is not None:
                command.extend(["-f", str(CHART / "values-rag.yaml")])
            command.extend(["-f", str(configuration)])
            return subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=HELM_TEMPLATE_TIMEOUT_SECONDS,
                check=False,
            )

    def documents(self, rag=None, preset=None, mode=None, **values):
        result = self.render(rag, preset=preset, mode=mode, **values)
        self.assertEqual(0, result.returncode, result.stderr)
        return [document for document in yaml.safe_load_all(result.stdout) if document]

    def deployment(self, documents):
        deployment, = component_resources(documents, kind="Deployment")
        return deployment

    def test_disabled_adds_no_resources(self):
        documents = self.documents()
        self.assertEqual([], component_resources(documents))
        self.assertFalse(any("-rag" in document["metadata"]["name"] for document in documents))

    def test_default_enable_uses_one_consistent_port_and_internal_server(self):
        documents = self.documents({})
        deployment = self.deployment(documents)
        container, = pod(deployment)["spec"]["containers"]
        service, = component_resources(documents, kind="Service")
        self.assertEqual(8001, container["ports"][0]["containerPort"])
        self.assertEqual("http", service["spec"]["ports"][0]["targetPort"])
        self.assertEqual(8001, service["spec"]["ports"][0]["port"])
        environment = env_by_name(container)
        self.assertEqual(
            "fixture-hugegraph-server.graphs.svc:8080",
            environment["GRAPH_URL"]["value"],
        )
        self.assertEqual("hugegraph", environment["GRAPH_NAME"]["value"])
        self.assertEqual("DEFAULT", environment["GRAPH_SPACE"]["value"])
        self.assertEqual("admin", environment["GRAPH_USER"]["value"])
        self.assertEqual("fixture-admin", environment["GRAPH_PWD"]["valueFrom"]["secretKeyRef"]["name"])
        self.assertEqual("password", environment["GRAPH_PWD"]["valueFrom"]["secretKeyRef"]["key"])
        self.assertEqual("/tmp/hugegraph-llm.env", environment["HUGEGRAPH_LLM_ENV_PATH"]["value"])
        self.assertEqual("hugegraph/rag:latest", container["image"])

    def test_single_and_cluster_presets_share_the_same_graph_contract(self):
        cases = (("values-single.yaml", 1), ("values-cluster.yaml", 2))
        for preset, expected_replicas in cases:
            with self.subTest(preset=preset):
                documents = self.documents({}, preset=preset)
                deployment = self.deployment(documents)
                container, = pod(deployment)["spec"]["containers"]
                self.assertEqual(expected_replicas, deployment["spec"]["replicas"])
                environment = env_by_name(container)
                self.assertEqual(
                    "fixture-hugegraph-server.graphs.svc:8080",
                    environment["GRAPH_URL"]["value"],
                )

    def test_mode_controls_only_the_default_service_exposure(self):
        expected = {"easy": "NodePort", "strict": "ClusterIP"}
        for mode, service_type in expected.items():
            with self.subTest(mode=mode):
                documents = self.documents({}, mode=mode)
                service, = component_resources(documents, kind="Service")
                self.assertEqual(service_type, service["spec"]["type"])
        documents = self.documents({"service": {"type": "ClusterIP"}}, mode="easy")
        service, = component_resources(documents, kind="Service")
        self.assertEqual("ClusterIP", service["spec"]["type"])

    def test_user_environment_replaces_automatic_entries(self):
        documents = self.documents({
            "env": [
                {"name": "GRAPH_URL", "value": "http://external:8080"},
                {"name": "GRAPH_PWD", "valueFrom": {"secretKeyRef": {"name": "external", "key": "pwd"}}},
            ]
        })
        container, = pod(self.deployment(documents))["spec"]["containers"]
        entries = [entry for entry in container["env"] if entry["name"] in ("GRAPH_URL", "GRAPH_PWD")]
        self.assertEqual(2, len(entries))
        environment = env_by_name(container)
        self.assertEqual("http://external:8080", environment["GRAPH_URL"]["value"])
        self.assertEqual("external", environment["GRAPH_PWD"]["valueFrom"]["secretKeyRef"]["name"])

    def test_external_server_uses_only_explicit_password_secret(self):
        documents = self.documents({"hugegraph": {
            "autoConfigure": True,
            "url": "http://remote.graphs.svc:8080",
            "passwordSecret": {"name": "remote-admin", "key": "credential"},
        }})
        container, = pod(self.deployment(documents))["spec"]["containers"]
        environment = env_by_name(container)
        self.assertEqual("http://remote.graphs.svc:8080", environment["GRAPH_URL"]["value"])
        self.assertEqual("remote-admin", environment["GRAPH_PWD"]["valueFrom"]["secretKeyRef"]["name"])
        self.assertEqual("credential", environment["GRAPH_PWD"]["valueFrom"]["secretKeyRef"]["key"])

        documents = self.documents({"hugegraph": {
            "autoConfigure": True,
            "url": "http://remote.graphs.svc:8080",
        }})
        container, = pod(self.deployment(documents))["spec"]["containers"]
        self.assertNotIn("GRAPH_PWD", env_by_name(container))

    def test_persistence_seeds_the_resource_directory(self):
        documents = self.documents({"persistence": {"enabled": True}})
        deployment = self.deployment(documents)
        pvc, = component_resources(documents, kind="PersistentVolumeClaim")
        self.assertEqual("fixture-hugegraph-rag-data", pvc["metadata"]["name"])
        self.assertEqual("Recreate", deployment["spec"]["strategy"]["type"])
        init, = pod(deployment)["spec"]["initContainers"]
        self.assertIn("cp -a", init["args"][0])
        container, = pod(deployment)["spec"]["containers"]
        mount, = [item for item in container["volumeMounts"] if item["name"] == "data"]
        self.assertEqual(
            "/home/work/hugegraph-llm/src/hugegraph_llm/resources",
            mount["mountPath"],
        )

    def test_multiple_generated_persistent_replicas_require_rwx(self):
        result = self.render({"replicas": 2, "persistence": {"enabled": True}})
        self.assertNotEqual(0, result.returncode)
        self.assertIn("ReadWriteMany", result.stderr)
        documents = self.documents({
            "replicas": 2,
            "persistence": {"enabled": True, "accessModes": ["ReadWriteMany"]},
        })
        self.assertEqual(2, self.deployment(documents)["spec"]["replicas"])

    def test_strict_network_policy_allows_internal_server_only(self):
        documents = self.documents({}, mode="strict")
        policies = component_resources(documents, kind="NetworkPolicy")
        self.assertEqual(2, len(policies))
        rag_policy = next(policy for policy in policies if policy["spec"]["podSelector"]["matchLabels"]["app.kubernetes.io/component"] == "rag")
        server_allow = next(policy for policy in policies if policy is not rag_policy)
        self.assertEqual("server", server_allow["spec"]["podSelector"]["matchLabels"]["app.kubernetes.io/component"])
        self.assertTrue(any(rule.get("to") for rule in rag_policy["spec"]["egress"]))

        documents = self.documents({"hugegraph": {
            "autoConfigure": True,
            "url": "http://external:8080",
        }}, mode="strict")
        policy, = component_resources(documents, kind="NetworkPolicy")
        self.assertFalse(any(
            peer.get("podSelector", {}).get("matchLabels", {}).get("app.kubernetes.io/component") == "server"
            for rule in policy["spec"]["egress"]
            for peer in rule.get("to", [])
        ))

    def test_global_registry_and_digest_render(self):
        digest = "sha256:" + "a" * 64
        documents = self.documents(
            {"image": {"repository": "hugegraph/rag", "tag": "ignored", "digest": digest}},
            **{"global": {"imageRegistry": "registry.example.com"}},
        )
        deployment = self.deployment(documents)
        expected = f"registry.example.com/hugegraph/rag@{digest}"
        self.assertEqual(expected, pod(deployment)["spec"]["containers"][0]["image"])

    def test_invalid_service_contracts_fail_before_render(self):
        invalid = (
            {"service": {"type": "ExternalName"}},
            {"service": {"type": "ClusterIP", "nodePort": 31001}},
            {"port": 0},
            {"replicas": 0},
        )
        for values in invalid:
            with self.subTest(values=values):
                self.assertNotEqual(0, self.render(values).returncode)


if __name__ == "__main__":
    unittest.main()
