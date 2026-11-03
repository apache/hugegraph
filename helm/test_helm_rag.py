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
RAG_MOUNT = "/home/work/hugegraph-llm/src/hugegraph_llm/resources"


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
    def test_profile_is_internal_persistent_and_matches_the_image_contract(self):
        values = yaml.safe_load((CHART / "values-rag.yaml").read_text(encoding="utf-8"))
        rag = values["rag"]
        self.assertTrue(rag["enabled"])
        self.assertNotIn("replicas", rag, "topology presets own the optional replica count")
        self.assertEqual("hugegraph/rag", rag["image"]["repository"])
        self.assertEqual(8001, rag["port"])
        self.assertEqual({"autoConfigure": True}, rag["hugegraph"])
        self.assertEqual("ClusterIP", rag["service"]["type"])
        self.assertTrue(rag["persistence"]["enabled"])
        self.assertEqual(RAG_MOUNT, rag["persistence"]["mountPath"])

    def test_single_and_cluster_presets_keep_one_persistent_rag_replica(self):
        single = yaml.safe_load((CHART / "values-single.yaml").read_text(encoding="utf-8"))
        cluster = yaml.safe_load((CHART / "values-cluster.yaml").read_text(encoding="utf-8"))
        self.assertEqual({"enabled": False, "replicas": 1}, single["rag"])
        self.assertEqual({"enabled": False, "replicas": 1}, cluster["rag"])

    def test_templates_use_one_process_port_and_persistent_native_paths(self):
        deployment = (CHART / "templates" / "rag-deployment.yaml").read_text(encoding="utf-8")
        service = (CHART / "templates" / "rag-service.yaml").read_text(encoding="utf-8")
        helpers = (CHART / "templates" / "_rag.tpl").read_text(encoding="utf-8")
        self.assertIn("hugegraph_llm.demo.rag_demo.app", deployment)
        self.assertIn('containerPort: {{ include "hugegraph.rag.port" . | int }}', deployment)
        self.assertIn('include "hugegraph.rag.port" . | int | quote', deployment)
        self.assertIn("targetPort: http", service)
        self.assertIn('include "hugegraph.server.clientUrl"', helpers)
        self.assertIn('trimPrefix "http://"', helpers)
        self.assertIn("HUGEGRAPH_LLM_ENV_PATH", deployment)
        self.assertIn("HUGEGRAPH_LLM_PROMPT_CONFIG_PATH", deployment)
        self.assertNotIn("touch /data/.hugegraph-rag-seeded", deployment)


class RagChartTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if shutil.which("helm") is None:
            raise unittest.SkipTest("Helm is required for rendered chart regression tests")

    def render(self, rag=None, preset=None, mode=None, use_profile=True, **values):
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
            if rag is not None and use_profile:
                command.extend(["-f", str(CHART / "values-rag.yaml")])
            command.extend(["-f", str(configuration)])
            return subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=HELM_TEMPLATE_TIMEOUT_SECONDS,
                check=False,
            )

    def documents(self, rag=None, preset=None, mode=None, use_profile=True, **values):
        result = self.render(rag, preset=preset, mode=mode, use_profile=use_profile, **values)
        self.assertEqual(0, result.returncode, result.stderr)
        return [document for document in yaml.safe_load_all(result.stdout) if document]

    def deployment(self, documents):
        deployment, = component_resources(documents, kind="Deployment")
        return deployment

    def container(self, documents):
        container, = pod(self.deployment(documents))["spec"]["containers"]
        return container

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
        self.assertFalse(container["securityContext"]["allowPrivilegeEscalation"])
        self.assertEqual(["ALL"], container["securityContext"]["capabilities"]["drop"])

        environment = env_by_name(container)
        self.assertEqual(f"{RAG_MOUNT}/.env", environment["HUGEGRAPH_LLM_ENV_PATH"]["value"])
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

        self.assertEqual("fixture-hugegraph-server.graphs.svc:8080", environment["GRAPH_URL"]["value"])
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
        self.assertEqual("9001", container["args"][3])
        self.assertEqual(9001, container["ports"][0]["containerPort"])
        self.assertEqual(9001, service["spec"]["ports"][0]["port"])
        for name in ("startupProbe", "readinessProbe", "livenessProbe"):
            self.assertEqual("http", container[name]["httpGet"]["port"])

    def test_single_and_cluster_presets_share_the_same_graph_contract(self):
        for preset in ("values-single.yaml", "values-cluster.yaml"):
            with self.subTest(preset=preset):
                documents = self.documents({}, preset=preset)
                deployment = self.deployment(documents)
                environment = env_by_name(self.container(documents))
                self.assertEqual(1, deployment["spec"]["replicas"])
                self.assertEqual(
                    "fixture-hugegraph-server.graphs.svc:8080",
                    environment["GRAPH_URL"]["value"],
                )

    def test_service_stays_internal_unless_exposure_is_explicit(self):
        for mode in ("easy", "strict"):
            with self.subTest(mode=mode):
                documents = self.documents({}, mode=mode)
                service, = component_resources(documents, kind="Service")
                self.assertEqual("ClusterIP", service["spec"]["type"])
        documents = self.documents({"service": {"type": "NodePort", "nodePort": 31001}}, mode="easy")
        service, = component_resources(documents, kind="Service")
        self.assertEqual("NodePort", service["spec"]["type"])
        self.assertEqual(31001, service["spec"]["ports"][0]["nodePort"])

    def test_user_environment_replaces_automatic_entries(self):
        documents = self.documents({
            "env": [
                {"name": "GRAPH_URL", "value": "external:8080"},
                {"name": "GRAPH_PWD", "valueFrom": {"secretKeyRef": {"name": "external", "key": "pwd"}}},
            ]
        })
        environment = env_by_name(self.container(documents))
        self.assertEqual("external:8080", environment["GRAPH_URL"]["value"])
        self.assertEqual("external", environment["GRAPH_PWD"]["valueFrom"]["secretKeyRef"]["name"])

    def test_external_graph_url_in_env_never_inherits_internal_admin_password(self):
        documents = self.documents({"env": [{"name": "GRAPH_URL", "value": "external:8080"}]})
        environment = env_by_name(self.container(documents))
        self.assertEqual("external:8080", environment["GRAPH_URL"]["value"])
        self.assertNotIn("GRAPH_PWD", environment)
        self.assertNotIn("checksum/hugegraph-auth", pod(self.deployment(documents))["metadata"].get("annotations", {}))

    def test_external_server_uses_only_explicit_password_secret(self):
        documents = self.documents({"hugegraph": {
            "autoConfigure": True,
            "url": "http://remote.graphs.svc:8080",
            "passwordSecret": {"name": "remote-admin", "key": "credential"},
        }})
        environment = env_by_name(self.container(documents))
        self.assertEqual("http://remote.graphs.svc:8080", environment["GRAPH_URL"]["value"])
        self.assertEqual("remote-admin", environment["GRAPH_PWD"]["valueFrom"]["secretKeyRef"]["name"])
        self.assertEqual("credential", environment["GRAPH_PWD"]["valueFrom"]["secretKeyRef"]["key"])
        self.assertIn("checksum/hugegraph-auth", pod(self.deployment(documents))["metadata"]["annotations"])

        documents = self.documents({"hugegraph": {
            "autoConfigure": True,
            "url": "http://remote.graphs.svc:8080",
        }})
        self.assertNotIn("GRAPH_PWD", env_by_name(self.container(documents)))

    def test_custom_password_secret_defaults_to_password_key(self):
        documents = self.documents({"hugegraph": {
            "passwordSecret": {"name": "rag-credentials"},
        }})
        password = env_by_name(self.container(documents))["GRAPH_PWD"]["valueFrom"]["secretKeyRef"]
        self.assertEqual("rag-credentials", password["name"])
        self.assertEqual("password", password["key"])

    def test_internal_server_secret_keeps_its_configured_key(self):
        documents = self.documents({}, server={"auth": {"admin": {"key": "admin-password"}}})
        password = env_by_name(self.container(documents))["GRAPH_PWD"]["valueFrom"]["secretKeyRef"]
        self.assertEqual("admin-password", password["key"])

    def test_persistence_seeds_and_reuses_all_writable_configuration(self):
        documents = self.documents({"persistence": {"enabled": True}})
        deployment = self.deployment(documents)
        pvc, = component_resources(documents, kind="PersistentVolumeClaim")
        self.assertEqual("fixture-hugegraph-rag-data", pvc["metadata"]["name"])
        self.assertEqual("Recreate", deployment["spec"]["strategy"]["type"])
        init, = pod(deployment)["spec"]["initContainers"]
        self.assertIn("cp -a -n", init["args"][0])
        self.assertNotIn(".hugegraph-rag-seeded", init["args"][0])
        container = self.container(documents)
        mount, = [item for item in container["volumeMounts"] if item["name"] == "data"]
        self.assertEqual(RAG_MOUNT, mount["mountPath"])
        environment = env_by_name(container)
        self.assertEqual(f"{RAG_MOUNT}/.env", environment["HUGEGRAPH_LLM_ENV_PATH"]["value"])
        self.assertEqual(
            f"{RAG_MOUNT}/demo/config_prompt.yaml",
            environment["HUGEGRAPH_LLM_PROMPT_CONFIG_PATH"]["value"],
        )

    def test_custom_mount_path_updates_both_native_config_paths(self):
        mount_path = "/var/lib/hugegraph-rag"
        documents = self.documents({"persistence": {"mountPath": mount_path}})
        container = self.container(documents)
        environment = env_by_name(container)
        self.assertEqual(f"{mount_path}/.env", environment["HUGEGRAPH_LLM_ENV_PATH"]["value"])
        self.assertEqual(
            f"{mount_path}/demo/config_prompt.yaml",
            environment["HUGEGRAPH_LLM_PROMPT_CONFIG_PATH"]["value"],
        )
        mount, = [item for item in container["volumeMounts"] if item["name"] == "data"]
        self.assertEqual(mount_path, mount["mountPath"])

    def test_persistence_can_be_disabled_for_disposable_runs(self):
        documents = self.documents({"persistence": {"enabled": False}})
        deployment = self.deployment(documents)
        self.assertEqual([], component_resources(documents, kind="PersistentVolumeClaim"))
        self.assertNotIn("initContainers", pod(deployment)["spec"])
        container = self.container(documents)
        self.assertFalse(any(item["name"] == "data" for item in container.get("volumeMounts", [])))
        environment = env_by_name(container)
        self.assertEqual("/tmp/hugegraph-llm.env", environment["HUGEGRAPH_LLM_ENV_PATH"]["value"])
        self.assertEqual(
            f"{RAG_MOUNT}/demo/config_prompt.yaml",
            environment["HUGEGRAPH_LLM_PROMPT_CONFIG_PATH"]["value"],
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
        expected = f"registry.example.com/hugegraph/rag@{digest}"
        deployment = self.deployment(documents)
        self.assertEqual(expected, pod(deployment)["spec"]["containers"][0]["image"])
        self.assertEqual(expected, pod(deployment)["spec"]["initContainers"][0]["image"])

    def test_invalid_contracts_fail_before_render(self):
        invalid = (
            ({"service": {"type": "ExternalName"}}, "service.type"),
            ({"service": {"type": "ClusterIP", "nodePort": 31001}}, "nodePort"),
            ({"service": {"type": "NodePort", "nodePort": "3100x"}}, "nodePort"),
            ({"port": 0}, "rag.port"),
            ({"port": 8001.5}, "rag.port"),
            ({"replicas": 0}, "rag.replicas"),
            ({"replicas": 1.5}, "rag.replicas"),
            ({"podSecurityContext": {"runAsUser": 0}}, "runAsUser"),
            ({"podSecurityContext": {"fsGroup": 0}}, "fsGroup"),
            ({"securityContext": {"allowPrivilegeEscalation": True}}, "allowPrivilegeEscalation"),
            ({"hugegraph": {"passwordSecret": {"key": "password"}}}, "passwordSecret.name"),
            ({"persistence": {"mountPath": "relative/path"}}, "mountPath"),
            ({"env": [{"name": "GRAPH_URL", "value": "a"}, {"name": "GRAPH_URL", "value": "b"}]}, "duplicate"),
        )
        for values, expected in invalid:
            with self.subTest(values=values):
                result = self.render(values)
                self.assertNotEqual(0, result.returncode)
                self.assertIn(expected, result.stderr)


if __name__ == "__main__":
    unittest.main()
