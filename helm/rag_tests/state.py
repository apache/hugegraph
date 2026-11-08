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

"""State and network regression coverage for the optional RAG component."""

import subprocess

import yaml

from .support import (
    RAG_EPHEMERAL_ENV, RAG_MOUNT, component_resources, env_by_name, pod,
)


class RagStateMixin:
    def test_native_process_owns_configuration_and_resource_initialization(self):
        documents = self.documents({})
        deployment = self.deployment(documents)
        container = self.container(documents)
        self.assertNotIn("initContainers", pod(deployment)["spec"])
        self.assertEqual(["python", "-m", "hugegraph_llm.demo.rag_demo.app"], container["command"])
        self.assertEqual(RAG_MOUNT, env_by_name(container)["HUGEGRAPH_LLM_RESOURCE_PATH"]["value"])

    def test_env_from_reaches_the_native_application(self):
        container = self.container(self.documents({"envFrom": [{"secretRef": {"name": "rag-model"}}]}))
        self.assertEqual([{"secretRef": {"name": "rag-model"}}], container["envFrom"])

    def test_custom_mount_path_sets_resource_root_for_application_defaults(self):
        mount_path = "/srv/hugegraph-rag"
        documents = self.documents({"persistence": {"mountPath": mount_path}})
        container = self.container(documents)
        environment = env_by_name(container)
        self.assertNotIn("HUGEGRAPH_LLM_ENV_PATH", environment)
        self.assertNotIn("HUGEGRAPH_LLM_PROMPT_CONFIG_PATH", environment)
        mount, = [item for item in container["volumeMounts"] if item["name"] == "data"]
        self.assertEqual(mount_path, mount["mountPath"])
        self.assertEqual(mount_path, environment["HUGEGRAPH_LLM_RESOURCE_PATH"]["value"])

    def test_explicit_configuration_paths_and_env_from_are_preserved(self):
        explicit = [
            {"name": "HUGEGRAPH_LLM_ENV_PATH", "value": "/mounted/config.env"},
            {"name": "HUGEGRAPH_LLM_PROMPT_CONFIG_PATH", "value": "/mounted/prompts.yaml"},
        ]
        env_from = [{"secretRef": {"name": "rag-model"}}]
        container = self.container(self.documents({"env": explicit, "envFrom": env_from}))
        environment = env_by_name(container)
        self.assertEqual("/mounted/config.env", environment["HUGEGRAPH_LLM_ENV_PATH"]["value"])
        self.assertEqual("/mounted/prompts.yaml", environment["HUGEGRAPH_LLM_PROMPT_CONFIG_PATH"]["value"])
        self.assertEqual(env_from, container["envFrom"])

    def test_persistence_can_be_disabled_for_disposable_runs(self):
        documents = self.documents({"persistence": {"enabled": False}})
        deployment = self.deployment(documents)
        self.assertEqual("Recreate", deployment["spec"]["strategy"]["type"])
        self.assertEqual([], component_resources(documents, kind="PersistentVolumeClaim"))
        self.assertNotIn("initContainers", pod(deployment)["spec"])
        container = self.container(documents)
        self.assertFalse(any(item["name"] == "data" for item in container.get("volumeMounts", [])))
        environment = env_by_name(container)
        self.assertEqual(RAG_EPHEMERAL_ENV, environment["HUGEGRAPH_LLM_ENV_PATH"]["value"])
        self.assertEqual(
            "/home/work/hugegraph-llm/src/hugegraph_llm/resources/demo/config_prompt.yaml",
            environment["HUGEGRAPH_LLM_PROMPT_CONFIG_PATH"]["value"],
        )

    def test_global_persistence_disable_is_inherited_when_rag_override_is_unset(self):
        documents = self.documents({}, use_profile=False, persistence={"enabled": False})
        self.assertEqual([], component_resources(documents, kind="PersistentVolumeClaim"))
        container = self.container(documents)
        self.assertEqual(
            RAG_EPHEMERAL_ENV,
            env_by_name(container)["HUGEGRAPH_LLM_ENV_PATH"]["value"],
        )

    def test_multiple_replicas_are_rejected_even_with_shared_storage(self):
        for persistence in (
            {"enabled": False},
            {"enabled": True, "accessModes": ["ReadWriteMany"]},
            {"enabled": True, "existingClaim": "shared-rag-resources"},
        ):
            with self.subTest(persistence=persistence):
                result = self.render({"replicas": 2, "persistence": persistence})
                self.assertNotEqual(0, result.returncode)
                self.assertIn("rag.replicas", result.stderr.replace("/", "."))

    def test_single_replica_remains_fixed_without_schema_validation(self):
        template_help = subprocess.run(
            ["helm", "template", "--help"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(0, template_help.returncode, template_help.stderr)
        if "--skip-schema-validation" not in template_help.stdout:
            self.skipTest("helm template lacks --skip-schema-validation")

        result = self.render({"replicas": 2}, skip_schema_validation=True)
        self.assertEqual(0, result.returncode, result.stderr)
        documents = [document for document in yaml.safe_load_all(result.stdout) if document]
        self.assertEqual(1, self.deployment(documents)["spec"]["replicas"])

    def test_strict_network_policy_allows_only_scoped_server_and_dns_rules(self):
        documents = self.documents({}, mode="strict")
        policies = component_resources(documents, kind="NetworkPolicy")
        self.assertEqual(2, len(policies))
        rag_policy = next(
            policy for policy in policies
            if policy["spec"]["podSelector"]["matchLabels"]["app.kubernetes.io/component"] == "rag"
        )
        server_allow = next(policy for policy in policies if policy is not rag_policy)
        self.assertEqual(
            "server",
            server_allow["spec"]["podSelector"]["matchLabels"]["app.kubernetes.io/component"],
        )

        egress = rag_policy["spec"]["egress"]
        server_rules = [
            rule for rule in egress
            if any(
                peer.get("podSelector", {}).get("matchLabels", {}).get(
                    "app.kubernetes.io/component"
                ) == "server"
                for peer in rule.get("to", [])
            )
        ]
        self.assertEqual(1, len(server_rules))
        server_ports = {
            (item.get("protocol"), item.get("port"))
            for item in server_rules[0].get("ports", [])
        }
        self.assertEqual(
            {("TCP", 8080)},
            server_ports,
            "Internal Server egress must remain scoped to the REST port",
        )
        for rule in egress:
            if rule in server_rules:
                continue
            self.assertFalse(rule.get("to"), "Only the scoped Server rule may name destination peers")
            ports = {(item.get("protocol"), item.get("port")) for item in rule.get("ports", [])}
            self.assertEqual(
                {("UDP", 53), ("TCP", 53)},
                ports,
                "Only the DNS rule may omit destination peers; an empty rule opens all egress",
            )

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
        for rule in policy["spec"]["egress"]:
            self.assertFalse(rule.get("to"), "Without a Server peer, only the DNS rule may be rendered")
            ports = {(item.get("protocol"), item.get("port")) for item in rule.get("ports", [])}
            self.assertEqual({("UDP", 53), ("TCP", 53)}, ports)

    def test_global_registry_and_digest_render(self):
        digest = "sha256:" + "a" * 64
        documents = self.documents(
            {"image": {"repository": "hugegraph/rag", "tag": "ignored", "digest": digest}},
            **{"global": {"imageRegistry": "registry.example.com"}},
        )
        expected = f"registry.example.com/hugegraph/rag@{digest}"
        deployment = self.deployment(documents)
        self.assertEqual(expected, pod(deployment)["spec"]["containers"][0]["image"])
