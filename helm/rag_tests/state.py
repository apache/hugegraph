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

    def test_custom_mount_path_updates_both_durable_config_paths(self):
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
                self.assertIn("rag.replicas", result.stderr)

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
        self.assertTrue(any(
            peer.get("podSelector", {}).get("matchLabels", {}).get("app.kubernetes.io/component") == "server"
            for rule in egress
            for peer in rule.get("to", [])
        ))
        for rule in egress:
            if rule.get("to"):
                continue
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
            if rule.get("to"):
                continue
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
