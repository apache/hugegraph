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

import os
from pathlib import Path
import subprocess
import sys
import tempfile

from .support import (
    RAG_EPHEMERAL_ENV,
    RAG_MOUNT,
    RAG_PERSISTENT_ENV,
    component_resources,
    env_by_name,
    pod,
)


_DOTENV_STUB = '''from pathlib import Path


def dotenv_values(path):
    values = {}
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        if not raw or raw.lstrip().startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        values[key] = value
    return values


def unset_key(path, key):
    target = Path(path)
    kept = []
    for raw in target.read_text(encoding="utf-8").splitlines():
        if "=" in raw and raw.split("=", 1)[0] == key:
            continue
        kept.append(raw)
    target.write_text("\\n".join(kept) + ("\\n" if kept else ""), encoding="utf-8")
    return True, key
'''


def _reconciliation_script(container):
    startup = container["args"][0]
    marker = "python - <<'PY'\n"
    if marker not in startup:
        raise AssertionError("rendered startup command does not contain the reconciliation script")
    return startup.split(marker, 1)[1].split("\nPY\n", 1)[0]


def _execution_environment(directory, env_path):
    module_dir = Path(directory) / "pythonpath"
    module_dir.mkdir()
    (module_dir / "dotenv.py").write_text(_DOTENV_STUB, encoding="utf-8")
    environment = os.environ.copy()
    existing = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = str(module_dir) + (os.pathsep + existing if existing else "")
    environment["HUGEGRAPH_LLM_ENV_PATH"] = str(env_path)
    return environment


def _read_env(path):
    values = {}
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        if not raw or raw.lstrip().startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        values[key] = value
    return values


class RagStateMixin:
    def test_persistence_seeds_resources_and_reconciles_runtime_environment(self):
        documents = self.documents({"persistence": {"enabled": True}})
        deployment = self.deployment(documents)
        pvc, = component_resources(documents, kind="PersistentVolumeClaim")
        self.assertEqual("fixture-hugegraph-rag-data", pvc["metadata"]["name"])
        self.assertEqual("Recreate", deployment["spec"]["strategy"]["type"])

        init, = pod(deployment)["spec"]["initContainers"]
        self.assertIn("cp -R -n", init["args"][0])
        self.assertNotIn("cp -a", init["args"][0])
        self.assertNotIn(".hugegraph-rag-seeded", init["args"][0])

        container = self.container(documents)
        startup = container["args"][0]
        self.assertIn("dotenv_values", startup)
        self.assertIn("managed_keys", startup)
        self.assertIn("OPENAI_CHAT_API_KEY", startup)
        self.assertIn("unset_key", startup)
        self.assertIn("exec python -m hugegraph_llm.demo.rag_demo.app", startup)

        mount, = [item for item in container["volumeMounts"] if item["name"] == "data"]
        self.assertEqual(RAG_MOUNT, mount["mountPath"])
        environment = env_by_name(container)
        self.assertEqual(RAG_PERSISTENT_ENV, environment["HUGEGRAPH_LLM_ENV_PATH"]["value"])
        self.assertEqual(
            f"{RAG_MOUNT}/demo/config_prompt.yaml",
            environment["HUGEGRAPH_LLM_PROMPT_CONFIG_PATH"]["value"],
        )

    def test_reconciliation_executes_rotations_and_rejects_empty_env_path(self):
        documents = self.documents({
            "env": [{"name": "GRAPH_URL", "value": "http://external:8080"}],
        })
        container = self.container(documents)
        self.assertNotIn("GRAPH_PWD", env_by_name(container))
        script = _reconciliation_script(container)

        pod_key = "RAG_TEST_POD_VALUE_7F11"
        file_key = "RAG_TEST_FILE_VALUE_7F11"
        stale_values = {
            pod_key: "stale",
            file_key: "keep",
            "GRAPH_URL": "http://old-internal:8080",
            "GRAPH_NAME": "old-graph",
            "GRAPH_PWD": "stale-local-admin-password",
            "OPENAI_CHAT_API_KEY": "stale-provider-key",
            "OPENAI_EXTRACT_API_KEY": "stale-provider-key",
            "OPENAI_TEXT2GQL_API_KEY": "stale-provider-key",
            "OPENAI_CHAT_API_BASE": "https://old-provider.invalid/v1",
            "OPENAI_EXTRACT_API_BASE": "https://old-provider.invalid/v1",
            "OPENAI_TEXT2GQL_API_BASE": "https://old-provider.invalid/v1",
            "OPENAI_EMBEDDING_API_BASE": "https://old-embedding.invalid/v1",
            "COHERE_BASE_URL": "https://old-cohere.invalid/v1/rerank",
        }
        with tempfile.TemporaryDirectory(prefix="rag-reconcile-") as directory:
            env_path = Path(directory) / ".env"
            env_path.write_text(
                "".join(f"{key}={value}\n" for key, value in stale_values.items()),
                encoding="utf-8",
            )
            environment = _execution_environment(directory, env_path)
            for key in stale_values:
                environment.pop(key, None)
            environment.update({
                pod_key: "fresh",
                "GRAPH_URL": "http://external:8080",
                "OPENAI_API_KEY": "fresh-provider-key",
                "OPENAI_BASE_URL": "https://new-provider.invalid/v1",
                "OPENAI_EMBEDDING_BASE_URL": "https://new-embedding.invalid/v1",
                "CO_API_URL": "https://new-cohere.invalid/v1/rerank",
            })
            result = subprocess.run(
                [sys.executable, "-"],
                input=script,
                capture_output=True,
                text=True,
                env=environment,
                timeout=30,
                check=False,
            )
            self.assertEqual(0, result.returncode, result.stderr)
            values = _read_env(env_path)
            removed = set(stale_values) - {file_key}
            for key in removed:
                self.assertNotIn(key, values, f"stale managed value {key} survived reconciliation")
            self.assertEqual("keep", values[file_key])

            environment["HUGEGRAPH_LLM_ENV_PATH"] = ""
            result = subprocess.run(
                [sys.executable, "-"],
                input=script,
                capture_output=True,
                text=True,
                env=environment,
                timeout=30,
                check=False,
            )
            self.assertNotEqual(0, result.returncode)
            self.assertIn("HUGEGRAPH_LLM_ENV_PATH must not be empty", result.stderr)

    def test_reconciliation_preserves_file_owned_settings_when_auto_configure_is_off(self):
        container = self.container(self.documents({
            "hugegraph": {"autoConfigure": False},
        }))
        script = _reconciliation_script(container)
        retained = {
            "GRAPH_URL": "http://operator-owned:8080",
            "GRAPH_PWD": "operator-owned-password",
            "OPENAI_CHAT_API_KEY": "operator-owned-provider-key",
        }
        with tempfile.TemporaryDirectory(prefix="rag-reconcile-owned-") as directory:
            env_path = Path(directory) / ".env"
            env_path.write_text(
                "".join(f"{key}={value}\n" for key, value in retained.items()),
                encoding="utf-8",
            )
            environment = _execution_environment(directory, env_path)
            for key in retained:
                environment.pop(key, None)
            environment.pop("OPENAI_API_KEY", None)
            result = subprocess.run(
                [sys.executable, "-"],
                input=script,
                capture_output=True,
                text=True,
                env=environment,
                timeout=30,
                check=False,
            )
            self.assertEqual(0, result.returncode, result.stderr)
            self.assertEqual(retained, _read_env(env_path))

    def test_env_from_is_rendered_and_covered_by_generic_precedence(self):
        documents = self.documents({
            "envFrom": [{"secretRef": {"name": "rag-model"}}],
        })
        container = self.container(documents)
        self.assertEqual([{"secretRef": {"name": "rag-model"}}], container["envFrom"])
        self.assertIn("if key in os.environ", container["args"][0])

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
            f"{RAG_MOUNT}/demo/config_prompt.yaml",
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
        self.assertEqual(expected, pod(deployment)["spec"]["initContainers"][0]["image"])
