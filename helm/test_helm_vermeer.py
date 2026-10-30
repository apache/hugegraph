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

"""Validate optional native Vermeer deployment and its integration boundaries."""

import base64
import configparser
import copy
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import yaml


CHART = Path(__file__).resolve().parent / "hugegraph"
COMPONENT = "app.kubernetes.io/component"
INSTANCE = "app.kubernetes.io/instance"
FACTOR = "vermeer-test-factor-0123456789abcdef0123456789abcdef"
NATIVE_ADMIN = "VermeerAdmin0123"


def resources(documents, kind=None, component=None):
    return [doc for doc in documents if (kind is None or doc["kind"] == kind)
            and (component is None or
                 doc["metadata"].get("labels", {}).get(COMPONENT) == component)]


def vermeer_resources(documents, kind=None):
    return [doc for doc in resources(documents, kind)
            if doc["metadata"].get("labels", {}).get(COMPONENT, "").startswith("vermeer")]


def pod(workload):
    return workload["spec"]["template"]


def env(container):
    return {entry["name"]: entry for entry in container.get("env", [])}


def merged(left, right):
    result = copy.deepcopy(left)
    for key, value in right.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merged(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def selector_matches(selector, labels):
    if any(labels.get(key) != value for key, value in selector.get("matchLabels", {}).items()):
        return False
    for expression in selector.get("matchExpressions", []):
        key, operator = expression["key"], expression["operator"]
        values = expression.get("values", [])
        if operator == "In" and labels.get(key) not in values:
            return False
        if operator == "NotIn" and labels.get(key) in values:
            return False
        if operator == "Exists" and key not in labels:
            return False
        if operator == "DoesNotExist" and key in labels:
            return False
    return True


def policy_allows(policies, target, peer, port, direction):
    policy_type, peer_key = ("Ingress", "from") if direction == "ingress" else ("Egress", "to")
    selected = [policy["spec"] for policy in policies
                if policy_type in policy["spec"].get("policyTypes", ["Ingress"])
                and selector_matches(policy["spec"]["podSelector"], target)]
    if not selected:
        return True
    for policy in selected:
        for rule in policy.get(direction, []):
            if rule.get("ports") and not any(entry["port"] == port and
                                             entry.get("protocol", "TCP") == "TCP"
                                             for entry in rule["ports"]):
                continue
            if not rule.get(peer_key):
                return True
            if any("ipBlock" not in candidate and
                   selector_matches(candidate.get("podSelector", {}), peer)
                   for candidate in rule[peer_key]):
                return True
    return False


class VermeerChartTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if shutil.which("helm") is None:
            raise RuntimeError("Helm is required for the Vermeer deployment regression")

    def render(self, vermeer=None, chart=CHART, preset=None, release="fixture", namespace="graphs",
               upgrade=False, deterministic_auth=True, **overrides):
        values = {"pd": {"auth": {"value": "pd-test-service-password"}},
                  "server": {"auth": {"admin": {"password": "graph-test-admin-password"},
                                      "token": {"value": "0123456789abcdef0123456789abcdef"}}}}
        if not deterministic_auth:
            values = {}
        if vermeer is not None:
            values["vermeer"] = merged({"enabled": True, "auth": {
                "value": FACTOR, "admin": {"password": NATIVE_ADMIN}}}, vermeer)
        values = merged(values, overrides)
        with tempfile.TemporaryDirectory(prefix="vermeer-values-") as directory:
            configuration = Path(directory) / "values.yaml"
            configuration.write_text(yaml.safe_dump(values))
            command = ["helm", "template", release, str(chart), "--namespace", namespace]
            if preset:
                command.extend(["--values", str(CHART / preset)])
            command.extend(["--values", str(configuration)])
            if upgrade:
                command.append("--is-upgrade")
            return subprocess.run(command, capture_output=True, text=True, timeout=120)

    def documents(self, vermeer=None, **values):
        result = self.render(vermeer, **values)
        self.assertEqual(0, result.returncode, result.stderr)
        return [doc for doc in yaml.safe_load_all(result.stdout) if doc]

    def workloads(self, documents):
        return vermeer_resources(documents, "StatefulSet")

    def role_container(self, documents, role):
        return next(container for workload in self.workloads(documents)
                    for container in pod(workload)["spec"]["containers"]
                    if container["name"] == role)

    def test_optional_components_stay_disabled_in_every_default_preset(self):
        defaults = yaml.safe_load((CHART / "values.yaml").read_text())
        self.assertFalse(defaults["vermeer"]["enabled"])
        self.assertFalse(defaults["seatunnel"]["enabled"])
        for preset in (None, "values-single.yaml", "values-cluster.yaml", "values-strict.yaml"):
            with self.subTest(preset=preset):
                documents = self.documents(preset=preset)
                self.assertEqual([], vermeer_resources(documents))
                self.assertEqual([], resources(documents, component="seatunnel"))

    def test_legacy_values_and_registry_preserve_existing_entry_defaults(self):
        with tempfile.TemporaryDirectory(prefix="vermeer-legacy-") as directory:
            chart = Path(directory) / "chart"
            shutil.copytree(CHART, chart)
            (chart / "values.yaml").write_bytes((CHART / "testdata/values-pre-hardening.yaml").read_bytes())
            for upgrade, registry in ((False, ""), (True, ""), (True, "registry.example.com")):
                with self.subTest(upgrade=upgrade, registry=registry):
                    documents = self.documents(chart=chart, upgrade=upgrade, deterministic_auth=False,
                                               **{"global": {"imageRegistry": registry}})
                    self.assertEqual([], vermeer_resources(documents))
            values = yaml.safe_load((CHART / "values.yaml").read_text())
            values.pop("mode")
            (chart / "values.yaml").write_text(yaml.safe_dump(values))
            documents = self.documents({}, chart=chart, seatunnel={"enabled": True})
            for component in ("vermeer-master", "seatunnel"):
                entry, = [service for service in resources(documents, "Service", component)
                          if service["spec"].get("clusterIP") != "None"]
                self.assertEqual("ClusterIP", entry["spec"]["type"])

    def test_standalone_shares_one_pod_with_two_native_processes(self):
        documents = self.documents({})
        workload, = self.workloads(documents)
        self.assertEqual(1, workload["spec"]["replicas"])
        self.assertEqual("vermeer-master", workload["metadata"]["labels"][COMPONENT])
        self.assertEqual({"master", "worker"},
                         {container["name"] for container in pod(workload)["spec"]["containers"]})
        self.assertFalse(pod(workload)["spec"].get("automountServiceAccountToken", True))
        self.assertEqual([], vermeer_resources(documents, "Deployment"))
        self.assertEqual([], vermeer_resources(documents, "Job"))

    def test_cluster_has_one_master_and_configurable_workers(self):
        for replicas in (3, 5):
            with self.subTest(replicas=replicas):
                documents = self.documents({"mode": "cluster", "worker": {"replicas": replicas}})
                workloads = {workload["metadata"]["labels"][COMPONENT]: workload
                             for workload in self.workloads(documents)}
                self.assertEqual({"vermeer-master", "vermeer-worker"}, set(workloads))
                self.assertEqual(1, workloads["vermeer-master"]["spec"]["replicas"])
                self.assertEqual(replicas, workloads["vermeer-worker"]["spec"]["replicas"])
                for component, role in (("vermeer-master", "master"), ("vermeer-worker", "worker")):
                    self.assertEqual([role], [container["name"] for container in
                                             pod(workloads[component])["spec"]["containers"]])

    def test_native_config_uses_common_pool_and_internal_master(self):
        for mode in ("standalone", "cluster"):
            with self.subTest(mode=mode):
                documents = self.documents({"mode": mode}, fullnameOverride="analysis")
                configmap, = vermeer_resources(documents, "ConfigMap")
                data = configmap["data"]
                master, worker = configparser.ConfigParser(), configparser.ConfigParser()
                master.read_string(data["master.ini"])
                worker.read_string(data["worker.ini"])
                self.assertEqual("master", master["default"]["run_mode"])
                self.assertEqual("worker", worker["default"]["run_mode"])
                self.assertEqual("token", master["default"]["auth"])
                self.assertEqual("$", worker["default"]["worker_group"])
                self.assertIn("127.0.0.1" if mode == "standalone" else "analysis-vermeer-master",
                              worker["default"]["master_peer"])
                container = self.role_container(documents, "worker")
                self.assertTrue(any(entry.get("valueFrom", {}).get("fieldRef", {}).get("fieldPath") ==
                                    "status.podIP" for entry in container.get("env", [])))
                launch = " ".join(container.get("command", []) + container.get("args", []))
                self.assertIn("-grpc_peer", launch)

    def test_explicit_entry_and_custom_ports_follow_the_rendered_release(self):
        settings = {"mode": "cluster", "master": {"httpPort": 7688, "grpcPort": 7689},
                    "worker": {"httpPort": 7788, "grpcPort": 7789},
                    "service": {"type": "NodePort", "nodePort": 8802,
                                "annotations": {"internal.example.com/owner": "analytics"}}}
        documents = self.documents(settings, mode="strict", fullnameOverride="analytics")
        client, = [service for service in vermeer_resources(documents, "Service")
                   if service["spec"].get("clusterIP") != "None"
                   and [port["port"] for port in service["spec"]["ports"]] == [7688]]
        self.assertEqual([7688], [port["port"] for port in client["spec"]["ports"]])
        self.assertEqual(8802, client["spec"]["ports"][0]["nodePort"])
        self.assertEqual("analytics", client["metadata"]["annotations"]["internal.example.com/owner"])
        master, = resources(documents, "StatefulSet", "vermeer-master")
        self.assertTrue(policy_allows(resources(documents, "NetworkPolicy"),
                                      pod(master)["metadata"]["labels"], {}, 7688, "ingress"))
        self.assertTrue(all(workload["metadata"]["name"].startswith("analytics-vermeer-")
                            for workload in self.workloads(documents)))
        hubble, = resources(documents, "Deployment", "hubble")
        self.assertEqual("http://analytics-vermeer-master:7688",
                         env(pod(hubble)["spec"]["containers"][0])["HG_HUBBLE_VERMEER_URL"]["value"])

    def test_service_inheritance_automatic_ports_and_explicit_clusterip(self):
        for service_type, node_port in ((None, None), (None, 0), ("ClusterIP", None)):
            with self.subTest(service_type=service_type, node_port=node_port):
                documents = self.documents({"service": {"type": service_type, "nodePort": node_port}})
                for service in vermeer_resources(documents, "Service"):
                    private = service_type == "ClusterIP" or service["spec"].get("clusterIP") == "None"
                    expected = "ClusterIP" if private else "NodePort"
                    self.assertEqual(expected, service["spec"].get("type", "ClusterIP"))
                    self.assertFalse(any("nodePort" in port for port in service["spec"]["ports"]))
                master, = resources(documents, "StatefulSet", "vermeer-master")
                self.assertEqual(service_type is None, policy_allows(resources(documents, "NetworkPolicy"),
                                 pod(master)["metadata"]["labels"], {}, 6688, "ingress"))

    def test_credentials_are_secret_references_and_not_configmap_or_process_arguments(self):
        documents = self.documents({})
        secret, = [secret for secret in vermeer_resources(documents, "Secret")
                   if "token-factor" in secret["data"]]
        self.assertEqual(FACTOR, base64.b64decode(secret["data"]["token-factor"]).decode())
        self.assertEqual("keep", secret["metadata"]["annotations"]["helm.sh/resource-policy"])
        for document in documents:
            if document["kind"] != "Secret":
                serialized = yaml.safe_dump(document)
                for value in (FACTOR, NATIVE_ADMIN, "graph-test-admin-password", "pd-test-service-password"):
                    self.assertNotIn(value, serialized)
        for container in (self.role_container(documents, "master"), self.role_container(documents, "worker")):
            self.assertIn("VERMEER_PD_PASSWORD_FILE", env(container))
            self.assertIn("VERMEER_PD_PEERS", env(container))

    def test_external_credential_secret_names_and_keys_are_preserved(self):
        documents = self.documents({"auth": {"existingSecret": "native-auth", "key": "signing-key",
                                             "admin": {"existingSecret": "native-admin", "key": "login"}}},
                                   pd={"auth": {"existingSecret": "metadata-auth", "key": "service-key"}},
                                   server={"auth": {"admin": {"existingSecret": "graph-admin", "key": "admin-key"}}})
        self.assertEqual([], vermeer_resources(documents, "Secret"))
        master = next(workload for workload in self.workloads(documents)
                      if workload["metadata"]["labels"][COMPONENT] == "vermeer-master")
        secrets = {volume["secret"]["secretName"]: volume["secret"]
                   for volume in pod(master)["spec"].get("volumes", []) if "secret" in volume}
        self.assertEqual({"native-auth", "native-admin", "metadata-auth", "graph-admin"}, set(secrets))
        for name, key in (("native-auth", "signing-key"), ("metadata-auth", "service-key"),
                          ("graph-admin", "admin-key"), ("native-admin", "login")):
            self.assertIn(key, [entry["key"] for entry in secrets[name]["items"]])

    def test_generated_factor_and_pod_checksum_use_the_same_random_value(self):
        documents = self.documents({"auth": {"value": ""}})
        secret, = [secret for secret in vermeer_resources(documents, "Secret")
                   if "token-factor" in secret["data"]]
        factor = base64.b64decode(secret["data"]["token-factor"]).decode()
        self.assertGreaterEqual(len(factor), 32)
        self.assertNotIn(factor, ("pd-test-service-password", "graph-test-admin-password"))
        digest = lambda value: hashlib.sha256(value.encode()).hexdigest()
        checksum = digest(secret["metadata"]["name"] + "|token-factor|" + digest(factor))
        master, = resources(documents, "StatefulSet", "vermeer-master")
        self.assertEqual(checksum, pod(master)["metadata"]["annotations"]["checksum/vermeer-auth"])

    def test_native_admin_password_is_short_separate_and_mounted_only_by_master(self):
        cases = (("standalone", "easy", 6), ("cluster", "easy", 6),
                 ("standalone", "strict", 16), ("cluster", "strict", 16))
        for mode, entry_mode, length in cases:
            with self.subTest(mode=mode, entry_mode=entry_mode):
                documents = self.documents({"mode": mode, "auth": {"admin": {"password": ""}}}, mode=entry_mode)
                secret, = [secret for secret in vermeer_resources(documents, "Secret")
                           if secret["metadata"]["name"] == "fixture-vermeer-admin"]
                password = base64.b64decode(secret["data"]["password"]).decode()
                self.assertRegex(password, rf"^[A-Za-z0-9]{{{length}}}$")
                self.assertNotIn(password, (FACTOR, "graph-test-admin-password", "pd-test-service-password"))
                self.assertEqual("keep", secret["metadata"]["annotations"]["helm.sh/resource-policy"])
                self.assert_master_only_admin_mount(documents, secret["metadata"]["name"], "password")
                digest = lambda value: hashlib.sha256(value.encode()).hexdigest()
                expected = digest(secret["metadata"]["name"] + "|password|" + digest(password))
                master, = resources(documents, "StatefulSet", "vermeer-master")
                self.assertEqual(expected, pod(master)["metadata"]["annotations"]["checksum/vermeer-admin"])
                for document in documents:
                    if document["kind"] != "Secret":
                        self.assertNotIn(password, yaml.safe_dump(document))

    def assert_master_only_admin_mount(self, documents, secret_name, key):
        master_count = 0
        for workload in resources(documents, "StatefulSet") + resources(documents, "Deployment"):
            spec = pod(workload)["spec"]
            admin_volumes = {volume["name"]: volume["secret"] for volume in spec.get("volumes", [])
                             if volume.get("secret", {}).get("secretName") == secret_name}
            for container in spec.get("containers", []) + spec.get("initContainers", []):
                if workload["metadata"]["labels"].get(COMPONENT) == "vermeer-master" and container["name"] == "master":
                    master_count += 1
                    path = env(container)["VERMEER_ADMIN_PASSWORD_FILE"]["value"]
                    mounts = [mount for mount in container["volumeMounts"] if mount["name"] in admin_volumes]
                    self.assertEqual(1, len(mounts))
                    self.assertTrue(mounts[0]["readOnly"])
                    self.assertEqual(mounts[0]["mountPath"] + "/password", path)
                    self.assertEqual([{"key": key, "path": "password"}],
                                     admin_volumes[mounts[0]["name"]]["items"])
                else:
                    self.assertNotIn("VERMEER_ADMIN_PASSWORD_FILE", env(container))
                    self.assertFalse(any(mount["name"] in admin_volumes
                                         for mount in container.get("volumeMounts", [])))
            if workload["metadata"]["labels"].get(COMPONENT) != "vermeer-master":
                self.assertEqual({}, admin_volumes)
        self.assertEqual(1, master_count)

    def test_native_admin_external_secret_key_is_referenced_without_managing_or_leaking_it(self):
        documents = self.documents({"mode": "cluster", "auth": {"admin": {
            "existingSecret": "operator-native-admin", "key": "ui-password", "password": "unused-inline"}}})
        self.assertFalse(any(secret["metadata"]["name"] == "operator-native-admin"
                             or secret["metadata"]["name"] == "fixture-vermeer-admin"
                             for secret in resources(documents, "Secret")))
        self.assert_master_only_admin_mount(documents, "operator-native-admin", "ui-password")
        for document in documents:
            self.assertNotIn("unused-inline", yaml.safe_dump(document))

    def test_native_admin_rotation_rolls_master_without_changing_worker_or_hubble(self):
        first = self.documents({"mode": "cluster"})
        second = self.documents({"mode": "cluster", "auth": {"admin": {"password": "RotatedAdmin0123"}}})
        before, = resources(first, "StatefulSet", "vermeer-master")
        after, = resources(second, "StatefulSet", "vermeer-master")
        self.assertNotEqual(pod(before)["metadata"]["annotations"]["checksum/vermeer-admin"],
                            pod(after)["metadata"]["annotations"]["checksum/vermeer-admin"])
        self.assertEqual(pod(before)["metadata"]["annotations"]["checksum/vermeer-auth"],
                         pod(after)["metadata"]["annotations"]["checksum/vermeer-auth"])
        for component, kind in (("vermeer-worker", "StatefulSet"), ("hubble", "Deployment")):
            before, = resources(first, kind, component)
            after, = resources(second, kind, component)
            self.assertEqual(pod(before), pod(after))

    def test_native_admin_preserves_an_explicit_single_line_password(self):
        password = "  ops'\"\\$密码:  "
        documents = self.documents({"auth": {"admin": {"password": password, "key": "login-key"}}})
        secret, = [secret for secret in vermeer_resources(documents, "Secret")
                   if secret["metadata"]["name"] == "fixture-vermeer-admin"]
        self.assertEqual(password, base64.b64decode(secret["data"]["login-key"]).decode())
        self.assert_master_only_admin_mount(documents, secret["metadata"]["name"], "login-key")

    def test_schema_reader_override_replaces_the_server_admin_reference(self):
        documents = self.documents({"hugegraphCredentials": {"existingSecret": "schema-reader",
                                                              "key": "graph-credentials"}})
        worker = self.role_container(documents, "worker")
        self.assertIn("VERMEER_HUGEGRAPH_CREDENTIALS_FILE", env(worker))
        self.assertNotIn("VERMEER_HUGEGRAPH_PASSWORD_FILE", env(worker))
        workload, = self.workloads(documents)
        secret_volumes = [volume["secret"] for volume in pod(workload)["spec"]["volumes"]
                          if "secret" in volume]
        self.assertFalse(any(volume["secretName"] == "fixture-admin" for volume in secret_volumes))
        schema, = [volume for volume in secret_volumes if volume["secretName"] == "schema-reader"]
        self.assertEqual([{"key": "graph-credentials", "path": "credentials.json"}], schema["items"])

    def test_auth_rotation_changes_only_relevant_vermeer_and_hubble_checksums(self):
        first, second = self.documents({}), self.documents({"auth": {"value": FACTOR + "rotated"}})
        for component, kind in (("vermeer-master", "StatefulSet"), ("hubble", "Deployment")):
            before, = resources(first, kind, component)
            after, = resources(second, kind, component)
            self.assertNotEqual(pod(before)["metadata"]["annotations"]["checksum/vermeer-auth"],
                                pod(after)["metadata"]["annotations"]["checksum/vermeer-auth"])
        for component, kind in (("pd", "StatefulSet"), ("store", "StatefulSet"), ("server", "Deployment")):
            before, = resources(first, kind, component)
            after, = resources(second, kind, component)
            self.assertEqual(pod(before), pod(after))

    def test_hubble_uses_native_helper_and_keeps_signing_factor_out_of_app_container(self):
        documents = self.documents({})
        hubble, = resources(documents, "Deployment", "hubble")
        spec = pod(hubble)["spec"]
        app, = spec["containers"]
        token, = [container for container in spec["initContainers"] if container["name"] == "vermeer-token"]
        self.assertEqual(self.role_container(documents, "master")["image"], token["image"])
        self.assertEqual(["/go/bin/vermeer-token"], token["command"])
        self.assertIn("-quiet", token["args"])
        self.assertIn("-i", token["args"])
        self.assertEqual("$DEFAULT", token["args"][token["args"].index("-space") + 1])
        factor_volumes = {volume["name"] for volume in spec["volumes"] if
                          volume.get("secret", {}).get("secretName") == "fixture-vermeer-auth"}
        self.assertTrue(factor_volumes)
        self.assertFalse(any(mount["name"] in factor_volumes for mount in app["volumeMounts"]))
        token_path = env(app)["HG_HUBBLE_VERMEER_TOKEN_FILE"]["value"]
        self.assertTrue(any(mount.get("readOnly") and token_path.startswith(mount["mountPath"])
                            for mount in app["volumeMounts"]))

    def test_hubble_disabled_leaves_native_engine_operational(self):
        documents = self.documents({}, hubble={"enabled": False})
        self.assertEqual([], resources(documents, component="hubble"))
        self.assertEqual(1, len(self.workloads(documents)))
        self.assertEqual({"master", "worker"}, {container["name"] for workload in self.workloads(documents)
                                                 for container in pod(workload)["spec"]["containers"]})

    def test_disabled_integration_clears_image_defaults_and_does_not_mount_token(self):
        documents = self.documents()
        hubble, = resources(documents, "Deployment", "hubble")
        spec = pod(hubble)["spec"]
        app, = spec["containers"]
        self.assertEqual("", env(app)["HG_HUBBLE_VERMEER_URL"]["value"])
        self.assertEqual("", env(app)["HG_HUBBLE_VERMEER_TOKEN_FILE"]["value"])
        self.assertFalse(any(container["name"] == "vermeer-token" for container in spec.get("initContainers", [])))
        script = "\n".join(app.get("args", []))
        self.assertIn("vermeer.url", script)
        self.assertIn("vermeer.token_file", script)
        self.assertIn("operations.vermeer.enabled=false", script)
        self.assertIn("operations.vermeer.address=", script)
        self.assertIn("operations.vermeer.token_file=", script)
        self.assertNotIn("fixture-vermeer-master", script)

    def test_hubble_wrapper_replaces_stale_and_duplicate_vermeer_properties(self):
        cases = ((None, {}), ({}, {}),
                 ({"mode": "cluster", "master": {"httpPort": 7688}},
                  {"fullnameOverride": "analysis"}))
        for vermeer, overrides in cases:
            enabled = vermeer is not None
            with self.subTest(vermeer=vermeer, overrides=overrides):
                documents = self.documents(vermeer, **overrides)
                hubble, = resources(documents, "Deployment", "hubble")
                container, = pod(hubble)["spec"]["containers"]
                environment = dict(os.environ)
                for entry in container.get("env", []):
                    environment[entry["name"]] = str(entry.get("value", "fixture-secret"))
                if not enabled:
                    environment["HG_HUBBLE_VERMEER_URL"] = "http://stale-runtime-default"
                    environment["HG_HUBBLE_VERMEER_TOKEN_FILE"] = "/stale-runtime-token"
                with tempfile.TemporaryDirectory(prefix="vermeer-hubble-wrapper-") as directory:
                    root = Path(directory)
                    (root / "conf").mkdir()
                    (root / "bin").mkdir()
                    properties = root / "conf/hugegraph-hubble.properties"
                    properties.write_text("unrelated.setting=preserved\n"
                                          "vermeer.url=http://stale-image-default\n"
                                          "vermeer.token_file=/stale-token\n"
                                          "vermeer.url=http://duplicate-default\n"
                                          "operations.vermeer.enabled=true\n"
                                          "operations.vermeer.address=http://stale-operations-default\n"
                                          "operations.vermeer.token_file=/stale-operations-token\n"
                                          "operations.vermeer.enabled=false\n"
                                          "operations.vermeer.address=http://duplicate-default\n"
                                          "operations.vermeer.token_file=/duplicate-token\n")
                    start = root / "bin/start-hubble.sh"
                    start.write_text("#!/bin/sh\nexit 0\n")
                    start.chmod(0o755)
                    result = subprocess.run(container["command"] + container["args"],
                                            cwd=root, env=environment, capture_output=True,
                                            text=True, timeout=15)
                    self.assertEqual(0, result.returncode, result.stderr)
                    final = properties.read_text().splitlines()
                self.assertIn("unrelated.setting=preserved", final)
                expected_url = environment["HG_HUBBLE_VERMEER_URL"] if enabled else ""
                expected_token = environment["HG_HUBBLE_VERMEER_TOKEN_FILE"] if enabled else ""
                self.assertEqual(["vermeer.url=" + expected_url],
                                 [line for line in final if line.startswith("vermeer.url=")])
                self.assertEqual(["vermeer.token_file=" + expected_token],
                                 [line for line in final if line.startswith("vermeer.token_file=")])
                self.assertEqual(["operations.vermeer.enabled=" + str(enabled).lower()],
                                 [line for line in final if line.startswith("operations.vermeer.enabled=")])
                self.assertEqual(["operations.vermeer.address=" + expected_url],
                                 [line for line in final if line.startswith("operations.vermeer.address=")])
                self.assertEqual(["operations.vermeer.token_file=" + expected_token],
                                 [line for line in final if line.startswith("operations.vermeer.token_file=")])

    def test_persistence_inheritance_and_explicit_temporary_storage(self):
        for mode in ("standalone", "cluster"):
            for persistent in (False, True):
                with self.subTest(mode=mode, persistent=persistent):
                    documents = self.documents({"mode": mode}, persistence={"enabled": persistent})
                    claims = [claim for workload in self.workloads(documents)
                              for claim in workload["spec"].get("volumeClaimTemplates", [])]
                    claims += vermeer_resources(documents, "PersistentVolumeClaim")
                    self.assertEqual(2 if persistent else 0, len(claims))
                    for workload in self.workloads(documents):
                        for container in pod(workload)["spec"]["containers"]:
                            self.assertTrue(any(mount["mountPath"] == "/go/bin/vermeer_data"
                                                for mount in container["volumeMounts"]))
        documents = self.documents({"master": {"storage": {"enabled": False}},
                                    "worker": {"storage": {"enabled": True, "size": "7Gi"}}},
                                   persistence={"enabled": False})
        workload, = self.workloads(documents)
        self.assertEqual([], workload["spec"].get("volumeClaimTemplates", []))
        claim, = vermeer_resources(documents, "PersistentVolumeClaim")
        self.assertEqual("7Gi", claim["spec"]["resources"]["requests"]["storage"])

    def test_default_and_custom_tag_cover_both_processes_and_token_helper(self):
        for mode in ("standalone", "cluster"):
            for image, expected in (({}, "hugegraph/vermeer:latest"),
                                    ({"tag": "dev-build"}, "hugegraph/vermeer:dev-build")):
                with self.subTest(mode=mode, image=image):
                    documents = self.documents({"mode": mode, "image": image})
                    containers = [container for workload in self.workloads(documents)
                                  for container in pod(workload)["spec"]["containers"]]
                    hubble, = resources(documents, "Deployment", "hubble")
                    containers += [container for container in pod(hubble)["spec"]["initContainers"]
                                   if container["name"] == "vermeer-token"]
                    self.assertEqual(3, len(containers))
                    for container in containers:
                        self.assertEqual(expected, container["image"])
                        self.assertEqual("Always", container["imagePullPolicy"])

    def test_registry_digest_and_pull_secrets_cover_both_processes_and_token_helper(self):
        digest = "sha256:" + "a" * 64
        documents = self.documents({"image": {"repository": "team/vermeer", "tag": "ignored", "digest": digest,
                                               "pullPolicy": "Never"}},
                                   **{"global": {"imageRegistry": "registry.internal.example.com/project"},
                                      "imagePullSecrets": [{"name": "private-registry"}]})
        expected = "registry.internal.example.com/project/team/vermeer@" + digest
        for workload in self.workloads(documents):
            self.assertEqual([{"name": "private-registry"}], pod(workload)["spec"]["imagePullSecrets"])
            for container in pod(workload)["spec"]["containers"]:
                self.assertEqual(expected, container["image"])
                self.assertEqual("Never", container["imagePullPolicy"])
        hubble, = resources(documents, "Deployment", "hubble")
        helper = next(container for container in pod(hubble)["spec"]["initContainers"]
                      if container["name"] == "vermeer-token")
        self.assertEqual(expected, helper["image"])
        self.assertEqual("Never", helper["imagePullPolicy"])

    def test_strict_network_covers_reverse_rpc_and_peer_rpc(self):
        documents = self.documents({"mode": "cluster"}, mode="strict")
        policies = resources(documents, "NetworkPolicy")
        labels = {component: pod(workload)["metadata"]["labels"] for component in
                  ("hubble", "vermeer-master", "vermeer-worker")
                  for workload in resources(documents, "StatefulSet") + resources(documents, "Deployment")
                  if workload["metadata"]["labels"].get(COMPONENT) == component}
        paths = [("hubble", "vermeer-master", 6688),
                 ("vermeer-worker", "vermeer-master", 6689),
                 ("vermeer-master", "vermeer-worker", 6789),
                 ("vermeer-worker", "vermeer-worker", 6789)]
        for source, destination, port in paths:
            with self.subTest(source=source, destination=destination, port=port):
                self.assertTrue(policy_allows(policies, labels[destination], labels[source], port, "ingress"))
                self.assertTrue(policy_allows(policies, labels[source], labels[destination], port, "egress"))

    def test_operations_needs_only_master_http_not_worker_or_rpc_access(self):
        for mode in ("standalone", "cluster"):
            with self.subTest(mode=mode):
                documents = self.documents({"mode": mode, "master": {"httpPort": 7688,
                                                                 "grpcPort": 7689},
                                            "worker": {"httpPort": 7788, "grpcPort": 7789}},
                                           mode="strict")
                policies = resources(documents, "NetworkPolicy")
                workloads = resources(documents, "StatefulSet") + resources(documents, "Deployment")
                labels = {workload["metadata"]["labels"][COMPONENT]: pod(workload)["metadata"]["labels"]
                          for workload in workloads}
                hubble, master = labels["hubble"], labels["vermeer-master"]
                self.assertTrue(policy_allows(policies, hubble, master, 7688, "egress"))
                self.assertTrue(policy_allows(policies, master, hubble, 7688, "ingress"))
                worker = labels["vermeer-worker"] if mode == "cluster" else master
                for target, port in ((master, 7689), (worker, 7788), (worker, 7789)):
                    self.assertFalse(policy_allows(policies, hubble, target, port, "egress"))
                    self.assertFalse(policy_allows(policies, target, hubble, port, "ingress"))

    def test_backend_connections_need_no_extra_rules_for_either_worker_pod_layout(self):
        for topology in ("standalone", "cluster"):
            for entry_mode, policy_enabled in (("easy", None), ("strict", None),
                                               ("easy", True), ("strict", False)):
                for ports in ((8686, 8500, 8080), (18686, 18500, 18080)):
                    with self.subTest(topology=topology, entry_mode=entry_mode,
                                      policy_enabled=policy_enabled, ports=ports):
                        overrides = {"mode": entry_mode, "pd": {"ports": {"grpc": ports[0]}},
                                     "store": {"ports": {"grpc": ports[1]}}, "server": {"port": ports[2]}}
                        if policy_enabled is not None:
                            overrides["networkPolicy"] = {"enabled": policy_enabled}
                        # No component extraIngress/extraEgress is supplied.
                        documents = self.documents({"mode": topology}, **overrides)
                        policies = resources(documents, "NetworkPolicy")
                        workloads = resources(documents, "StatefulSet") + resources(documents, "Deployment")
                        labels = {workload["metadata"]["labels"][COMPONENT]: pod(workload)["metadata"]["labels"]
                                  for workload in workloads}
                        roles = ("vermeer-master",) if topology == "standalone" else (
                            "vermeer-master", "vermeer-worker")
                        isolated = entry_mode == "strict" if policy_enabled is None else policy_enabled
                        for role in roles:
                            for destination, port in zip(("pd", "store", "server"), ports):
                                with self.subTest(role=role, destination=destination, port=port):
                                    # Standalone Worker shares vermeer-master labels;
                                    # both ends of a TCP flow must admit that source.
                                    self.assertTrue(policy_allows(
                                        policies, labels[role], labels[destination], port, "egress"))
                                    self.assertTrue(policy_allows(
                                        policies, labels[destination], labels[role], port, "ingress"))
                                    if isolated:
                                        outsider = {**labels[role], INSTANCE: "other-release"}
                                        other_backend = {**labels[destination], INSTANCE: "other-release"}
                                        self.assertFalse(policy_allows(
                                            policies, labels[destination], outsider, port, "ingress"))
                                        self.assertFalse(policy_allows(
                                            policies, labels[role], other_backend, port, "egress"))

    def test_http_follows_service_while_rpc_stays_release_scoped_without_strict_egress(self):
        for entry_mode in ("easy", "strict"):
            for mode in ("standalone", "cluster"):
                with self.subTest(entry_mode=entry_mode, mode=mode):
                    self.assert_native_ingress(mode, entry_mode)

    def assert_native_ingress(self, mode, entry_mode):
        documents = self.documents({"mode": mode}, mode=entry_mode,
                                   seatunnel={"enabled": True, "mode": mode},
                                   networkPolicy={"enabled": False})
        policies = resources(documents, "NetworkPolicy")
        for component in ("vermeer-master", "vermeer-worker", "seatunnel"):
            for service in resources(documents, "Service", component):
                spec = service["spec"]
                self.assertNotIn("externalIPs", spec)
                entry = spec.get("clusterIP") != "None"
                self.assertEqual("NodePort" if entry and entry_mode == "easy" else "ClusterIP",
                                 spec.get("type", "ClusterIP"))
                self.assertFalse(any("nodePort" in port for port in spec["ports"]))
                if entry:
                    self.assertEqual([8080 if component == "seatunnel" else 6688],
                                     [port["port"] for port in spec["ports"]])
                    self.assertEqual("http" if component == "seatunnel" else "master-http",
                                     spec["ports"][0]["targetPort"])
                else:
                    self.assertTrue(all(port["port"] in (6689, 6789, 5801) for port in spec["ports"]))
        engine, = resources(documents, "StatefulSet", "seatunnel")
        engine_labels = pod(engine)["metadata"]["labels"]
        self.assertEqual(entry_mode == "easy", policy_allows(policies, engine_labels, {}, 8080, "ingress"))
        self.assertFalse(policy_allows(policies, engine_labels, {}, 5801, "ingress"))
        self.assertTrue(vermeer_resources(documents, "NetworkPolicy"))
        for policy in vermeer_resources(documents, "NetworkPolicy"):
            self.assertEqual(["Ingress"], policy["spec"]["policyTypes"])
            opened = [port["port"] for rule in policy["spec"]["ingress"] if not rule.get("from")
                      for port in rule["ports"]]
            expected = [6688] if entry_mode == "easy" and policy["metadata"]["labels"][COMPONENT] == "vermeer-master" else []
            self.assertEqual(expected, opened)
            self.assertFalse(any("ipBlock" in peer for rule in policy["spec"]["ingress"]
                                 for peer in rule.get("from", [])))
        for workload in self.workloads(documents):
            labels = pod(workload)["metadata"]["labels"]
            is_master = labels[COMPONENT] == "vermeer-master"
            http_port, rpc_port = (6688, 6689) if is_master else (6788, 6789)
            for component in ("vermeer-master", "vermeer-worker", "hubble", "test"):
                peer = {**labels, COMPONENT: component}
                self.assertEqual(is_master or component.startswith("vermeer-"),
                                 policy_allows(policies, labels, peer, http_port, "ingress"))
                self.assertEqual(component.startswith("vermeer-"),
                                 policy_allows(policies, labels, peer, rpc_port, "ingress"))
            for peer in ({**labels, INSTANCE: "other-release"},
                         {**labels, COMPONENT: "server"}, {**labels, COMPONENT: "unknown"}):
                self.assertEqual(entry_mode == "easy" and is_master,
                                 policy_allows(policies, labels, peer, http_port, "ingress"))
                for port in (6689, 6789, 6788):
                    self.assertFalse(policy_allows(policies, labels, peer, port, "ingress"))

    def test_native_network_extensions_are_preserved(self):
        ingress = {"from": [{"namespaceSelector": {"matchLabels": {"team": "analytics"}},
                             "podSelector": {"matchLabels": {"app": "analysis-client"}}}],
                   "ports": [{"port": 6688}]}
        egress = {"to": [{"ipBlock": {"cidr": "10.16.0.0/16"}}], "ports": [{"port": 9443}]}
        documents = self.documents({"mode": "cluster"}, mode="strict", networkPolicy={
            "vermeer": {"extraIngress": [ingress], "extraEgress": [egress]}})
        for policy in vermeer_resources(documents, "NetworkPolicy"):
            self.assertIn(ingress, policy["spec"]["ingress"])
            self.assertIn(egress, policy["spec"]["egress"])

    def test_native_ingress_extensions_require_explicit_internal_pod_selectors(self):
        for rule in ({}, {"ports": [{"port": 6688}]}, {"from": []}, {"from": [{}]},
                     {"from": [{"ipBlock": {"cidr": "0.0.0.0/0"}}]},
                     {"from": [{"ipBlock": {"cidr": "10.0.0.0/8"}}]}):
            with self.subTest(rule=rule):
                result = self.render({}, networkPolicy={"vermeer": {"extraIngress": [rule]}})
                self.assertNotEqual(0, result.returncode)

    def test_process_probes_do_not_depend_on_space_worker_availability(self):
        for container in (self.role_container(self.documents({}), "master"),
                          self.role_container(self.documents({}), "worker")):
            for probe in ("startupProbe", "livenessProbe", "readinessProbe"):
                self.assertIn(probe, container)
                self.assertNotIn("workers/availability", yaml.safe_dump(container[probe]))

    def test_relative_algorithm_outputs_are_inside_each_roles_writable_data_volume(self):
        for mode in ("standalone", "cluster"):
            with self.subTest(mode=mode):
                documents = self.documents({"mode": mode})
                for workload in self.workloads(documents):
                    for container in pod(workload)["spec"]["containers"]:
                        mount, = [entry for entry in container["volumeMounts"]
                                  if entry["mountPath"] == "/go/bin/vermeer_data"]
                        self.assertFalse(mount.get("readOnly", False))
                        directory = Path(container["workingDir"])
                        self.assertTrue(directory == Path(mount["mountPath"]) or
                                        Path(mount["mountPath"]) in directory.parents,
                                        "Relative Hubble algorithm output must use the role data volume")

    def test_seatunnel_can_be_enabled_independently_without_changing_engine_identity(self):
        documents = self.documents({}, seatunnel={"enabled": True, "auth": {"password": "seatunnel-test-password"}})
        self.assertEqual(1, len(resources(documents, "StatefulSet", "seatunnel")))
        self.assertEqual(1, len(self.workloads(documents)))
        self.assertTrue(all(secret["metadata"]["name"] != "fixture-vermeer-auth"
                            for secret in resources(documents, "Secret", "seatunnel")))

    def test_signing_factor_matches_the_native_file_length_boundary(self):
        for length in (4, 15):
            with self.subTest(length=length):
                result = self.render({"auth": {"value": "x" * length}})
                self.assertNotEqual(0, result.returncode)
                self.assertRegex(result.stderr, r"vermeer[./]auth[./]value")
        documents = self.documents({"auth": {"value": "x" * 16,
                                              "admin": {"password": "Ab12xY"}}})
        secret, = [item for item in resources(documents, "Secret") if "token-factor" in item["data"]]
        self.assertEqual("x" * 16, base64.b64decode(secret["data"]["token-factor"]).decode())

    def test_invalid_enabled_values_fail_before_install(self):
        cases = ({"mode": "unknown"}, {"mode": "cluster", "worker": {"replicas": 0}},
                 {"master": {"httpPort": 0}}, {"worker": {"grpcPort": 65536}},
                 {"service": {"type": "ExternalName"}}, {"service": {"type": "LoadBalancer"}},
                 {"service": {"nodePort": -1}}, {"service": {"nodePort": 65536}},
                 {"service": {"type": "ClusterIP", "nodePort": 31688}},
                 {"service": {"externalIPs": ["192.0.2.10"]}},
                 {"auth": {"value": "", "autoGenerate": False}},
                 {"auth": {"admin": {"password": "", "autoGenerate": False}}},
                 {"auth": {"admin": {"password": "  "}}},
                 {"auth": {"admin": {"password": "line1\nline2"}}},
                 {"master": {"extraEnv": [{"name": "VERMEER_ADMIN_PASSWORD_FILE", "value": "/wrong"}]}},
                 {"master": {"extraEnv": [{"name": "VERMEER_AUTH_TOKEN_FACTOR_FILE", "value": "/wrong"}]}},
                 {"worker": {"extraEnv": [{"name": "POD_IP", "value": "0.0.0.0"}]}})
        for settings in cases:
            with self.subTest(settings=settings):
                self.assertNotEqual(0, self.render(settings).returncode)
        result = self.render({}, hubble={"mode": "direct"})
        self.assertNotEqual(0, result.returncode)
        self.assertIn("pd", result.stderr.lower())
        for location in ("securityContext", "podSecurityContext"):
            with self.subTest(non_root_context=location):
                result = self.render({}, hubble={location: {"runAsNonRoot": True}})
                self.assertNotEqual(0, result.returncode)
                self.assertIn("runAsUser", result.stderr)
                self.documents({}, hubble={location: {"runAsNonRoot": True, "runAsUser": 10001}})
        # --set yields integer types distinct from values.yaml's numbers;
        # collision validation must compare their numeric values.
        result = subprocess.run(["helm", "template", "fixture", str(CHART),
                                 "--set", "vermeer.enabled=true", "--set",
                                 "vermeer.master.httpPort=6788"],
                                capture_output=True, text=True, timeout=120)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("distinct", result.stderr)


if __name__ == "__main__":
    unittest.main()
