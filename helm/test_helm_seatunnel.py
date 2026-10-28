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

"""Check native hybrid deployment, release isolation, connectivity and packaging."""

import base64
import importlib.util
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest
from unittest import mock

import yaml


CHART = Path(__file__).resolve().parent / "hugegraph"
HELM_TEMPLATE_TIMEOUT_SECONDS = 120
HELM_PACKAGE_TIMEOUT_SECONDS = 300


def matches(selector, labels):
    return all(labels.get(key) == value for key, value in selector.items())


def ingress_allows(policy, peer, port):
    for rule in policy["ingress"]:
        if rule.get("ports") and not any(entry["port"] == port and
                                        entry.get("protocol", "TCP") == "TCP"
                                        for entry in rule["ports"]):
            continue
        if not rule.get("from"):
            return True
        for candidate in rule["from"]:
            if "ipBlock" in candidate:
                continue
            selector = candidate.get("podSelector", {})
            if not matches(selector.get("matchLabels", {}), peer):
                continue
            expressions_match = all(
                expression["operator"] == "In" and peer.get(expression["key"]) in expression["values"]
                for expression in selector.get("matchExpressions", []))
            if expressions_match:
                return True
    return False


def engine_resources(documents, kind=None):
    return [doc for doc in documents if (kind is None or doc["kind"] == kind)
            and doc["metadata"].get("labels", {}).get("app.kubernetes.io/component") == "seatunnel"]


def pod(workload):
    return workload["spec"]["template"]


def engine_auth_secret(documents):
    return next(doc for doc in engine_resources(documents, "Secret")
                if "seatunnel.yaml" in doc.get("data", {}))


def engine_config(documents):
    data = {}
    for doc in engine_resources(documents, "ConfigMap"):
        data.update(doc["data"])
    data["seatunnel.yaml"] = base64.b64decode(engine_auth_secret(documents)["data"]["seatunnel.yaml"]).decode()
    return data


class SeaTunnelChartTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if shutil.which("helm") is None:
            raise RuntimeError("Helm is required for the SeaTunnel integration regression")

    def run_helm(self, arguments, timeout, check=False):
        command = ["helm", *arguments]
        try:
            return subprocess.run(command, check=check, capture_output=True,
                                  text=True, timeout=timeout)
        except subprocess.TimeoutExpired as error:
            self.fail(f"{' '.join(command)} timed out after {timeout} seconds: {error}")

    def render(self, seatunnel=None, chart=CHART, namespace="default", release="fixture", **values):
        if seatunnel is not None:
            values["seatunnel"] = {"enabled": True, **seatunnel}
            # Keep independent render comparisons deterministic; generation is tested separately.
            values["seatunnel"]["auth"] = {"password": "native-test-admin-password",
                                           **seatunnel.get("auth", {})}
        with tempfile.TemporaryDirectory(prefix="seatunnel-values-") as directory:
            configuration = Path(directory) / "values.yaml"
            configuration.write_text(yaml.safe_dump(values))
            result = self.run_helm(["template", release, str(chart),
                                    "--namespace", namespace, "-f", str(configuration)],
                                   HELM_TEMPLATE_TIMEOUT_SECONDS)
        return result

    def documents(self, seatunnel=None, chart=CHART, namespace="default", release="fixture", **values):
        result = self.render(seatunnel, chart=chart, namespace=namespace, release=release, **values)
        self.assertEqual(0, result.returncode, result.stderr)
        return [doc for doc in yaml.safe_load_all(result.stdout) if doc]

    def test_disabled_has_no_engine_resources(self):
        documents = self.documents()
        self.assertEqual([], engine_resources(documents))
        self.assertFalse(any("seatunnel" in doc["metadata"]["name"] for doc in documents))

    def test_schema_copy_rejects_unsupported_edges_before_contacting_target(self):
        path = CHART / "examples/seatunnel/clone/copy-schema.py"
        spec = importlib.util.spec_from_file_location("copy_schema", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for label_type in ("PARENT", "SUB", "UNKNOWN"):
            with self.subTest(label_type=label_type):
                source = mock.Mock(return_value={"edgelabels": [
                    {"name": "relation", "edgelabel_type": label_type}]})
                target = mock.Mock()
                with mock.patch.object(module, "client", side_effect=[source, target]) as factory, \
                        mock.patch("sys.argv", [str(path), "--source", "http://source/graph",
                                                "--target", "http://target/graph"]):
                    with self.assertRaisesRegex(SystemExit, "only supports NORMAL edge labels"):
                        module.main()
                source.assert_called_once_with("GET", "/schema")
                factory.assert_called_once_with("http://source/graph", "SOURCE")
                target.assert_not_called()

    def test_legacy_release_without_seatunnel_values_still_renders(self):
        with tempfile.TemporaryDirectory(prefix="seatunnel-legacy-") as directory:
            chart = Path(directory) / "chart"
            shutil.copytree(CHART, chart)
            (chart / "values.yaml").write_bytes((CHART / "testdata/values-pre-hardening.yaml").read_bytes())
            for arguments in ([], ["--is-upgrade"], ["--set", "global.imageRegistry=registry.example.com"]):
                with self.subTest(arguments=arguments):
                    result = self.run_helm(["template", "legacy", str(chart), *arguments],
                                           HELM_TEMPLATE_TIMEOUT_SECONDS)
                    self.assertEqual(0, result.returncode, result.stderr)
                    documents = [doc for doc in yaml.safe_load_all(result.stdout) if doc]
                    self.assertEqual([], engine_resources(documents))
            # An earlier enabled release also predates the entry/auth fields.
            previous = yaml.safe_load((CHART / "values.yaml").read_text())
            previous["seatunnel"]["enabled"] = True
            for field in ("service", "auth"):
                previous["seatunnel"].pop(field)
            (chart / "values.yaml").write_text(yaml.safe_dump(previous))
            documents = self.documents(chart=chart)
            http = yaml.safe_load(engine_config(documents)["seatunnel.yaml"])["seatunnel"]["engine"]["http"]
            self.assertTrue(http["enable-basic-auth"])
            self.assertEqual("admin", http["basic-auth-username"])

    def test_modes_share_one_native_hybrid_workload_and_services(self):
        modes = [self.documents({}), self.documents({"mode": "cluster", "replicas": 3})]
        workloads = []
        for documents, replicas, backups in zip(modes, (1, 3), (0, 1)):
            workload, = engine_resources(documents, "StatefulSet")
            workloads.append(workload)
            self.assertEqual([], engine_resources(documents, "Deployment"))
            self.assertEqual(replicas, workload["spec"]["replicas"])
            container, = pod(workload)["spec"]["containers"]
            launch = " ".join(container.get("command", []) + container.get("args", []))
            self.assertIn("/opt/seatunnel/bin/seatunnel-cluster.sh", launch)
            self.assertNotIn(" -r ", launch)
            data = engine_config(documents)
            for option in ("-Xms512m", "-Xmx1g", "-XX:+UseG1GC", "-XX:MaxMetaspaceSize=256m",
                           "-XX:+HeapDumpOnOutOfMemoryError", "-XX:HeapDumpPath=/tmp/seatunnel.hprof"):
                self.assertIn(option, data["jvm_options"].splitlines())
            engine = yaml.safe_load(data["seatunnel.yaml"])["seatunnel"]["engine"]
            self.assertEqual(backups, engine["backup-count"])
            self.assertTrue(engine["http"]["enable-http"])
            self.assertEqual(8080, engine["http"]["port"])
        self.assertEqual(workloads[0]["metadata"]["name"], workloads[1]["metadata"]["name"])
        self.assertEqual(workloads[0]["spec"]["selector"], workloads[1]["spec"]["selector"])
        services = lambda docs: {doc["metadata"]["name"]: doc["spec"]["selector"]
                                 for doc in engine_resources(docs, "Service")}
        self.assertEqual(services(modes[0]), services(modes[1]))

    def test_dns_discovery_and_services_are_scoped_to_engine_and_release(self):
        documents = self.documents({"mode": "cluster"}, namespace="graphs", fullnameOverride="transfer")
        workload, = engine_resources(documents, "StatefulSet")
        labels = pod(workload)["metadata"]["labels"]
        services = engine_resources(documents, "Service")
        headless, = [service for service in services if service["spec"].get("clusterIP") == "None"]
        api, = [service for service in services if service is not headless]
        self.assertEqual("transfer-seatunnel", workload["metadata"]["name"])
        self.assertEqual(headless["metadata"]["name"], workload["spec"]["serviceName"])
        self.assertTrue(headless["spec"]["publishNotReadyAddresses"])
        self.assertIn(8080, [port["port"] for port in api["spec"]["ports"]])
        for service in services:
            selector = service["spec"]["selector"]
            self.assertTrue(matches(selector, labels))
            self.assertFalse(matches(selector, {**labels, "app.kubernetes.io/instance": "other"}))
            for other in documents:
                if other["kind"] in ("Deployment", "StatefulSet") and other is not workload:
                    self.assertFalse(matches(selector, pod(other)["metadata"]["labels"]))
        data = engine_config(documents)
        discovery = yaml.safe_load(data["hazelcast.yaml"])["hazelcast"]["network"]["join"]["kubernetes"]
        self.assertTrue(discovery["enabled"])
        self.assertTrue(discovery["service-dns"].startswith(headless["metadata"]["name"] + ".graphs.svc"))
        self.assertNotIn("service-name", discovery)
        # DNS discovery otherwise dials Hazelcast's default5701 and creates
        # isolated one-member clusters even while both Pods report Ready.
        member_port = next(port["containerPort"] for port in
                           pod(workload)["spec"]["containers"][0]["ports"]
                           if port["name"] == "hazelcast")
        self.assertEqual(member_port, discovery["service-port"])
        client = yaml.safe_load(data["hazelcast-client.yaml"])["hazelcast-client"]
        self.assertIn(discovery["service-dns"] + ":5801", client["network"]["cluster-members"])
        self.assertFalse(any(doc["kind"] in ("Role", "RoleBinding") for doc in engine_resources(documents)))

    def test_http_entry_remains_internal_and_preserves_annotations(self):
        for mode in ("easy", "strict"):
            with self.subTest(mode=mode):
                documents = self.documents({}, mode=mode)
                for service in engine_resources(documents, "Service"):
                    self.assertEqual("ClusterIP", service["spec"].get("type", "ClusterIP"))
                    self.assertNotIn("externalIPs", service["spec"])
                    self.assertFalse(any("nodePort" in port for port in service["spec"]["ports"]))
        for mode in ("easy", "strict"):
            service = {"type": "ClusterIP", "annotations": {"internal.example.com/owner": "imports"}}
            with self.subTest(mode=mode):
                documents = self.documents({"service": service}, mode=mode)
                headless, = [doc for doc in engine_resources(documents, "Service")
                             if doc["spec"].get("clusterIP") == "None"]
                api, = [doc for doc in engine_resources(documents, "Service") if doc is not headless]
                self.assertEqual("ClusterIP", api["spec"]["type"])
                self.assertEqual("imports", api["metadata"]["annotations"]["internal.example.com/owner"])
                self.assertNotIn("nodePort", api["spec"]["ports"][0])
                self.assertNotIn("nodePort", headless["spec"]["ports"][0])
        for service in ({"type": "NodePort"}, {"type": "LoadBalancer"}, {"nodePort": 0},
                        {"type": "ClusterIP", "nodePort": 31081}, {"externalIPs": ["192.0.2.10"]}):
            with self.subTest(service=service):
                self.assertNotEqual(0, self.render({"service": service}).returncode)

    def test_legacy_null_service_fields_keep_the_native_entry_internal(self):
        documents = self.documents({"service": {"type": None, "nodePort": None}})
        for service in engine_resources(documents, "Service"):
            self.assertEqual("ClusterIP", service["spec"].get("type", "ClusterIP"))
            self.assertFalse(any("nodePort" in port for port in service["spec"]["ports"]))

    def test_local_graph_defaults_follow_rendered_server_and_admin_secret(self):
        documents = self.documents({}, namespace="graphs", release="fixture-" + "r" * 40,
                                   fullnameOverride="graph-copy-" + "n" * 55,
                                   server={"port": 8182, "graphSpace": "tenant_a"})
        server, = [doc for doc in documents if doc["kind"] == "Service"
                   and doc["metadata"]["labels"].get("app.kubernetes.io/component") == "server"
                   and doc["spec"].get("clusterIP") != "None"]
        server_workload, = [doc for doc in documents if doc["kind"] == "Deployment"
                            and doc["metadata"]["labels"].get("app.kubernetes.io/component") == "server"]
        server_container = next(entry for entry in pod(server_workload)["spec"]["containers"]
                                if entry["name"] == "server")
        server_password = next(entry["valueFrom"] for entry in server_container["env"]
                               if entry["name"] == "PASSWORD")
        workload, = engine_resources(documents, "StatefulSet")
        container, = pod(workload)["spec"]["containers"]
        env = {entry["name"]: entry for entry in container["env"]}
        self.assertEqual(server["metadata"]["name"] + ".graphs.svc", env["HG_HOST"]["value"])
        self.assertEqual(str(server["spec"]["ports"][0]["port"]), env["HG_PORT"]["value"])
        self.assertEqual("http", env["HG_PROTOCOL"]["value"])
        self.assertEqual("tenant_a", env["HG_GRAPH_SPACE"]["value"])
        self.assertEqual("graph_a", env["HG_GRAPH_NAME"]["value"])
        self.assertEqual("admin", env["HG_USERNAME"]["value"])
        self.assertEqual(server_password, env["HG_PASSWORD"]["valueFrom"])

    def test_existing_admin_secret_and_disabled_auth_reach_local_graph_environment(self):
        cases = (({"enabled": True, "admin": {"existingSecret": "graph-admin", "key": "admin-pass"}},
                  {"name": "HG_PASSWORD", "valueFrom": {
                      "secretKeyRef": {"name": "graph-admin", "key": "admin-pass"}}}),
                 ({"enabled": False}, {"name": "HG_PASSWORD", "value": ""}))
        for auth, password in cases:
            with self.subTest(auth=auth):
                documents = self.documents({}, server={"auth": auth}, hubble={"enabled": False})
                workload, = engine_resources(documents, "StatefulSet")
                container, = pod(workload)["spec"]["containers"]
                self.assertIn(password, container["env"])

    def test_explicit_environment_overrides_defaults_without_duplicate_names(self):
        overrides = [{"name": "HG_HOST", "value": "external-graph.example.com"},
                     {"name": "HG_PORT", "value": "8443"},
                     {"name": "HG_PROTOCOL", "value": "https"},
                     {"name": "HG_GRAPH_SPACE", "value": "other"},
                     {"name": "HG_GRAPH_NAME", "value": "existing_graph"},
                     {"name": "HG_USERNAME", "value": "writer"},
                     {"name": "HG_PASSWORD", "valueFrom": {
                         "secretKeyRef": {"name": "writer-auth", "key": "password"}}},
                     {"name": "TZ", "value": "UTC"}]
        documents = self.documents({"env": overrides})
        workload, = engine_resources(documents, "StatefulSet")
        container, = pod(workload)["spec"]["containers"]
        env = {entry["name"]: entry for entry in container["env"]}
        self.assertEqual(len(env), len(container["env"]))
        for expected in overrides:
            self.assertEqual(expected, env[expected["name"]])

    def test_demo_jobs_are_mounted_and_keep_credentials_as_placeholders(self):
        password = "render-test-password"
        documents = self.documents({}, server={"auth": {"admin": {"password": password}}})
        configmaps = engine_resources(documents, "ConfigMap")
        jobs = {doc["metadata"]["name"]: doc["data"] for doc in configmaps
                if any(filename.endswith(".conf") for filename in doc["data"])}
        self.assertEqual(2, sum(filename.endswith(".conf") for data in jobs.values() for filename in data))
        workload, = engine_resources(documents, "StatefulSet")
        spec = pod(workload)["spec"]
        container, = spec["containers"]
        for configmap_name, data in jobs.items():
            volume_names = {volume["name"] for volume in spec["volumes"]
                            if volume.get("configMap", {}).get("name") == configmap_name}
            self.assertTrue(volume_names)
            mounts = [mount for mount in container["volumeMounts"] if mount["name"] in volume_names]
            self.assertTrue(mounts)
            for filename, content in data.items():
                if filename.endswith(".conf"):
                    self.assertTrue(any(mount["mountPath"] == "/opt/seatunnel/config/" + filename
                                        for mount in mounts))
                    for name in ("HG_HOST", "HG_PORT", "HG_PROTOCOL", "HG_GRAPH_SPACE", "HG_GRAPH_NAME",
                                 "HG_USERNAME", "HG_PASSWORD"):
                        self.assertIn("${" + name + "}", content)
        for data in (doc["data"] for doc in configmaps):
            self.assertFalse(any(password in content for content in data.values()))

    def test_seatunnel_admin_is_generated_separately_and_kept_out_of_public_configuration(self):
        graph_password = "graph-only-admin-password"
        for mode, length in (("easy", 6), ("strict", 16)):
            with self.subTest(mode=mode):
                documents = self.documents({"auth": {"password": ""}}, mode=mode,
                                           server={"auth": {"admin": {"password": graph_password}}})
                secret = engine_auth_secret(documents)
                username = base64.b64decode(secret["data"]["username"]).decode()
                password = base64.b64decode(secret["data"]["password"]).decode()
                self.assertEqual("admin", username)
                self.assertRegex(password, rf"^[A-Za-z0-9]{{{length}}}$")
                self.assertNotEqual(graph_password, password)
                self.assertEqual("keep", secret["metadata"]["annotations"]["helm.sh/resource-policy"])
                http = yaml.safe_load(engine_config(documents)["seatunnel.yaml"])["seatunnel"]["engine"]["http"]
                self.assertTrue(http["enable-basic-auth"])
                self.assertEqual(username, http["basic-auth-username"])
                self.assertEqual(password, http["basic-auth-password"])
                for configmap in engine_resources(documents, "ConfigMap"):
                    self.assertNotIn("seatunnel.yaml", configmap["data"])
                    self.assertNotIn(password, yaml.safe_dump(configmap["data"]))
                workload, = engine_resources(documents, "StatefulSet")
                self.assertNotIn(password, yaml.safe_dump(pod(workload)))
                volumes = {volume["name"] for volume in pod(workload)["spec"]["volumes"]
                           if volume.get("secret", {}).get("secretName") == secret["metadata"]["name"]}
                container, = pod(workload)["spec"]["containers"]
                self.assertTrue(any(mount["name"] in volumes
                                    and mount["mountPath"] == "/opt/seatunnel/config/seatunnel.yaml"
                                    for mount in container["volumeMounts"]))

    def test_native_basic_auth_preserves_special_credentials_and_cannot_be_disabled_by_config(self):
        username = "ops'\"\\$"
        password = "quote'\" backslash\\ dollar$ colon:"
        native = {"seatunnel": {"engine": {"backup-count": 0, "http": {
            "enable-http": True, "port": 8080, "enable-basic-auth": False,
            "basic-auth-username": "ignored-user", "basic-auth-password": "ignored-password"}}}}
        documents = self.documents({"auth": {"username": username, "password": password},
                                    "config": {"seatunnel.yaml": yaml.safe_dump(native)}})
        secret = engine_auth_secret(documents)
        self.assertEqual(username, base64.b64decode(secret["data"]["username"]).decode())
        self.assertEqual(password, base64.b64decode(secret["data"]["password"]).decode())
        http = yaml.safe_load(engine_config(documents)["seatunnel.yaml"])["seatunnel"]["engine"]["http"]
        self.assertTrue(http["enable-basic-auth"])
        self.assertEqual(username, http["basic-auth-username"])
        self.assertEqual(password, http["basic-auth-password"])

    def test_image_secret_environment_and_native_file_overrides_reach_engine(self):
        custom = {"jvm_options": "-Xms128m\n-Xmx384m\n", "log4j2.properties": "status=warn\n"}
        secret_env = {"name": "HG_PASSWORD", "valueFrom": {"secretKeyRef": {"name": "graph-auth", "key": "password"}}}
        documents = self.documents({"image": {"repository": "team/seatunnel", "tag": "tested", "pullPolicy": "Never"},
                                    "env": [secret_env], "config": custom},
                                   **{"global": {"imageRegistry": "registry.example.com"},
                                      "imagePullSecrets": [{"name": "registry-auth"}]})
        workload, = engine_resources(documents, "StatefulSet")
        spec = pod(workload)["spec"]
        container, = spec["containers"]
        self.assertEqual("registry.example.com/team/seatunnel:tested", container["image"])
        self.assertEqual("Never", container["imagePullPolicy"])
        self.assertEqual([{"name": "registry-auth"}], spec["imagePullSecrets"])
        self.assertIn(secret_env, container["env"])
        data = engine_config(documents)
        mounts = {entry["subPath"] for entry in container["volumeMounts"]}
        for filename, content in custom.items():
            self.assertEqual(content, data[filename])
            self.assertIn(filename, mounts)
        previous, = engine_resources(self.documents({}), "StatefulSet")
        self.assertNotEqual(pod(previous)["metadata"]["annotations"]["checksum/config"],
                            pod(workload)["metadata"]["annotations"]["checksum/config"])

    def test_invalid_enabled_mode_and_cluster_size_fail_before_install(self):
        for values in ({"mode": "invalid"}, {"mode": "cluster", "replicas": 0},
                       {"mode": "cluster", "replicas": 1},
                       {"auth": {"username": "${user.name}"}},
                       {"auth": {"password": "${user.name}"}},
                       {"auth": {"username": " admin"}},
                       {"auth": {"password": "password "}},
                       {"auth": {"username": "admin:operator"}}):
            with self.subTest(values=values):
                self.assertNotEqual(0, self.render(values).returncode)

    def test_strict_policies_allow_engine_server_dns_and_native_extensions(self):
        extra_in = {"from": [{"namespaceSelector": {"matchLabels": {"team": "imports"}},
                              "podSelector": {"matchLabels": {"app": "import-client"}}}],
                    "ports": [{"port": 8080}]}
        extra_out = {"to": [{"ipBlock": {"cidr": "10.20.0.0/16"}}], "ports": [{"port": 9099}]}
        documents = self.documents({"mode": "cluster"}, mode="strict", server={"port": 8182},
                                   networkPolicy={"seatunnel": {"extraIngress": [extra_in], "extraEgress": [extra_out]}})
        policies = [doc for doc in documents if doc["kind"] == "NetworkPolicy"]
        engine = next(doc["spec"] for doc in policies if doc["metadata"]["name"].endswith("-seatunnel"))
        server = next(doc["spec"] for doc in policies if doc["metadata"]["name"].endswith("-server"))
        workload, = engine_resources(documents, "StatefulSet")
        self.assertTrue(matches(engine["podSelector"]["matchLabels"], pod(workload)["metadata"]["labels"]))
        self.assertIn(extra_in, engine["ingress"])
        self.assertIn(extra_out, engine["egress"])

        def allows(rules, direction, selector, port):
            return any(peer.get("podSelector", {}).get("matchLabels") == selector
                       and any(entry["port"] == port for entry in rule.get("ports", []))
                       for rule in rules for peer in rule.get(direction, []))

        selector = engine["podSelector"]["matchLabels"]
        self.assertTrue(allows(server["ingress"], "from", selector, 8182))
        self.assertTrue(allows(engine["egress"], "to", server["podSelector"]["matchLabels"], 8182))
        for direction, peer_key in (("ingress", "from"), ("egress", "to")):
            for port in (5801, 8080):
                self.assertTrue(allows(engine[direction], peer_key, selector, port))
        dns = [port for rule in engine["egress"] if not rule.get("to") for port in rule["ports"]]
        self.assertTrue({("UDP", 53), ("TCP", 53)} <= {(p["protocol"], p["port"]) for p in dns})

    def test_http_and_member_ingress_remain_isolated_without_full_network_policy(self):
        for mode in ("easy", "strict"):
            with self.subTest(mode=mode):
                documents = self.documents({}, mode=mode, networkPolicy={"enabled": False})
                policy, = engine_resources(documents, "NetworkPolicy")
                spec = policy["spec"]
                self.assertEqual(["Ingress"], spec["policyTypes"])
                self.assertNotIn("egress", spec)
                workload, = engine_resources(documents, "StatefulSet")
                labels = pod(workload)["metadata"]["labels"]
                self.assertTrue(matches(spec["podSelector"]["matchLabels"], labels))
                member_rules = [rule for rule in spec["ingress"]
                                if any(port["port"] == 5801 for port in rule["ports"])]
                self.assertEqual(1, len(member_rules))
                self.assertEqual([{"podSelector": {"matchLabels": spec["podSelector"]["matchLabels"]}}],
                                 member_rules[0]["from"])
                selector = member_rules[0]["from"][0]["podSelector"]["matchLabels"]
                self.assertFalse(matches(selector, {**labels, "app.kubernetes.io/component": "server"}))
                self.assertFalse(matches(selector, {**labels, "app.kubernetes.io/instance": "other"}))
                open_rules = [rule for rule in spec["ingress"] if not rule.get("from")]
                self.assertEqual([], open_rules)
                self.assertFalse(any("ipBlock" in peer for rule in spec["ingress"] for peer in rule["from"]))
                for component in ("seatunnel", "hubble", "test"):
                    peer = {**labels, "app.kubernetes.io/component": component}
                    self.assertTrue(ingress_allows(spec, peer, 8080))
                    self.assertEqual(component == "seatunnel", ingress_allows(spec, peer, 5801))
                for peer in ({**labels, "app.kubernetes.io/component": "server"},
                             {**labels, "app.kubernetes.io/instance": "other"}):
                    for port in (8080, 5801):
                        self.assertFalse(ingress_allows(spec, peer, port))

    def test_native_ingress_extensions_require_explicit_internal_pod_selectors(self):
        for rule in ({}, {"ports": [{"port": 8080}]}, {"from": []}, {"from": [{}]},
                     {"from": [{"ipBlock": {"cidr": "0.0.0.0/0"}}]},
                     {"from": [{"ipBlock": {"cidr": "10.0.0.0/8"}}]}):
            with self.subTest(rule=rule):
                result = self.render({}, networkPolicy={"seatunnel": {"extraIngress": [rule]}})
                self.assertNotEqual(0, result.returncode)

    def test_engine_security_defaults_and_native_scheduling_overrides(self):
        workload, = engine_resources(self.documents({}), "StatefulSet")
        container, = pod(workload)["spec"]["containers"]
        self.assertEqual({"allowPrivilegeEscalation": False,
                          "capabilities": {"drop": ["ALL"]},
                          "seccompProfile": {"type": "RuntimeDefault"}}, container["securityContext"])
        overrides = {
            "podSecurityContext": {"runAsUser": 1000, "fsGroup": 1000},
            "securityContext": {"runAsNonRoot": True},
            "nodeSelector": {"workload": "ingestion"},
            "tolerations": [{"key": "ingestion", "operator": "Exists", "effect": "NoSchedule"}],
            "affinity": {"podAntiAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": []}},
            "priorityClassName": "ingestion",
        }
        workload, = engine_resources(self.documents(overrides), "StatefulSet")
        spec = pod(workload)["spec"]
        self.assertEqual(overrides["podSecurityContext"], spec["securityContext"])
        for field in ("nodeSelector", "tolerations", "affinity", "priorityClassName"):
            self.assertEqual(overrides[field], spec[field])
        container, = spec["containers"]
        self.assertTrue(container["securityContext"]["runAsNonRoot"])
        self.assertFalse(container["securityContext"]["allowPrivilegeEscalation"])
        for invalid in ({"securityContext": []}, {"nodeSelector": []}, {"tolerations": {}},
                        {"affinity": []}, {"priorityClassName": 1}):
            with self.subTest(invalid=invalid):
                self.assertNotEqual(0, self.render(invalid).returncode)

    def test_packaged_chart_has_both_modes_without_seatunnel_dependency(self):
        with tempfile.TemporaryDirectory(prefix="seatunnel-package-") as directory:
            self.run_helm(["package", str(CHART), "--destination", directory],
                          HELM_PACKAGE_TIMEOUT_SECONDS, check=True)
            package = next(Path(directory).glob("*.tgz"))
            with tarfile.open(package, "r:gz") as archive:
                metadata = yaml.safe_load(archive.extractfile("hugegraph-helm/Chart.yaml"))
                self.assertFalse(any("seatunnel" in dependency["name"]
                                     for dependency in metadata.get("dependencies", [])))
                self.assertTrue(any(name.endswith("/templates/seatunnel.yaml") for name in archive.getnames()))
            self.assertEqual([], engine_resources(self.documents(chart=package)))
            for mode, replicas in (("standalone", 1), ("cluster", 2)):
                documents = self.documents({"mode": mode}, chart=package)
                workload, = engine_resources(documents, "StatefulSet")
                self.assertEqual(replicas, workload["spec"]["replicas"])
                source, = engine_resources(self.documents({"mode": mode}), "StatefulSet")
                self.assertEqual(source["spec"], workload["spec"])


if __name__ == "__main__":
    unittest.main()
