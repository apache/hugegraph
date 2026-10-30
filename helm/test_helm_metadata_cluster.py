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

"""Keep Server schema writes and Store schema reads in the same PD namespace."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import yaml


CHART = Path(__file__).resolve().parent / "hugegraph"
COMPONENT = "app.kubernetes.io/component"
CLUSTER_ANNOTATION = "hugegraph.apache.org/metadata-cluster"


class MetadataClusterChartTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if shutil.which("helm") is None:
            raise RuntimeError("Helm is required for the metadata namespace regression")

    def render(self, values=None, chart=CHART, upgrade=False):
        with tempfile.TemporaryDirectory(prefix="metadata-cluster-values-") as directory:
            configuration = Path(directory) / "values.yaml"
            configuration.write_text(yaml.safe_dump(values or {}))
            command = ["helm", "template", "metadata", str(chart), "--namespace", "custom-namespace",
                       "--values", str(configuration)]
            if upgrade:
                command.append("--is-upgrade")
            return subprocess.run(command, capture_output=True, text=True, timeout=120)

    def documents(self, values=None, **arguments):
        result = self.render(values, **arguments)
        self.assertEqual(0, result.returncode, result.stderr)
        return [document for document in yaml.safe_load_all(result.stdout) if document]

    def workload(self, documents, component):
        workload, = [document for document in documents
                     if document["kind"] in ("StatefulSet", "Deployment") and
                     document["metadata"]["labels"].get(COMPONENT) == component]
        return workload

    def container(self, workload, name):
        return next(container for container in workload["spec"]["template"]["spec"]["containers"]
                    if container["name"] == name)

    def assert_namespace(self, documents, expected):
        server = self.workload(documents, "server")
        store = self.workload(documents, "store")
        self.assertEqual(expected, server["metadata"]["annotations"][CLUSTER_ANNOTATION])
        for workload, role, variable in ((server, "server", "HG_SERVER_CLUSTER"),
                                          (store, "store", "PD_CLUSTER")):
            entries = [entry for entry in self.container(workload, role)["env"]
                       if entry["name"] == variable]
            self.assertEqual([{"name": variable, "value": expected}], entries)

    def test_default_retains_the_existing_server_metadata_namespace(self):
        defaults = yaml.safe_load((CHART / "values.yaml").read_text())
        self.assertEqual("hg-test", defaults["metadataCluster"])
        self.assert_namespace(self.documents(), "hg-test")

    def test_custom_metadata_namespace_reaches_both_roles_with_or_without_vermeer(self):
        for enabled in (False, True):
            with self.subTest(vermeer=enabled):
                documents = self.documents({"metadataCluster": "production-graph_42",
                                            "vermeer": {"enabled": enabled}})
                self.assert_namespace(documents, "production-graph_42")

    def test_legacy_values_missing_the_shared_field_keep_hg_test(self):
        with tempfile.TemporaryDirectory(prefix="metadata-cluster-legacy-") as directory:
            chart = Path(directory) / "chart"
            shutil.copytree(CHART, chart)
            (chart / "values.yaml").write_bytes((CHART / "testdata/values-pre-hardening.yaml").read_bytes())
            for upgrade in (False, True):
                with self.subTest(upgrade=upgrade):
                    documents = self.documents(chart=chart, upgrade=upgrade)
                    self.assert_namespace(documents, "hg-test")
                    self.assertFalse(any(document["metadata"].get("labels", {}).get(COMPONENT, "")
                                         .startswith("vermeer") for document in documents))

    def test_rendered_server_environment_reaches_native_property_mapping(self):
        selected = "production-graph_42"
        documents = self.documents({"metadataCluster": selected})
        container = self.container(self.workload(documents, "server"), "server")
        environment = {"PATH": os.environ["PATH"], "LC_ALL": "C"}
        for entry in container["env"]:
            environment[entry["name"]] = str(entry.get("value", "fixture-secret-" + "x" * 32))
        with tempfile.TemporaryDirectory(prefix="metadata-cluster-wrapper-") as directory:
            root = Path(directory)
            (root / "conf").mkdir()
            (root / "conf/graphs").mkdir()
            (root / "conf/graphs/hugegraph.properties").write_text("backend=hstore\n")
            (root / "bin").mkdir()
            properties = root / "conf/rest-server.properties"
            properties.write_text("cluster=unrelated-image-default\nunrelated.setting=preserved\n")
            entrypoint = root / "docker-entrypoint.sh"
            entrypoint.write_bytes((CHART.parents[1] / "hugegraph-server/hugegraph-dist/docker/"
                                    "docker-entrypoint.sh").read_bytes())
            entrypoint.chmod(0o755)
            # Execute the real property mapping with service startup and
            # readiness isolated; this fixture starts no JVM or HTTP client.
            (root / "bin/util.sh").write_text(
                "record_jvm_startup_diagnostics() { :; }\n"
                "diagnose_jvm_startup_failure() { :; }\n"
                "read_property() { printf 'http://127.0.0.1:8080'; }\n"
                "wait_for_startup() { :; }\n")
            for name in ("enable-auth.sh", "wait-storage.sh", "init-store.sh",
                         "start-hugegraph.sh", "wait-partition.sh"):
                executable = root / "bin" / name
                # The entrypoint pipes PASSWORD into init-store. Consume the
                # stream before returning so scheduler order cannot give its
                # printf producer SIGPIPE under the real pipefail setting.
                executable.write_text("#!/bin/sh\ncat >/dev/null\n" if name == "init-store.sh"
                                      else "#!/bin/sh\nexit 0\n")
                executable.chmod(0o755)
            result = subprocess.run(["bash", "-c", container["args"][0]], cwd=root,
                                    env=environment, capture_output=True, text=True, timeout=15)
            self.assertEqual(0, result.returncode, result.stderr)
            final = properties.read_text().splitlines()
        self.assertIn("unrelated.setting=preserved", final)
        self.assertEqual(["cluster=" + selected], [line for line in final if line.startswith("cluster=")])

    def test_component_environment_cannot_bypass_the_shared_namespace(self):
        for role, variable in (("server", "HG_SERVER_CLUSTER"), ("store", "PD_CLUSTER")):
            with self.subTest(role=role):
                result = self.render({role: {"extraEnv": [{"name": variable, "value": "other"}]}})
                self.assertNotEqual(0, result.returncode)
                self.assertIn(variable, result.stderr)

    def test_store_system_property_cannot_override_the_shared_namespace(self):
        for namespace in ("other", "hg-test"):
            with self.subTest(namespace=namespace):
                result = self.render({"store": {"javaOpts": "-Xmx512m -Dpd.cluster=" + namespace}})
                self.assertNotEqual(0, result.returncode)
                self.assertIn("metadataCluster", result.stderr)
        self.assert_namespace(self.documents({"store": {"javaOpts": "-Xmx512m -Dpd.cluster.metrics=true"}}),
                              "hg-test")

    def test_empty_or_non_string_namespace_is_rejected(self):
        for namespace in ("", 42):
            with self.subTest(namespace=namespace):
                result = self.render({"metadataCluster": namespace})
                self.assertNotEqual(0, result.returncode)
                self.assertIn("metadataCluster", result.stderr)


if __name__ == "__main__":
    unittest.main()
