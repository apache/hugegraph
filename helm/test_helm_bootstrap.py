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

"""Compare whole rendered Pod templates across controlled topology changes."""

from pathlib import Path
import os
import re
import subprocess
import tempfile
import unittest


CHART = Path(__file__).resolve().parent / "hugegraph"


class BootstrapLifecycleTest(unittest.TestCase):
    def render(self, pd=3, stores=3, initial_pd=3, initial_stores=3, initial_count=3,
               upgrade=True, wait_timeout=900):
        values = {
            "fullnameOverride": "cluster",
            "pd.replicas": pd,
            "store.replicas": stores,
            "store.waitTimeoutSeconds": wait_timeout,
            "pd.pdb.minAvailable": pd // 2 + 1,
            "store.pdb.minAvailable": max(stores - 1, 1),
            "pd.auth.existingSecret": "fixture-pd",
            "server.auth.admin.existingSecret": "fixture-admin",
            "server.auth.token.existingSecret": "fixture-token",
        }
        if upgrade:
            values.update({
                "bootstrap.pdReplicas": initial_pd,
                "bootstrap.storeReplicas": initial_stores,
                "bootstrap.initialStoreCount": initial_count,
            })
        command = ["helm", "template", "bootstrap-test", str(CHART),
                   "--namespace", "bootstrap-test"]
        if upgrade:
            command.append("--is-upgrade")
        for key, value in values.items():
            command.extend(["--set", f"{key}={value}"])
        try:
            result = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
                timeout=300,
            )
        except subprocess.CalledProcessError as error:
            self.fail(f"helm template failed (exit {error.returncode}):\n{error.stderr}")
        except subprocess.TimeoutExpired as error:
            self.fail(f"helm template timed out after {error.timeout}s")
        documents = {}
        for document in result.stdout.split("---\n"):
            source = re.search(r"^# Source: [^\n]+/templates/([^\n]+)$", document, re.M)
            if source:
                documents[source.group(1)] = document
        return documents

    def template(self, documents, component):
        name = "server-deployment.yaml" if component == "server" else component + "-statefulset.yaml"
        lines = documents[name].splitlines(keepends=True)
        start = next(i for i, line in enumerate(lines) if line == "  template:\n")
        end = start + 1
        while end < len(lines) and (not lines[end].strip() or lines[end].startswith("    ")):
            end += 1
        return "".join(lines[start:end])

    def assert_templates_unchanged(self, before, after):
        for component in ("pd", "store", "server"):
            with self.subTest(component=component):
                self.assertEqual(self.template(before, component), self.template(after, component))

    def test_store_growth_only_changes_capacity_resources(self):
        self.assert_templates_unchanged(self.render(stores=3), self.render(stores=4))

    def test_store_growth_across_default_rf_boundary_preserves_bootstrap(self):
        before = self.render(stores=1, initial_stores=1, initial_count=1)
        after = self.render(stores=3, initial_stores=1, initial_count=1)
        self.assert_templates_unchanged(before, after)
        self.assertIn("-Dpartition.default-shard-count=1", self.template(after, "pd"))

    def test_pd_growth_keeps_initial_peers_and_consumers_stable(self):
        self.assert_templates_unchanged(self.render(pd=3), self.render(pd=5))

    def test_pd_shrink_keeps_recorded_five_peer_bootstrap(self):
        before = self.render(pd=5, initial_pd=5)
        after = self.render(pd=3, initial_pd=5)
        self.assert_templates_unchanged(before, after)

    def test_initial_single_pd_seed_loss_blocks_store_even_after_pd_growth(self):
        # Model the supported script boundary, not a running Raft cluster: PD1/PD2
        # answer successfully but the historical PD0 seed is unavailable.
        documents = self.render(pd=3, initial_pd=1, wait_timeout=1)
        lines = documents["store-statefulset.yaml"].splitlines()
        start = lines.index("              set -eu")
        script = []
        for line in lines[start:]:
            if not line.startswith("              "):
                break
            script.append(line[14:])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "curl").write_text(
                '#!/bin/sh\nprintf "%s\\n" "$*" >> "$CURL_CALLS"\n'
                'case "$*" in *cluster-pd-0.*) exit 1;; *) exit 0;; esac\n')
            (root / "date").write_text(
                '#!/bin/sh\nif [ -e "$FAKE_TIME_FILE" ]; then echo 2; '
                'else touch "$FAKE_TIME_FILE"; echo 0; fi\n')
            for name in ("curl", "date"):
                (root / name).chmod(0o755)
            environment = {**os.environ, "PATH": directory + os.pathsep + os.environ["PATH"],
                           "CURL_CALLS": str(root / "calls"),
                           "FAKE_TIME_FILE": str(root / "time")}
            result = subprocess.run(["sh", "-c", "\n".join(script)], env=environment,
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(1, result.returncode)
            self.assertIn("Timed out", result.stderr)
            calls = (root / "calls").read_text()
            self.assertIn("cluster-pd-0.", calls)
            self.assertNotIn("cluster-pd-1.", calls)
            self.assertNotIn("cluster-pd-2.", calls)

    def test_removed_switch_cannot_bypass_bootstrap_protection(self):
        for enabled in ("false", "true"):
            with self.subTest(old_switch=enabled):
                result = subprocess.run(
                    ["helm", "template", "removed-switch", str(CHART),
                     "--set", "bootstrap.enabled=" + enabled],
                    capture_output=True, text=True, timeout=300)
                self.assertNotEqual(0, result.returncode)
                self.assertRegex(result.stderr.lower(), r"additional propert(?:y|ies).*enabled")

    def test_fresh_topologies_bootstrap_the_requested_members(self):
        for count in (1, 3, 5):
            with self.subTest(replicas=count):
                documents = self.render(pd=count, stores=count, upgrade=False)
                pd = documents["pd-statefulset.yaml"]
                value = re.search(r'- name: HG_PD_RAFT_PEERS_LIST\n\s+value: "([^"]+)"', pd).group(1)
                self.assertEqual(count, len(value.split(",")))
                self.assertIn(f'  pdReplicas: "{count}"', documents["bootstrap-configmap.yaml"])
                self.assertIn(f'  initialStoreCount: "{count}"', documents["bootstrap-configmap.yaml"])


if __name__ == "__main__":
    unittest.main()
