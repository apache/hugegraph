#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Cover the first Vermeer enablement on a release that predates its defaults."""

from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import yaml


CHART = Path(__file__).resolve().parent / "hugegraph"


class VermeerLegacyEnablementTest(unittest.TestCase):
    def test_release_without_native_login_defaults_gets_an_actionable_upgrade_error(self):
        with tempfile.TemporaryDirectory(prefix="vermeer-admin-upgrade-") as directory:
            chart = Path(directory) / "chart"
            shutil.copytree(CHART, chart)
            old_values = yaml.safe_load((CHART / "values.yaml").read_text())
            old_values["vermeer"]["auth"].pop("admin")
            existing_values = Path(directory) / "existing-values.yaml"
            existing_values.write_text(yaml.safe_dump(old_values))
            (chart / "values.yaml").write_text(yaml.safe_dump(old_values))
            for enabled in (False, True):
                with self.subTest(enabled=enabled):
                    result = subprocess.run(
                        ["helm", "template", "legacy", str(chart), "--is-upgrade",
                         "--set", "vermeer.enabled=" + str(enabled).lower()],
                        text=True, capture_output=True, timeout=120)
                    if enabled:
                        self.assertNotEqual(0, result.returncode)
                        self.assertIn("vermeer.auth.admin is missing new chart defaults", result.stderr)
                        self.assertIn("-f existing-values.yaml", result.stderr)
                        self.assertNotIn("nil pointer", result.stderr)
                    else:
                        self.assertEqual(0, result.returncode, result.stderr)
            result = subprocess.run(
                ["helm", "template", "legacy", str(CHART), "--is-upgrade",
                 "-f", str(existing_values), "--set", "vermeer.enabled=true"],
                text=True, capture_output=True, timeout=120)
            self.assertEqual(0, result.returncode, result.stderr)
            documents = [doc for doc in yaml.safe_load_all(result.stdout) if doc]
            self.assertTrue(any(doc["kind"] == "Secret"
                                and doc["metadata"]["name"] == "legacy-vermeer-admin"
                                for doc in documents))

    def test_first_enablement_explains_how_to_import_new_defaults(self):
        with tempfile.TemporaryDirectory(prefix="vermeer-first-enable-") as directory:
            chart = Path(directory) / "chart"
            shutil.copytree(CHART, chart)
            old_values = yaml.safe_load((CHART / "values.yaml").read_text())
            old_values.pop("vermeer")
            old_values["networkPolicy"].pop("vermeer", None)
            existing_values = Path(directory) / "existing-values.yaml"
            existing_values.write_text(yaml.safe_dump(old_values))
            (chart / "values.yaml").write_text(yaml.safe_dump(old_values))
            command = ["helm", "template", "legacy", str(chart), "--is-upgrade",
                       "--set", "vermeer.enabled=true"]
            result = subprocess.run(command, text=True, capture_output=True)
            self.assertNotEqual(0, result.returncode)
            self.assertIn("legacy --reuse-values does not import", result.stderr)
            self.assertIn("-f existing-values.yaml", result.stderr)
            self.assertNotIn("nil pointer", result.stderr)
            self.assertNotIn("wrong type", result.stderr)

            # The documented values-file workflow imports defaults from the new chart.
            result = subprocess.run(
                ["helm", "template", "legacy", str(CHART), "--is-upgrade",
                 "-f", str(existing_values), "--set", "vermeer.enabled=true"],
                text=True, capture_output=True)
            self.assertEqual(0, result.returncode, result.stderr)
            documents = [doc for doc in yaml.safe_load_all(result.stdout) if doc]
            workloads = [doc for doc in documents if doc["kind"] == "StatefulSet"
                         and doc["metadata"]["labels"].get("app.kubernetes.io/component")
                         == "vermeer-master"]
            self.assertEqual(1, len(workloads))


if __name__ == "__main__":
    unittest.main()
