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

"""Exercise the rendered Hubble wrapper without starting a JVM or a cluster."""

import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest

import yaml


CHART = Path(__file__).resolve().parent / "hugegraph"
KEY_ENV = "HG_HUBBLE_TEXT2GQL_MASTER_KEY_FILE"
KEY_PROPERTY = "text2gql.shared.master_key_file="
KEY_BYTES = b"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=\n"
START = """import json
import os
from pathlib import Path
import stat
import sys

key = Path(os.environ["HG_HUBBLE_TEXT2GQL_MASTER_KEY_FILE"])
Path("started.json").write_text(json.dumps({
    "args": sys.argv[1:],
    "mode": stat.S_IMODE(key.stat().st_mode) if key.exists() else None,
    "key": key.read_bytes().hex() if key.is_file() else None,
}))
"""


class HubbleText2GqlTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if shutil.which("helm") is None:
            raise RuntimeError("Helm is required for the Hubble startup regression")

    def fixture(self, mode="pd", persistence=True, properties="", extra_env=None,
                vermeer=False, run_as_user=None):
        temporary = tempfile.TemporaryDirectory(prefix="hubble-wrapper-")
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        values = {"hubble": {"mode": mode, "persistence": {"enabled": persistence}}}
        if vermeer:
            values["vermeer"] = {"enabled": True}
        if run_as_user is not None:
            values["hubble"]["securityContext"] = {"runAsUser": run_as_user}
        if extra_env is not None:
            values["hubble"]["extraEnv"] = extra_env
        configuration = root / "values.yaml"
        configuration.write_text(yaml.safe_dump(values))
        rendered = subprocess.run(
            ["helm", "template", "fixture", str(CHART), "--values", str(configuration),
             "--show-only", "templates/hubble-deployment.yaml"],
            capture_output=True, text=True, timeout=120)
        self.assertEqual(0, rendered.returncode, rendered.stderr)
        container, = yaml.safe_load(rendered.stdout)["spec"]["template"]["spec"]["containers"]
        environment = {"PATH": os.environ["PATH"], "LC_ALL": "C"}
        for entry in container["env"]:
            environment[entry["name"]] = str(entry.get("value", "fixture-secret"))
        self.assertEqual("/hubble-data/text2gql-master.key", environment[KEY_ENV])
        # Relocate only the chart-managed file into this fixture; execute the
        # rendered wrapper unchanged, including its real configuration rewrite.
        key = root / "data/text2gql-master.key"
        key.parent.mkdir()
        environment[KEY_ENV] = str(key)
        (root / "conf").mkdir()
        (root / "conf/hugegraph-hubble.properties").write_text(
            "unrelated.setting=preserved\n" + properties)
        (root / "bin").mkdir()
        start = root / "bin/start-hubble.sh"
        start.write_text(f"#!{sys.executable}\n" + START)
        start.chmod(0o755)
        return root, key, container, environment

    def run_wrapper(self, root, container, environment):
        (root / "started.json").unlink(missing_ok=True)
        return subprocess.run(container["command"] + container["args"], cwd=root,
                              env=environment, capture_output=True, text=True, timeout=15)

    def assert_started(self, root, result, mode=None):
        self.assertEqual(0, result.returncode, result.stderr)
        snapshot = json.loads((root / "started.json").read_text())
        self.assertEqual(["-f"], snapshot["args"])
        self.assertEqual(mode, snapshot["mode"])
        self.assertEqual(KEY_BYTES.hex() if mode is not None else None, snapshot["key"])

    def deny_key_chmod(self, root, environment):
        executable = root / "bin/chmod"
        executable.write_text(
            f"#!{sys.executable}\n"
            "import os, sys\n"
            "if sys.argv[-1] == os.environ['HG_HUBBLE_TEXT2GQL_MASTER_KEY_FILE']:\n"
            "    print('fixture chmod denied', file=sys.stderr)\n"
            "    sys.exit(73)\n"
            f"os.execv({shutil.which('chmod')!r}, ['chmod', *sys.argv[1:]])\n")
        executable.chmod(0o755)
        environment["PATH"] = str(root / "bin") + os.pathsep + environment["PATH"]

    def test_missing_key_is_not_created_and_property_rewrite_is_idempotent(self):
        for mode in ("pd", "direct"):
            for persistence in (False, True):
                for properties in ("", KEY_PROPERTY + "data/text2gql-master.key\n",
                                   KEY_PROPERTY + "data/old.key\n" + KEY_PROPERTY + "data/duplicate.key"):
                    with self.subTest(mode=mode, persistence=persistence, properties=properties):
                        root, key, container, environment = self.fixture(mode, persistence, properties)
                        for _ in range(2):
                            result = self.run_wrapper(root, container, environment)
                            self.assert_started(root, result)
                            self.assertFalse(key.exists())
                            lines = (root / "conf/hugegraph-hubble.properties").read_text().splitlines()
                            self.assertIn("unrelated.setting=preserved", lines)
                            self.assertEqual([KEY_PROPERTY + str(key)],
                                             [line for line in lines if line.startswith(KEY_PROPERTY)])

    def test_existing_key_permissions_are_restored_before_start_without_rotating_it(self):
        for mode in ("pd", "direct"):
            with self.subTest(mode=mode):
                root, key, container, environment = self.fixture(mode)
                key.write_bytes(KEY_BYTES)
                # Reproduce fsGroup widening on each volume mount, including a
                # repeated replacement, and retain already owner-only files.
                for permissions in (0o660, 0o600, 0o660):
                    key.chmod(permissions)
                    result = self.run_wrapper(root, container, environment)
                    self.assert_started(root, result, 0o600)
                    self.assertEqual(0o600, stat.S_IMODE(key.stat().st_mode))
                    self.assertEqual(KEY_BYTES, key.read_bytes())

    def test_environment_master_key_does_not_require_chmod_of_unused_file(self):
        override = [{"name": "HUBBLE_TEXT2GQL_MASTER_KEY", "value": KEY_BYTES.decode().strip()}]
        root, key, container, environment = self.fixture(extra_env=override)
        key.write_bytes(KEY_BYTES)
        key.chmod(0o660)
        self.deny_key_chmod(root, environment)
        result = self.run_wrapper(root, container, environment)
        self.assert_started(root, result, 0o660)

    def test_empty_or_whitespace_environment_master_key_uses_the_file(self):
        for override in ("", " \t\n"):
            with self.subTest(override=override):
                root, key, container, environment = self.fixture()
                environment["HUBBLE_TEXT2GQL_MASTER_KEY"] = override
                key.write_bytes(KEY_BYTES)
                key.chmod(0o660)
                result = self.run_wrapper(root, container, environment)
                self.assert_started(root, result, 0o600)

    def test_permission_repair_does_not_follow_symbolic_links(self):
        root, key, container, environment = self.fixture()
        target = root / "other.key"
        target.write_bytes(KEY_BYTES)
        target.chmod(0o660)
        key.symlink_to(target)
        self.deny_key_chmod(root, environment)
        result = self.run_wrapper(root, container, environment)
        self.assert_started(root, result, 0o660)
        self.assertTrue(key.is_symlink())
        self.assertEqual(0o660, stat.S_IMODE(target.stat().st_mode))

    def test_chmod_failure_warns_without_blocking_hubble_startup(self):
        for mode, vermeer in (("pd", False), ("direct", False), ("pd", True)):
            with self.subTest(mode=mode, vermeer=vermeer):
                root, key, container, environment = self.fixture(
                    mode, properties=KEY_PROPERTY + "data/old.key\n",
                    vermeer=vermeer, run_as_user=10001)
                self.assertEqual(10001, container["securityContext"]["runAsUser"])
                key.write_bytes(KEY_BYTES)
                key.chmod(0o660)
                # Simulate a key owned by a former UID without needing root or
                # changing this test process's identity. Vermeer honors the
                # explicit UID above; enabling it alone does not select 10001.
                self.deny_key_chmod(root, environment)
                result = self.run_wrapper(root, container, environment)
                self.assert_started(root, result, 0o660)
                self.assertIn("fixture chmod denied", result.stderr)
                self.assertIn("warning:", result.stderr)
                self.assertIn("Text2GQL master key", result.stderr)
                self.assertIn("continuing Hubble startup", result.stderr)
                self.assertIn("runAsUser", result.stderr)
                self.assertEqual(0o660, stat.S_IMODE(key.stat().st_mode))
                self.assertEqual(KEY_BYTES, key.read_bytes())
                lines = (root / "conf/hugegraph-hubble.properties").read_text().splitlines()
                self.assertIn("unrelated.setting=preserved", lines)
                self.assertEqual([KEY_PROPERTY + str(key)],
                                 [line for line in lines if line.startswith(KEY_PROPERTY)])


if __name__ == "__main__":
    unittest.main()
