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

"""Execute the rendered Helm hook with local command fixtures, without networking."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm" / "hugegraph"
MOCK = r'''
import json, os, pathlib, sys
command = pathlib.Path(sys.argv[0]).name
directory = pathlib.Path(os.environ["FIXTURE_DIR"])
if command == "date":
    counter = directory / "time"
    value = int(counter.read_text()) + 100 if counter.exists() else 0
    counter.write_text(str(value))
    print(value)
elif command == "getent":
    for address in os.environ["FIXTURE_IPS"].split():
        print(address, "STREAM server")
elif command == "curl":
    args = sys.argv[1:]
    config = sys.stdin.read() if "--config" in args else ""
    url = args[-1]
    body = json.loads(args[args.index("-d") + 1]) if "-d" in args else None
    with (directory / "requests.jsonl").open("a") as output:
        output.write(json.dumps({"url": url, "body": body,
                                 "args": args, "config": config}) + "\n")
    if body is None:
        print("{}")
    else:
        case = os.environ.get("FIXTURE_CASE", "healthy")
        bad_pod = "10.0.0.2" in url
        business = body["gremlin"] == os.environ.get("FIXTURE_BUSINESS")
        failure = bad_pod and (case in ("binding", "invalid", "creating", "rebuilding") or
                               (case == "business" and business))
        if failure:
            message = {"binding": "Could not rebind graph",
                       "invalid": "Index by_age is INVALID (CREATED expected)",
                       "creating": "Index by_age is CREATING (CREATED expected)",
                       "rebuilding": "Index by_age is REBUILDING (CREATED expected)",
                       "business": "Assertion failed: expected index results"}[case]
            print(json.dumps({"message": message}))
            print("400" if case == "binding" else "500")
        else:
            print('{"result":{"data":[0]}}\n200')
'''


class HelmHookTest(unittest.TestCase):
    def render(self, directory, legacy_values=False, **values):
        chart = CHART
        if legacy_values:
            # --reuse-values can render with old defaults that predate this key.
            chart = directory / "legacy-chart"
            shutil.copytree(CHART, chart)
            defaults_file = chart / "values.yaml"
            defaults = yaml.safe_load(defaults_file.read_text())
            defaults["server"].pop("testGraphAlias")
            defaults_file.write_text(yaml.safe_dump(defaults))
        configuration = directory / "values.yaml"
        configuration.write_text(yaml.safe_dump(values))
        result = subprocess.run(
            ["helm", "template", "fixture", str(chart), "-f", str(configuration),
             "--show-only", "templates/tests/test-connection.yaml"],
            capture_output=True, text=True)
        self.assertEqual(0, result.returncode, result.stderr)
        return yaml.safe_load(result.stdout)["spec"]["containers"][0]["command"][2]

    def run_hook(self, case="healthy", addresses=None, legacy_values=False, **values):
        temporary = tempfile.TemporaryDirectory(prefix="helm-hook-")
        self.addCleanup(temporary.cleanup)
        directory = Path(temporary.name)
        script = self.render(directory, legacy_values=legacy_values, **values)
        commands = directory / "bin"
        commands.mkdir()
        for name in ("curl", "date", "getent", "sleep"):
            executable = commands / name
            executable.write_text(f"#!{sys.executable}\n" + MOCK)
            executable.chmod(0o755)
        business = values.get("server", {}).get("testGremlin", "")
        environment = dict(os.environ, PATH=f"{commands}:{os.environ['PATH']}",
                           FIXTURE_DIR=str(directory), FIXTURE_CASE=case,
                           FIXTURE_BUSINESS=business, PASSWORD="fixture-secret",
                           HG_TEST_DEADLINE_S="1", FIXTURE_IPS=addresses or
                           "10.0.0.1 10.0.0.2 10.0.0.3")
        result = subprocess.run(["sh", "-c", script], env=environment, cwd=directory,
                                capture_output=True, text=True, timeout=15)
        requests = [json.loads(line) for line in
                    (directory / "requests.jsonl").read_text().splitlines()]
        return result, requests, directory

    def test_every_pod_is_checked_with_auth(self):
        result, requests, _ = self.run_hook()
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual(3, sum(request["body"] is not None for request in requests))
        self.assertTrue(all("Authorization: Basic " in request["config"] for request in requests))
        self.assertIn("business assertions skipped", result.stdout)

    def test_failed_binding_or_unready_cached_index_reports_pod_and_stage(self):
        for case, message in (("binding", "Could not rebind graph"),
                              ("invalid", "Index by_age is INVALID (CREATED expected)"),
                              ("creating", "Index by_age is CREATING (CREATED expected)"),
                              ("rebuilding", "Index by_age is REBUILDING (CREATED expected)")):
            with self.subTest(case=case):
                result, requests, _ = self.run_hook(case)
                self.assertNotEqual(0, result.returncode)
                self.assertIn("Gremlin binding/index-state failed on http://10.0.0.2:8080/gremlin",
                              result.stdout)
                self.assertIn(message, result.stdout)
                self.assertEqual(3, sum(request["body"] is not None for request in requests))

    def test_business_assertion_runs_on_every_pod_and_reports_failure(self):
        business = "assert graph.traversal().V().count().next() == 7; true"
        for case, expected in (("healthy", 0), ("business", 1)):
            with self.subTest(case=case):
                result, requests, _ = self.run_hook(case, server={"testGremlin": business})
                self.assertEqual(expected, result.returncode, result.stdout + result.stderr)
                self.assertEqual(3, sum(request["body"] is not None and
                                       request["body"]["gremlin"] == business for request in requests))
                if expected:
                    self.assertIn("business assertion failed on http://10.0.0.2:8080/gremlin",
                                  result.stdout)
                    self.assertIn("Assertion failed: expected index results", result.stdout)

    def test_selected_alias_applies_to_index_and_business_checks(self):
        business = "assert graph.traversal().V().count().next() == 7; true"
        result, requests, _ = self.run_hook(
            server={"testGraphAlias": "team_a-demo", "testGremlin": business})
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        payloads = [request["body"] for request in requests if request["body"]]
        self.assertEqual(6, len(payloads))
        self.assertTrue(all(payload["aliases"] == {"graph": "team_a-demo"}
                            for payload in payloads))

    def test_missing_or_null_alias_preserves_default_for_both_checks(self):
        business = "assert graph.traversal().V().count().next() == 7; true"
        for missing in (False, True):
            with self.subTest(alias="missing" if missing else "null"):
                server = {"testGremlin": business}
                if not missing:
                    server["testGraphAlias"] = None
                result, requests, _ = self.run_hook(legacy_values=missing, server=server)
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                payloads = [request["body"] for request in requests if request["body"]]
                self.assertEqual(6, len(payloads))
                self.assertEqual([{"graph": "DEFAULT-hugegraph"}] * 6,
                                 [payload["aliases"] for payload in payloads])

    def test_business_script_is_json_data_and_auth_can_be_disabled(self):
        business = '''assert 'single' != "double"
// `touch injected` $(touch injected) ' "
true'''
        result, requests, directory = self.run_hook(
            server={"testGremlin": business, "auth": {"enabled": False}},
            hubble={"enabled": False})
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertFalse((directory / "injected").exists())
        self.assertTrue(all(not request["config"] and "--config" not in request["args"]
                            for request in requests))
        self.assertEqual(3, sum(request["body"] is not None and
                               request["body"]["gremlin"] == business for request in requests))

    def test_dual_stack_does_not_count_pods_twice(self):
        result, requests, _ = self.run_hook(
            addresses="10.0.0.1 10.0.0.2 fd00::1 fd00::2")
        self.assertNotEqual(0, result.returncode)
        self.assertIn("pods=2 floor=3", result.stderr)
        self.assertEqual(2, sum(request["body"] is not None for request in requests))

    def test_rendered_groovy_checks_cached_ids_without_refreshing_indexes(self):
        self.assertTrue(os.environ.get("GROOVY_JAR"),
                        "Set GROOVY_JAR to the Server Groovy jar before running this suite")
        _, requests, directory = self.run_hook()
        payload = next(request["body"] for request in requests if request["body"])
        script = directory / "smoke.groovy"
        script.write_text(payload["gremlin"])
        result = subprocess.run(
            [shutil.which("java"), "-cp", os.environ["GROOVY_JAR"], "groovy.ui.GroovyMain",
             str(ROOT / "helm" / "testdata" / "test_index_check.groovy"), str(script)],
            capture_output=True, text=True, timeout=30)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("cached index checks passed", result.stdout)


if __name__ == "__main__":
    unittest.main()
