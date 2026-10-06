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

"""Executable Git fixtures and fail-closed gate tests for CI selection."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("policy", Path(__file__).with_name("ci-policy.py"))
policy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(policy)

class PolicyTest(unittest.TestCase):

    def live_pr(self):
        return {"state": "open", "head": {"sha": "head", "ref": "feature", "repo": {"full_name": "alice/server"}},
                "base": {"sha": "base", "repo": {"full_name": "apache/server"}}}

    def setUp(self):
        # All tests stay local; successful PR gates query only this current-PR fixture.
        api_mock = patch.object(policy, "api", return_value=self.live_pr())
        self.addCleanup(api_mock.stop)
        self.api_mock = api_mock.start()

    def test_dependency_expansion(self):
        self.assertEqual({"client", "loader", "tools", "spark", "hubble"},
                         policy.select("toolchain", ["hugegraph-client/src/A.java"]))
        self.assertEqual({"loader", "hubble"}, policy.select("toolchain", ["hugegraph-loader/src/A.java"]))
        self.assertEqual({"server", "pd", "store", "hstore", "cluster"}, policy.select("server", ["hugegraph-server/A.java"]))

    def test_server_backend_and_startup_dependents(self):
        selected = policy.select("server", ["hugegraph-server/hugegraph-hstore/src/test/HstoreTableTest.java"])
        self.assertIn("hstore", selected)
        self.assertIn("pd_store", policy.suites("server", selected))
        selected = policy.select("server", ["hugegraph-server/hugegraph-dist/src/assembly/travis/test-start-hugegraph-pd.sh"])
        self.assertIn("pd_store", policy.suites("server", selected))
        self.assertIn("docker", selected)
        for module in ["hugegraph-core", "hugegraph-api", "hugegraph-test"]:
            selected = policy.select("server", [f"hugegraph-server/{module}/src/main/A.java"])
            self.assertIn("hstore", selected)
            self.assertIn("server", selected)
        self.assertEqual(set(), policy.select("server", ["hugegraph-server/hugegraph-hstore/README.md"]))
        self.assertIn("hstore", policy.select("server", ["hugegraph-server/hugegraph-rocksdb/src/main/A.java"]))

    def test_audited_consumer_edges(self):
        expected = {"server", "pd", "store", "hstore", "cluster"}
        for path in ["hugegraph-server/hugegraph-hstore/src/A.java", "hugegraph-server/hugegraph-rocksdb/A.java"]:
            self.assertTrue(expected.issubset(policy.select("server", [path])))
            self.assertNotIn("helm", policy.select("server", [path]))
        self.assertEqual(set(policy.MODULES["server"]), policy.select("server", [".github/workflows/server-ci.yml"]))
        self.assertTrue({"commons", "docker"}.issubset(policy.select("server", [
            "hugegraph-commons/hugegraph-common/src/main/resources/version.properties"])))
        self.assertIn("go", policy.select("toolchain", ["hugegraph-client/assembly/travis/start-hugegraph-servers.sh"]))
        self.assertNotIn("go", policy.select("toolchain", ["hugegraph-client/src/A.java"]))
        self.assertNotIn("server", policy.select("server", ["hugegraph-pd/src/A.java"]))
        self.assertTrue({"store", "docker"}.issubset(policy.select("server", [
            "hugegraph-store/hg-store-dist/src/assembly/static/bin/util.sh"])))

    def test_pd_store_distribution_inputs_include_docker_consumers(self):
        for module, distribution, startup in [
            ("pd", "hg-pd-dist", "start-hugegraph-pd.sh"),
            ("store", "hg-store-dist", "start-hugegraph-store.sh"),
        ]:
            for relative in [f"src/assembly/static/bin/{startup}",
                             "src/assembly/static/conf/application.yml",
                             "src/assembly/descriptor/server-assembly.xml", "pom.xml"]:
                path = f"hugegraph-{module}/{distribution}/{relative}"
                with self.subTest(path=path):
                    selected = policy.select("server", [path])
                    self.assertTrue({module, "hstore", "cluster", "docker"}.issubset(selected))
                    self.assertIn("pd_store", policy.suites("server", selected))
                    self.assertNotIn("server", selected)
                    self.assertNotIn("helm", selected)
                    self.assertEqual(relative == "pom.xml", "dependency_license" in selected)
            source = f"hugegraph-{module}/hg-{module}-core/src/main/java/Example.java"
            with self.subTest(path=source):
                selected = policy.select("server", [source])
                self.assertTrue({module, "hstore", "cluster"}.issubset(selected))
                self.assertNotIn("docker", selected)

    def test_version_resource_follows_real_docker_consumer(self):
        root = Path(__file__).resolve().parents[2]
        consumer = root / ".github/scripts/check-docker-images.sh"
        if not consumer.exists():
            self.skipTest("Server Docker consumer is not in the Toolchain repository")
        resource = next(line.strip().rstrip(")") for line in consumer.read_text().splitlines()
                        if line.strip().startswith("hugegraph-commons/") and "version.properties" in line)
        self.assertTrue((root / resource).is_file())
        self.assertTrue({"commons", "docker"}.issubset(policy.select("server", [resource])))

    def test_single_workflow_has_no_global_fanout(self):
        self.assertEqual({"hubble"}, policy.select("toolchain", [".github/workflows/hubble-ci.yml"]))
        self.assertEqual({"docker"}, policy.select("server", [".github/workflows/docker-build-ci.yml"]))

    def test_unknown_and_proto_fail_conservative(self):
        for path in ["pom.xml", ".github/scripts/new.py", "hugegraph-pd/api.proto", "mystery"]:
            self.assertEqual(set(policy.MODULES["server"]), policy.select("server", [path]))

    def test_strict_docs_allowlist(self):
        self.assertEqual(set(), policy.select("server", ["README.md", "docs/guide.md"]))
        for path in ["hugegraph-client/src/test/resources/README.md", "docs/type.ts", "docs/config.yml", "docs/example.java", "hugegraph-server/type.ts"]:
            self.assertTrue(policy.select("server", [path]))

    def test_real_git_modes_rename_and_cli(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args):
                return subprocess.check_output(["git", *args], cwd=root, text=True).strip()
            git("init", "-q")
            git("config", "user.email", "ci@example.invalid")
            git("config", "user.name", "CI")
            (root / "README.md").write_text("initial")
            git("add", ".")
            git("commit", "-qm", "base")
            base = git("rev-parse", "HEAD")
            (root / "README.md").write_text("docs change")
            git("commit", "-qam", "docs")
            head = git("rev-parse", "HEAD")
            old = os.getcwd()
            try:
                os.chdir(root)
                (root / "type.ts").write_text("type changed")
                git("add", ".")
                git("commit", "-qm", "types")
                (root / "README.md").chmod(0o755)
                git("add", "README.md")
                git("commit", "-qm", "executable prose")
                self.assertTrue(policy.unsafe_documentation("HEAD", ["README.md"]))
                (root / "README.md").unlink()
                (root / "README.md").symlink_to("type.ts")
                git("add", "README.md")
                git("commit", "-qm", "symlink prose")
                self.assertTrue(policy.unsafe_documentation("HEAD", ["README.md"]))
                (root / "hugegraph-server").mkdir()
                (root / "hugegraph-server" / "A.java").write_text("source")
                git("add", ".")
                git("commit", "-qm", "source")
                before_rename = git("rev-parse", "HEAD")
                (root / "docs").mkdir()
                git("mv", "hugegraph-server/A.java", "docs/renamed.md")
                git("commit", "-qm", "rename code to prose")
                changed = git("diff", "--no-renames", "--name-only", before_rename, "HEAD").splitlines()
                self.assertIn("server", policy.select("server", changed))
            finally:
                os.chdir(old)
            event = root / "event.json"
            event.write_text(json.dumps({"before": base}))
            output = root / "output"
            subprocess.run(["python3", str(Path(policy.__file__).resolve()), "plan", "--project", "server",
                            "--repository", "apache/server", "--event-path", str(event),
                            "--output", str(root / "plan.json")], cwd=root, check=True,
                           env={**os.environ, "GITHUB_OUTPUT": str(output)})
            self.assertIn("server=true\n", output.read_text())
            self.assertEqual(policy.suites("server", set(policy.MODULES["server"])),
                             json.loads((root / "plan.json").read_text())["expected"])

    def plan(self):
        return {"schema": 1, "project": "server", "repository": "apache/server", "pr": 7,
                "source": "alice/server", "branch": "feature", "base": "base", "head": "head",
                "testedMergeSHA": "merge", "expected": ["server_memory", "server_rocksdb", "pd_store", "cluster"],
                "selected": ["server", "pd", "store", "hstore", "cluster"]}

    def test_memory_gate_ignores_advisory_cancellation(self):
        plan = self.plan()
        results = {"plan": {"result": "success"}, "server_memory": {"result": "success"},
                   "cluster": {"result": "cancelled"}, "server_rocksdb": {"result": "failure"}}
        self.assertEqual(["server_memory"], policy.gate(plan, results, mode="memory")["executed"])
        for state in ["failure", "cancelled", "skipped", None]:
            results["server_memory"]["result"] = state
            with self.assertRaisesRegex(ValueError, "server_memory"):
                policy.gate(plan, results, mode="memory")
        with self.assertRaises(policy.GateFailure) as failure:
            policy.gate(plan, results, mode="advisory")
        self.assertEqual("cancelled", failure.exception.report["results"]["cluster"])
        self.assertNotIn("server_memory", failure.exception.report["results"])

    def test_api_pom_is_a_docker_input(self):
        self.assertIn("docker", policy.select("server", ["hugegraph-server/hugegraph-api/pom.xml"]))
        self.assertNotIn("docker", policy.select("server", ["hugegraph-server/hugegraph-api/src/main/A.java"]))

    def test_codeql_and_smoke_follow_affected_inputs(self):
        def plan_for(path):
            def git(*args):
                if args[0] == "diff":
                    return path
                return "base"
            with patch.object(policy, "git", side_effect=git), patch.object(policy, "unsafe_documentation", return_value=False):
                return policy.create_plan("server", {"before": "base"}, "apache/server")
        for path in ["README.md", "docs/guide.md", "helm/templates/service.yaml"]:
            with self.subTest(path=path):
                plan = plan_for(path)
                self.assertFalse(plan["security"])
                self.assertFalse(plan["smoke"])
        plan = plan_for("docker/Dockerfile")
        self.assertFalse(plan["security"])
        self.assertTrue(plan["smoke"])
        for path in [".mvn/maven.config", "pom.xml", "hugegraph-pd/src/main/A.java",
                     ".github/workflows/codeql-analysis.yml"]:
            with self.subTest(path=path):
                self.assertTrue(plan_for(path)["security"])

    def test_memory_result_survives_metadata_outage_and_base_movement(self):
        plan = self.plan()
        results = {"plan": {"result": "success"}, "server_memory": {"result": "success"}}
        def unavailable(_):
            raise subprocess.CalledProcessError(1, "gh")
        self.assertEqual(["server_memory"], policy.gate(plan, results, unavailable, mode="memory")["executed"])
        advanced = self.live_pr()
        advanced["base"]["sha"] = "advanced"
        policy.gate(plan, results, lambda _: advanced, mode="memory")
        advanced["head"]["sha"] = "new-head"
        with self.assertRaises(policy.StaleInputError):
            policy.gate(plan, results, lambda _: advanced, mode="memory")

    def test_memory_rejects_confirmed_source_and_malformed_metadata(self):
        plan = self.plan()
        results = {"plan": {"result": "success"}, "server_memory": {"result": "success"}}
        live = self.live_pr()
        live["head"]["repo"]["full_name"] = "another/fork"
        with self.assertRaises(policy.StaleInputError):
            policy.gate(plan, results, lambda _: live, mode="memory")
        for malformed in [{"state": "open"}, {"state": "open", "head": None}]:
            with self.subTest(metadata=malformed), self.assertRaises((KeyError, TypeError)):
                policy.gate(plan, results, lambda _: malformed, mode="memory")

    def test_gate_rejects_missing_failed_cancelled_skipped(self):
        plan = self.plan()
        results = {suite: {"result": "success"} for suite in plan["expected"]}
        results["plan"] = {"result": "success"}
        # Even an old plan with claimed successes cannot exempt any current suite.
        plan["reused"] = {suite: {"runID": 42, "testedMergeSHA": "forged"} for suite in plan["expected"]}
        for suite in plan["expected"]:
            for result in [None, "failure", "cancelled", "skipped"]:
                with self.subTest(suite=suite, result=result), self.assertRaisesRegex(ValueError, suite):
                    policy.gate(plan, dict(results, **{suite: {"result": result}}))
        self.assertEqual(plan["expected"], policy.gate(plan, results)["executed"])

    def test_third_party_failure_does_not_exempt_required_tests(self):
        selected = policy.select("server", ["pom.xml"])
        self.assertIn("dependency_license", selected)
        expected = {"server_memory", "server_rocksdb", "commons", "pd_store", "cluster", "docker", "helm"}
        self.assertEqual(expected, set(policy.suites("server", selected)))
        plan = dict(self.plan(), selected=sorted(selected), expected=sorted(expected))
        results = {suite: {"result": "success"} for suite in expected}
        results["plan"] = {"result": "success"}
        for result in ["failure", "cancelled", "skipped", None]:
            results["dependency_license"] = {"result": result}
            self.assertEqual(expected, set(policy.gate(plan, results)["executed"]))
        results["cluster"] = {"result": "failure"}
        with self.assertRaisesRegex(ValueError, "cluster"):
            policy.gate(plan, results)

    def test_incomplete_pr_event_never_becomes_a_push_plan(self):
        event = {"pull_request": dict(self.live_pr(), number=7)}
        cases = []
        for change in ("null_repo", "missing_repo", "missing_number", "empty_ref"):
            altered = json.loads(json.dumps(event))
            pr = altered["pull_request"]
            if change == "null_repo":
                pr["head"]["repo"] = None
            elif change == "missing_repo":
                del pr["head"]["repo"]
            elif change == "missing_number":
                del pr["number"]
            else:
                pr["head"]["ref"] = ""
            cases.append(altered)
        cases.extend([{"pull_request": {}}, {"pull_request": None}])
        with patch.object(policy, "git", return_value="merge"):
            for altered in cases:
                with self.subTest(event=altered), self.assertRaises(policy.StaleInputError):
                    policy.create_plan('server', altered, 'apache/server',
                                       lambda _: self.fail("Incomplete identity must fail before any API query"))

    def test_gate_refuses_failed_planner_even_empty(self):
        plan = dict(self.plan(), expected=[], selected=[])
        for result in ["failure", "skipped", "cancelled", None]:
            with self.subTest(result=result), self.assertRaises(ValueError):
                policy.gate(plan, {"plan": {"result": result}})
        report = policy.gate(plan, {"plan": {"result": "success"}})
        self.assertEqual([], report["executed"])
        self.assertNotIn("proofs", report)

    def test_cumulative_source_and_docs_always_run_current_tests_without_historical_apis(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args):
                return subprocess.check_output(["git", *args], cwd=root, text=True).strip()
            git("init", "-q")
            git("config", "user.email", "ci@example.invalid")
            git("config", "user.name", "CI")
            (root / "README.md").write_text("base")
            git("add", ".")
            git("commit", "-qm", "base")
            base = git("rev-parse", "HEAD")
            git("checkout", "-qb", "feature")
            (root / "hugegraph-server").mkdir()
            source = root / "hugegraph-server/A.java"
            source.write_text("source A")
            git("add", ".")
            git("commit", "-qm", "source")
            for text in ["docs B after test failure", "docs C after claimed old success"]:
                (root / "README.md").write_text(text)
                git("commit", "-qam", "docs")
                head = git("rev-parse", "HEAD")
                # Synthetic PR merge; a different tree with the same parents also exists.
                tree = git("rev-parse", "HEAD^{tree}")
                merge = git("commit-tree", tree, "-p", base, "-p", head, "-m", "PR merge")
                false_merge = git("commit-tree", git("rev-parse", base + "^{tree}"),
                                  "-p", base, "-p", head, "-m", "unexecuted tree")
                self.assertNotEqual(merge, false_merge)
                git("checkout", "--detach", "-q", merge)
                live = {"state": "open", "head": {"sha": head, "ref": "feature", "repo": {"full_name": "alice/server"}},
                        "base": {"sha": base, "repo": {"full_name": "apache/server"}}}
                event = {"pull_request": dict(live, number=7)}
                calls = []
                def fetch(path):
                    calls.append(path)
                    self.assertEqual("repos/apache/server/pulls/7", path)
                    return live
                old = os.getcwd()
                try:
                    os.chdir(root)
                    plan = policy.create_plan("server", event, "apache/server", fetch)
                finally:
                    os.chdir(old)
                self.assertEqual(["repos/apache/server/pulls/7"], calls)
                self.assertTrue(plan["server"])
                self.assertTrue(plan["pd_store"])
                self.assertTrue(plan["cluster"])
                self.assertTrue(plan["compatibility"])
                self.assertTrue(plan["security"])
                self.assertNotIn("reused", plan)
                results = {suite: {"result": "success"} for suite in plan["expected"]}
                results["plan"] = {"result": "success"}
                results["server_memory"] = {"result": "skipped"}
                with self.assertRaisesRegex(ValueError, "server_memory"):
                    policy.gate(plan, results)
                git("checkout", "-q", "feature")

    def test_pure_docs_pr_and_metadata_fail_closed(self):
        live = self.live_pr()
        event = {"pull_request": dict(live, number=7)}
        def git(*args):
            if args[0] == "show":
                return "base head"
            if args[0] == "diff":
                return "README.md\ndocs/ci.md"
            return "merge"
        with patch.object(policy, "git", side_effect=git), patch.object(policy, "unsafe_documentation", return_value=False):
            plan = policy.create_plan("server", event, "apache/server", lambda p: live)
            self.assertEqual([], plan["expected"])
            self.assertFalse(plan["server"])
            self.assertFalse(plan["security"])
            advanced = dict(live, base={"sha": "new-base", "repo": {"full_name": "apache/server"}})
            self.assertFalse(policy.create_plan("server", event, "apache/server", lambda p: advanced)["server"])
            with patch.object(policy, "git", side_effect=lambda *a: "wrong parents" if a[0] == "show" else git(*a)):
                with self.assertRaises(policy.StaleInputError):
                    policy.create_plan("server", event, "apache/server", lambda p: live)

    def test_api_failure_runs_full_required_without_losing_pr_identity(self):
        event = {"pull_request": dict(self.live_pr(), number=7)}
        def git(*args):
            return "base head" if args[0] == "show" else "merge"
        def fail(path):
            raise subprocess.CalledProcessError(1, "gh")
        with patch.object(policy, "git", side_effect=git):
            plan = policy.create_plan("server", event, "apache/server", fail)
        self.assertEqual(set(policy.MODULES["server"]), set(plan["selected"]))
        self.assertEqual((7, "alice/server", "feature", "base", "head"),
                         tuple(plan[key] for key in ["pr", "source", "branch", "base", "head"]))
        results = {suite: {"result": "success"} for suite in plan["expected"]}
        results.update(plan={"result": "success"}, fixture={"result": "success"},
                       **{"hubble-fixture": {"result": "success"}})
        with self.assertRaises(subprocess.CalledProcessError):
            policy.gate(plan, results, fail)
        self.assertEqual(plan["expected"], policy.gate(plan, results)["executed"])
        self.api_mock.assert_called_once_with("repos/apache/server/pulls/7")

    def test_real_pr_git_snapshot_and_final_gate_freshness(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args):
                return subprocess.check_output(["git", *args], cwd=root, text=True).strip()
            git("init", "-q")
            git("config", "user.email", "ci@example.invalid")
            git("config", "user.name", "CI")
            (root / "README.md").write_text("base")
            git("add", ".")
            git("commit", "-qm", "base")
            base = git("rev-parse", "HEAD")
            git("checkout", "-qb", "feature")
            (root / "hugegraph-client").mkdir()
            (root / "hugegraph-client/A.java").write_text("source")
            git("add", ".")
            git("commit", "-qm", "source")
            head = git("rev-parse", "HEAD")
            tree = git("rev-parse", "HEAD^{tree}")
            merge = git("commit-tree", tree, "-p", base, "-p", head, "-m", "PR merge")
            advanced_head = git("commit-tree", tree, "-p", head, "-m", "new source head")
            advanced_base = git("commit-tree", git("rev-parse", base + "^{tree}"),
                                "-p", base, "-m", "new target base")
            git("checkout", "--detach", "-q", merge)
            live = self.live_pr()
            live["head"]["sha"], live["base"]["sha"] = head, base
            event = {"pull_request": dict(live, number=7)}
            old = os.getcwd()
            try:
                os.chdir(root)
                plan = policy.create_plan("server", event, "apache/server", lambda _: live)
                results = {suite: {"result": "success"} for suite in plan["expected"]}
                results.update(plan={"result": "success"}, fixture={"result": "success"},
                               **{"hubble-fixture": {"result": "success"}})
                self.assertEqual(plan["expected"], policy.gate(plan, results, lambda _: live)["executed"])
                for section, field, value in [("head", "sha", advanced_head),
                                              ("head", "ref", "other-branch"),
                                              ("head", "repo", {"full_name": "other/fork"}),
                                              ("base", "repo", {"full_name": "other/target"})]:
                    changed = dict(live, **{section: dict(live[section], **{field: value})})
                    with self.subTest(section=section, field=field):
                        with self.assertRaises(policy.StaleInputError):
                            policy.create_plan("server", event, "apache/server", lambda _: changed)
                        with self.assertRaises(policy.StaleInputError):
                            policy.gate(plan, results, lambda _: changed)
                with self.assertRaises(policy.StaleInputError):
                    policy.gate(plan, results, lambda _: dict(live, state="closed"))
                git("checkout", "--detach", "-q", head)
                with self.assertRaises(policy.StaleInputError):
                    policy.create_plan("server", event, "apache/server", lambda _: live)
            finally:
                os.chdir(old)

    def test_push_gate_does_not_query_pr(self):
        plan = dict(self.plan(), pr=0, expected=[], selected=[])
        policy.gate(plan, {"plan": {"result": "success"}},
                    lambda _: self.fail("push gate must not query a PR"))


if __name__ == "__main__":
    unittest.main()
