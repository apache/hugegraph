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

"""Path/event selection, real PR snapshots, and coordinator scheduling contracts."""
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import textwrap
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("policy", Path(__file__).with_name("ci-policy.py"))
policy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(policy)


class GitFixture:
    def __init__(self, root):
        self.root = root
        self.git("init", "-q")
        self.git("config", "user.email", "ci@example.invalid")
        self.git("config", "user.name", "CI")

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.root, text=True).strip()

    def commit(self, path, content, message):
        file = self.root / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(content)
        self.git("add", ".")
        self.git("commit", "-qm", message)
        return self.git("rev-parse", "HEAD")

    def plan(self, event, fetch=None, event_name=None):
        with patch.object(policy, "git", side_effect=self.git), patch.dict(
                os.environ, {"GITHUB_SHA": self.git("rev-parse", "HEAD"), "GITHUB_EVENT_NAME": ""}):
            old = os.getcwd()
            try:
                os.chdir(self.root)
                return policy.create_plan("server", event, "apache/server", fetch=fetch, event_name=event_name)
            finally:
                os.chdir(old)


class PolicyTest(unittest.TestCase):
    def plan_for(self, paths, event_name="pull_request", event=None):
        # Identity checks have their own real-Git fixtures; isolate event selection here.
        def git(*args):
            if args[0] == "diff":
                return "\n".join(paths)
            if args[0] == "show":
                return "base head"
            return "merge"
        event = event or {"before": "base"}
        if event_name == "pull_request":
            event = {"pull_request": {"number": 7, "head": {"sha": "head", "ref": "feature",
                     "repo": {"full_name": "alice/server"}}, "base": {"sha": "base"}}}
        with patch.object(policy, "git", side_effect=git), patch.object(
                policy, "unsafe_documentation", return_value=False), patch.object(
                policy, "require_current_pr"), patch.dict(os.environ, {"GITHUB_SHA": "merge"}):
            return policy.create_plan("server", event, "apache/server", event_name=event_name)

    def test_ordinary_server_changes_only_run_mainline_server_and_arm_smoke(self):
        for path in ["hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/traversal/A.java",
                     "hugegraph-server/hugegraph-api/src/main/java/org/apache/hugegraph/api/traversers/A.java",
                     "hugegraph-server/hugegraph-test/src/main/java/org/apache/hugegraph/traversal/A.java"]:
            with self.subTest(path=path):
                plan = self.plan_for([path])
                self.assertEqual({"server", "macos"}, set(plan["selected"]))
                self.assertEqual({"server_memory", "server_rocksdb", "macos"}, set(plan["expected"]))
                self.assertFalse(plan["macos_full"])
                self.assertFalse(plan["pd_store"])

    def test_distributed_server_contracts_keep_hstore_cluster_without_pd_store_units(self):
        for path in [policy.SERVER_CORE + "backend/store/BackendStore.java",
                     policy.SERVER_CORE + "meta/MetaManager.java", policy.SERVER_CORE + "StandardHugeGraph.java",
                     policy.SERVER_CORE + "task/DistributedTaskScheduler.java", policy.SERVER_CORE + "task/TaskManager.java",
                     policy.SERVER_CORE + "masterelection/GlobalMasterInfo.java",
                     policy.SERVER_API + "core/GraphManager.java",
                     "hugegraph-server/hugegraph-hstore/src/main/A.java"]:
            with self.subTest(path=path):
                plan = self.plan_for([path])
                self.assertTrue({"server", "macos", "hstore", "cluster", "distributed_smoke"}.issubset(plan["selected"]))
                self.assertTrue(plan["pd_store"])
                self.assertFalse(plan["pd"])
                self.assertFalse(plan["store"])
                self.assertFalse(plan["hbase"])
                self.assertFalse(plan["riscv64"])

    def test_shared_runtime_consumers_and_protocols(self):
        for module in ["commons", "struct"]:
            selected = policy.select("server", [f"hugegraph-{module}/src/main/A.java"])
            self.assertTrue({"server", "pd", "store", "hstore", "cluster", "macos"}.issubset(selected))
            self.assertNotIn("hbase", selected)
            self.assertNotIn("riscv64", selected)
        for module in ["pd", "store"]:
            plan = self.plan_for([f"hugegraph-{module}/src/main/A.java"])
            self.assertTrue(plan["pd_store"])
            self.assertTrue(plan["hstore"])
            self.assertTrue(plan["cluster"])
            self.assertFalse(plan["server"])
            self.assertFalse(plan["macos"])
        self.assertTrue({"pd", "store", "hstore", "cluster"}.issubset(policy.select(
            "server", ["hugegraph-pd/hg-pd-grpc/src/main/proto/common.proto"])))

    def test_dependency_snapshot_inputs_do_not_fan_out_to_runtime_suites(self):
        for path in [".github/scripts/dependency-snapshot.py", ".github/scripts/dependency-inventory.py",
                     ".github/workflows/submit-dependencies.yml"]:
            self.assertEqual({"dependency_license"}, set(self.plan_for([path])["selected"]))
            self.assertEqual({"dependency_license"}, self.coordinator_jobs(self.plan_for([path])))
        fixture = ".github/scripts/test_dependency_snapshot.py"
        self.assertEqual([], self.plan_for([fixture])["expected"])
        runtime = "hugegraph-server/hugegraph-core/src/main/A.java"
        self.assertEqual(self.plan_for([runtime])["selected"], self.plan_for([runtime, fixture])["selected"])
        # Classifier changes still receive conservative mainline validation.
        self.assertEqual(policy.select("server", [".github/scripts/ci-policy.py"]),
                         policy.select("server", [".github/scripts/ci-policy.py", fixture]))

    def test_noncore_platforms_only_follow_direct_inputs(self):
        for path in ["hugegraph-server/hugegraph-hbase/src/main/A.java",
                     "hugegraph-server/hugegraph-dist/src/assembly/travis/install-hbase.sh",
                     "hugegraph-server/hugegraph-dist/src/assembly/travis/hbase-ci-site.xml"]:
            self.assertEqual({"hbase"}, policy.select("server", [path]))
        self.assertEqual({"riscv64"}, policy.select("server", [".github/workflows/riscv64-ci.yml"]))
        self.assertTrue(self.plan_for([".github/workflows/server-compatibility-ci.yml"])["riscv64"])
        self.assertTrue(self.plan_for([".github/workflows/server-tests.yml"])["hbase"])
        self.assertTrue(self.plan_for(["hugegraph-server/hugegraph-rocksdb/pom.xml"])["riscv64"])
        self.assertTrue(self.plan_for(["hugegraph-server/hugegraph-dist/pom.xml"])["riscv64"])
        for path in ["hugegraph-server/hugegraph-rocksdb/src/main/A.java",
                     "hugegraph-server/hugegraph-dist/src/assembly/static/bin/start-hugegraph.sh"]:
            plan = self.plan_for([path])
            self.assertTrue(plan["macos"])
            self.assertTrue(plan["macos_full"])
            self.assertFalse(plan["hbase"])
            self.assertEqual(path.startswith("hugegraph-server/hugegraph-dist/src/assembly/static/bin/"),
                             plan["riscv64"])
        for path in ["docker/hbase/Dockerfile", "docker/hbase/entrypoint.sh", policy.TRAVIS + "install-backend.sh"]:
            self.assertTrue(self.plan_for([path])["hbase"])
        install = self.plan_for([policy.TRAVIS + "install-backend.sh"])
        self.assertTrue(install["hstore"])
        java = self.plan_for([".github/workflows/.java-version"])
        self.assertTrue(java["macos_full"])
        self.assertTrue(java["hbase"])
        self.assertTrue(java["riscv64"])
        for path in policy.PLATFORM_POMS:
            self.assertTrue(self.plan_for([path])["riscv64"])
            self.assertTrue(self.plan_for([path])["macos_full"])
        for path in ["hugegraph-server/hugegraph-core/src/main/resources/proto/A.proto",
                     "hugegraph-pd/hg-pd-grpc/src/main/proto/A.proto"]:
            self.assertTrue(self.plan_for([path])["riscv64"])
        self.assertFalse(self.plan_for(["hugegraph-store/hg-store-grpc/src/main/proto/A.proto"])["riscv64"])
        self.assertFalse(self.plan_for(["hugegraph-pd/hg-pd-core/pom.xml"])["riscv64"])
        native = self.plan_for(["hugegraph-server/hugegraph-dist/src/assembly/travis/run-native-runtime-smoke-test.sh"])
        self.assertTrue(native["macos_full"])
        self.assertTrue(native["riscv64"])
        self.assertTrue(self.plan_for([policy.TRAVIS + "check-rocksdb-only-dist.sh"])["riscv64"])

    def test_security_tracks_sensitive_inputs_and_dependencies(self):
        paths = [policy.SERVER_CORE + "auth/AuthManager.java", policy.SERVER_CORE + "security/HugeSecurityManager.java",
                 policy.SERVER_CORE + "config/AuthOptions.java",
                 policy.SERVER_API + "api/filter/AuthenticationFilter.java", "hugegraph-pd/hg-pd-core/pom.xml",
                 ".github/workflows/codeql-analysis.yml",
                 policy.SERVER_CORE + "backend/cache/CachedSchemaTransaction.java",
                 policy.SERVER_CORE + "backend/cache/CachedSchemaTransactionV2.java"]
        for path in paths:
            with self.subTest(path=path):
                self.assertTrue(self.plan_for([path])["security"])
        for path in [policy.SERVER_CORE + "api/Author.java", "hugegraph-pd/hg-pd-core/src/main/A.java",
                     "docker/Dockerfile", "helm/templates/service.yaml",
                     policy.SERVER_CORE + "backend/cache/CacheManager.java",
                     policy.SERVER_CORE + "backend/cache/CachedSchemaTransactionTest.java"]:
            self.assertFalse(self.plan_for([path])["security"])

    def test_documentation_and_nested_readmes_skip_every_test_lane(self):
        paths = ["README.md", "CONTRIBUTING.md", "notes.txt", "LICENSE", "NOTICE", "docs/ci.md",
                 ".github/outdated/APACHE_PROPOSAL.md", "hugegraph-pd/design.md",
                 "hugegraph-pd/docs/raft.md", "hugegraph-store/docs/guide.txt", "hugegraph-server/hugegraph-hstore/README.md",
                 "hugegraph-server/nested/component/README.md", "hugegraph-pd/hg-pd-core/AGENTS.md", "docker/README.md"]
        for event_name, event in [("pull_request", {}), ("push", {"ref": "refs/heads/master"})]:
            plan = self.plan_for(paths, event_name, event)
            self.assertEqual([], plan["selected"])
            self.assertEqual([], plan["expected"])
            self.assertFalse(any(plan[module] for module in policy.MODULES["server"]))
        for path in ["hugegraph-server/hugegraph-test/src/test/resources/README.md", "docs/example.java",
                     "docs/config.yml", "hugegraph-pd/docs/fixtures/example.txt", "helm/templates/README.md",
                     "hugegraph-store/hg-store-dist/src/assembly/static/conf/startup.txt"]:
            with self.subTest(path=path):
                self.assertTrue(policy.select("server", [path]))

    def test_master_push_runs_smoke_without_repeated_full_suites(self):
        event = {"before": "base", "ref": "refs/heads/master"}
        generic = self.plan_for(["hugegraph-server/hugegraph-core/src/main/A.java"], "push", event)
        self.assertEqual({"macos", "distributed_smoke"}, set(generic["selected"]))
        pd = self.plan_for(["hugegraph-pd/hg-pd-core/src/main/A.java"], "push", event)
        self.assertEqual({"distributed_smoke"}, set(pd["selected"]))
        for path in ["hugegraph-server/hugegraph-hbase/src/main/A.java", ".github/workflows/riscv64-ci.yml",
                     ".github/workflows/codeql-analysis.yml"]:
            self.assertEqual([], self.plan_for([path], "push", event)["expected"])
        native = self.plan_for(["hugegraph-server/hugegraph-rocksdb/src/main/A.java"], "push", event)
        self.assertTrue(native["macos"])
        self.assertFalse(native["macos_full"])
        self.assertTrue(self.plan_for(["docker/Dockerfile"], "push", event)["docker"])

    def test_manual_presets_weekly_and_release_branches(self):
        for preset, selected in policy.PRESETS.items():
            with self.subTest(preset=preset):
                plan = self.plan_for([], "workflow_dispatch", {"inputs": {"test-set": preset}})
                self.assertEqual(selected, set(plan["selected"]))
        default = self.plan_for([], "workflow_dispatch")
        self.assertEqual(policy.MAINLINE, set(default["selected"]))
        self.assertFalse(default["macos_full"])
        weekly = self.plan_for([], "schedule")
        self.assertEqual(set(policy.MODULES["server"]), set(weekly["selected"]))
        self.assertTrue(weekly["macos_full"])
        for branch in ["release-1.7", "test-ci", "tinkerpop-3.8"]:
            plan = self.plan_for(["hugegraph-server/hugegraph-core/src/main/A.java"], "push",
                                 {"before": "base", "ref": "refs/heads/" + branch})
            self.assertTrue(plan["server"])
            self.assertFalse(plan["hbase"])
            self.assertFalse(plan["riscv64"])

    def test_unknown_inputs_fall_back_to_mainline_without_expensive_noncore(self):
        for path in [".github/scripts/ci-policy.py", ".mvn/maven.config", "unknown"]:
            self.assertEqual(policy.MAINLINE, policy.select("server", [path]))
        with patch.object(policy, "git", side_effect=lambda *args: "merge" if args[0] == "rev-parse" else
                          (_ for _ in ()).throw(subprocess.CalledProcessError(1, "git"))):
            plan = policy.create_plan("server", {"before": "base"}, "apache/server", event_name="push")
        self.assertEqual(policy.MAINLINE, set(plan["selected"]))
        self.assertFalse(plan["macos_full"])
        self.assertFalse(plan["hbase"])
        self.assertFalse(plan["riscv64"])

    def test_deployment_and_specific_workflows_keep_specific_owners(self):
        for path in [".github/workflows/docker-build-ci.yml", ".github/scripts/check-docker-images.sh",
                     ".github/scripts/docker-deployment.py", ".github/scripts/test_docker_deployment.py",
                     "hugegraph-server/hugegraph-dist/docker/Dockerfile"]:
            self.assertEqual({"docker"}, policy.select("server", [path]))
        for path in [".asf.yaml", ".github/dependabot.yml", ".editorconfig"]:
            self.assertEqual(set(), policy.select("server", [path]))
        for module in ["pd", "store"]:
            selected = policy.select("server", [f"hugegraph-{module}/hg-{module}-dist/src/assembly/static/bin/start.sh"])
            self.assertTrue({module, "hstore", "cluster", "docker", "distributed_smoke"}.issubset(selected))
            self.assertNotIn("server", selected)
        version = "hugegraph-commons/hugegraph-common/src/main/resources/version.properties"
        self.assertTrue({"commons", "docker"}.issubset(policy.select("server", [version])))

    def test_toolchain_selection_and_fixture_gate_contract_remain_compatible(self):
        quiet = patch("builtins.print")
        quiet.start()
        self.addCleanup(quiet.stop)
        self.assertEqual({"client", "loader", "tools", "spark", "hubble"}, policy.select(
            "toolchain", ["hugegraph-client/src/A.java"]))
        self.assertEqual({"loader", "hubble"}, policy.select("toolchain", ["hugegraph-loader/src/A.java"]))
        self.assertEqual(set(policy.MODULES["toolchain"]), policy.select("toolchain", ["hugegraph-client/api.proto"]))
        self.assertIn("go", policy.select("toolchain", ["hugegraph-client/assembly/travis/start-hugegraph-servers.sh"]))
        plan = {"schema": 1, "repository": "apache/toolchain", "project": "toolchain", "pr": 0,
                "source": "apache/toolchain", "branch": "master", "base": "base", "head": "head",
                "testedMergeSHA": "head", "selected": ["client"], "expected": ["client"]}
        results = {"plan": {"result": "success"}, "client": {"result": "success"}, "fixture": {"result": "success"}}
        self.assertEqual(["client"], policy.gate(plan, results, lambda _: self.fail("No post-run API query"))["executed"])
        for state in ["failure", "cancelled", "skipped", None]:
            for job in ["plan", "client", "fixture"]:
                with self.subTest(state=state, job=job), self.assertRaises(policy.GateFailure):
                    policy.gate(plan, dict(results, **{job: {"result": state}}))

    def test_toolchain_gate_keeps_summary_current_pr_and_cli_all(self):
        plan = {"schema": 1, "repository": "apache/toolchain", "project": "toolchain", "pr": 7,
                "source": "alice/toolchain", "branch": "feature", "base": "base", "head": "head",
                "testedMergeSHA": "merge", "selected": ["client"], "expected": ["client"]}
        live = {"state": "open", "head": {"sha": "head", "ref": "feature", "repo": {"full_name": "alice/toolchain"}},
                "base": {"repo": {"full_name": "apache/toolchain"}}}
        results = {"plan": {"result": "success"}, "client": {"result": "success"}, "fixture": {"result": "success"}}
        with tempfile.TemporaryDirectory() as directory, patch("builtins.print"):
            root = Path(directory)
            summary = root / "summary.md"
            with patch.dict(os.environ, {"GITHUB_STEP_SUMMARY": str(summary)}):
                policy.gate(plan, results, lambda _: live)
            self.assertIn("| client | yes | advisory | affected inputs | success |", summary.read_text())
            with self.assertRaises(policy.StaleInputError):
                policy.gate(plan, results, lambda _: dict(live, head=dict(live["head"], sha="new-head")))
            (root / "plan.json").write_text(json.dumps(dict(plan, pr=0)))
            (root / "results.json").write_text(json.dumps(results))
            outcome = subprocess.run(["python3", str(Path(policy.__file__).resolve()), "gate", "--mode", "all",
                                      "--plan", str(root / "plan.json"), "--results", str(root / "results.json"),
                                      "--output", str(root / "report.json")], capture_output=True, text=True)
            self.assertEqual(0, outcome.returncode, outcome.stderr)
            self.assertEqual(["client"], json.loads((root / "report.json").read_text())["executed"])

    def test_summary_has_only_advisory_checks(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(
                os.environ, {"GITHUB_STEP_SUMMARY": str(Path(directory) / "summary.md")}):
            plan = self.plan_for(["pom.xml"])
            summary = (Path(directory) / "summary.md").read_text()
            self.assertIn("| server_memory | yes | advisory |", summary)
            self.assertNotIn("| required |", summary)
            self.assertEqual([], plan["required"])
            self.assertEqual(1, summary.count("| dependency_license |"))

    def coordinator_jobs(self, plan, selection="success", java="success", result="success", cancelled=False):
        workflow = Path(__file__).resolve().parents[1] / "workflows/server-ci.yml"
        text = workflow.read_text().split("\njobs:\n", 1)[1]
        jobs = {}
        for match in re.finditer(r"^  ([\w-]+):\n(.*?)(?=^  [\w-]+:\n|\Z)", text, re.M | re.S):
            name, body = match.groups()
            if name == "preflight":
                continue
            condition = re.search(r"^    if: \$\{\{ (.+) \}\}$", body, re.M)
            self.assertIsNotNone(condition, "Caller needs an explicit cancellation/selection condition: " + name)
            expression = condition.group(1)
            # GitHub otherwise injects success(), which would hide tests after a license failure.
            self.assertIn("cancelled()", expression)
            outputs = {key: str(value).lower() for key, value in plan.items() if isinstance(value, bool)}
            outputs.update(selection_result=selection, java_result=java)
            expression = re.sub(r"needs\.preflight\.outputs\.([\w]+)",
                                lambda match: repr(outputs.get(match.group(1), "")), expression)
            expression = expression.replace("needs.preflight.result", repr(result))
            expression = expression.replace("!cancelled()", "not cancelled")
            expression = expression.replace("&&", "and").replace("||", "or")
            jobs[name] = bool(eval(expression, {"__builtins__": {}}, {"cancelled": cancelled}))
        return {job for job, run in jobs.items() if run}

    def test_real_coordinator_keeps_selected_jobs_after_license_or_selftest_failure(self):
        workflow = (Path(__file__).resolve().parents[1] / "workflows/server-ci.yml").read_text()
        preflight = re.search(r"^  preflight:\n(.*?)(?=^  [\w-]+:|\Z)", workflow, re.M | re.S).group(1)
        self.assertEqual("check-license", re.search(r"^    name: (.+)$", preflight, re.M).group(1))
        full = self.plan_for([], "workflow_dispatch", {"inputs": {"test-set": "full"}})
        expected = {"build-server", "server-rocksdb", "commons", "pd_store", "cluster", "docker", "helm",
                    "dependency_license", "compatibility", "codeql"}
        self.assertEqual(expected, self.coordinator_jobs(full))
        self.assertEqual(expected, self.coordinator_jobs(full, result="failure"))
        docs = self.plan_for(["hugegraph-pd/docs/guide.txt", "docker/README.md"])
        self.assertEqual(set(), self.coordinator_jobs(docs, result="failure"))
        source = self.plan_for(["hugegraph-server/hugegraph-core/src/main/A.java"])
        self.assertEqual({"build-server", "server-rocksdb", "compatibility"}, self.coordinator_jobs(source))
        master = self.plan_for(["hugegraph-server/hugegraph-core/src/main/A.java"], "push",
                               {"before": "base", "ref": "refs/heads/master"})
        self.assertEqual({"docker", "compatibility"}, self.coordinator_jobs(master))

    def test_real_coordinator_blocks_selection_runtime_failure_and_cancellation(self):
        full = self.plan_for([], "schedule")
        for state in ["failure", "cancelled", "skipped", ""]:
            with self.subTest(selection=state):
                self.assertEqual(set(), self.coordinator_jobs(full, selection=state, result="failure"))
            with self.subTest(java=state):
                self.assertEqual({"helm"}, self.coordinator_jobs(full, java=state, result="failure"))
        self.assertEqual(set(), self.coordinator_jobs(full, cancelled=True))

    def test_contract_scripts_reach_their_actual_inner_jobs_and_steps(self):
        workflows = Path(__file__).resolve().parents[1] / "workflows"
        cases = [("test-stop-hugegraph-store.sh", {"struct"}),
                 ("test-check-java-runtime-logs.sh", {"struct"}),
                 ("test-check-jacoco-report.sh", {"struct"}),
                 ("test-codecov-upload-config.sh", {"struct"}),
                 ("test-riscv64-java-runtime.sh", {"server", "riscv64"})]
        for name, suites in cases:
            script = policy.TRAVIS + name
            with self.subTest(script=script):
                plan = self.plan_for([script])
                self.assertEqual(suites, set(plan["selected"]))
                callers = self.coordinator_jobs(plan)
                if "struct" in suites:
                    self.assertEqual({"pd_store"}, callers)
                    root = (workflows / "server-ci.yml").read_text()
                    caller = re.search(r"^  pd_store:\n(.*?)(?=^  [\w-]+:|\Z)", root, re.M | re.S).group(1)
                    inputs = {}
                    for key in ["struct", "store"]:
                        binding = re.search(r"^      run-" + key + r": \$\{\{ (.+) \}\}$", caller, re.M).group(1)
                        binding = binding.replace("needs.preflight.outputs." + key, repr(str(plan[key]).lower()))
                        inputs[key] = eval(binding, {"__builtins__": {}})
                    workflow = (workflows / "pd-store-ci.yml").read_text()
                    struct = workflow.split("\n  struct:\n", 1)[1].split("\n  pd:\n", 1)[0]
                    condition = re.search(r"^    if: (.+)$", struct, re.M).group(1)
                    condition = condition.replace("inputs.run-struct", str(inputs["struct"]))
                    condition = condition.replace("inputs.run-store", str(inputs["store"]))
                    self.assertTrue(eval(condition.replace("||", "or"), {"__builtins__": {}}))
                    step = next(step for step in struct.split("\n      - ") if Path(script).name in step)
                    self.assertNotRegex(step, r"(?m)^        if:")
                else:
                    self.assertIn("server-rocksdb", callers)
                    workflow = (workflows / "server-ci.yml").read_text()
                    # Read the backend from the actual caller rather than assuming a preset.
                    caller = re.search(r"^  server-rocksdb:\n(.*?)(?=^  [\w-]+:|\Z)",
                                       workflow, re.M | re.S).group(1)
                    backend = re.search(r"^      backend: (\w+)$", caller, re.M).group(1)
                    inner = (workflows / "server-tests.yml").read_text()
                    step = next(step for step in inner.split("\n      - ") if Path(script).name in step)
                    condition = re.search(r"^        if: (.+)$", step, re.M).group(1)
                    self.assertTrue(eval(condition.replace("inputs.backend", repr(backend)),
                                         {"__builtins__": {}}))
        mixed = self.plan_for([policy.TRAVIS + "test-stop-hugegraph-store.sh", "hugegraph-pd/src/A.java"])
        self.assertTrue({"struct", "pd", "store", "hstore", "cluster"}.issubset(mixed["selected"]))
        checker = self.plan_for([policy.TRAVIS + "check-java-runtime-logs.sh"])
        self.assertEqual({"build-server", "server-rocksdb", "commons", "pd_store", "cluster", "compatibility"},
                         self.coordinator_jobs(checker))
        self.assertTrue(checker["hbase"] and checker["riscv64"] and checker["macos_full"])
        master = {"ref": "refs/heads/master"}
        for script in policy.CONTRACT_SUITES:
            self.assertEqual([], self.plan_for([script], "push", master)["expected"])
        runtime = "hugegraph-server/hugegraph-core/src/main/A.java"
        self.assertEqual(self.plan_for([runtime], "push", master)["selected"],
                         self.plan_for([runtime, policy.TRAVIS + "check-java-runtime-logs.sh"],
                                       "push", master)["selected"])

    def test_coordinator_reads_and_validates_real_java_input(self):
        workflow = (Path(__file__).resolve().parents[1] / "workflows/server-ci.yml").read_text()
        code = workflow.split("python3 - <<'PYTHON'\n", 1)[1].split("\n          PYTHON", 1)[0]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            java = root / ".github/workflows/.java-version"
            java.parent.mkdir(parents=True)
            output = root / "outputs.txt"
            for version in ["17", "17.0.20", "21.0.1", "", "bad", '17\n21']:
                java.write_text(version)
                result = subprocess.run(["python3", "-c", textwrap.dedent(code)], cwd=root,
                                        env={**os.environ, "GITHUB_OUTPUT": str(output)}, capture_output=True, text=True)
                with self.subTest(version=version):
                    self.assertEqual(version in {"17", "17.0.20", "21.0.1"}, result.returncode == 0, result.stderr)
            self.assertEqual(['versions=["17"]', 'versions=["17.0.20"]', 'versions=["21.0.1"]'],
                             output.read_text().splitlines())

    def test_real_git_document_modes_renames_and_cli_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = GitFixture(root)
            base = fixture.commit("README.md", "base", "base")
            fixture.commit("README.md", "docs", "docs")
            self.assertEqual([], fixture.plan({"before": base, "ref": "refs/heads/master"})["expected"])
            (root / "README.md").chmod(0o755)
            fixture.git("add", ".")
            fixture.git("commit", "-qm", "executable prose")
            unsafe = fixture.plan({"before": base, "ref": "refs/heads/feature"})
            self.assertEqual(policy.MAINLINE, set(unsafe["selected"]))
            (root / "README.md").unlink()
            (root / "README.md").symlink_to("target.txt")
            fixture.git("add", ".")
            fixture.git("commit", "-qm", "symlink prose")
            self.assertEqual(policy.MAINLINE, set(fixture.plan({"before": base})["selected"]))
            before = fixture.commit("hugegraph-server/A.java", "source", "source")
            (root / "docs").mkdir()
            fixture.git("mv", "hugegraph-server/A.java", "docs/renamed.md")
            fixture.git("commit", "-qm", "rename source to prose")
            self.assertTrue(fixture.plan({"before": before})["server"])
            event = root / "event.json"
            event.write_text("{}")
            output = root / "outputs.txt"
            subprocess.run(["python3", str(Path(policy.__file__).resolve()), "plan", "--project", "server",
                            "--repository", "apache/server", "--event-path", str(event), "--event-name", "schedule",
                            "--output", str(root / "plan.json")], cwd=root, check=True,
                           env={**os.environ, "GITHUB_OUTPUT": str(output)})
            self.assertIn("macos_full=true\n", output.read_text())
            self.assertIn("pd_store=true\n", output.read_text())
            self.assertNotIn("compatibility=", output.read_text())
            self.assertEqual(set(policy.MODULES["server"]), set(json.loads((root / "plan.json").read_text())["selected"]))

    def test_cumulative_pr_snapshot_keeps_source_after_document_commit_and_checks_current_head(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = GitFixture(Path(directory))
            base = fixture.commit("README.md", "base", "base")
            fixture.git("checkout", "-qb", "feature")
            fixture.commit("hugegraph-server/A.java", "source", "source")
            head = fixture.commit("README.md", "docs after source", "docs")
            merge = fixture.git("commit-tree", fixture.git("rev-parse", "HEAD^{tree}"),
                                "-p", base, "-p", head, "-m", "PR merge")
            fixture.git("checkout", "--detach", "-q", merge)
            live = {"number": 7, "state": "open", "head": {"sha": head, "ref": "feature",
                    "repo": {"full_name": "alice/server"}}, "base": {"sha": base, "repo": {"full_name": "apache/server"}}}
            calls = []
            def fetch(path):
                calls.append(path)
                return live
            plan = fixture.plan({"pull_request": live}, fetch)
            self.assertEqual(["repos/apache/server/pulls/7"], calls)
            self.assertTrue(plan["server"])
            self.assertFalse(plan["pd_store"])
            self.assertFalse(plan["security"])
            self.assertIn("hugegraph-server/A.java", plan["changedPaths"])
            for section, field, value in [("head", "sha", "new-head"), ("head", "ref", "other"),
                                          ("head", "repo", {"full_name": "other/fork"}),
                                          ("base", "repo", {"full_name": "other/target"})]:
                changed = dict(live, **{section: dict(live[section], **{field: value})})
                with self.subTest(section=section, field=field), self.assertRaises(policy.StaleInputError):
                    fixture.plan({"pull_request": live}, lambda _: changed)
            with self.assertRaises(policy.StaleInputError):
                fixture.plan({"pull_request": live}, lambda _: dict(live, state="closed"))
            fixture.git("checkout", "--detach", "-q", head)
            with self.assertRaises(policy.StaleInputError):
                fixture.plan({"pull_request": live}, fetch)

    def test_queued_merge_uses_actual_target_parent_and_api_outage_keeps_pr_identity(self):
        live = {"number": 7, "state": "open", "head": {"sha": "head", "ref": "feature",
                "repo": {"full_name": "alice/server"}}, "base": {"sha": "event-base", "repo": {"full_name": "apache/server"}}}
        def git(*args):
            if args[0] == "show":
                return "current-base head"
            if args[0] == "diff":
                return "README.md"
            return "merge"
        with patch.dict(os.environ, {"GITHUB_SHA": "merge", "GITHUB_EVENT_NAME": ""}), patch.object(
                policy, "git", side_effect=git), patch.object(policy, "unsafe_documentation", return_value=False):
            plan = policy.create_plan("server", {"pull_request": live}, "apache/server", lambda _: live)
            self.assertEqual("current-base", plan["base"])
            self.assertEqual([], plan["expected"])
            def unavailable(_):
                raise subprocess.CalledProcessError(1, "gh")
            fallback = policy.create_plan("server", {"pull_request": live}, "apache/server", unavailable)
            self.assertEqual(policy.MAINLINE, set(fallback["selected"]))
            self.assertEqual((7, "alice/server", "feature", "current-base", "head"),
                             tuple(fallback[key] for key in ["pr", "source", "branch", "base", "head"]))
            for malformed in [{"pull_request": None}, {"pull_request": {}},
                              {"pull_request": dict(live, head=dict(live["head"], repo=None))}]:
                with self.subTest(event=malformed), self.assertRaises(policy.StaleInputError):
                    policy.create_plan("server", malformed, "apache/server", lambda _: self.fail("Invalid identity"))


if __name__ == "__main__":
    unittest.main()
