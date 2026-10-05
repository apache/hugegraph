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

"""Executable Git fixtures and API proof tests for CI selection."""
import hashlib
import importlib.util
import io
import zipfile
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
    def setUp(self):
        mock = patch.object(policy, "remote_fingerprint", side_effect=lambda *a, **k: "policy" if k.get("policy_only") else "inputs")
        mock.start()
        self.addCleanup(mock.stop)

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

    def test_version_resource_follows_real_docker_consumer(self):
        root = Path(__file__).resolve().parents[2]
        consumer = root / ".github/scripts/check-docker-images.sh"
        if not consumer.exists():
            self.skipTest("Server Docker consumer is not in the Toolchain repository")
        resource = next(line.strip().rstrip(")") for line in consumer.read_text().splitlines()
                        if line.strip().startswith("hugegraph-commons/") and "version.properties" in line)
        self.assertTrue((root / resource).is_file())
        self.assertTrue({"commons", "docker"}.issubset(policy.select("server", [resource])))

    def test_external_hubble_baseline_only_affects_hubble(self):
        a = {"server": {"sha": "release"}, "hubble": {"sha": "old"}}
        b = {"server": {"sha": "release"}, "hubble": {"sha": "new"}}
        self.assertEqual(policy.external_context(a, ["go"]), policy.external_context(b, ["go"]))
        self.assertNotEqual(policy.external_context(a, ["hubble"]), policy.external_context(b, ["hubble"]))

    def test_packaged_readme_content_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args):
                return subprocess.check_output(["git", *args], cwd=root, text=True).strip()
            git("init", "-q")
            git("config", "user.email", "ci@example.invalid")
            git("config", "user.name", "CI")
            (root / "hugegraph-hubble").mkdir()
            readme = root / "hugegraph-hubble/README.md"
            readme.write_text("nonempty docs")
            git("add", ".")
            git("commit", "-qm", "valid readme")
            old = os.getcwd()
            try:
                os.chdir(root)
                initial = policy.fingerprint("HEAD", project="toolchain")
                readme.write_text("changed nonempty docs")
                git("commit", "-qam", "valid docs edit")
                self.assertEqual(initial, policy.fingerprint("HEAD", project="toolchain"))
                for content in ["", "  \t\n"]:
                    readme.write_text(content)
                    git("commit", "-qam", "empty docs")
                    self.assertNotEqual("valid", policy.packaged_readme_state("HEAD"))
                    self.assertNotEqual(initial, policy.fingerprint("HEAD", project="toolchain"))
                readme.write_text("nonempty")
                readme.chmod(0o755)
                git("add", ".")
                git("commit", "-qm", "executable docs")
                self.assertNotEqual("valid", policy.packaged_readme_state("HEAD"))
                readme.unlink()
                readme.symlink_to("missing-target")
                git("add", ".")
                git("commit", "-qm", "symlink docs")
                self.assertNotEqual("valid", policy.packaged_readme_state("HEAD"))
                readme.unlink()
                git("add", ".")
                git("commit", "-qm", "deleted docs")
                self.assertEqual("missing", policy.packaged_readme_state("HEAD"))
                self.assertNotEqual(initial, policy.fingerprint("HEAD", project="toolchain"))
            finally:
                os.chdir(old)

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

    def plan(self):
        return {"schema": 1, "project": "toolchain", "repository": "apache/t", "pr": 7,
                "source": "alice/t", "branch": "feature", "base": "base", "head": "new", "testedMergeSHA": "merge", "inputFingerprint": hashlib.sha256(b"inputs{}").hexdigest(),
                "policyFingerprint": "policy", "workflowID": 99, "expected": ["client"],
                "selected": ["client"], "reused": {}}

    def receipt(self):
        r = self.plan()
        r["proofs"] = {"client": {"runID": 42, "head": "old", "testedMergeSHA": "oldmerge", "jobIDs": [123]}}
        return r

    def fetch(self, path):
        if "/git/commits/" in path:
            return {"parents": [{"sha": "base"}, {"sha": "old"}]}
        if path.endswith("/jobs?per_page=100&page=1"):
            return {"jobs": [{"name": "client / client-ci", "id": 123, "conclusion": "success"},
                             {"name": "affected-module-tests", "id": 124, "conclusion": "success"}]}
        return {"status": "completed", "conclusion": "success", "event": "pull_request",
                "head_sha": "old", "head_branch": "feature", "workflow_id": 99, "head_repository": {"full_name": "alice/t"},
                "pull_requests": [{"number": 7, "base": {"sha": "base"}}]}

    def test_valid_original_proof(self):
        with patch.object(policy, "remote_fingerprint", side_effect=lambda *a, **k: "policy" if k.get("policy_only") else "inputs"):
            self.assertIn("client", policy.validate_receipt(self.plan(), self.receipt(), self.fetch))

    def test_empty_fork_association_owner_branch_fallback(self):
        def fetch(path):
            if "/pulls?" in path:
                self.assertIn("head=alice%3Afeature", path)
                return [{"number": 7, "head": {"ref": "feature", "repo": {"full_name": "alice/t"}},
                         "base": {"repo": {"full_name": "apache/t"}}}]
            value = self.fetch(path)
            if "/actions/runs/" in path and "/jobs?" not in path:
                value.update(pull_requests=[], head_branch="feature",
                             head_repository={"full_name": "alice/t", "owner": {"login": "alice"}})
            return value
        self.assertIn("client", policy.validate_receipt(self.plan(), self.receipt(), fetch))

    def test_nonblocking_failure_keeps_core_proof_but_bad_gate_rejects(self):
        def fetch(path):
            value = self.fetch(path)
            if "/jobs?" not in path and "/actions/runs/" in path:
                value["conclusion"] = "failure"
            return value
        self.assertIn("client", policy.validate_receipt(self.plan(), self.receipt(), fetch))
        def bad_gate(path):
            value = fetch(path)
            if "/jobs?" in path:
                value["jobs"][1]["conclusion"] = "failure"
            return value
        with self.assertRaises(ValueError):
            policy.validate_receipt(self.plan(), self.receipt(), bad_gate)

    def test_artifact_core_reuse_with_failed_compatibility(self):
        receipt = self.receipt()
        receipt["runID"] = 42
        data = io.BytesIO()
        with zipfile.ZipFile(data, "w") as archive:
            archive.writestr("receipt.json", json.dumps(receipt))
        artifact_size = 256
        def fetch(path, binary=False):
            if "/actions/workflows/" in path:
                self.assertIn("status=completed", path)
                return {"workflow_runs": [{"id": 42, "head_repository": {"full_name": "alice/t"}, "head_branch": "feature", "pull_requests": [{"number": 7}]}]}
            if "/artifacts?" in path:
                return {"artifacts": [{"name": "ci-test-receipt", "id": 3, "expired": False, "size_in_bytes": artifact_size}]}
            if "/actions/artifacts/" in path:
                return data.getvalue()
            value = self.fetch(path)
            if "/jobs?" in path:
                value["jobs"].append({"name": "HBase compatibility", "id": 9, "conclusion": "failure"})
            elif "/actions/runs/" in path:
                value["conclusion"] = "failure"
            return value
        plan = self.plan()
        with patch.dict(os.environ, {"GITHUB_RUN_ID": "99"}):
            proof = policy.find_reuse(plan, fetch)
        self.assertIn("client", proof)
        self.assertFalse(plan["optionalSuccess"]["compatibility"])
        self.assertFalse(plan["optionalSuccess"]["security"])
        for artifact_size in [None, 0, 1048577, "256"]:
            with patch.dict(os.environ, {"GITHUB_RUN_ID": "99"}):
                self.assertEqual({}, policy.find_reuse(self.plan(), fetch))
        artifact_size = 256
        data = io.BytesIO()
        with zipfile.ZipFile(data, "w") as archive:
            archive.writestr("receipt.json", json.dumps(receipt))
            archive.writestr("unexpected.txt", "extra")
        with patch.dict(os.environ, {"GITHUB_RUN_ID": "99"}):
            self.assertEqual({}, policy.find_reuse(self.plan(), fetch))

    def optional_fixture(self, receipts, optional_results=None):
        optional_results = optional_results or {}
        def fetch(path, binary=False):
            if "/actions/workflows/" in path:
                return {"workflow_runs": [{"id": run_id, "head_repository": {"full_name": "alice/t"},
                                            "head_branch": "feature", "pull_requests": [{"number": 7}]}
                                           for run_id in sorted(receipts, reverse=True)]}
            if "/artifacts?" in path:
                run_id = int(path.split("/runs/")[1].split("/")[0])
                return {"artifacts": [{"name": "ci-test-receipt", "id": run_id,
                                       "expired": False, "size_in_bytes": 512}]}
            if "/actions/artifacts/" in path:
                run_id = int(path.split("/artifacts/")[1].split("/")[0])
                data = io.BytesIO()
                with zipfile.ZipFile(data, "w") as archive:
                    archive.writestr("receipt.json", json.dumps(receipts[run_id]))
                return data.getvalue()
            if "/git/commits/" in path:
                head = "new" if path.endswith("/merge") else "old"
                return {"parents": [{"sha": "base"}, {"sha": head}]}
            if "/jobs?" in path:
                run_id = int(path.split("/runs/")[1].split("/")[0])
                jobs = [{"name": "affected-module-tests", "id": run_id * 10, "conclusion": "success"}]
                if run_id == 42:
                    jobs.append({"name": "client / client-ci", "id": 123, "conclusion": "success"})
                for index, name in enumerate(sorted(policy.optional_groups(self.plan())["security"])):
                    result = optional_results.get(run_id, "success" if run_id == 42 else "skipped")
                    jobs.append({"name": name, "id": run_id * 100 + index, "conclusion": result})
                return {"jobs": jobs}
            value = self.fetch(path)
            if "/actions/runs/43" in path:
                value["head_sha"] = "new"
            return value
        return fetch

    def find_optional(self, receipts, optional_results=None):
        plan = self.plan()
        with patch.dict(os.environ, {"GITHUB_RUN_ID": "99"}), patch.object(
                policy, "remote_fingerprint", side_effect=lambda *a, **k: "policy" if k.get("policy_only") else "inputs"):
            plan["reused"] = policy.find_reuse(plan, self.optional_fixture(receipts, optional_results))
        return plan

    def test_optional_original_proof_survives_two_documentation_updates(self):
        original = self.receipt()
        original.update(runID=42, head="old", testedMergeSHA="oldmerge")
        second = self.find_optional({42: original})
        self.assertTrue(second["optionalSuccess"]["security"])
        self.assertEqual(42, second["optionalProofs"]["security"]["runID"])
        with patch.dict(os.environ, {"GITHUB_RUN_ID": "43"}):
            forwarded = policy.gate(second, {"plan": {"result": "success"}}, self.fetch)
        third = self.find_optional({43: forwarded, 42: original})
        self.assertTrue(third["optionalSuccess"]["security"])
        self.assertEqual(second["optionalProofs"], third["optionalProofs"])
        self.assertEqual(second["reused"], third["reused"])
        for result in ["failure", "cancelled", "timed_out", None]:
            with self.subTest(result=result):
                latest = self.find_optional({43: forwarded, 42: original}, {43: result})
                self.assertTrue(latest["reused"])
                self.assertFalse(latest["optionalSuccess"]["security"])

    def test_newer_unverifiable_receipt_blocks_older_optional_green(self):
        original = self.receipt()
        original.update(runID=42, head="old", testedMergeSHA="oldmerge")
        invalid = dict(original, runID=43, source="other/t")
        plan = self.find_optional({43: invalid, 42: original})
        self.assertTrue(plan["reused"])
        self.assertFalse(plan["optionalSuccess"]["security"])

    def test_server_optional_matrix_follows_project_runtime(self):
        plan = dict(self.plan(), project="server")
        runtime_file = Path(__file__).resolve().parents[1] / "workflows/.java-version"
        if not runtime_file.exists():
            self.skipTest("Server runtime configuration is not in the Toolchain repository")
        runtime = runtime_file.read_text().strip()
        names = policy.optional_groups(plan)["compatibility"]
        self.assertIn(f"HBase compatibility (Java {runtime})", names)
        self.assertEqual(4, len(names))
        jobs = [{"name": name, "id": index, "conclusion": "success"}
                for index, name in enumerate(sorted(names))]
        self.assertEqual(4, len(policy.optional_jobs(jobs, names)))
        for missing in range(len(jobs)):
            with self.assertRaises(ValueError):
                policy.optional_jobs(jobs[:missing] + jobs[missing + 1:], names)

    def test_optional_forwarded_forgery_and_original_failure_reject(self):
        original = self.receipt()
        original.update(runID=42, head="old", testedMergeSHA="oldmerge")
        second = self.find_optional({42: original})
        with patch.dict(os.environ, {"GITHUB_RUN_ID": "43"}):
            forwarded = policy.gate(second, {"plan": {"result": "success"}}, self.fetch)
        for field in ["jobIDs", "jobNames", "source", "base", "inputFingerprint", "policyFingerprint", "head"]:
            changed = json.loads(json.dumps(forwarded))
            changed["optionalProofs"]["security"][field] = [999] if field == "jobIDs" else "forged"
            with self.subTest(field=field):
                latest = self.find_optional({43: changed, 42: original})
                self.assertTrue(latest["reused"])
                self.assertFalse(latest["optionalSuccess"]["security"])
        latest = self.find_optional({43: forwarded, 42: original}, {42: "failure"})
        self.assertFalse(latest["optionalSuccess"]["security"])

    def test_optional_group_requires_complete_known_matrix(self):
        plan = self.plan()
        original = self.receipt()
        original.update(runID=42, head="old", testedMergeSHA="oldmerge")
        fetch = self.optional_fixture({42: original})
        for mutation in ["missing", "unknown", "duplicate", "api-error"]:
            def bad_fetch(path, binary=False):
                if "/actions/runs/" in path and mutation == "api-error":
                    raise OSError("unavailable")
                value = fetch(path, binary)
                if "/jobs?" in path:
                    if mutation == "missing":
                        value["jobs"].pop()
                    elif mutation == "unknown":
                        value["jobs"].append({"name": "security / Analyze (unknown)", "id": 999, "conclusion": "success"})
                    elif mutation == "duplicate":
                        value["jobs"].append(dict(value["jobs"][-1]))
                return value
            jobs = fetch("repos/apache/t/actions/runs/42/jobs?per_page=100&page=1")["jobs"]
            if mutation != "api-error":
                jobs = bad_fetch("repos/apache/t/actions/runs/42/jobs?per_page=100&page=1")["jobs"]
            with patch.object(policy, "remote_fingerprint", side_effect=lambda *a, **k: "policy" if k.get("policy_only") else "inputs"):
                self.assertEqual({}, policy.optional_proofs(plan, original, jobs, set(), bad_fetch))

    def test_branch_filtered_bounded_run_pagination(self):
        calls = []
        def fetch(path, binary=False):
            if "/actions/workflows/" in path:
                calls.append(path)
                self.assertIn("branch=feature", path)
                if "page=1" in path:
                    return {"workflow_runs": [{"id": n, "head_repository": {"full_name": "alice/t"},
                                                "head_branch": "other"} for n in range(30)]}
                return {"workflow_runs": []}
            return self.fetch(path)
        with patch.dict(os.environ, {"GITHUB_RUN_ID": "99"}):
            self.assertEqual({}, policy.find_reuse(self.plan(), fetch))
        self.assertEqual(2, len(calls))

    def test_tested_merge_parent_mismatch_rejects(self):
        def fetch(path):
            if "/git/commits/" in path:
                return {"parents": [{"sha": "advanced-base"}, {"sha": "old"}]}
            return self.fetch(path)
        with self.assertRaises(ValueError):
            policy.validate_receipt(self.plan(), self.receipt(), fetch)

    def test_actual_tree_fingerprint_mismatch_rejects(self):
        with patch.object(policy, "remote_fingerprint", return_value="forged"):
            with self.assertRaises(ValueError):
                policy.validate_receipt(self.plan(), self.receipt(), self.fetch)

    def test_provenance_changes_refuse_reuse(self):
        for key in ["base", "inputFingerprint", "policyFingerprint", "source", "repository", "pr", "schema"]:
            receipt = self.receipt()
            receipt[key] = "changed"
            with self.subTest(key=key), self.assertRaises(ValueError):
                policy.validate_receipt(self.plan(), receipt, self.fetch)

    def test_original_run_and_actual_job_proof_required(self):
        for failure in ["failure", "cancelled", "skipped"]:
            def fetch(path):
                value = self.fetch(path)
                if "jobs?" in path:
                    value["jobs"][0]["conclusion"] = failure
                return value
            with self.subTest(failure=failure), self.assertRaises(ValueError):
                policy.validate_receipt(self.plan(), self.receipt(), fetch)
        receipt = self.receipt()
        receipt["proofs"]["client"]["jobIDs"] = [456]
        with self.assertRaises(ValueError):
            policy.validate_receipt(self.plan(), receipt, self.fetch)

    def test_gate_rejects_missing_failed_cancelled_skipped(self):
        for result in [None, "failure", "cancelled", "skipped"]:
            with self.subTest(result=result), self.assertRaises(ValueError):
                policy.gate(self.plan(), {"plan": {"result": "success"}, "client": {"result": result}}, self.fetch)

    def test_third_party_failure_does_not_exempt_required_tests(self):
        selected = policy.select("server", ["pom.xml"])
        self.assertIn("dependency_license", selected)
        expected = {"server_memory", "server_rocksdb", "commons", "pd_store", "cluster", "docker", "helm"}
        self.assertEqual(expected, set(policy.suites("server", selected)))
        plan = self.plan()
        plan.update(project="server", selected=sorted(selected), expected=sorted(expected))
        results = {suite: {"result": "success"} for suite in expected}
        results["plan"] = {"result": "success"}
        for result in ["failure", "cancelled", "skipped", None]:
            results["dependency_license"] = {"result": result}
            with self.subTest(third_party=result):
                receipt = policy.gate(plan, results, self.fetch)
                self.assertEqual(expected, set(receipt["executed"]))
                self.assertNotIn("dependency_license", receipt["proofs"])
        for result in ["failure", "cancelled", "skipped", None]:
            results["cluster"] = {"result": result}
            with self.subTest(cluster=result), self.assertRaisesRegex(ValueError, "cluster"):
                policy.gate(plan, results, self.fetch)

    def test_gate_success_receipt_and_verified_reuse(self):
        with patch.dict(os.environ, {"GITHUB_RUN_ID": "42"}):
            result = policy.gate(self.plan(), {"plan": {"result": "success"}, "client": {"result": "success"}, "fixture": {"result": "success"}}, self.fetch)
            self.assertEqual([123], result["proofs"]["client"]["jobIDs"])
            plan = self.plan()
            plan["reused"] = result["proofs"]
            self.assertEqual(["client"], policy.gate(plan, {"plan": {"result": "success"}, "client": {"result": "skipped"}}, self.fetch)["reused"])

    def test_gate_refuses_failed_planner_even_empty_or_reused(self):
        for result in ["failure", "skipped", "cancelled", None]:
            plan = self.plan()
            plan["expected"] = []
            with self.subTest(result=result), self.assertRaises(ValueError):
                policy.gate(plan, {"plan": {"result": result}}, self.fetch)

    def test_fixture_missing_rejects_successful_test(self):
        with self.assertRaises(ValueError):
            policy.gate(self.plan(), {"plan": {"result": "success"}, "client": {"result": "success"}}, self.fetch)
        plan = self.plan()
        plan["expected"] = ["hubble"]
        with self.assertRaises(ValueError):
            policy.gate(plan, {"plan": {"result": "success"}, "hubble": {"result": "success"}}, self.fetch)

    def test_proof_failure_disables_receipt_without_falsifying_gate(self):
        def failure(path):
            raise subprocess.CalledProcessError(1, "gh")
        result = policy.gate(self.plan(), {"plan": {"result": "success"}, "client": {"result": "success"},
                                         "fixture": {"result": "success"}}, failure)
        self.assertEqual({}, result["proofs"])
        self.assertEqual(["client"], result["executed"])

    def test_doc_update_reuses_only_with_valid_receipt(self):
        event = {"pull_request": {"number": 7, "head": {"sha": "new"}, "base": {"sha": "base"}}}
        live = {"state": "open", "head": {"sha": "new", "ref": "feature", "repo": {"full_name": "alice/t"}},
                "base": {"sha": "base", "repo": {"full_name": "apache/t"}}}
        def git(*args):
            if args[0] == "show":
                return "base new"
            if args[0] == "diff":
                return "hugegraph-client/src/A.java\nREADME.md"
            return "new"
        with patch.object(policy, "packaged_readme_state", return_value="valid"), patch.object(policy, "unsafe_documentation", return_value=False), patch.object(policy, "git", side_effect=git), patch.object(policy, "fingerprint", return_value="hash"), patch.object(
                policy, "find_reuse", return_value={m: {"runID": 42} for m in ["client", "loader", "tools", "spark", "hubble"]}):
            plan = policy.create_plan("toolchain", event, "apache/t", lambda p: live,
                                      external_inputs={"serverSHA": "immutable"})
            self.assertFalse(plan["client"])
            self.assertFalse(plan["needsFixture"])
            self.assertTrue(plan["security"])
        with patch.object(policy, "packaged_readme_state", return_value="valid"), patch.object(policy, "unsafe_documentation", return_value=False), patch.object(policy, "git", side_effect=git), patch.object(policy, "fingerprint", return_value="hash"), patch.object(
                policy, "find_reuse", return_value={}):
            plan = policy.create_plan("toolchain", event, "apache/t", lambda p: live,
                                      external_inputs={"serverSHA": "immutable"})
            self.assertTrue(plan["client"])
            self.assertTrue(plan["needsFixture"])

    def test_real_git_fingerprint_and_cli(self):
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
                self.assertEqual(policy.fingerprint(base), policy.fingerprint(head))
                (root / "type.ts").write_text("type changed")
                git("add", ".")
                git("commit", "-qm", "types")
                self.assertNotEqual(policy.fingerprint(base), policy.fingerprint("HEAD"))
                (root / "README.md").chmod(0o755)
                git("add", "README.md")
                git("commit", "-qm", "executable prose")
                self.assertTrue(policy.unsafe_documentation("HEAD", ["README.md"]))
                self.assertNotEqual(policy.fingerprint(base), policy.fingerprint("HEAD"))
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

    def test_api_failure_runs_full_required(self):
        event = {"pull_request": {"number": 7, "head": {"sha": "old"}}}
        with patch.object(policy, "git", return_value="head"):
            def fail(path):
                raise subprocess.CalledProcessError(1, "gh")
            plan = policy.create_plan("toolchain", event, "apache/t", fail)
            self.assertEqual(set(policy.MODULES["toolchain"]), set(plan["selected"]))
            self.assertTrue(plan["security"])
            self.assertEqual({}, plan["reused"])


if __name__ == "__main__":
    unittest.main()
