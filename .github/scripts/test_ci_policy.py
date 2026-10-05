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
        self.assertEqual({"server", "cluster"}, policy.select("server", ["hugegraph-server/A.java"]))

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
        self.assertNotIn("hstore", policy.select("server", ["hugegraph-server/hugegraph-rocksdb/src/main/A.java"]))

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
        with patch.object(policy, "unsafe_documentation", return_value=False), patch.object(policy, "git", side_effect=git), patch.object(policy, "fingerprint", return_value="hash"), patch.object(
                policy, "find_reuse", return_value={m: {"runID": 42} for m in ["client", "loader", "tools", "spark", "hubble"]}):
            plan = policy.create_plan("toolchain", event, "apache/t", lambda p: live,
                                      external_inputs={"serverSHA": "immutable"})
            self.assertFalse(plan["client"])
            self.assertFalse(plan["needsFixture"])
            self.assertTrue(plan["security"])
        with patch.object(policy, "unsafe_documentation", return_value=False), patch.object(policy, "git", side_effect=git), patch.object(policy, "fingerprint", return_value="hash"), patch.object(
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
