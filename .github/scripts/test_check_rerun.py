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

"""Behavioral tests for retry freshness, without credentials or network."""
import copy
import io
import json
import zipfile
import importlib.util
from pathlib import Path
import unittest
import os
import subprocess
import tempfile
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("check_rerun", Path(__file__).with_name("check-rerun.py"))
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class FreshnessTest(unittest.TestCase):
    def setUp(self):
        self.repository = "hugegraph/hugegraph-toolchain"
        self.run = {"status": "completed", "conclusion": "failure", "run_attempt": 1,
                    "event": "pull_request", "head_sha": "old", "head_branch": "feature",
                    "head_repository": {"full_name": "alice/toolchain", "owner": {"login": "alice"}}}
        self.pr = {"number": 7, "state": "open", "head": {"sha": "old", "ref": "feature", "repo": {"full_name": "alice/toolchain", "owner": {"login": "alice"}}},
                   "base": {"sha": "base", "repo": {"full_name": self.repository}}}
        self.run["pull_requests"] = [copy.deepcopy(self.pr)]
        self.calls = []

    def fetch(self, path, binary=False):
        if "/artifacts?" in path:
            return {"artifacts": [{"name": "ci-plan", "id": 99, "expired": False}]}
        if "/actions/artifacts/" in path:
            data = io.BytesIO()
            with zipfile.ZipFile(data, "w") as archive:
                archive.writestr("plan.json", json.dumps({"repository": self.repository, "pr": 7,
                                                         "head": "old", "base": "base", "testedMergeSHA": "merge"}))
            return data.getvalue()
        if "/git/commits/" in path:
            return {"parents": [{"sha": "base"}, {"sha": "old"}]}
        self.calls.append(path)
        if "/actions/runs/" in path:
            return self.run
        if "/commits/" in path:
            return [self.pr]
        if "/commits?" in path:
            return [{"sha": self.pr["head"]["sha"]}]
        if "/pulls?" in path:
            return [self.pr]
        if "/pulls/" in path:
            return self.pr
        raise AssertionError(path)

    def decide(self):
        return checker.decide(self.repository, 42, 1, 2, self.fetch)[0]

    def test_current_open_pr(self):
        self.assertEqual("rerun", self.decide())

    def test_slim_workflow_run_pr_association(self):
        self.run["pull_requests"] = [{"number": 7, "base": {"repo": {"id": 1, "name": "hugegraph-toolchain",
                                                                   "url": f"https://api.github.com/repos/{self.repository}"}}}]
        self.assertEqual("rerun", self.decide())

    def test_api_failure_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            with patch.dict(os.environ, {"GITHUB_OUTPUT": str(output)}), patch(
                    "sys.argv", ["check-rerun.py", "--repository", self.repository,
                                 "--run-id", "42", "--run-attempt", "1", "--max-reruns", "2"]), patch.object(
                    checker, "decide", side_effect=subprocess.CalledProcessError(1, "gh")):
                checker.main()
            self.assertIn("action=skip\n", output.read_text())

    def test_closed_pr(self):
        self.pr["state"] = "closed"
        self.assertEqual("skip", self.decide())

    def test_updated_pr(self):
        self.pr["head"]["sha"] = "new"
        self.assertEqual("skip", self.decide())

    def test_empty_association_falls_back_to_commit(self):
        self.run["pull_requests"] = []
        self.assertEqual("rerun", self.decide())
        self.assertTrue(any("/commits/old/pulls" in p for p in self.calls))

    def test_fork_empty_associations_owner_qualified_fallback(self):
        self.run.update(pull_requests=[], head_repository={"full_name": "alice/toolchain", "owner": {"login": "alice"}})
        self.pr["head"].update(ref="feature", repo={"full_name": "alice/toolchain", "owner": {"login": "alice"}})
        def fetch(path, **kwargs):
            if "/commits/old/pulls" in path:
                return []
            if "/pulls?" in path:
                self.assertIn("head=alice%3Afeature", path)
                return [self.pr]
            return self.fetch(path, **kwargs)
        self.assertEqual("rerun", checker.decide(self.repository, 42, 1, 2, fetch)[0])
        self.pr["head"]["repo"]["owner"]["login"] = "bob"
        self.assertEqual("skip", checker.decide(self.repository, 42, 1, 2, fetch)[0])

    def test_other_repository_pr_is_ignored(self):
        self.run["pull_requests"][0]["base"]["repo"]["full_name"] = "apache/hugegraph-toolchain"
        self.assertEqual("rerun", self.decide())
        self.assertTrue(any("/commits/old/pulls" in p for p in self.calls))

    def test_foreign_pr_fallback_still_rejects_foreign_pr(self):
        self.run["pull_requests"][0]["base"]["repo"]["full_name"] = "apache/hugegraph-toolchain"
        self.pr["base"]["repo"]["full_name"] = "apache/hugegraph-toolchain"
        self.assertEqual("skip", self.decide())

    def test_same_head_moved_base_is_stale(self):
        self.pr["base"]["sha"] = "new-base"
        self.assertEqual("skip", self.decide())

    def test_missing_immutable_plan_refuses_retry(self):
        def fetch(path, **kwargs):
            if "/artifacts?" in path:
                return {"artifacts": []}
            return self.fetch(path, **kwargs)
        self.assertEqual("skip", checker.decide(self.repository, 42, 1, 2, fetch)[0])

    def test_same_sha_other_fork_cannot_authorize_retry(self):
        self.pr["head"]["repo"].update(full_name="bob/toolchain", owner={"login": "bob"})
        self.assertEqual("skip", self.decide())
        self.pr["head"]["repo"].update(full_name="alice/toolchain", owner={"login": "alice"})
        self.pr["head"]["ref"] = "other"
        self.assertEqual("skip", self.decide())

    def test_changed_attempt(self):
        self.run["run_attempt"] = 2
        self.assertEqual("skip", self.decide())

    def test_current_run_must_remain_failed_and_completed(self):
        for key, value in (("status", "in_progress"), ("conclusion", "success"),
                           ("conclusion", "cancelled")):
            with self.subTest(key=key, value=value):
                old = self.run[key]
                self.run[key] = value
                self.assertEqual("skip", self.decide())
                self.run[key] = old

    def test_retry_limit(self):
        self.run["run_attempt"] = 3
        self.assertEqual("skip", checker.decide(self.repository, 42, 3, 2, self.fetch)[0])

    def test_push_checks_repository_and_encodes_branch(self):
        self.run.update(event="push", head_branch="release/test")
        self.assertEqual("rerun", self.decide())
        self.assertIn(f"repos/{self.repository}/commits?sha=release%2Ftest&per_page=1", self.calls)
        self.pr["head"]["sha"] = "new"
        self.assertEqual("skip", self.decide())

    def test_unsupported_event(self):
        self.run["event"] = "workflow_dispatch"
        self.assertEqual("skip", self.decide())

    def test_missing_metadata(self):
        self.run["head_sha"] = None
        self.assertEqual("skip", self.decide())


if __name__ == "__main__":
    unittest.main()
