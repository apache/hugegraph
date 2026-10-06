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

"""Behavioral tests for push retry freshness, without credentials or network."""
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
        self.repository = "apache/hugegraph"
        self.run = {"status": "completed", "conclusion": "failure", "run_attempt": 1,
                    "path": ".github/workflows/server-memory-ci.yml", "event": "push", "head_sha": "old", "head_branch": "release/test",
                    "repository": {"full_name": self.repository},
                    "head_repository": {"full_name": self.repository}}
        self.current_head = "old"
        self.calls = []

    def fetch(self, path):
        self.calls.append(path)
        if path == f"repos/{self.repository}/actions/runs/42":
            return self.run
        if path == f"repos/{self.repository}/commits?sha=release%2Ftest&per_page=1":
            return [{"sha": self.current_head}] if self.current_head else []
        raise AssertionError("Unexpected lookup: " + path)

    def decide(self):
        return checker.decide(self.repository, 42, 1, 2, self.fetch)[0]

    def test_only_required_workflows_retry(self):
        for path in [".github/workflows/server-ci.yml", ".github/workflows/codeql-analysis.yml",
                     ".github/workflows/server-compatibility-ci.yml"]:
            self.run["path"] = path
            self.assertEqual("skip", self.decide())
        self.run["path"] = ".github/workflows/licence-checker.yml"
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

    def test_pr_is_paused_without_reading_artifacts_or_candidates(self):
        self.run.update(event="pull_request", head_repository={"full_name": "alice/hugegraph"})
        self.assertEqual("skip", self.decide())
        self.assertEqual([f"repos/{self.repository}/actions/runs/42"], self.calls)

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
        self.run["run_attempt"] = 2
        self.assertEqual("rerun", checker.decide(self.repository, 42, 2, 2, self.fetch)[0])

    def test_push_checks_repository_and_encodes_branch(self):
        self.assertEqual("rerun", self.decide())
        self.assertIn(f"repos/{self.repository}/commits?sha=release%2Ftest&per_page=1", self.calls)
        self.current_head = "new"
        self.assertEqual("skip", self.decide())
        self.current_head = None
        self.assertEqual("skip", self.decide())

    def test_push_rejects_other_repository(self):
        for field in ["repository", "head_repository"]:
            with self.subTest(field=field):
                self.run[field] = {"full_name": "alice/hugegraph"}
                self.assertEqual("skip", self.decide())
                self.run[field] = {"full_name": self.repository}

    def test_missing_metadata_and_unsupported_event(self):
        for field in ["head_sha", "head_branch", "repository", "head_repository"]:
            old = self.run.pop(field)
            with self.subTest(field=field):
                self.assertEqual("skip", self.decide())
            self.run[field] = old
        self.run["event"] = "workflow_dispatch"
        self.assertEqual("skip", self.decide())


if __name__ == "__main__":
    unittest.main()
