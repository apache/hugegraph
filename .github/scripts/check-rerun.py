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

"""Check the failed run again immediately before an automatic retry."""

import argparse
import json
import os
import subprocess
from urllib.parse import urlencode


def api(path):
    result = subprocess.run(
        ["gh", "api", "--method", "GET", path], check=True,
        capture_output=True, timeout=30,
    )
    return json.loads(result.stdout)


def decide(repository, run_id, expected_attempt, max_reruns, fetch=api):
    run = fetch(f"repos/{repository}/actions/runs/{run_id}")
    if run.get("path") not in {".github/workflows/server-memory-ci.yml", ".github/workflows/licence-checker.yml"}:
        return "skip", "only required workflows automatically retry"
    if (run.get("status") != "completed" or run.get("conclusion") != "failure"
            or run.get("run_attempt") != expected_attempt):
        return "skip", "source run changed or is no longer a completed failure"
    if expected_attempt > max_reruns:
        return "skip", "retry limit reached"
    sha = run.get("head_sha")
    if not sha:
        return "skip", "missing head SHA"
    if run.get("event") == "pull_request":
        return "skip", "PR automatic retry paused: CI artifacts cannot independently prove the tested code"
    if run.get("event") == "push":
        if (run.get("repository", {}).get("full_name") != repository
                or run.get("head_repository", {}).get("full_name") != repository):
            return "skip", "push source repository differs"
        branch = run.get("head_branch")
        if not branch:
            return "skip", "missing push branch"
        # Read from the workflow repository, never a similarly named fork branch.
        commits = fetch(f"repos/{repository}/commits?" + urlencode({"sha": branch, "per_page": 1}))
        if commits and commits[0].get("sha") == sha:
            return "rerun", "push branch head unchanged"
        return "skip", "push branch head moved"
    return "skip", "unsupported source event"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--run-id", required=True, type=int)
    parser.add_argument("--run-attempt", required=True, type=int)
    parser.add_argument("--max-reruns", required=True, type=int)
    args = parser.parse_args()
    try:
        action, reason = decide(args.repository, args.run_id, args.run_attempt, args.max_reruns)
    except (subprocess.SubprocessError, OSError, ValueError, KeyError, TypeError, AttributeError):
        action, reason = "skip", "unable to verify current run and source metadata"
    print(f"{action}: {reason}")
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        output.write(f"action={action}\nreason={reason}\n")


if __name__ == "__main__":
    main()
