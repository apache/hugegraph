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
import io
import json
import zipfile
import os
import subprocess
from urllib.parse import quote, urlencode


def api(path, binary=False):
    result = subprocess.run(
        ["gh", "api", "--method", "GET", path], check=True,
        capture_output=True, timeout=30,
    )
    return result.stdout if binary else json.loads(result.stdout)


def tested_base_matches(repository, run_id, pr, sha, fetch):
    artifacts = fetch(f"repos/{repository}/actions/runs/{run_id}/artifacts?per_page=100")["artifacts"]
    candidates = [a for a in artifacts if a.get("name") == "ci-plan" and not a.get("expired")]
    for artifact in candidates:
        payload = fetch(f"repos/{repository}/actions/artifacts/{artifact['id']}/zip", binary=True)
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            info = archive.getinfo("plan.json")
            if info.file_size > 1048576:
                continue
            plan = json.loads(archive.read(info))
        base = pr.get("base", {}).get("sha")
        if (plan.get("repository") != repository or plan.get("pr") != pr.get("number")
                or plan.get("head") != sha or not base or plan.get("base") != base
                or not plan.get("testedMergeSHA")):
            continue
        merge = fetch(f"repos/{repository}/git/commits/{plan['testedMergeSHA']}")
        if [parent["sha"] for parent in merge.get("parents", [])] == [base, sha]:
            return True
    return False


def decide(repository, run_id, expected_attempt, max_reruns, fetch=api):
    run = fetch(f"repos/{repository}/actions/runs/{run_id}")
    if (run.get("status") != "completed" or run.get("conclusion") != "failure"
            or run.get("run_attempt") != expected_attempt):
        return "skip", "source run changed or is no longer a completed failure"
    if expected_attempt > max_reruns:
        return "skip", "retry limit reached"
    sha = run.get("head_sha")
    if not sha:
        return "skip", "missing head SHA"
    if run.get("event") == "pull_request":
        def local(candidate):
            base_repository = candidate.get("base", {}).get("repo", {})
            return (base_repository.get("full_name") == repository
                    or base_repository.get("url") == f"https://api.github.com/repos/{repository}")

        source = run.get("head_repository") or {}
        source_owner = (source.get("owner") or {}).get("login")
        source_name = source.get("full_name")
        source_branch = run.get("head_branch")
        if not source_owner or not source_name or not source_branch:
            return "skip", "missing source repository or branch metadata"
        def source_matches(pr):
            head = pr.get("head") or {}
            repo = head.get("repo") or {}
            return (head.get("ref") == source_branch and repo.get("full_name") == source_name
                    and (repo.get("owner") or {}).get("login") == source_owner)
        fallback_owner = None
        candidates = [pr for pr in run.get("pull_requests", []) if local(pr)]
        if not candidates:
            candidates = [pr for pr in fetch(
                f"repos/{repository}/commits/{quote(sha, safe='')}/pulls") if local(pr)]
        if not candidates:
            owner = run.get("head_repository", {}).get("owner", {}).get("login")
            branch = run.get("head_branch")
            if owner and branch:
                fallback_owner = owner
                # Fork runs can have no commit/PR association in either endpoint.
                # Use the qualified fork owner, never the branch name alone.
                for page in range(1, 101):
                    batch = fetch(f"repos/{repository}/pulls?" + urlencode({
                        "state": "open", "head": f"{owner}:{branch}",
                        "per_page": 100, "page": page}))
                    candidates.extend(pr for pr in batch if local(pr)
                                      and pr.get("head", {}).get("ref") == branch
                                      and pr.get("head", {}).get("repo", {}).get("owner", {}).get("login") == owner)
                    if len(batch) < 100:
                        break
        for candidate in candidates:
            number = candidate.get("number")
            if not isinstance(number, int):
                continue
            pr = fetch(f"repos/{repository}/pulls/{number}")
            if not source_matches(pr):
                continue
            if (pr.get("base", {}).get("repo", {}).get("full_name") == repository
                    and pr.get("state") == "open" and pr.get("head", {}).get("sha") == sha):
                if tested_base_matches(repository, run_id, pr, sha, fetch):
                    return "rerun", "open PR head and tested base unchanged"
                return "skip", "tested base moved or immutable CI plan proof unavailable"
        return "skip", "no open PR in this repository with the failed head"
    if run.get("event") == "push":
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
    except (subprocess.SubprocessError, OSError, ValueError, KeyError, TypeError, AttributeError, zipfile.BadZipFile):
        action, reason = "skip", "unable to verify current run and source metadata"
    print(f"{action}: {reason}")
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        output.write(f"action={action}\nreason={reason}\n")


if __name__ == "__main__":
    main()
