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

"""Conservative CI selection and GitHub-verified test receipt reuse."""

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import zipfile
from urllib.parse import urlencode

MODULES = {
    "server": ["server", "commons", "struct", "pd", "store", "hstore", "cluster", "docker", "helm", "dependency_license"],
    "toolchain": ["client", "loader", "tools", "spark", "hubble", "go"],
}
PREFIXES = {
    "server": {"hugegraph-server/": "server", "hugegraph-commons/": "commons",
               "hugegraph-struct/": "struct", "hugegraph-pd/": "pd", "hugegraph-store/": "store",
               "hugegraph-cluster-test/": "cluster", "docker/": "docker", "helm/": "helm"},
    "toolchain": {"hugegraph-client/": "client", "hugegraph-client-go/": "go",
                  "hugegraph-loader/": "loader", "hugegraph-tools/": "tools",
                  "hugegraph-spark-connector/": "spark", "hugegraph-hubble/": "hubble"},
}
WORKFLOWS = {
    "server": {"server-ci.yml": ["server"], "commons-ci.yml": ["commons"],
               "pd-store-ci.yml": ["pd", "store", "hstore"], "cluster-test-ci.yml": ["cluster"],
               "docker-build-ci.yml": ["docker"], "helm-chart-ci.yml": ["helm"],
               "codeql-analysis.yml": [], "riscv64-ci.yml": ["server"], "check-dependencies.yml": ["dependency_license"]},
    "toolchain": {"client-ci.yml": ["client"], "client-go-ci.yml": ["go"],
                  "loader-ci.yml": ["loader"], "tools-ci.yml": ["tools"],
                  "spark-connector-ci.yml": ["spark"], "hubble-ci.yml": ["hubble"],
                  "codeql-analysis.yml": []},
}
DEPENDENTS = {
    "server": {"commons": ["server", "pd", "store", "hstore", "cluster"],
               "struct": ["server", "pd", "store", "hstore", "cluster"],
               "server": ["cluster"], "pd": ["store", "hstore", "cluster"],
               "store": ["hstore", "cluster"], "hstore": ["cluster"]},
    "toolchain": {"client": ["loader", "tools", "spark", "hubble"], "loader": ["hubble"]},
}


def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()


def api(path, binary=False):
    result = subprocess.run(["gh", "api", "--method", "GET", path], check=True, capture_output=True, timeout=30)
    return result.stdout if binary else json.loads(result.stdout)


def documentation(path, mode="100644"):
    """Only prose and static documentation assets; never source or config."""
    if mode != "100644":
        return False
    p = Path(path)
    prose_names = {"README.md", "README_CN.md", "README_ZH.md", "AGENTS.md"}
    module_roots = {prefix.rstrip("/") for mapping in PREFIXES.values() for prefix in mapping}
    module_roots.update("hugegraph-server/" + module for module in [
        "hugegraph-api", "hugegraph-core", "hugegraph-dist", "hugegraph-example",
        "hugegraph-hbase", "hugegraph-hstore", "hugegraph-rocksdb", "hugegraph-test"])
    if p.name in prose_names and (str(p.parent) == "." or str(p.parent) in module_roots):
        return True
    return path.startswith("docs/") and p.suffix.lower() in {".md", ".rst", ".txt", ".png", ".jpg", ".jpeg", ".svg"}


def unsafe_documentation(ref, paths):
    entries = subprocess.check_output(["git", "ls-tree", "-rz", "--full-tree", ref]).split(b"\0")
    for entry in entries:
        if not entry:
            continue
        metadata, name = entry.split(b"\t", 1)
        path = name.decode("utf-8", "surrogateescape")
        if path in paths and documentation(path) and metadata.split(b" ", 1)[0] != b"100644":
            return True
    return False


def select(project, paths):
    selected = set()
    for path in paths:
        if documentation(path):
            continue
        if project == "server" and (Path(path).name == "pom.xml" or path.startswith("install-dist/")):
            selected.add("dependency_license")
        if path.startswith(".github/workflows/") and Path(path).name in WORKFLOWS[project]:
            selected.update(WORKFLOWS[project][Path(path).name])
            continue
        if path.endswith(".proto"):
            return set(MODULES[project])
        if project == "server":
            if path.startswith("hugegraph-server/hugegraph-hstore/"):
                selected.add("hstore")
                continue
            if path.startswith("hugegraph-server/hugegraph-dist/"):
                selected.update(["server", "pd", "store", "hstore", "docker", "cluster"])
                continue
            if path.startswith(("hugegraph-server/hugegraph-core/", "hugegraph-server/hugegraph-api/",
                                "hugegraph-server/hugegraph-test/")):
                selected.update(["server", "hstore"])
                continue
        module = next((value for prefix, value in PREFIXES[project].items() if path.startswith(prefix)), None)
        if module is None or path.endswith(".proto"):
            return set(MODULES[project])
        selected.add(module)
        if project == "server" and ("Dockerfile" in Path(path).name or "docker-entrypoint" in path):
            selected.add("docker")
    changed = True
    while changed:
        before = set(selected)
        for module in before:
            selected.update(DEPENDENTS[project].get(module, []))
        changed = before != selected
    return selected


def fingerprint(ref, policy=False):
    """Hash blob identities, including paths; exclude only the strict prose whitelist."""
    entries = subprocess.check_output(["git", "ls-tree", "-rz", "--full-tree", ref]).split(b"\0")
    digest = hashlib.sha256()
    for entry in sorted(entries, key=lambda e: e.split(b"\t", 1)[-1]):
        if not entry:
            continue
        metadata, name = entry.split(b"\t", 1)
        path = name.decode("utf-8", "surrogateescape")
        if policy:
            if not (path.startswith(".github/") or path == ".asf.yaml"):
                continue
        elif documentation(path, metadata.split(b" ", 1)[0].decode()):
            continue
        digest.update(metadata + b"\t" + name + b"\0")
    return digest.hexdigest()


def remote_fingerprint(repository, sha, policy_only=False, fetch=api):
    tree = fetch(f"repos/{repository}/git/trees/{sha}?recursive=1")
    if tree.get("truncated"):
        raise ValueError("truncated proof tree")
    digest = hashlib.sha256()
    for entry in sorted(tree["tree"], key=lambda item: item["path"].encode()):
        path = entry["path"]
        if entry["type"] == "tree":
            continue
        if policy_only:
            if not (path.startswith(".github/") or path == ".asf.yaml"):
                continue
        elif documentation(path, entry["mode"]):
            continue
        digest.update((entry["mode"] + " " + entry["type"] + " " + entry["sha"]
                       + "\t" + path + "\0").encode())
    return digest.hexdigest()


def proof_pr(plan, run, fetch):
    associations = run.get("pull_requests", [])
    if any(pr.get("number") == plan["pr"] for pr in associations):
        return True
    owner = run.get("head_repository", {}).get("owner", {}).get("login")
    branch = run.get("head_branch")
    if not owner or not branch:
        return False
    candidates = pages(f"repos/{plan['repository']}/pulls?" + urlencode({
        "state": "open", "head": f"{owner}:{branch}"}), fetch)
    return any(pr.get("number") == plan["pr"]
               and pr.get("base", {}).get("repo", {}).get("full_name") == plan["repository"]
               and pr.get("head", {}).get("ref") == branch
               and pr.get("head", {}).get("repo", {}).get("full_name") == plan["source"]
               for pr in candidates)


def suites(project, selected):
    if project == "toolchain":
        return sorted(selected)
    result = []
    if "server" in selected:
        result.extend(["server_memory", "server_rocksdb"])
    for module in ["commons", "cluster", "docker", "helm", "dependency_license"]:
        if module in selected:
            result.append(module)
    if selected.intersection({"pd", "store", "hstore", "struct"}):
        result.append("pd_store")
    return sorted(result)


def pages(path, fetch=api):
    result = []
    for page in range(1, 11):
        value = fetch(path + ("&" if "?" in path else "?") + urlencode({"per_page": 100, "page": page}))
        batch = value["jobs"] if isinstance(value, dict) else value
        result.extend(batch)
        if len(batch) < 100:
            return result
    raise ValueError("API pagination exceeded verification bound")


def successful_jobs(repository, run_id, suite, fetch=api):
    jobs = pages(f"repos/{repository}/actions/runs/{run_id}/jobs", fetch)
    matching = [job for job in jobs if job.get("name") == suite or job.get("name", "").startswith((suite + " / ", suite + " ("))]
    if not matching or any(job.get("conclusion") != "success" for job in matching):
        raise ValueError("missing or unsuccessful suite jobs")
    return sorted(job["id"] for job in matching)


def validate_receipt(plan, receipt, fetch=api):
    for key in ["schema", "repository", "project", "pr", "source", "branch", "base", "inputFingerprint", "policyFingerprint"]:
        if receipt.get(key) != plan.get(key):
            raise ValueError("receipt provenance differs: " + key)
    proof = receipt.get("proofs", {})
    required = plan["expected"]
    if not required or not set(required).issubset(proof):
        raise ValueError("receipt lacks selected suites")
    verified = {}
    for suite in required:
        item = proof[suite]
        run = fetch(f"repos/{plan['repository']}/actions/runs/{int(item['runID'])}")
        if (run.get("status") != "completed"
                or run.get("event") != "pull_request" or run.get("head_sha") != item.get("head")
                or run.get("workflow_id") != plan["workflowID"]
                or run.get("head_repository", {}).get("full_name") != plan["source"]
                or run.get("head_branch") != plan["branch"]):
            raise ValueError("proof run is not a successful trusted workflow")
        successful_jobs(plan["repository"], item["runID"], "affected-module-tests", fetch)
        if not proof_pr(plan, run, fetch):
            raise ValueError("proof run lacks matching PR association")
        merge = fetch(f"repos/{plan['repository']}/git/commits/{item['testedMergeSHA']}")
        if [parent["sha"] for parent in merge.get("parents", [])] != [plan["base"], item["head"]]:
            raise ValueError("tested merge parents differ")
        actual_input = remote_fingerprint(plan["repository"], item["testedMergeSHA"], fetch=fetch)
        if plan["project"] == "toolchain":
            context = json.dumps(plan.get("externalInputs", {}), sort_keys=True, separators=(",", ":"))
            actual_input = hashlib.sha256((actual_input + context).encode()).hexdigest()
        if (actual_input != plan["inputFingerprint"] or remote_fingerprint(
                plan["repository"], item["testedMergeSHA"], policy_only=True, fetch=fetch)
                != plan["policyFingerprint"]):
            raise ValueError("actual tested inputs differ from claimed receipt")
        ids = successful_jobs(plan["repository"], item["runID"], suite, fetch)
        if ids != item.get("jobIDs"):
            raise ValueError("actual job evidence differs")
        verified[suite] = item
    return verified


def find_reuse(plan, fetch=api):
    """Receipts are uploaded by this workflow, not read from the PR tree."""
    repository = plan["repository"]
    current = fetch(f"repos/{repository}/actions/runs/{os.environ['GITHUB_RUN_ID']}")
    plan["workflowID"] = current["workflow_id"]
    runs = fetch(f"repos/{repository}/actions/workflows/{plan['workflowID']}/runs?" + urlencode({
        "event": "pull_request", "status": "completed", "per_page": 30}))["workflow_runs"]
    for run in runs:
        if run["id"] == int(os.environ["GITHUB_RUN_ID"]):
            continue
        if (run.get("head_repository", {}).get("full_name") != plan["source"]
                or run.get("head_branch") != plan["branch"]):
            continue
        if not proof_pr(plan, run, fetch):
            continue
        artifacts = fetch(f"repos/{repository}/actions/runs/{run['id']}/artifacts?per_page=100")["artifacts"]
        for artifact in artifacts:
            size = artifact.get("size_in_bytes")
            if (artifact.get("name") != "ci-test-receipt" or artifact.get("expired")
                    or type(size) is not int or not 0 < size <= 1048576):
                continue
            try:
                payload = fetch(f"repos/{repository}/actions/artifacts/{artifact['id']}/zip", binary=True)
                with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                    entries = archive.infolist()
                    if len(entries) != 1 or entries[0].filename != "receipt.json":
                        raise ValueError("unexpected receipt archive entries")
                    info = entries[0]
                    mode = (info.external_attr >> 16) & 0o170000
                    if (info.is_dir() or info.flag_bits & 1 or mode not in {0, 0o100000}
                            or info.file_size > 1048576):
                        raise ValueError("oversized receipt")
                    receipt = json.loads(archive.read(info))
                if receipt.get("runID") != run["id"]:
                    raise ValueError("receipt not uploaded by claimed run")
                successful_jobs(repository, run["id"], "affected-module-tests", fetch)
                verified = validate_receipt(plan, receipt, fetch)
                jobs = pages(f"repos/{repository}/actions/runs/{run['id']}/jobs", fetch)
                def good_matches(predicate, minimum=1):
                    matching = [job for job in jobs if predicate(job.get("name", "").lower())]
                    return len(matching) >= minimum and all(job.get("conclusion") == "success" for job in matching)
                plan["optionalSuccess"] = {
                    "compatibility": (good_matches(lambda n: "hbase compatibility" in n)
                                      and good_matches(lambda n: "macos" in n, 2)
                                      and good_matches(lambda n: "riscv64" in n)),
                    "security": good_matches(lambda n: n.startswith(("codeql /", "security /"))),
                }
                return verified
            except (ValueError, KeyError, TypeError, AttributeError, zipfile.BadZipFile):
                continue
    return {}


def create_plan(project, event, repository, fetch=api, external_inputs=None):
    plan = {"schema": 1, "project": project, "repository": repository, "pr": 0,
            "source": repository, "branch": "", "base": "", "head": git("rev-parse", "HEAD"),
            "reused": {}, "workflowID": 0, "reason": "affected inputs",
            "externalInputs": external_inputs or {}, "testedMergeSHA": git("rev-parse", "HEAD")}
    try:
        pr = event.get("pull_request")
        if pr:
            # Refresh PR metadata: event payload may predate a new push or base movement.
            live = fetch(f"repos/{repository}/pulls/{pr['number']}")
            if (live.get("state") != "open" or live["head"]["sha"] != pr["head"]["sha"]
                    or live["base"]["repo"]["full_name"] != repository):
                raise ValueError("PR metadata changed")
            plan.update(pr=pr["number"], source=live["head"]["repo"]["full_name"],
                        base=pr["base"]["sha"], head=live["head"]["sha"], branch=live["head"]["ref"])
            parents = git("show", "-s", "--format=%P", plan["testedMergeSHA"]).split()
            if parents != [plan["base"], plan["head"]]:
                raise ValueError("checkout is not the event PR merge")
            if live["base"]["sha"] != plan["base"]:
                raise ValueError("base advanced after event checkout")
            # head/base objects must exist locally; workflow fetches both before planning.
            ancestor = git("merge-base", plan["base"], plan["head"])
            paths = git("diff", "--no-renames", "--name-only", ancestor, plan["head"]).splitlines()
        else:
            plan["base"] = event.get("before", "")
            paths = git("diff", "--no-renames", "--name-only", plan["base"], plan["head"]).splitlines()
        selected = select(project, paths)
        if unsafe_documentation(plan["base"], paths) or unsafe_documentation(plan["testedMergeSHA"], paths):
            selected = set(MODULES[project])
        plan["inputFingerprint"] = fingerprint(plan["testedMergeSHA"])
        if project == "toolchain":
            context = json.dumps(external_inputs or {}, sort_keys=True, separators=(",", ":"))
            plan["inputFingerprint"] = hashlib.sha256((plan["inputFingerprint"] + context).encode()).hexdigest()
        plan["policyFingerprint"] = fingerprint(plan["testedMergeSHA"], policy=True)
        plan["expected"] = suites(project, selected)
        if pr and plan["expected"] and (project != "toolchain" or external_inputs):
            plan["reused"] = find_reuse(plan, fetch)
    except (subprocess.SubprocessError, OSError, ValueError, KeyError, TypeError, AttributeError):
        selected = set(MODULES[project])
        plan.update(reused={}, reason="verification unavailable: full required coverage")
        plan.setdefault("inputFingerprint", "")
        plan.setdefault("policyFingerprint", "")
        plan["expected"] = suites(project, selected)
    if not selected:
        plan["reason"] = "cumulative PR diff contains only plain prose documentation; modules unaffected"
    elif plan["reused"]:
        plan["reason"] = "affected inputs match independently verified successful tests"
    plan["selected"] = sorted(selected)
    for module in MODULES[project]:
        module_suites = suites(project, {module})
        plan[module] = module in selected and not (module_suites and all(s in plan["reused"] for s in module_suites))
    plan["pd_store"] = any(plan.get(m, False) for m in ["pd", "store", "hstore", "struct"])
    plan["needsFixture"] = project == "toolchain" and any(plan.get(m, False) for m in MODULES[project])
    plan["fixture"] = plan["needsFixture"]
    plan["compatibility"] = not plan.get("optionalSuccess", {}).get("compatibility", False) and project == "server" and bool(selected.intersection({"server", "commons", "struct", "pd", "store", "hstore", "cluster"}))
    plan["security"] = (bool(selected) and not plan.get("optionalSuccess", {}).get("security", False)) or any(p.startswith(".github/workflows/codeql") for p in locals().get("paths", []))
    plan["security_languages"] = json.dumps(["java"])
    return plan


def gate(plan, results, fetch=api):
    if results.get("plan", {}).get("result") != "success":
        raise ValueError("planner did not succeed")
    proofs = dict(plan["reused"])
    executed = []
    for suite in plan["expected"]:
        if suite in proofs:
            continue
        if results.get(suite, {}).get("result") != "success":
            raise ValueError("selected suite did not succeed: " + suite)
        executed.append(suite)
    if plan["project"] == "toolchain":
        if set(executed).intersection({"client", "loader", "tools", "spark", "go"}):
            if results.get("fixture", {}).get("result") != "success":
                raise ValueError("selected tests lack successful fixture")
        if "hubble" in executed and results.get("hubble-fixture", {}).get("result") != "success":
            raise ValueError("selected Hubble tests lack successful baseline fixture")
    receipt = {key: plan[key] for key in ["schema", "repository", "project", "pr", "source", "branch", "base",
                                          "head", "testedMergeSHA", "inputFingerprint", "policyFingerprint", "workflowID"]}
    receipt["runID"] = int(os.environ.get("GITHUB_RUN_ID", "0"))
    # Successful tests can gate even if receipt publication is unavailable. Never reuse
    # unverified evidence: a missing API proof only disables future receipt reuse.
    for suite in executed:
        try:
            ids = successful_jobs(plan["repository"], receipt["runID"], suite, fetch)
            proofs[suite] = {"runID": receipt["runID"], "head": plan["head"], "testedMergeSHA": plan["testedMergeSHA"], "jobIDs": ids}
        except (subprocess.SubprocessError, OSError, ValueError, KeyError, TypeError, AttributeError):
            pass
    receipt["proofs"] = proofs
    receipt["executed"] = executed
    receipt["reused"] = sorted(plan["reused"])
    summary = "Selection: " + plan.get("reason", "affected inputs") + "\nExecuted: " + ", ".join(executed) + "\nReused: " + ", ".join(receipt["reused"])
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as out:
            out.write("## Affected module tests\n" + summary + "\n\nUnselected modules: "
                      + ", ".join(set(MODULES[plan["project"]]) - set(plan["selected"])) + "\n")
    print(summary)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--project", choices=MODULES, required=True)
    p.add_argument("--event-path", required=True)
    p.add_argument("--repository", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--inputs-json")
    p = sub.add_parser("gate")
    p.add_argument("--plan", required=True)
    p.add_argument("--results", required=True)
    p.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.command == "plan":
        external = json.loads(Path(args.inputs_json).read_text()) if args.inputs_json else None
        value = create_plan(args.project, json.loads(Path(args.event_path).read_text()), args.repository,
                            external_inputs=external)
        if os.environ.get("GITHUB_OUTPUT"):
            with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
                for key, item in value.items():
                    if isinstance(item, bool):
                        output.write(f"{key}={str(item).lower()}\n")
                output.write(f"security_languages={value['security_languages']}\nplan_file={args.output}\n")
    else:
        value = gate(json.loads(Path(args.plan).read_text()), json.loads(Path(args.results).read_text()))
    Path(args.output).write_text(json.dumps(value, indent=2) + "\n")


if __name__ == "__main__":
    main()
