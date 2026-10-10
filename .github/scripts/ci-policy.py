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

"""Conservative CI selection and gating of this run's selected tests."""

import argparse
import json
import os
from pathlib import Path
import subprocess

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
    "server": {"server-ci.yml": MODULES["server"], "commons-ci.yml": ["commons"],
               "pd-store-ci.yml": ["pd", "store", "hstore"], "cluster-test-ci.yml": ["cluster"],
               "docker-build-ci.yml": ["docker"], "helm-chart-ci.yml": ["helm"],
               "codeql-analysis.yml": [], "server-memory-ci.yml": ["server"],
               "server-tests.yml": ["server"], "server-compatibility-ci.yml": ["server", "docker"],
               "riscv64-ci.yml": ["server"], "check-dependencies.yml": ["dependency_license"]},
    "toolchain": {"client-ci.yml": ["client"], "client-go-ci.yml": ["go"],
                  "loader-ci.yml": ["loader"], "tools-ci.yml": ["tools"],
                  "spark-connector-ci.yml": ["spark"], "hubble-ci.yml": ["hubble"],
                  "codeql-analysis.yml": []},
}
# Exact maintenance inputs have no product consumers; their checks run in the planner.
MAINTENANCE_INPUTS = {
    ".github/PULL_REQUEST_TEMPLATE.md": [],
    ".github/dependabot.yml": [],
    ".github/scripts/check-rerun.py": [],
    ".github/scripts/test_check_rerun.py": [],
    ".github/scripts/check-docker-images.sh": ["docker"],
    ".github/scripts/docker-deployment.py": ["docker"],
    ".github/scripts/test_docker_deployment.py": ["docker"],
}
DEPENDENTS = {
    "server": {"commons": ["server", "pd", "store", "hstore", "cluster"],
               "struct": ["server", "pd", "store", "hstore", "cluster"],
               "server": ["pd", "store", "hstore", "cluster"], "pd": ["store", "hstore", "cluster"],
               "store": ["hstore", "cluster"], "hstore": ["cluster"]},
    "toolchain": {"client": ["loader", "tools", "spark", "hubble"], "loader": ["hubble"]},
}


def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()


def api(path):
    result = subprocess.run(["gh", "api", "--method", "GET", path], check=True, capture_output=True, timeout=30)
    return json.loads(result.stdout)


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
        if project == "server" and path in MAINTENANCE_INPUTS:
            selected.update(MAINTENANCE_INPUTS[path])
            continue
        if project == "server" and (Path(path).name == "pom.xml" or path.startswith("install-dist/")):
            selected.add("dependency_license")
        if path.startswith(".github/workflows/") and Path(path).name in WORKFLOWS[project]:
            selected.update(WORKFLOWS[project][Path(path).name])
            continue
        if path.endswith(".proto"):
            return set(MODULES[project])
        if project == "server" and path == "hugegraph-commons/hugegraph-common/src/main/resources/version.properties":
            selected.update(["commons", "docker"])
            continue
        if project == "toolchain" and path.startswith("hugegraph-client/assembly/travis/"):
            selected.update(["client", "go"])
            continue
        if project == "server":
            if path in {"hugegraph-server/pom.xml", "hugegraph-server/hugegraph-api/pom.xml"}:
                selected.add("docker")
            if path.startswith(("hugegraph-pd/hg-pd-dist/", "hugegraph-store/hg-store-dist/")):
                selected.add("docker")
            if path.startswith("hugegraph-server/hugegraph-hstore/"):
                selected.update(["server", "hstore"])
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


def suites(project, selected):
    if project == "toolchain":
        return sorted(selected)
    result = []
    if "server" in selected:
        result.extend(["server_memory", "server_rocksdb"])
    # Third-party inventory/review remains visible without becoming a merge gate.
    for module in ["commons", "cluster", "docker", "helm"]:
        if module in selected:
            result.append(module)
    if selected.intersection({"pd", "store", "hstore", "struct"}):
        result.append("pd_store")
    return sorted(result)


class StaleInputError(RuntimeError):
    """The checkout no longer represents the current PR inputs."""


def require_current_pr(plan, fetch):
    if not plan["pr"]:
        return
    live = fetch(f"repos/{plan['repository']}/pulls/{plan['pr']}")
    if (live.get("state") != "open" or live["head"]["sha"] != plan["head"]
            or live["head"]["repo"]["full_name"] != plan["source"]
            or live["head"]["ref"] != plan["branch"]
            or live["base"]["repo"]["full_name"] != plan["repository"]):
        raise StaleInputError("PR inputs changed; refresh the branch and start a new PR run")


def create_plan(project, event, repository, fetch=api):
    paths = []
    plan = {"schema": 1, "project": project, "repository": repository, "pr": 0,
            "source": repository, "branch": "", "base": "", "head": git("rev-parse", "HEAD"),
            "reason": "affected inputs", "testedMergeSHA": git("rev-parse", "HEAD")}
    try:
        pr = event.get("pull_request")
        if "pull_request" in event:
            if not isinstance(pr, dict) or type(pr.get("number")) is not int or pr["number"] <= 0:
                raise StaleInputError("PR event lacks a valid number")
            plan["pr"] = pr["number"]
            # An incomplete PR event must never fall back to a push plan with pr=0.
            try:
                plan.update(source=pr["head"]["repo"]["full_name"], base=pr["base"]["sha"],
                            head=pr["head"]["sha"], branch=pr["head"]["ref"])
            except (KeyError, TypeError) as error:
                raise StaleInputError("PR event lacks required input identity") from error
            if not all(isinstance(plan[key], str) and plan[key] for key in ("source", "base", "head", "branch")):
                raise StaleInputError("PR event has an empty input identity")
            parents = git("show", "-s", "--format=%P", plan["testedMergeSHA"]).split()
            if (plan["testedMergeSHA"] != os.environ.get("GITHUB_SHA")
                    or len(parents) != 2 or parents[1] != plan["head"]):
                raise StaleInputError("checkout is not the event PR merge; start a new PR run")
            # The event base may lag the synthetic merge after the target branch advances.
            plan["base"] = parents[0]
            require_current_pr(plan, fetch)
            # head/base objects must exist locally; workflow fetches both before planning.
            ancestor = git("merge-base", plan["base"], plan["head"])
            paths = git("diff", "--no-renames", "--name-only", ancestor, plan["head"]).splitlines()
        else:
            plan["base"] = event.get("before", "")
            paths = git("diff", "--no-renames", "--name-only", plan["base"], plan["head"]).splitlines()
        selected = select(project, paths)
        if unsafe_documentation(plan["base"], paths) or unsafe_documentation(plan["testedMergeSHA"], paths):
            selected = set(MODULES[project])
            plan["reason"] = "non-regular documentation input: conservative full coverage"
        plan["expected"] = suites(project, selected)
    except (subprocess.SubprocessError, OSError, ValueError, KeyError, TypeError, AttributeError):
        selected = set(MODULES[project])
        plan["reason"] = "verification unavailable: conservative full coverage"
        plan["expected"] = suites(project, selected)
    if not selected:
        plan["reason"] = "cumulative PR diff contains only plain prose documentation; modules unaffected"
    plan["selected"] = sorted(selected)
    for module in MODULES[project]:
        plan[module] = module in selected
    plan["pd_store"] = any(plan.get(m, False) for m in ["pd", "store", "hstore", "struct"])
    plan["needsFixture"] = project == "toolchain" and any(plan.get(m, False) for m in MODULES[project])
    plan["fixture"] = plan["needsFixture"]
    plan["compatibility"] = project == "server" and bool(selected.intersection({"server", "commons", "struct", "pd", "store", "hstore", "cluster"}))
    plan["smoke"] = project == "server" and bool(selected.intersection({"server", "docker"}))
    plan["security"] = bool(selected - {"docker", "helm"}) or any(
        p.startswith(".github/workflows/codeql") for p in locals().get("paths", []))
    plan["required"] = ["server_memory"] if project == "server" and "server" in selected else []
    plan["security_languages"] = json.dumps(["java"])
    plan["changedPaths"] = paths
    plan["selectionReasons"] = {
        suite: [path for path in paths if suite in suites(project, select(project, [path]))]
        for suite in plan["expected"]
    }
    if "dependency_license" in selected:
        plan["selectionReasons"]["dependency_license"] = [
            path for path in paths if "dependency_license" in select(project, [path])]
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as out:
            out.write("## Selected CI checks\n" + selection_table(plan) + "\n")
    return plan


def selection_reason(plan, suite):
    paths = plan.get("selectionReasons", {}).get(suite, [])
    if not paths:
        selected = suite in plan["expected"] or suite in plan["selected"]
        return plan.get("reason", "affected inputs") if selected else "no affected inputs"
    # Bound the summary size; paths in the plan artifact retain the full explanation.
    shown = ["`" + path.replace("`", "\\`").replace("|", "\\|").replace("\n", " ") + "`"
             for path in paths[:3]]
    suffix = f" (+{len(paths) - 3} more)" if len(paths) > 3 else ""
    return ", ".join(shown) + suffix


def selection_table(plan):
    summary = "| Check | Selected | Merge requirement | Changed inputs |\n| --- | --- | --- | --- |\n"
    checks = suites(plan["project"], set(MODULES[plan["project"]]))
    if "dependency_license" in plan["selected"]:
        checks.append("dependency_license")
    for suite in checks:
        chosen = suite in plan["expected"] or suite == "dependency_license"
        role = "required" if suite == "server_memory" else "advisory"
        summary += f"| {suite} | {'yes' if chosen else 'no'} | {role} | {selection_reason(plan, suite)} |\n"
    return summary


class GateFailure(ValueError):
    def __init__(self, message, report):
        super().__init__(message)
        self.report = report


def gate(plan, results, fetch=None, mode="all"):
    """Report actual outcomes; Memory and advisory checks have independent dependencies."""
    expected = list(plan["expected"])
    if mode == "memory":
        expected = [suite for suite in expected if suite == "server_memory"]
    elif mode == "advisory":
        expected = [suite for suite in expected if suite != "server_memory"]
    report = {key: plan[key] for key in ["schema", "repository", "project", "pr", "source", "branch", "base",
                                        "head", "testedMergeSHA", "selected"]}
    report["selectionReasons"] = plan.get("selectionReasons", {})
    report.update(runID=int(os.environ.get("GITHUB_RUN_ID", "0")),
                  runAttempt=int(os.environ.get("GITHUB_RUN_ATTEMPT", "1")), executed=expected,
                  results={suite: results.get(suite, {}).get("result", "missing") for suite in expected})
    if mode != "memory" and "dependency_license" in plan["selected"]:
        report["results"]["dependency_license"] = results.get("dependency_license", {}).get("result", "missing")
    summary = "Selection: " + plan.get("reason", "affected inputs") + "\n"
    summary += "Tested base: `" + plan["base"] + "`; merge: `" + plan["testedMergeSHA"] + "`\n\n"
    summary += "| Check | Selected | Merge requirement | Changed inputs | Result |\n| --- | --- | --- | --- | --- |\n"
    for suite in suites(plan["project"], set(MODULES[plan["project"]])):
        if mode == "memory" and suite != "server_memory":
            continue
        if mode == "advisory" and suite == "server_memory":
            continue
        chosen = suite in expected
        role = "required" if suite == "server_memory" else "advisory"
        status = report["results"].get(suite, "not selected")
        summary += f"| {suite} | {'yes' if chosen else 'no'} | {role} | {selection_reason(plan, suite)} | {status} |\n"
    if "dependency_license" in report["results"]:
        summary += "| dependency_license | yes | advisory | " + selection_reason(plan, "dependency_license") + " | " + str(report["results"]["dependency_license"]) + " |\n"
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as out:
            out.write("## " + ("Memory result" if mode == "memory" else "Affected module results") + "\n" + summary + "\n")
    print(summary)
    errors = []
    if results.get("plan", {}).get("result") != "success":
        errors.append("planner did not succeed")
    errors.extend("selected suite did not succeed: " + suite for suite in expected
                  if report["results"][suite] != "success")
    if plan["project"] == "toolchain":
        if set(expected).intersection({"client", "loader", "tools", "spark", "go"}):
            if results.get("fixture", {}).get("result") != "success":
                errors.append("selected tests lack successful fixture")
        if "hubble" in expected and results.get("hubble-fixture", {}).get("result") != "success":
            errors.append("selected Hubble tests lack successful baseline fixture")
    if errors:
        raise GateFailure("; ".join(errors), report)
    # Checks are attached to their actual commit; target-branch advancement alone is not stale.
    # A transient API outage cannot invalidate a genuine completed Memory result.
    try:
        require_current_pr(plan, fetch or api)
    except (subprocess.SubprocessError, OSError):
        if mode != "memory":
            raise
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--project", choices=MODULES, required=True)
    p.add_argument("--event-path", required=True)
    p.add_argument("--repository", required=True)
    p.add_argument("--output", required=True)
    p = sub.add_parser("gate")
    p.add_argument("--plan", required=True)
    p.add_argument("--results", required=True)
    p.add_argument("--mode", choices=["all", "memory", "advisory"], default="all")
    p.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.command == "plan":
        value = create_plan(args.project, json.loads(Path(args.event_path).read_text()), args.repository)
        if os.environ.get("GITHUB_OUTPUT"):
            with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
                for key, item in value.items():
                    if isinstance(item, bool):
                        output.write(f"{key}={str(item).lower()}\n")
                output.write(f"security_languages={value['security_languages']}\nplan_file={args.output}\n")
    else:
        try:
            value = gate(json.loads(Path(args.plan).read_text()), json.loads(Path(args.results).read_text()), mode=args.mode)
        except GateFailure as error:
            Path(args.output).write_text(json.dumps(error.report, indent=2) + "\n")
            raise
    Path(args.output).write_text(json.dumps(value, indent=2) + "\n")


if __name__ == "__main__":
    main()
