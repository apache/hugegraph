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

"""Select affected CI checks from the current event and cumulative PR diff."""

import argparse
import json
import os
from pathlib import Path
import subprocess

MAINLINE = {"server", "commons", "struct", "pd", "store", "hstore", "cluster", "docker", "helm",
            "dependency_license", "macos", "security", "distributed_smoke"}
MODULES = {
    "server": sorted(MAINLINE | {"macos_full", "hbase", "riscv64"}),
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
    "server": {"server-ci.yml": MAINLINE, "commons-ci.yml": {"commons"},
               "pd-store-ci.yml": {"struct", "pd", "store", "hstore"}, "cluster-test-ci.yml": {"cluster"},
               "docker-build-ci.yml": {"docker"},
               "codeql-analysis.yml": {"security"}, "server-memory-ci.yml": {"server"},
               "server-tests.yml": {"server", "hbase"},
               "server-compatibility-ci.yml": {"macos", "macos_full", "hbase", "riscv64"},
               "riscv64-ci.yml": {"riscv64"}, "check-dependencies.yml": {"dependency_license"},
               "submit-dependencies.yml": {"dependency_license"}},
    "toolchain": {"client-ci.yml": {"client"}, "client-go-ci.yml": {"go"},
                  "loader-ci.yml": {"loader"}, "tools-ci.yml": {"tools"},
                  "spark-connector-ci.yml": {"spark"}, "hubble-ci.yml": {"hubble"},
                  "codeql-analysis.yml": set()},
}
# Shared runtime consumers; ordinary Server edits do not fan out to PD/Store.
DEPENDENTS = {
    "server": {"commons": {"server", "pd", "store", "hstore", "cluster"},
               "struct": {"server", "pd", "store", "hstore", "cluster"},
               "pd": {"store", "hstore", "cluster"}, "store": {"hstore", "cluster"}, "hstore": {"cluster"}},
    "toolchain": {"client": {"loader", "tools", "spark", "hubble"}, "loader": {"hubble"}},
}
MAINTENANCE_INPUTS = {
    ".github/actions/helm-checks/action.yml": {"helm"},
    ".github/actions/rocksdb-compatibility/action.yml": {"pd", "store", "hstore"},
    ".asf.yaml": set(), ".editorconfig": set(), ".licenserc.yaml": set(),
    ".github/PULL_REQUEST_TEMPLATE.md": set(), ".github/dependabot.yml": set(),
    ".github/workflows/.java-version": MAINLINE | {"macos_full", "hbase", "riscv64"},
    ".github/scripts/dependency-snapshot.py": {"dependency_license"},
    ".github/scripts/dependency-inventory.py": {"dependency_license"},
    ".github/scripts/test_dependency_snapshot.py": set(),  # Covered by preflight discovery.
    ".github/scripts/check-docker-images.sh": {"docker"},
    ".github/scripts/docker-deployment.py": {"docker"},
    ".github/scripts/test_docker_deployment.py": {"docker"},
}
PRESETS = {
    "mainline": MAINLINE,
    "dependencies": {"dependency_license"},
    "distributed": {"struct", "pd", "store", "hstore", "cluster", "distributed_smoke"},
    "macos-arm": {"macos", "macos_full"}, "hbase": {"hbase"},
    "riscv64": {"riscv64"}, "codeql": {"security"}, "full": set(MODULES["server"]),
}
SERVER_CORE = "hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/"
SERVER_API = "hugegraph-server/hugegraph-api/src/main/java/org/apache/hugegraph/"
SECURITY_INPUTS = {
    SERVER_CORE + "backend/cache/CachedSchemaTransaction.java",
    SERVER_CORE + "backend/cache/CachedSchemaTransactionV2.java",
}
DISTRIBUTED_SERVER_INPUTS = (
    "hugegraph-server/hugegraph-hstore/", SERVER_CORE + "backend/", SERVER_CORE + "meta/",
    SERVER_CORE + "space/", SERVER_CORE + "task/", SERVER_CORE + "masterelection/", SERVER_CORE + "HugeGraph",
    SERVER_CORE + "StandardHugeGraph", SERVER_API + "core/", SERVER_API + "rpc/",
    SERVER_API + "api/raft/", SERVER_API + "api/space/",
    "hugegraph-server/hugegraph-test/src/main/java/org/apache/hugegraph/store/",
    "hugegraph-server/hugegraph-test/src/main/java/org/apache/hugegraph/meta/",
)
# Test/resources/templates can contain executable examples and runtime fixtures.
RUNTIME_PARTS = {"src", "resources", "fixtures", "templates", "static", "bin", "conf", "configs"}
# These build files declare native JNI dependencies/classifiers or the RISC-V protobuf profile.
PLATFORM_POMS = {"pom.xml", "hugegraph-server/pom.xml", "hugegraph-server/hugegraph-core/pom.xml",
                 "hugegraph-server/hugegraph-rocksdb/pom.xml", "hugegraph-server/hugegraph-dist/pom.xml",
                 "hugegraph-pd/hg-pd-grpc/pom.xml"}
RISCV_PROTO_ROOTS = ("hugegraph-server/hugegraph-core/", "hugegraph-pd/hg-pd-grpc/")
TRAVIS = "hugegraph-server/hugegraph-dist/src/assembly/travis/"
# Contract tests follow their executing suites, without runtime consumer fan-out.
CONTRACT_SUITES = {
    TRAVIS + "test-check-java-runtime-logs.sh": {"struct"},
    TRAVIS + "check-java-runtime-logs.sh": {"server", "commons", "struct", "pd", "store", "hstore", "cluster",
                                          "macos", "macos_full", "hbase", "riscv64"},
    TRAVIS + "test-stop-hugegraph-store.sh": {"struct"},
    TRAVIS + "test-check-jacoco-report.sh": {"struct"},
    TRAVIS + "test-codecov-upload-config.sh": {"struct"},
    TRAVIS + "test-riscv64-java-runtime.sh": {"server", "riscv64"},
    TRAVIS + "test-store-docker-entrypoint.sh": {"store"},
    TRAVIS + "test-start-hugegraph-store.sh": {"store"},
    TRAVIS + "test-start-server.sh": {"server"},
}


def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()


def api(path):
    result = subprocess.run(["gh", "api", "--method", "GET", path], check=True, capture_output=True, timeout=30)
    return json.loads(result.stdout)


def documentation(path, mode="100644"):
    """Only plain prose/static assets outside runtime and fixture directories."""
    if mode != "100644":
        return False
    p = Path(path)
    if RUNTIME_PARTS.intersection(p.parts[:-1]):
        return False
    if p.suffix.lower() in {".md", ".rst"}:
        return True
    if len(p.parts) == 1 and (p.suffix.lower() == ".txt" or p.name in {"LICENSE", "NOTICE"}):
        return True
    return "docs" in p.parts[:-1] and p.suffix.lower() in {".md", ".rst", ".txt", ".png", ".jpg", ".jpeg", ".svg"}


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
    contracts = set()
    for path in paths:
        if documentation(path):
            continue
        p = Path(path)
        if project == "server":
            if path in CONTRACT_SUITES:
                contracts.update(CONTRACT_SUITES[path])
                continue
            if path in MAINTENANCE_INPUTS:
                selected.update(MAINTENANCE_INPUTS[path])
                continue
            if p.name == "pom.xml":
                selected.update({"dependency_license", "security"})
            if path in PLATFORM_POMS:
                selected.update({"macos", "macos_full", "riscv64"})
            if path.startswith("install-dist/"):
                selected.add("dependency_license")
                continue
            if (path in SECURITY_INPUTS
                    or {"auth", "security", "permission", "permissions"}.intersection(p.parts)
                    or p.stem.startswith(("Authentication", "Authorization"))
                    or p.stem.endswith(("Authentication", "Authenticator", "Permission", "WithAuth"))
                    or p.name in {"AuthOptions.java", "OkHttpBasicAuthInterceptor.java", "java-security.properties",
                                  "test-java-security-properties.sh", "enable-auth.sh"}):
                selected.add("security")
            if path.startswith(".github/workflows/") and p.name in WORKFLOWS[project]:
                selected.update(WORKFLOWS[project][p.name])
                continue
            if path == TRAVIS + "install-backend.sh":
                selected.update({"hbase", "hstore"})
            if path.startswith("docker/hbase/"):
                selected.update({"docker", "hbase"})
                continue
            if path.startswith("hugegraph-server/hugegraph-hbase/") or "hbase" in p.name.lower():
                selected.add("hbase")
                continue
            if "riscv" in path.lower():
                selected.add("riscv64")
                continue
            if path.startswith(DISTRIBUTED_SERVER_INPUTS) or path.endswith(".proto"):
                selected.update({"hstore", "cluster", "distributed_smoke"})
            if path.endswith(".proto") and path.startswith(RISCV_PROTO_ROOTS):
                selected.add("riscv64")
            if path.startswith("hugegraph-server/hugegraph-dist/docker/"):
                selected.add("docker")
                continue
            if path.startswith("hugegraph-server/hugegraph-dist/"):
                selected.update({"server", "docker", "macos", "macos_full"})
                if (path.startswith("hugegraph-server/hugegraph-dist/src/assembly/static/bin/")
                        or "native-runtime" in p.name or "rocksdb-jni" in p.name
                        or p.name == "check-rocksdb-only-dist.sh"):
                    selected.add("riscv64")
                if any(word in path for word in ("pd", "store", "distributed")):
                    selected.update({"hstore", "cluster", "distributed_smoke"})
                continue
            if path.startswith("hugegraph-server/hugegraph-rocksdb/"):
                selected.update({"server", "macos", "macos_full"})
                continue
            if path in {"hugegraph-server/pom.xml", "hugegraph-server/hugegraph-api/pom.xml"}:
                selected.add("docker")
            if path.startswith(("hugegraph-pd/hg-pd-dist/", "hugegraph-store/hg-store-dist/")):
                selected.add("docker")
            if path == "hugegraph-commons/hugegraph-common/src/main/resources/version.properties":
                selected.add("docker")
        elif path.endswith(".proto"):
            selected.update(MODULES[project])
            continue
        elif path.startswith(".github/workflows/") and p.name in WORKFLOWS[project]:
            selected.update(WORKFLOWS[project][p.name])
            continue
        elif path.startswith("hugegraph-client/assembly/travis/"):
            selected.update({"client", "go"})
            continue
        module = next((value for prefix, value in PREFIXES[project].items() if path.startswith(prefix)), None)
        if module is None:
            selected.update(MAINLINE if project == "server" else MODULES[project])
        else:
            selected.add(module)
    changed = True
    while changed:
        before = set(selected)
        for module in before:
            selected.update(DEPENDENTS[project].get(module, set()))
        changed = before != selected
    if project == "server":
        if "server" in selected:
            selected.add("macos")
        if selected.intersection({"pd", "store", "hstore", "struct", "cluster"}):
            selected.add("distributed_smoke")
        selected.update(contracts)
    return selected


def event_selection(project, paths, event, event_name):
    """Presets and post-merge smoke share the same affected-input classifier."""
    if project != "server":
        return select(project, paths)
    if event_name == "schedule":
        return set(PRESETS["full"])
    if event_name == "workflow_dispatch":
        preset = event.get("inputs", {}).get("test-set", "mainline")
        if preset not in PRESETS:
            raise ValueError("unknown test-set: " + str(preset))
        return set(PRESETS[preset])
    if event_name == "push" and event.get("ref") == "refs/heads/master":
        # Test contracts do not change deployed runtime inputs.
        selected = select(project, [path for path in paths if path not in CONTRACT_SUITES])
        smoke = selected.intersection({"docker", "helm", "dependency_license", "macos", "distributed_smoke"})
        if selected.intersection({"server", "commons", "struct", "pd", "store", "hstore", "cluster"}):
            smoke.add("distributed_smoke")
        return smoke
    return select(project, paths)


def suites(project, selected):
    if project == "toolchain":
        return sorted(selected)
    result = selected.intersection({"commons", "cluster", "docker", "helm", "dependency_license", "macos",
                                    "hbase", "riscv64", "security", "distributed_smoke"})
    if "server" in selected:
        result.update({"server_memory", "server_rocksdb"})
    if selected.intersection({"pd", "store", "hstore", "struct"}):
        result.add("pd_store")
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


def create_plan(project, event, repository, fetch=api, event_name=None):
    event_name = event_name or os.environ.get("GITHUB_EVENT_NAME") or (
        "pull_request" if "pull_request" in event else "push")
    paths = []
    plan = {"schema": 1, "project": project, "repository": repository, "pr": 0,
            "source": repository, "branch": "", "base": "", "head": git("rev-parse", "HEAD"),
            "reason": "affected inputs", "testedMergeSHA": git("rev-parse", "HEAD"), "event": event_name}
    try:
        pr = event.get("pull_request")
        if "pull_request" in event:
            if not isinstance(pr, dict) or type(pr.get("number")) is not int or pr["number"] <= 0:
                raise StaleInputError("PR event lacks a valid number")
            plan["pr"] = pr["number"]
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
            plan["base"] = parents[0]
            require_current_pr(plan, fetch)
            ancestor = git("merge-base", plan["base"], plan["head"])
            paths = git("diff", "--no-renames", "--name-only", ancestor, plan["head"]).splitlines()
        elif event_name == "push":
            plan["base"] = event.get("before", "")
            plan["branch"] = event.get("ref", "").removeprefix("refs/heads/")
            paths = git("diff", "--no-renames", "--name-only", plan["base"], plan["head"]).splitlines()
        selected = event_selection(project, paths, event, event_name)
        if paths and (unsafe_documentation(plan["base"], paths)
                      or unsafe_documentation(plan["testedMergeSHA"], paths)):
            selected = event_selection(project, ["unknown"], event, event_name)
            plan["reason"] = "non-regular documentation input: conservative mainline coverage"
        if event_name in {"schedule", "workflow_dispatch"}:
            plan["reason"] = "weekly full coverage" if event_name == "schedule" else "manual " + event.get(
                "inputs", {}).get("test-set", "mainline")
    except (subprocess.SubprocessError, OSError, ValueError, KeyError, TypeError, AttributeError):
        selected = event_selection(project, ["unknown"], {"ref": event.get("ref")},
                                   "push" if event_name == "push" else "pull_request")
        plan["reason"] = "verification unavailable: conservative mainline coverage"
    if not selected:
        plan["reason"] = "plain prose documentation or maintenance inputs; tests unaffected"
    plan["selected"] = sorted(selected)
    plan["expected"] = suites(project, selected)
    for module in MODULES[project]:
        plan[module] = module in selected
    plan["pd_store"] = any(plan.get(m, False) for m in ["pd", "store", "hstore", "struct"])
    plan["needsFixture"] = project == "toolchain" and bool(selected)
    plan["fixture"] = plan["needsFixture"]
    if project == "toolchain":
        plan["security"] = bool(selected) or any(p.startswith(".github/workflows/codeql") for p in paths)
    plan["required"] = []
    plan["security_languages"] = json.dumps(["java"])
    plan["changedPaths"] = paths
    plan["selectionReasons"] = {
        suite: [path for path in paths if suite in suites(project, event_selection(project, [path], event, event_name))]
        for suite in plan["expected"]
    }
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
    for suite in checks:
        chosen = suite in plan["expected"]
        role = "advisory"
        summary += f"| {suite} | {'yes' if chosen else 'no'} | {role} | {selection_reason(plan, suite)} |\n"
    return summary


class GateFailure(ValueError):
    def __init__(self, message, report):
        super().__init__(message)
        self.report = report


def gate(plan, results, fetch=None):
    """Keep the Toolchain result summary and current-PR gate contract."""
    expected = list(plan["expected"])
    report = {key: plan[key] for key in ["schema", "repository", "project", "pr", "source", "branch", "base",
                                        "head", "testedMergeSHA", "selected"]}
    report["selectionReasons"] = plan.get("selectionReasons", {})
    report.update(runID=int(os.environ.get("GITHUB_RUN_ID", "0")),
                  runAttempt=int(os.environ.get("GITHUB_RUN_ATTEMPT", "1")), executed=expected,
                  results={suite: results.get(suite, {}).get("result", "missing") for suite in expected})
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
    summary = "Selection: " + plan.get("reason", "affected inputs") + "\n"
    summary += "Tested base: `" + plan["base"] + "`; merge: `" + plan["testedMergeSHA"] + "`\n\n"
    summary += "| Check | Selected | Merge requirement | Changed inputs | Result |\n| --- | --- | --- | --- | --- |\n"
    for suite in suites(plan["project"], set(MODULES[plan["project"]])):
        chosen = suite in expected
        status = report["results"].get(suite, "not selected")
        summary += f"| {suite} | {'yes' if chosen else 'no'} | advisory | {selection_reason(plan, suite)} | {status} |\n"
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as out:
            out.write("## Affected module results\n" + summary + "\n")
    print(summary)
    if errors:
        raise GateFailure("; ".join(errors), report)
    if plan["project"] == "toolchain":
        require_current_pr(plan, fetch or api)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--project", choices=MODULES, required=True)
    p.add_argument("--event-path", required=True)
    p.add_argument("--repository", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--event-name")
    p = sub.add_parser("gate")
    p.add_argument("--plan", required=True)
    p.add_argument("--results", required=True)
    p.add_argument("--mode", choices=["all"], default="all")
    p.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.command == "plan":
        value = create_plan(args.project, json.loads(Path(args.event_path).read_text()), args.repository,
                            event_name=args.event_name)
        if os.environ.get("GITHUB_OUTPUT"):
            with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
                for key, item in value.items():
                    if isinstance(item, bool):
                        output.write(f"{key}={str(item).lower()}\n")
                output.write(f"security_languages={value['security_languages']}\nplan_file={args.output}\n")
    else:
        try:
            value = gate(json.loads(Path(args.plan).read_text()), json.loads(Path(args.results).read_text()))
        except GateFailure as error:
            Path(args.output).write_text(json.dumps(error.report, indent=2) + "\n")
            raise
    Path(args.output).write_text(json.dumps(value, indent=2) + "\n")


if __name__ == "__main__":
    main()
