#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#/

"""Run the script-only REST transport comparison after the focused Maven tests."""

import argparse
import datetime
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import xml.etree.ElementTree as ET


def main():
    module = Path(__file__).resolve().parents[1]
    repo = module.parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path,
                        default=module / "target/gremlin-rest-benchmark.json")
    args = parser.parse_args()
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    report = module / "target/surefire-reports/TEST-org.apache.hugegraph.auth.EmbeddedGremlinRestExecutorTest.xml"
    if not report.exists():
        parser.error("Run EmbeddedGremlinRestExecutorTest with Maven first")
    properties = {entry.attrib["name"]: entry.attrib["value"]
                  for entry in ET.parse(report).getroot().find("properties")}
    java = str(Path(os.environ["JAVA_HOME"]) / "bin/java") if "JAVA_HOME" in os.environ else shutil.which("java")
    if not java:
        parser.error("Java 17 is required; set JAVA_HOME")
    version = subprocess.check_output([java, "-version"], stderr=subprocess.STDOUT, text=True)
    if 'version "17.' not in version:
        parser.error("Use Java 17 to match the supported Server runtime")
    started = datetime.datetime.now(datetime.timezone.utc).isoformat()
    subprocess.run([
        java,
        "@" + str(repo / "hugegraph-server/hugegraph-dist/src/assembly/static/bin/jvm-module.options"),
        "@" + str(module / "conf/jvm-test-module.options"),
        "-cp", properties["java.class.path"],
        "org.apache.hugegraph.auth.EmbeddedGremlinRestExecutorTest", str(output),
    ], cwd=module, check=True)
    data = json.loads(output.read_text())
    data["environment"] = {
        "started_at_utc": started,
        "base_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip(),
        "working_tree_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=repo)),
        "architecture": platform.machine(),
        "platform": platform.platform(),
        "authentication": "anonymous",
        "gremlin_workers": 1,
        "rest_workers_per_mode": 2,
        "clients": 1,
        "warmup_requests_per_mode_per_script": 100,
        "rounds": 4,
        "requests_per_mode_per_round": 200,
        "ordering": "alternate forward/embedded order across rounds",
        "limitations": "same-host single-JVM script-only comparison; no backend I/O, CPU isolation or production load",
    }
    output.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")
    print(f"Saved correctness-checked samples to {output}")


if __name__ == "__main__":
    main()
