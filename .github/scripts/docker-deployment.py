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

"""Start the four locally built PR images using the shipped Compose files."""

import base64
import json
import os
from pathlib import Path
import subprocess
import sys
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET


ADMIN_PASSWORD = "ci-compose-password"
PD_PASSWORD = "ci-compose-pd-secret"


def command(*args):
    return subprocess.check_output(args, text=True, timeout=60).strip()


def verify_image(container, image, expected):
    actual = command("docker", "inspect", "--format", "{{.Image}}", container)
    if not expected or actual != expected:
        raise RuntimeError(f"Container {container} uses {actual}; expected {image} ({expected})")


def response(url, credentials=None):
    request = urllib.request.Request(url)
    if credentials:
        encoded = base64.b64encode(credentials.encode()).decode()
        request.add_header("Authorization", "Basic " + encoded)
    try:
        with urllib.request.urlopen(request, timeout=15) as result:
            return result.status, result.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def expect_response(url, expected=200, credentials=None):
    actual, body = response(url, credentials)
    if actual != expected:
        raise RuntimeError(f"{url}: HTTP {actual}, expected {expected}")
    return body


def expected_versions():
    root = Path(__file__).resolve().parents[2]
    ns = {"m": "http://maven.apache.org/POM/4.0.0"}
    revision = ET.parse(root / "pom.xml").findtext("m:properties/m:revision", namespaces=ns)
    gremlin = ET.parse(root / "hugegraph-server/pom.xml").findtext(
        "m:properties/m:tinkerpop.version", namespaces=ns)
    properties = dict(line.split("=", 1) for line in
                      (root / "hugegraph-commons/hugegraph-common/src/main/resources/version.properties")
                      .read_text().splitlines() if "=" in line and not line.startswith("#"))
    if properties["VersionInBash"] != revision:
        raise RuntimeError("VersionInBash does not match the project revision")
    # Packaged API classes use their manifest version before the resource fallback.
    api = ET.parse(root / "hugegraph-server/hugegraph-api/pom.xml").findtext(
        ".//m:manifestEntries/m:Implementation-Version", namespaces=ns) or properties["ApiVersion"]
    expected = {"version": "v1", "core": revision, "gremlin": gremlin, "api": api}
    if any(not isinstance(value, str) or not value for value in expected.values()):
        raise RuntimeError("Source POMs did not define non-empty expected versions")
    return expected


def verify_versions(payload):
    versions = payload.get("versions") if isinstance(payload, dict) else None
    expected = expected_versions()
    if not isinstance(versions, dict) or any(versions.get(key) != value for key, value in expected.items()):
        raise RuntimeError(f"Server did not return the expected versions object: {expected}; got {versions}")


def verify_server():
    url = "http://localhost:8080"
    expect_response(url + "/graphspaces/DEFAULT/graphs", 401)
    graphs = json.loads(expect_response(url + "/graphspaces/DEFAULT/graphs",
                                        credentials=f"admin:{ADMIN_PASSWORD}"))
    names = graphs.get("graphs") if isinstance(graphs, dict) else None
    if not isinstance(names, list) or "hugegraph" not in names:
        raise RuntimeError("Server did not return its initialized hugegraph in the graphs array")
    payload = json.loads(expect_response(url + "/versions"))
    verify_versions(payload)


def verify_storage():
    for port in (8620, 8520):
        if expect_response(f"http://localhost:{port}/v1/health") != "":
            raise RuntimeError(f"Port {port} did not return the PD/Store health response")
    ready = json.loads(expect_response("http://localhost:8620/v1/ready"))
    if not isinstance(ready, dict) or ready.get("ready") is not True:
        raise RuntimeError("PD did not report ready=true")
    url = "http://localhost:8620/v1/stores"
    expect_response(url, 401)
    payload = json.loads(expect_response(url, credentials=f"hg:{PD_PASSWORD}"))
    data = payload.get("data") if isinstance(payload, dict) else None
    if (not isinstance(data, dict) or payload.get("status") != 0 or
            not isinstance(data.get("stores"), list) or not data["stores"]):
        raise RuntimeError("Authenticated PD did not return its registered stores array")


def verify_graph(run_id):
    root = Path(__file__).resolve().parents[2]
    script = root / "hugegraph-server/hugegraph-dist/src/assembly/travis/run-server-e2e-smoke-test.sh"
    environment = dict(os.environ, HUGEGRAPH_USERNAME="admin", HUGEGRAPH_PASSWORD=ADMIN_PASSWORD)
    # Reuse the write/read/Gremlin assertions; allow its readiness and bounded HTTP requests.
    subprocess.run(["bash", str(script), "http://localhost:8080", "create", run_id],
                   check=True, timeout=960, env=environment)


def smoke(tag, topology, images):
    root = Path(__file__).resolve().parents[2]
    project = f"hg-pr-{tag}-{Path(topology).stem}"
    compose = ["docker", "compose", "-p", project, "-f", str(root / "docker" / topology)]
    # Capture identities before startup so a tag change cannot bless an unrelated container.
    expected = {service: command("docker", "image", "inspect", "--format", "{{.Id}}",
                                 f"{repository}:{tag}") for service, repository in images.items()}
    failed = False
    try:
        subprocess.run(compose + ["up", "-d", "--pull", "never", "--wait",
                                  "--wait-timeout", "600"] + list(images), check=True, timeout=660)
        for service, repository in images.items():
            container = command(*compose, "ps", "-q", service)
            if not container or "\n" in container:
                raise RuntimeError(f"Expected exactly one {service} container")
            verify_image(container, f"{repository}:{tag}", expected[service])
            health = command("docker", "inspect", "--format", "{{.State.Health.Status}}", container)
            if health != "healthy":
                raise RuntimeError(f"{service} is {health}")
        verify_server()
        if "pd" in images:
            verify_storage()
        verify_graph(project.replace("-", "_"))
    except BaseException:
        failed = True
        for args in (["ps"], ["logs", "--no-color", "--tail", "200"]):
            try:
                subprocess.run(compose + args, check=False, timeout=60)
            except (OSError, subprocess.SubprocessError) as error:
                print(f"Could not collect Compose diagnostics: {error}", file=sys.stderr)
        raise
    finally:
        try:
            subprocess.run(compose + ["down", "-v", "--remove-orphans"], check=True, timeout=120)
        except (OSError, subprocess.SubprocessError) as error:
            if not failed:
                raise
            print(f"Compose cleanup also failed: {error}", file=sys.stderr)


def main(tag):
    if not tag.startswith("ci-") or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789-" for c in tag):
        raise ValueError("Use a unique ci- image tag")
    os.environ.update(HUGEGRAPH_VERSION=tag, HUGEGRAPH_PULL_POLICY="never",
                      HUGEGRAPH_ADMIN_PASSWORD=ADMIN_PASSWORD,
                      HUGEGRAPH_AUTH_TOKEN_SECRET="0123456789abcdef" * 4,
                      HG_PD_AUTH_SECRET_KEY=PD_PASSWORD)
    smoke(tag, "docker-compose.yml", {"server": "hugegraph/hugegraph"})
    smoke(tag, "docker-compose-hstore.yml", {"pd": "hugegraph/pd", "store": "hugegraph/store",
                                           "server": "hugegraph/server"})


if __name__ == "__main__":
    main(sys.argv[1])
