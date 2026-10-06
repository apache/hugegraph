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


def verify_server():
    url = "http://localhost:8080"
    expect_response(url + "/graphspaces/DEFAULT/graphs", 401)
    graphs = json.loads(expect_response(url + "/graphspaces/DEFAULT/graphs",
                                        credentials=f"admin:{ADMIN_PASSWORD}"))
    names = graphs.get("graphs") if isinstance(graphs, dict) else None
    if not isinstance(names, list) or "hugegraph" not in names:
        raise RuntimeError("Server did not return its initialized hugegraph in the graphs array")
    payload = json.loads(expect_response(url + "/versions"))
    versions = payload.get("versions") if isinstance(payload, dict) else None
    if not isinstance(versions, dict) or versions.get("version") != "v1" or any(
            not isinstance(versions.get(key), str) or not versions[key]
            for key in ("core", "gremlin", "api")):
        raise RuntimeError("Server did not return the expected versions object")


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
