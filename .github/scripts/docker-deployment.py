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
import re
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


ADMIN_PASSWORD = "ci-compose-password"
PD_PASSWORD = "ci-compose-pd-secret"
GRAPH_CONFIG_LINK = "../rocksdb-data/.hugegraph-config/graphs"
PERSISTED_GRAPH = "ci_persisted"
PERSISTED_VERTEX = "retained-vertex"


def command(*args):
    return subprocess.check_output(args, text=True, timeout=60).strip()


def verify_image(container, image, expected):
    actual = command("docker", "inspect", "--format", "{{.Image}}", container)
    if not expected or actual != expected:
        raise RuntimeError(f"Container {container} uses {actual}; expected {image} ({expected})")


def response(url, credentials=None, *, method=None, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        request.add_header("Content-Type", "application/json")
    if credentials:
        encoded = base64.b64encode(credentials.encode()).decode()
        request.add_header("Authorization", "Basic " + encoded)
    try:
        with urllib.request.urlopen(request, timeout=15) as result:
            return result.status, result.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def expect_response(url, expected=200, credentials=None, **request_options):
    actual, body = response(url, credentials, **request_options)
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


def create_persisted_graph():
    url = f"http://localhost:8080/graphspaces/DEFAULT/graphs/{PERSISTED_GRAPH}"
    credentials = f"admin:{ADMIN_PASSWORD}"
    expect_response(url, 201, credentials, method="POST", payload={
        "backend": "rocksdb",
        "rocksdb.data_path": "/hugegraph-server/rocksdb-data/ci-persisted/data",
        "rocksdb.wal_path": "/hugegraph-server/rocksdb-data/ci-persisted/wal",
    })
    expect_response(url + "/schema/propertykeys", 202, credentials, method="POST",
                    payload={"name": "name", "data_type": "TEXT", "cardinality": "SINGLE"})
    expect_response(url + "/schema/vertexlabels", 201, credentials, method="POST",
                    payload={"name": "retained", "id_strategy": "CUSTOMIZE_STRING",
                             "properties": ["name"]})
    expect_response(url + "/graph/vertices", 201, credentials, method="POST",
                    payload={"id": PERSISTED_VERTEX, "label": "retained",
                             "properties": {"name": "survives-recreate"}})
    verify_persisted_graph()


def verify_persisted_graph():
    url = f"http://localhost:8080/graphspaces/DEFAULT/graphs/{PERSISTED_GRAPH}"
    credentials = f"admin:{ADMIN_PASSWORD}"
    vertex_id = urllib.parse.quote(json.dumps(PERSISTED_VERTEX), safe="")
    vertex = json.loads(expect_response(url + "/graph/vertices/" + vertex_id,
                                        credentials=credentials))
    if vertex.get("properties", {}).get("name") != "survives-recreate":
        raise RuntimeError("API-created graph data did not survive container replacement")


def verify_standalone_recreate(compose, container, image, expected):
    # GraphManager writes dynamic definitions through conf/graphs. The image
    # redirects that path into the existing data volume; prove a real Compose
    # replacement keeps the path and does not merely pass a static Dockerfile check.
    link = command("docker", "exec", container, "readlink", "/hugegraph-server/conf/graphs")
    if link != GRAPH_CONFIG_LINK:
        raise RuntimeError(f"Graph configuration does not use the data volume: {link}")
    create_persisted_graph()
    subprocess.run(compose + ["up", "-d", "--pull", "never", "--force-recreate",
                              "--no-deps", "--wait", "--wait-timeout", "600", "server"],
                   check=True, timeout=660)
    replacement = command(*compose, "ps", "-q", "server")
    if not replacement or "\n" in replacement or replacement == container:
        raise RuntimeError("Compose did not replace the standalone Server container")
    verify_image(replacement, image, expected)
    health = command("docker", "inspect", "--format", "{{.State.Health.Status}}", replacement)
    if health != "healthy":
        raise RuntimeError(f"Replacement Server is {health}")
    verify_server()
    verify_persisted_graph()


def verify_readonly_graphs(image):
    """Start actual images with directory and individual-file read-only binds."""
    environment = json.loads(command("docker", "image", "inspect", "--format",
                                     "{{json .Config.Env}}", image))
    selected = dict(item.split("=", 1) for item in environment if "=" in item)[
        "TOPLINGDB_ROCKSDB_PROVIDER"]
    if selected not in {"rocksdb", "topling"}:
        raise RuntimeError(f"Unexpected image provider: {selected}")
    default = command("docker", "run", "--rm", "--pull", "never", "--entrypoint", "cat",
                      image, "/hugegraph-server/conf/graphs.default/hugegraph.properties")
    default = re.sub(r"(?m)^[ \t]*rocksdb\.provider\s*[:=].*$", "", default) + "\n"
    with tempfile.TemporaryDirectory(prefix="hg-readonly-") as temporary:
        root = Path(temporary)
        for mount_kind in ("directory", "file"):
            for configured in (None, "rocksdb", "topling"):
                case = f"{mount_kind}-{configured or 'default'}"
                graphs = root / case
                graphs.mkdir()
                # The private temp parent stays 0700. Give the image UID baseline
                # access to these non-secret fixtures so only the read-only bind
                # denies writes, independently of the runner's umask/identity.
                graphs.chmod(0o777)
                config = graphs / "hugegraph.properties"
                config.write_text(default + (f"rocksdb.provider={configured}\n" if configured else ""))
                config.chmod(0o666)
                original = config.read_bytes()
                source = graphs if mount_kind == "directory" else config
                destination = "/hugegraph-server/conf/graphs"
                if mount_kind == "file":
                    destination += "/hugegraph.properties"
                name = root.name + "-" + case
                should_start = (configured or "rocksdb") == selected
                try:
                    container = command("docker", "run", "--detach", "--pull", "never", "--name", name,
                                        "--publish", "127.0.0.1::8080", "--mount",
                                        f"type=bind,src={source},dst={destination},readonly", image)
                    port = (command("docker", "port", container, "8080/tcp").rsplit(":", 1)[1]
                            if should_start else None)
                    deadline = time.monotonic() + 180
                    while True:
                        state = json.loads(command("docker", "inspect", "--format",
                                                   "{{json .State}}", container))
                        if not state["Running"]:
                            break
                        if should_start:
                            try:
                                if response(f"http://127.0.0.1:{port}/graphspaces/DEFAULT/graphs")[0] == 200:
                                    break
                            except (urllib.error.URLError, ConnectionError, TimeoutError):
                                pass
                        if time.monotonic() >= deadline:
                            raise RuntimeError(f"Read-only graph case timed out: {case}")
                        time.sleep(1)
                    if should_start:
                        if not state["Running"]:
                            raise RuntimeError(f"Matching read-only graph did not start: {case}")
                        probe = subprocess.run(["docker", "exec", container, "sh", "-c",
                                                "printf probe >> /hugegraph-server/conf/graphs/hugegraph.properties"],
                                               capture_output=True, text=True, timeout=30)
                        if probe.returncode == 0 or "Read-only file system" not in probe.stderr:
                            raise RuntimeError(f"Graph configuration mount is not read-only: {case}")
                        subprocess.run(["docker", "stop", "--time", "60", container], check=True, timeout=90)
                        stopped = json.loads(command("docker", "inspect", "--format",
                                                     "{{json .State}}", container))
                        if stopped["OOMKilled"] or stopped["ExitCode"] not in {0, 143}:
                            raise RuntimeError(f"Read-only graph shutdown failed: {case}: {stopped}")
                    else:
                        logs = command("docker", "logs", container)
                        expected = f"cannot change rocksdb.provider from '{configured or 'rocksdb'}'"
                        if state["ExitCode"] == 0 or expected not in logs or \
                           f"'{selected}' in ./conf/graphs/hugegraph.properties" not in logs or \
                           "Initializing HugeGraph Store" in logs:
                            raise RuntimeError(f"Mismatched read-only graph did not fail clearly: {case}: {logs}")
                    if config.read_bytes() != original:
                        raise RuntimeError(f"Read-only graph configuration changed: {case}")
                    print(f"PASS: {selected} {case} read-only graph configuration", flush=True)
                except BaseException:
                    subprocess.run(["docker", "logs", name], check=False, timeout=30)
                    raise
                finally:
                    subprocess.run(["docker", "rm", "--force", "--volumes", name], check=True, timeout=120)


def smoke(tag, topology, images):
    root = Path(__file__).resolve().parents[2]
    project = f"hg-pr-{tag}-{Path(topology).stem}"
    compose = ["docker", "compose", "-p", project, "-f", str(root / "docker" / topology)]
    # Capture identities before startup so a tag change cannot bless an unrelated container.
    expected = {service: command("docker", "image", "inspect", "--format", "{{.Id}}",
                                 f"{repository}:{tag}") for service, repository in images.items()}
    failed = False
    containers = {}
    try:
        subprocess.run(compose + ["up", "-d", "--pull", "never", "--wait",
                                  "--wait-timeout", "600"] + list(images), check=True, timeout=660)
        for service, repository in images.items():
            container = command(*compose, "ps", "-q", service)
            if not container or "\n" in container:
                raise RuntimeError(f"Expected exactly one {service} container")
            containers[service] = container
            verify_image(container, f"{repository}:{tag}", expected[service])
            health = command("docker", "inspect", "--format", "{{.State.Health.Status}}", container)
            if health != "healthy":
                raise RuntimeError(f"{service} is {health}")
        verify_server()
        if "pd" in images:
            verify_storage()
        verify_graph(project.replace("-", "_"))
        if topology == "docker-compose.yml":
            verify_standalone_recreate(compose, containers["server"],
                                       f"{images['server']}:{tag}", expected["server"])
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
    verify_readonly_graphs(f"hugegraph/hugegraph:{tag}")
    smoke(tag, "docker-compose-hstore.yml", {"pd": "hugegraph/pd", "store": "hugegraph/store",
                                           "server": "hugegraph/server"})


if __name__ == "__main__":
    main(sys.argv[1])
