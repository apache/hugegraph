#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Generate a bounded, fresh official 1.7 HStore source; NOT an upgrade test."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tarfile
import time
import urllib.request
import zipfile

RELEASE = "https://github.com/apache/hugegraph/releases/download/1.7.0/apache-hugegraph-incubating-1.7.0.tar.gz"
RELEASE_SHA = "e9b95b5afac156f251a12134a03a27579cca9ed7cca7e42ad0529e639b1cde1e"
FIXTURE_SHA = "52c52778263e4ba81686a86de8eac2cf4b0df7ae"
FIXTURE = f"https://raw.githubusercontent.com/SebastianGruza/hugegraph-validation/{FIXTURE_SHA}/results/upgrade-170-to-master/scripts/hg_data.py"


def http(path, body=None):
    payload = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(path, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=60) as reply:
        raw = reply.read()
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw.decode()


def download(url, target, expected=None):
    h = hashlib.sha256()
    with urllib.request.urlopen(url, timeout=120) as response, target.open("xb") as output:
        while chunk := response.read(1024 * 1024):
            h.update(chunk)
            output.write(chunk)
    digest = h.hexdigest()
    if expected and digest != expected:
        raise RuntimeError(f"Checksum mismatch for {url}: {digest}")
    return digest


def property_file(path, updates):
    lines = path.read_text().splitlines()
    lines = [line for line in lines if line.partition("=")[0].strip() not in updates]
    path.write_text("\n".join(lines + [f"{k}={v}" for k, v in updates.items()]) + "\n")


def main():
    root = Path(sys.argv[1]).resolve()
    root.mkdir(parents=True, exist_ok=False)
    evidence = root / "evidence"
    evidence.mkdir()
    processes = []
    handles = []
    components = {}
    forced_stops = []

    def launch(name, command, cwd):
        log = (evidence / f"{name}.log").open("wb")
        handles.append(log)
        process = subprocess.Popen(command, cwd=cwd, stdin=subprocess.DEVNULL, stdout=log,
                                   stderr=subprocess.STDOUT)
        processes.append((name, process))
        return process

    def wait_port(process, port):
        deadline = time.monotonic() + 150
        while time.monotonic() < deadline and process.poll() is None:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1):
                    return
            except OSError:
                time.sleep(1)
        raise RuntimeError(f"Service did not listen on {port}; exit={process.poll()}")

    def stop(process):
        if process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=35)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
            forced_stops.append(process.pid)
            print("Stopped a stuck old-version process with SIGKILL; retain Raft for validation", flush=True)

    try:
        archive = root / "release.tar.gz"
        download(RELEASE, archive, RELEASE_SHA)
        with tarfile.open(archive) as tar:
            members = [entry for entry in tar.getmembers() if not Path(entry.name).name.startswith("._")]
            (evidence / "excluded-appledouble.txt").write_text("\n".join(
                entry.name for entry in tar.getmembers() if Path(entry.name).name.startswith("._")))
            tar.extractall(root / "release", members=members, filter="data")
        for name in ("pd", "store", "server"):
            matches = list((root / "release").glob(f"**/apache-hugegraph-{name}-incubating-1.7.0"))
            if len(matches) != 1:
                raise RuntimeError(f"Expected one {name} distribution, found {matches}")
            components[name] = matches[0]
        pd, store, server = (components[name] for name in ("pd", "store", "server"))
        config = pd / "conf/application.yml"
        config.write_text(config.read_text().replace("store-max-shard-count: 12", "store-max-shard-count: 2"))
        property_file(server / "conf/rest-server.properties", {
            "usePD": "true", "pd.peers": "127.0.0.1:8686", "memory_monitor.threshold": "1",
            "batch.max_write_threads": "2"})
        property_file(server / "conf/graphs/hugegraph.properties", {
            "backend": "hstore", "pd.peers": "127.0.0.1:8686", "vertex.cache_capacity": "1000",
            "edge.cache_capacity": "1000"})
        java17 = str(Path(os.environ["JAVA_HOME_17_X64"]) / "bin/java")
        java11 = str(Path(os.environ["JAVA_HOME_11_X64"]) / "bin/java")
        jvm = ["-Xms128m", "-Xmx768m", "-XX:ActiveProcessorCount=2",
               "--add-opens=java.base/java.lang=ALL-UNNAMED",
               "--add-opens=java.base/java.nio=ALL-UNNAMED",
               "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
               "--add-exports=java.base/jdk.internal.reflect=ALL-UNNAMED"]
        for name, component, pattern, port in (("pd", pd, "hg-pd-service-*.jar", 8686),
                                               ("store", store, "hg-store-node-*.jar", 8500)):
            jars = list((component / "lib").glob(pattern))
            if len(jars) != 1:
                raise RuntimeError(f"Ambiguous {name} executable: {jars}")
            process = launch(name, [java17, *jvm, f"-Dspring.config.location={component}/conf/application.yml",
                                    "-jar", str(jars[0])], component)
            wait_port(process, port)
        store_process = processes[-1][1]
        jars = sorted((server / "lib").rglob("*.jar"))
        # Prefer released modular Netty classes over an older bundled fat netty-all.
        modular_netty = [p for p in jars if p.name.startswith("netty-") and not p.name.startswith("netty-all-")]
        hugegraph = [p for p in jars if p.name.startswith("hugegraph")]
        ordered = list(dict.fromkeys(modular_netty + hugegraph + jars))
        ordered += sorted((server / "ext").rglob("*.jar")) + sorted((server / "plugins").rglob("*.jar"))
        classpath = os.pathsep.join(map(str, ordered))
        (evidence / "server-classpath.txt").write_text("\n".join(p.name for p in ordered))
        for attempt in range(3):
            with (evidence / f"init-{attempt}.log").open("wb") as output:
                result = subprocess.run([java11, *jvm, "-cp", classpath, "org.apache.hugegraph.cmd.InitStore",
                                         "conf/rest-server.properties"], cwd=server, input=b"\n",
                                        stdout=output, stderr=subprocess.STDOUT, timeout=120)
            if result.returncode == 0:
                break
            time.sleep(3)
        else:
            raise RuntimeError("Official 1.7 InitStore failed")
        sv = launch("server", [java11, *jvm, "-Dlog4j.configurationFile=conf/log4j2.xml", "-cp", classpath,
                               "org.apache.hugegraph.dist.HugeGraphServer", "conf/gremlin-server.yaml",
                               "conf/rest-server.properties"], server)
        wait_port(sv, 8080)
        deadline = time.monotonic() + 90
        while True:
            try:
                http("http://127.0.0.1:8080/graphs")
                break
            except Exception:
                if sv.poll() is not None or time.monotonic() > deadline:
                    raise
                time.sleep(1)
        fixture = root / "hg_data.py"
        fixture_digest = download(FIXTURE, fixture)
        for mode in ("write", "read"):
            with (evidence / f"fixture-{mode}.log").open("wb") as output:
                subprocess.run([sys.executable, str(fixture), mode, "http://127.0.0.1:8080",
                                str(evidence / f"fixture-{mode}.json")], stdout=output, stderr=subprocess.STDOUT,
                               check=True, timeout=180)
        baseline = json.loads((evidence / "fixture-read.json").read_text())
        checks = [entry for entry in baseline if entry["step"].startswith("gremlin ")]
        if len(checks) != 16 or any(entry["status"] != 200 for entry in checks):
            raise RuntimeError("The official source did not pass all 16 fixture requests")
        by_name = {entry["step"]: entry["data"] for entry in checks}
        if by_name["gremlin g.V().count()"] != [29] or by_name["gremlin g.E().count()"] != [63]:
            raise RuntimeError(f"Unexpected baseline counts: {by_name}")
        for graph in ("hugegraph", "~sys_graph"):
            query = "['~variables','~task','~server'].collectEntries { n -> [(n): graph.existsVertexLabel(n) ? [id:graph.vertexLabel(n).id().asLong(),name:n] : null] }"
            try:
                result = http("http://127.0.0.1:8080/gremlin", {
                    "gremlin": query, "language": "gremlin-groovy", "bindings": {},
                    "aliases": {"graph": "DEFAULT-" + graph, "g": "__g_DEFAULT-" + graph}})
            except Exception as error:
                result = {"error": str(error)}
            (evidence / f"hidden-labels-{graph}.json").write_text(json.dumps(result, default=str))
        stop(sv)
        snapshot_response = http("http://127.0.0.1:8520/test/snapshot")
        (evidence / "snapshot-response.json").write_text(json.dumps(snapshot_response))
        deadline = time.monotonic() + 120
        while True:
            snapshots = list((store / "storage/raft").glob("*/snapshot/snapshot_*/data/CURRENT"))
            if len(snapshots) >= 2:
                break
            if store_process.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError("Expected two persisted partition snapshots")
            time.sleep(1)
        time.sleep(3)
        stop(store_process)
        stop(processes[0][1])
        shutil.copytree(store / "storage", evidence / "store-data")
        if (pd / "pd_data").exists():
            shutil.copytree(pd / "pd_data", evidence / "pd-data")
        (evidence / "SOURCE_GENERATED_NOT_UPGRADED.json").write_text(json.dumps({
            "release_sha256": RELEASE_SHA, "fixture_revision": FIXTURE_SHA,
            "fixture_script_sha256": fixture_digest, "vertices": 29, "edges": 63,
            "gremlin_requests": 16, "snapshots_found": len(snapshots), "forced_stops": forced_stops,
            "warning": "Source generation only; no conversion, no version upgrade tested"}, indent=2))
        print("PASS: official 1.7 source created, 29 V / 63 E, 16 successful Gremlin requests; NOT UPGRADED", flush=True)
    finally:
        for _, process in reversed(processes):
            if process.poll() is None:
                try:
                    stop(process)
                except Exception as error:
                    print(f"Cleanup: {error}", flush=True)
        for name, component in components.items():
            for folder in ("conf", "logs"):
                if (component / folder).exists():
                    shutil.copytree(component / folder, evidence / name / folder, dirs_exist_ok=True)
        if "server" in components:
            libraries = evidence / "released-codecs"
            libraries.mkdir(exist_ok=True)
            prefixes = ("hugegraph-core-", "hugegraph-common-", "hg-store-core-", "hg-store-rocksdb-",
                        "hg-pd-grpc-", "hugegraph-struct-", "jraft-core-", "slf4j-api-", "commons-io-")
            for jar in (components["server"] / "lib").glob("*.jar"):
                if jar.name.startswith(prefixes):
                    shutil.copy2(jar, libraries / jar.name)
            for executable in (components["store"] / "lib").glob("hg-store-node-*.jar"):
                with zipfile.ZipFile(executable) as jar:
                    for entry in jar.namelist():
                        if entry.startswith("BOOT-INF/lib/") and Path(entry).name.startswith(prefixes):
                            (libraries / Path(entry).name).write_bytes(jar.read(entry))
            repo = Path(__file__).resolve().parents[5]
            subprocess.run(["git", "archive", "--format=tar.gz", "--output=" + str(evidence / "source-tree.tar.gz"),
                            "HEAD", "hugegraph-store", "hugegraph-struct", "hugegraph-server/hugegraph-core",
                            "hugegraph-commons", "hugegraph-pd"], cwd=repo, check=True)
        for handle in handles:
            handle.close()


if __name__ == "__main__":
    main()
