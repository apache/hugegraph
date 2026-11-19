#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Linux standalone RocksDB comparison; owns only the supplied NEW output directory.

Uses Python's standard library. Closed-loop clients validate every response.
ABBA restarts compare a forward-default baseline with the automatic-selection build.
Both builds share a persistent dataset and JVM settings.
Server CPU affinity: 0-5; load generator: 6-7. Requires at least eight CPUs.
"""

import argparse
import concurrent.futures
import gzip
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import platform
import resource
import shutil
import socket
import subprocess
import threading
import time
import urllib.parse


def save(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Client:
    def __init__(self, port):
        self.connection = http.client.HTTPConnection("127.0.0.1", port, timeout=40)

    def request(self, script, bindings=None, aliases=None, get=False):
        payload = {"gremlin": script, "language": "gremlin-groovy",
                   "bindings": bindings or {}, "aliases": aliases or {"g": "DEFAULT-hugegraph"}}
        if get:
            query = {"gremlin": script, "language": "gremlin-groovy"}
            query.update({"bindings." + key: value for key, value in payload["bindings"].items()})
            query.update({"aliases." + key: value for key, value in payload["aliases"].items()})
            self.connection.request("GET", "/gremlin?" + urllib.parse.urlencode(query))
        else:
            self.connection.request("POST", "/gremlin", json.dumps(payload),
                                    {"Content-Type": "application/json"})
        response = self.connection.getresponse()
        raw = response.read()
        return response.status, json.loads(raw), len(raw), response.getheader("Content-Type")

    def success(self, script, bindings=None, get=False):
        status, body, _, _ = self.request(script, bindings, get=get)
        assert status == 200, (status, body)
        assert body["status"]["code"] == 200, body
        return body["result"]["data"]

    def close(self):
        self.connection.close()


def percentile(values, quantile):
    return values[max(0, math.ceil(len(values) * quantile) - 1)]


def clear_records(client):
    deleted = 0
    while True:
        batch = client.success("ids=g.traversal().V().hasLabel('record').not(hasId(999998L))"
                               ".limit(1000).id().toList();"
                               "if(!ids.isEmpty()){g.traversal().V(ids).drop().iterate()};ids.size()")[0]
        deleted += batch
        if batch == 0:
            return deleted


def proc_sample(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().split()
    return {"cpu_ticks": int(fields[13]) + int(fields[14]),
            "rss_bytes": int(fields[23]) * os.sysconf("SC_PAGE_SIZE"),
            "threads": int(fields[19]), "host_load": os.getloadavg(),
            "host_cpu": Path("/proc/stat").read_text().splitlines()[0],
            "pressure": Path("/proc/pressure/cpu").read_text().strip()}


def workload(name, n):
    cases = {
        "constant": ("1+2", lambda i: {}, lambda data, i: data == [3]),
        "point": ("g.traversal().V(vid).values('name')",
                  lambda i: {"vid": i % n}, lambda data, i: data == [f"p{i % n}"]),
        "one_hop": ("g.traversal().V(vid).out('knows').id().order()",
                    lambda i: {"vid": i % n},
                    lambda data, i: data == sorted([(i % n + j) % n for j in range(1, 6)])),
        "two_hop": ("g.traversal().V(vid).out('knows').out('knows').id().order()",
                    lambda i: {"vid": i % n},
                    lambda data, i: data == sorted([(i % n + j + k) % n
                                                    for j in range(1, 6) for k in range(1, 6)])),
        "result_1000": ("g.traversal().V(ids).values('name').order()",
                        lambda i: {"ids": list(range(1000))},
                        lambda data, i: data == sorted([f"p{j}" for j in range(1000)])),
        "write": ("g.addVertex(org.apache.tinkerpop.gremlin.structure.T.label,'record',"
                  "org.apache.tinkerpop.gremlin.structure.T.id,vid,'payload',payload);vid",
                  lambda i: {"vid": 1000000 + i, "payload": "x" * 64},
                  lambda data, i: data == [1000000 + i]),
    }
    return cases[name]


def measure(port, pid, name, n, concurrency, duration, sequence, warmup=False):
    script, binding, validate = workload(name, n)
    barrier = threading.Barrier(concurrency + 1)
    stop = threading.Event()

    def worker(index):
        client = Client(port)
        samples, sizes, failures = [], [], []
        counter = sequence + index
        barrier.wait()
        try:
            while not stop.is_set():
                started = time.perf_counter_ns()
                try:
                    status, body, length, media = client.request(script, binding(counter))
                    data = body.get("result", {}).get("data")
                    assert status == 200 and body["status"]["code"] == 200, (status, body)
                    assert media.startswith("application/json"), media
                    assert validate(data, counter), (name, counter, data)
                    samples.append(time.perf_counter_ns() - started)
                    sizes.append(length)
                except Exception as error:
                    failures.append(repr(error)[:1000])
                    stop.set()
                counter += concurrency
        finally:
            client.close()
        return samples, sizes, failures

    before = proc_sample(pid)
    usage = resource.getrusage(resource.RUSAGE_SELF)
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(worker, i) for i in range(concurrency)]
        started = time.monotonic()
        barrier.wait()
        stop.wait(duration)
        stop.set()
        results = [future.result() for future in futures]
        elapsed = time.monotonic() - started
    after = proc_sample(pid)
    usage_after = resource.getrusage(resource.RUSAGE_SELF)
    samples = sorted(sample for result in results for sample in result[0])
    failures = [error for result in results for error in result[2]]
    assert samples and not failures, (name, failures)
    return {"workload": name, "concurrency": concurrency, "warmup": warmup,
            "seconds": elapsed, "completed": len(samples), "errors": failures,
            "throughput": len(samples) / elapsed,
            "p50_ms": percentile(samples, .50) / 1000000,
            "p95_ms": percentile(samples, .95) / 1000000,
            "p99_ms": percentile(samples, .99) / 1000000,
            "response_bytes_mean": sum(sum(result[1]) for result in results) / len(samples),
            "server_cpu_cores": (after["cpu_ticks"] - before["cpu_ticks"]) /
                                os.sysconf("SC_CLK_TCK") / elapsed,
            "client_cpu_cores": (usage_after.ru_utime + usage_after.ru_stime -
                                usage.ru_utime - usage.ru_stime) / elapsed,
            "before": before, "after": after, "latency_ns": samples}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--distribution", type=Path, required=True)
    parser.add_argument("--forward-distribution", type=Path, required=True,
                        help="Pre-automatic-selection build whose default REST path is forwarding")
    parser.add_argument("--java-home", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--vertices", type=int, default=10000)
    parser.add_argument("--seconds", type=float, default=3)
    parser.add_argument("--warmup-seconds", type=float, default=5)
    args = parser.parse_args()
    assert args.vertices >= 1000
    assert set(range(8)).issubset(os.sched_getaffinity(0))
    args.distribution = args.distribution.resolve()
    args.forward_distribution = args.forward_distribution.resolve()
    assert args.forward_distribution != args.distribution, "Use a separate forward-default baseline build"
    args.java_home = args.java_home.resolve()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    os.sched_setaffinity(0, {6, 7})
    dist, output = args.distribution, args.output
    java = str(args.java_home / "bin/java")
    assert (args.java_home / "conf/security/java.security").is_file(), "Incomplete Java 17 configuration"
    version = subprocess.check_output([java, "-version"], stderr=subprocess.STDOUT, text=True)
    assert 'version "17.' in version, version
    distributions = {"forward": args.forward_distribution, "embedded": dist}
    classpaths = {}
    for mode, distribution in distributions.items():
        assert (distribution / "lib").is_dir(), distribution
        jars = sorted((distribution / "lib").glob("hugegraph*.jar"))
        jars += sorted(p for p in (distribution / "lib").glob("*.jar")
                       if not p.name.startswith("hugegraph"))
        classpaths[mode] = ":".join(str(p) for p in jars)
    (output / "graphs").mkdir()
    (output / "graphs/hugegraph.properties").write_text(
        "gremlin.graph=org.apache.hugegraph.HugeFactory\nbackend=rocksdb\nserializer=binary\n"
        f"store=hugegraph\nrocksdb.data_path={output}/data\nrocksdb.wal_path={output}/data\n"
        "vertex.cache_capacity=10000\nedge.cache_capacity=50000\n")
    shutil.copytree(dist / "scripts", output / "scripts")
    rp, gp = free_port(), free_port()
    template = (dist / "conf/gremlin-server.yaml").read_text()
    (output / "gremlin.yaml").write_text(
        f"host: 127.0.0.1\nport: {gp}\ngremlinPool: 4\nthreadPoolBoss: 1\nthreadPoolWorker: 4\n" +
        template)
    common = (f"graphs={output}/graphs\nrestserver.url=http://127.0.0.1:{rp}\n"
              f"gremlinserver.url=http://127.0.0.1:{gp}\nrestserver.max_worker_threads=16\n"
              "gremlinserver.max_route=16\ngremlinserver.timeout=30\nmemory_monitor.threshold=1\n")
    for mode in ("forward", "embedded"):
        (output / f"{mode}.properties").write_text(common)
    jvm = [java, "-Xms1g", "-Xmx2g", "-XX:ActiveProcessorCount=6",
           "@" + str(dist / "bin/jvm-module.options"),
           "-Dlog4j.configurationFile=" + str(dist / "conf/log4j2.xml")]
    env = {**os.environ, "JAVA_HOME": str(args.java_home)}
    affinity = lambda: os.sched_setaffinity(0, set(range(6)))
    with (output / "init.log").open("w") as log:
        subprocess.run(jvm + ["-cp", classpaths["embedded"], "org.apache.hugegraph.cmd.InitStore",
                              str(output / "forward.properties")], cwd=output, env=env,
                       stdout=log, stderr=subprocess.STDOUT, check=True, timeout=300,
                       preexec_fn=affinity)
    result = {"environment": {"host": platform.node(), "platform": platform.platform(),
                              "java": version, "cpus": os.cpu_count(), "server_cpus": list(range(6)),
                              "client_cpus": [6, 7], "load_model": "closed-loop, same host",
                              "cache": "warm application/RocksDB/OS; no cache dropping",
                              "vertices": args.vertices, "edges": args.vertices * 5,
                              "duration_seconds": args.seconds, "mode_order": ["forward", "embedded",
                                                                                  "embedded", "forward"],
                              "warmup_seconds": args.warmup_seconds,
                              "comparison": "previous forward-default build vs automatic local execution",
                              "distribution_sha256": {
                                  mode: {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                         for p in (distribution / "lib").glob("hugegraph*.jar")}
                                  for mode, distribution in distributions.items()}},
              "runs": []}
    n, sequence = args.vertices, 0
    for run_id, mode in enumerate(result["environment"]["mode_order"]):
        run_dir = output / f"run-{run_id}-{mode}"
        run_dir.mkdir()
        with (run_dir / "server.log").open("w") as log:
            process = subprocess.Popen(
                jvm + [f"-Xlog:gc*:file={run_dir}/gc.log:time,uptime,level,tags",
                       "-cp", classpaths[mode], "org.apache.hugegraph.dist.HugeGraphServer",
                       str(output / "gremlin.yaml"), str(output / f"{mode}.properties")],
                cwd=output, env=env, stdout=log, stderr=subprocess.STDOUT, preexec_fn=affinity)
            run = {"id": run_id, "mode": mode, "checks": {}, "measurements": []}
            try:
                deadline = time.monotonic() + 300
                while True:
                    assert process.poll() is None, f"Server exited; see {run_dir}/server.log"
                    try:
                        client = Client(rp)
                        assert client.success("1+2") == [3]
                        break
                    except (OSError, AssertionError, json.JSONDecodeError):
                        client.close()
                        if time.monotonic() > deadline:
                            raise
                        time.sleep(.5)
                if run_id == 0:
                    client.success("s=g.schema();s.propertyKey('name').asText().ifNotExist().create();"
                                   "s.propertyKey('payload').asText().ifNotExist().create();"
                                   "s.vertexLabel('person').useCustomizeNumberId().properties('name')"
                                   ".ifNotExist().create();"
                                   "s.vertexLabel('record').useCustomizeNumberId().properties('payload')"
                                   ".ifNotExist().create();"
                                   "s.edgeLabel('knows').sourceLabel('person').targetLabel('person')"
                                   ".ifNotExist().create();'schema-ok'")
                    for begin in range(0, n, 200):
                        client.success("(begin..<end).each{i->g.addVertex("
                                       "org.apache.tinkerpop.gremlin.structure.T.label,'person',"
                                       "org.apache.tinkerpop.gremlin.structure.T.id,i.longValue(),"
                                       "'name','p'+i)};end",
                                       {"begin": begin, "end": min(begin + 200, n)})
                    for begin in range(0, n, 100):
                        client.success("(begin..<end).each{i->v=g.traversal().V(i).next();"
                                       "(1..5).each{j->v.addEdge('knows',"
                                       "g.traversal().V((i+j)%n).next())}};end",
                                       {"begin": begin, "end": min(begin + 100, n), "n": n})
                    print("dataset loaded", flush=True)
                assert client.success("g.traversal().V().hasLabel('person').count()") == [n]
                assert client.success("g.traversal().E().count()") == [n * 5]
                clear_records(client)
                assert client.success("x+y", {"x": 2, "y": 5}) == [7]
                assert client.success("1+2", get=True) == [3]
                nested = client.success("[['text':'中文','items':[null,1,true]]]")
                assert nested == [{"text": "中文", "items": [None, 1, True]}], nested
                status, body, _, _ = client.request("1", aliases={"g": "missing_graph"})
                assert status == 400, (status, body)
                run["checks"]["unknown_alias"] = {"status": status, "body": body}
                status, body, _, _ = client.request(
                    "g.addVertex(org.apache.tinkerpop.gremlin.structure.T.label,'record',"
                    "org.apache.tinkerpop.gremlin.structure.T.id,999999L,'payload','rollback');"
                    "throw new IllegalArgumentException('rollback')")
                assert status == 400, (status, body)
                assert client.success("g.traversal().V(999999L).count()") == [0]
                run["checks"]["rollback"] = {"status": status, "exception": body.get("exception")}
                if run_id > 0:
                    assert client.success("g.traversal().V(999998L).values('payload')") == ["persisted"]
                else:
                    client.success("g.addVertex(org.apache.tinkerpop.gremlin.structure.T.label,'record',"
                                   "org.apache.tinkerpop.gremlin.structure.T.id,999998L,"
                                   "'payload','persisted');true")
                run["checks"]["get_post_bindings_json_alias_rollback_persistence"] = "passed"
                client.close()
                for name in ("constant", "point", "one_hop", "two_hop", "result_1000", "write"):
                    warm = measure(rp, process.pid, name, n, 4, args.warmup_seconds, sequence, warmup=True)
                    sequence += 1000000
                    written = warm["completed"] if name == "write" else 0
                    for concurrency in (1, 8, 32):
                        measurement = measure(rp, process.pid, name, n, concurrency,
                                              args.seconds, sequence)
                        sequence += 1000000
                        if name == "write":
                            written += measurement["completed"]
                        raw = measurement.pop("latency_ns")
                        sample_file = f"{name}-c{concurrency}-latency-ns.json.gz"
                        with gzip.open(run_dir / sample_file, "wt") as sample:
                            json.dump(raw, sample)
                        measurement["samples"] = f"{run_dir.name}/{sample_file}"
                        run["measurements"].append(measurement)
                        save(output / "progress.json", {"run": run_id, "mode": mode,
                                                       "last": measurement})
                        print(json.dumps({"run": run_id, "mode": mode, "workload": name,
                                          "concurrency": concurrency, "n": measurement["completed"],
                                          "rps": round(measurement["throughput"], 1),
                                          "p95_ms": round(measurement["p95_ms"], 3)}), flush=True)
                    if name == "write":
                        client = Client(rp)
                        assert client.success("g.traversal().V().hasLabel('record').count()") == [written + 1]
                        run["checks"]["committed_writes"] = written
                        assert clear_records(client) == written
                client.close()
            finally:
                if process.poll() is None:
                    process.terminate()
                try:
                    process.wait(timeout=90)
                    run["shutdown_exit"] = process.returncode
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise RuntimeError(f"Shutdown timed out: {run_dir}")
            result["runs"].append(run)
            save(output / "results.json", result)
    print("all runs completed: " + str(output / "results.json"), flush=True)


if __name__ == "__main__":
    main()
