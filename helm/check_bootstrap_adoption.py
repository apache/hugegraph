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

"""Read-only, same-topology adoption checks; does not verify graph contents.

Prepare a reachable PD URL and one Store URL per ordinal (for example, separate
operator-managed Pod port-forwards). Verify each URL's target Pod yourself; a
load-balanced Service cannot stand in for individual Store observations. Store
IDs below are PD observations; the Store API does not attest the serving Pod's
raw persisted identity. Pause concurrent operations while checking. PASS is a
necessary observation check, not proof of graph contents or safe recovery.
"""

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


class CheckFailed(Exception):
    pass


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        # Never forward management credentials to another host or URL.
        return None


def require(condition, message):
    if not condition:
        raise CheckFailed(message)


def endpoint(peer):
    address = peer.get("endpoint", peer)
    return f"{address['ip']}:{address['port']}"


def validate_kubernetes(resources, volumes, args):
    """Bind each ordinal to its controller, mounted PVC and backing PV claim UID."""
    objects = {(item["kind"], item["metadata"]["name"]): item for item in resources}
    pvs = {item["metadata"]["name"]: item for item in volumes}
    result = {"controllers": {}, "pods": {}, "volumes": {}}
    addresses = {}
    for role, count in (("pd", args.pd_count), ("store", args.store_count)):
        name = args.fullname + "-" + role
        controller = objects[("StatefulSet", name)]
        spec, meta = controller["spec"], controller["metadata"]
        require(meta["labels"].get("app.kubernetes.io/instance") == args.release,
                f"{role}: unexpected release owner")
        require(spec["replicas"] == count and spec["serviceName"] == name,
                f"{role}: replica count or stable service name differs")
        claims = spec.get("volumeClaimTemplates", [])
        require(len(claims) == 1 and claims[0]["metadata"]["name"] == role + "-data",
                f"{role}: expected the chart's single persistent data claim")
        container = next(item for item in spec["template"]["spec"]["containers"]
                         if item["name"] == role)
        ports = {item["name"]: item["containerPort"] for item in container["ports"]}
        addresses[role] = {f"{name}-{i}.{name}.{args.namespace}.svc:{ports['raft']}"
                           for i in range(count)}
        if role == "store":
            addresses["storeGrpc"] = {
                f"{name}-{i}.{name}.{args.namespace}.svc:{ports['grpc']}" for i in range(count)}
        result["controllers"][name] = {
            "uid": meta["uid"], "templateSha256": hashlib.sha256(
                json.dumps(spec["template"], sort_keys=True).encode()).hexdigest()}
        expected_pods = {f"{name}-{i}" for i in range(count)}
        expected_claims = {role + "-data-" + pod for pod in expected_pods}
        claim_pattern = re.compile(re.escape(role + "-data-" + name) + r"-[0-9]+$")
        actual_claims = {key[1] for key in objects if key[0] == "PersistentVolumeClaim"
                         and claim_pattern.fullmatch(key[1])}
        require(actual_claims == expected_claims, f"{role}: missing or orphaned data PVC")
        actual_pods = {key[1] for key, item in objects.items() if key[0] == "Pod"
                       and any(owner.get("uid") == meta["uid"]
                               for owner in item["metadata"].get("ownerReferences", []))}
        require(actual_pods == expected_pods, f"{role}: unexpected or missing Pod ordinals")
        for pod_name in sorted(expected_pods):
            pod = objects[("Pod", pod_name)]
            require(not pod["metadata"].get("deletionTimestamp"), f"{pod_name}: terminating")
            require(any(c.get("type") == "Ready" and c.get("status") == "True"
                        for c in pod.get("status", {}).get("conditions", [])),
                    f"{pod_name}: not Ready (readiness alone does not prove recovery)")
            claim_name = role + "-data-" + pod_name
            require(any(v.get("persistentVolumeClaim", {}).get("claimName") == claim_name
                        and v["name"] == role + "-data"
                        for v in pod["spec"].get("volumes", [])),
                    f"{pod_name}: mounted data claim differs")
            pvc = objects[("PersistentVolumeClaim", claim_name)]
            require(not pvc["metadata"].get("deletionTimestamp") and
                    pvc.get("status", {}).get("phase") == "Bound",
                    f"{claim_name}: PVC is not stably Bound")
            pv = pvs[pvc["spec"]["volumeName"]]
            ref = pv["spec"]["claimRef"]
            require((ref.get("namespace"), ref.get("name"), ref.get("uid")) ==
                    (args.namespace, claim_name, pvc["metadata"]["uid"]),
                    f"{claim_name}: PV claimRef identity differs")
            result["pods"][pod_name] = pod["metadata"]["uid"]
            result["volumes"][claim_name] = {
                "pvcUid": pvc["metadata"]["uid"], "pvUid": pv["metadata"]["uid"],
                "volumeName": pvc["spec"]["volumeName"]}
    server = objects.get(("Deployment", args.fullname + "-server"))
    if server:
        require(server["metadata"]["labels"].get("app.kubernetes.io/instance") == args.release,
                "Server: unexpected release owner")
        result["controllers"][server["metadata"]["name"]] = {
            "uid": server["metadata"]["uid"], "templateSha256": hashlib.sha256(
                json.dumps(server["spec"]["template"], sort_keys=True).encode()).hexdigest()}
        selector = server["spec"]["selector"]["matchLabels"]
        require(selector.get("app.kubernetes.io/instance") == args.release and
                selector.get("app.kubernetes.io/component") == "server",
                "Server: selector identity differs")
        pods = [item for (kind, _), item in objects.items() if kind == "Pod" and
                all(item["metadata"].get("labels", {}).get(k) == v for k, v in selector.items())]
        require(len(pods) == server["spec"]["replicas"], "Server: missing or extra Pod")
        for pod in pods:
            require(not pod["metadata"].get("deletionTimestamp") and
                    any(c.get("type") == "Ready" and c.get("status") == "True"
                        for c in pod.get("status", {}).get("conditions", [])),
                    "Server: rollout or unavailable Pod")
            result["pods"][pod["metadata"]["name"]] = pod["metadata"]["uid"]
    return result, addresses


def validate_database(members, cluster, stores, groups, engines, addresses, replica_count):
    """Require fully observed, stable voters rather than desired PD shard metadata alone."""
    voters = members["data"]["pdList"]
    require(len(voters) == len(addresses["pd"]) and
            {member["raftUrl"] for member in voters} == addresses["pd"],
            "PD: actual member endpoints differ from same-topology expectations")
    require(all(m["state"] == "Up" and m["role"] in ("Leader", "Follower") for m in voters)
            and sum(m["role"] == "Leader" for m in voters) == 1,
            "PD: unavailable voter, learner or missing/duplicate leader")
    require(cluster["data"]["shardCount"] == replica_count,
            "persisted PD replica policy differs from --replica-count")
    require(cluster["data"]["state"] == "Cluster_OK" and
            cluster["data"].get("dataState") == "PState_Normal",
            "PD: cluster or data state is not healthy")
    store_list = stores["data"]["stores"]
    require(len(store_list) == len(addresses["store"]) and
            {s["address"] for s in store_list} == addresses["storeGrpc"] and
            {s["raftAddress"] for s in store_list} == addresses["store"],
            "Store: registration endpoints/count differ from the expected topology")
    require(all(s["state"] == "Up" for s in store_list),
            "Store: Pending/Exiting/Offline/Tombstone is not eligible for adoption")
    identities = {str(s["storeId"]): s["raftAddress"] for s in store_list}
    require(len(identities) == len(store_list) and "0" not in identities,
            "Store: duplicate or unassigned observed identity")
    expected = {int(group.get("id", 0)): group for group in groups["shardGroups"]}
    require(bool(expected) and len(expected) == len(groups["shardGroups"]),
            "no existing shard groups or duplicate group IDs; seed and verify data first")
    actual = {group: [] for group in expected}
    leaders = {}
    for state in engines:
        require(state.get("status") == 0, "Store: partition query did not succeed")
        seen = set()
        for engine in state["partitions"]:
            group = int(engine["groupId"])
            require(group in actual and group not in seen,
                    "Store: unknown or duplicate local Raft group")
            seen.add(group)
            require(engine["role"] in ("STATE_LEADER", "STATE_FOLLOWER"),
                    f"group {group}: election or membership transition in progress")
            require(engine["logIndex"] > 0 and engine["partitions"] and
                    all(p["workState"] == "PState_Normal" and p["leader"] == "OK"
                        for p in engine["partitions"]),
                    f"group {group}: partition state is not recovered")
            actual[group].append(engine)
            if engine["role"] == "STATE_LEADER":
                require(group not in leaders, f"group {group}: multiple observed leaders")
                leaders[group] = engine
    memberships = {}
    for group, metadata in expected.items():
        shards = metadata["shards"]
        require(metadata.get("state", "PState_Normal") == "PState_Normal" and
                len(shards) == replica_count and
                all(s["role"] in ("Leader", "Follower") for s in shards),
                f"group {group}: PD placement RF/roles differ")
        target = {identities[str(s["storeId"])] for s in shards}
        require(len(target) == replica_count, f"group {group}: duplicate voter identity")
        require(group in leaders and len(actual[group]) == replica_count,
                f"group {group}: missing leader or local engine replica")
        leader = leaders[group]
        peers = {endpoint(p) for p in leader["peers"]}
        require(not leader.get("learners") and len(leader["peers"]) == replica_count
                and peers == target, f"group {group}: actual voters/learners differ from PD")
        require(all(endpoint(e["leader"]) in peers for e in actual[group]),
                f"group {group}: a replica reports an unexpected leader")
        memberships[str(group)] = sorted(peers)
    return {"pdVoters": sorted(addresses["pd"]), "observedStoreIds": identities,
            "replicaCount": replica_count, "groups": memberships}


def get_json(url):
    parsed = urlsplit(url)
    require(parsed.scheme in ("http", "https") and parsed.netloc and not parsed.username
            and not parsed.password, "use HTTP(S) URLs without embedded credentials")
    request = Request(url)
    password = os.environ.get("HG_ADOPTION_HTTP_PASSWORD")
    if password:
        user = os.environ.get("HG_ADOPTION_HTTP_USER", "hg")
        encoded = base64.b64encode(f"{user}:{password}".encode()).decode()
        request.add_header("Authorization", "Basic " + encoded)
    try:
        with build_opener(NoRedirect).open(request, timeout=30) as response:
            result = json.load(response)
    except HTTPError as error:
        raise CheckFailed(f"HTTP read failed with status {error.code}") from None
    except (URLError, TimeoutError, json.JSONDecodeError):
        raise CheckFailed("HTTP read failed or returned invalid JSON") from None
    require(isinstance(result, dict) and result.get("status") == 0,
            "management API did not return a successful status")
    return result


def kube(args, kind):
    command = ["kubectl"]
    if args.context:
        command += ["--context", args.context]
    command += ["--request-timeout=30s", "--namespace", args.namespace,
                "get", kind, "-o", "json"]
    try:
        return json.loads(subprocess.run(command, check=True, capture_output=True,
                                         text=True, timeout=40).stdout)["items"]
    except (subprocess.SubprocessError, OSError, json.JSONDecodeError):
        raise CheckFailed(f"kubectl get {kind} failed; check read permissions/context") from None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("namespace", "release", "fullname", "pd-url"):
        parser.add_argument("--" + option, required=True)
    for option in ("pd-count", "store-count", "replica-count"):
        parser.add_argument("--" + option, required=True, type=int)
    parser.add_argument("--store-url", action="append", required=True,
                        help="one distinct reachable URL per Store ordinal, in ordinal order")
    parser.add_argument("--context")
    parser.add_argument("--before", type=Path, help="compare with the previous PASS JSON report")
    args = parser.parse_args()
    try:
        require(1 <= args.replica_count <= args.store_count <= 99 and
                1 <= args.pd_count <= 99 and args.replica_count % 2 == 1,
                "expected counts are invalid")
        require(len(set(args.store_url)) == args.store_count == len(args.store_url),
                "supply one distinct --store-url for each Store ordinal")
        resources, addresses = validate_kubernetes(
            kube(args, "statefulsets,deployments,pods,pvc"), kube(args, "pv"), args)
        pd = args.pd_url.rstrip("/")
        database = validate_database(
            get_json(pd + "/v1/members"), get_json(pd + "/v1/cluster"),
            get_json(pd + "/v1/stores"), get_json(pd + "/v1/shardGroups"),
            [get_json(url.rstrip("/") + "/v1/partitions") for url in args.store_url],
            addresses, args.replica_count)
        report = {"result": "PASS", "scope": "observations with operator-verified URL-to-Pod binding; "
                  "graph contents and raw persisted Store identity are not checked",
                  "namespace": args.namespace,
                  "release": args.release, "kubernetes": resources, "database": database}
        if args.before:
            before = json.loads(args.before.read_text())
            require(before == report,
                    "identity, membership, PVC binding or Pod template changed from --before")
        print(json.dumps(report, sort_keys=True, indent=2))
        return 0
    except (CheckFailed, KeyError, TypeError, ValueError, StopIteration, OSError) as error:
        message = str(error) if isinstance(error, CheckFailed) else "incomplete identity/API evidence"
        print(json.dumps({"result": "FAIL", "reason": message}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
