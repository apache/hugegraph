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

import copy
from contextlib import redirect_stderr, redirect_stdout
from http.server import BaseHTTPRequestHandler, HTTPServer
import io
import json
from pathlib import Path
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import check_bootstrap_adoption as adoption
from check_bootstrap_adoption import CheckFailed, get_json, validate_database, validate_kubernetes


def fixture(count=3):
    args = SimpleNamespace(namespace="test", release="graph", fullname="graph",
                           pd_count=count, store_count=count)
    resources, volumes = [], []
    for role in ("pd", "store"):
        name = "graph-" + role
        uid = name + "-uid"
        resources.append({"kind": "StatefulSet", "metadata": {
            "name": name, "uid": uid, "labels": {"app.kubernetes.io/instance": "graph"}},
            "spec": {"replicas": count, "serviceName": name,
                     "volumeClaimTemplates": [{"metadata": {"name": role + "-data"}}],
                     "template": {"spec": {"containers": [{"name": role, "ports": [
                         {"name": "raft", "containerPort": 8610 if role == "pd" else 8510},
                         {"name": "grpc", "containerPort": 8686 if role == "pd" else 8500}]}]}}}})
        for ordinal in range(count):
            pod = f"{name}-{ordinal}"
            claim = f"{role}-data-{pod}"
            resources.append({"kind": "Pod", "metadata": {
                "name": pod, "uid": pod + "-uid", "ownerReferences": [{"uid": uid}]},
                "spec": {"volumes": [{"name": role + "-data",
                                      "persistentVolumeClaim": {"claimName": claim}}]},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]}})
            resources.append({"kind": "PersistentVolumeClaim", "metadata": {
                "name": claim, "uid": claim + "-uid"}, "spec": {"volumeName": claim + "-pv"},
                "status": {"phase": "Bound"}})
            volumes.append({"metadata": {"name": claim + "-pv", "uid": claim + "-pv-uid"},
                            "spec": {"claimRef": {"namespace": "test", "name": claim,
                                                  "uid": claim + "-uid"}}})
    _, addresses = validate_kubernetes(resources, volumes, args)
    raft = sorted(addresses["store"])
    stores = {"data": {"stores": [{"storeId": str(i + 1), "address": grpc,
                                    "raftAddress": r, "state": "Up"}
                                   for i, (grpc, r) in enumerate(zip(
                                       sorted(addresses["storeGrpc"]), raft))]}}
    members = {"data": {"pdList": [{"raftUrl": address, "state": "Up",
                                    "role": "Leader" if i == 0 else "Follower"}
                                   for i, address in enumerate(sorted(addresses["pd"]))]}}
    cluster = {"data": {"shardCount": count, "state": "Cluster_OK",
                         "dataState": "PState_Normal"}}
    groups = {"shardGroups": [{"shards": [{"storeId": str(i + 1),
                                          "role": "Leader" if i == 0 else "Follower"}
                                         for i in range(count)]}]}
    peers = [{"endpoint": {"ip": address.rsplit(":", 1)[0], "port": 8510}}
             for address in raft]
    engines = [{"status": 0, "partitions": [{
        "groupId": 0, "role": "STATE_LEADER" if i == 0 else "STATE_FOLLOWER",
        "logIndex": 10, "partitions": [{"workState": "PState_Normal", "leader": "OK"}],
        "leader": peers[0]["endpoint"], "peers": peers if i == 0 else None,
        "learners": [] if i == 0 else None}]} for i in range(count)]
    return args, resources, volumes, [members, cluster, stores, groups, engines, addresses, count]


class AdoptionChecksTest(unittest.TestCase):
    def setUp(self):
        self.args, self.resources, self.volumes, self.database = fixture()

    def assert_database_rejected(self, message):
        with self.assertRaisesRegex(CheckFailed, message):
            validate_database(*self.database)

    def test_accepts_one_and_three_node_same_topology_with_zero_group_id(self):
        for count in (1, 3):
            args, resources, volumes, database = fixture(count)
            kube, _ = validate_kubernetes(resources, volumes, args)
            self.assertEqual(count * 2, len(kube["volumes"]))
            result = validate_database(*database)
            self.assertEqual(count, len(result["groups"]["0"]))

    def test_persisted_rf_mismatch_is_rejected(self):
        self.database[1]["data"]["shardCount"] = 1
        self.assert_database_rejected("persisted PD replica policy")

    def test_pd_learner_and_unavailable_voters_are_rejected(self):
        for field, value in (("role", "Learner"), ("state", "Offline")):
            with self.subTest(field=field):
                database = copy.deepcopy(self.database)
                database[0]["data"]["pdList"][1][field] = value
                with self.assertRaisesRegex(CheckFailed, "unavailable voter"):
                    validate_database(*database)

    def test_pending_and_exiting_store_are_not_adoption_capacity(self):
        for state in ("Pending", "Exiting", "Offline", "Tombstone"):
            with self.subTest(state=state):
                self.database[2]["data"]["stores"][0]["state"] = state
                self.assert_database_rejected("not eligible for adoption")

    def test_target_pd_metadata_does_not_prove_actual_voters(self):
        self.database[4][0]["partitions"][0]["peers"].pop()
        self.assert_database_rejected("actual voters/learners differ")

    def test_actual_learner_is_rejected_even_when_pd_says_full_rf(self):
        self.database[4][0]["partitions"][0]["learners"] = [
            {"ip": "unexpected", "port": 8510}]
        self.assert_database_rejected("actual voters/learners differ")

    def test_missing_engine_replica_is_rejected(self):
        self.database[4][1]["partitions"] = []
        self.assert_database_rejected("missing leader or local engine replica")

    def test_leftover_unknown_engine_is_rejected(self):
        self.database[4][0]["partitions"].append({"groupId": 99})
        self.assert_database_rejected("unknown or duplicate")

    def test_data_and_replica_recovery_must_be_healthy(self):
        self.database[4][1]["partitions"][0]["partitions"][0]["workState"] = "PState_Warn"
        self.assert_database_rejected("partition state is not recovered")

    def test_duplicate_store_identity_is_rejected(self):
        self.database[2]["data"]["stores"][1]["storeId"] = "1"
        self.assert_database_rejected("duplicate or unassigned")

    def test_bound_pvc_with_wrong_claim_uid_is_rejected(self):
        self.volumes[0]["spec"]["claimRef"]["uid"] = "old-deleted-pvc-uid"
        with self.assertRaisesRegex(CheckFailed, "claimRef identity differs"):
            validate_kubernetes(self.resources, self.volumes, self.args)

    def test_pod_mounting_different_claim_is_rejected(self):
        pod = next(r for r in self.resources if r["kind"] == "Pod")
        pod["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"] = "other-data"
        with self.assertRaisesRegex(CheckFailed, "mounted data claim differs"):
            validate_kubernetes(self.resources, self.volumes, self.args)

    def test_changed_topology_is_rejected_before_database_checks(self):
        self.args.store_count = 4
        with self.assertRaisesRegex(CheckFailed, "replica count or stable service name"):
            validate_kubernetes(self.resources, self.volumes, self.args)

    def test_retired_ordinal_pvc_is_not_silently_ignored(self):
        self.resources.append({"kind": "PersistentVolumeClaim", "metadata": {
            "name": "store-data-graph-store-3", "uid": "retired-uid"}})
        with self.assertRaisesRegex(CheckFailed, "orphaned data PVC"):
            validate_kubernetes(self.resources, self.volumes, self.args)

    def test_orphan_pod_does_not_prove_controller_identity(self):
        pod = next(r for r in self.resources if r["kind"] == "Pod")
        pod["metadata"]["ownerReferences"][0]["uid"] = "old-controller"
        with self.assertRaisesRegex(CheckFailed, "unexpected or missing Pod"):
            validate_kubernetes(self.resources, self.volumes, self.args)

    def test_server_pod_and_template_are_in_identity_snapshot(self):
        selector = {"app.kubernetes.io/instance": "graph",
                    "app.kubernetes.io/component": "server"}
        self.resources.extend([
            {"kind": "Deployment", "metadata": {"name": "graph-server", "uid": "server-uid",
                                                  "labels": selector},
             "spec": {"replicas": 1, "selector": {"matchLabels": selector},
                      "template": {"spec": {"containers": [{"name": "server", "image": "fixed"}]}}}},
            {"kind": "Pod", "metadata": {"name": "graph-server-x", "uid": "server-pod-uid",
                                           "labels": selector},
             "status": {"conditions": [{"type": "Ready", "status": "True"}]}}])
        before, _ = validate_kubernetes(self.resources, self.volumes, self.args)
        self.assertEqual("server-pod-uid", before["pods"]["graph-server-x"])
        self.resources[-2]["spec"]["template"]["spec"]["containers"][0]["image"] = "changed"
        after, _ = validate_kubernetes(self.resources, self.volumes, self.args)
        self.assertNotEqual(before["controllers"], after["controllers"])

    def test_before_report_rejects_changed_store_identity_after_upgrade(self):
        argv = ["check", "--namespace", "test", "--release", "graph", "--fullname", "graph",
                "--pd-count", "3", "--store-count", "3", "--replica-count", "3",
                "--pd-url", "http://pd"]
        for i in range(3):
            argv += ["--store-url", f"http://store{i}"]

        def request(url):
            for suffix, index in (("members", 0), ("cluster", 1), ("stores", 2), ("shardGroups", 3)):
                if url.endswith("/" + suffix):
                    return self.database[index]
            return self.database[4][int(url.split("store")[1].split("/")[0])]

        def run(command):
            output, error = io.StringIO(), io.StringIO()
            with patch.object(sys, "argv", command), patch.object(adoption, "get_json", request), \
                    patch.object(adoption, "kube", side_effect=[self.resources, self.volumes]), \
                    redirect_stdout(output), redirect_stderr(error):
                result = adoption.main()
            return result, output.getvalue(), error.getvalue()

        result, output, _ = run(argv)
        self.assertEqual(0, result)
        with tempfile.TemporaryDirectory() as directory:
            baseline = Path(directory) / "before.json"
            baseline.write_text(output)
            self.assertEqual(0, run(argv + ["--before", str(baseline)])[0])
            self.database[2]["data"]["stores"][0]["storeId"] = "9"
            self.database[3]["shardGroups"][0]["shards"][0]["storeId"] = "9"
            result, _, error = run(argv + ["--before", str(baseline)])
            self.assertEqual(1, result)
            self.assertIn("identity, membership, PVC binding or Pod template changed", error)


class HttpChecksTest(unittest.TestCase):
    def test_bad_api_status_and_redirects_fail_without_forwarding_credentials(self):
        calls = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                calls.append(self.path)
                if self.path == "/redirect":
                    self.send_response(302)
                    self.send_header("Location", "/credential-recipient")
                    self.end_headers()
                else:
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(json.dumps({"status": 500}).encode())

            def log_message(self, *_):
                pass

        server = HTTPServer(("127.0.0.1", 0), Handler)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            url = f"http://127.0.0.1:{server.server_port}"
            with patch.dict("os.environ", {"HG_ADOPTION_HTTP_PASSWORD": "fixture-only"}):
                with self.assertRaisesRegex(CheckFailed, "status 302"):
                    get_json(url + "/redirect")
                with self.assertRaisesRegex(CheckFailed, "successful status"):
                    get_json(url + "/failed")
            self.assertEqual(["/redirect", "/failed"], calls)
        finally:
            server.shutdown()
            server.server_close()
            worker.join()

    def test_embedded_credentials_are_not_accepted(self):
        with self.assertRaisesRegex(CheckFailed, "without embedded credentials"):
            get_json("http://user:fixture-only@127.0.0.1/")


if __name__ == "__main__":
    unittest.main()
