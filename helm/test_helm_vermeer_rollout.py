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

"""Pin role-scoped Vermeer rollout boundaries."""

import copy
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import yaml


CHART = Path(__file__).resolve().parent / "hugegraph"
COMPONENT = "app.kubernetes.io/component"


def merged(left, right):
    result = copy.deepcopy(left)
    for key, value in right.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merged(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


class VermeerRolloutContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if shutil.which("helm") is None:
            raise RuntimeError("Helm is required for the Vermeer rollout regression")

    def render(self, vermeer=None):
        values = {
            "pd": {"auth": {"value": "pd-test-service-password"}},
            "server": {
                "auth": {
                    "admin": {"password": "graph-test-admin-password"},
                    "token": {"value": "0123456789abcdef0123456789abcdef"},
                }
            },
            "vermeer": {
                "enabled": True,
                "mode": "cluster",
                "auth": {
                    "value": "vermeer-test-factor-0123456789abcdef",
                    "admin": {"password": "VermeerAdmin0123"},
                },
            },
        }
        if vermeer:
            values["vermeer"] = merged(values["vermeer"], vermeer)
        with tempfile.TemporaryDirectory(prefix="vermeer-rollout-") as directory:
            configuration = Path(directory) / "values.yaml"
            configuration.write_text(yaml.safe_dump(values))
            result = subprocess.run(
                ["helm", "template", "fixture", str(CHART),
                 "--namespace", "graphs", "--values", str(configuration)],
                capture_output=True, text=True, timeout=120,
            )
        self.assertEqual(0, result.returncode, result.stderr)
        return [doc for doc in yaml.safe_load_all(result.stdout) if doc]

    @staticmethod
    def workload(documents, component):
        return next(
            document for document in documents
            if document.get("kind") == "StatefulSet"
            and document.get("metadata", {}).get("labels", {}).get(COMPONENT) == component
        )

    def checksum(self, documents, component):
        workload = self.workload(documents, component)
        return workload["spec"]["template"]["metadata"]["annotations"]["checksum/config"]

    def test_worker_only_values_do_not_roll_the_single_master(self):
        baseline = self.render()
        master = self.workload(baseline, "vermeer-master")["spec"]["template"]
        cases = (
            {"worker": {"replicas": 5}},
            {"worker": {"resources": {"requests": {"cpu": "750m", "memory": "1Gi"}}}},
            {"worker": {"storage": {"size": "7Gi"}}},
            {"service": {"annotations": {"internal.example.com/owner": "analytics"}}},
        )
        for override in cases:
            with self.subTest(override=override):
                rendered = self.render(override)
                current = self.workload(rendered, "vermeer-master")["spec"]["template"]
                self.assertEqual(master, current)

    def test_config_checksum_tracks_only_the_role_inputs_that_are_read(self):
        baseline = self.render()
        master = self.checksum(baseline, "vermeer-master")
        worker = self.checksum(baseline, "vermeer-worker")

        master_http = self.render({"master": {"httpPort": 7688}})
        self.assertNotEqual(master, self.checksum(master_http, "vermeer-master"))
        self.assertEqual(worker, self.checksum(master_http, "vermeer-worker"))

        master_grpc = self.render({"master": {"grpcPort": 7689}})
        self.assertNotEqual(master, self.checksum(master_grpc, "vermeer-master"))
        self.assertNotEqual(worker, self.checksum(master_grpc, "vermeer-worker"))

        worker_ports = self.render({"worker": {"httpPort": 7788, "grpcPort": 7789}})
        self.assertEqual(master, self.checksum(worker_ports, "vermeer-master"))
        self.assertNotEqual(worker, self.checksum(worker_ports, "vermeer-worker"))


if __name__ == "__main__":
    unittest.main()
