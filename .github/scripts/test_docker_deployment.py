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

"""Reject successful service checks performed against an unrelated image."""

import importlib.util
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("deployment", Path(__file__).with_name("docker-deployment.py"))
deployment = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deployment)


class ImageIdentityTest(unittest.TestCase):
    def test_rejects_old_image_even_when_service_started(self):
        with patch.object(deployment, "command", return_value="sha256:old"):
            with self.assertRaisesRegex(RuntimeError, "uses sha256:old"):
                deployment.verify_image("container", "hugegraph/server:ci-1-1", "sha256:pr")

    def test_rejects_missing_image_identity(self):
        with patch.object(deployment, "command", return_value=""):
            with self.assertRaises(RuntimeError):
                deployment.verify_image("container", "hugegraph/server:ci-1-1", "")

    def test_accepts_exact_built_image(self):
        with patch.object(deployment, "command", return_value="sha256:pr"):
            deployment.verify_image("container", "hugegraph/server:ci-1-1", "sha256:pr")

    def test_identity_failure_cleans_compose_and_never_checks_http(self):
        with patch.object(deployment, "command", side_effect=["sha256:pr", "container"]), \
             patch.object(deployment, "verify_image", side_effect=RuntimeError("wrong image")), \
             patch.object(deployment.subprocess, "run") as run, \
             patch.object(deployment, "response") as response:
            with self.assertRaisesRegex(RuntimeError, "wrong image"):
                deployment.smoke("ci-1-1", "docker-compose.yml", {"server": "hugegraph/hugegraph"})
            calls = [call.args[0] for call in run.call_args_list]
            startup = next(args for args in calls if "up" in args)
            self.assertEqual(startup[-1], "server")
            self.assertEqual(startup[startup.index("--pull") + 1], "never")
            self.assertNotIn("hubble", startup)
            self.assertTrue(any("logs" in args for args in calls))
            self.assertEqual(calls[-1][-3:], ["down", "-v", "--remove-orphans"])
            response.assert_not_called()

    def test_startup_timeout_preserves_failure_and_cleans_compose(self):
        timeout = deployment.subprocess.TimeoutExpired("up", 660)
        cleanup = deployment.subprocess.CalledProcessError(1, "down")
        with patch.object(deployment, "command", return_value="sha256:pr"), \
             patch.object(deployment.subprocess, "run", side_effect=[timeout, None, None, cleanup]) as run:
            with self.assertRaises(deployment.subprocess.TimeoutExpired):
                deployment.smoke("ci-1-1", "docker-compose.yml", {"server": "hugegraph/hugegraph"})
            self.assertEqual(run.call_args_list[-1].args[0][-3:], ["down", "-v", "--remove-orphans"])

    def test_hstore_starts_only_pr_services_and_cleans_after_checks(self):
        images = {"pd": "hugegraph/pd", "store": "hugegraph/store", "server": "hugegraph/server"}
        identities = ["sha256:pd", "sha256:store", "sha256:server"]
        containers = [value for service in images for value in (service, f"sha256:{service}", "healthy")]
        with patch.object(deployment, "command", side_effect=identities + containers), \
             patch.object(deployment.subprocess, "run") as run, \
             patch.object(deployment, "verify_server") as server, \
             patch.object(deployment, "verify_storage") as storage, \
             patch.object(deployment, "verify_graph") as graph:
            deployment.smoke("ci-1-1", "docker-compose-hstore.yml", images)
            startup = run.call_args_list[0].args[0]
            self.assertEqual(startup[-3:], ["pd", "store", "server"])
            self.assertNotIn("hubble", startup)
            server.assert_called_once_with()
            storage.assert_called_once_with()
            graph.assert_called_once_with("hg_pr_ci_1_1_docker_compose_hstore")
            self.assertEqual(run.call_args_list[-1].args[0][-3:], ["down", "-v", "--remove-orphans"])


class GraphSmokeTest(unittest.TestCase):
    def test_reuses_graph_write_read_and_gremlin_checks_with_auth(self):
        with patch.dict(os.environ, {"PATH": "/existing/tools"}), \
             patch.object(deployment.subprocess, "run") as run:
            deployment.verify_graph("ci_1_1")
            invocation = run.call_args
            self.assertTrue(invocation.args[0][1].endswith("run-server-e2e-smoke-test.sh"))
            self.assertEqual(invocation.args[0][2:], ["http://localhost:8080", "create", "ci_1_1"])
            self.assertTrue(invocation.kwargs["check"])
            self.assertEqual(invocation.kwargs["timeout"], 960)
            environment = invocation.kwargs["env"]
            self.assertEqual(environment["PATH"], "/existing/tools")
            self.assertEqual(environment["HUGEGRAPH_USERNAME"], "admin")
            self.assertEqual(environment["HUGEGRAPH_PASSWORD"], deployment.ADMIN_PASSWORD)

    def test_graph_failure_propagates_and_cleans_both_topologies(self):
        for topology, images in (
                ("docker-compose.yml", {"server": "hugegraph/hugegraph"}),
                ("docker-compose-hstore.yml", {"pd": "hugegraph/pd", "store": "hugegraph/store",
                                              "server": "hugegraph/server"})):
            identities = [f"sha256:{service}" for service in images]
            containers = [value for service in images for value in (service, f"sha256:{service}", "healthy")]
            failure = deployment.subprocess.CalledProcessError(1, "graph smoke")
            with self.subTest(topology=topology), \
                 patch.object(deployment, "command", side_effect=identities + containers), \
                 patch.object(deployment.subprocess, "run") as run, \
                 patch.object(deployment, "verify_server"), \
                 patch.object(deployment, "verify_storage"), \
                 patch.object(deployment, "verify_graph", side_effect=failure) as graph:
                with self.assertRaises(deployment.subprocess.CalledProcessError) as caught:
                    deployment.smoke("ci-1-1", topology, images)
                self.assertIs(caught.exception, failure)
                graph.assert_called_once()
                calls = [call.args[0] for call in run.call_args_list]
                self.assertTrue(any("logs" in args for args in calls))
                self.assertEqual(calls[-1][-3:], ["down", "-v", "--remove-orphans"])


class PayloadTest(unittest.TestCase):
    VERSIONS = json.dumps({"versions": deployment.expected_versions()})
    GRAPHS = '{"graphs":["hugegraph"]}'

    def test_accepts_public_versions_and_authenticated_graphs(self):
        with patch.object(deployment, "response", side_effect=[(401, ""), (200, self.GRAPHS),
                                                               (200, self.VERSIONS)]) as response:
            deployment.verify_server()
            calls = response.call_args_list
            self.assertIsNone(calls[0].args[1])
            self.assertEqual(calls[1].args[1], "admin:ci-compose-password")
            self.assertIsNone(calls[2].args[1])

    def test_rejects_graph_error_body_even_when_http_status_is_200(self):
        for body in ('{"status":-1,"error":"Unauthorized"}', '{"graphs":"hugegraph"}',
                     '{"graphs":[]}'):
            with self.subTest(body=body), \
                 patch.object(deployment, "response", side_effect=[(401, ""), (200, body)]):
                with self.assertRaisesRegex(RuntimeError, "graphs array"):
                    deployment.verify_server()

    def test_rejects_invalid_versions_even_when_http_status_is_200(self):
        for body in ('{"status":200}', '{"versions":{"version":"v1","core":"1.7.0"}}',
                     '{"versions":{"version":"v1","core":"1","api":"","gremlin":"3"}}'):
            with self.subTest(body=body), \
                 patch.object(deployment, "response", side_effect=[(401, ""), (200, self.GRAPHS), (200, body)]):
                with self.assertRaisesRegex(RuntimeError, "versions object"):
                    deployment.verify_server()

    def test_rejects_wrong_release_gremlin_and_protocol_versions(self):
        for key, wrong in (("core", "1.7.0"), ("gremlin", "3.7.3"), ("gremlin", None),
                           ("api", "1.8.0"), ("version", "v2")):
            payload = json.loads(self.VERSIONS)
            payload["versions"][key] = wrong
            with self.subTest(key=key), self.assertRaisesRegex(RuntimeError, "versions object"):
                deployment.verify_versions(payload)

    def test_rejects_missing_expected_gremlin_version(self):
        parse = deployment.ET.parse

        def read_pom(path):
            if path == Path(deployment.__file__).resolve().parents[2] / "hugegraph-server/pom.xml":
                return deployment.ET.ElementTree(deployment.ET.Element("project"))
            return parse(path)

        with patch.object(deployment.ET, "parse", side_effect=read_pom):
            with self.assertRaisesRegex(RuntimeError, "non-empty expected versions"):
                deployment.expected_versions()

    def test_rejects_unauthenticated_server_access(self):
        with patch.object(deployment, "response", return_value=(200, self.GRAPHS)):
            with self.assertRaisesRegex(RuntimeError, "expected 401"):
                deployment.verify_server()

    def test_accepts_pd_store_health_readiness_and_authenticated_registration(self):
        responses = [(200, ""), (200, ""), (200, '{"ready":true}'), (401, ""),
                     (200, '{"status":0,"data":{"stores":[{"address":"store:8500"}]}}')]
        with patch.object(deployment, "response", side_effect=responses) as response:
            deployment.verify_storage()
            self.assertEqual(response.call_args_list[-1].args[1], "hg:ci-compose-pd-secret")

    def test_rejects_pd_false_readiness_or_error_body(self):
        for body in ('{"ready":false}', '{"ready":"true"}', '{"status":-1}'):
            with self.subTest(body=body), \
                 patch.object(deployment, "response", side_effect=[(200, ""), (200, ""), (200, body)]):
                with self.assertRaisesRegex(RuntimeError, "ready=true"):
                    deployment.verify_storage()

    def test_rejects_health_error_body_even_when_http_status_is_200(self):
        with patch.object(deployment, "response", return_value=(200, '{"error":"Unauthorized"}')):
            with self.assertRaisesRegex(RuntimeError, "health response"):
                deployment.verify_storage()

    def test_rejects_unregistered_store_or_pd_error(self):
        for body in ('{"status":0,"data":{"stores":[]}}',
                     '{"status":-1,"data":{"stores":[{}]}}'):
            responses = [(200, ""), (200, ""), (200, '{"ready":true}'), (401, ""), (200, body)]
            with self.subTest(body=body), patch.object(deployment, "response", side_effect=responses):
                with self.assertRaisesRegex(RuntimeError, "registered stores array"):
                    deployment.verify_storage()
