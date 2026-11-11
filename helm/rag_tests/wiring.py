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

"""Regression coverage for the optional HugeGraph RAG Helm component."""

from .support import component_resources, env_by_name, pod


class RagWiringMixin:
    def test_single_and_cluster_presets_share_the_same_graph_contract(self):
        for preset in ("values-single.yaml", "values-cluster.yaml"):
            with self.subTest(preset=preset):
                documents = self.documents({}, preset=preset)
                deployment = self.deployment(documents)
                environment = env_by_name(self.container(documents))
                self.assertEqual(1, deployment["spec"]["replicas"])
                self.assertEqual(
                    "http://fixture-hugegraph-server.graphs.svc:8080",
                    environment["GRAPH_URL"]["value"],
                )

    def test_service_stays_internal_unless_exposure_is_explicit(self):
        for mode in ("easy", "strict"):
            with self.subTest(mode=mode):
                documents = self.documents({}, mode=mode)
                service, = component_resources(documents, kind="Service")
                self.assertEqual("ClusterIP", service["spec"]["type"])
        documents = self.documents({"service": {"type": "NodePort", "nodePort": 31001}}, mode="easy")
        service, = component_resources(documents, kind="Service")
        self.assertEqual("NodePort", service["spec"]["type"])
        self.assertEqual(31001, service["spec"]["ports"][0]["nodePort"])

    def test_user_environment_replaces_automatic_entries(self):
        documents = self.documents({
            "env": [
                {"name": "GRAPH_URL", "value": "external:8080"},
                {"name": "GRAPH_PWD", "valueFrom": {"secretKeyRef": {"name": "external", "key": "pwd"}}},
            ]
        })
        environment = env_by_name(self.container(documents))
        self.assertEqual("external:8080", environment["GRAPH_URL"]["value"])
        self.assertEqual("external", environment["GRAPH_PWD"]["valueFrom"]["secretKeyRef"]["name"])

    def test_external_graph_url_in_env_never_inherits_internal_admin_password(self):
        documents = self.documents({"env": [{"name": "GRAPH_URL", "value": "external:8080"}]})
        environment = env_by_name(self.container(documents))
        self.assertEqual("external:8080", environment["GRAPH_URL"]["value"])
        self.assertEqual("", environment["GRAPH_PWD"]["value"])
        self.assertNotIn("checksum/hugegraph-auth", pod(self.deployment(documents))["metadata"].get("annotations", {}))

    def test_external_server_uses_only_explicit_password_secret(self):
        documents = self.documents({"hugegraph": {
            "autoConfigure": True,
            "url": "http://remote.graphs.svc:8080",
            "passwordSecret": {"name": "remote-admin", "key": "credential"},
        }})
        environment = env_by_name(self.container(documents))
        self.assertEqual("http://remote.graphs.svc:8080", environment["GRAPH_URL"]["value"])
        self.assertEqual("remote-admin", environment["GRAPH_PWD"]["valueFrom"]["secretKeyRef"]["name"])
        self.assertEqual("credential", environment["GRAPH_PWD"]["valueFrom"]["secretKeyRef"]["key"])
        self.assertIn("checksum/hugegraph-auth", pod(self.deployment(documents))["metadata"]["annotations"])

        documents = self.documents({"hugegraph": {
            "autoConfigure": True,
            "url": "http://remote.graphs.svc:8080",
        }})
        self.assertEqual("", env_by_name(self.container(documents))["GRAPH_PWD"]["value"])

    def test_custom_password_secret_defaults_to_password_key(self):
        documents = self.documents({"hugegraph": {
            "passwordSecret": {"name": "rag-credentials"},
        }})
        password = env_by_name(self.container(documents))["GRAPH_PWD"]["valueFrom"]["secretKeyRef"]
        self.assertEqual("rag-credentials", password["name"])
        self.assertEqual("password", password["key"])

    def test_internal_server_secret_keeps_its_configured_key(self):
        documents = self.documents({}, server={"auth": {"admin": {"key": "admin-password"}}})
        password = env_by_name(self.container(documents))["GRAPH_PWD"]["valueFrom"]["secretKeyRef"]
        self.assertEqual("admin-password", password["key"])
