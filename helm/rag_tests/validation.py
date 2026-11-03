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


class RagValidationMixin:
    def test_invalid_contracts_fail_before_render(self):
        invalid = (
            ({"service": {"type": "ExternalName"}}, "service.type"),
            ({"service": {"type": "ClusterIP", "nodePort": 31001}}, "nodePort"),
            ({"service": {"type": "NodePort", "nodePort": "3100x"}}, "nodePort"),
            ({"port": 0}, "rag.port"),
            ({"port": 8001.5}, "rag.port"),
            ({"replicas": 0}, "rag.replicas"),
            ({"replicas": 1.5}, "rag.replicas"),
            ({"replicas": 2}, "rag.replicas"),
            ({"podSecurityContext": {"runAsUser": 0}}, "runAsUser"),
            ({"podSecurityContext": {"fsGroup": 0}}, "fsGroup"),
            ({"podSecurityContext": {"seccompProfile": {"type": "Unconfined"}}}, "seccompProfile"),
            ({"securityContext": {"privileged": True}}, "privileged"),
            ({"securityContext": {"allowPrivilegeEscalation": True}}, "allowPrivilegeEscalation"),
            ({"securityContext": {"seccompProfile": {"type": "Unconfined"}}}, "seccompProfile"),
            ({"securityContext": {"capabilities": {"add": ["SYS_ADMIN"]}}}, "capabilities.add"),
            ({"securityContext": {"capabilities": {"drop": ["NET_RAW"]}}}, "capabilities.drop"),
            ({"serviceAccount": {"automountServiceAccountToken": True}}, "automountServiceAccountToken"),
            ({"hugegraph": {"passwordSecret": {"key": "password"}}}, "passwordSecret.name"),
            ({"persistence": {"mountPath": "relative/path"}}, "mountPath"),
            ({"env": [{"name": "GRAPH_URL", "value": "a"}, {"name": "GRAPH_URL", "value": "b"}]}, "duplicate"),
            ({"persistance": {"enabled": False}}, "persistance"),
        )
        for values, expected in invalid:
            with self.subTest(values=values):
                result = self.render(values)
                self.assertNotEqual(0, result.returncode)
                self.assertIn(expected, result.stderr)

