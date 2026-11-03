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

import unittest

import yaml

from .support import CHART, RAG_ENV, RAG_MOUNT


class RagSourceContractTest(unittest.TestCase):
    def test_profile_is_internal_persistent_and_matches_the_image_contract(self):
        values = yaml.safe_load((CHART / "values-rag.yaml").read_text(encoding="utf-8"))
        rag = values["rag"]
        self.assertTrue(rag["enabled"])
        self.assertNotIn("replicas", rag, "topology presets own the single replica value")
        self.assertEqual("hugegraph/rag", rag["image"]["repository"])
        self.assertEqual(8001, rag["port"])
        self.assertEqual({"autoConfigure": True}, rag["hugegraph"])
        self.assertEqual("ClusterIP", rag["service"]["type"])
        self.assertTrue(rag["persistence"]["enabled"])
        self.assertEqual(RAG_MOUNT, rag["persistence"]["mountPath"])

    def test_single_and_cluster_presets_keep_one_persistent_rag_replica(self):
        single = yaml.safe_load((CHART / "values-single.yaml").read_text(encoding="utf-8"))
        cluster = yaml.safe_load((CHART / "values-cluster.yaml").read_text(encoding="utf-8"))
        self.assertEqual({"enabled": False, "replicas": 1}, single["rag"])
        self.assertEqual({"enabled": False, "replicas": 1}, cluster["rag"])

    def test_template_validation_publishes_the_single_replica_and_key_contract(self):
        helpers = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted((CHART / "templates").glob("_rag*.tpl"))
        )
        self.assertIn("rag.replicas must be 1", helpers)
        self.assertIn("rag contains unsupported field", helpers)
        self.assertIn("rag.persistence contains unsupported field", helpers)

    def test_templates_use_one_process_port_and_split_runtime_from_durable_config(self):
        deployment = (CHART / "templates" / "rag-deployment.yaml").read_text(encoding="utf-8")
        service = (CHART / "templates" / "rag-service.yaml").read_text(encoding="utf-8")
        helpers = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted((CHART / "templates").glob("_rag*.tpl"))
        )
        self.assertIn("hugegraph_llm.demo.rag_demo.app", deployment)
        self.assertIn('containerPort: {{ include "hugegraph.rag.port" . | int }}', deployment)
        self.assertIn('include "hugegraph.rag.port" . | int | quote', deployment)
        self.assertIn("targetPort: http", service)
        self.assertIn('include "hugegraph.server.clientUrl"', helpers)
        self.assertIn('trimPrefix "http://"', helpers)
        self.assertIn(RAG_ENV, helpers)
        self.assertIn("HUGEGRAPH_LLM_ENV_PATH", deployment)
        self.assertIn("HUGEGRAPH_LLM_PROMPT_CONFIG_PATH", deployment)
        self.assertNotIn("touch /data/.hugegraph-rag-seeded", deployment)

