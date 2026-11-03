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

"""Source-level contracts for the optional HugeGraph RAG component."""

import unittest

import yaml

from .support import CHART, RAG_MOUNT, RAG_PERSISTENT_ENV


class RagSourceContractTest(unittest.TestCase):
    def test_profile_is_internal_and_inherits_persistence(self):
        values = yaml.safe_load((CHART / "values-rag.yaml").read_text(encoding="utf-8"))
        rag = values["rag"]
        self.assertTrue(rag["enabled"])
        self.assertNotIn("replicas", rag, "topology presets own the single replica value")
        self.assertEqual("hugegraph/rag", rag["image"]["repository"])
        self.assertEqual(8001, rag["port"])
        self.assertEqual({"autoConfigure": True}, rag["hugegraph"])
        self.assertEqual("ClusterIP", rag["service"]["type"])
        self.assertIsNone(rag["persistence"]["enabled"])
        self.assertEqual(RAG_MOUNT, rag["persistence"]["mountPath"])

    def test_single_and_cluster_presets_keep_one_rag_replica(self):
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

    def test_templates_use_one_port_and_kubernetes_env_precedence(self):
        deployment = (CHART / "templates" / "rag-deployment.yaml").read_text(encoding="utf-8")
        service = (CHART / "templates" / "rag-service.yaml").read_text(encoding="utf-8")
        helpers = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted((CHART / "templates").glob("_rag*.tpl"))
        )
        self.assertIn("hugegraph_llm.demo.rag_demo.app", deployment)
        self.assertIn('containerPort: {{ include "hugegraph.rag.port" . | int }}', deployment)
        self.assertIn("--port {{ include", deployment)
        self.assertIn("targetPort: http", service)
        self.assertIn('include "hugegraph.server.clientUrl"', helpers)
        self.assertIn('trimPrefix "http://"', helpers)
        self.assertIn('printf "%s/.env"', helpers)
        self.assertIn("HUGEGRAPH_LLM_ENV_PATH", deployment)
        self.assertIn("if key in os.environ", deployment)
        self.assertIn("unset_key", deployment)
        self.assertIn("cp -R -n", deployment)
        self.assertNotIn("cp -a", deployment)
        self.assertNotIn("touch /data/.hugegraph-rag-seeded", deployment)
        self.assertIn(RAG_PERSISTENT_ENV.rsplit("/", 1)[0], helpers)
