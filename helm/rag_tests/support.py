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

from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import yaml


CHART = Path(__file__).resolve().parents[1] / "hugegraph"
HELM_TEMPLATE_TIMEOUT_SECONDS = 120
RAG_MOUNT = "/var/lib/hugegraph-rag"
RAG_PERSISTENT_ENV = f"{RAG_MOUNT}/.env"
RAG_EPHEMERAL_ENV = "/tmp/hugegraph-llm.env"
RAG_ENV = RAG_PERSISTENT_ENV


def component_resources(documents, component="rag", kind=None):
    return [
        document
        for document in documents
        if (kind is None or document["kind"] == kind)
        and document["metadata"].get("labels", {}).get("app.kubernetes.io/component") == component
    ]


def pod(workload):
    return workload["spec"]["template"]


def env_by_name(container):
    return {entry["name"]: entry for entry in container.get("env", [])}


class RagChartBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if shutil.which("helm") is None:
            raise unittest.SkipTest("Helm is required for rendered chart regression tests")

    def render(self, rag=None, preset=None, mode=None, use_profile=True, **values):
        if rag is not None:
            values["rag"] = {"enabled": True, **rag}
        if mode is not None:
            values["mode"] = mode
        with tempfile.TemporaryDirectory(prefix="rag-values-") as directory:
            configuration = Path(directory) / "values.yaml"
            configuration.write_text(yaml.safe_dump(values), encoding="utf-8")
            command = ["helm", "template", "fixture", str(CHART), "--namespace", "graphs"]
            if preset is not None:
                command.extend(["-f", str(CHART / preset)])
            if rag is not None and use_profile:
                command.extend(["-f", str(CHART / "values-rag.yaml")])
            command.extend(["-f", str(configuration)])
            return subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=HELM_TEMPLATE_TIMEOUT_SECONDS,
                check=False,
            )

    def documents(self, rag=None, preset=None, mode=None, use_profile=True, **values):
        result = self.render(rag, preset=preset, mode=mode, use_profile=use_profile, **values)
        self.assertEqual(0, result.returncode, result.stderr)
        return [document for document in yaml.safe_load_all(result.stdout) if document]

    def deployment(self, documents):
        deployment, = component_resources(documents, kind="Deployment")
        return deployment

    def container(self, documents):
        container, = pod(self.deployment(documents))["spec"]["containers"]
        return container
