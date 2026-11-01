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

"""Failure-safety regression tests for the SeaTunnel schema-copy helper."""

import contextlib
import copy
import importlib.util
import io
from pathlib import Path
import sys
import unittest
from unittest import mock


SCRIPT = (Path(__file__).resolve().parents[1] /
          "hugegraph/examples/seatunnel/clone/copy-schema.py")
SPEC = importlib.util.spec_from_file_location("seatunnel_copy_schema", SCRIPT)
COPY_SCHEMA = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COPY_SCHEMA)


def schema(property_names=()):
    result = {kind: [] for kind in COPY_SCHEMA.KINDS}
    result["propertykeys"] = [
        {"id": index + 1, "name": name, "data_type": "TEXT",
         "cardinality": "SINGLE", "user_data": {}}
        for index, name in enumerate(property_names)
    ]
    return result


class SchemaCopyFailureSafetyTest(unittest.TestCase):

    def run_copy(self, source_schema, target):
        source = mock.Mock(return_value=source_schema)
        with mock.patch.object(COPY_SCHEMA, "client",
                               side_effect=[source, target]), \
                mock.patch.object(sys, "argv", [str(SCRIPT),
                                                 "--source", "http://source/graph",
                                                 "--target", "http://target/graph"]):
            return COPY_SCHEMA.main()

    def test_partial_post_failure_keeps_target_in_restoring_mode(self):
        source_schema = schema(("name", "city"))
        calls = []
        posts = 0

        def target(method, suffix, body=None):
            nonlocal posts
            calls.append((method, suffix, body))
            if (method, suffix) == ("GET", "/schema"):
                return schema()
            if (method, suffix) == ("GET", "/mode"):
                return {"mode": "NONE"}
            if method == "POST":
                posts += 1
                if posts == 2:
                    raise RuntimeError("injected schema write failure")
            return None

        with self.assertRaisesRegex(SystemExit, "remains in RESTORING mode"):
            self.run_copy(source_schema, target)
        self.assertEqual(["RESTORING"], [body for method, suffix, body in calls
                                         if (method, suffix) == ("PUT", "/mode")])

    def test_schema_mismatch_keeps_target_in_restoring_mode(self):
        source_schema = schema(("name",))
        calls = []
        schema_reads = iter((schema(), schema()))

        def target(method, suffix, body=None):
            calls.append((method, suffix, body))
            if (method, suffix) == ("GET", "/schema"):
                return next(schema_reads)
            if (method, suffix) == ("GET", "/mode"):
                return {"mode": "NONE"}
            return None

        with self.assertRaisesRegex(SystemExit, "Schema comparison failed"):
            self.run_copy(source_schema, target)
        self.assertEqual(["RESTORING"], [body for method, suffix, body in calls
                                         if (method, suffix) == ("PUT", "/mode")])

    def test_verified_copy_restores_original_mode(self):
        source_schema = schema(("name",))
        calls = []
        schema_reads = iter((schema(), copy.deepcopy(source_schema)))

        def target(method, suffix, body=None):
            calls.append((method, suffix, body))
            if (method, suffix) == ("GET", "/schema"):
                return next(schema_reads)
            if (method, suffix) == ("GET", "/mode"):
                return {"mode": "NONE"}
            return None

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.run_copy(source_schema, target)
        self.assertEqual(["RESTORING", "NONE"],
                         [body for method, suffix, body in calls
                          if (method, suffix) == ("PUT", "/mode")])
        self.assertIn('"schema_equal": true', output.getvalue())


if __name__ == "__main__":
    unittest.main()
