#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0.
# See the License for the specific language governing permissions and
# limitations under the License.
# http://www.apache.org/licenses/LICENSE-2.0
import importlib.util
import json
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "capture_history", Path(__file__).with_name("capture_history.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class Response:
    def __init__(self, body):
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def read(self):
        return self.body


def test_capture_records_write_and_read():
    responses = [Response(b"{}"), Response(json.dumps({"value": 7}).encode())]
    with patch.object(module.OPENER, "open", side_effect=responses):
        history = module.capture("http://example.invalid", 7)
    assert [item["op"] for item in history] == ["write", "read"]
    assert history[1]["value"] == 7
    assert all(item["start"] <= item["end"] for item in history)


if __name__ == "__main__":
    test_capture_records_write_and_read()
    print("capture history tests passed")
