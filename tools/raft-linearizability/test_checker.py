#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0.
# See the License for the specific language governing permissions and
# limitations under the License.
import importlib.util
import json
from pathlib import Path

spec = importlib.util.spec_from_file_location("checker", Path(__file__).with_name("checker.py"))
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


def test_valid_concurrent_history():
    h = [{"id": 1, "op": "write", "value": 7, "start": 0, "end": 4},
         {"id": 2, "op": "read", "value": 7, "start": 2, "end": 3}]
    assert checker.check(h)[0]


def test_invalid_read_before_completed_write():
    h = [{"id": 1, "op": "write", "value": 7, "start": 0, "end": 2},
         {"id": 2, "op": "read", "value": 0, "start": 3, "end": 4}]
    assert not checker.check(h)[0]


def test_overlapping_writes_and_read_are_linearizable():
    # The write and read overlap, so the read may linearize after the write.
    h = [{"id": 1, "op": "write", "value": 1, "start": 0, "end": 5},
         {"id": 2, "op": "write", "value": 2, "start": 1, "end": 3},
         {"id": 3, "op": "read", "value": 1, "start": 2, "end": 6}]
    assert checker.check(h)[0]


def test_overlapping_writes_and_reads_are_not_linearizable():
    # Both writes complete before either read starts; the first read cannot
    # observe value 1 after write(2) has completed.
    h = [{"id": 1, "op": "write", "value": 1, "start": 0, "end": 2},
         {"id": 2, "op": "write", "value": 2, "start": 1, "end": 3},
         {"id": 3, "op": "read", "value": 1, "start": 4, "end": 5},
         {"id": 4, "op": "read", "value": 2, "start": 6, "end": 7}]
    assert not checker.check(h)[0]


def test_three_node_report_example_has_metadata_and_result():
    path = Path(__file__).parent / "examples" / "three-node-sample.json"
    report = json.loads(path.read_text())
    assert {"commit", "config", "seed"} <= report["metadata"].keys()
    ok, detail = checker.check(report["history"])
    assert report["checker"]["linearizable"] == ok
    assert report["checker"]["detail"] == list(detail)


def test_invalid_operation_is_rejected():
    ok, detail = checker.check([{"id": 1, "op": "delete", "value": 0,
                                "start": 0, "end": 1}])
    assert not ok
    assert detail == "invalid operation"


def test_duplicate_operation_id_is_rejected():
    ok, detail = checker.check([
        {"id": 1, "op": "write", "value": 1, "start": 0, "end": 1},
        {"id": 1, "op": "read", "value": 1, "start": 1, "end": 2},
    ])
    assert not ok
    assert detail == "duplicate operation id"
