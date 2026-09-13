import importlib.util
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
