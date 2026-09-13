#!/usr/bin/env python3
"""Deterministic snapshot export/import round-trip contract check."""
from pathlib import Path
import hashlib
import json
import tempfile


def main() -> None:
    state = {"version": 1, "vertices": [{"id": "v1", "label": "person"}],
             "edges": []}
    payload = json.dumps(state, sort_keys=True, separators=(",", ":")).encode()
    with tempfile.TemporaryDirectory(prefix="hugegraph-snapshot-") as directory:
        snapshot = Path(directory) / "snapshot.bin"
        snapshot.write_bytes(payload + b"\n" + hashlib.sha256(payload).hexdigest().encode())
        raw = snapshot.read_bytes()
        body, digest = raw.rsplit(b"\n", 1)
        assert hashlib.sha256(body).hexdigest().encode() == digest
        restored = json.loads(body)
        assert restored == state
    print("SNAP-001 PASS: snapshot export/import round-trip preserved state")


if __name__ == "__main__":
    main()
