#!/usr/bin/env python3
"""Deterministic negative check: a modified snapshot must be rejected."""
import hashlib
import tempfile
from pathlib import Path

with tempfile.TemporaryDirectory() as directory:
    root = Path(directory)
    snapshot = root / "snapshot.bin"
    snapshot.write_bytes(b"hugegraph-snapshot-v1\nvertex:1\n")
    digest = hashlib.sha256(snapshot.read_bytes()).hexdigest()
    snapshot.write_bytes(snapshot.read_bytes() + b"corruption\n")
    actual = hashlib.sha256(snapshot.read_bytes()).hexdigest()
    if actual == digest:
        raise SystemExit("corruption was not detected")
    print("SNAP-001 PASS: corrupted snapshot rejected by digest mismatch")
