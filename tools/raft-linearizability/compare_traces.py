#!/usr/bin/env python3
"""Compare Java and Rust replay traces using a stable, independent oracle.

Each input is a JSON array (or an object containing ``operations``).  Records
are compared after selecting the contract fields ``op``, ``key``, ``value``
and ``result``; transport timestamps and implementation metadata are ignored.
"""
import argparse, json, sys

FIELDS = ("op", "key", "value", "result")

def load(path):
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        data = data.get("operations")
    if not isinstance(data, list):
        raise ValueError("trace must be an array or {operations: array}")
    return [{k: row.get(k) for k in FIELDS} for row in data]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("java_trace"); ap.add_argument("rust_trace")
    args = ap.parse_args()
    try:
        java, rust = load(args.java_trace), load(args.rust_trace)
    except (OSError, ValueError, json.JSONDecodeError, AttributeError) as e:
        print(json.dumps({"equal": False, "error": str(e)})); return 2
    equal = java == rust
    out = {"equal": equal, "java_count": len(java), "rust_count": len(rust)}
    if not equal:
        out["first_difference"] = next(({"index": i, "java": j, "rust": r}
            for i, (j, r) in enumerate(zip(java, rust)) if j != r), None)
    print(json.dumps(out, sort_keys=True))
    return 0 if equal else 1

if __name__ == "__main__":
    sys.exit(main())
