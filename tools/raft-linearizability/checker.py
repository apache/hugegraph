#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0.
# See the License for the specific language governing permissions and
# limitations under the License.
#!/usr/bin/env python3
"""Small, dependency-free linearizability checker for a Raft register history."""
import argparse, json


def check(history):
    """Return (ok, explanation). History entries: id, op (read|write), value,
    start and end (monotonic timestamps)."""
    ops = list(history)
    ids = {o["id"] for o in ops}
    if len(ids) != len(ops):
        return False, "duplicate operation id"
    for o in ops:
        if o["op"] not in ("read", "write") or o["end"] < o["start"]:
            return False, "invalid operation"
    before = {o["id"]: {p["id"] for p in ops if p["end"] <= o["start"]}
              for o in ops}
    by_id = {o["id"]: o for o in ops}

    def search(done, value):
        if len(done) == len(ops):
            return ()
        candidates = [o for o in ops if o["id"] not in done and
                      before[o["id"]] <= done]
        for o in candidates:
            if o["op"] == "read" and o["value"] != value:
                continue
            nv = o.get("value") if o["op"] == "write" else value
            tail = search(done | {o["id"]}, nv)
            if tail is not None:
                return (o["id"],) + tail
        return None

    result = search(set(), None)
    return (True, result) if result is not None else (False, "no legal sequential ordering")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("history", help="JSON file containing an array of operations")
    args = ap.parse_args()
    try:
        with open(args.history, encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload, list):
            raise ValueError("history must be a JSON array")
        ok, detail = check(payload)
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        print(json.dumps({"linearizable": False, "detail": f"invalid history: {exc}"}))
        return 2
    print(json.dumps({"linearizable": ok, "detail": detail}))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
