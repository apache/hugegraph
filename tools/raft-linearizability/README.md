# Raft linearizability checker

`checker.py` is a standalone, dependency-free checker for histories of a single
Raft replicated register. It does not connect to or assume anything about a
Raft implementation. The checker searches for a sequential ordering that
respects real-time precedence (`end <= start`) and register read/write
semantics. It is intentionally small and suitable as a test oracle.

Each JSON operation has `id`, `op` (`read` or `write`), `value`, `start`, and
`end`; timestamps only need to be comparable. Run:

```bash
python3 tools/raft-linearizability/checker.py history.json
```

Exit status is zero when linearizable and one otherwise. The JSON output
contains a witness operation order or an explanation.
