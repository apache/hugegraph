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

For collected three-node histories, keep a stable report envelope containing
`metadata.commit`, `metadata.config`, `metadata.seed`, the operation array in
`history`, and the checker output in `checker`. A ready-to-copy example is
`examples/three-node-sample.json`; the checker itself remains independent of
any service or cluster.

For HugeGraph capture, map a committed vertex mutation to `write` and a vertex
lookup to `read`, using the canonical vertex identifier as the register key.
Record HTTP send/receive monotonic times plus status and body hash in metadata;
the checker evaluates ordering and values only.
