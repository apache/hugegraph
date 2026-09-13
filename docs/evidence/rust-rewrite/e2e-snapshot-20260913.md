# Local E2E and recovery execution (2026-09-13)

Command: `bash tools/rust-rewrite-cluster/smoke.sh`

Environment: Docker Compose project `hugegraph-3x3`, compose file `docker/docker-compose-3pd-3store-3server.yml`, loopback proxy bypass. The script successfully started 3 PD, 3 Store and 3 Server containers and health checks passed. It created a unique fixture, stopped/restarted `pd0`, then stopped `store0`. Reads through surviving servers timed out repeatedly (`curl: (28) Operation timed out after 15000 milliseconds with 0 bytes received`), so the script could not establish post-failure data visibility and was terminated after retries. No snapshot round-trip or corruption test is implemented by the current script.

Result: **FAIL** for recovery E2E gate (exit not produced because process was terminated after hung retries); evidence is the captured `/tmp/e2e.log` output and command behavior. This is a reproducible environment/runtime failure, not a relaxed assertion.
