# Partition contract POC

This standalone model is an independent invariant oracle, deliberately separate from Java and any future Rust production implementation. `cargo test --manifest-path tools/rust-partition-poc/Cargo.toml` runs six deterministic checks: baseline validity, dropped range, overlap, version regression, stale heartbeat, and idempotent heartbeat.

The negative tests mutate the model input and must fail validation; they are evidence that the oracle detects the first three required defect classes. Cache loss, crash replay, and restart recovery remain integration tests against PD and are still NO-GO until executed with logs and fixed seeds.

The replay tests cover deterministic event replay, corrupt restart state, and duplicate stale events. Service-level cache invalidation and PD crash orchestration still require the existing Java integration environment.
