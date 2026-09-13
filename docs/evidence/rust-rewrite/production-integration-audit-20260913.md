# Production Rust store integration audit (2026-09-13)

The Rust store workspace is wired as a production workflow in
`.github/workflows/hugegraph-store-rust.yml`. The workflow runs formatting,
warnings-as-errors clippy, locked debug tests, and locked release tests.

The `BackendSelector` is the explicit runtime boundary: `Rust` and
`JavaFallback` share `StoreBackend`, preserve state while switching, and are
covered by the differential switch/rollback trace in
`differential-switch-rollback-20260913.log`.

Maintainer sign-off: **PENDING**
Reviewer sign-off: **PENDING**
