# SNAP-001 independent oracle

The deterministic snapshot round-trip restores exported state byte-for-byte and verifies SHA-256 framing; corruption input is rejected. Evidence: `snapshot-roundtrip-20260913.log` and `snapshot-corruption-20260913.log`.
