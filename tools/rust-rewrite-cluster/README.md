# Rust rewrite cluster smoke

`smoke.sh` runs the reversible three-PD/three-Store/three-Server preflight against
`docker/docker-compose-3pd-3store-3server.yml`. It checks all service health
endpoints, creates a uniquely named fixture, stops and restores `pd0`, stops and
restores `store0`, and verifies the fixture through the surviving servers.

The runner requires a working Docker daemon, the HugeGraph images, and local
proxy bypass for loopback. It is a smoke/failure-recovery gate; it does not
prove linearizability, durability under concurrent writes, snapshot corruption
handling, or Java/Rust differential equivalence.
