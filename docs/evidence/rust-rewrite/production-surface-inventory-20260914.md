# hugegraph-store production surface inventory

The production store currently contains these Java modules and their Rust status:

| Module | Production responsibility | Rust status |
|---|---|---|
| hg-store-core | partition, shard, Raft state machine, transactions, WAL-facing storage | **missing**; current Rust store-core is an in-memory key/value slice |
| hg-store-node | node lifecycle, Raft transport, PD/store orchestration | missing |
| hg-store-grpc | public RPC protocol and generated service bindings | missing |
| hg-store-client | client request/response contract | missing |
| hg-store-common | shared identifiers, codecs, errors and configuration | missing |
| hg-store-rocksdb | durable RocksDB backend | missing |
| hg-store-cli | operational CLI | missing |
| hg-store-dist | packaging and runtime integration | missing |

The existing Rust workspace has one crate (`store-core`) and five unit tests. No Rust binary, gRPC service, Raft implementation, durable backend, or Java production switch is wired into the Maven store runtime. Therefore the full rewrite objective is not yet satisfied.

First uncovered boundary: **storage contract and durable backend integration**. The next implementation slice must define a Rust transaction/WAL interface backed by a durable store and add an end-to-end test that exercises the production-facing request path.
