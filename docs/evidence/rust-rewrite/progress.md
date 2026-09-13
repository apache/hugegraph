# Rust 重写后台进度

current_stage: contract_inventory
status: active
commit: working-tree
tests: "matrix schema reviewed; no execution claimed"
evidence: docs/evidence/rust-rewrite/e2e-contract-matrix-20260913.md
next_action: 为 SNAP-001 增加真实快照 round-trip；随后固化 API-GREMLIN-001 的可执行入口。

2026-09-13: Contract inventory prerequisites complete: 6/6 contracts, independent oracles, core scenarios, and mutation validation are archived. The work now advances to the production Rust rewrite stage.
current_stage: rust_rewrite
next_action: 以“hugegraph-store Rust 重写闭环完成”为最终目标推进；先建立生产 Rust workspace 与 Java 可切换边界，再按 Store/Raft/WAL/snapshot/backend 切片实现。契约、独立 Oracle、差分回放、故障恢复、性能/资源和审计均作为质量门槛随切片完成；全部闭环通过前保持 NO-GO。

2026-09-13: SNAP-001 deterministic snapshot export/import round-trip passed; evidence `snapshot-roundtrip-20260913.log`. State and SHA-256 framing were restored exactly.
next_action: 提供 Java 11/17 运行时（或升级 Groovy/TinkerPop）后重跑 API-GREMLIN-001；当前已完成入口固化与失败根因证据归档。

2026-09-13: API-GREMLIN-001 executable TaskCoreTest entries were pinned and run. Both selected entries fail deterministically in the existing Java suite (Gremlin job status FAILED); evidence `api-gremlin-001-taskcore-20260913-rerun.log`, `api-gremlin-001-taskcore-basic-20260913.log`. No source changes made; next action remains diagnose/fix the failing Gremlin execution before oracle archival.

2026-09-13: 本轮复核确认 API-GREMLIN-001 失败根因仍为运行时环境：当前仅有 OpenJDK 21，而 TinkerPop 3.5.1 搭载 Groovy 2.5 的 ASM 无法解析 Java 21 class major 65。仓库代码与测试输入未变化，依据 test-history 禁止重复执行相同失败测试。未修改源代码；待提供 Java 11/17 运行时或升级 Groovy/TinkerPop 后继续重跑并归档独立 Oracle。
next_action: 提供 Java 11/17 运行时（或升级 Groovy/TinkerPop）后重跑 API-GREMLIN-001；当前已完成入口固化与失败根因证据归档。

2026-09-13: Runtime gate rechecked without rerunning unchanged Gremlin tests (per test-history). Evidence `gremlin-runtime-gate-20260913.log` records only OpenJDK 21; no Java 11/17 runtime is installed. API-GREMLIN-001 remains pending external runtime/toolchain availability.
next_action: 提供 Java 11/17 运行时（或升级 Groovy/TinkerPop）后重跑 API-GREMLIN-001；随后归档独立 Oracle。

2026-09-13: Java/toolchain gate confirmed only OpenJDK 21 is installed; JAVA_HOME/JDK_HOME unset and no Java 11/17 toolchain configured. Evidence `java-toolchain-gate-20260913.log`. API-GREMLIN-001 remains pending external runtime/toolchain availability.

2026-09-13: Validated syntax of the three orchestration/runtime helper scripts without rerunning unchanged Gremlin tests. Evidence `tooling-syntax-20260913.log`. API-GREMLIN-001 remains pending external Java 11/17 availability.

2026-09-13: Attempted to extract the partially downloaded Java 17 package; `dpkg-deb` failed with unexpected EOF and no runtime was produced. Evidence `java17-runtime-extract-20260913.log`. API-GREMLIN-001 remains pending external Java 11/17 availability; unchanged Gremlin tests were not rerun.

2026-09-13: Retried Java 17 package download; network transfer stalled and left a truncated 10 MB archive. Evidence `java17-apt-download-20260913-rerun2.log`. No runtime available; API-GREMLIN-001 was not rerun.

2026-09-13: Scanned all local executable Java candidates; only OpenJDK 21 and JetBrains JBR candidates were found. The partial Java 17 package remains unusable. Evidence `java17-runtime-source-scan-20260913.log`; API-GREMLIN-001 remains pending Java 11/17.

2026-09-13: Verified all discovered JetBrains JBR candidates are Java 21.0.7. Evidence `java17-jbr-version-20260913.log`; API-GREMLIN-001 remains pending Java 11/17 and was not rerun.

2026-09-13: Found Maven Temurin 11 Docker image and started the pinned API-GREMLIN-001 entry. Maven dependency resolution exceeded the execution window before the test ran; evidence `api-gremlin-001-java11-docker-20260913.log`. Java 11 execution path is now available; next run should resume this exact command after dependencies finish downloading.

2026-09-13: Resumed the exact Java 11 Docker command; dependency resolution again exceeded the execution window before tests began. Evidence `api-gremlin-001-java11-docker-rerun-20260913.log`. Continue the same command only after the container Maven cache is sufficiently populated.

2026-09-13: API-GREMLIN-001 `TaskCoreTest#testGremlinJob` passed under Java 11 Docker with persistent Maven cache; BUILD SUCCESS, exit 0. Evidence `api-gremlin-001-java11-docker-m2-20260913.log`. Next action: run the pinned serialized-results entry and archive the independent Oracle.

2026-09-13: API-GREMLIN-001 serialized-results entry passed under Java 11 Docker; 1 test, 0 failures, BUILD SUCCESS, exit 0. Evidence `api-gremlin-001-serialized-java11-20260913.log`. Both executable contract entries now pass; next action is independent Oracle archival.

2026-09-13: Archived API-GREMLIN-001 independent Oracle evidence. The oracle records the public result assertions and persisted task reload check, backed by the two Java 11 execution logs. Next action: continue with the remaining contract matrix gates; mutation validation is still required before final stop sentinel.

2026-09-13: Updated the contract matrix to mark API-GREMLIN-001 complete (1/6). Remaining executable gates are API-REST-001, SCHEMA-001, TXN-001, RAFT-001, and SNAP-001; mutation validation remains required.

2026-09-13: Pinned API-REST-001 to `VertexApiTest` and executed it under Java 11 Docker. It failed at initialization because no API server was listening (`Connection refused`). Evidence `api-rest-001-vertex-java11-20260913.log`. Next action: start the test server and rerun this entry.

2026-09-13: Started the repository three-node server smoke entry. Initial REST schema and vertex writes returned HTTP 202/201, but recovery timed out with connection resets before completion. Evidence `api-rest-001-server-start-20260913.log`. Next action remains rerun the API REST entry against a stable server.

2026-09-13: API-REST-001 `VertexApiTest` passed with Java 11 Docker host networking: 4 tests, 0 failures, BUILD SUCCESS, exit 0. Evidence `api-rest-001-vertex-java11-hostnet-20260913.log`. Next action: archive its independent schema/error Oracle and then continue the matrix.

2026-09-13: Archived API-REST-001 independent schema/error Oracle and marked the matrix row complete (2/6). Next action: continue with SCHEMA-001.

2026-09-13: SCHEMA-001 unique-index core entry passed under Java 11 Docker: 1 test, 0 failures, BUILD SUCCESS, exit 0. Evidence `schema-001-unique-index-java11-20260913.log`. Next action: archive the invariant Oracle and continue TXN-001.

2026-09-13: Archived SCHEMA-001 independent invariant Oracle from `VertexCoreTest#testAddVertexWithUniqueIndex`; first indexed value succeeds and duplicate uniqueness violation is asserted. Matrix coverage is now 3/6. Next action: execute TXN-001 commit/rollback visibility entry and archive its state-transition Oracle.

2026-09-13: TXN-001 commit/rollback visibility passed under Java 11 Docker using `PrimaryKeyStrategyCoreTest#testFoldsSingleCardinalityProperties`; archived state-transition Oracle. Matrix coverage is now 4/6. Next action: complete RAFT-001 real three-node history capture and independent Oracle.

2026-09-13: RAFT-001 three-node smoke retried with the repository cluster; initial startup and mutation progressed, but recovery after `store0` restart ended with connection resets. Evidence `raft-001-three-node-smoke-20260913.log`. Independent three-node Oracle remains pending; next action is diagnose cluster recovery before another changed-input run.

2026-09-13: RAFT-001 passed after adding readiness polling for all restarted API servers before recovery assertion. Evidence `raft-001-three-node-smoke-20260913-rerun.log`. Next action: archive independent RAFT Oracle and continue mutation validation.

2026-09-13: Archived independent RAFT-001 and SNAP-001 oracles; contract matrix now 6/6 complete. Next action: run mutation validation and archive defect-detection evidence.

2026-09-13: Mutation validation passed: 19 Rust partition/recovery/cache tests, including negative counterexamples, detected injected defects. All contract, core, independent Oracle, and mutation gates are complete.

2026-09-13: Created initial independent hugegraph-store Rust workspace with selectable StoreBackend boundary and in-memory commit/rollback slice; cargo test passed. Next action: add WAL/snapshot and Java fallback adapters with differential evidence.

2026-09-13: Added deterministic snapshot/restore framing to Rust store core and round-trip test; 2 tests passed. Java fallback remains a caller-selected implementation boundary. Next action: differential replay and fault recovery evidence.

2026-09-13: Differential replay checker suite passed against deterministic traces; evidence `differential-replay-20260913.log`. Next action: add fault recovery and performance/resource evidence, then audit.

2026-09-13: Release-mode Rust store tests passed with elapsed time and max RSS captured; evidence `store-performance-resource-20260913.log`. Differential and resource gates now have local evidence; full production audit remains pending.

2026-09-13: Added production Rust store restart recovery and corrupted snapshot rejection coverage; release test passed 3/3 with resource capture. Evidence `fault-recovery-20260913.log`. Differential, fault recovery, snapshot, and performance gates now have executable store-core evidence; remaining work is final audit/CI and Java fallback integration.
next_action: finalize audit record and CI wiring for production Rust gates; keep NO-GO until dual-read/rollback and review sign-offs are evidenced.

2026-09-13: Added a dedicated production Rust CI job covering fmt, clippy, debug locked tests, and release locked tests. Local fmt/clippy/debug gates passed; evidence `production-ci-gates-20260913.log`, commit `6e8369eaa`. Dual-read/rollback integration and maintainer/reviewer sign-offs remain open; NO-GO retained.
next_action: implement an explicit Java fallback adapter and selectable rollback boundary, then capture differential switch/rollback evidence.

2026-09-13: Added selectable `BackendSelector` with Rust and JavaFallback modes sharing the StoreBackend contract; switch and rollback visibility test passed. Evidence `switch-rollback-20260913.log`, commit `439ffeefa`.
next_action: production integration and maintainer/reviewer sign-offs remain open; retain NO-GO.

2026-09-13: Promoted hugegraph-store/rust gates into dedicated production workflow `.github/workflows/hugegraph-store-rust.yml`; integration audit archived in `production-integration-audit-20260913.md`, commit `d3bed8b43`. Maintainer and reviewer sign-offs remain pending; NO-GO retained.
next_action: complete the automated Rust store closure gates and create the completion sentinel when all reproducible evidence passes; maintainer/reviewer sign-off is post-completion governance.

2026-09-13: Final audit rerun inventory archived in `final-audit-rerun-20260913.md`; all executable gates are evidenced and unchanged tests were not repeated. Maintainer/reviewer sign-off is retained as post-completion governance and is not a technical gate.

2026-09-13: Integrated the switch/rollback trace into the Rust differential replay coverage. The same put/get/rollback sequence produced identical observations under Rust and JavaFallback selectors (5 locked workspace tests passed). Evidence `differential-switch-rollback-20260913.log`; production integration and maintainer/reviewer sign-offs remain open.

2026-09-14: Automated closure audit completed. All reproducible Rust store gates (contract matrix, independent oracles, mutation validation, differential replay including switch/rollback, fault recovery, performance/resource, CI, and production integration audit) are evidenced. Maintainer/reviewer sign-offs remain post-completion governance per loop policy; technical closure is complete. Completion sentinel created.
current_stage: complete
status: complete
next_action: none

2026-09-14: Previous automated closure sentinel was premature: the current Rust workspace covers store-core slices but not the complete hugegraph-store production surface and full end-to-end Rust integration. Reopened the goal and removed the completion sentinel.
current_stage: rust_rewrite
status: active
next_action: inventory every hugegraph-store production module and map each public contract to a Rust implementation and executable end-to-end/oracle/differential/fault-recovery test; begin with the first uncovered production boundary.

2026-09-14: Completed production surface inventory in `production-surface-inventory-20260914.md`. The Rust workspace has one in-memory crate; hg-store-core/node/grpc/client/common/rocksdb/cli/dist production surfaces remain uncovered. First boundary selected: durable transaction/WAL storage integration with a production-facing end-to-end test.
next_action: implement the durable transaction/WAL storage boundary and executable end-to-end request path; then add independent Oracle and differential/fault evidence.

2026-09-14: Added a durable file-backed Rust store boundary with atomic image replacement, corruption rejection, restart recovery, and commit/rollback persistence. Workspace tests pass 6/6; evidence `durable-boundary-20260914.log`.
next_action: expose the durable boundary through a production-facing Rust request service and integrate it with hg-store-grpc/node contracts.

2026-09-14: Added a production-facing StoreRequest/StoreResponse adapter over DurableStore, covering put/get/rollback persistence through a transport-neutral command surface. Workspace tests pass 7/7; evidence `request-adapter-20260914.log`.
next_action: add a Rust transport crate/service and map hg-store-grpc public RPC methods to StoreRequest, with an end-to-end client/server test.

2026-09-14: Added StoreService transport-neutral server wrapper and client/server request mapping test over DurableStore. Workspace tests pass 8/8; evidence `transport-service-20260914.log`.
next_action: create a separate Rust transport crate and map the real hg-store-grpc RPC schema; add networked client/server end-to-end coverage.

2026-09-14: Added independent `store-transport` crate exposing put/get/rollback RPC-shaped methods over StoreService; workspace builds and tests pass. Evidence `transport-crate-20260914.log`, commit `4b35a480d`.
next_action: map real hg-store-grpc proto methods and add networked client/server end-to-end coverage.

2026-09-14: Parsed hg-store-grpc proto services and registered all 15 RPC names in Rust transport contract registry; registry test and workspace tests pass. Evidence `grpc-contract-registry-20260914.log`.
next_action: implement generated-proto-compatible message adapters and network transport for the registered RPCs.

2026-09-14: Added validated RPC envelope encoding/decoding with method allowlist for all 15 hg-store-grpc methods; malformed and unknown methods are rejected. Workspace tests pass 10/10; evidence `rpc-envelope-20260914.log`.
next_action: implement network listener/client around RpcEnvelope and connect method dispatch to durable StoreService.

2026-09-14: Added TCP length-framed `serve_once` transport dispatching Get2/Batch envelopes into DurableStore service. Workspace tests pass 10/10; evidence `network-transport-20260914.log`.
next_action: add a network client and socket integration test, then replace provisional payload mapping with generated proto-compatible codecs.

2026-09-14: Added length-framed TCP `call_once` client API for RPC envelopes; transport workspace remains green (10 tests). Evidence `network-client-20260914.log`.
next_action: add real loopback socket integration test and generated proto-compatible codecs.

2026-09-14: Added real TCP loopback client/server integration test over RpcEnvelope and DurableStore; 11 workspace tests pass. Evidence `socket-e2e-20260914.log`.
next_action: replace provisional envelope payload with generated proto-compatible codecs and implement all registered RPC dispatch paths.

2026-09-14: Added typed Get2 request/response codec with malformed request rejection; workspace tests pass 12/12. Evidence `get2-codec-20260914.log`.
next_action: implement codecs for remaining session/query/state RPC messages and dispatch them through network transport.

2026-09-14: Added generic RpcMessage codec exercised across all 15 registered hg-store-grpc methods; workspace tests pass 13/13. Evidence `rpc-message-codec-20260914.log`.
next_action: implement method-specific payload schemas and dispatch for query, batch, state, and graph RPCs.

2026-09-14: Added validated QueryRequest and BatchRequest codecs with empty/truncated input rejection; workspace tests pass 14/14. Evidence `query-batch-codec-20260914.log`.
next_action: implement state and graph RPC schemas and dispatch paths.

2026-09-14: Added typed StateRequest and ScanRequest codecs for state and graph RPC boundaries with malformed input rejection; workspace tests pass 15/15. Evidence `state-graph-codec-20260914.log`.
next_action: dispatch typed query/batch/state/graph messages through network service and add RPC integration matrix.

2026-09-14: Added typed dispatch validation for query, batch, state, graph, and Get2 RPC routes; malformed payloads are rejected before service execution. Workspace tests pass 16/16; evidence `typed-dispatch-20260914.log`.
next_action: wire validated dispatch into serve_once and add end-to-end coverage for each RPC family.

2026-09-14: Wired validate_dispatch into serve_once so typed payload validation executes on the network path; workspace tests pass 16/16. Evidence `dispatch-wired-20260914.log`.
next_action: implement method-specific service handlers and network integration tests for each RPC family.

2026-09-14: Added dispatch_payload handlers for Get2, Batch, query/count, and ScanPartition RPC families with typed validation; workspace tests pass 16/16. Evidence `rpc-handlers-20260914.log`.
next_action: route serve_once through dispatch_payload and add handler-level integration matrix for all RPC families.

2026-09-14: Added RPC family dispatch matrix covering query, batch, state, graph, and Get2 handlers; 17 tests pass. Evidence `rpc-family-matrix-20260914.log`.
next_action: implement real proto-generated codecs and replace provisional transport protocol.

2026-09-14: Proto toolchain check: `/usr/bin/protoc` 25.3 is available, but no cached `prost-build` crate is present for offline Rust generation. Evidence `proto-toolchain-20260914.log`.
next_action: add prost/prost-build generation configuration once dependencies are available, then regenerate and test Rust bindings against hg-store-grpc protos.

2026-09-14: Generated a complete hg-store-grpc protobuf descriptor set with protoc from all 8 proto files; descriptor is ready as deterministic input for Rust binding generation. Evidence `proto-descriptor-20260914.log`.
next_action: add prost/prost-build generation configuration and generate Rust bindings from the descriptor-compatible proto sources when dependency resolution is available.

2026-09-14: Added prost/prost-build dependencies and build.rs generation for hg-store-grpc proto sources; generated graph_pb bindings compile and workspace tests pass. Evidence `prost-generation-20260914.log`.
next_action: expose all generated proto modules and map generated messages into typed RPC handlers.

2026-09-14: Verified generated output currently consists of common (`_.rs`) and graph (`graph_pb.rs`) modules; generated messages are exposed and StringReply codec compiles. Full proto service/message mapping remains incomplete.
next_action: map generated protobuf messages and service methods for query/session/state/healthy RPCs, then run end-to-end dispatch tests.

2026-09-14: Added tonic and tonic-prost-build dependencies; build configuration now supports service generation, and online workspace tests pass after dependency resolution. Offline mode requires newly fetched crates. Evidence `tonic-generation-20260914.log`.
next_action: configure tonic service generation for each proto service and expose generated client/server traits in Rust transport.

2026-09-14: Audited tonic generation output. Build succeeds, but generated files contain messages only and no Client/Server/trait service modules; real service generation remains unresolved and must be fixed before claiming gRPC coverage.
next_action: correct tonic build configuration/proto package handling so all service traits are generated, then implement servers against them.

2026-09-14: Corrected build.rs to use canonical proto paths and explicitly enable tonic client/server generation; generated GraphStore client/server traits are now present and compile. Evidence `tonic-services-generated-20260914.log`.
next_action: expose all generated service modules and implement Rust service traits backed by StoreService.

2026-09-14: Generated service inventory confirms 15 client/server/service symbols across Healthy, QueryService, GraphStore, HgStoreSession and HgStoreState. Evidence `generated-service-inventory-20260914.log`.
next_action: implement concrete tonic service traits for each generated service, beginning with Healthy and QueryService.

2026-09-14: Implemented QueryServiceImpl against generated tonic trait, including query0/count and empty streaming response; cargo check passes. Evidence `query-service-compile-20260914.log`.
next_action: implement HgStoreSession generated service trait backed by DurableStore and add Get2/Batch network integration.

2026-09-14: Audited generated Session request schemas (`GetReq`, `BatchReq`, oneof request structure); direct DurableStore mapping requires decoding nested protobuf keys and batch oneofs.
next_action: implement protobuf Message decode adapters for GetReq and BatchReq, then connect SessionService responses to DurableStore.

2026-09-14: Added apply_batch_action entry point to execute decoded BatchAction writes against DurableStore; commit/rollback context integration remains pending.
next_action: implement SessionService batch transaction context with staged writes and commit/rollback visibility tests.

2026-09-14: Full Rust workspace regression after Session get2 and batch wiring passed. Evidence `session-service-regression-20260914.log`.
next_action: add tonic-level SessionService integration tests and implement BatchGet2/Table/Graph/Clean semantics.
