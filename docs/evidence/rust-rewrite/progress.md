# Rust 重写后台进度

current_stage: contract_inventory
status: active
commit: working-tree
tests: "matrix schema reviewed; no execution claimed"
evidence: docs/evidence/rust-rewrite/e2e-contract-matrix-20260913.md
next_action: 为 SNAP-001 增加真实快照 round-trip；随后固化 API-GREMLIN-001 的可执行入口。

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
