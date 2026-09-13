# PD/Store 测试资产审计（可审查版本）

## 范围与盘点结果

本审计覆盖 `hugegraph-pd/hg-pd-test` 与 `hugegraph-store/hg-store-test` 的源码测试（不计 `target/` 生成物）。当前分别发现 55、64 个 `*Test.java` 文件。两者测试主要位于 `src/main/java`，因此不能仅依赖 Maven 默认 `src/test` 规则判断执行情况。

PD 测试按 `core`、`raft`、`client`、`grpc`、`rest`、`common` 等 suite 组织；Store 测试按 `service` suite 及核心、业务、客户端、Raft fake-PD 场景组织。必须以 suite 配置和 Surefire XML 为实际执行证据。

## 已有覆盖

| 能力 | 现有证据 | 结论 |
|---|---|---|
| PD 分区/Store 节点服务 | `PartitionServiceTest`、`StoreServiceTest`、`StoreNodeServiceTest` | 有单服务行为覆盖；缺少跨进程故障证据 |
| PD Raft 基础 | `RaftEngineReadinessTest`、`RaftEngineLeaderAddressTest`、`RaftEngineIpAuthIntegrationTest` | 包含就绪、leader 地址及认证；未证明日志恢复和线性一致性 |
| PD 客户端/Watch | `PDClientTest`、`PDWatchTest`、`PDPulseTest` | 有 API/回调覆盖；缺少断线重连与乱序事件模型 |
| Store Raft 路径 | `HgSessionManager*RaftFakePDTest` | fake-PD 单节点/多 partition 场景；不等价于真实 Raft 集群 |
| 快照 | `HgBusinessImplTest.testLoadSnapshot` 及 snapshot 相关方法 | 有加载路径；缺少生成、校验失败、增量、崩溃恢复 |
| 分区扫描/合并 | `OrderedMultiPartitionIteratorTest`、scan 场景 | 有排序和游标测试；缺少迁移期间一致性 |

## 执行入口

- PD：`mvn test -pl hugegraph-pd/hg-pd-test -am`；`jacoco` 仅生成覆盖报告，不代表额外测试。
- Store：`mvn test -pl hugegraph-store/hg-store-test -am`；`jacoco` profile 引入 RocksDB 并生成报告。
- 最终清单必须从 Maven effective-pom、suite 类、Surefire XML 和 CI workflow 交叉确认；路径扫描结果不得作为“已执行”证明。

## 关键缺口（迁移阻断）

1. **Raft 正确性**：三节点选举、网络分区、消息重复/乱序、leader 崩溃、成员变更、日志截断与快照安装；用线性一致性 checker 或状态机模型作 Oracle。
2. **持久化恢复**：提交前/后进程崩溃、磁盘重启、损坏日志、快照校验失败、旧格式读取；要求可重复故障计划和校验和。
3. **分区生命周期**：创建/分裂/迁移/删除期间读写，副本落后追赶，leader 转移；验证数据不丢、不重、不跨分区错读。
4. **事务语义**：并发写、可见性、重试幂等、超时后的提交状态；使用独立状态模型而非 Java 输出作为唯一真值。
5. **升级回滚**：滚动升级、协议版本兼容、快照跨版本恢复、回滚后继续写入；保留旧版本基线。
6. **真实集成**：fake-PD 测试需增加真实 PD/Store 多进程或容器拓扑，并纳入 CI nightly。

## 补测计划与验收门禁

先以一个三节点 PD + 三节点 Store 的最小拓扑做 POC：固定数据集、配置、随机 seed 和故障脚本，记录每次操作日志。随后补齐上述五类场景，每类至少包含成功、超时、崩溃和恢复用例。故意注入丢日志、跳过索引更新、错误 leader 判断等缺陷，测试必须失败（mutation score 目标 100% 针对关键不变量）。

Rust 迁移单元只有在：相关测试方法已映射契约编号；Oracle 独立于 Java 实现；CI profile 可重复执行；故障与恢复场景通过；性能基线（吞吐、P99、CPU、内存）有报告；失败可回滚，才允许扩大替换范围。未满足即标记 `migration-blocked`。

## 审计限制

本文件是基于源码和构建配置的资产审计，不声称现有测试完备。方法级契约映射、实际 Surefire 执行结果和补测报告需在 POC 阶段提交后，才能将条目标记为 `covered`。

2026-09-13 复核：完整 `hg-store-test` 构建曾成功，现有报告显示 61 项无失败；但 Snapshot 测试类 `HgSnapshotHandlerTest` 未纳入 `CoreSuiteTest`（该 suite 当前整体注释），且用假的 SnapshotReader/Writer，不能证明真实快照持久化恢复。Store Snapshot 仍保持迁移硬门禁，需新增可执行 suite、真实文件 round-trip、损坏快照和重启恢复测试。

进一步复核：`HgSnapshotHandlerTest` 针对的是已标记 `@Deprecated` 的 `HgSnapshotHandler`，而运行时状态机使用 `SnapshotHandler`。因此现有 4 个 Snapshot 测试不能作为生产 Snapshot 契约证据；必须迁移到活动实现并执行真实文件 round-trip。

2026-09-13 活动实现复验：将测试切换为 `SnapshotHandler` 并更名文件后，离线执行未能进入编译；`hugegraph-core` 仍缺少多项缓存依赖（如 jackson/kubernetes/gremlin），日志 `/tmp/snapshot-active-offline.log`。该测试目前没有结果证据，Snapshot 门禁继续保持未满足。

活动实现复验通过（2026-09-13）：将测试切换为 `SnapshotHandler`，并修复 `StoreEngineTestBase` 未设置 `JobOptions`（线程参数为 0）导致引擎启动失败的问题；使用 `-Djacoco.skip=true`（当前 JaCoCo 不支持 JDK class 65）执行，`SnapshotHandlerTest` **4/4 通过，0 失败，BUILD SUCCESS**。日志：`/tmp/snapshot-active-final4.log`。测试仍未覆盖真实损坏快照和跨进程重启恢复。

Snapshot 本地保存标记测试（2026-09-13）：新增 `SnapshotHandlerTest.testSnapshotLoadSkipsLocallySavedSnapshot`，活动 `SnapshotHandler` 测试共 **5/5 通过**（`-Djacoco.skip=true`，因 JaCoCo/JDK class 65 不兼容）。该测试覆盖跳过路径，真实损坏文件和跨进程恢复仍未覆盖。

Store 全量回归（2026-09-13）：活动 SnapshotHandler 测试纳入后，执行完整 `hg-store-test`（`-Djacoco.skip=true`），所有已启用 suite 均通过，`BUILD SUCCESS`；日志：`/tmp/store-regression-final.log`。该回归仍不包含真实跨进程崩溃恢复和损坏快照注入。

Profile 隔离复验（2026-09-13）：尝试将活动 SnapshotHandlerTest 加入 `store-core-test` 后，Snapshot 5/5 通过，但随后 BatchGraphIsolationTest 因 JRaft `TableFormatConfig` 静态重复注册失败。已撤销该 profile 合并，保留独立 Snapshot 执行入口；Store 测试 profile 需要 fork/JVM 隔离设计后才能宣称 CI 覆盖闭环。

Profile 隔离修复（2026-09-13）：为 `store-core-test` 设置 `forkCount=1`、`reuseForks=false` 并纳入活动 `SnapshotHandlerTest`；Snapshot 5/5、BatchGraphIsolation 6/6 均通过，profile 总计 11 项通过，`BUILD SUCCESS`。日志：`/tmp/store-core-fork.log`。该配置消除了 JRaft 静态注册跨 suite 污染。
