# PD Partition 契约盘点试点

状态：POC 计划（文档审核通过后执行）。本文定义验证方法和通过条件，不将计划当作已完成验证。

这是将测试盘点方法落地的第一个试点边界。选择 Partition 是因为它同时包含状态变更、心跳、清理任务和合并操作，且存在 core/service 两层测试。

## 复核更正（2026-09-13）

此前“模型层闭环”的结论撤回：现有 POC 尚未连接 Java 实现，也没有进程崩溃、持久化恢复或实现级变异执行器。输入反例测试不能替代实现级变异杀伤率，重复调用 replay 不能证明崩溃恢复。下方历史执行记录只证明当时所运行用例的结果。

本次修正两个检查错误：范围覆盖不要求不同分区的版本递增；固定参考状态的缓存一致性要求范围与版本全部相等，不能只比较版本大小。新增同版本错误范围、未来版本两个反例。verify.sh 实际执行 14 项通过，同时 fmt/clippy 通过；这些结果仍不是 Java 兼容或生产恢复证明。

## 1. 当前实际测试入口

| 测试类 | 已发现方法 | 执行入口 |
| --- | --- | --- |
| `pd/core/PartitionServiceTest` | `testCombinePartition`、`testCombinePartition2`、`testHandleCleanTask`、`testPartitionHeartbeat` | `hg-pd-test`，`pd-core-test`（需用 Surefire 报告确认） |
| `pd/service/PartitionServiceTest` | `testCombinePartition`、`testCombinePartition2`、`testHandleCleanTask` | `hg-pd-test`，service/rest 相关 profile（需确认） |
| `pd/common/PartitionUtilsTest` | 以类中实际 `@Test` 方法为准 | common profile（需确认） |
| `pd/common/PartitionCacheTest` | 以类中实际 `@Test` 方法为准 | common profile（需确认） |

## 2. 独立契约与 Oracle

| 契约 | 独立 Oracle | 现有状态 |
| --- | --- | --- |
| 分区合并不丢失或重复范围 | 分区范围不重叠、覆盖集合等价、版本单调 | 有测试，未见统一不变量比较器 |
| 心跳更新只接受合法节点/版本 | 状态机不变量和拒绝矩阵 | 有测试，需补过期、乱序和重复心跳 |
| 清理任务幂等 | 重复执行后的状态摘要相同 | 有测试，需补崩溃重试 |
| 缓存失效后读到最新元数据 | 服务端状态与缓存结果等价 | 有测试，需补并发失效 |
| 合并/迁移期间的故障恢复 | 操作日志重放后状态等价 | 缺少明确端到端故障 Oracle |

## 3. 负向验证

试点必须先加入能杀死错误实现的测试，再进行 Rust 实现。至少验证以下人为缺陷会失败：

1. 合并时丢弃右侧分区；
2. 接受更旧版本的心跳覆盖新状态；
3. 清理任务重复执行产生重复副作用；
4. 缓存失效通知丢失后继续返回旧元数据；
5. 节点重启后恢复出不连续的分区范围。

若任一缺陷无法被测试发现，应先修订 Oracle 或补测，不能把该边界标记为可迁移。

## 4. 试点完成条件

- 逐方法确认真实 profile 和 CI job；
- 每个方法映射到契约编号和 Oracle 类型；
- 补齐乱序、重复、崩溃、重启和恢复场景；
- 完成至少五类错误注入并全部被测试捕获；
- Java 基线与 Rust 实现通过相同确定性历史的规范化比较；
- 产出包含输入、seed、配置、日志、差异和结论的报告。

试点通过后，复制模板到 Store Snapshot、事务和 Raft 成员变更；试点未通过时，暂停生产路径迁移。

## 6. POC execution evidence (2026-09-10)

A standalone invariant oracle was implemented at `tools/rust-partition-poc`. It has no dependency on Java output or production PD code. Running `cargo test --manifest-path tools/rust-partition-poc/Cargo.toml` produced **6 passed, 0 failed**. The suite detects dropped ranges, overlaps, version regression, stale heartbeats, and verifies heartbeat idempotence. This is a model-level POC only; cache invalidation, crash replay, restart recovery, Java differential replay, and two-person review remain open gates.

补充执行结果：Oracle 已加入确定性日志重放、损坏重启状态和重复过期事件测试，当前共 **9 passed, 0 failed**。这完成了模型层的恢复/重放闭环；服务级缓存通知丢失和 PD 进程崩溃编排仍必须在 `hg-pd-test` 集成环境执行，不能由模型测试替代。

真实集成执行记录（2026-09-10）：RAT 已通过，但 Maven 在解析 `maven-surefire-plugin:2.20` 时因当前环境无法写入 `/home/zy/.m2` 且缺少缓存而停止，未进入测试阶段。该结果标记为环境阻塞，不能计作通过；重试需提供可写 Maven 本地仓库并保存 Surefire 报告。

重试记录：改用 `-Dmaven.repo.local=/tmp/hugegraph-m2` 后，构建因 DNS 无法解析 `repo.maven.apache.org`，缺少 `org.apache:apache:23` 父 POM而停止。该环境阻塞仍未计作测试通过。

Rust 工程门禁记录：`cargo fmt -- --check`、`cargo clippy --all-targets -- -D warnings` 和 `cargo test` 均通过；测试仍为 9 项通过。

缓存 Oracle 补充：新增缓存版本等价与失效拒绝测试；`verify.sh` 运行结果为 **11 passed, 0 failed**。这仍是模型层证据，真实 watch 通知丢失场景待 PD 集成环境恢复后执行。

仓库级校验记录（2026-09-10）：`mvn -B -o -DskipTests validate` 全模块通过（`BUILD SUCCESS`），RAT、Checkstyle 与 POM 校验均通过；该结果不替代需要外部依赖的真实集成测试。

真实集成结果（2026-09-10，修复初始化边界后）：`hg-pd-test` Partition 过滤测试共 32 项，30 通过、0 错误、2 失败。`PartitionServiceTest.testCombinePartition` 与 `testCombinePartition2` 均在合并后范围断言失败：期望 `endKey=5462`，实际 `16386`。该差异尚未解释，不得标记契约兼容或 GO；Surefire 原始报告位于 `hugegraph-pd/hg-pd-test/target/surefire-reports`，完整日志为 `/tmp/pd-partition-test-2.log`。

真实集成修复与复验（2026-09-10）：发现测试类共享 `graph0` 元数据导致分区配置污染；将 core PartitionServiceTest 改为专属 graph 名称后，真实 Maven 执行 `PartitionServiceTest` **4/4 通过，0 失败，BUILD SUCCESS**。该修复已提交 `202650f71`。

完整 Partition 集成复验（2026-09-10）：清理隔离数据后执行 `-Dtest='*Partition*Test'`，core 4、service 3、cache 23、utils 2，共 **32/32 通过，0 失败，BUILD SUCCESS**。完整日志：`/tmp/pd-partition-all.log`。

跨模块回归复验（2026-09-10）：完整 `pd-core-test,pd-common-test` 通过；Common 83 项，Core 104 项（跳过 2 项），失败/错误均为 0，`BUILD SUCCESS`。完整日志：`/tmp/pd-core-common-all.log`。

POC Oracle 复核（2026-09-13）：修正版本比较与缓存等价性错误后，`verify.sh` 为 14 项通过。该结果仅证明独立模型自身的反例检测，不能替代服务实现级变异和 Java/Rust 差分。

Release 基线（2026-09-13）：`cargo test --release` 14/14 通过；本机一次运行 `elapsed_seconds=0.64`、`max_rss_kb=124420`（仅模型 POC，不能代表生产性能）。完整输出：`/tmp/rust-partition-release.log`。

最新复验归档（2026-09-13）：`tools/rust-partition-poc/verify.sh` 在提交 `100834d5ab318f6285f0d177f47d26017e600cae` 上完成 14 项测试；原始输出已归档至 `docs/evidence/rust-rewrite/partition-poc-verify.log`，SHA-256 为 `e96e1c0767dd1995d1a7e9e7ae3c71118115e3ab1f902f291ec7d4d1b9f54ec0`。
