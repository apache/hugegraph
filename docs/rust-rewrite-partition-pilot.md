# PD Partition 契约盘点试点

状态：POC 计划（文档审核通过后执行）。本文定义验证方法和通过条件，不将计划当作已完成验证。

这是将测试盘点方法落地的第一个试点边界。选择 Partition 是因为它同时包含状态变更、心跳、清理任务和合并操作，且存在 core/service 两层测试。

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
