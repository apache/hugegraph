# Rust 重构验收矩阵

状态：第一轮盘点完成；“已发现”不等于“已通过”。本矩阵是迁移的强制门禁。

## 1. 现有测试资产覆盖盘点

以下统计使用 `rg -l '<关键词>' --glob '*Test.java'`，用于定位测试资产，不用于证明覆盖率：

| 能力域 | 命中测试文件 | 主要资产位置 | 当前判断 |
| --- | ---: | --- | --- |
| REST/API | 68 | `hugegraph-test`、PD REST、Store gRPC/service | 有大量测试，需抽取协议断言 |
| Gremlin/遍历 | 38 | `hugegraph-test` API、traversal、TinkerPop suites | 有覆盖，需建立规范化结果 Oracle |
| Schema | 48 | server core/API、PD metadata | 有覆盖，需补跨版本和失败恢复 |
| Index | 28 | server core/backend/API | 有覆盖，需补重启、删除和重建一致性 |
| Transaction | 26 | graph/backend/store | 有覆盖，需补并发、可见性和幂等 |
| Snapshot | 11 | Store RocksDB、PD/Store 服务 | 数量偏少，列为迁移阻断项，需补恢复矩阵 |
| Raft | 31 | PD raft、Store raftcore | 有基础测试，需补分区、崩溃、成员变更和线性一致性历史检查 |
| Partition | 23 | PD partition、Store engine | 有覆盖，需补重平衡和故障中迁移 |
| Auth/权限 | 37 | server auth/API/PD auth | 有覆盖，需拆出协议和安全不变量 |
| 升级/迁移 | 2 | 分散在配置/版本测试 | 明显不足，列为迁移阻断项 |

执行入口不是单一套件：Server 使用 `unit-test`、`core-test`、`api-test`；PD 使用 `pd-core-test` 等 profile；Store 使用 `store-core-test`、`store-raftcore-test`。CI 还通过脚本分别执行 unit/core/API/Raft 任务，盘点必须以 CI 实际命令为准。

## 2. 验收契约矩阵

| 契约 | 独立 Oracle | 必须执行的验证 | 通过条件 |
| --- | --- | --- | --- |
| REST/gRPC 请求响应 | OpenAPI/Protobuf + 黄金样例 | Java/Rust 差分回放、非法输入、超时和错误码 | 字段、状态、错误语义无未解释差异 |
| Gremlin/遍历 | TinkerPop 语义 + 结果规范化器 | 固定语料、随机遍历、空/重复/排序边界 | 结果集合、顺序承诺和异常一致 |
| Schema/索引 | 图数据不变量 | 创建/更新/删除、重启、重建、约束失败 | 不变量始终成立，索引不丢不重 |
| 事务 | 可见性和提交不变量 | 并发、冲突、重试、崩溃点注入 | 提交/回滚/可见性符合契约 |
| 持久化格式 | 版本化格式规范 | 旧数据读取、快照、升级、回滚 | 新实现可读旧数据，失败可恢复 |
| Raft/分布式 | 线性一致性模型 | 分区、崩溃、乱序、重复、成员变更 | 历史通过模型检查，无数据丢失 |
| 认证权限 | 权限矩阵和拒绝不变量 | 全角色/资源/操作组合、过期凭证 | 无越权，错误响应稳定 |
| 性能资源 | 现有版本基线 | 固定数据集、固定并发、长稳测试 | 达到预先批准的阈值且无退化逃逸 |
| 运维升级 | 发布/回滚状态机 | 滚动升级、降级、配置兼容、备份恢复 | 任一步失败可停止并回滚 |

## 2.1 现有入口到契约的追踪

这是当前已确认的测试入口；迁移实施时必须把每个入口下的方法继续展开到测试方法级。

| 测试入口 | Profile/执行方式 | 当前可覆盖契约 | 必须补出的 Oracle/场景 |
| --- | --- | --- | --- |
| `hugegraph-server/hugegraph-test` | `unit-test`、`core-test`、`api-test` | core、API、Gremlin、Schema、事务、序列化 | 统一请求生成器、Java/Rust 规范化差分、故障恢复 |
| `hugegraph-server/.../tinkerpop` | core/TinkerPop profile，受 filter 影响 | TinkerPop structure/process | 明确 HugeGraph 承诺的步骤子集、顺序和异常 Oracle |
| `hugegraph-pd/hg-pd-test` | `pd-core-test`、client/common/rest profile | PD 服务、客户端、分区、Raft readiness | 三节点分区历史、快照、成员变更和升级 Oracle |
| `hugegraph-store/hg-store-test` | `store-core-test`、`store-raftcore-test` | Store core、RocksDB、Raft、partition、session | 崩溃恢复、重复/乱序消息、线性一致性和数据校验 |
| `hugegraph-store/hg-store-rocksdb` | 模块测试 | RocksDB session、factory、snapshot | 旧格式读取、损坏快照、恢复后索引/计数等价 |
| `.github/workflows/*-ci.yml` | CI 脚本组合 | 实际执行集合和报告 | 固定 CI 清单，禁止 filter 静默漏测，上传差分报告 |

## 3. Oracle 与反射防止

Java 版本只作为参考实现，不能单独作为 Oracle。每条关键契约必须至少有一个规范或关系 Oracle；差分测试只负责发现差异，最终由规范/不变量判定对错。随机测试必须保存 seed、生成器版本、初始快照和故障计划，并将失败缩减为确定性回归用例。

## 4. 小步替换闭环

每个 Rust 边界必须按以下状态机推进：

```text
盘点现状 → 写契约 → 补 Oracle/测试 → Java 基线
   → Rust 实现 → 单测/集成/故障/差分
   → 双读或灰度 → 观察窗口 → 扩大范围或回滚
```

### 进入条件

- 本边界的能力、协议和数据不变量已登记；
- 现有测试已映射到 profile 和 CI 命令；
- 所有关键行为有独立 Oracle；
- 缺口已补测试，或被明确登记为禁止迁移项；
- 已定义回滚开关、数据校验和观察指标。

### 退出条件

- 协议、语义、持久化和权限测试全部通过；
- 故障注入、恢复、升级和回滚通过；
- 差分回放无未解释差异；
- 性能、资源和长稳达到批准阈值；
- 生成带版本、配置、输入、seed、日志和差异清单的验收报告。

任一退出条件失败，边界保持旧实现，不得扩大迁移范围。

## 5. 当前阻断项

在完成以下工作前，不应迁移 Store 写路径或核心图执行引擎：

1. 补齐快照/恢复、升级/回滚和分布式故障测试；
2. 确认 CI profile 的实际测试集合，清理被 filter 排除但仍属于契约的场景；
3. 为 REST、Gremlin、Schema、索引和事务建立规范化差分工具；
4. 建立 Raft 历史检查和可重复故障注入；
5. 为每项契约指定负责人、测试路径和通过阈值。

完成这些项目并提交验收报告后，方案才形成可执行闭环。

## 6. 补测任务清单

以下任务是从当前盘点直接得到的，不是围绕 Rust 实现临时编写的测试：

1. 导出所有 Maven profile 和 CI 脚本实际运行的测试类/方法，生成基线清单；
2. 为 REST、gRPC、Gremlin、Schema、索引、事务定义脱离实现的规范化比较器；
3. 增加旧 RocksDB 数据、快照导入导出、版本升级和失败回滚的端到端用例；
4. 增加 PD/Store 三节点崩溃、分区、延迟、重复消息、成员变更和恢复用例；
5. 为随机图操作和事务历史保存 seed，并增加最小化失败历史的回归格式；
6. 建立测试结果报告，强制记录 profile、提交版本、配置、数据集、Oracle 版本和差异；
7. 对每个缺口指定“补测后才允许迁移”的门禁，禁止以提高覆盖率数字替代语义验证。
