# Rust 重构测试 Oracle 与契约追踪最终化方案

## 目标

将测试资产盘点从文件清单升级为可审计的“能力—契约—测试—Oracle—结果”证据链。该文档定义字段、状态机、独立 Oracle、负向测试和最终完成条件，作为 Rust 小步替换的准入门禁。

## 追踪记录模型

每条测试方法一行，使用稳定的 `trace_id`（模块、类、方法名变更时不得复用旧语义）。必填字段如下：

| 字段 | 含义 |
|---|---|
| `trace_id` | 唯一追踪标识 |
| `module` / `class` / `method` | 源码位置 |
| `capability` | API、图语义、Schema、事务、索引、存储、Raft、运维等能力域 |
| `contract_id` | 契约登记号；没有契约不得标记 covered |
| `test_kind` | unit、integration、e2e、property、model、fault、compatibility |
| `execution_profile` / `ci_job` | 实际执行入口 |
| `oracle_type` | spec、invariant、model、reference、differential |
| `oracle_ref` | 规范、模型或代码路径的可定位引用 |
| `independent` | Oracle 是否不依赖被测实现（yes/no/partial） |
| `negative_case` | 是否包含错误注入或反例（yes/no） |
| `replay` | 数据、配置、seed、故障计划是否可复现 |
| `status` | 见下方状态机 |
| `evidence` | CI 报告、日志、差异文件、审查记录 |
| `reviewer` / `reviewed_at` | 非实现人员审核信息 |

## 状态机

`inventory` → `classified` → `contract-linked` → `oracle-linked` → `executed` → `reviewed` → `accepted`。任一步失败转为 `blocked`，并填写阻断原因；修复后回到失败前状态重新验证。`accepted` 不允许直接编辑，契约或实现改变时创建新版本记录。

状态定义：

- `inventory`：仅确认测试存在。
- `classified`：完成能力域、测试类型和执行入口归类。
- `contract-linked`：契约有输入、输出、错误和不变量。
- `oracle-linked`：Oracle 可定位且证明独立性。
- `executed`：在固定提交、配置、数据集和 seed 下实际运行。
- `reviewed`：独立审核通过，且差异有解释。
- `accepted`：满足迁移门禁，可作为 Rust 替换证据。
- `blocked`：缺契约、Oracle、执行入口、复现条件或存在未解释失败。

## Oracle 分层规则

优先级从高到低：公开协议/标准（TinkerPop、HTTP、Protobuf）→ 数据与状态不变量 → 独立有限状态模型/属性生成器 → 经过审查的参考实现 → Java 对照差分。Java 只能发现行为差异，不能单独证明正确性。`independent=no` 的记录最高只能为 `partial`，不能进入 `accepted`。

关键域最低要求：Raft 必须有线性一致性检查、崩溃/分区故障注入和快照恢复；事务必须有提交可见性不变量及并发模型测试；数据格式必须有跨版本黄金样本；API 必须有规范断言和错误响应矩阵。

## 负向测试

每个关键契约至少一个可重复的错误变异：删除索引更新、改变事务可见性、丢弃或重复 Raft 日志、损坏快照、篡改错误码或跳过权限校验。测试必须检测到变异并失败；否则标记 `missing-oracle`/`blocked`。变异脚本、seed 和预期失败断言纳入 `evidence`，禁止只凭代码覆盖率替代。

## 最终化流程

1. 解析 Maven profile、Surefire、filter 和 CI，冻结实际执行全集。
2. 为每项能力登记版本化契约和不变量。
3. 逐方法填充追踪字段，双人独立归类并解决分歧。
4. 建立独立 Oracle，执行负向变异验证其杀伤力。
5. 固定环境运行，保存报告、日志、seed、配置和数据集。
6. 对失败进行最小化并转为确定性回归用例。
7. 非实现人员审核后生成冻结版 CSV 和缺口清单。

## 完成条件

只有同时满足以下条件，测试盘点才可称为最终版并允许 Rust 单元替换：

- 所有 CI 实际执行方法均达到 `reviewed` 或明确标记为 `irrelevant`；
- 所有关键能力均有契约，且每条关键契约至少一个独立 Oracle；
- 关键契约的负向变异测试全部被检测；
- 无 `blocked` 项，或阻断项已登记为迁移阶段硬门禁并有负责人和截止版本；
- 测试可在固定环境中重复，结果和差异证据已归档；
- 追踪矩阵、契约目录、Oracle 版本和 CI 报告相互可追溯；
- 至少一名非实现人员完成审核并签名。

未满足任一条件时，只能称为“盘点底稿”，不得宣称契约兼容或测试完备。
