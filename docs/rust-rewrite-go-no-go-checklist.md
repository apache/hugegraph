# Rust 重构正式开工 Go/No-Go 清单

本文是从设计阶段进入正式实现阶段的唯一门禁。本轮仅要求本地可复现的端到端集成测试和完备 Oracle；不依赖生产流量、生产数据、外部凭证或社区签核。

本轮验收范围限定为本地固定数据下的数据不回退、重构前后 Rust 契约一致，以及 Java/Rust 本地端到端集成结果一致。生产发布相关条件不属于本轮验收范围。

## 1. 本地必要条件

以下任一项未满足，正式重构不得开始：

- [ ] 重构目标、非目标和成功指标已写入版本化文档，并由独立子 agent 检查一致性；
- [ ] 本次实现边界、依赖边界和不迁移范围明确；
- [ ] 外部协议、数据语义、事务、索引、Raft、快照和恢复不变量已编号；
- [ ] 现有测试入口、Maven profile、CI job 和 filter 已完成资产盘点；
- [ ] 当前边界的测试已映射到契约、Oracle 和执行入口；
- [ ] Java 基线、输入数据、配置和结果可重复；
- [ ] Rust toolchain、MSRV、依赖许可证、`unsafe`/FFI 政策和 CI 门禁已确定；
- [ ] 数据格式、升级、降级和回滚方案已由独立子 agent 评审；
- [ ] 本地测试所需的资源、稳定性和恢复阈值已预先定义；
- [ ] 独立子 agent review 的反馈已分类并有处理结论。

## 2. 充分条件

只有以下证据全部具备，才能把边界状态标记为 **GO**：

- [ ] 设计文档、测试审计、验收矩阵和工程规范通过 review；
- [ ] 该边界至少完成一次 POC，证明架构假设可行；
- [ ] 关键契约有独立 Oracle，而不是只复制 Java 输出；
- [ ] 负向变异测试能捕获预先注入的错误；
- [ ] 集成测试、差分回放、故障注入和恢复测试均有确定性结果；
- [ ] POC 未发现无法解释的协议、数据或一致性差异；
- [ ] 本地双实现差分和可重复回滚测试已通过；
- [ ] 本地资源结果达到文档中定义的阈值；
- [ ] 至少两个相互独立的子 agent 分别完成上下文隔离的 review；其中一个子 agent 不参与主要实现，并分别记录 review 结论；
- [ ] 契约覆盖率、核心场景覆盖率和独立 Oracle 覆盖率均为 100%；
- [ ] 关键契约变异杀伤率为 100%，未解释差异和未执行测试均为 0；
- [ ] Go/No-Go 记录包含提交版本、配置、数据集、seed、日志、报告链接和未解决风险。

## 3. 当前状态

当前已补齐 Partition POC、三节点故障/网络分区恢复、跨服务器可见性、构建/RAT 证据和独立单寄存器线性一致性 Oracle；负向快照/损坏恢复、真实三节点历史线性一致性、变异测试、本地端到端集成和 Oracle 仍未完成，因此当前结论为 **NO-GO（允许继续本地重构）**。

## 4. 本地重构顺序

```text
独立子 agent review 文档
→ 修订并签核 Go/No-Go 清单
→ 完成一个边界 POC
→ 执行契约/集成/故障/负向验证
→ 记录报告并确认 GO
→ 进入该边界的 Rust 实现
→ 每个小步重复同一门禁
```

每个边界都必须重复本地契约、集成和 Oracle 验证。

## 5. 证据索引与责任闭环

本轮循环门禁验证记录：`evidence/rust-rewrite/continuation-gate-20260913.md`（脚本语法检查通过，Partition POC 19 项测试通过）。

清单中的复选框只在证据已经产生、可复现并由责任人签核后勾选。当前仓库内已经形成的证据如下；它们证明“准备工作完成”，不等同于生产 GO：

| 条件 | 证据 | 当前结论 | 下一责任人/产物 |
|---|---|---|---|
| 目标、边界、不变量 | `rust-rewrite-design.md`、`rust-rewrite-principles.md` | 待维护者确认 | 维护者；评审结论 |
| 测试资产与执行入口 | `rust-rewrite-test-inventory.md`、`rust-rewrite-server-test-audit.md`、`rust-rewrite-pd-store-test-audit.md` | 已盘点，语义映射仍需逐项复核 | 测试负责人；追踪矩阵 |
| 验收与阈值 | `rust-rewrite-acceptance-matrix.md` | 已定义，待批准 | 维护者；阈值签核 |
| Rust 工程规范 | `rust-engineering-standard.md` | 已定义，待 CI 落地 | Rust 负责人；CI 配置与检查日志 |
| Oracle 与差分策略 | `rust-rewrite-oracle-finalization.md`、`tools/raft-linearizability/`、`evidence/rust-rewrite/raft-linearizability-oracle-20260913-concurrent.log` | 已有独立单寄存器顺序/重叠历史 Oracle 和输入校验回归；尚未接入真实三节点历史 | 测试负责人；三节点历史采集、差分报告 |
| POC 范围与退出条件 | `rust-rewrite-partition-pilot.md`、`evidence/rust-rewrite/partition-poc-verify.log` | POC 已执行通过 | 测试负责人；扩大故障矩阵 |
| 变异、故障、恢复验证 | `rust-rewrite-acceptance-matrix.md`、`evidence/rust-rewrite/compose-*-recovery-20260913.log` | 已完成部分故障/恢复证据，仍缺快照损坏和线性一致性 | 测试负责人；补齐负向与并发报告 |
| 独立子 agent review | PR 分支 `refactor/rust-rewrite-design` | 尚未签核 | 至少两个独立子 agent；分别记录 review 结论 |

因此文档与部分验证阶段已闭环；在独立 Oracle、负向/并发验证和外部签核进入仓库前，清单必须保持 **NO-GO**，不得用“计划存在”替代“证据存在”。
