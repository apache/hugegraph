# HugeGraph Server 测试资产审计

## 范围与可复现基线

本审计覆盖 `hugegraph-server`（含 `hugegraph-test`、各后端和 dist）。截至审计日，仓库中匹配 `*Test.java` 的测试文件为 **165** 个；该数字是资产盘点，不等于已执行覆盖。测试大量位于 `hugegraph-test/src/main/java`，必须以 Maven Surefire 报告确认实际执行。

主要入口来自 `hugegraph-server/hugegraph-test/pom.xml`：`unit-test`、`core-test`、`api-test`、`tinkerpop-structure-test`、`tinkerpop-process-test`，并通过 `rocksdb`/`memory`/`hbase` backend profile 组合。CI (`.github/workflows/server-ci.yml`) 按 backend 矩阵运行 unit、core、API、RAFT API 和 TinkerPop；`pd-store-ci.yml` 还调用 server 的脚本。`methods.filter` 与 `fast-methods.filter` 会改变实际集合，执行清单必须从 Surefire XML 生成。

## 能力域盘点

| 能力域 | 现有资产（代表性套件/类） | Oracle/断言现状 | 首要缺口 |
|---|---|---|---|
| API/REST | `ApiTestSuite`、Vertex/Edge/Schema/Manager/User/Login/Task/Metrics API、traverser API | HTTP 状态、JSON 字段和错误断言；多为 Java 服务自测 | OpenAPI/协议级黄金样例、错误兼容矩阵、分页/并发/超时与幂等 |
| Gremlin/Cypher | `GremlinApiTest`、`GremlinQueryAPITest`、`ProcessStandardTest`、`StructureStandardTest`、`CypherApiTest` | TinkerPop 标准测试 + 结果断言 | 结果顺序/类型规范化、异常语义、长遍历资源上限、跨后端差分 |
| Schema | PropertyKey/VertexLabel/EdgeLabel/IndexLabel/SchemaTemplate 测试及 CoreTest | 创建、更新、删除和约束断言 | 并发 schema 变更、旧版本读取、失败回滚、与索引/数据不变量关联 |
| 索引/查询 | `IndexLabelApiTest`、`IndexLabelCoreTest`、`QueryTest`、RocksDB query tests | 查询结果和索引状态断言 | 索引延迟/重建、崩溃恢复、覆盖率与全表扫描等价性 Oracle |
| 事务/可见性 | `GraphTransactionTest`、`GraphIndexTransactionTest`、Cached*TransactionTest | 提交/回滚及局部可见性 | 并发冲突、隔离级别、重试/超时、进程崩溃后的 durability |
| 权限/认证 | `AuthTest`、`LoginApiTest`、`RolePermissionTest`、GraphSpace auth tests | 角色和 HTTP 鉴权断言 | 权限矩阵完整性、租户隔离、token 过期/撤销、负向越权测试 |
| 持久化/恢复 | `RestoreCoreTest`、RocksDB session/table tests、dist `InitStoreTest` | 本地 backend 操作和恢复断言 | 跨版本格式、损坏介质、快照原子性、备份恢复及升级/回滚 |
| 分布式/RAFT | `RoleElectionStateMachineTest`，server RAFT API 脚本 | 局部状态机/接口断言 | 三节点分区、成员变更、重复/乱序消息、线性一致性和故障注入 |

## 完备性判定

当前结论为“资产已收集、语义覆盖未证明”。最终矩阵必须逐测试方法记录 `contract_id`、实际 CI job/profile、Oracle 类型、可回放性、故障计划和结果。Java 实现只能用于差分，不能单独充当真值。关键契约应同时依赖公开规范、数据不变量或模型 Oracle，并以故意注入错误实现的负向测试证明测试能失败。

## 补测与迁移门禁

在 Rust 替换前，优先补齐：API 黄金样例与错误矩阵；事务并发/崩溃恢复；索引重建与全表等价性；旧格式和快照损坏恢复；RAFT 分区、成员变更和线性一致性；权限越权；以及所有 filter 排除项的审计。每个替换单元须先在 Java 基线固定数据、配置、seed 和 Surefire 报告，再运行 Rust 与 Java 的规范化差分、故障注入和性能基准；任一契约、恢复或性能门禁失败即停止扩大范围并回滚。

## 证据产物

最终审核需提交：Surefire 方法全集及未执行清单、能力—契约—Oracle 追踪矩阵、独立 Oracle 说明、负向测试报告、故障回放包、跨 backend 差分报告和性能基线。没有这些证据，不得将本审计标记为“完备”。
