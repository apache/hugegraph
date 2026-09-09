# Rust 重构测试资产盘点（初版）

状态：盘点进行中。本文记录可重复的现状扫描结果，不把测试数量当作完备性证明。

## 1. 可重复统计

统计命令：

```bash
find <module> -type f -name '*Test.java' | wc -l
```

当前仓库扫描结果：

| 模块 | `*Test.java` 文件数 | 初步观察 |
| --- | ---: | --- |
| `hugegraph-server` | 165 | API、core、后端和集成测试分散在多个模块；部分测试位于 `src/main/java` 的测试工程目录 |
| `hugegraph-pd` | 61 | 有 REST、gRPC、客户端、服务和 Raft 相关测试，也有多节点 live 测试资源 |
| `hugegraph-store` | 70 | 有 RocksDB、Raft、分区、服务和会话测试 |
| `hugegraph-commons` | 52 | 共享工具和 RPC 基础测试 |
| `hugegraph-struct` | 2 | 数据结构测试较少，需要单独核对序列化契约 |

这些数字只代表文件数量，不代表测试用例数量、覆盖率、断言质量或可作为 Rust Oracle 的程度。

## 2. 已发现的测试资产

- Server：`hugegraph-test` 包含 API、Gremlin、Schema、事务、序列化、缓存、遍历和 RocksDB 单元测试；`hugegraph-core`、`hugegraph-hstore` 和 `hugegraph-dist` 另有模块测试。
- PD：包含 REST/gRPC、客户端、分区服务、配置、元数据、Raft engine readiness 和多节点 live 测试。
- Store：包含 RocksDB、snapshot、partition engine、session、Raft core 和服务测试。
- 测试配置：发现 `methods.filter`、`fast-methods.filter`、多节点 application 配置和测试套件类，需要确认 CI 实际执行范围。

CI 和模块文档显示，测试并非一个统一套件：Server 至少分为 `unit-test`、`core-test`、`api-test`，PD 有 `pd-core-test` 等 profile，Store 有 `store-core-test` 和 `store-raftcore-test`；GitHub Actions 还分别调用 unit/core/API/Raft 脚本。另一个重要事实是 PD/Store 的大量测试位于 `src/main/java`，不能只扫描标准 `src/test` 得出结论。

## 3. 当前不能据此得出的结论

目前还不能证明以下事项：

1. 所有测试都在默认 CI/profile 中执行；
2. REST、Gremlin、Cypher、权限、事务和索引的外部契约均有端到端断言；
3. Raft 日志持久化、分区、快照恢复、成员变更和滚动升级已有完整故障测试；
4. 测试断言来自独立规范或不变量，而不是只验证当前 Java 实现；
5. Java 与 Rust 可以共享同一批确定性输入并进行规范化差分比较。

## 4. 下一步盘点输出

盘点必须逐测试套件记录：模块、执行 profile、能力域、前置数据、操作序列、断言、Oracle 类型、是否可独立重放、是否注入故障，以及对应的重构契约条款。最终生成“能力—契约—测试—实现”追踪矩阵，并将未覆盖条目标记为迁移阻断项。

在矩阵完成前，不开始替换生产写路径；后续每个 Rust 小步替换都必须先补齐该边界的测试和独立 Oracle，再运行 Java 基线、Rust 实现、差分回放和集成故障测试。
