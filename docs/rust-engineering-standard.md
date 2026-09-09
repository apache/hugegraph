# HugeGraph Rust 工程规范

状态：审核版；Rust 子项目建立后应转为仓库级强制规范。

本规范参考 TiKV、raft-rs 和 Tokio 的公开实践。TiKV 将格式化、Clippy、静态检查和多配置测试集中到开发门禁，并维护工具链、格式、Clippy 和依赖审计配置；raft-rs 对共识代码采用更严格的评审并要求 Clippy/rustfmt【https://github.com/tikv/tikv/blob/master/Makefile】【https://github.com/tikv/raft-rs/blob/master/CONTRIBUTING.md】。Tokio 明确 MSRV、语义化版本和长期支持策略，并使用 Loom 做并发排列测试、Miri 做未定义行为检查【https://github.com/tokio-rs/tokio/blob/master/CONTRIBUTING.md】【https://github.com/tokio-rs/tokio/blob/master/docs/contributing/pull-requests.md】。

## 1. 工具链与目录

- 使用仓库锁定的 `rust-toolchain.toml`，明确 Rust 版本和组件（`rustfmt`、`clippy`）。
- 设置并持续验证 MSRV（Minimum Supported Rust Version）；升级 MSRV 必须记录兼容性影响。
- 使用 Cargo workspace，统一依赖版本、feature 和 `Cargo.lock`；服务和库不得各自漂移版本。
- 重大架构或公共 API 变更必须先提交 RFC；版本策略遵循 SemVer，明确 MSRV、弃用周期和回滚方案。
- 目录按领域边界划分，例如 `api`、`domain`、`storage`、`raft`、`transport`、`runtime`，禁止通过循环依赖拼接模块。

## 2. 强制质量门禁

提交和 CI 至少执行：

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo audit
cargo deny check
```

发布构建还要执行依赖许可证检查、可重复构建检查、集成测试、故障测试和基准回归。禁止用 `#[allow(...)]` 静默绕过 Clippy；例外必须有范围、理由和责任人。

并发原语、无锁结构和调度器相关代码必须提供 Loom 测试；涉及指针、FFI 或复杂内存模型的代码应在 nightly 环境增加 Miri 检查。Loom/Miri 是补充验证，不得替代正常集成测试。

## 3. API、错误和异步规范

- 公共库 API 使用明确的类型和生命周期；避免用 `String` 表示可枚举的协议状态。
- 库错误使用 `thiserror`，服务边界使用可分类、可观测的错误码；`anyhow` 只允许出现在应用层。
- 不跨线程传递未说明所有权的对象；所有后台任务必须具备取消、超时、退出和 join 策略。
- Tokio 任务中禁止执行未隔离的阻塞磁盘或 CPU 工作；使用专门线程池并记录队列等待时间。
- 不在持锁期间执行 RPC、磁盘 IO 或等待异步任务；锁的范围和顺序必须文档化。
- 时间、重试、退避、超时和取消都必须显式配置，禁止无限重试。

## 4. `unsafe`、FFI 与数据边界

- 默认禁止 `unsafe`；每处 `unsafe` 必须有安全不变量、最小封装模块、审查记录和测试。
- C/C++ FFI 只允许通过窄 C ABI；所有指针、长度、线程归属、释放函数、异常和 ABI 版本必须明确。
- Rust panic 不得跨 FFI；C++ 异常不得跨边界传播。
- Protobuf、JSON、RocksDB value 和快照输入都必须先做长度、版本和范围校验。
- 序列化格式必须有版本号；禁止依赖 Rust 内存布局或 `repr` 缺省行为作为持久化格式。

## 5. 并发、Raft 与持久化

- Raft 核心只复用成熟库；HugeGraph 负责适配层、日志落盘、状态机、快照和恢复测试。
- 提交顺序必须明确记录：持久化日志、提交索引、状态机应用和响应客户端的先后关系。
- 所有状态机命令必须可重放；副作用必须通过幂等键或提交索引去重。
- 快照生成、安装、日志截断和恢复必须具备崩溃点测试。
- RocksDB 版本、编译选项、列族和 key 编码固定并纳入兼容性测试。

## 6. 可观测性与安全

- 使用 `tracing`，日志字段统一包含请求 ID、图名、分区、节点、Raft group 和 trace ID（适用时）。
- 指标名称、单位和标签数量受控；禁止把用户输入直接作为高基数标签。
- 密钥不得进入日志、错误文本、快照或 metrics；敏感配置使用专门的 secret provider。
- 依赖变更必须经过漏洞、许可证和来源审查；生产构建锁定校验和并生成 SBOM。

## 7. 测试规范

- 单元测试验证局部不变量；集成测试验证协议和模块边界；端到端测试验证用户可观察行为。
- 测试不得只复制实现逻辑作为断言，关键契约使用规范 Oracle、关系 Oracle 或独立参考模型。
- 随机和故障测试必须记录 seed、版本、配置和最小化后的操作历史。
- 性能测试固定数据集、并发、硬件和采样方式；基准变化必须有解释。
- 每个 bug 修复都要增加确定性回归用例。

## 8. 评审清单

代码评审至少检查：契约是否改变、错误是否可分类、异步任务是否可退出、锁和 IO 是否安全、`unsafe`/FFI 是否有不变量、持久化是否可升级、指标和日志是否足够、测试是否独立于实现，以及是否保留回滚路径。

## 9. 从 TiKV/Tokio 借鉴的流程要求

- 大变更先 RFC，小 PR 分步合入；每个 PR 说明行为变化、测试和回滚方式。
- CI 使用与仓库锁定的编译器/Clippy 版本一致的工具链，避免开发机版本差异造成误报。
- 测试覆盖 feature 组合，而不是只测试默认 feature；发布 feature 必须有独立构建和测试。
- 公共 API 生成文档并对文档构建启用 warnings-as-errors；复杂代码按模块、类型、函数写 doc comment。
- 并发代码同时保留常规测试、Loom 小模型测试和长稳/故障测试，记录已验证的状态空间边界。
- 贡献流程要求代码审查、变更日志、许可证/DCO（若项目采用）和安全问题单独处理。
