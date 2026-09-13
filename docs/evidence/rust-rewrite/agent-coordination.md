# Rust 重写后台 agent 协作约束

## 任务定位

Rust 是独立重写实现，不是 Java 逐行翻译。Java 代码保留为可切换参考后端，可运行 Java、Rust 或双实现模式。双实现只比较契约规定的兼容边界、持久化安全和错误语义；Rust 可以在不破坏既有承诺时改进性能、并发、资源占用和稳定性。

## 验收顺序

先补齐本地端到端集成测试、契约矩阵和独立 Oracle，再逐个契约边界实现 Rust。每个边界必须有正常、边界、错误、故障和恢复场景。Oracle 优先使用公开规范、数据不变量和独立模型；Java 只能作为差异发现参考，不能单独作为真值。

完备性要求：契约覆盖率、核心场景覆盖率、独立 Oracle 覆盖率和关键契约变异杀伤率均为 100%；未解释差异和未执行测试为 0。代码覆盖率只能辅助诊断，不能替代契约覆盖。

## 测试去重与证据

执行测试前读取 `test-history.md`。若代码、配置、测试、数据集和契约均未变化，禁止重复已通过命令；失败测试修复后必须重跑。每次执行记录精确命令、commit、变更路径、结果、seed 和 evidence 路径。不要修改契约或降低门槛来消除失败。

## 本地 Java/Groovy 环境

Groovy 2.5/TinkerPop 3.5.1 不能解析 Java 21 class major 65。优先使用 Java 17 toolchain 运行本地 E2E；Java 17 包下载使用本机 HTTP 代理 `http://127.0.0.1:10809`。`wget` 已验证可用，APT 必须显式传入：

```bash
apt-get download -o Acquire::http::Proxy=http://127.0.0.1:10809 -o Acquire::https::Proxy=http://127.0.0.1:10809 openjdk-17-jdk-headless
```

下载文件必须检查大小和 `dpkg-deb --info`；不完整包不得解包或作为证据。网络失败使用有限重试并记录原因，不能无限下载。

## 协作安全

后台循环读取本文件、`progress.md` 和 `test-history.md`。只处理 `progress.md` 的 `current_stage/next_action`，不得覆盖他人未提交修改。交互式 agent 工作时使用暂停文件 `/tmp/hugegraph-rust-rewrite-codex.pause`。认证、只读文件系统、网络不可达或连续无进展时停止，不创建无状态的新会话。
