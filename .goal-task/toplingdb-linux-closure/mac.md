# Mac 开发与恢复入口

与 [state.md](state.md) 一起读取即可恢复本机工作；Linux 实测不在本文件维护。

## 基线与下一动作

本轮基线 4e1db45215d875b19250b0283e42a1484a5ce3f6，与 2026-09-27 初始化时远端 toplingdb 一致。
工作目录 `/Users/zhu/.codex/worktrees/topling-local-fixes/hugegraph-server`，detached HEAD；
主目录 `/Users/zhu/github/hugegraph-server` 在 master 且有无关未跟踪文件，不在那里实施。
远端 hugegraph 指向 https://github.com/hugegraph/hugegraph.git；推送目标 HEAD:toplingdb。
恢复时核查路径、HEAD、工作区和远端，以上不代表未来时刻状态。

本次只做文档初始化，没有代码补丁或本轮功能测试。后续明确启动 goal 后，先复现配置分叉，
统一处理 #250/#251/#253；独立推进 #254/#255；#249 单独做恢复失败设计与故障注入。
不等待 Linux 未提交 diff，不宣称已取得该实现；可基于已提交源码独立修复，但不覆盖或提交 Linux 脏工作区。

## 开发清单与验收

以下均未开始实施；表中为用户确认的目标行为，不是当前已经实现的行为。

| Issue | 设计决定与本地验收 | 状态 |
| --- | --- | --- |
| [#250](https://github.com/hugegraph/hugegraph/issues/250) | 图配置为准，TOPLINGDB_ROCKSDB_PROVIDER 冲突报错；覆盖双向冲突、缺省、多图，保持默认 RocksDB 与直接/容器启动一致 | 待开发 |
| [#251](https://github.com/hugegraph/hugegraph/issues/251) | 从有效 REST 配置读取 graphs，与 Java 工作目录语义一致；覆盖默认、相对/绝对、空格、目录不存在及混合 provider | 待开发 |
| [#253](https://github.com/hugegraph/hugegraph/issues/253) | 保留 Docker 默认图配置生成；任何数据库打开前检查所有实际图 data/WAL 根；覆盖额外图、标记冲突、原数据不变，不支持的解析明确拒绝 | 待开发 |
| [#254](https://github.com/hugegraph/hugegraph/issues/254) | 实际 adapter truncate 覆盖空表、单 key、乱序多 key、二进制边界、多 CF、清空后读写；分别断言两种 provider 预期分支 | 待开发 |
| [#255](https://github.com/hugegraph/hugegraph/issues/255) | 仅豁免明确已知 CF 断言且前置探测成功的诊断；缺配置、JNI 加载、映射、读写和未知错误均失败；保留日志和 JNI 身份 | 待开发 |
| [#249](https://github.com/hugegraph/hugegraph/issues/249) | 保留独立/嵌套 WAL 恢复，先设计恢复失败保护；覆盖 rename/copy/发布失败、同名旧 WAL、短复制/同长损坏、符号链接及失败后重启，不丢唯一恢复源、不重放旧 WAL、不伪报成功 | 待设计与开发 |

#249 已提交源码的风险是 data 替换先于独立 WAL 处理，移动失败可能保留活动旧日志。
不能仅增加重试或直接回退恢复扩展；恢复事务、回滚或启动保护的具体实现需依据源码及故障回归设计，
其共同硬约束为失败后不能继续打开不一致状态，且恢复源仍可用。
#254 当前是覆盖不足，不是已证明 truncate 损坏。

## 最小验证与审查

- shell 使用隔离临时目录、假 runtime 和 init 截断 fixture；记录这不能证明真实 native 正确性。
- 优先复用 test-topling-runtime-selection.sh、test-topling-docker-entrypoints.sh 和现有 RocksDBSessionsTest，
  补能在旧实现失败的回归；实际 adapter 路径与标准 JNI 小测试在本机执行。
- 从仓库 POM 与模块 AGENTS.md 确定定向 Java 命令；依赖缺失先修构建路径，不把未执行说成通过。
  推送代码前执行仓库规定的 mvn editorconfig:format、mvn clean compile -Dmaven.javadoc.skip=true 及相关测试；
  检查格式化没有夹带无关变化。文档阶段不用这些重检查。
- 配置与数据隔离、诊断门禁、WAL 等主要阶段按共同合同接受 3 人独立审查和必要复审。
  缺少审查或测试仍保留未完成状态；编写者自查不替代独立审查。

## 交给 Linux 的记录格式

每项开发完成时填写：issue、确定提交、行为变化、验证命令与退出码、断言结果/skip、审查结论、
剩余边界、Linux 待验场景和期望。由 Linux 在 linux.md 接收并记录实际结果，Mac 不填写验收通过。
当前没有本轮代码交付提交；六项的 Linux 场景已经在 linux.md 列出。

## 历史开发证据

- 4a852e2ea 同步 master 2f827d6e8；旧会话记录格式、编译及 18 项 Raft 定向测试通过。
  这不是本轮重跑，也不是当前全部代码的验证。
- 旧 Mac 的通用指标 session 补丁及 3 项回归保留在旧工作区；#241 已有 Apache PR #3081 相同生产修复，
  不搬入本次 TP 范围。曾经历依赖解析失败，后用 reactor 实跑成功。
- #250/#251/#253 历史 shell fixture 只证明选择或调用覆盖缺口，没有加载真实 Topling，也没有证明数据损坏。
- 完整来源及通用问题诊断见 [历史开发交接](development-handoff-history-20260926.md)。

## 文档初始化交付

2026-09-27：按双环境职责拆分，历史原文保存，goal 尚未启动。
四份档案与基线原文逐字节一致；59 个活动相对链接及锚点、git diff --check 通过。
一名独立只读审查者检查完整迁移，无阻塞发现；Linux 未实时核实的限制已明确保留。
本轮不声明任何功能修复完成，不运行功能测试。文档交付提交可由本节所在提交追溯。
