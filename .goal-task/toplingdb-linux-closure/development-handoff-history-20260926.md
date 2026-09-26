# 2026-09-26 开发交接

接手基线 `6790d53bf`，已于本轮同步 master 至 `4a852e2ea`，PR #179。本文件记录代码诊断与复测条件；
分项验收状态仍只在 [todo.md](todo.md)，执行分工见 [state.md](state.md)。

## 2026-09-26 范围收敛

按用户最新确认，TP 主线聚焦适配、TP 相关 bugfix、功能、易用性、架构切换和可维护性。
非 TP 专属问题统一退出本任务，在 [todo.md](todo.md) 标记 `ignore` 并关联独立 PR/issue。
已建立 [Summary #240](https://github.com/hugegraph/hugegraph/issues/240)，21 个原生子 issue：15 个新增、6 个复用。
以下通用问题只保留诊断、补丁与测试证据，不再作为“下一批 TP 修复”或整体合入门禁。

schema 一致性复用 Apache #3235 / PR #3237（组织 PR #236）；指标 session 修复已存在于
Apache PR #3081；旧连接恢复沿 Apache #3124 / 已合并 PR #3130 跟进。
clear/drop 的残余缓存问题、truncate 异常传播和通用 HStore 图快照继续独立归档。
这些链接及覆盖边界以 todo.md 的归属表为准；本轮已建立缺失条目，没有创建修复 PR。
本地通用指标源码和回归暂时保留，不随 TP 文档提交，也不因范围调整删除。

本 PR 新增的 WAL 恢复补丁等共用代码仍需对自身回归负责；未知 native 问题先归因。
后续 TP 架构检查以[开发指南](../../docs/toplingdb/toplingdb-development.md)为准。

## TP 专项审查与 issue 更新

审查源码基线 `4a852e2ea`。六个只读分工覆盖整体架构、核心生命周期、launcher、打包/部署、测试和对抗复核。
最终确认并归档如下，没有修改生产代码：

| Issue | 确认范围 | 验证边界 |
| --- | --- | --- |
| [#250](https://github.com/hugegraph/hugegraph/issues/250) | 环境 override 选 TP runtime，Java provider 仍为 rocksdb | fake runtime shell fixture 复现；未运行 native truncate |
| [#251](https://github.com/hugegraph/hugegraph/issues/251) | selector 忽略实际自定义 graphs 目录 | fake runtime fixture 复现选择错位；未启动服务 |
| [#253](https://github.com/hugegraph/hugegraph/issues/253) | 容器只将默认数据根交给 marker 校验，遗漏额外图 | 真实 entrypoint + spy helper 验证调用覆盖；初始化前截断，未打开数据 |
| [#254](https://github.com/hugegraph/hugegraph/issues/254) | clearTables 单测每 CF 单 key，跳过范围删除 | 源码覆盖核对；不否认 #212/#214 的历史真实生命周期验证 |
| [#255](https://github.com/hugegraph/hugegraph/issues/255) | native diagnostic 所有失败都非阻塞并报告为 #212 | workflow 条件核对；不声称其他 required checks 也检测不到错误 |

#212 现正文保留 `938e4b9c4` / `7afe25947` 的修复、旧镜像三轮通过及近期残余关闭告警。
#213 更新为 producer `e819a6df` 核对结果：候选 JNI 可用，正式来源/发布链仍待完成。
#214 改为历史里程碑，保留旧源码四镜像发布 run，monitor 默认值更正为 false。
#217 已并入 committed-path 后续通过记录，保留 no-commit 待确认边界。

原 issue 正文与评论已备份到开发机 `/Users/zhu/.codex/artifacts/topling-20260926-issue-refresh/`。
有效提交、失败 run、历史通过与未验证边界迁入公开正文后，清理了 11 条被替代的本人旧进度评论。
未删除未解决的外部讨论，也未把仍有缺口的 issue 关闭。

## master 同步已推送

合并提交 `4a852e2ea` 的父提交为 `6790d53bf` 与 master `2f827d6e8`。
7 处显式冲突已处理，同时消除了自动合并引入的 Iterator 重复方法和快照测试变量重名。
采用上游 JRaft 属性划分，保留 Topling 分支的 `1.3.14`，同步 LICENSE 与依赖清单。

`editorconfig:format`、离线 `clean compile`、effective POM 检查通过；
PD `RaftStateMachineTest` 4 项、Store `PartitionStateMachineTest` 10 项与
`PartitionEngineErrorTest` 4 项全部通过。没有启动集群，也没有运行真实 compaction/snapshot 集成测试。
三名独立审查无阻塞发现。远端 head 已核对，GitHub 返回 `MERGEABLE`，CI 尚在运行。
以下指标修复保留为独立本地补丁，没有混入合并提交。

## 通用问题历史补丁：Store 指标采集泄漏 session

`SystemMetricService.loadRocksDbInfo()` 每轮通过 `queryGraphDB()` 获取 session，
旧实现没有释放。工厂返回的是增加共享引用计数的 clone；
`releaseGraphDB()` 从注册表移除数据库后只释放工厂持有的引用。
指标采集泄漏的引用会阻止实际关库，后续 `releaseAllGraphDB()` 也无法遍历到它。

修复改用 try-with-resources，正常与异常路径均释放本次借用；
数据库在枚举后被移除、查询返回 null 时跳过。没有修改停机等待、强制关库或数据清理逻辑。
这确认了一条 Java 引用泄漏路径，尚不能断言解释了全部 Topling `db not closed`。

### 本机验证边界

- 三名独立只读审查者已审完整补丁，无阻塞发现；审查不替代 Java 编译或运行验证。
- `test-topling-runtime-selection.sh` 已退出 0；使用假 JVM/库文件验证 provider 与 classpath 选择，未加载 Topling。
- 首轮单模块定向离线编译 `hg-store-core` 在依赖解析阶段失败，尚未进入 Java 编译。
  本地缺少 `hg-store-rocksdb`、`hg-pd-core` 产物，已安装 POM 的 `${revision}` 父坐标也无法解析。
- master 同步后使用包含上游模块的 reactor 解析依赖，`SystemMetricServiceTest` 已实跑：
  3 项通过，0 失败、0 错误、0 跳过，JUnit 用时 1.144 秒。没有启动集群或运行性能测试。
  未完成旧实现的反向运行验证，不能把通过结果表述为 red/green 均已完成。
- POM XML、交接文档相对链接和 `git diff --check` 通过。补丁尚未提交或推送。

### 独立 Store 任务的复测参考（不安排在 TP 主线）

1. 在依赖已齐全的构建环境运行 `SystemMetricServiceTest`，覆盖重复采集、统计读取异常和查询返回 null
   （模拟枚举后数据库被移除，不是真实并发交错测试）。该测试加载标准 JNI，但不开数据库或服务。
   macOS 修复后 3 项已通过；Linux 复核时记录其源码版本，修复前生产源码的释放断言仍需反向验证。
2. 用通过审查和构建的确定提交生成 Store 镜像，记录 revision、镜像 digest 和实际 JNI。
3. 在标准 RocksDB 和 Topling 上分别做少量指标采集，确认没有累积 session 引用。
4. 按 Linux 现有实验门禁安排正常停机，核对分区数据库实际关闭以及 `db not closed`；
   验证先前确认写入的数据在恢复后仍可读。不要用强制关库掩盖仍活跃的 worker。

这些是后续验收步骤，本轮没有在 Linux 执行，也没有改变历史故障注入限制。

## 通用问题诊断备忘：多 Server 图生命周期

- **已确认通知缺口**：`truncateBackend()` 只发进程内 `STORE_TRUNCATE`。
  `MetaManager.notifyGraphClear()` 没有调用者，但 `GraphManager.graphClearHandler()`
  已注册跨 Server 接收器。这解释了其他 Server 缓存不会收到清空通知的路径。
- **需一起处理的失败语义**：`HstoreStore.truncate()` 捕获异常后只记日志，调用方可能误判成功。
  通知应在实际 truncate 成功后发送；异步通知没有全副本完成应答，不能宣称 HTTP 204 即所有缓存均已清空。
- **待复现**：同名图 drop/recreate 复用缓存，以及 off-heap 缓存持有旧 graph 引用，
  可能关联 `graph closed`；目前只有源码线索，不能写成已确认根因。

channel refresh 和 WAL 沿用 Linux 未提交工作的边界，本轮没有改动。
