# ToplingDB 跨环境 issue 索引

共同入口为 [state.md](state.md)。本文件只维护归属和依赖，不维护详细执行状态。
开发状态与证据在 [mac.md](mac.md)，验收状态与证据在 [linux.md](linux.md)。
同一项可开发完成而 Linux 待验，不能合并为一个通过标记。

## TP 依赖映射

| Issue | 开发责任 | 验收责任与依赖 |
| --- | --- | --- |
| #250 / #251 / #253 | [Mac 配置与数据隔离](mac.md#开发清单与验收)，同一配置来源问题组 | [Linux 启动验收](linux.md#验收清单)，依赖对应确定提交 |
| #254 | [Mac adapter 回归](mac.md#开发清单与验收) | [Linux TP JNI](linux.md#验收清单)，依赖 fixture 及 provider 修复 |
| #255 | [Mac CI 分类门禁](mac.md#开发清单与验收) | [Linux native diagnostic](linux.md#验收清单)，依赖分类实现 |
| #249 | [Mac WAL 失败安全](mac.md#开发清单与验收)，保留恢复能力 | [Linux 标准/TP 恢复](linux.md#验收清单)，依赖审查后的实现 |
| #212 / #248 | 必要源码线索交给 Mac，不能直接认定 native 根因 | [Linux 生命周期调查](linux.md#验收清单)，不作为本机 goal 完成条件 |
| #213 | 不在本机 goal 实现正式发布链 | [Linux 与 producer 发布核查](linux.md#验收清单)，正式发行要求 |
| #252 | 不在 Mac 执行性能测试 | [Linux 对照实验](linux.md#验收清单)，依赖相关正确性和资源条件 |

父汇总 [#240](https://github.com/hugegraph/hugegraph/issues/240)，原 [#214](https://github.com/hugegraph/hugegraph/issues/214)
仅为历史里程碑。等待一项不阻塞独立项目；Mac 完成不自动关闭 issue。

## ignore：非 TP 专属，独立跟进

| 状态 | 问题 | 已有跟进与边界 |
| --- | --- | --- |
| `ignore` | 跨 Server schema cache 一致性 | [Apache #3235](https://github.com/apache/hugegraph/issues/3235)、[PR #3237](https://github.com/apache/hugegraph/pull/3237) / [组织 PR #236](https://github.com/hugegraph/hugegraph/pull/236)，复用现有方案，不另做一套 |
| `ignore` | graph clear 顶点/边缓存、truncate 吞异常、同名图重建 | 分别沿 [#242](https://github.com/hugegraph/hugegraph/issues/242)、[#243](https://github.com/hugegraph/hugegraph/issues/243)、[#244](https://github.com/hugegraph/hugegraph/issues/244) 跟进；schema PR 不覆盖全部这些行为；[#3151](https://github.com/apache/hugegraph/issues/3151) 仅相关。保留标准 provider 复现与待归因边界 |
| `ignore` | Store 指标 session 泄漏 | [#241](https://github.com/hugegraph/hugegraph/issues/241)； [Apache PR #3081](https://github.com/apache/hugegraph/pull/3081) 已有相同生产修复；旧工作区补丁与回归见 Mac 历史证据，不随 TP 提交 |
| `ignore` | 通用 Store 停机后 JVM 不退出 | [组织 #211](https://github.com/hugegraph/hugegraph/issues/211)，标准 provider 也复现；不等同已证明全部 TP native 关闭告警的根因 |
| `ignore` | Store 地址变化后的旧连接、通用 Loader 重试 | [#245](https://github.com/hugegraph/hugegraph/issues/245)； [Apache #3124](https://github.com/apache/hugegraph/issues/3124)、[已合并 PR #3130](https://github.com/apache/hugegraph/pull/3130)；当前分支已含修复，残余扫描场景另行回归，Linux 未提交 channel refresh 不混入 TP 主线 |
| `ignore` | HStore 图级快照协议 | [#246](https://github.com/hugegraph/hugegraph/issues/246)； 通用能力缺口，本次不新增跨分区协议；[组织 PR #235](https://github.com/hugegraph/hugegraph/pull/235) 是单机 RocksDB 备份，不能冒充 HStore 图快照支持 |
| `ignore` | 完整 HA 网络分区矩阵、Compose/Helm 通用配置对齐 | [#247](https://github.com/hugegraph/hugegraph/issues/247)； 部署沿 [Apache #3131](https://github.com/apache/hugegraph/issues/3131) / [组织 PR #221](https://github.com/hugegraph/hugegraph/pull/221) 跟进；TP provider 注入、数据隔离和 JNI 选择仍在主线 |
| `ignore` | mini-cluster 临时端口竞争 | 复用 [#216](https://github.com/hugegraph/hugegraph/issues/216)，不在 TP 里做局部端口补丁 |
| `ignore` | HStore 联合索引 | 复用 [#217](https://github.com/hugegraph/hugegraph/issues/217)，已并入 committed-path 历史验证进展，no-commit 覆盖单独确认 |

以上归属沿用 2026-09-26 核对，不声称关联 issue/PR 已解决所有残余现象。后续发现由 TP 改动独有触发时重新归类并记录依据。

## 历史记录

旧复选框、P1–P7、失败与后置证据完整保存在 [历史清单](todo-history-20260926.md)，
仅在追溯具体项时读取，不再作为当前全量门禁。
