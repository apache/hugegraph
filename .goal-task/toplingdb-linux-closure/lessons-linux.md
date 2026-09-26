# Linux 验收经验

[通用经验](lessons.md) · [验收入口](linux.md)

| 触发条件 | 做法与边界 | 依据 |
| --- | --- | --- |
| 单节点 kind 的 Pod 恢复与 Ready | 核对已确认数据和实际拓扑；不得表述为物理多机或网络分区容错 | [历史终态](todo-history-20260926.md#终态但未勾选) |
| PD 升级但 Server/Store 保持旧镜像 | 逐组件记录 revision、digest 和 JNI；标签或某个组件升级不能代表全栈源码一致 | [PD 327737f16 历史记录](state-history-20260926.md) |
| Store IP 变化后请求最终恢复 | 同时保留旧 IP 重连失败与后来成功；成功本身不能证明原因已修复或必须靠重启 | [Loader 历史记录](todo-history-20260926.md#p6-loader) |
| 停机仍有 db not closed | 区分正常关库证据、残余 native 告警与卡住 worker 超时；不强制关库制造通过 | [关闭边界证据](todo-history-20260926.md#终态但未勾选) |
