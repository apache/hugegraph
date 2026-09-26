活动执行合同见 [state.md](state.md)。本文件只保留 2026-09-24 的本机环境快照。

# 本机实测状态

核对时间：2026-09-24，Asia/Singapore。

## kind

- context：`kind-kind`
- 节点：`kind-control-plane`，`Ready`
- Kubernetes：`v1.37.0`
- container runtime：`containerd://2.3.4`
- 不执行全局清理，不影响任务外 namespace。

## 当前保留的 namespace

| namespace | 当前状态 | 用途 |
| --- | --- | --- |
| `hg-closure-standard-111` | PD/Server/Store 各 1 个，全部 Running | 标准 RocksDB 1+1+1 生命周期和 API 验证 |
| `hg-closure-standard-333` | PD/Server/Store 各 3 个，全部 Running | 标准 RocksDB 3+3+3 部署与 JNI 采样 |

当前镜像 tag 为 `closure-std-9abae9dbaaa1`。该 tag 只标识本地标准 RocksDB 验证镜像，不能当作最终提交 SHA 或 Topling 候选。

## 已完成

- 1+1+1 的 API 套件曾完成 155 tests、0 failures/errors、50 skips。
- 1+1+1 的 Store 同 PVC recreate 和数据复查曾完成。
- 3+3+3 的 9 个 pod 全部 Ready，PD/Store 标准 JNI hash 已采样。
- PD、Store、Commons、Struct 的模块级标准 RocksDB 回归有本地证据。

## 未完成

- 3+3+3 的 leader/follower 退出、网络分区、多数派丢失恢复和写入一致性。
- Topling 单机、1+1+1、3+3+3 的真实服务与 JNI mapping。
- PD/Store 混合 provider 组合。
- Loader 固定子集导入与邻接/重启验证。
- 任何 benchmark 结论。

## 交接注意

- 另一台机器应先 fetch `toplingdb`，核对当前 HEAD 与远端关系，再决定是否合并。
- 本机实测只写文档、测试状态和证据索引；产品代码改动仍回到开发机完成。
- 每轮测试前先固定源码 SHA 和镜像 digest，测试后把未覆盖边界写清楚。
