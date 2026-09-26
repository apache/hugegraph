# Linux 验收与恢复入口

与 [state.md](state.md) 一起读取；先确定要验收的 Mac 交付提交，再按需查看证据，不全量重跑历史清单。
本文件中的历史环境与结果来自已提交记录，本次没有连接 Linux 核实，也没有启动实验。

## 环境和恢复边界

- 历史工作区 `/home/soc-baidu/.codex/worktrees/f29e/hugegraph`，本地分支 codex/toplingdb-linux-validation，
  远端 org/toplingdb。旧主 checkout `/home/soc-baidu/github/hugegraph` 不作为恢复依据。
- 恢复先核对主机、工作区、HEAD、远端 URL 与未提交文件。Mac 代码经推送后再整合，禁止 force-push 或覆盖本地改动。
- 历史未提交 channel refresh 及 WAL 文件有审查未收口记录。channel refresh 属通用问题，维持独立跟进；
  WAL 当前由 Mac 基于已提交版本重新设计，不自动复用或提交 Linux 旧补丁，不自动重开旧补丁审查轮次。
- Kubernetes 历史配置为 /home/soc-baidu/.kube/config、kind-kind。只在确认资源归属后操作，
  不原位升级历史 namespace、不清理无关资源；单节点 kind 不证明物理多机或真实网络分区。
- 同一时间只运行一个重任务；Maven 全量、镜像构建、部署和故障注入不叠加。
  不提交 evidence/、数据库、镜像、原始大 JSON、数据集或凭据。

## 验收清单

当前没有本轮 Mac 修复提交可验收，以下均为待交付/待验证，不因历史成功勾选。

| 项目 | 验收场景与预期 | 状态 |
| --- | --- | --- |
| #250/#251/#253 | 直接及容器启动，默认/自定义目录和额外图；实际 JNI 与 Java provider 一致；冲突在数据库打开前失败，原数据不变 | 等待 Mac 提交 |
| #254 | 用真实 TP JNI 经 adapter 执行多 key truncate，旧数据全空、CF 保留、可重新读写并关闭；标准 provider 对照自身预期分支 | 等待 Mac 提交 |
| #255 | 真正运行 runtime diagnostic，检查前置探测、错误分类、原始日志和 JNI 身份；仅已知断言得到例外，其他错误阻塞 | 等待 Mac 提交 |
| #249 | 标准/TP 确定提交分别验证 snapshot 成功与故障恢复，包含独立/嵌套 WAL、失败后重启及源文件校验 | 等待 Mac 提交 |
| #212 | 核对真实 DB/CF 和服务生命周期的残余关闭告警，区分已知合成断言、正常关库和卡住 worker | 待归因，已有历史成功 |
| #248 | 复查 clear 后首次 Server 重启丢可见性的单次线索，固定确认写入及查询证据；第二次成功不覆盖第一次异常 | 待定位 |
| #213 | 核实不可变 JNI 坐标、源码/工具链、CPU 基线、校验和及许可/发布链 | 正式发布要求未完成 |
| #252 | 相关正确性与资源条件满足后，同源码、workload、资源和配置做标准/TP 至少三轮性能对照，保留原始结果及统计 | 未开始，仅 Linux |

多节点和部署验收在 Linux 阶段按实际拓扑记录；通用 HA 建设、图级快照或完整平台矩阵不作为所有 TP 项统一前提。
下一动作：接收 Mac 确定提交并核对环境；独立调查已有 #212/#248/#213 时可继续，不必等待其他代码项。
本次文档初始化并不调度这些实验。

## 每次验收的证据

记录 issue、源码 SHA/是否干净、构建输入、镜像 digest 和 revision、实际 JNI 路径及 hash、
配置、拓扑/namespace、命令、退出码、计数与 skip、前后数据断言、未覆盖边界和原始证据位置。
镜像标签、Pod Ready 或重试后成功不能单独代表通过；失败项目保留失败证据和解除条件。
Mac 完成与 Linux 完成分别记录，不用此文件的待验状态阻止独立 Mac 工作。

## 历史结果摘要

- #212 已有 938e4b9c4、7afe25947 生命周期修复，以及旧 SHA 三轮真实验证；不能说从未验证。
- e109012a0 已修复 memtable_as_log_index 配置兼容；a35ebeb17 标准/TP standalone snapshot 成功路径
  曾证明快照前数据保留、快照后消失。它们不证明本轮 #249 的失败安全。
- a35 单机、1+1+1、3+3+3 有 CRUD、重启、JNI、固定子集数据等局部证据；仍有 db not closed 告警。
  PD 曾升级到 327737f16，Server/Store 仍可能是 a35，必须逐组件核对，不能写成同一整体版本。
- clear 后一次首次 Server 重启查询为空沿 #248；旧 Store IP 残余连接沿通用 #245，后续读到数据不证明根因修复。
- HStore snapshot_create 曾返回 500，不等同 standalone snapshot；网络分区未完成，benchmark 未开始。
- 全量 LAW 曾因数据规模和共享验证存储后置；固定子集 1000000 顶点、2098771 边的成功不代表全量或失败重试覆盖。
- 旧 WAL 20 项通过只是未提交版本的局部测试，最新差异未完成审查，不可当作交付依据。

原始证据逻辑位置见 [证据索引](evidence-index.md)；完整提交、镜像身份和时间线见
[历史日志](state-history-20260926.md)，历史勾选及终态见 [历史清单](todo-history-20260926.md)，
旧环境快照见 [历史环境](local-status-history-20260926.md)。仅在调查具体问题时读取。
