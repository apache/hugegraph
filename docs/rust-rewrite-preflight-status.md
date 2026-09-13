# Rust 重构前置条件当前审计

更新时间：2026-09-13

已具备证据：设计与不变量文档；测试入口盘点；独立 Partition 模型 Oracle（14 项 fmt/clippy/test 通过）；PD Partition 真实测试 32/32 通过；PD core/common 回归 187 项执行、2 项跳过、0 失败；Store 核心 profile 11/11 通过、全量回归已启用 suite 0 失败（归档证据）；RAT/Checkstyle/Maven 离线 validate 通过（日志已归档）。

尚未具备证据：活动 `SnapshotHandler` 的真实文件 round-trip、损坏快照与重启恢复；Store Raft 故障注入；事务并发 Oracle；Java/Rust 差分回放；实现级负向变异；性能/资源/长稳基线；双读灰度回滚；维护者和社区签核。

结论：可继续非生产 POC，整体保持 NO-GO。任何一项“未具备证据”完成后，必须保存提交版本、配置、数据集、seed、命令、日志和报告，再更新门禁。

环境复核（2026-09-13）：仓库已有 `docker/docker-compose-3pd-3store-3server.yml`，但当前 WSL 发行版未启用 Docker CLI/WSL integration（`docker: command not found`）。因此三节点故障、分区和跨进程恢复已具备运行环境；已完成一次 pd0 停止/恢复健康检查，完整故障矩阵仍待执行。

环境复核更新（2026-09-13）：已在 WSL2 Ubuntu 24.04 安装原生 Docker Engine 29.1.3 与 Compose 2.40.3，daemon active；`docker compose -f docker/docker-compose-3pd-3store-3server.yml config --services` 成功解析 `pd0..pd2`、`store0..store2`、`server0..server2`、`hubble`。此前 Docker 不可用的阻断已解除，下一步进入容器启动与多节点健康检查。

多节点镜像复核（2026-09-13）：daemon 访问 Docker Hub 的 IPv4/IPv6 均超时；本地构建也无法开始，`hugegraph-pd/Dockerfile` 的 legacy builder 未提供自动 `BUILDPLATFORM` 参数，且基础镜像尚未缓存。该环境无法继续生成容器级证据，不能将 compose 配置解析成功当作集群通过。

镜像源复核（2026-09-13）：USTC Docker Hub mirror TLS 失败，DaoCloud mirror 对 `hugegraph/pd:latest` 返回 403，阿里云公共地址需要账户专属 ACR 加速器；已恢复 daemon 默认配置，避免把不可验证的第三方镜像源固化进开发环境。

多节点故障复验（2026-09-13）：完成 store0 停止/恢复和 pd0 网络断开/重接；剩余节点在故障期间健康，恢复后全节点健康。证据分别归档于 `compose-store0-fault-recovery-20260913.log`、`compose-pd0-network-partition-20260913.log`；这仍不等价于线性一致性、数据无损或成员变更证明。

服务层复验（2026-09-13）：PD/Store 故障恢复后，三 Server 的 `/versions` 与 `/graphs` 共 6 次请求全部 HTTP 200，图列表包含 `hugegraph`；证据归档于 `compose-server-smoke-20260913.log`。这仍是健康/可达性证据，不替代事务、数据校验和线性一致性 Oracle。

Store 网络分区复验（2026-09-13）：断开并恢复 store0 网络连接；store1/store2 与三 PD 在分区期间健康，恢复后三 Store 健康。证据归档于 `compose-store0-network-partition-20260913.log`；尚未证明写入线性一致性和数据无损。

本机代理修复（2026-09-13）：将 shell 与 Docker daemon 的 `NO_PROXY/no_proxy` 固化为 `localhost,127.0.0.1,::1`，避免 `127.*` 通配写法导致本地健康请求被代理拦截；Docker 重启后集群可恢复，直接请求 PD `/v1/health` 返回 HTTP 200。

跨 Server 数据可见性复验（2026-09-13）：通过 server0 创建唯一 schema 与 vertex，从 server0/server1/server2 读取均返回相同 id、label 和 property；证据归档于 `compose-cross-server-visibility-20260913.log`。该结果证明基本复制可见性，不替代并发事务、线性一致性或故障期间写入验证。

故障期间读取复验（2026-09-13）：store0 停止期间，server1/server2 均读取到已提交 fixture；store0 恢复后 server0 仍读取到相同值。证据归档于 `compose-store0-outage-read-consistency-20260913.log`；不替代线性一致性历史检查。

可重复集群 smoke runner（2026-09-13）：`tools/rust-rewrite-cluster/smoke.sh` 退出码 0，自动完成三 PD/三 Store/三 Server 健康检查、pd0 停止恢复、store0 停止恢复及故障期间跨 Server fixture 读取；fixture `rust-gate-20260913114719`。原始日志归档于 `compose-cluster-smoke-20260913.log`。
