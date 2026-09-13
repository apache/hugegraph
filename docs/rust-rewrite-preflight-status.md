# Rust 重构前置条件当前审计

更新时间：2026-09-13

已具备证据：设计与不变量文档；测试入口盘点；独立 Partition 模型 Oracle（14 项 fmt/clippy/test 通过）；PD Partition 真实测试 32/32 通过；PD core/common 回归 187 项执行、2 项跳过、0 失败；Store 核心 profile 11/11 通过、全量回归已启用 suite 0 失败（归档证据）；RAT/Checkstyle/Maven 离线 validate 通过（日志已归档）。

尚未具备证据：活动 `SnapshotHandler` 的真实文件 round-trip、损坏快照与重启恢复；Store Raft 故障注入；事务并发 Oracle；Java/Rust 差分回放；实现级负向变异；性能/资源/长稳基线；双读灰度回滚；维护者和社区签核。

结论：可继续非生产 POC，整体保持 NO-GO。任何一项“未具备证据”完成后，必须保存提交版本、配置、数据集、seed、命令、日志和报告，再更新门禁。

环境复核（2026-09-13）：仓库已有 `docker/docker-compose-3pd-3store-3server.yml`，但当前 WSL 发行版未启用 Docker CLI/WSL integration（`docker: command not found`）。因此三节点故障、分区和跨进程恢复已具备运行环境；已完成一次 pd0 停止/恢复健康检查，完整故障矩阵仍待执行。

环境复核更新（2026-09-13）：已在 WSL2 Ubuntu 24.04 安装原生 Docker Engine 29.1.3 与 Compose 2.40.3，daemon active；`docker compose -f docker/docker-compose-3pd-3store-3server.yml config --services` 成功解析 `pd0..pd2`、`store0..store2`、`server0..server2`、`hubble`。此前 Docker 不可用的阻断已解除，下一步进入容器启动与多节点健康检查。

多节点镜像复核（2026-09-13）：daemon 访问 Docker Hub 的 IPv4/IPv6 均超时；本地构建也无法开始，`hugegraph-pd/Dockerfile` 的 legacy builder 未提供自动 `BUILDPLATFORM` 参数，且基础镜像尚未缓存。该环境无法继续生成容器级证据，不能将 compose 配置解析成功当作集群通过。

镜像源复核（2026-09-13）：USTC Docker Hub mirror TLS 失败，DaoCloud mirror 对 `hugegraph/pd:latest` 返回 403，阿里云公共地址需要账户专属 ACR 加速器；已恢复 daemon 默认配置，避免把不可验证的第三方镜像源固化进开发环境。
