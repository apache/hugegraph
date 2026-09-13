# Rust 重构前置条件当前审计

更新时间：2026-09-13

已具备证据：设计与不变量文档；测试入口盘点；独立 Partition 模型 Oracle（14 项 fmt/clippy/test 通过）；PD Partition 真实测试 32/32 通过；PD core/common 回归 187 项执行、2 项跳过、0 失败；Store 核心 profile 11/11 通过、全量回归已启用 suite 0 失败（归档证据）；RAT/Checkstyle/Maven 离线 validate 通过（日志已归档）。

尚未具备证据：活动 `SnapshotHandler` 的真实文件 round-trip、损坏快照与重启恢复；Store Raft 故障注入；事务并发 Oracle；Java/Rust 差分回放；实现级负向变异；性能/资源/长稳基线；双读灰度回滚；维护者和社区签核。

结论：可继续非生产 POC，整体保持 NO-GO。任何一项“未具备证据”完成后，必须保存提交版本、配置、数据集、seed、命令、日志和报告，再更新门禁。
