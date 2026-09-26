# Linux 历史证据索引

本文件只记录本地证据的逻辑位置和用途，不提交原始日志、大 JSON、镜像或数据集。原始证据保存在原 Linux 主机忽略的 `.goal-task/toplingdb-linux-closure/evidence/`，Mac 本次未读取原始文件；PR 中只保留本索引。

当前验收入口为 [linux.md](linux.md)。下表均为历史结果，未绑定完整 SHA 的项必须先复核来源。

| 逻辑证据 | 用途 | 历史结论 |
| --- | --- | --- |
| `repaired-full-core-summary.json` | standalone 标准 RocksDB CoreTestSuite | 818 tests，0 failures/errors，42 skips |
| `repaired-full-api-summary.json` | standalone 标准 RocksDB API suite | 161 tests，0 failures/errors，14 skips |
| `helm-standard-111-full-api-summary.json` | Helm 1+1+1 标准 RocksDB API | 155 tests，0 failures/errors，50 skips |
| `helm-standard-333-runtime.json` | Helm 3+3+3 pod 与 JNI 采样 | 9 pods Ready；HA 未验证 |
| `pd-common-core-standard-summary.json` | PD common/core | 83/104，0 failures/errors |
| `pd-client-rest-standard-summary.json` | PD client/rest 与服务退出 | 83/22，0 failures/errors |
| `commons-struct-current-summary.json` | Commons/RPC/Struct | 351/24/8，0 failures/errors |
| `helm-standard-333-store-majority.json` | 历史标准 3+3+3 两个 Store Pod 同时删除 | 已提交数据保持 200；期间写入超时且最终 404；恢复后写入成功 |
| `helm-standard-333-store-leader.json` | 历史标准 3+3+3 Store leader Pod 恢复 | 旧写入保持 200；删除瞬间新写入失败；恢复后写入成功 |
| `helm-standard-333-server-replica.json` | 历史标准 3+3+3 Server 副本切换 | 删除一个 Server 后其余副本继续读写；新 Pod 可读 |
| `helm-standard-333-store-pod-restart.json` | 历史标准 3+3+3 Store-2 Pod 恢复 | 142.869 秒 Ready；PVC 不变；恢复前后读取 200 |
| `helm-standard-333-auth-function.json` | 历史标准 3+3+3 认证和边一致性 | 错误口令 401；三 Server 读边 200；图空间 auth=false |
| `helm-standard-333-write-consistency.json` | 历史标准 3+3+3 三 Server 写入一致性 | Server-0 写 201，三个 Server 读 200；未认证 401 |
| `helm-standard-111-lifecycle.json` | 历史标准 1+1+1 删图、truncate、新卷恢复 | 删图和 truncate 通过；新卷恢复后已确认顶点 404 |
| `helm-standard-111-stop-restart-confirmed.json` | 历史标准 1+1+1 停止/重启和已确认写入 | 写 201，重启前后读 200；PVC 保持；JNI 为标准 RocksDB |
| `store-standard-audit-summary.json` | Store RocksDB/client/core audit | 3/50/22，0 failures/errors |

## 证据使用规则

- 历史证据可能没有在文件内部嵌入 commit SHA；使用前先核对对应 build input、日志和源 hash。
- 不得把上述标准 RocksDB 结果写成 Topling、HStore HA 或 benchmark 结果。
- 后续每轮新增证据时，优先写一个小的 summary JSON 或 Markdown，并在文档中记录 commit、镜像 digest、JNI hash、命令和边界。
