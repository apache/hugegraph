# 本地测试历史

每次执行测试前检查本文件。若代码、配置、数据集或测试本身没有变化，不重复执行已通过命令。

| command | commit | result | evidence |
|---|---|---|---|
| `bash -n tools/run-codex-loop.sh` | `c2463296d` | PASS | `continuation-script-fix-20260913T112316Z.log` |
| `python3 tools/raft-linearizability/test_capture_history.py` | `c2463296d` | PASS | `continuation-script-fix-20260913T112316Z.log` |
| `python3 tools/raft-linearizability/test_checker.py` | `c2463296d` | PASS | `continuation-script-fix-20260913T112316Z.log` |
