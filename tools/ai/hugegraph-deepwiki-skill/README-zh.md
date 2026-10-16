# HugeGraph 仓库知识助手

[English](README.md) | [中文](README-zh.md)

这个独立模块将 [Apache HugeGraph](https://github.com/apache/hugegraph) 源码仓库问答能力打包为 Claude Code 和 Codex 可安装的 skill。

DeepWiki 是底层线上知识库和 MCP 传输通道：

```text
https://deepwiki.com/apache/hugegraph
https://mcp.deepwiki.com/mcp
```

## 功能

- 回答 HugeGraph 架构、模块、API、存储后端、schema、traversal、配置、构建、测试和实现细节相关问题。
- 使用 `read_wiki_contents` 构建本地 DeepWiki wiki 缓存，并优先搜索缓存。
- 当缓存内容不能直接、精准回答问题时，使用 `ask_question` 获取线上答案。
- 普通问答不会 clone 上游源码仓库。

## 前置要求

- Python 3.9 或更高版本，用于运行随附的 MCP 客户端脚本。
- 当前环境需要能访问 `https://mcp.deepwiki.com/mcp`。

## 目录结构

```text
tools/ai/hugegraph-deepwiki-skill/
├── README.md
├── README-zh.md
├── .agents/plugins/marketplace.json
├── .claude-plugin/marketplace.json
└── plugins/hugegraph-deepwiki-skill/
    ├── .claude-plugin/plugin.json
    ├── .codex-plugin/plugin.json
    └── skills/hugegraph-deepwiki-skill/
        ├── SKILL.md
        ├── agents/openai.yaml
        ├── references/repos.json
        └── scripts/deepwiki_mcp.py
```

## Claude Code 安装

从当前仓库安装：

```bash
cd tools/ai/hugegraph-deepwiki-skill
claude plugin marketplace add "$(pwd)"
claude plugin install hugegraph-deepwiki-skill@hugegraph-deepwiki-skill
```

从已发布分支安装时，先 clone 仓库，再从本地模块路径安装：

```bash
git clone -b <branch> https://github.com/<owner>/hugegraph.git
cd hugegraph/tools/ai/hugegraph-deepwiki-skill
claude plugin marketplace add "$(pwd)"
claude plugin install hugegraph-deepwiki-skill@hugegraph-deepwiki-skill
```

手动安装用户级 skill：

```bash
mkdir -p ~/.claude/skills
cp -R plugins/hugegraph-deepwiki-skill/skills/hugegraph-deepwiki-skill ~/.claude/skills/
```

### 让 Claude Code 自动安装

在 HugeGraph 仓库根目录的 Claude Code 里粘贴：

```text
Install the HugeGraph repository assistant from this checkout. Enter `tools/ai/hugegraph-deepwiki-skill`, run `claude plugin marketplace add "$(pwd)"`, then run `claude plugin install hugegraph-deepwiki-skill@hugegraph-deepwiki-skill`. Do not hardcode absolute paths.
```

## Codex 安装

从当前仓库安装：

```bash
cd tools/ai/hugegraph-deepwiki-skill
codex plugin marketplace add "$(pwd)"
codex plugin add hugegraph-deepwiki-skill@hugegraph-deepwiki-skill
```

从已发布分支安装时，先 clone 仓库，再从本地模块路径安装：

```bash
git clone -b <branch> https://github.com/<owner>/hugegraph.git
cd hugegraph/tools/ai/hugegraph-deepwiki-skill
codex plugin marketplace add "$(pwd)"
codex plugin add hugegraph-deepwiki-skill@hugegraph-deepwiki-skill
```

如果当前 Codex 版本不能直接安装 plugin，可以安装 raw skill：

```bash
CODEX_HOME="${CODEX_HOME:-$HOME/.codex}"
mkdir -p "$CODEX_HOME/skills"
cp -R plugins/hugegraph-deepwiki-skill/skills/hugegraph-deepwiki-skill "$CODEX_HOME/skills/"
```

### 让 Codex 自动安装

在 HugeGraph 仓库根目录的 Codex 里粘贴：

```text
Install the HugeGraph repository assistant from this checkout. Enter `tools/ai/hugegraph-deepwiki-skill`, run `codex plugin marketplace add "$(pwd)"`, then run `codex plugin add hugegraph-deepwiki-skill@hugegraph-deepwiki-skill`. If this Codex build has no plugin add command, copy `plugins/hugegraph-deepwiki-skill/skills/hugegraph-deepwiki-skill` into `${CODEX_HOME:-$HOME/.codex}/skills`. Do not hardcode absolute paths.
```

## 使用方式

安装后，可以在提问时显式指定：

```text
Use $hugegraph-deepwiki-skill to explain HugeGraph schema and traversal behavior.
```

HugeGraph AI 相关问题请安装 `apache/hugegraph-ai` 仓库中的独立 HugeGraph AI 仓库知识助手。
