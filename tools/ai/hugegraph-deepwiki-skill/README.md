# HugeGraph Repository Assistant

[中文](README-zh.md) | [English](README.md)

This standalone module packages a Claude Code and Codex skill for answering questions about the [Apache HugeGraph](https://github.com/apache/hugegraph) source repository.

DeepWiki is used as the online knowledge and MCP transport layer:

```text
https://deepwiki.com/apache/hugegraph
https://mcp.deepwiki.com/mcp
```

## What It Does

- Answers repository-grounded questions about HugeGraph architecture, modules, APIs, storage backends, schema, traversal, configuration, build, tests, and implementation details.
- Uses `read_wiki_contents` to build a local DeepWiki wiki cache and searches that cache before answering.
- Uses `ask_question` when the cached context does not directly and precisely answer the question.
- Avoids cloning upstream repositories for ordinary Q&A.

## Requirements

- Python 3.9 or later for the bundled MCP client script.
- Network access to `https://mcp.deepwiki.com/mcp`.

## Layout

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

## Claude Code Install

From this repository:

```bash
cd tools/ai/hugegraph-deepwiki-skill
claude plugin marketplace add "$(pwd)"
claude plugin install hugegraph-deepwiki-skill@hugegraph-deepwiki-skill
```

From a published branch, clone the repository first and install from the local module path:

```bash
git clone -b <branch> https://github.com/<owner>/hugegraph.git
cd hugegraph/tools/ai/hugegraph-deepwiki-skill
claude plugin marketplace add "$(pwd)"
claude plugin install hugegraph-deepwiki-skill@hugegraph-deepwiki-skill
```

Manual user-level skill install:

```bash
mkdir -p ~/.claude/skills
cp -R plugins/hugegraph-deepwiki-skill/skills/hugegraph-deepwiki-skill ~/.claude/skills/
```

### Ask Claude Code To Install It

Paste this into Claude Code from the HugeGraph repository root:

```text
Install the HugeGraph repository assistant from this checkout. Enter `tools/ai/hugegraph-deepwiki-skill`, run `claude plugin marketplace add "$(pwd)"`, then run `claude plugin install hugegraph-deepwiki-skill@hugegraph-deepwiki-skill`. Do not hardcode absolute paths.
```

## Codex Install

From this repository:

```bash
cd tools/ai/hugegraph-deepwiki-skill
codex plugin marketplace add "$(pwd)"
codex plugin add hugegraph-deepwiki-skill@hugegraph-deepwiki-skill
```

From a published branch, clone the repository first and install from the local module path:

```bash
git clone -b <branch> https://github.com/<owner>/hugegraph.git
cd hugegraph/tools/ai/hugegraph-deepwiki-skill
codex plugin marketplace add "$(pwd)"
codex plugin add hugegraph-deepwiki-skill@hugegraph-deepwiki-skill
```

If your Codex build cannot install plugins directly, install the raw skill:

```bash
CODEX_HOME="${CODEX_HOME:-$HOME/.codex}"
mkdir -p "$CODEX_HOME/skills"
cp -R plugins/hugegraph-deepwiki-skill/skills/hugegraph-deepwiki-skill "$CODEX_HOME/skills/"
```

### Ask Codex To Install It

Paste this into Codex from the HugeGraph repository root:

```text
Install the HugeGraph repository assistant from this checkout. Enter `tools/ai/hugegraph-deepwiki-skill`, run `codex plugin marketplace add "$(pwd)"`, then run `codex plugin add hugegraph-deepwiki-skill@hugegraph-deepwiki-skill`. If this Codex build has no plugin add command, copy `plugins/hugegraph-deepwiki-skill/skills/hugegraph-deepwiki-skill` into `${CODEX_HOME:-$HOME/.codex}/skills`. Do not hardcode absolute paths.
```

## Usage

After installation, ask for the skill explicitly when needed:

```text
Use $hugegraph-deepwiki-skill to explain HugeGraph schema and traversal behavior.
```

For HugeGraph AI questions, install the separate HugeGraph AI repository assistant from the `apache/hugegraph-ai` repository instead.
