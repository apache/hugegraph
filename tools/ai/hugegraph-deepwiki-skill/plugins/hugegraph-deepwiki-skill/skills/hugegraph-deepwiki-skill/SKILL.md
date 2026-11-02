---
name: hugegraph-deepwiki-skill
description: Use this skill as a repository knowledge assistant for Apache HugeGraph, apache/hugegraph source code, architecture, modules, APIs, configuration, storage backends, Gremlin/traversal behavior, schema/modeling, server/client tooling, build/test workflows, or implementation details. It answers questions grounded in apache/hugegraph and uses the official DeepWiki MCP wiki as the underlying retrieval channel.
metadata:
  short-description: Apache HugeGraph repository assistant
---

# HugeGraph Repository Knowledge Assistant

Answer questions about the Apache HugeGraph source repository. Use the official DeepWiki MCP server as the underlying knowledge retrieval channel.

- Source repository: `https://github.com/apache/hugegraph`
- DeepWiki page: `https://deepwiki.com/apache/hugegraph`
- MCP endpoint: `https://mcp.deepwiki.com/mcp`
- Default repository: `apache/hugegraph`
- Runtime requirements: Python 3.9+ and network access to the MCP endpoint.

## Default Workflow

1. Preserve the user's question, including code snippets, version constraints, error messages, and environment details.
2. Change directory to this skill directory, the directory containing this `SKILL.md`.
3. Search the local DeepWiki wiki cache for relevant context. If the cache does not exist yet, this command fetches `read_wiki_contents` from DeepWiki once and saves it under the user's cache directory. It prints only relevant snippets, not the full wiki dump:

```bash
python3 scripts/deepwiki_mcp.py context --repo hugegraph --query "<user question>"
```

4. Answer from cached context only when the snippets directly and precisely answer the user's question. If they are merely related background, continue to `ask`.
5. For broad navigation questions, read the wiki structure instead:

```bash
python3 scripts/deepwiki_mcp.py structure --repo hugegraph
```

6. If the cached wiki context does not directly and precisely answer the question, do not answer the user yet. You must use DeepWiki's AI `ask_question` tool to request an online answer:

```bash
python3 scripts/deepwiki_mcp.py ask --repo hugegraph --question "<user question>"
```

7. For `ask`, preserve the user's original question. Do not expand it with extra requirements, long source-reference requests, or your own multi-part prompt; longer generated questions are more likely to time out.
8. If `ask` returns uncertainty, times out, or reports a transport/query error, retry once with the shortest faithful form of the user's original question. If it still fails, say so plainly and answer only from the cached context if it is sufficient.
9. If the user needs source references for an `ask` answer, use the cached context or contents to identify the relevant wiki page snippets and source-file references. `ask` usually returns the final answer plus suggested wiki pages or a DeepWiki search link, not the raw code files used to generate the answer.

## Routing Rules

- Use `structure` first for navigation, table-of-contents, or "where should I look?" questions.
- Use `context` first for normal Q&A, source-reference requests, and token-efficient grounding.
- Use `ask` after `context` whenever cached snippets do not provide a direct and precise answer, or when the question needs synthesis across multiple areas. Do not answer directly from related-but-insufficient cached snippets.
- If both an online answer and source references are needed, run `ask` for the answer and use `context` to collect source references.
- Do not clone the repository for ordinary Q&A or verification. If current source verification is truly required, prefer online source links or raw GitHub files and clearly distinguish that from DeepWiki-grounded content.

## When to Read Structure or Pages

For broad navigation questions, or when the user asks where something lives, inspect the wiki structure:

```bash
cd <directory-containing-this-SKILL.md>
python3 scripts/deepwiki_mcp.py structure --repo hugegraph
```

If the user needs a fuller wiki dump for offline review or synthesis, read the wiki contents:

```bash
cd <directory-containing-this-SKILL.md>
python3 scripts/deepwiki_mcp.py contents --repo hugegraph
```

The `contents` command uses the same local cache by default. Use `--refresh` only when the user explicitly needs a fresh DeepWiki snapshot.

For normal Q&A, prefer `context` over `contents` so only the relevant cached snippets enter the model context. When the cached wiki context does not directly and precisely answer the question, run `ask` for an online DeepWiki answer before responding.

## Repository Profile

The repository alias lives in `references/repos.json`.

- `hugegraph` maps to `apache/hugegraph`.
- For Apache HugeGraph AI questions, use the separate `hugegraph-ai-deepwiki-skill` instead of this skill.

## Answering Guidance

- Keep responses practical: include class/module names, configuration keys, command names, or API names when DeepWiki provides them.
- Prefer online DeepWiki retrieval and cached wiki search. Do not clone the source repository just to answer a question.
- If the user asks for code changes in a local HugeGraph checkout, use DeepWiki for orientation, then inspect and edit the local repository directly.
- Do not invent details that DeepWiki does not provide. Clearly distinguish DeepWiki-grounded facts from your own inference.
- For version-sensitive release, dependency, or API-compatibility questions, verify with the live repository or official docs when the user needs current facts beyond the DeepWiki answer.
