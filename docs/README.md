# Repository documentation

This directory contains source-specific documentation for Apache HugeGraph. It complements the [official website documentation](https://hugegraph.apache.org/docs/) and should not duplicate complete user guides that already live there.

## Find the right document

| Task | Document |
|---|---|
| Understand the repository and deployment choices | [Root README](../README.md) |
| Build the project from source | [BUILDING.md](BUILDING.md) |
| Prepare and validate a contribution | [CONTRIBUTING.md](CONTRIBUTING.md) |
| Understand CI selection and required checks | [ci.md](ci.md) |
| Call the REST API | [rest-api-usage.md](rest-api-usage.md) |
| Use the Cypher endpoint | [cypher-api.md](cypher-api.md) |
| Use the same-neighbors batch API | [same-neighbors-batch.md](same-neighbors-batch.md) |
| Configure clients or diagnose Server runtime behavior | [server-runtime.md](server-runtime.md) |
| Select, upgrade or recover a storage backend | [storage-lifecycle.md](storage-lifecycle.md) |
| Understand shared module ownership and migration boundaries | [shared-foundation-migration.md](shared-foundation-migration.md) |
| Deploy or operate Kubernetes resources | [Helm README](../helm/hugegraph/README.md) and [Helm docs index](../helm/hugegraph/docs/README.md) |
| Run Docker or Compose environments | [Docker README](../docker/README.md) |
| Navigate the repository with Serena | [Serena project context](../.serena/README.md) |

Module-specific implementation details belong beside the module they describe. Contributor and agent navigation belongs in the root or module `AGENTS.md`; short tool-specific caches under `.serena/` must link back to these authoritative files.

## Source-of-truth hierarchy

Use this order when information overlaps:

1. Executable configuration and source code define current behavior.
2. The closest module README documents module usage and local constraints.
3. This directory documents cross-module build, API, runtime and design topics.
4. The website repository owns long-form user tutorials and release-oriented guides.
5. `AGENTS.md` and `.serena/` summarize navigation for development tools; they must not become independent product documentation.

A secondary document should link to the canonical source instead of copying version numbers, option tables or procedures that will drift.

## Writing durable documentation

- Describe the current contract, supported workflow and operational boundary. Do not record implementation chronology or review discussions.
- Do not embed pull-request links, commit hashes or temporary branch names as durable explanations. Link to a stable file, configuration key, public issue or website section only when it remains useful after the change is merged.
- Give every durable document a clear owner and add it to this index or a module-local index. Avoid isolated Markdown files that are discoverable only through repository search.
- Use task-oriented headings and explicit terms such as component names, configuration keys, file paths and expected results. This improves both human navigation and LLM retrieval.
- Keep prose paragraphs as logical lines. Do not hard-wrap every sentence; wrap near 160 characters only when a long source line becomes difficult to review. Preserve intentional line breaks in lists, tables, code and quoted output.
- Prefer one concise example that users can run over multiple nearly identical examples. Move uncommon recovery procedures or compatibility details into a focused document and link to it.
- State prerequisites, safety implications and verification steps near the operation they qualify.

## Adding or changing a document

Before merging a documentation change:

1. Confirm that an existing document is not already the canonical owner of the topic.
2. Link the new or moved page from this index, a module README or another obvious entry point.
3. Remove duplicated or obsolete text rather than leaving two competing versions.
4. Check relative links, headings, code blocks and commands from the directory where users will run them.
5. Update matching website documentation when public installation, configuration or behavior changes.
