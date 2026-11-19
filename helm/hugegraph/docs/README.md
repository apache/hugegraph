# Helm documentation index

The chart [README](../README.md) is the installation and configuration entry point. This directory contains focused operational and optional-component guides that would make the entry point difficult to scan if kept inline.

## Documents

| Document | Read it when you need to... |
|---|---|
| [distribution.md](distribution.md) | Install from OCI or HTTP repositories, mirror images, work offline, or choose a single-node distribution path |
| [operations.md](operations.md) | Manage credentials, persistence, bootstrap, membership, scaling, upgrades, scheduling, policies, probes and troubleshooting |
| [rag.md](rag.md) | Deploy and secure the HugeGraph AI RAG service through the chart |
| [seatunnel.md](seatunnel.md) | Configure SeaTunnel import, export and migration in standalone or cluster mode |
| [vermeer.md](vermeer.md) | Configure Vermeer computation, Hubble integration, native administration, storage and recovery |

Reviewed value combinations live under [`../examples/`](../examples/). The complete value schema and comments live in [`../values.yaml`](../values.yaml). Keep those as the source of truth instead of copying every value into each guide.

## Recommended reading paths

### First installation

1. [Chart README](../README.md)
2. [Distribution](distribution.md) when the published OCI chart or default images are not directly reachable
3. [Examples](../examples/README.md) for a complete registry, storage, Ingress or GitOps configuration

### Production preparation or upgrade

1. [Operations](operations.md)
2. The active release values from `helm get values ... --all`
3. Relevant optional-component guide when SeaTunnel, Vermeer or RAG is enabled

### Incident or recovery

Start with the matching section in [operations.md](operations.md). Follow links into component-specific guides only when the failure belongs to that component. Record durable recovery rules in the canonical guide rather than appending an incident timeline.

## Documentation ownership

- `../README.md` owns chart purpose, first install, topology selection, common settings and high-level boundaries.
- `operations.md` owns procedures that can affect availability, identity, credentials or persistent data.
- Optional-component guides own their component-specific setup and failure modes.
- `../values.yaml` owns option names, defaults and field-level comments.
- The website owns long-form user tutorials and release-facing documentation.

## Authoring rules

- Link every durable page from this index or an obvious parent section. Do not leave operational instructions as isolated Markdown files.
- Explain the supported current procedure, prerequisites, safety boundary and verification result. Avoid review history, temporary workarounds and change-by-change narratives.
- Do not use pull-request links, commit hashes or branch names as permanent evidence. Prefer stable source paths, value names and public user documentation.
- Keep prose paragraphs as logical lines. Wrap near 160 characters only when necessary for review; preserve structure in tables, lists and code blocks.
- When a value or default changes, update `values.yaml`, the smallest relevant guide and any corresponding website page in the same change.
