# Serena project context

This directory is a compact navigation cache for coding agents. It is not an independent documentation set and must not duplicate product guides, option tables or release history.

## Read order

1. [`../AGENTS.md`](../AGENTS.md) for repository-wide constraints and validation expectations.
2. [`../docs/README.md`](../docs/README.md) for the authoritative documentation map.
3. The nearest module `AGENTS.md` and README for the code being changed.
4. A memory below only when a quick architecture or file-location map is useful.

## Maintained memories

| Memory | Purpose |
|---|---|
| [project_overview.md](memories/project_overview.md) | Stable product scope and authoritative entry points |
| [architecture_and_modules.md](memories/architecture_and_modules.md) | High-level module ownership and runtime relationships |
| [key_file_locations.md](memories/key_file_locations.md) | Search anchors for common work areas |

Build commands, style rules, test selection and completion checks belong in `AGENTS.md`, module files and executable CI configuration. Keeping them there prevents this cache from drifting.

## Maintenance policy

- Store stable relationships and search anchors, not timestamps, onboarding status, pull-request links, commit hashes or copied release numbers.
- Link to the owning file instead of reproducing long procedures.
- Update or remove a memory in the same change when module ownership or a referenced path changes.
- Keep each memory small enough to load on demand; do not turn the cache into a second README tree.
