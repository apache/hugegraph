# Contributing to HugeGraph

Keep each contribution focused on one problem, with enough context and validation for review.
Report reproducible bugs and proposed features through [GitHub issues](https://github.com/apache/hugegraph/issues).

## Prepare a change

- Read [AGENTS.md](../AGENTS.md) and the guidance for the modules you will change.
  The same repository rules apply to changes made by people and coding agents.
- Work on a topic branch or isolated worktree based on the intended target branch.
  Preserve existing uncommitted work; stage only files belonging to your change.
- Check existing implementations and dependencies before adding new abstractions or libraries.
  Keep unrelated cleanup out of the PR.
- Follow [.editorconfig](../.editorconfig) and [Checkstyle](../style/checkstyle.xml):
  four-space indentation, 120-column Java lines, and no wildcard imports.

## Build and validate

Use Java 17 and Maven 3.6.3+. Compiler settings and project versions are defined in
[pom.xml](../pom.xml); module guidance describes test profiles and service prerequisites.
Run commands from the repository root.

```bash
# Build the server and its dependencies without running tests
mvn clean install -pl hugegraph-server -am -DskipTests

# Before pushing code, format and compile
mvn editorconfig:format
mvn clean compile -Dmaven.javadoc.skip=true
```

The root POM and child modules flatten CI-friendly versions during `process-resources`,
so installed and deployed POMs use a concrete version instead of `${revision}`. Build the
reactor with `install` before building modules separately; keep Maven Resolver validation
enabled, including on Maven 3.10+.

Run the affected module tests and add regression coverage when behavior changes.
A successful build with skipped tests does not validate behavior; Commons tests require
`-DskipCommonsTests=false`. For documentation-only changes, verify links and paths and run
`git diff --check`. Inspect formatter output before staging to avoid unrelated edits.

See [CI policy](ci.md) for affected-module selection, required checks, result reuse,
and automatic cancellation and retries. Report what you actually validated and any limitations.

## Documentation and dependencies

- User-visible features, configuration and deployment changes need matching repository
  documentation in the same PR. When website documentation is affected, link a paired
  [apache/hugegraph-doc](https://github.com/apache/hugegraph-doc) PR and coordinate both merges.
  A follow-up issue does not replace the required documentation.
- In the PR template, use `Doc - TODO` while documentation is pending, `Doc - Done` with
  its location when ready, or `Doc - No Need` for internal-only changes.
- New or updated dependencies need appropriate license and notice updates in the root
  and [release documentation](../install-dist/release-docs/), including bundled license files.
  Update the [dependency inventory](../install-dist/scripts/dependency/known-dependencies.txt)
  using [the regeneration script](../install-dist/scripts/dependency/regenerate_known_dependencies.sh).

Install/package the current reactor before regenerating the inventory:

```bash
mvn clean install -DskipTests -Dmaven.javadoc.skip=true
bash install-dist/scripts/dependency/regenerate_known_dependencies.sh
```

Clean rebuilds the current revision's distributions to avoid stale dependency jars.
It also removes root-level `*.tar.gz` distribution archives; other revision directories remain.
The build must produce Server, PD and Store distributions for the same current revision.
It skips tests and is not validation evidence. The inventory combines Maven runtime
dependencies with flat and nested distribution jars, including Spring Boot `BOOT-INF/lib`
dependencies inserted by repackaging. Missing distributions fail collection; source or POM
inspection alone cannot establish the full shipped inventory. Review all additions and
removals and their license/NOTICE coverage. The dependency check compares the exact inventory;
repeat it for release platform/profile variants as needed.

## Submit and review

Use `type(scope): description` for commits; omit the scope when it is unclear.
Types are `feat`, `fix`, `chore`, `docs`, `refactor`, or `BREAKING CHANGE`.
Start the description with a lowercase verb, keep it under 50 characters, and include
three to five core change points in the body. For example: `fix(server): handle missing vertices`.

Push your topic branch and open or update a PR against the intended upstream branch.
Use the [PR template](../.github/PULL_REQUEST_TEMPLATE.md) to explain the problem,
the resulting behavior, relevant validation, and documentation status. Link the related
issue when applicable. A short example or diagram is useful when it clarifies the change;
commit histories and raw test logs are unnecessary.

Address review findings within the agreed scope and validate the affected paths again.
Resolve a discussion after its issue is fixed or clarified. Inspect failing CI jobs before
retrying; a rerun is appropriate for a temporary infrastructure failure, not a reproducible bug.
