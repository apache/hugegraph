# Task Completion Checklist

## 1. Validation scope
Documentation-only changes: check links, paths and `git diff --check`.
For code changes, select relevant checks below and follow root `AGENTS.md` before pushing.
```bash
mvn apache-rat:check -ntp       # License headers
mvn editorconfig:check          # Style (.editorconfig)
mvn checkstyle:check            # Style (style/checkstyle.xml)
mvn clean compile -Dmaven.javadoc.skip=true  # Compile warnings
```

## 2. Testing
- Choose backend: `memory` (fast), `rocksdb` (realistic), `hbase` (deprecated compatibility)
- Single-test selection: check the module test POM and confirm the requested test actually ran.
- Bug fix → existing tests; New feature → write tests; Refactor → affected module tests

## 3. Dependencies (if adding new)
1. License file → `install-dist/release-docs/licenses/`
2. Declare in `install-dist/release-docs/LICENSE`
3. Append NOTICE → `install-dist/release-docs/NOTICE`
4. Run `./install-dist/scripts/dependency/regenerate_known_dependencies.sh`

## 4. CI Awareness
- `server-ci.yml`: memory/rocksdb/hbase × Java 11
- `rerun-ci.yml`: auto-retries flaky failures
- `licence-checker.yml`: header validation
- Server Raft API tests are branch-gated; Store raft-core and core tests run in normal PD/Store CI.
- TinkerPop tests: only `release-*`/`test-*` branches

## 5. Documentation
- Follow root `AGENTS.md`: user-visible feature/configuration/deployment changes ship with matching docs.

## 6. Commit
- NEVER commit unless explicitly asked
- Format: `feat|fix|refactor(module): msg`
- Include issue ID if available
