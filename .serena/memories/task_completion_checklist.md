# Task Completion Checklist

## 1. Code Quality (MANDATORY)
```bash
mvn apache-rat:check -ntp       # License headers
mvn editorconfig:check          # Style (.editorconfig)
mvn checkstyle:check            # Style (style/checkstyle.xml)
mvn clean compile -Dmaven.javadoc.skip=true  # Compile warnings
```

## 2. Testing
- Choose backend: `memory` (fast), `rocksdb` (realistic), `hbase` (distributed)
- Single test: `-Dtest=ClassName` works with all profiles
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
- Raft tests: only `test*`/`raft*` branches
- TinkerPop tests: only `release-*`/`test-*` branches

## 5. Commit
- NEVER commit unless explicitly asked
- Format: `feat|fix|refactor(module): msg`
- Include issue ID if available
