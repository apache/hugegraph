# Task Completion Checklist

When completing a coding task, follow these steps to ensure quality and compliance:

## 1. Code Quality Checks (MANDATORY)

### License Header Check
Run Apache RAT to verify all files have proper license headers:
```bash
mvn apache-rat:check -ntp
```
Fix any violations by adding the Apache license header.

### Code Style Check
Run EditorConfig validation:
```bash
mvn editorconfig:check
```
Fix violations according to `.editorconfig` rules.

### Compilation Check
Compile with warnings enabled:
```bash
mvn clean compile -Dmaven.javadoc.skip=true
```
Resolve all compiler warnings, especially unchecked operations.

## 2. Testing (REQUIRED)

### Determine Test Scope
Check project README or ask user for test commands. Common patterns:

#### Server Module Tests
- Unit tests:
```bash
mvn test -pl hugegraph-server/hugegraph-test -am -P unit-test,memory
```
- Core tests (choose backend):
```bash
mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,memory
mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,rocksdb
```
- API tests:
```bash
mvn test -pl hugegraph-server/hugegraph-test -am -P api-test,rocksdb
```

#### PD Module Tests
```bash
# Build struct dependency first
mvn install -pl hugegraph-struct -am -DskipTests
# Run PD tests
mvn test -pl hugegraph-pd/hg-pd-test -am
```

#### Store Module Tests
```bash
# Build struct dependency first
mvn install -pl hugegraph-struct -am -DskipTests
# Run Store tests
mvn test -pl hugegraph-store/hg-store-test -am
```

### Run Appropriate Tests
Execute tests relevant to your changes:
- For bug fixes: run existing tests to verify fix
- For new features: write and run new tests
- For refactoring: run all affected module tests

## 3. Dependencies Management

If adding new third-party dependencies:

1. Add license file to `install-dist/release-docs/licenses/`
2. Declare dependency in `install-dist/release-docs/LICENSE`
3. Append NOTICE (if exists) to `install-dist/release-docs/NOTICE`
4. Update dependency list:
```bash
./install-dist/scripts/dependency/regenerate_known_dependencies.sh
```
Or manually update `install-dist/scripts/dependency/known-dependencies.txt`

## 4. Build Verification

Build the affected module(s) with tests:
```bash
mvn clean install -pl <module> -am
```

## 5. Documentation (if applicable)

- Update JavaDoc for public APIs
- Update README if adding user-facing features
- Update AGENTS.md if adding dev-facing information

## 6. Commit Preparation

### NEVER Commit Unless Explicitly Asked
- Do NOT auto-commit changes
- Only commit when user explicitly requests it
- This is CRITICAL to avoid surprising users

### When Asked to Commit
- Write clear commit messages:
```
Fix bug: <brief description>

fix #ISSUE_ID
```
- Include issue ID if available
- Describe what and how the change works

## 7. Pre-PR Checklist

Before creating a Pull Request, ensure:
- [ ] All license checks pass
- [ ] All code style checks pass
- [ ] All relevant tests pass
- [ ] Code compiles without warnings
- [ ] Dependencies properly documented (if added)
- [ ] Changes tested locally
- [ ] Commit message is clear and references issue

## Common CI Workflows

Your changes will be validated by:
- `server-ci.yml`: Compiles + unit/core/API tests (memory, rocksdb, hbase)
- `licence-checker.yml`: License header validation
- `pd-store-ci.yml`: PD and Store module tests
- `commons-ci.yml`: Commons module tests
- `cluster-test-ci.yml`: Distributed cluster tests

## Notes

- **Test Backend Selection**: Use `memory` for quick tests, `rocksdb` for realistic tests, `hbase` for distributed scenarios
- **TinkerPop Tests**: Only run on release branches (release-*/test-*)
- **Raft Tests**: Only run when branch name starts with `test` or `raft`
- **Build Time**: Full build can take 5-15 minutes depending on hardware
- **Test Time**: Test suites can take 10-30 minutes depending on backend
