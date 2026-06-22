# HugeGraph PD Development Guide

This document provides comprehensive guidance for developing, testing, and contributing to HugeGraph PD.

## Table of Contents

- [Development Environment Setup](#development-environment-setup)
- [Building from Source](#building-from-source)
- [Testing](#testing)
- [Development Workflows](#development-workflows)
- [Code Style and Standards](#code-style-and-standards)
- [Debugging](#debugging)
- [Contributing](#contributing)

## Development Environment Setup

### Prerequisites

Ensure you have the following tools installed:

| Tool | Minimum Version | Recommended | Purpose |
|------|----------------|-------------|---------|
| **JDK** | 11 | 11 or 17 | Java runtime and compilation |
| **Maven** | 3.5.0 | 3.8+ | Build tool and dependency management |
| **Git** | 2.0+ | Latest | Version control |
| **IDE** | N/A | IntelliJ IDEA | Development environment |

### Verify Installation

```bash
# Check Java version
java -version
# Expected: openjdk version "11.0.x" or later

# Check Maven version
mvn -version
# Expected: Apache Maven 3.5.0 or later

# Check Git version
git --version
# Expected: git version 2.x
```

### Clone Repository

```bash
# Clone HugeGraph repository
git clone https://github.com/apache/hugegraph.git
cd hugegraph

# PD module location
cd hugegraph-pd
```

### IDE Setup (IntelliJ IDEA)

#### Import Project

1. Open IntelliJ IDEA
2. **File → Open** → Select `hugegraph-pd` directory
3. Wait for Maven to download dependencies (may take 5-10 minutes)

#### Configure Code Style

1. **File → Settings → Editor → Code Style**
2. **Import Scheme → IntelliJ IDEA code style XML**
3. Select `hugegraph-style.xml` from repository root
4. **Apply** and **OK**

#### Enable Annotation Processing

Required for Lombok support:

1. **File → Settings → Build, Execution, Deployment → Compiler → Annotation Processors**
2. Check **Enable annotation processing**
3. **Apply** and **OK**

#### Configure JDK

1. **File → Project Structure → Project**
2. **Project SDK**: Select JDK 11 or 17
3. **Project language level**: 11
4. **Apply** and **OK**

## Building from Source

### Full Build

Build all PD modules from the `hugegraph-pd` directory:

```bash
cd hugegraph-pd
mvn clean install -DskipTests
```

**Output**:
- JARs in each module's `target/` directory
- Distribution package: `hg-pd-dist/target/hugegraph-pd-<version>.tar.gz`

**Build Time**: 2-5 minutes (first build may take longer for dependency download)

### Module-Specific Builds

Build individual modules:

```bash
# Build gRPC module only (regenerate proto stubs)
mvn clean compile -pl hg-pd-grpc

# Build core module only
mvn clean install -pl hg-pd-core -am -DskipTests

# Build service module only
mvn clean install -pl hg-pd-service -am -DskipTests

# Build distribution package only
mvn clean package -pl hg-pd-dist -am -DskipTests
```

**Maven Flags**:
- `-pl <module>`: Build specific module
- `-am`: Also build required dependencies (--also-make)
- `-DskipTests`: Skip test execution (faster builds)
- `-Dmaven.test.skip=true`: Skip test compilation and execution

### Clean Build

Remove all build artifacts:

```bash
mvn clean

# This also removes:
# - *.tar, *.tar.gz files
# - .flattened-pom.xml (CI-friendly versioning)
```

### Build from Project Root

Build PD from HugeGraph root directory:

```bash
cd /path/to/hugegraph

# Build PD and dependencies
mvn clean package -pl hugegraph-pd -am -DskipTests
```

## Testing

### Test Organization

PD tests are located in `hg-pd-test/src/main/java/` (non-standard location):

```
hg-pd-test/src/main/java/org/apache/hugegraph/pd/
├── BaseTest.java               # Base test class with common setup
├── core/                       # Core service tests
│   ├── PartitionServiceTest.java
│   ├── StoreNodeServiceTest.java
│   └── ...
├── client/                     # Client library tests
├── raft/                       # Raft integration tests
└── PDCoreSuiteTest.java        # Test suite (runs all tests)
```

### Running Tests

#### All Tests

```bash
# Run all PD tests
mvn test

# Run all tests with coverage report
mvn test jacoco:report
# Coverage report: hg-pd-test/target/site/jacoco/index.html
```

#### Module-Specific Tests

```bash
# Core module tests
mvn test -pl hugegraph-pd/hg-pd-test -am -P pd-core-test

# Client module tests
mvn test -pl hugegraph-pd/hg-pd-test -am -P pd-client-test

# Common module tests
mvn test -pl hugegraph-pd/hg-pd-test -am -P pd-common-test

# REST API tests
mvn test -pl hugegraph-pd/hg-pd-test -am -P pd-rest-test
```

#### Single Test Class

```bash
# Run specific test class
mvn -pl hugegraph-pd/hg-pd-test test -Dtest=PartitionServiceTest -DfailIfNoTests=false

# Run specific test method
mvn -pl hugegraph-pd/hg-pd-test test -Dtest=PartitionServiceTest#testSplitPartition -DfailIfNoTests=false
```

#### Test from IDE

**IntelliJ IDEA**:
1. Open test class (e.g., `PartitionServiceTest.java`)
2. Right-click on class name or test method
3. Select **Run 'PartitionServiceTest'**

### Test Coverage

View test coverage report:

```bash
# Generate coverage report
mvn test jacoco:report

# Open report in browser
open hg-pd-test/target/site/jacoco/index.html
```

**Target Coverage**:
- Core services: >80%
- Utility classes: >70%
- Generated gRPC code: Excluded from coverage

**What Integration Tests Cover**:
- Raft cluster formation and leader election
- Partition allocation and balancing
- Store registration and heartbeat processing
- Metadata persistence and recovery
- gRPC service interactions


### Debugging Raft Issues

Enable detailed Raft logging in `conf/log4j2.xml`:

```xml
<Loggers>
    <!-- Enable Raft debug logging -->
    <Logger name="com.alipay.sofa.jraft" level="DEBUG"/>

    <!-- Specific Raft components -->
    <Logger name="com.alipay.sofa.jraft.core.NodeImpl" level="DEBUG"/>
    <Logger name="com.alipay.sofa.jraft.storage.impl" level="DEBUG"/>
</Loggers>
```

**Raft State Inspection**:
```bash
# Check Raft data directory
ls -lh pd_data/raft/

# Raft logs
ls -lh pd_data/raft/log/

# Raft snapshots
ls -lh pd_data/raft/snapshot/
```

**Common Raft Issues**:

| Issue | Symptom | Solution |
|-------|---------|----------|
| **Split-brain** | Multiple leaders | Check `peers-list` consistency, network partitioning |
| **Leader election failure** | Constant candidate state | Check network latency, increase election timeout |
| **Log replication lag** | Followers behind leader | Check follower disk I/O, network bandwidth |
| **Snapshot transfer failure** | Followers can't catch up | Check snapshot directory permissions, disk space |

## Code Style and Standards

### Code Formatting

HugeGraph PD follows Apache HugeGraph code style.

**Import Code Style**:
1. IntelliJ IDEA: **File → Settings → Editor → Code Style**
2. **Import Scheme** → Select `hugegraph-style.xml` (in repository root)

**Key Style Rules**:
- **Indentation**: 4 spaces (no tabs)
- **Line length**: 100 characters (Java), 120 characters (comments)
- **Braces**: K&R style (opening brace on same line)
- **Imports**: No wildcard imports (`import java.util.*`)

### License Headers

All source files must include Apache License header.

**Check License Headers**:
```bash
mvn apache-rat:check

# Output: target/rat.txt (lists files missing license headers)
```

**Add License Header**:
Manually add to new files:
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
```

### Naming Conventions

| Type | Convention | Example |
|------|------------|---------|
| **Classes** | PascalCase | `PartitionService`, `StoreNodeService` |
| **Interfaces** | PascalCase (prefix with `I` optional) | `MetadataStore` or `IMetadataStore` |
| **Methods** | camelCase | `getPartition()`, `registerStore()` |
| **Variables** | camelCase | `storeId`, `partitionCount` |
| **Constants** | UPPER_SNAKE_CASE | `MAX_RETRY_COUNT`, `DEFAULT_TIMEOUT` |
| **Packages** | lowercase | `org.apache.hugegraph.pd.core` |

### JavaDoc

Public APIs must include JavaDoc comments.

**Example**:
```java
/**
 * Get partition by partition code.
 *
 * @param graphName   the graph name
 * @param partitionId the partition ID
 * @return the partition metadata
 * @throws PDException if partition not found or Raft error
 */
public Partition getPartitionByCode(String graphName, int partitionId) throws PDException {
    // Implementation...
}
```

**Required for**:
- All public classes and interfaces
- All public and protected methods
- Complex private methods (optional but recommended)

### Error Handling

Use custom `PDException` for PD-specific errors.

**Example**:
```java
if (store == null) {
    throw new PDException(ErrorType.STORE_NOT_FOUND,
                         "Store not found: " + storeId);
}
```

**Exception Hierarchy**:
- `PDException`: Base exception for all PD errors
- `RaftException`: Raft-related errors (from JRaft)
- `GrpcException`: gRPC communication errors

## Debugging

### Local Debugging in IDE

#### Run PD from IDE

1. Create run configuration in IntelliJ IDEA:
   - **Run → Edit Configurations**
   - **Add New Configuration → Application**
   - **Main class**: `org.apache.hugegraph.pd.HgPdApplication` (in `hg-pd-service`)
   - **Program arguments**: `--spring.config.location=file:./conf/application.yml`
   - **Working directory**: `hugegraph-pd/hg-pd-dist/target/hugegraph-pd-<version>/`
   - **JRE**: 11 or 17

2. Set breakpoints in code

3. Click **Debug** (Shift+F9)

#### Debug Tests

1. Open test class (e.g., `PartitionServiceTest.java`)
2. Set breakpoints
3. Right-click on test method → **Debug 'testMethod'**

### Remote Debugging

Debug PD running on a remote server.

**Start PD with Debug Port**:
```bash
bin/start-hugegraph-pd.sh -j "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"
```

**Connect from IDE**:
1. **Run → Edit Configurations → Add New → Remote JVM Debug**
2. **Host**: PD server IP
3. **Port**: `5005`
4. **Debugger mode**: Attach
5. Click **Debug**

### Logging

Increase log verbosity for troubleshooting.

**Edit `conf/log4j2.xml`**:
```xml
<Loggers>
    <!-- Enable DEBUG logging for PD -->
    <Logger name="org.apache.hugegraph.pd" level="DEBUG"/>

    <!-- Enable DEBUG for specific package -->
    <Logger name="org.apache.hugegraph.pd.core.PartitionService" level="DEBUG"/>
</Loggers>
```

**View Logs**:
```bash
# Real-time log monitoring
tail -f logs/hugegraph-pd.log

# Search logs
grep "ERROR" logs/hugegraph-pd.log
grep "PartitionService" logs/hugegraph-pd.log
```

### Performance Profiling

Use JVM profiling tools to identify performance bottlenecks.

**Async-profiler** (recommended):
```bash
# Download async-profiler
wget https://github.com/jvm-profiling-tools/async-profiler/releases/download/v2.9/async-profiler-2.9-linux-x64.tar.gz
tar -xzf async-profiler-2.9-linux-x64.tar.gz

# Profile running PD process
./profiler.sh -d 60 -f /tmp/pd-profile.svg <PD_PID>

# View flamegraph
open /tmp/pd-profile.svg
```

**JProfiler**:
1. Download JProfiler from https://www.ej-technologies.com/products/jprofiler/overview.html
2. Attach to running PD process
3. Analyze CPU, memory, and thread usage

## Contributing

### Contribution Workflow

1. **Fork Repository**:
   - Fork https://github.com/apache/hugegraph on GitHub

2. **Clone Your Fork**:
   ```bash
   git clone https://github.com/YOUR_USERNAME/hugegraph.git
   cd hugegraph
   ```

3. **Create Feature Branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

4. **Make Changes**:
   - Write code
   - Add tests
   - Update documentation

5. **Run Tests**:
   ```bash
   mvn test -pl hugegraph-pd/hg-pd-test -am
   ```

6. **Check Code Style**:
   ```bash
   mvn apache-rat:check
   ```

7. **Commit Changes**:
   ```bash
   git add .
   git commit -m "feat(pd): add new feature description"
   ```

   **Commit Message Format**:
   ```
   <type>(<scope>): <subject>

   <body>

   <footer>
   ```

   **Types**: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`

   **Example**:
   ```
   feat(pd): add partition auto-splitting

   - Implement partition split threshold detection
   - Add split operation via Raft proposal
   - Update partition metadata after split

   Closes #123
   ```

8. **Push to Fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

9. **Create Pull Request**:
   - Go to https://github.com/apache/hugegraph
   - Click **New Pull Request**
   - Select your fork and branch
   - Fill in PR description (what, why, how)
   - Submit PR

### Pull Request Guidelines

**PR Title Format**:
```
[PD] Brief description of changes
```

**PR Description Template**:
```markdown
## What changes were proposed in this pull request?
<!-- Describe the changes in detail -->

## Why are the changes needed?
<!-- Explain the motivation and context -->

## How was this patch tested?
<!-- Describe the testing process -->

## Related Issues
<!-- Link to related issues: Closes #123 -->
```

**Before Submitting**:
- [ ] Tests pass locally
- [ ] Code style is correct (`mvn apache-rat:check`)
- [ ] JavaDoc added for public APIs
- [ ] Documentation updated (if applicable)
- [ ] Commit messages follow convention

### Code Review Process

1. **Automated Checks**:
   - CI builds and tests PR
   - Code style validation
   - License header check

2. **Reviewer Feedback**:
   - Address reviewer comments
   - Push updates to same branch
   - PR automatically updates

3. **Approval**:
   - At least 1 committer approval required
   - All CI checks must pass

4. **Merge**:
   - Committer merges PR
   - Delete feature branch

## Additional Resources

### Documentation

- [Architecture Documentation](architecture.md) - System design and components
- [API Reference](api-reference.md) - gRPC APIs and examples
- [Configuration Guide](configuration.md) - Configuration options and tuning

### Community

- **Mailing List**: dev@hugegraph.apache.org
- **GitHub Issues**: https://github.com/apache/hugegraph/issues
- **GitHub Discussions**: https://github.com/apache/hugegraph/discussions

### Useful Commands

```bash
# Quick build (no tests)
mvn clean install -DskipTests -pl hugegraph-pd -am

# Run specific test
mvn test -pl hugegraph-pd/hg-pd-test -am -Dtest=PartitionServiceTest

# Generate coverage report
mvn test jacoco:report -pl hugegraph-pd/hg-pd-test -am

# Check license headers
mvn apache-rat:check -pl hugegraph-pd

# Package distribution
mvn clean package -DskipTests -pl hugegraph-pd/hg-pd-dist -am

# Clean all build artifacts
mvn clean -pl hugegraph-pd
```

## Summary

This guide covers:
- **Setup**: Environment configuration and IDE setup
- **Building**: Maven commands for full and module-specific builds
- **Testing**: Running tests and viewing coverage reports
- **Development**: Adding gRPC services, metadata stores, and modifying core logic
- **Debugging**: Local and remote debugging, logging, profiling
- **Contributing**: Workflow, PR guidelines, and code review process

For questions or assistance, reach out to the HugeGraph community via mailing list or GitHub issues.

Happy coding!
