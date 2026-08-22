# AGENTS.md

This file provides guidance to an AI coding tool when working with code in this repository.

## Project Overview

hugegraph-commons is a shared utility module for Apache HugeGraph and its peripheral components. It provides core infrastructure components (locks, config, events, iterators, REST client, RPC framework) to simplify development across the HugeGraph ecosystem.

**Technology Stack**:
- Java 17 (currently supported release; compiler release: 17)
- Apache Maven 3.6.3+
- Apache Commons Configuration2 for config management
- OkHttp 4.10.0 for REST client (hugegraph-common)
- Sofa-RPC 5.7.6 for RPC framework (hugegraph-rpc)
- JUnit 4.13.1 and Mockito 4.1.0 for testing

## Architecture

### Two-Module Structure

This is a Maven multi-module project with 2 main modules:

1. **hugegraph-common**: Core utilities library
   - Lock implementations (atomic, key, row, lock groups)
   - Configuration system with type-safe options
   - Event hub for async notifications
   - Iterator utilities (map, filter, flat-map, batch)
   - RESTful client (OkHttp-based)
   - Utilities (perf analysis, version checking, collections, logging)
   - License management

2. **hugegraph-rpc**: RPC communication framework
   - Sofa-RPC based client/server implementation
   - Consumer and provider configuration
   - Service registration and discovery
   - **Depends on hugegraph-common**

### Key Design Patterns

1. **Type-Safe Configuration System**: `HugeConfig` + `OptionSpace` pattern
   - Config options defined as typed `ConfigOption` objects
   - Supports both `.properties` and `.yaml` files
   - Options organized in `OptionSpace` groups for validation
   - Security checks on load

2. **Lock Hierarchy**: Multiple lock implementations for different use cases
   - `AtomicLock`: Basic atomic locking
   - `KeyLock`: Lock by specific key
   - `RowLock`: Row-level locking for table-like structures
   - `LockGroup`: Manage multiple related locks
   - `LockManager`: Central lock coordination

3. **Event System**: Async event notification
   - `EventHub`: Central event dispatcher
   - `EventListener`: Typed event handlers
   - Thread-safe event publishing

4. **Iterator Composition**: Chainable iterator wrappers
   - `MapperIterator`, `FilterIterator`, `LimitIterator`
   - `FlatMapperIterator` for nested iteration
   - `BatchMapperIterator` for batch processing
   - All extend `ExtendableIterator` base

5. **RPC Architecture**: Sofa-RPC abstraction layer
   - `RpcServer`: Service provider side
   - `RpcClientProvider`: Service consumer side
   - `RpcProviderConfig`/`RpcConsumerConfig`: Configuration wrappers
   - Supports multiple protocols (bolt, rest, grpc)

## Build & Development Commands

### Prerequisites
```bash
# Verify Java version (Java 17 required)
java -version

# Verify Maven version (3.6.3+ required)
mvn -version
```

### Build Commands

```bash
# Clean build without tests (fastest)
mvn clean install -DskipTests

# Build with tests enabled
mvn clean install

# Build specific module only
mvn clean install -pl hugegraph-common -DskipTests
mvn clean install -pl hugegraph-rpc -am -DskipTests  # -am includes dependencies

# Compile with warnings visible
mvn clean compile -Dmaven.javadoc.skip=true
```

**Note**: Tests are skipped by default via `<skipCommonsTests>true</skipCommonsTests>` in pom.xml. To run tests, override with `-DskipCommonsTests=false`.

### Testing

```bash
# Run all tests (override default skip)
mvn test -DskipCommonsTests=false

# Run tests for specific module
mvn test -pl hugegraph-common -DskipCommonsTests=false
mvn test -pl hugegraph-rpc -am -DskipCommonsTests=false

# Run single test class
mvn test -pl hugegraph-common -Dtest=HugeConfigTest -DskipCommonsTests=false \
  -Dsurefire.failIfNoSpecifiedTests=true

# Run test suite (includes all unit tests)
mvn test -pl hugegraph-common -Dtest=UnitTestSuite -DskipCommonsTests=false \
  -Dsurefire.failIfNoSpecifiedTests=true
```

### Code Quality

```bash
# License header check (Apache RAT)
mvn apache-rat:check

# Checkstyle validation
mvn checkstyle:check

# Both checks run automatically during validate phase
mvn validate
```

### Code Coverage

```bash
# Generate JaCoCo coverage report
mvn clean test -DskipCommonsTests=false
# Report: target/jacoco/index.html
```

## Important File Locations

### Source Code Structure
- hugegraph-common sources: `hugegraph-common/src/main/java/org/apache/hugegraph/`
  - `concurrent/`: Lock implementations
  - `config/`: Configuration system (HugeConfig, OptionSpace, ConfigOption)
  - `event/`: Event hub and listeners
  - `iterator/`: Iterator utilities
  - `rest/`: REST client implementation
  - `util/`: Various utilities (collections, logging, version, etc.)
  - `perf/`: Performance measurement (PerfUtil, Stopwatch)
  - `license/`: License management

- hugegraph-rpc sources: `hugegraph-rpc/src/main/java/org/apache/hugegraph/`
  - `rpc/`: RPC server and client implementations
  - `config/`: RPC-specific config options

### Test Structure
- Unit tests: `hugegraph-{module}/src/test/java/org/apache/hugegraph/unit/`
- Test suites: `UnitTestSuite.java` lists all test classes
- Test utilities: `hugegraph-common/src/main/java/org/apache/hugegraph/testutil/`
  - `Whitebox`: Reflection utilities for testing private members
  - `Assert`: Enhanced assertion utilities

### Configuration Files
- Parent POM: `pom.xml` (defines all dependencies and versions)
- Module POMs: `hugegraph-{module}/pom.xml`
- Test resources: `hugegraph-rpc/src/test/resources/*.properties`
- Checkstyle config: `style/checkstyle.xml` (referenced in parent POM)

### Version Management
- Version property: `${revision}` in pom.xml (currently 1.7.0)
- Version classes:
  - `hugegraph-common/src/main/java/org/apache/hugegraph/version/CommonVersion.java`
  - `hugegraph-rpc/src/main/java/org/apache/hugegraph/version/RpcVersion.java`
- **IMPORTANT**: When changing version in pom.xml, also update version in these Java files

## Development Workflow

### Module Dependencies

**Dependency order**:
```
hugegraph-common (no internal dependencies)
    ↓
hugegraph-rpc (depends on hugegraph-common)
```

When making changes:
- Changes to `hugegraph-common` require rebuilding `hugegraph-rpc`
- Always build common first: `mvn install -pl hugegraph-common -DskipTests`
- Then build rpc: `mvn install -pl hugegraph-rpc -am -DskipTests`

### Working with Configuration System

When adding new configuration options:
1. Define `ConfigOption` in appropriate config class (e.g., `RpcOptions.java`)
2. Register option in an `OptionSpace` for validation
3. Load via `HugeConfig.get(option)` or `config.get(option)`
4. Example:
```java
public static final ConfigOption<String> RPC_SERVER_HOST =
    new ConfigOption<>("rpc.server_host", "...", "127.0.0.1");
```

### Working with RPC Framework

RPC configuration pattern:
- Server side: Create `RpcServer` with `HugeConfig`
- Client side: Use `RpcClientProvider` for service proxies
- Config files: Properties format with `rpc.*` keys
- Protocol: Default is Sofa-Bolt (binary protocol)

### Testing Patterns

1. **Unit tests** extend `BaseUnitTest` (for common module) or use standard JUnit
2. **Test organization**: Tests mirror source package structure
3. **Naming**: `{ClassName}Test.java` for unit tests
4. **Mocking**: Use Mockito for external dependencies
5. **Reflection testing**: Use `Whitebox.setInternalState()` for private field access

### Adding Dependencies

When adding third-party dependencies:
1. Add to `dependencyManagement` section in parent pom if used by multiple modules
2. Declare version in `<properties>` section
3. Add license info to `hugegraph-dist/release-docs/licenses/`
4. Update `hugegraph-dist/release-docs/LICENSE`
5. Update `hugegraph-dist/release-docs/NOTICE` if upstream has NOTICE

## Common Workflows

### Running a Specific Test

```bash
# Single test class
mvn test -pl hugegraph-common -Dtest=HugeConfigTest -DskipCommonsTests=false \
  -Dsurefire.failIfNoSpecifiedTests=true

# Single test method
mvn test -pl hugegraph-common -Dtest=HugeConfigTest#testGetOption \
  -DskipCommonsTests=false -Dsurefire.failIfNoSpecifiedTests=true

# Pattern matching
mvn test -pl hugegraph-common -Dtest=*ConfigTest -DskipCommonsTests=false \
  -Dsurefire.failIfNoSpecifiedTests=true
```

### Debugging Tips

1. **Enable debug logging**: Modify `log4j2.xml` in test resources
2. **Maven debug output**: Add `-X` flag to any Maven command
3. **Skip checkstyle temporarily**: Add `-Dcheckstyle.skip=true`
4. **Force dependency updates**: `mvn clean install -U`

### Working with Parent POM

This module has a parent POM (`../pom.xml` - hugegraph main project). If working standalone:
- The `<revision>` property comes from parent (1.7.0)
- Flatten Maven plugin resolves `${revision}` to actual version
- `.flattened-pom.xml` is auto-generated (excluded from RAT checks)

## Special Notes

### Version Synchronization

Three places to update when changing version:
1. `pom.xml`: `<revision>` property
2. `hugegraph-common/.../CommonVersion.java`: Update version constant
3. `hugegraph-rpc/.../RpcVersion.java`: Update version constant

### REST Client Implementation

The REST client in `hugegraph-common/rest/` uses OkHttp (not Jersey as older docs suggest):
- Switched from Jersey to OkHttp in recent versions
- Supports connection pooling, timeouts, interceptors
- See `AbstractRestClient.java` for base implementation

### RPC Version Note

hugegraph-rpc uses Sofa-RPC 5.7.6 which has known security issues. There's a TODO to upgrade to 5.12+:
- See comment in `hugegraph-rpc/pom.xml:65-66`
- This is a known technical debt item

### Checkstyle Configuration

Checkstyle runs during `validate` phase by default:
- Config: `style/checkstyle.xml`
- Failures block the build
- Skip with `-Dcheckstyle.skip=true` for quick iteration
- Always fix before committing
