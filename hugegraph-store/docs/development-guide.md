# Development Guide

Comprehensive guide for developing, testing, and contributing to HugeGraph Store.

## Table of Contents

- [Development Environment Setup](#development-environment-setup)
- [Module Architecture](#module-architecture)
- [Build and Test](#build-and-test)
- [gRPC Development](#grpc-development)
- [Debugging](#debugging)
- [Contribution Guidelines](#contribution-guidelines)

---

## Development Environment Setup

### Prerequisites

**Required**:
- Java: 11 or higher (OpenJDK or Oracle JDK)
- Maven: 3.5 or higher
- Git: Latest version
- IDE: IntelliJ IDEA (recommended) or Eclipse

**Optional** (for testing):
- Docker: For containerized testing
- grpcurl: For gRPC API testing
- Prometheus/Grafana: For metrics testing

### Clone Repository

```bash
# Clone HugeGraph repository
git clone https://github.com/apache/hugegraph.git
cd hugegraph

# Checkout development branch
git checkout 1.7-rebase
```

### IDE Setup (IntelliJ IDEA)

**Import Project**:
1. File → Open → Select `hugegraph` directory
2. IntelliJ detects Maven project → Click "Import"
3. Wait for Maven to download dependencies

**Code Style**:
```bash
# Import code style
# File → Settings → Editor → Code Style → Java
# Import Scheme → hugegraph-style.xml
```

**Run Configuration**:
1. Run → Edit Configurations
2. Add new "Application" configuration:
   - Main class: `org.apache.hugegraph.store.node.StoreNodeApplication`
   - VM options: `-Xms4g -Xmx4g -Dconfig.file=conf/application.yml`
   - Working directory: `hugegraph-store/hg-store-dist/target/apache-hugegraph-store-incubating-1.7.0`
   - Use classpath of module: `hg-store-node`

### Build from Source

**Build entire project**:
```bash
# From hugegraph root
mvn clean install -DskipTests
```

**Build Store module only**:
```bash
# Build hugegraph-struct first (required dependency)
mvn install -pl hugegraph-struct -am -DskipTests

# Build Store
cd hugegraph-store
mvn clean install -DskipTests
```

**Build with tests**:
```bash
mvn clean install
```

---

## Module Architecture

### Module Dependency Graph

```
hugegraph-struct (external dependency)
    ↓
hg-store-common
    ↓
    ├─→ hg-store-grpc (proto definitions)
    ├─→ hg-store-rocksdb
    ↓
hg-store-core
    ↓
    ├─→ hg-store-client
    ├─→ hg-store-node
    ↓
    ├─→ hg-store-cli
    ├─→ hg-store-dist
    └─→ hg-store-test
```

### Module Details

#### hg-store-common

**Location**: `hugegraph-store/hg-store-common`

**Purpose**: Shared utilities and query abstractions

**Key Packages**:
- `buffer`: ByteBuffer utilities
- `constant`: Constants and enums
- `query`: Query abstraction classes
  - `Condition`: Filter conditions
  - `Aggregate`: Aggregation types
  - `QueryCondition`: Query parameters
- `term`: Term matching utilities
- `util`: General utilities

**Adding New Utility**:
1. Create class in appropriate package (e.g., `util`)
2. Add Javadoc comments
3. Add unit tests in `hg-store-test`

#### hg-store-grpc

**Location**: `hugegraph-store/hg-store-grpc`

**Purpose**: gRPC protocol definitions

**Structure**:
```
hg-store-grpc/
├── src/main/proto/          # Protocol definitions
│   ├── store_session.proto
│   ├── query.proto
│   ├── graphpb.proto
│   ├── store_state.proto
│   ├── store_stream_meta.proto
│   ├── healthy.proto
│   └── store_common.proto
└── target/generated-sources/  # Generated Java code (git-ignored)
```

**Generated Code**: Excluded from source control and Apache RAT checks

#### hg-store-core

**Location**: `hugegraph-store/hg-store-core`

**Purpose**: Core storage engine logic

**Key Classes**:

**`HgStoreEngine.java`** (~500 lines):
- Singleton per Store node
- Manages all `PartitionEngine` instances
- Coordinates with PD
- Entry point for partition lifecycle

**`PartitionEngine.java`** (~300 lines):
- One instance per partition replica
- Wraps Raft node
- Delegates to `BusinessHandler`

**`HgStoreStateMachine.java`** (~400 lines):
- Implements JRaft's `StateMachine`
- Applies Raft log entries
- Handles snapshot save/load

**`BusinessHandler.java`** (interface) / `BusinessHandlerImpl.java`** (~800 lines):
- Implements data operations (put, get, delete, scan)
- Processes queries with filters and aggregations

**Key Packages**:
- `business/`: Business logic layer
- `meta/`: Metadata management
- `raft/`: Raft integration
- `pd/`: PD client and integration
- `cmd/`: Command processing
- `snapshot/`: Snapshot management

#### hg-store-client

**Location**: `hugegraph-store/hg-store-client`

**Purpose**: Java client library

**Key Classes**:
- `HgStoreClient`: Main client interface
- `HgStoreSession`: Session-based operations
- `HgStoreNodeManager`: Connection management
- `HgStoreQuery`: Query builder

**Usage**: See [Integration Guide](integration-guide.md)

#### hg-store-node

**Location**: `hugegraph-store/hg-store-node`

**Purpose**: Store node server

**Key Classes**:
- `StoreNodeApplication`: Spring Boot main class
- `HgStoreSessionService`: gRPC service implementation
- `HgStoreQueryService`: Query service implementation

**Start Server**:
```bash
cd hugegraph-store/hg-store-dist/target/apache-hugegraph-store-incubating-1.7.0
bin/start-hugegraph-store.sh
```

If you want to run store module in debug mode
Directly run HgStoreNodeService in your IDE (ensure PD is on).
---

## Build and Test

### Build Commands

**Clean build**:
```bash
mvn clean install -DskipTests
```

**Compile only**:
```bash
mvn compile
```

**Package distribution**:
```bash
mvn clean package -DskipTests

# Output: hg-store-dist/target/apache-hugegraph-store-incubating-<version>.tar.gz
```

**Regenerate gRPC stubs** (after modifying `.proto` files):
```bash
cd hugegraph-store/hg-store-grpc
mvn clean compile

# Generated files: target/generated-sources/protobuf/
```

### Testing

#### Test Profiles

Store tests use Maven profiles (all active by default):

```xml
<profile>
    <id>store-client-test</id>
    <activation><activeByDefault>true</activeByDefault></activation>
</profile>
<profile>
    <id>store-core-test</id>
    <activation><activeByDefault>true</activeByDefault></activation>
</profile>
<profile>
    <id>store-common-test</id>
    <activation><activeByDefault>true</activeByDefault></activation>
</profile>
<profile>
    <id>store-rocksdb-test</id>
    <activation><activeByDefault>true</activeByDefault></activation>
</profile>
<profile>
    <id>store-server-test</id>
    <activation><activeByDefault>true</activeByDefault></activation>
</profile>
<profile>
    <id>store-raftcore-test</id>
    <activation><activeByDefault>true</activeByDefault></activation>
</profile>
```

#### Run Tests

**All tests**:
```bash
cd hugegraph-store
mvn test
```

**Specific profile**:
```bash
mvn test -P store-core-test
```

**Specific test class**:
```bash
mvn test -Dtest=HgStoreEngineTest
```

**Specific test method**:
```bash
mvn test -Dtest=HgStoreEngineTest#testPartitionCreation
```

**From IntelliJ**:
- Right-click test class → Run 'TestClassName'
- Right-click test method → Run 'testMethodName'

#### Test Structure

**Location**: `hugegraph-store/hg-store-test/src/main/java` (non-standard location)

**Packages**:
- `client/`: Client library tests
- `common/`: Common utilities tests
- `core/`: Core storage tests
  - `raft/`: Raft tests
  - `snapshot/`: Snapshot tests
  - `store/`: Storage engine tests
- `meta/`: Metadata tests
- `raftcore/`: Raft core tests
- `rocksdb/`: RocksDB tests
- `service/`: Service tests

**Base Test Class**: `BaseTest.java`
- Provides common test utilities
- Sets up test environment

#### Writing Tests

**Example Test Class**:
```java
package org.apache.hugegraph.store.core;

import org.apache.hugegraph.store.BaseTest;
import org.junit.Test;

import static org.junit.Assert.*;

public class HgStoreEngineTest extends BaseTest {

    @Test
    public void testEngineCreation() {
        // Arrange
        HgStoreEngineConfig config = HgStoreEngineConfig.builder()
            .dataPath("./test-data")
            .build();

        // Act
        HgStoreEngine engine = HgStoreEngine.getInstance();
        engine.init(config);

        // Assert
        assertNotNull(engine);
        assertTrue(engine.isInitialized());

        // Cleanup
        engine.shutdown();
    }
}
```

**Integration Test Example**:
```java
@Test
public void testRaftConsensus() throws Exception {
    // Setup 3-node Raft group
    List<PartitionEngine> engines = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
        PartitionEngine engine = createPartitionEngine(i);
        engines.add(engine);
        engine.start();
    }

    // Wait for leader election
    Thread.sleep(2000);

    // Perform write on leader
    PartitionEngine leader = findLeader(engines);
    leader.put("key1".getBytes(), "value1".getBytes());

    // Wait for replication
    Thread.sleep(1000);

    // Verify on all nodes
    for (PartitionEngine engine : engines) {
        byte[] value = engine.get("key1".getBytes());
        assertEquals("value1", new String(value));
    }

    // Cleanup
    for (PartitionEngine engine : engines) {
        engine.stop();
    }
}
```

### Code Coverage

**Generate Coverage Report**:
```bash
mvn clean test jacoco:report

# Report: hg-store-test/target/site/jacoco/index.html
```

**View in Browser**:
```bash
open hg-store-test/target/site/jacoco/index.html
```

---

## gRPC Development

### Adding a New gRPC Service

#### Step 1: Define Protocol

Create or edit `.proto` file in `hg-store-grpc/src/main/proto/`:

**Example**: `my_service.proto`
```protobuf
syntax = "proto3";

package org.apache.hugegraph.store.grpc;

import "store_common.proto";

service MyService {
  rpc MyOperation(MyRequest) returns (MyResponse);
}

message MyRequest {
  Header header = 1;
  string key = 2;
}

message MyResponse {
  bytes value = 1;
}
```

#### Step 2: Generate Java Stubs

```bash
cd hg-store-grpc
mvn clean compile

# Generated classes:
# - MyServiceGrpc.java (service stub)
# - MyRequest.java
# - MyResponse.java
```

#### Step 3: Implement Service

Create service implementation in `hg-store-node/src/main/java/.../service/`:

```java
package org.apache.hugegraph.store.node.service;

import io.grpc.stub.StreamObserver;
import org.apache.hugegraph.store.grpc.MyServiceGrpc;
import org.apache.hugegraph.store.grpc.MyRequest;
import org.apache.hugegraph.store.grpc.MyResponse;

public class MyServiceImpl extends MyServiceGrpc.MyServiceImplBase {

    @Override
    public void myOperation(MyRequest request, StreamObserver<MyResponse> responseObserver) {
        try {
            // Extract request parameters
            String key = request.getKey();

            // Perform operation (delegate to HgStoreEngine)
            byte[] value = performOperation(key);

            // Build response
            MyResponse response = MyResponse.newBuilder()
                .setValue(ByteString.copyFrom(value))
                .build();

            // Send response
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    private byte[] performOperation(String key) {
        // Implementation
        return new byte[0];
    }
}
```

#### Step 4: Register Service

In `StoreNodeApplication.java`:
```java
@Bean
public Server grpcServer() {
    return ServerBuilder.forPort(grpcPort)
        .addService(new HgStoreSessionService())
        .addService(new HgStoreQueryService())
        .addService(new MyServiceImpl())  // Add new service
        .build();
}
```

#### Step 5: Test Service

**Using grpcurl**:
```bash
# List services
grpcurl -plaintext localhost:8500 list

# Call method
grpcurl -plaintext -d '{"key": "test"}' localhost:8500 org.apache.hugegraph.store.grpc.MyService/MyOperation
```

**Unit Test**:
```java
@Test
public void testMyService() {
    // Setup gRPC channel
    ManagedChannel channel = ManagedChannelBuilder
        .forAddress("localhost", 8500)
        .usePlaintext()
        .build();

    // Create stub
    MyServiceGrpc.MyServiceBlockingStub stub = MyServiceGrpc.newBlockingStub(channel);

    // Build request
    MyRequest request = MyRequest.newBuilder()
        .setKey("test")
        .build();

    // Call service
    MyResponse response = stub.myOperation(request);

    // Verify
    assertNotNull(response.getValue());

    // Cleanup
    channel.shutdown();
}
```

---

## Debugging

### Local Debugging

**Debug Store Node in IntelliJ**:
1. Set breakpoints in source code
2. Run → Debug 'StoreNodeApplication'
3. Debugger pauses at breakpoints

**Debug with Remote Store**:
1. Start Store with debug port:
   ```bash
   # Edit start-hugegraph-store.sh
   JAVA_OPTS="$JAVA_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
   bin/start-hugegraph-store.sh
   ```

2. Attach debugger in IntelliJ:
   - Run → Edit Configurations → Add "Remote JVM Debug"
   - Host: localhost, Port: 5005
   - Run → Debug 'Remote Store'

### Logging

**Log Configuration**: `hg-store-dist/src/assembly/static/conf/log4j2.xml`

**Enable Debug Logging**:
```xml
<!-- Store core -->
<Logger name="org.apache.hugegraph.store" level="DEBUG"/>

<!-- Raft -->
<Logger name="com.alipay.sofa.jraft" level="DEBUG"/>

<!-- RocksDB -->
<Logger name="org.rocksdb" level="DEBUG"/>

<!-- gRPC -->
<Logger name="io.grpc" level="DEBUG"/>
```

**Restart to apply**:
```bash
bin/restart-hugegraph-store.sh
```

**View Logs**:
```bash
tail -f logs/hugegraph-store.log
tail -f logs/hugegraph-store.log | grep ERROR
```

### Debugging Raft

**Check Raft State**:
```bash
# Raft logs location
ls -lh storage/raft/partition-*/log/

# Raft snapshots
ls -lh storage/raft/partition-*/snapshot/
```

**Raft Metrics** (in code):
```java
// Get Raft node status
RaftNode node = partitionEngine.getRaftNode();
NodeStatus status = node.getNodeStatus();
System.out.println("Term: " + status.getTerm());
System.out.println("State: " + status.getState());  // Leader, Follower, Candidate
System.out.println("Peers: " + status.getPeers());
```

**Enable Raft Logging**:
```xml
<Logger name="com.alipay.sofa.jraft" level="DEBUG"/>
```

### Debugging RocksDB

**RocksDB Statistics**:
```java
// In code
RocksDB db = rocksDBSession.getDb();
String stats = db.getProperty("rocksdb.stats");
System.out.println(stats);
```

**Dump RocksDB Data** (for inspection):
```bash
# Using ldb tool (included with RocksDB)
ldb --db=storage/rocksdb scan --max_keys=100
```

### Performance Profiling

**JVM Profiling** (using async-profiler):
```bash
# Download async-profiler
wget https://github.com/jvm-profiling-tools/async-profiler/releases/download/v2.9/async-profiler-2.9-linux-x64.tar.gz
tar -xzf async-profiler-2.9-linux-x64.tar.gz

# Start profiling
./profiler.sh -d 60 -f flamegraph.html $(pgrep -f hugegraph-store)

# View flamegraph
open flamegraph.html
```

**Memory Profiling**:
```bash
# Heap dump
jmap -dump:format=b,file=heap.bin $(pgrep -f hugegraph-store)

# Analyze with VisualVM or Eclipse MAT
```

---

## Contribution Guidelines

### Code Style

**Java**:
- Follow Apache HugeGraph code style (import `hugegraph-style.xml`)
- Use 4 spaces for indentation (no tabs)
- Max line length: 120 characters
- Braces on same line (K&R style)

**Example**:
```java
public class MyClass {
    private static final Logger LOG = LoggerFactory.getLogger(MyClass.class);

    public void myMethod(String param) {
        if (param == null) {
            throw new IllegalArgumentException("param cannot be null");
        }
        // Implementation
    }
}
```

### Commit Messages

**Format**:
```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types**:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `refactor`: Code refactoring
- `test`: Test additions or changes
- `chore`: Build or tooling changes

**Example**:
```
feat(store): add query aggregation pushdown

Implement COUNT, SUM, MIN, MAX, AVG aggregations at Store level
to reduce network traffic and improve query performance.

Closes #1234
```

### Pull Request Process

1. **Fork and Clone**:
   ```bash
   # Fork on GitHub
   git clone https://github.com/YOUR_USERNAME/hugegraph.git
   cd hugegraph
   git remote add upstream https://github.com/apache/hugegraph.git
   ```

2. **Create Branch**:
   ```bash
   git checkout -b feature-my-feature
   ```

3. **Develop and Test**:
   ```bash
   # Make changes
   # Add tests
   mvn clean install  # Ensure all tests pass
   ```

4. **Check Code Quality**:
   ```bash
   # License header check
   mvn apache-rat:check

   # Code style check
   mvn editorconfig:check
   ```

5. **Commit**:
   ```bash
   git add .
   git commit -m "feat(store): add new feature"
   ```

6. **Push and Create PR**:
   ```bash
   git push origin feature-my-feature
   # Create PR on GitHub
   ```

7. **Code Review**:
   - Address review comments
   - Update PR with fixes
   - Request re-review

8. **Merge**:
   - Maintainers merge after approval

### License and Dependencies

**Adding Dependencies**:

When adding third-party dependencies:
1. Add to `pom.xml`
2. Add license file to `install-dist/release-docs/licenses/`
3. Update `install-dist/release-docs/LICENSE`
4. If upstream has NOTICE, update `install-dist/release-docs/NOTICE`
5. Update `install-dist/scripts/dependency/known-dependencies.txt`

**Run Dependency Check**:
```bash
cd install-dist/scripts/dependency
./regenerate_known_dependencies.sh
```

### Documentation

**When to Update Docs**:
- New feature: Add usage examples
- API changes: Update API reference
- Configuration changes: Update configuration guide
- Bug fixes: Update troubleshooting section

**Documentation Location**:
- Main README: `hugegraph-store/README.md`
- Detailed docs: `hugegraph-store/docs/`

---

## Additional Resources

**Official Documentation**:
- HugeGraph Docs: https://hugegraph.apache.org/docs/
- Apache TinkerPop: https://tinkerpop.apache.org/docs/

**Community**:
- Mailing List: dev@hugegraph.apache.org
- GitHub Issues: https://github.com/apache/hugegraph/issues
- Slack: (link in project README)

**Related Projects**:
- SOFA-JRaft: https://github.com/sofastack/sofa-jraft
- RocksDB: https://rocksdb.org/
- gRPC: https://grpc.io/docs/languages/java/

---

For operational procedures, see [Operations Guide](operations-guide.md).

For production best practices, see [Best Practices](best-practices.md).
