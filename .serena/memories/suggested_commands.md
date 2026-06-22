# Suggested Development Commands

## Quick Reference

### Prerequisites Check
```bash
java -version        # Must be 11+
mvn -version         # Must be 3.5+
```

### Build Commands
```bash
# Full build without tests (fastest)
mvn clean install -DskipTests

# Full build with all tests
mvn clean install

# Build specific module (e.g., server)
mvn clean install -pl hugegraph-server -am -DskipTests

# Compile only
mvn clean compile -U -Dmaven.javadoc.skip=true -ntp

# Build distribution package
mvn clean package -DskipTests
# Output: install-dist/target/hugegraph-<version>.tar.gz
```

### Testing Commands
```bash
# Unit tests (memory backend)
mvn test -pl hugegraph-server/hugegraph-test -am -P unit-test,memory

# Core tests with specific backend
mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,memory
mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,rocksdb
mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,hbase

# API tests
mvn test -pl hugegraph-server/hugegraph-test -am -P api-test,rocksdb

# TinkerPop compliance tests (release branches)
mvn test -pl hugegraph-server/hugegraph-test -am -P tinkerpop-structure-test,memory
mvn test -pl hugegraph-server/hugegraph-test -am -P tinkerpop-process-test,memory

# Run single test class
mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,memory -Dtest=YourTestClass

# PD module tests (build struct first)
mvn install -pl hugegraph-struct -am -DskipTests
mvn test -pl hugegraph-pd/hg-pd-test -am

# Store module tests (build struct first)
mvn install -pl hugegraph-struct -am -DskipTests
mvn test -pl hugegraph-store/hg-store-test -am
```

### Code Quality & Validation
```bash
# License header check (Apache RAT)
mvn apache-rat:check -ntp

# Code style check (EditorConfig)
mvn editorconfig:check

# Compile with warnings
mvn clean compile -Dmaven.javadoc.skip=true
```

### Server Operations
```bash
# Scripts location: hugegraph-server/hugegraph-dist/src/assembly/static/bin/

# Initialize storage backend
bin/init-store.sh

# Start HugeGraph server
bin/start-hugegraph.sh

# Stop HugeGraph server
bin/stop-hugegraph.sh

# Start Gremlin console
bin/gremlin-console.sh

# Enable authentication
bin/enable-auth.sh

# Dump effective configuration
bin/dump-conf.sh

# Monitor server
bin/monitor-hugegraph.sh
```

### Git Operations (macOS/Darwin)
```bash
# View git log (avoid pager)
git --no-pager log -n 20 --oneline

# View git diff (avoid pager)
git --no-pager diff

# Check git status
git status
```

### Docker Commands (Test/Dev)
```bash
# Start HugeGraph in Docker (RocksDB backend)
docker run -itd --name=graph -p 8080:8080 hugegraph/hugegraph:1.5.0

# Start with preloaded sample graph
docker run -itd --name=graph -e PRELOAD=true -p 8080:8080 hugegraph/hugegraph:1.5.0
```

### Distributed Components Build (BETA)
```bash
# 1. Build hugegraph-struct (required dependency)
mvn install -pl hugegraph-struct -am -DskipTests

# 2. Build hugegraph-pd (Placement Driver)
mvn clean package -pl hugegraph-pd -am -DskipTests

# 3. Build hugegraph-store (distributed storage)
mvn clean package -pl hugegraph-store -am -DskipTests

# 4. Build hugegraph-server with HStore backend
mvn clean package -pl hugegraph-server -am -DskipTests
```
