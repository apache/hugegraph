# Suggested Development Commands

## Build
```bash
mvn clean install -DskipTests                           # Full build
mvn clean install -pl hugegraph-server -am -DskipTests  # Server only
mvn clean compile -U -Dmaven.javadoc.skip=true -ntp     # Compile only
mvn clean package -DskipTests                           # Distribution → install-dist/target/
```

## Test
```bash
# Server tests (memory/rocksdb/hbase backends)
mvn test -pl hugegraph-server/hugegraph-test -am -P unit-test
mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,memory
mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,rocksdb
mvn test -pl hugegraph-server/hugegraph-test -am -P api-test,rocksdb

# Single test class
mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,memory -Dtest=YourTestClass

# TinkerPop compliance (release/test branches only)
mvn test -pl hugegraph-server/hugegraph-test -am -P tinkerpop-structure-test,memory

# PD/Store (build struct first)
mvn install -pl hugegraph-struct -am -DskipTests
mvn test -pl hugegraph-pd/hg-pd-test -am
mvn test -pl hugegraph-store/hg-store-test -am
```

## Validation
```bash
mvn apache-rat:check -ntp        # License headers
mvn editorconfig:check           # Code style (.editorconfig)
mvn checkstyle:check             # Code style (style/checkstyle.xml)
mvn clean compile -Dmaven.javadoc.skip=true  # Compile warnings
```

## Server Ops
Scripts in `hugegraph-server/hugegraph-dist/src/assembly/static/bin/` (or extracted distribution `bin/`):
```bash
bin/init-store.sh && bin/start-hugegraph.sh  # Init + start
bin/stop-hugegraph.sh                         # Stop
bin/enable-auth.sh                            # Enable auth
```

## Docker
```bash
cd docker && docker compose up -d                                          # Single-node (bridge network)
cd docker && docker compose -f docker-compose-3pd-3store-3server.yml up -d # Cluster
```

## Distributed Build (BETA)
```bash
mvn install -pl hugegraph-struct -am -DskipTests  # 1. Struct first
mvn clean package -pl hugegraph-pd -am -DskipTests  # 2. PD
mvn clean package -pl hugegraph-store -am -DskipTests  # 3. Store
```
