# Apache HugeGraph Project Overview

## Project Purpose
Apache HugeGraph is a fast-speed, highly-scalable graph database supporting 10+ billion vertices/edges for OLTP workloads. Graduated from Apache Incubator (incubating branding removed).

## Key Capabilities
- Apache TinkerPop 3 compliant graph database
- Gremlin + OpenCypher query languages
- Schema metadata management (VertexLabel, EdgeLabel, PropertyKey, IndexLabel) with TTL update support
- Multi-type indexes (exact, range, complex conditions)
- Pluggable backend storage (RocksDB default, HStore distributed)
- GraphSpace multi-tenancy (standalone mode disables GraphSpaceAPI/ManagerAPI)
- Swagger UI for REST API documentation
- Integration with Flink/Spark/HDFS

## Technology Stack
- **Language**: Java 11+ (required)
- **Build**: Maven 3.5+
- **Graph Framework**: Apache TinkerPop 3.5.1
- **RPC**: gRPC + Protocol Buffers
- **API Docs**: Swagger (io.swagger.core.v3)
- **Storage**: RocksDB (default/embedded), HStore (distributed/production)
- **Legacy backends** (≤1.5.0): MySQL, PostgreSQL, Cassandra, ScyllaDB, HBase, Palo

## Version
- Current: 1.7.0 (`${revision}` property, Maven flatten plugin)
- License: Apache License 2.0
