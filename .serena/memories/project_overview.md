# Apache HugeGraph Project Overview

## Project Purpose
Apache HugeGraph is a fast-speed and highly-scalable graph database that supports billions of vertices and edges (10+ billion scale). It is designed for OLTP workloads with excellent performance and scalability.

## Key Capabilities
- Graph database compliant with Apache TinkerPop 3 framework
- Supports both Gremlin and Cypher query languages
- Schema metadata management (VertexLabel, EdgeLabel, PropertyKey, IndexLabel)
- Multi-type indexes (exact, range, complex conditions)
- Pluggable backend storage architecture
- Integration with big data platforms (Flink/Spark/HDFS)
- Complete graph ecosystem (computing, visualization, AI/ML)

## Technology Stack
- **Language**: Java 11+ (required)
- **Build Tool**: Apache Maven 3.5+ (required)
- **Graph Framework**: Apache TinkerPop 3.5.1
- **RPC**: gRPC with Protocol Buffers
- **Storage Backends**: 
  - RocksDB (default, embedded)
  - HStore (distributed, production)
  - Legacy (≤1.5.0): MySQL, PostgreSQL, Cassandra, ScyllaDB, HBase, Palo

## Project Version
- Current version: 1.7.0 (managed via `${revision}` property)
- Version management uses Maven flatten plugin for CI-friendly versioning

## License
- Apache License 2.0
- All code must include Apache license headers
- Third-party dependencies require proper license documentation

## Repository Structure
This is a multi-module Maven project
