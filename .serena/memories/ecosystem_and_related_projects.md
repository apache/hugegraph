# HugeGraph Ecosystem and Related Projects

## Core Repository (This Project)
**Repository**: apache/hugegraph (server)
**Purpose**: Core graph database engine (OLTP)

## Related Repositories

### 1. hugegraph-toolchain
**Repository**: https://github.com/apache/hugegraph-toolchain
**Components**:
- **hugegraph-loader**: Bulk data loading tool
- **hugegraph-hubble**: Web-based visualization dashboard
- **hugegraph-tools**: Command-line utilities
- **hugegraph-client**: Java client SDK

### 2. hugegraph-computer
**Repository**: https://github.com/apache/hugegraph-computer
**Purpose**: Distributed graph computing framework (OLAP)
**Features**: PageRank, Connected Components, Shortest Path, Community Detection

### 3. hugegraph-ai
**Repository**: https://github.com/apache/incubator-hugegraph-ai
**Purpose**: Graph AI, LLM, and Knowledge Graph integration
**Features**: Graph-enhanced LLM, KG construction, Graph RAG, NL to Gremlin/Cypher

### 4. hugegraph-website
**Repository**: https://github.com/apache/hugegraph-doc
**Purpose**: Official documentation and website
**URL**: https://hugegraph.apache.org/

## Integration Points

### Data Pipeline
```
Data Sources → hugegraph-loader → hugegraph-server
                                        ↓
                    ┌───────────────────┼───────────────────┐
                    ↓                   ↓                   ↓
            hugegraph-hubble    hugegraph-computer   hugegraph-ai
            (Visualization)      (Analytics)         (AI/ML)
```

## External Integrations

### Big Data Platforms
- Apache Flink, Apache Spark, HDFS

### Storage Backends
- RocksDB (default), HBase, Cassandra, ScyllaDB, MySQL, PostgreSQL

### Query Languages
- Gremlin (Apache TinkerPop), Cypher (OpenCypher), REST API

## Version Compatibility
- Server: 1.7.0
- TinkerPop: 3.5.1
- Java: 11+ required

## Use Cases
- Social networks, Fraud detection, Recommendation systems
- Knowledge graphs, Network analysis, Supply chain management
- IT operations, Bioinformatics
