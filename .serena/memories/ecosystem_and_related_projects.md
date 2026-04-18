# HugeGraph Ecosystem and Related Projects

## This Repo: apache/hugegraph (server, OLTP)

## Ecosystem
| Repo | Purpose |
|------|---------|
| hugegraph-toolchain | Loader, Hubble (visualization), Tools CLI, Java Client |
| hugegraph-computer | OLAP: PageRank, Connected Components, Shortest Path |
| incubator-hugegraph-ai | Graph RAG, KG construction, NL→Gremlin/Cypher |
| hugegraph-doc | Docs & website (hugegraph.apache.org) |

## Data Flow
```
Sources → hugegraph-loader → hugegraph-server → Hubble / Computer / AI
```

## Integrations
- Big Data: Flink, Spark, HDFS
- Queries: Gremlin (TinkerPop 3.5.1), OpenCypher, REST API + Swagger UI
- Storage: RocksDB (default), HStore (distributed)

## Version: Server 1.7.0, TinkerPop 3.5.1, Java 11+
