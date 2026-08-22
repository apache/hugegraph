# AGENTS.md

This file provides guidance to an AI coding tool when working with code in this repository.

## Module Overview

**hugegraph-struct** is a foundational data structures module that defines the core abstractions shared across HugeGraph distributed components. This module **must be built before hugegraph-pd and hugegraph-store** as they depend on its structure definitions.

**Key Responsibilities**:
- Schema element definitions (VertexLabel, EdgeLabel, PropertyKey, IndexLabel)
- Graph element structures (BaseVertex, BaseEdge, BaseProperty)
- Binary serialization/deserialization for efficient storage and RPC
- Type system definitions (HugeType enum, data types, ID strategies)
- Query abstractions (Query, ConditionQuery, IdQuery, Aggregate)
- Chinese text analyzers (multiple implementations: Jieba, IK, HanLP, etc.)
- Authentication utilities (JWT token generation, constants)

## Build Commands

### Building This Module

```bash
# From hugegraph-struct directory
mvn clean install -DskipTests

# Build with tests (if any exist in future)
mvn clean install

# From parent directory (hugegraph root)
mvn install -pl hugegraph-struct -am -DskipTests
```

### Dependency Chain

This module is a **critical dependency** for distributed components:

```bash
# Correct build order for distributed components:
# 1. Build hugegraph-struct first
mvn install -pl hugegraph-struct -am -DskipTests

# 2. Then build PD
mvn clean package -pl hugegraph-pd -am -DskipTests

# 3. Then build Store
mvn clean package -pl hugegraph-store -am -DskipTests
```

## Code Architecture

### Package Structure

```
org.apache.hugegraph/
├── struct/schema/          # Schema element definitions
│   ├── SchemaElement       # Base class for all schema types
│   ├── VertexLabel         # Vertex label definitions
│   ├── EdgeLabel           # Edge label definitions
│   ├── PropertyKey         # Property key definitions
│   ├── IndexLabel          # Index label definitions
│   └── builder/            # Builder pattern implementations
├── structure/              # Graph element structures
│   ├── BaseElement         # Base class for vertices/edges
│   ├── BaseVertex          # Vertex implementation
│   ├── BaseEdge            # Edge implementation
│   ├── BaseProperty        # Property implementation
│   └── builder/            # Element builders
├── type/                   # Type system
│   ├── HugeType            # Enum for all graph types (VERTEX, EDGE, etc.)
│   ├── GraphType           # Type interface
│   ├── Namifiable          # Name-based types
│   ├── Idfiable            # ID-based types
│   └── define/             # Type definitions (DataType, IdStrategy, etc.)
├── id/                     # ID generation and management
│   ├── Id                  # ID interface
│   ├── IdGenerator         # ID generation utilities
│   ├── EdgeId              # Edge-specific ID handling
│   └── IdUtil              # ID utility methods
├── serializer/             # Binary serialization
│   ├── BytesBuffer         # Buffer for binary I/O
│   ├── BinaryElementSerializer    # Element serialization
│   └── DirectBinarySerializer     # Direct binary access
├── query/                  # Query abstractions
│   ├── Query               # Base query interface
│   ├── ConditionQuery      # Conditional queries
│   ├── IdQuery             # ID-based queries
│   ├── Condition           # Query conditions
│   └── Aggregate           # Aggregation queries
├── analyzer/               # Text analyzers (Chinese NLP)
│   ├── Analyzer            # Base analyzer interface
│   ├── AnalyzerFactory     # Factory for creating analyzers
│   ├── IKAnalyzer          # IK Chinese word segmentation
│   ├── JiebaAnalyzer       # Jieba segmentation
│   ├── HanLPAnalyzer       # HanLP NLP
│   ├── AnsjAnalyzer        # Ansj segmentation
│   ├── WordAnalyzer        # Word-based analysis
│   ├── JcsegAnalyzer       # Jcseg segmentation
│   ├── MMSeg4JAnalyzer     # MMSeg4J segmentation
│   └── SmartCNAnalyzer     # Lucene SmartCN
├── auth/                   # Authentication utilities
│   ├── TokenGenerator      # JWT token generation
│   └── AuthConstant        # Auth constants
├── backend/                # Backend abstractions
│   ├── BinaryId            # Binary ID representation
│   ├── BackendColumn       # Column abstraction
│   └── Shard               # Shard information
├── options/                # Configuration options
│   ├── CoreOptions         # Core configuration
│   └── AuthOptions         # Auth configuration
├── util/                   # Utilities
│   ├── StringEncoding      # String encoding utilities
│   ├── GraphUtils          # Graph utility methods
│   ├── LZ4Util             # LZ4 compression
│   ├── Blob                # Binary blob handling
│   └── collection/         # Collection utilities (IdSet, CollectionFactory)
└── exception/              # Exception hierarchy
    ├── HugeException       # Base exception
    ├── BackendException    # Backend errors
    ├── NotSupportException # Unsupported operations
    ├── NotFoundException   # Not found errors
    └── NotAllowException   # Permission errors
```

### Key Architectural Concepts

#### 1. Two-Layer Schema System

The module defines a dual schema hierarchy:

- **`struct.schema.*`**: Schema element definitions (VertexLabel, EdgeLabel, etc.) - these are *metadata* about the graph structure
- **`structure.*`**: Actual graph elements (BaseVertex, BaseEdge, etc.) - these are *data* instances

The schema layer defines the "blueprint" while the structure layer implements the "instances".

#### 2. Type System

The `HugeType` enum (type/HugeType.java) defines all possible types:
- Schema types: `VERTEX_LABEL`, `EDGE_LABEL`, `PROPERTY_KEY`, `INDEX_LABEL`
- Data types: `VERTEX`, `EDGE`, `PROPERTY`, `AGGR_PROPERTY_V`, `AGGR_PROPERTY_E`
- Special types: `META`, `COUNTER`, `TASK`, `OLAP`, `INDEX`

#### 3. ID Management

IDs are critical for distributed systems:
- `Id` interface provides abstraction over different ID types
- `IdGenerator` creates IDs based on strategy (AUTO_INCREMENT, PRIMARY_KEY, CUSTOMIZE)
- `EdgeId` uses special encoding: source vertex ID + edge label ID + sort values + target vertex ID
- Binary serialization optimizes ID storage

#### 4. Binary Serialization

`BytesBuffer` and serializers enable:
- Efficient storage in RocksDB and other backends
- Fast gRPC message passing between PD/Store/Server
- Compact on-disk and in-memory representation

#### 5. Query Abstraction

Query classes provide backend-agnostic query building:
- `Query`: Base interface with limit, offset, ordering
- `ConditionQuery`: Supports conditions (EQ, GT, LT, IN, CONTAINS, etc.)
- `IdQuery`: Direct ID-based lookups
- `Aggregate`: Aggregation operations (SUM, MAX, MIN, AVG)

## Dependencies

### Critical Dependencies

- **hg-pd-client** (${project.version}): PD client for metadata coordination
- **hugegraph-common** (${project.version}): Shared utilities
- **Apache TinkerPop 3.8.1**: Graph computing framework
- **Guava 25.1-jre**: Google utilities
- **Eclipse Collections 10.4.0**: High-performance collections
- **fastutil 8.1.0**: Fast primitive collections

### Text Analysis Dependencies

Multiple Chinese NLP libraries for different use cases:
- **jieba-analysis 1.0.2**: Popular Chinese word segmentation
- **IKAnalyzer 2012_u6**: IK word segmentation
- **HanLP portable-1.5.0**: Natural language processing
- **Ansj 5.1.6**: Ansj segmentation
- **Word 1.3**: APDPlat word segmentation
- **Jcseg 2.2.0**: Jcseg segmentation
- **mmseg4j-core 1.10.0**: MMSeg4J segmentation
- **lucene-analyzers-smartcn 7.4.0**: Lucene SmartCN

### Security Dependencies

- **jjwt-api/impl/jackson 0.11.2**: JWT token handling
- **jbcrypt 0.4**: Password hashing

## Development Notes

### When Modifying This Module

1. **Understand the impact**: Changes here affect hugegraph-pd, hugegraph-store, and hugegraph-server
2. **Rebuild dependent modules**: After modifying, rebuild PD and Store modules
3. **Binary compatibility**: Serialization changes require careful version migration
4. **ID changes**: Modifying ID generation can break existing data

### Working with Schema Elements

When adding or modifying schema elements in `struct/schema/`:
- Extend `SchemaElement` base class
- Implement required interfaces (`Namifiable`, `Typifiable`)
- Add corresponding `HugeType` enum value if needed
- Update serialization logic in `BinaryElementSerializer`
- Verify schema builder patterns in `struct/schema/builder/`

### Working with Binary Serialization

When modifying serialization:
- Changes to `BytesBuffer` format require version migration
- Test with all backends (RocksDB, HStore)
- Ensure backward compatibility or provide migration path
- Update both write and read paths consistently

### Adding Text Analyzers

To add a new text analyzer:
1. Implement the `Analyzer` interface in `analyzer/`
2. Register in `AnalyzerFactory`
3. Add dependency to pom.xml
4. Test with Chinese text queries

## Common Patterns

### Creating Schema Elements

```java
// Schema elements use builders
PropertyKey propertyKey = schema.propertyKey("name")
                                .asText()
                                .valueSingle()
                                .create();
```

### ID Generation

```java
// Generate IDs based on strategy
Id id = IdGenerator.of(value, IdType.LONG);
Id edgeId = EdgeId.parse(sourceId, direction, label, sortValues, targetId);
```

### Binary Serialization

```java
// Write to buffer
BytesBuffer buffer = BytesBuffer.allocate(size);
buffer.writeId(id);
buffer.writeString(name);

// Read from buffer
Id id = buffer.readId();
String name = buffer.readString();
```

## Cross-Module References

This module is referenced by:
- **hugegraph-pd**: Uses schema definitions for metadata management
- **hugegraph-store**: Uses serialization for storage and RPC
- **hugegraph-server/hugegraph-core**: Uses all abstractions for graph operations
- **hugegraph-server/hugegraph-api**: Uses structures for REST API serialization

## License and Compliance

This module follows Apache Software Foundation guidelines:
- All files must have Apache 2.0 license headers
- Third-party dependencies require license documentation in `install-dist/release-docs/licenses/`
- Excluded from Apache RAT: None (all source files checked)
