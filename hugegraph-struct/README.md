# HugeGraph-Struct

### Overview

**hugegraph-struct** is a foundational data structures module that defines the core abstractions and type definitions shared across HugeGraph's distributed components. It serves as the "data contract layer" enabling type-safe communication between hugegraph-pd (Placement Driver), hugegraph-store (distributed storage), and hugegraph-server (graph engine).

**Key Characteristics**:
- Pure data structure definitions without business logic
- Lightweight and stateless (no `HugeGraph` instance dependencies)
- Shared type system for distributed RPC communication
- Binary serialization for efficient storage and network transmission

### Why hugegraph-struct?

#### The Problem

Originally, all data structures and graph engine logic resided in `hugegraph-server/hugegraph-core`. As HugeGraph evolved toward a distributed architecture, this created several challenges:

1. **Tight Coupling**: PD and Store components needed schema definitions but not the entire graph engine
2. **Circular Dependencies**: Distributed components couldn't share types without pulling in heavy dependencies
3. **Build Inefficiency**: Changes to core required rebuilding all dependent modules
4. **Large Dependencies**: PD/Store had to depend on Jersey, JRaft, K8s client, and other server-specific libraries

#### The Solution

We extracted **stateless data structures** from `hugegraph-core` into a separate `hugegraph-struct` module:

```
Before (Monolithic):
hugegraph-server/hugegraph-core (everything together)
  ├─ Data structures (schema, types, IDs)
  ├─ Graph engine (traversal, optimization)
  ├─ Transactions (GraphTransaction, SchemaTransaction)
  ├─ Storage backends (memory, raft, cache)
  └─ Business logic (jobs, tasks, auth)

After (Modular):
hugegraph-struct (shared foundation)
  ├─ Schema definitions (VertexLabel, EdgeLabel, PropertyKey, IndexLabel)
  ├─ Type system (HugeType, DataType, IdStrategy)
  ├─ Data structures (BaseVertex, BaseEdge, BaseProperty)
  ├─ Serialization (BytesBuffer, BinarySerializer)
  ├─ Query abstractions (Query, ConditionQuery, Aggregate)
  └─ Utilities (ID generation, text analyzers, exceptions)

hugegraph-core (graph engine only)
  ├─ Depends on hugegraph-struct
  ├─ Implements graph engine logic
  ├─ Manages transactions and storage
  └─ Provides TinkerPop API
```

### Module Responsibilities

| Module | Purpose | Dependencies |
|--------|---------|--------------|
| **hugegraph-struct** | Shared data structures, type definitions, serialization | Minimal (Guava, TinkerPop, serialization libs) |
| **hugegraph-core** | Graph engine, traversal, transactions, storage abstraction | hugegraph-struct + heavy libs (Jersey, JRaft, K8s) |
| **hugegraph-pd** | Metadata coordination, service discovery | hugegraph-struct only |
| **hugegraph-store** | Distributed storage with Raft | hugegraph-struct only |

### Dependency Architecture

```
                    hugegraph-struct (foundational)
                           ↑
        ┌──────────────────┼──────────────────┐
        │                  │                  │
   hugegraph-pd      hugegraph-store    hugegraph-core
        │                  │                  │
        └──────────────────┼──────────────────┘
                           ↓
                   hugegraph-server (REST API)
```

**Build Order**:
```bash
# 1. Build struct first (required dependency)
mvn install -pl hugegraph-struct -am -DskipTests

# 2. Then build dependent modules
mvn install -pl hugegraph-pd -am -DskipTests
mvn install -pl hugegraph-store -am -DskipTests
mvn install -pl hugegraph-server -am -DskipTests
```

### Migration Plan

**Current Status (Transition Period)**:

Both `hugegraph-struct` and `hugegraph-core` contain similar data structures for backward compatibility. This is a **temporary state** during the migration period.

**Future Direction**:

- ✅ **hugegraph-struct**: Will become the **single source of truth** for all data structure definitions
- ⚠️ **hugegraph-core**: Data structure definitions will be **gradually removed** and replaced with references to hugegraph-struct
- 🎯 **End Goal**: hugegraph-core will only contain graph engine logic and depend on hugegraph-struct for all type definitions

**Migration Strategy**:

1. **Phase 1 (Current)**: Both modules coexist; new features use struct
2. **Phase 2 (In Progress)**: Gradually migrate core's data structures to import from struct
3. **Phase 3 (Future)**: Remove duplicate definitions from core completely

**Example Migration**:

```java
// OLD (hugegraph-core)
import org.apache.hugegraph.schema.SchemaElement;  // ❌ Will be deprecated

// NEW (hugegraph-struct)
import org.apache.hugegraph.struct.schema.SchemaElement;  // ✅ Use this
```

### Developer Guide

#### When to Use hugegraph-struct

Use struct when:
- Building distributed components (PD, Store)
- Defining data transfer objects (DTOs) for RPC
- Implementing serialization/deserialization logic
- Working with type definitions, schema elements, or IDs
- Creating shared utilities needed across modules

#### When to Use hugegraph-core

Use core when:
- Implementing graph engine features
- Working with TinkerPop API (Gremlin traversal)
- Managing transactions or backend storage
- Implementing graph algorithms or jobs
- Building server-side business logic

#### Adding New Data Structures

**Rule**: All new shared data structures should go into `hugegraph-struct`, not `hugegraph-core`.

Example:
```java
// ✅ Correct: Add to hugegraph-struct/src/main/java/org/apache/hugegraph/struct/
public class NewSchemaType extends SchemaElement {
    // Pure data structure, no HugeGraph dependency
}

// ❌ Wrong: Don't add to hugegraph-core unless it's graph engine logic
```

#### Modifying Existing Structures

If you need to modify a data structure:

1. **Check if it exists in struct**: Modify the struct version
2. **If it only exists in core**: Consider migrating it to struct first
3. **Update serialization**: Ensure binary compatibility or provide migration

### Package Structure

```
org.apache.hugegraph/
├── struct/schema/          # Schema definitions (VertexLabel, EdgeLabel, etc.)
├── structure/              # Graph elements (BaseVertex, BaseEdge, BaseProperty)
├── type/                   # Type system (HugeType, DataType, IdStrategy)
├── id/                     # ID generation and management
├── serializer/             # Binary serialization (BytesBuffer, BinarySerializer)
├── query/                  # Query abstractions (Query, ConditionQuery, Aggregate)
├── analyzer/               # Text analyzers (8 Chinese NLP implementations)
├── auth/                   # Auth utilities (JWT, constants)
├── backend/                # Backend abstractions (BinaryId, BackendColumn, Shard)
├── options/                # Configuration options
├── util/                   # Utilities (encoding, compression, collections)
└── exception/              # Exception hierarchy
```

### Key Design Principles

1. **Stateless**: No `HugeGraph` instance dependencies in struct
2. **Minimal Dependencies**: Only essential libraries (no Jersey, JRaft, K8s)
3. **Serialization-Friendly**: All structures support binary serialization
4. **Type Safety**: Strong typing for distributed RPC communication
5. **Backward Compatible**: Careful versioning to avoid breaking changes

### Building and Testing

```bash
# Build struct module
mvn clean install -DskipTests

# Build with tests (when tests are added)
mvn clean install

# From parent directory
cd /path/to/hugegraph
mvn install -pl hugegraph-struct -am -DskipTests
```

### Contributing

When contributing to hugegraph-struct:

1. **No Business Logic**: Keep it pure data structures
2. **No Graph Instances**: Avoid `HugeGraph graph` fields
3. **Document Changes**: Update AGENTS.md if adding new packages
4. **Binary Compatibility**: Consider serialization impact
5. **Minimal Dependencies**: Justify any new dependency additions

### License

Apache License 2.0 - See [LICENSE](../LICENSE) file for details.
