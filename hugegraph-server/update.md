# TinkerPop 3.5.1 → 3.8.1 升级 & JDK 21 支持变更记录

## 概述

1. 将项目依赖的 Apache TinkerPop 版本从 **3.5.1** 升级至 **3.8.1**，涉及大量 API 破坏性变更适配。
2. 将项目 JDK 版本从 **JDK 11** 升级至 **JDK 21**，同时保留 JDK 11 兼容性（CI 双版本矩阵测试）。
3. 编译验证已通过（`mvn clean compile -DskipTests`，全部 40 个模块 BUILD SUCCESS）。

---

## 一、版本号变更

| 文件 | 变更内容 |
|------|---------|
| `hugegraph-struct/pom.xml` | `<tinkerpop.version>3.5.1</tinkerpop.version>` → `3.8.1` |
| `hugegraph-struct/pom.xml` | `gremlin-shaded` 硬编码版本 `3.5.1` → `${tinkerpop.version}` |
| `hugegraph-server/pom.xml` | `<tinkerpop.version>3.5.1</tinkerpop.version>` → `3.8.1` |
| `hugegraph-server/.../CoreVersion.java` | `GREMLIN_VERSION = "3.5.1"` → `"3.8.1"` |

---

## 二、新增依赖

**`hugegraph-server/pom.xml`** 新增 `gremlin-util` 依赖（TinkerPop 3.5.x 将 `RequestMessage`/`ResponseMessage`/`ResponseStatusCode` 等类从 `gremlin-driver` 迁移至该模块）：

```xml
<dependency>
    <groupId>org.apache.tinkerpop</groupId>
    <artifactId>gremlin-util</artifactId>
    <version>${tinkerpop.version}</version>
</dependency>
```

---

## 三、API 破坏性变更适配

### 3.1 `P` 构造函数：`BiPredicate` → `PBiPredicate`

TinkerPop 3.8.x 中 `P` 的构造函数参数从 `java.util.function.BiPredicate` 改为自定义接口 `PBiPredicate`。

| 文件 | 变更 |
|------|------|
| `ConditionP.java` | `import java.util.function.BiPredicate` → `import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate`；构造函数参数类型相应修改 |
| `Condition.java` | `RelationType` 枚举声明从 `implements BiPredicate<Object, Object>` → `implements PBiPredicate<Object, Object>` |

### 3.2 `AggregateGlobalStep`/`AggregateLocalStep` 合并为 `AggregateStep`

TinkerPop 3.8.x 将 `AggregateGlobalStep` 和 `AggregateLocalStep` 合并为单一的 `AggregateStep`。

| 文件 | 变更 |
|------|------|
| `HugeCountStepStrategy.java` | 移除 `AggregateGlobalStep` 和 `AggregateLocalStep` 的 import，合并为 `AggregateStep`；相应的 `instanceof` 条件判断也合并 |

### 3.3 `HasContainerHolder` 变为泛型接口

TinkerPop 3.8.x 中 `HasContainerHolder` 从原始类型变为 `HasContainerHolder<S, E>` 泛型接口，且 `getHasContainers()` 返回类型变为 `Iterable<?>`。

| 文件 | 变更 |
|------|------|
| `QueryHolder.java` | 接口声明从 `extends HasContainerHolder` → `extends HasContainerHolder<S, E>` |
| `HugeGraphStep.java` | 类声明改为 `implements QueryHolder<S, E>` |
| `HugeVertexStep.java` | 类声明改为 `implements QueryHolder<Vertex, E>` |
| `TraversalUtil.java` | 多处添加 `@SuppressWarnings({"rawtypes", "unchecked"})`；`getHasContainers()` 返回值使用 `Object` 类型遍历后强制转换为 `HasContainer` |

### 3.4 `Mutating.configure()` 迁移至 `Configuring` 接口

TinkerPop 3.8.x 将 `configure()` 方法从 `Mutating` 接口移至新的 `Configuring` 接口。

| 文件 | 变更 |
|------|------|
| `HugePrimaryKeyStrategy.java` | `Mutating` import → `Configuring`；`curAddStep` 变量类型相应修改 |

### 3.5 `RequestMessage`/`ResponseMessage`/`ResponseStatusCode` 包迁移 + `Tokens` 移除

TinkerPop 3.8.x 将这些类从 `org.apache.tinkerpop.gremlin.driver.message` 迁移至 `org.apache.tinkerpop.gremlin.util.message`，并移除了 `Tokens` 常量类，所有 `Tokens.*` 常量需改为字符串字面量。

| 文件 | 变更 |
|------|------|
| `CypherClient.java` | import 路径改为 `gremlin.util.message`；移除 `Tokens` import；`Tokens.OPS_EVAL` → `"eval"`；`.add(Tokens.ARGS_GREMLIN, ...)` → `.addArg("gremlin", ...)` |
| `CypherOpProcessor.java` | `ResponseStatusCode`/`RequestMessage`/`ResponseMessage` import 路径改为 `gremlin.util.message`；移除 `Tokens` import；所有 `Tokens.*` 常量替换为字符串字面量：`ARGS_GREMLIN` → `"gremlin"`、`ARGS_ALIASES` → `"aliases"`、`VAL_TRAVERSAL_SOURCE_ALIAS` → `"g"`、`ARGS_EVAL_TIMEOUT` → `"evaluationTimeout"`、`ARGS_BINDINGS` → `"bindings"` |

### 3.6 `StoreTest` 类被移除

TinkerPop 3.8.x 移除了 `StoreTest` 测试类。

| 文件 | 变更 |
|------|------|
| `ProcessBasicSuite.java` | 移除 `StoreTest` 的 import 和 `@Suite.SuiteClasses` 中的引用（2 处） |

---

## 四、修改文件完整列表

```
hugegraph-struct/pom.xml
hugegraph-server/pom.xml
hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/version/CoreVersion.java
hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/backend/query/Condition.java
hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/traversal/optimize/ConditionP.java
hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/traversal/optimize/HugeCountStepStrategy.java
hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/traversal/optimize/HugeGraphStep.java
hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/traversal/optimize/HugeVertexStep.java
hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/traversal/optimize/HugePrimaryKeyStrategy.java
hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/traversal/optimize/QueryHolder.java
hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/traversal/optimize/TraversalUtil.java
hugegraph-server/hugegraph-api/src/main/java/org/apache/hugegraph/api/cypher/CypherClient.java
hugegraph-server/hugegraph-api/src/main/java/org/apache/hugegraph/opencypher/CypherOpProcessor.java
hugegraph-server/hugegraph-test/src/main/java/org/apache/hugegraph/tinkerpop/ProcessBasicSuite.java
```

---

## 五、JDK 11 → JDK 21 升级

### 5.1 Maven 编译版本

| 文件 | 变更内容 |
|------|---------|
| `pom.xml` | `maven.compiler.source/target` 11 → 21；`maven-compiler-plugin` 3.1 → 3.11.0；`compilerArguments` → `compilerArgs`；`lombok` 1.18.30 → 1.18.34 |
| `hugegraph-struct/pom.xml` | `maven.compiler.source/target` 11 → 21 |

### 5.2 CI Workflow（全部新增 JDK 21 矩阵，同时保留 JDK 11）

| 文件 | 变更内容 |
|------|---------|
| `.github/workflows/server-ci.yml` | `JAVA_VERSION: ['11']` → `['11', '21']` |
| `.github/workflows/commons-ci.yml` | `JAVA_VERSION: ['11']` → `['11', '21']`；`setup-java@v3` → `v4` |
| `.github/workflows/pd-store-ci.yml` | `struct`/`pd`/`store`/`hstore` 四个 job 全部新增 `strategy.matrix.JAVA_VERSION: ['11', '21']`；`setup-java@v3` → `v4` |
| `.github/workflows/cluster-test-ci.yml` | 新增 `strategy.matrix.JAVA_VERSION: ['11', '21']`；`setup-java@v3` → `v4` |
| `.github/workflows/check-dependencies.yml` | JDK 11 → 21；`setup-java@v3` → `v4` |
| `.github/workflows/codeql-analysis.yml` | JDK 11 → 21；`setup-java@v3` → `v4` |

### 5.3 Dockerfile（基础镜像升级）

| 文件 | 变更内容 |
|------|---------|
| `hugegraph-server/Dockerfile` | `maven:3.9.0-eclipse-temurin-11` → `maven:3.9.9-eclipse-temurin-21`；`eclipse-temurin:11-jre` → `eclipse-temurin:21-jre` |
| `hugegraph-pd/Dockerfile` | 同上 |
| `hugegraph-store/Dockerfile` | 同上 |
| `hugegraph-server/Dockerfile-hstore` | 同上 |
